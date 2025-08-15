package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// BrokerInfo Broker信息
type BrokerInfo struct {
	BrokerName     string            `json:"brokerName"`
	BrokerId       int64             `json:"brokerId"`
	ClusterName    string            `json:"clusterName"`
	BrokerAddr     string            `json:"brokerAddr"`
	Version        string            `json:"version"`
	DataVersion    int64             `json:"dataVersion"`
	LastUpdateTime int64             `json:"lastUpdateTime"`
	Role           string            `json:"role"` // MASTER, SLAVE
	Status         BrokerStatus      `json:"status"`
	Metrics        *BrokerMetrics    `json:"metrics,omitempty"`
	Topics         map[string]*TopicRouteInfo `json:"topics,omitempty"`
}

// BrokerStatus Broker状态
type BrokerStatus int

const (
	// ONLINE 在线
	ONLINE BrokerStatus = iota
	// OFFLINE 离线
	OFFLINE
	// SUSPECT 可疑（可能故障）
	SUSPECT
)

// BrokerMetrics Broker指标
type BrokerMetrics struct {
	CpuUsage       float64 `json:"cpuUsage"`
	MemoryUsage    float64 `json:"memoryUsage"`
	DiskUsage      float64 `json:"diskUsage"`
	NetworkIn      int64   `json:"networkIn"`
	NetworkOut     int64   `json:"networkOut"`
	MessageCount   int64   `json:"messageCount"`
	Tps            int64   `json:"tps"`
	QueueDepth     int64   `json:"queueDepth"`
	ConnectionCount int32  `json:"connectionCount"`
}

// TopicRouteInfo Topic路由信息
type TopicRouteInfo struct {
	TopicName    string             `json:"topicName"`
	QueueDatas   []*QueueData       `json:"queueDatas"`
	BrokerDatas  []*BrokerData      `json:"brokerDatas"`
	OrderTopicConf *OrderTopicConf  `json:"orderTopicConf,omitempty"`
}

// QueueData 队列数据
type QueueData struct {
	BrokerName     string `json:"brokerName"`
	ReadQueueNums  int32  `json:"readQueueNums"`
	WriteQueueNums int32  `json:"writeQueueNums"`
	Perm           int32  `json:"perm"`
	TopicSynFlag   int32  `json:"topicSynFlag"`
}

// BrokerData Broker数据
type BrokerData struct {
	Cluster    string            `json:"cluster"`
	BrokerName string            `json:"brokerName"`
	BrokerAddrs map[int64]string `json:"brokerAddrs"`
}

// OrderTopicConf 顺序Topic配置
type OrderTopicConf struct {
	OrderConf string `json:"orderConf"`
}

// ClusterManager 集群管理器
type ClusterManager struct {
	clusterName    string
	brokers        map[string]*BrokerInfo // brokerName -> BrokerInfo
	topicRoutes    map[string]*TopicRouteInfo // topicName -> TopicRouteInfo
	mutex          sync.RWMutex
	healthChecker  *HealthChecker
	loadBalancer   *LoadBalancer
	running        bool
	shutdown       chan struct{}

	// 配置
	healthCheckInterval time.Duration
	brokerTimeout       time.Duration
}

// NewClusterManager 创建集群管理器
func NewClusterManager(clusterName string) *ClusterManager {
	cm := &ClusterManager{
		clusterName:         clusterName,
		brokers:             make(map[string]*BrokerInfo),
		topicRoutes:         make(map[string]*TopicRouteInfo),
		shutdown:            make(chan struct{}),
		healthCheckInterval: 30 * time.Second,
		brokerTimeout:       60 * time.Second,
	}
	
	// 初始化健康检查器和负载均衡器
	cm.healthChecker = NewHealthChecker(cm)
	cm.loadBalancer = NewLoadBalancer(cm)
	
	return cm
}

// Start 启动集群管理器
func (cm *ClusterManager) Start() error {
	if cm.running {
		return fmt.Errorf("cluster manager already running")
	}

	cm.running = true
	log.Printf("Starting cluster manager for cluster: %s", cm.clusterName)

	// 启动健康检查器
	if err := cm.healthChecker.Start(); err != nil {
		return fmt.Errorf("failed to start health checker: %v", err)
	}

	// 启动定期清理过期Broker的goroutine
	go cm.cleanupExpiredBrokers()

	return nil
}

// Stop 停止集群管理器
func (cm *ClusterManager) Stop() {
	if !cm.running {
		return
	}

	log.Printf("Stopping cluster manager for cluster: %s", cm.clusterName)
	cm.running = false
	close(cm.shutdown)

	if cm.healthChecker != nil {
		cm.healthChecker.Stop()
	}
}

// RegisterBroker 注册Broker
func (cm *ClusterManager) RegisterBroker(brokerInfo *BrokerInfo) error {
	if brokerInfo == nil {
		return fmt.Errorf("broker info cannot be nil")
	}
	
	if brokerInfo.BrokerName == "" {
		return fmt.Errorf("broker name cannot be empty")
	}

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	brokerInfo.LastUpdateTime = time.Now().UnixMilli()
	// 只有在状态为0（未设置）时才设置为ONLINE
	if brokerInfo.Status == 0 {
		brokerInfo.Status = ONLINE
	}
	cm.brokers[brokerInfo.BrokerName] = brokerInfo

	log.Printf("Registered broker: %s (id=%d, addr=%s, role=%s)", 
		brokerInfo.BrokerName, brokerInfo.BrokerId, brokerInfo.BrokerAddr, brokerInfo.Role)

	return nil
}

// UnregisterBroker 注销Broker
func (cm *ClusterManager) UnregisterBroker(brokerName string) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if broker, exists := cm.brokers[brokerName]; exists {
		broker.Status = OFFLINE
		log.Printf("Unregistered broker: %s", brokerName)
		delete(cm.brokers, brokerName)
	}
}

// UpdateBrokerMetrics 更新Broker指标
func (cm *ClusterManager) UpdateBrokerMetrics(brokerName string, metrics *BrokerMetrics) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	broker, exists := cm.brokers[brokerName]
	if !exists {
		return fmt.Errorf("broker %s not found", brokerName)
	}

	broker.Metrics = metrics
	broker.LastUpdateTime = time.Now().UnixMilli()

	return nil
}

// GetBroker 获取Broker信息
func (cm *ClusterManager) GetBroker(brokerName string) (*BrokerInfo, bool) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	broker, exists := cm.brokers[brokerName]
	return broker, exists
}

// GetAllBrokers 获取所有Broker
func (cm *ClusterManager) GetAllBrokers() map[string]*BrokerInfo {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	result := make(map[string]*BrokerInfo)
	for name, broker := range cm.brokers {
		result[name] = broker
	}
	return result
}

// GetOnlineBrokers 获取在线Broker
func (cm *ClusterManager) GetOnlineBrokers() []*BrokerInfo {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	var onlineBrokers []*BrokerInfo
	for _, broker := range cm.brokers {
		if broker.Status == ONLINE {
			onlineBrokers = append(onlineBrokers, broker)
		}
	}
	return onlineBrokers
}

// RegisterTopicRoute 注册Topic路由
func (cm *ClusterManager) RegisterTopicRoute(topicName string, routeInfo *TopicRouteInfo) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.topicRoutes[topicName] = routeInfo
	log.Printf("Registered topic route: %s", topicName)
}

// GetTopicRoute 获取Topic路由
func (cm *ClusterManager) GetTopicRoute(topicName string) (*TopicRouteInfo, bool) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	route, exists := cm.topicRoutes[topicName]
	return route, exists
}

// SelectBrokerForProducer 为生产者选择Broker
func (cm *ClusterManager) SelectBrokerForProducer(topicName string) (*BrokerInfo, error) {
	return cm.loadBalancer.SelectBrokerForProducer(topicName)
}

// SelectBrokerForConsumer 为消费者选择Broker
func (cm *ClusterManager) SelectBrokerForConsumer(topicName string, queueId int32) (*BrokerInfo, error) {
	return cm.loadBalancer.SelectBrokerForConsumer(topicName, queueId)
}

// cleanupExpiredBrokers 清理过期的Broker
func (cm *ClusterManager) cleanupExpiredBrokers() {
	ticker := time.NewTicker(cm.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.shutdown:
			return
		case <-ticker.C:
			cm.doCleanupExpiredBrokers()
		}
	}
}

// doCleanupExpiredBrokers 执行清理过期Broker
func (cm *ClusterManager) doCleanupExpiredBrokers() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	now := time.Now().UnixMilli()
	expiredBrokers := make([]string, 0)

	for brokerName, broker := range cm.brokers {
		if now-broker.LastUpdateTime > cm.brokerTimeout.Milliseconds() {
			broker.Status = OFFLINE
			expiredBrokers = append(expiredBrokers, brokerName)
			log.Printf("Broker %s marked as offline due to timeout", brokerName)
		}
	}

	// 删除过期的Broker
	for _, brokerName := range expiredBrokers {
		delete(cm.brokers, brokerName)
	}
}

// GetClusterStatus 获取集群状态
func (cm *ClusterManager) GetClusterStatus() map[string]interface{} {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	status := make(map[string]interface{})
	status["clusterName"] = cm.clusterName
	status["running"] = cm.running
	status["totalBrokers"] = len(cm.brokers)

	onlineCount := 0
	offlineCount := 0
	suspectCount := 0
	var totalMessages int64 = 0
	var totalTps int64 = 0

	for _, broker := range cm.brokers {
		switch broker.Status {
		case ONLINE:
			onlineCount++
		case OFFLINE:
			offlineCount++
		case SUSPECT:
			suspectCount++
		}
		
		if broker.Metrics != nil {
			totalMessages += broker.Metrics.MessageCount
			totalTps += broker.Metrics.Tps
		}
	}

	status["onlineBrokers"] = onlineCount
	status["offlineBrokers"] = offlineCount
	status["suspectBrokers"] = suspectCount
	status["totalTopics"] = len(cm.topicRoutes)
	status["totalMessages"] = totalMessages
	status["totalTps"] = totalTps

	return status
}

// GetBrokerMetrics 获取Broker指标汇总
func (cm *ClusterManager) GetBrokerMetrics() map[string]*BrokerMetrics {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	metrics := make(map[string]*BrokerMetrics)
	for brokerName, broker := range cm.brokers {
		if broker.Metrics != nil {
			metrics[brokerName] = broker.Metrics
		}
	}
	return metrics
}

// LoadBalancer 负载均衡器
type LoadBalancer struct {
	clusterManager *ClusterManager
	strategy       LoadBalanceStrategy
}

// LoadBalanceStrategy 负载均衡策略
type LoadBalanceStrategy int

const (
	// ROUND_ROBIN 轮询
	ROUND_ROBIN LoadBalanceStrategy = iota
	// RANDOM 随机
	RANDOM
	// LEAST_ACTIVE 最少活跃
	LEAST_ACTIVE
	// WEIGHTED_ROUND_ROBIN 加权轮询
	WEIGHTED_ROUND_ROBIN
)

// NewLoadBalancer 创建负载均衡器
func NewLoadBalancer(cm *ClusterManager) *LoadBalancer {
	return &LoadBalancer{
		clusterManager: cm,
		strategy:       ROUND_ROBIN,
	}
}

// SelectBrokerForProducer 为生产者选择Broker
func (lb *LoadBalancer) SelectBrokerForProducer(topicName string) (*BrokerInfo, error) {
	// 获取Topic路由信息
	routeInfo, exists := lb.clusterManager.GetTopicRoute(topicName)
	if !exists {
		// 如果没有路由信息，从所有在线Master中选择
		return lb.selectFromOnlineMasters()
	}

	// 从路由信息中选择Broker
	return lb.selectFromRouteInfo(routeInfo)
}

// SelectBrokerForConsumer 为消费者选择Broker
func (lb *LoadBalancer) SelectBrokerForConsumer(topicName string, queueId int32) (*BrokerInfo, error) {
	// 获取Topic路由信息
	routeInfo, exists := lb.clusterManager.GetTopicRoute(topicName)
	if !exists {
		return nil, fmt.Errorf("topic route not found: %s", topicName)
	}

	// 根据queueId选择对应的Broker
	return lb.selectBrokerByQueueId(routeInfo, queueId)
}

// selectFromOnlineMasters 从在线Master中选择
func (lb *LoadBalancer) selectFromOnlineMasters() (*BrokerInfo, error) {
	onlineBrokers := lb.clusterManager.GetOnlineBrokers()
	if len(onlineBrokers) == 0 {
		return nil, fmt.Errorf("no online brokers available")
	}

	// 过滤出Master节点
	masters := make([]*BrokerInfo, 0)
	for _, broker := range onlineBrokers {
		if broker.Role == "MASTER" {
			masters = append(masters, broker)
		}
	}

	if len(masters) == 0 {
		return nil, fmt.Errorf("no master brokers available")
	}

	return lb.selectByStrategy(masters)
}

// selectFromRouteInfo 从路由信息中选择
func (lb *LoadBalancer) selectFromRouteInfo(routeInfo *TopicRouteInfo) (*BrokerInfo, error) {
	if len(routeInfo.BrokerDatas) == 0 {
		return nil, fmt.Errorf("no broker data in route info")
	}

	// 收集可用的Broker
	availableBrokers := make([]*BrokerInfo, 0)
	for _, brokerData := range routeInfo.BrokerDatas {
		for brokerId, _ := range brokerData.BrokerAddrs {
			if broker, exists := lb.clusterManager.GetBroker(brokerData.BrokerName); exists {
				if broker.Status == ONLINE && broker.BrokerId == brokerId {
					availableBrokers = append(availableBrokers, broker)
				}
			}
		}
	}

	if len(availableBrokers) == 0 {
		return nil, fmt.Errorf("no available brokers in route info")
	}

	return lb.selectByStrategy(availableBrokers)
}

// selectBrokerByQueueId 根据队列ID选择Broker
func (lb *LoadBalancer) selectBrokerByQueueId(routeInfo *TopicRouteInfo, queueId int32) (*BrokerInfo, error) {
	if len(routeInfo.QueueDatas) == 0 {
		return nil, fmt.Errorf("no queue data in route info")
	}

	// 根据queueId找到对应的Broker
	for _, queueData := range routeInfo.QueueDatas {
		if queueId < queueData.ReadQueueNums {
			if broker, exists := lb.clusterManager.GetBroker(queueData.BrokerName); exists {
				if broker.Status == ONLINE {
					return broker, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("no available broker for queue %d", queueId)
}

// selectByStrategy 根据策略选择Broker
func (lb *LoadBalancer) selectByStrategy(brokers []*BrokerInfo) (*BrokerInfo, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers to select from")
	}

	switch lb.strategy {
	case ROUND_ROBIN:
		return lb.roundRobinSelect(brokers), nil
	case RANDOM:
		return lb.randomSelect(brokers), nil
	case LEAST_ACTIVE:
		return lb.leastActiveSelect(brokers), nil
	case WEIGHTED_ROUND_ROBIN:
		return lb.weightedRoundRobinSelect(brokers), nil
	default:
		return brokers[0], nil
	}
}

// roundRobinSelect 轮询选择
func (lb *LoadBalancer) roundRobinSelect(brokers []*BrokerInfo) *BrokerInfo {
	// 简单实现：按名称排序后取第一个
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].BrokerName < brokers[j].BrokerName
	})
	return brokers[0]
}

// randomSelect 随机选择
func (lb *LoadBalancer) randomSelect(brokers []*BrokerInfo) *BrokerInfo {
	// 简单实现：取第一个
	return brokers[0]
}

// leastActiveSelect 最少活跃选择
func (lb *LoadBalancer) leastActiveSelect(brokers []*BrokerInfo) *BrokerInfo {
	// 根据连接数选择最少的
	minConnections := int32(999999)
	var selectedBroker *BrokerInfo

	for _, broker := range brokers {
		if broker.Metrics != nil && broker.Metrics.ConnectionCount < minConnections {
			minConnections = broker.Metrics.ConnectionCount
			selectedBroker = broker
		}
	}

	if selectedBroker != nil {
		return selectedBroker
	}
	return brokers[0]
}

// weightedRoundRobinSelect 加权轮询选择
func (lb *LoadBalancer) weightedRoundRobinSelect(brokers []*BrokerInfo) *BrokerInfo {
	// 根据CPU使用率选择负载最低的
	minCpuUsage := float64(100.0)
	var selectedBroker *BrokerInfo

	for _, broker := range brokers {
		if broker.Metrics != nil && broker.Metrics.CpuUsage < minCpuUsage {
			minCpuUsage = broker.Metrics.CpuUsage
			selectedBroker = broker
		}
	}

	if selectedBroker != nil {
		return selectedBroker
	}
	return brokers[0]
}

// SetStrategy 设置负载均衡策略
func (lb *LoadBalancer) SetStrategy(strategy LoadBalanceStrategy) {
	lb.strategy = strategy
}

// HTTPHandler HTTP处理器，用于集群管理API
func (cm *ClusterManager) HTTPHandler() http.Handler {
	mux := http.NewServeMux()

	// 获取集群状态
	mux.HandleFunc("/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		status := cm.GetClusterStatus()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	// 获取所有Broker
	mux.HandleFunc("/cluster/brokers", func(w http.ResponseWriter, r *http.Request) {
		brokers := cm.GetAllBrokers()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(brokers)
	})

	// 获取Broker指标
	mux.HandleFunc("/cluster/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics := cm.GetBrokerMetrics()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	})

	return mux
}