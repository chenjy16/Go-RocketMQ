package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
	"go-rocketmq/pkg/cluster"
)

// BrokerClusterConfig Broker 集群配置
type BrokerClusterConfig struct {
	ClusterName     string            `yaml:"cluster_name"`
	NameServerAddrs []string          `yaml:"nameserver_addrs"`
	BrokerGroups    []*BrokerGroup    `yaml:"broker_groups"`
	TopicConfig     []*TopicConfig    `yaml:"topic_config"`
	LoadBalance     LoadBalanceConfig `yaml:"load_balance"`
	Monitoring      MonitoringConfig  `yaml:"monitoring"`
}

// BrokerGroup Broker 组配置
type BrokerGroup struct {
	GroupName    string   `yaml:"group_name"`
	MasterAddr   string   `yaml:"master_addr"`
	SlaveAddrs   []string `yaml:"slave_addrs"`
	Role         string   `yaml:"role"` // MASTER, SLAVE
	Weight       int      `yaml:"weight"`
	MaxConnections int    `yaml:"max_connections"`
}

// TopicConfig Topic 配置
type TopicConfig struct {
	TopicName      string `yaml:"topic_name"`
	QueueNum       int    `yaml:"queue_num"`
	Permission     string `yaml:"permission"` // READ, WRITE, READ_WRITE
	BrokerGroups   []string `yaml:"broker_groups"`
}

// LoadBalanceConfig 负载均衡配置
type LoadBalanceConfig struct {
	Strategy    string `yaml:"strategy"` // ROUND_ROBIN, RANDOM, LEAST_ACTIVE
	StickySession bool `yaml:"sticky_session"`
	HealthCheck bool `yaml:"health_check"`
}

// MonitoringConfig 监控配置
type MonitoringConfig struct {
	Enabled         bool          `yaml:"enabled"`
	MetricsInterval time.Duration `yaml:"metrics_interval"`
	AlertThreshold  AlertThreshold `yaml:"alert_threshold"`
}

// AlertThreshold 告警阈值
type AlertThreshold struct {
	CpuUsage    float64 `yaml:"cpu_usage"`
	MemoryUsage float64 `yaml:"memory_usage"`
	DiskUsage   float64 `yaml:"disk_usage"`
	QueueDepth  int64   `yaml:"queue_depth"`
	Tps         int64   `yaml:"tps"`
}

// BrokerClusterManager Broker 集群管理器
type BrokerClusterManager struct {
	config         *BrokerClusterConfig
	clusterMgr     *cluster.ClusterManager
	brokerGroups   map[string]*BrokerGroupManager
	topicManagers  map[string]*TopicManager
	loadBalancer   *ClusterLoadBalancer
	monitor        *ClusterMonitor
	running        bool
	mutex          sync.RWMutex
}

// BrokerGroupManager Broker 组管理器
type BrokerGroupManager struct {
	config      *BrokerGroup
	masterInfo  *cluster.BrokerInfo
	slaveInfos  []*cluster.BrokerInfo
	producers   []*client.Producer
	consumers   []*client.Consumer
	status      BrokerGroupStatus
	metrics     *BrokerGroupMetrics
	mutex       sync.RWMutex
}

// BrokerGroupStatus Broker 组状态
type BrokerGroupStatus struct {
	IsHealthy       bool      `json:"is_healthy"`
	MasterOnline    bool      `json:"master_online"`
	SlaveOnlineCount int      `json:"slave_online_count"`
	LastUpdateTime  time.Time `json:"last_update_time"`
	ErrorMessage    string    `json:"error_message"`
}

// BrokerGroupMetrics Broker 组指标
type BrokerGroupMetrics struct {
	TotalMessages   int64   `json:"total_messages"`
	SuccessMessages int64   `json:"success_messages"`
	FailedMessages  int64   `json:"failed_messages"`
	AvgLatency      float64 `json:"avg_latency"`
	Throughput      float64 `json:"throughput"`
	QueueDepth      int64   `json:"queue_depth"`
	ConnectionCount int32   `json:"connection_count"`
	mutex           sync.RWMutex
}

// TopicManager Topic 管理器
type TopicManager struct {
	config        *TopicConfig
	brokerGroups  []*BrokerGroupManager
	routeInfo     *cluster.TopicRouteInfo
	producers     []*client.Producer
	consumers     []*client.Consumer
	metrics       *TopicMetrics
	mutex         sync.RWMutex
}

// TopicMetrics Topic 指标
type TopicMetrics struct {
	MessageCount    int64   `json:"message_count"`
	ProducerCount   int32   `json:"producer_count"`
	ConsumerCount   int32   `json:"consumer_count"`
	AvgMessageSize  float64 `json:"avg_message_size"`
	Throughput      float64 `json:"throughput"`
	mutex           sync.RWMutex
}

// ClusterLoadBalancer 集群负载均衡器
type ClusterLoadBalancer struct {
	config       *LoadBalanceConfig
	brokerGroups map[string]*BrokerGroupManager
	strategy     cluster.LoadBalanceStrategy
	mutex        sync.RWMutex
}

// ClusterMonitor 集群监控器
type ClusterMonitor struct {
	config         *MonitoringConfig
	brokerCluster  *BrokerClusterManager
	metrics        *ClusterMetrics
	alerts         []*Alert
	running        bool
	mutex          sync.RWMutex
}

// ClusterMetrics 集群指标
type ClusterMetrics struct {
	TotalBrokers    int32   `json:"total_brokers"`
	OnlineBrokers   int32   `json:"online_brokers"`
	TotalTopics     int32   `json:"total_topics"`
	TotalQueues     int32   `json:"total_queues"`
	TotalMessages   int64   `json:"total_messages"`
	TotalThroughput float64 `json:"total_throughput"`
	AvgLatency      float64 `json:"avg_latency"`
	LastUpdateTime  time.Time `json:"last_update_time"`
	mutex           sync.RWMutex
}

// Alert 告警信息
type Alert struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Level       string    `json:"level"` // INFO, WARNING, ERROR, CRITICAL
	Message     string    `json:"message"`
	BrokerGroup string    `json:"broker_group"`
	Topic       string    `json:"topic"`
	Timestamp   time.Time `json:"timestamp"`
	Resolved    bool      `json:"resolved"`
}

// NewBrokerClusterManager 创建 Broker 集群管理器
func NewBrokerClusterManager(config *BrokerClusterConfig) (*BrokerClusterManager, error) {
	clusterMgr := cluster.NewClusterManager(config.ClusterName)
	
	// 创建负载均衡器
	loadBalancer := &ClusterLoadBalancer{
		config:       &config.LoadBalance,
		brokerGroups: make(map[string]*BrokerGroupManager),
		strategy:     cluster.ROUND_ROBIN, // 默认轮询
	}
	
	// 创建监控器
	monitor := &ClusterMonitor{
		config:  &config.Monitoring,
		metrics: &ClusterMetrics{},
		alerts:  make([]*Alert, 0),
		running: false,
	}
	
	bcm := &BrokerClusterManager{
		config:        config,
		clusterMgr:    clusterMgr,
		brokerGroups:  make(map[string]*BrokerGroupManager),
		topicManagers: make(map[string]*TopicManager),
		loadBalancer:  loadBalancer,
		monitor:       monitor,
		running:       false,
	}
	
	monitor.brokerCluster = bcm
	loadBalancer.brokerGroups = bcm.brokerGroups
	
	return bcm, nil
}

// Start 启动 Broker 集群管理器
func (bcm *BrokerClusterManager) Start() error {
	log.Println("启动 Broker 集群管理器...")
	
	// 启动集群管理器
	if err := bcm.clusterMgr.Start(); err != nil {
		return fmt.Errorf("启动集群管理器失败: %v", err)
	}
	
	// 初始化 Broker 组
	if err := bcm.initializeBrokerGroups(); err != nil {
		return fmt.Errorf("初始化 Broker 组失败: %v", err)
	}
	
	// 初始化 Topic 管理器
	if err := bcm.initializeTopicManagers(); err != nil {
		return fmt.Errorf("初始化 Topic 管理器失败: %v", err)
	}
	
	// 启动监控
	if bcm.config.Monitoring.Enabled {
		go bcm.startMonitoring()
	}
	
	bcm.running = true
	log.Println("Broker 集群管理器启动完成")
	return nil
}

// initializeBrokerGroups 初始化 Broker 组
func (bcm *BrokerClusterManager) initializeBrokerGroups() error {
	log.Printf("初始化 %d 个 Broker 组...", len(bcm.config.BrokerGroups))
	
	for _, groupConfig := range bcm.config.BrokerGroups {
		groupMgr, err := bcm.createBrokerGroupManager(groupConfig)
		if err != nil {
			return fmt.Errorf("创建 Broker 组 %s 失败: %v", groupConfig.GroupName, err)
		}
		
		bcm.brokerGroups[groupConfig.GroupName] = groupMgr
		log.Printf("Broker 组 %s 初始化完成", groupConfig.GroupName)
	}
	
	return nil
}

// createBrokerGroupManager 创建 Broker 组管理器
func (bcm *BrokerClusterManager) createBrokerGroupManager(config *BrokerGroup) (*BrokerGroupManager, error) {
	groupMgr := &BrokerGroupManager{
		config:     config,
		slaveInfos: make([]*cluster.BrokerInfo, 0),
		producers:  make([]*client.Producer, 0),
		consumers:  make([]*client.Consumer, 0),
		status: BrokerGroupStatus{
			IsHealthy:        true,
			MasterOnline:     true,
			SlaveOnlineCount: len(config.SlaveAddrs),
			LastUpdateTime:   time.Now(),
		},
		metrics: &BrokerGroupMetrics{},
	}
	
	// 创建主 Broker 信息
	groupMgr.masterInfo = &cluster.BrokerInfo{
		BrokerName: config.GroupName + "_master",
		BrokerId:   0,
		ClusterName: bcm.config.ClusterName,
		BrokerAddr: config.MasterAddr,
		Role:       "MASTER",
		Status:     cluster.ONLINE,
	}
	
	// 创建从 Broker 信息
	for i, slaveAddr := range config.SlaveAddrs {
		slaveInfo := &cluster.BrokerInfo{
			BrokerName: fmt.Sprintf("%s_slave_%d", config.GroupName, i),
			BrokerId:   int64(i + 1),
			ClusterName: bcm.config.ClusterName,
			BrokerAddr: slaveAddr,
			Role:       "SLAVE",
			Status:     cluster.ONLINE,
		}
		groupMgr.slaveInfos = append(groupMgr.slaveInfos, slaveInfo)
	}
	
	// 创建生产者
	if err := groupMgr.createProducers(bcm.config.NameServerAddrs); err != nil {
		return nil, fmt.Errorf("创建生产者失败: %v", err)
	}
	
	// 创建消费者
	if err := groupMgr.createConsumers(bcm.config.NameServerAddrs); err != nil {
		return nil, fmt.Errorf("创建消费者失败: %v", err)
	}
	
	return groupMgr, nil
}

// createProducers 创建生产者
func (bgm *BrokerGroupManager) createProducers(nameServerAddrs []string) error {
	// 为主 Broker 创建生产者
	producer := client.NewProducer(bgm.config.GroupName + "_producer_group")
	producer.SetNameServers(nameServerAddrs)
	
	if err := producer.Start(); err != nil {
		return fmt.Errorf("启动生产者失败: %v", err)
	}
	
	bgm.producers = append(bgm.producers, producer)
	return nil
}

// createConsumers 创建消费者
func (bgm *BrokerGroupManager) createConsumers(nameServerAddrs []string) error {
	consumerConfig := &client.ConsumerConfig{
		GroupName:      bgm.config.GroupName + "_consumer_group",
		NameServerAddr: nameServerAddrs[0],
	}
	
	consumer := client.NewConsumer(consumerConfig)
	
	// 创建消息监听器
	listener := client.MessageListenerConcurrently(func(msgs []*client.MessageExt) client.ConsumeResult {
		log.Printf("Broker组 %s 消费者收到消息: %d条", bgm.config.GroupName, len(msgs))
		return client.ConsumeSuccess
	})
	
	// 订阅主题
	if err := consumer.Subscribe("broker_cluster_test_topic", "*", listener); err != nil {
		return fmt.Errorf("订阅主题失败: %v", err)
	}
	
	if err := consumer.Start(); err != nil {
		return fmt.Errorf("启动消费者失败: %v", err)
	}
	
	bgm.consumers = append(bgm.consumers, consumer)
	return nil
}

// initializeTopicManagers 初始化 Topic 管理器
func (bcm *BrokerClusterManager) initializeTopicManagers() error {
	log.Printf("初始化 %d 个 Topic 管理器...", len(bcm.config.TopicConfig))
	
	for _, topicConfig := range bcm.config.TopicConfig {
		topicMgr, err := bcm.createTopicManager(topicConfig)
		if err != nil {
			return fmt.Errorf("创建 Topic 管理器 %s 失败: %v", topicConfig.TopicName, err)
		}
		
		bcm.topicManagers[topicConfig.TopicName] = topicMgr
		log.Printf("Topic 管理器 %s 初始化完成", topicConfig.TopicName)
	}
	
	return nil
}

// createTopicManager 创建 Topic 管理器
func (bcm *BrokerClusterManager) createTopicManager(config *TopicConfig) (*TopicManager, error) {
	topicMgr := &TopicManager{
		config:        config,
		brokerGroups:  make([]*BrokerGroupManager, 0),
		producers:     make([]*client.Producer, 0),
		consumers:     make([]*client.Consumer, 0),
		metrics:       &TopicMetrics{},
	}
	
	// 关联 Broker 组
	for _, groupName := range config.BrokerGroups {
		if groupMgr, exists := bcm.brokerGroups[groupName]; exists {
			topicMgr.brokerGroups = append(topicMgr.brokerGroups, groupMgr)
		}
	}
	
	// 创建路由信息
	topicMgr.routeInfo = &cluster.TopicRouteInfo{
		TopicName:   config.TopicName,
		QueueDatas:  make([]*cluster.QueueData, 0),
		BrokerDatas: make([]*cluster.BrokerData, 0),
	}
	
	return topicMgr, nil
}

// startMonitoring 启动监控
func (bcm *BrokerClusterManager) startMonitoring() {
	bcm.monitor.running = true
	ticker := time.NewTicker(bcm.config.Monitoring.MetricsInterval)
	defer ticker.Stop()
	
	for bcm.monitor.running {
		select {
		case <-ticker.C:
			bcm.collectMetrics()
			bcm.checkAlerts()
		}
	}
}

// collectMetrics 收集指标
func (bcm *BrokerClusterManager) collectMetrics() {
	bcm.monitor.mutex.Lock()
	defer bcm.monitor.mutex.Unlock()
	
	metrics := bcm.monitor.metrics
	metrics.TotalBrokers = int32(len(bcm.brokerGroups))
	metrics.TotalTopics = int32(len(bcm.topicManagers))
	metrics.LastUpdateTime = time.Now()
	
	// 收集 Broker 组指标
	var totalMessages int64
	var totalThroughput float64
	var onlineBrokers int32
	
	for _, groupMgr := range bcm.brokerGroups {
		groupMgr.collectMetrics()
		
		if groupMgr.status.IsHealthy {
			onlineBrokers++
		}
		
		totalMessages += groupMgr.metrics.TotalMessages
		totalThroughput += groupMgr.metrics.Throughput
	}
	
	metrics.OnlineBrokers = onlineBrokers
	metrics.TotalMessages = totalMessages
	metrics.TotalThroughput = totalThroughput
	
	// 收集 Topic 指标
	var totalQueues int32
	for _, topicMgr := range bcm.topicManagers {
		topicMgr.collectMetrics()
		totalQueues += int32(topicMgr.config.QueueNum)
	}
	
	metrics.TotalQueues = totalQueues
	
	log.Printf("集群指标更新: Brokers=%d/%d, Topics=%d, Queues=%d, Messages=%d, Throughput=%.2f",
		metrics.OnlineBrokers, metrics.TotalBrokers, metrics.TotalTopics,
		metrics.TotalQueues, metrics.TotalMessages, metrics.TotalThroughput)
}

// collectMetrics 收集 Broker 组指标
func (bgm *BrokerGroupManager) collectMetrics() {
	bgm.mutex.Lock()
	defer bgm.mutex.Unlock()
	
	// 模拟指标收集
	bgm.metrics.TotalMessages += 100
	bgm.metrics.SuccessMessages += 95
	bgm.metrics.FailedMessages += 5
	bgm.metrics.Throughput = 50.0 // 50 msg/s
	bgm.metrics.AvgLatency = 10.5 // 10.5ms
	bgm.metrics.QueueDepth = 1000
	bgm.metrics.ConnectionCount = 50
	
	// 更新状态
	bgm.status.LastUpdateTime = time.Now()
}

// collectMetrics 收集 Topic 指标
func (tm *TopicManager) collectMetrics() {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	
	// 模拟指标收集
	tm.metrics.MessageCount += 50
	tm.metrics.ProducerCount = int32(len(tm.producers))
	tm.metrics.ConsumerCount = int32(len(tm.consumers))
	tm.metrics.AvgMessageSize = 1024.0 // 1KB
	tm.metrics.Throughput = 25.0       // 25 msg/s
}

// checkAlerts 检查告警
func (bcm *BrokerClusterManager) checkAlerts() {
	bcm.monitor.mutex.Lock()
	defer bcm.monitor.mutex.Unlock()
	
	threshold := bcm.config.Monitoring.AlertThreshold
	
	// 检查集群级别告警
	if bcm.monitor.metrics.OnlineBrokers < bcm.monitor.metrics.TotalBrokers {
		alert := &Alert{
			ID:        fmt.Sprintf("cluster_broker_offline_%d", time.Now().Unix()),
			Type:      "BROKER_OFFLINE",
			Level:     "WARNING",
			Message:   fmt.Sprintf("部分 Broker 离线: %d/%d", bcm.monitor.metrics.OnlineBrokers, bcm.monitor.metrics.TotalBrokers),
			Timestamp: time.Now(),
			Resolved:  false,
		}
		bcm.monitor.alerts = append(bcm.monitor.alerts, alert)
		log.Printf("告警: %s", alert.Message)
	}
	
	// 检查 Broker 组级别告警
	for groupName, groupMgr := range bcm.brokerGroups {
		groupMgr.mutex.RLock()
		metrics := groupMgr.metrics
		
		// 检查队列深度
		if metrics.QueueDepth > threshold.QueueDepth {
			alert := &Alert{
				ID:          fmt.Sprintf("queue_depth_%s_%d", groupName, time.Now().Unix()),
				Type:        "QUEUE_DEPTH_HIGH",
				Level:       "WARNING",
				Message:     fmt.Sprintf("Broker 组 %s 队列深度过高: %d", groupName, metrics.QueueDepth),
				BrokerGroup: groupName,
				Timestamp:   time.Now(),
				Resolved:    false,
			}
			bcm.monitor.alerts = append(bcm.monitor.alerts, alert)
			log.Printf("告警: %s", alert.Message)
		}
		
		// 检查吞吐量
		if metrics.Throughput > float64(threshold.Tps) {
			alert := &Alert{
				ID:          fmt.Sprintf("tps_high_%s_%d", groupName, time.Now().Unix()),
				Type:        "TPS_HIGH",
				Level:       "INFO",
				Message:     fmt.Sprintf("Broker 组 %s 吞吐量较高: %.2f", groupName, metrics.Throughput),
				BrokerGroup: groupName,
				Timestamp:   time.Now(),
				Resolved:    false,
			}
			bcm.monitor.alerts = append(bcm.monitor.alerts, alert)
			log.Printf("告警: %s", alert.Message)
		}
		
		groupMgr.mutex.RUnlock()
	}
}

// TestBrokerCluster 测试 Broker 集群
func (bcm *BrokerClusterManager) TestBrokerCluster() error {
	log.Println("开始 Broker 集群测试...")
	
	// 测试消息发送到不同 Broker 组
	if err := bcm.testMessageDistribution(); err != nil {
		return fmt.Errorf("消息分发测试失败: %v", err)
	}
	
	// 测试负载均衡
	if err := bcm.testLoadBalancing(); err != nil {
		return fmt.Errorf("负载均衡测试失败: %v", err)
	}
	
	// 测试故障转移
	if err := bcm.testFailover(); err != nil {
		return fmt.Errorf("故障转移测试失败: %v", err)
	}
	
	log.Println("Broker 集群测试完成")
	return nil
}

// testMessageDistribution 测试消息分发
func (bcm *BrokerClusterManager) testMessageDistribution() error {
	log.Println("测试消息分发...")
	
	for groupName, groupMgr := range bcm.brokerGroups {
		if len(groupMgr.producers) == 0 {
			continue
		}
		
		producer := groupMgr.producers[0]
		for i := 0; i < 5; i++ {
			msg := &client.Message{
				Topic: "broker_cluster_test_topic",
				Body:  []byte(fmt.Sprintf("message from group %s #%d", groupName, i)),
				Tags:  "cluster_test",
			}
			
			result, err := producer.SendSync(msg)
			if err != nil {
				log.Printf("Broker 组 %s 发送消息失败: %v", groupName, err)
			} else {
				log.Printf("Broker 组 %s 发送消息成功: %s", groupName, result.MsgId)
			}
			
			time.Sleep(100 * time.Millisecond)
		}
	}
	
	return nil
}

// testLoadBalancing 测试负载均衡
func (bcm *BrokerClusterManager) testLoadBalancing() error {
	log.Println("测试负载均衡...")
	
	// 模拟负载均衡测试
	for i := 0; i < 10; i++ {
		selectedGroup := bcm.loadBalancer.selectBrokerGroup("broker_cluster_test_topic")
		if selectedGroup != nil {
			log.Printf("负载均衡选择 Broker 组: %s", selectedGroup.config.GroupName)
		}
		time.Sleep(50 * time.Millisecond)
	}
	
	return nil
}

// selectBrokerGroup 选择 Broker 组
func (clb *ClusterLoadBalancer) selectBrokerGroup(topicName string) *BrokerGroupManager {
	clb.mutex.RLock()
	defer clb.mutex.RUnlock()
	
	// 获取健康的 Broker 组
	healthyGroups := make([]*BrokerGroupManager, 0)
	for _, groupMgr := range clb.brokerGroups {
		if groupMgr.status.IsHealthy {
			healthyGroups = append(healthyGroups, groupMgr)
		}
	}
	
	if len(healthyGroups) == 0 {
		return nil
	}
	
	// 简单轮询选择
	return healthyGroups[time.Now().Unix()%int64(len(healthyGroups))]
}

// testFailover 测试故障转移
func (bcm *BrokerClusterManager) testFailover() error {
	log.Println("测试故障转移...")
	
	// 模拟第一个 Broker 组故障
	if len(bcm.brokerGroups) > 0 {
		var firstGroup *BrokerGroupManager
		for _, groupMgr := range bcm.brokerGroups {
			firstGroup = groupMgr
			break
		}
		
		if firstGroup != nil {
			log.Printf("模拟 Broker 组 %s 故障", firstGroup.config.GroupName)
			
			// 标记为不健康
			firstGroup.mutex.Lock()
			firstGroup.status.IsHealthy = false
			firstGroup.status.ErrorMessage = "Simulated failure"
			firstGroup.mutex.Unlock()
			
			// 等待一段时间
			time.Sleep(2 * time.Second)
			
			// 恢复健康状态
			firstGroup.mutex.Lock()
			firstGroup.status.IsHealthy = true
			firstGroup.status.ErrorMessage = ""
			firstGroup.mutex.Unlock()
			
			log.Printf("Broker 组 %s 恢复正常", firstGroup.config.GroupName)
		}
	}
	
	return nil
}

// GetClusterStatus 获取集群状态
func (bcm *BrokerClusterManager) GetClusterStatus() map[string]interface{} {
	bcm.mutex.RLock()
	defer bcm.mutex.RUnlock()
	
	brokerGroupStatus := make(map[string]interface{})
	for groupName, groupMgr := range bcm.brokerGroups {
		groupMgr.mutex.RLock()
		brokerGroupStatus[groupName] = map[string]interface{}{
			"status":  groupMgr.status,
			"metrics": groupMgr.metrics,
			"master":  groupMgr.masterInfo,
			"slaves":  groupMgr.slaveInfos,
		}
		groupMgr.mutex.RUnlock()
	}
	
	topicStatus := make(map[string]interface{})
	for topicName, topicMgr := range bcm.topicManagers {
		topicMgr.mutex.RLock()
		topicStatus[topicName] = map[string]interface{}{
			"config":  topicMgr.config,
			"metrics": topicMgr.metrics,
		}
		topicMgr.mutex.RUnlock()
	}
	
	return map[string]interface{}{
		"running":        bcm.running,
		"cluster_name":   bcm.config.ClusterName,
		"broker_groups":  brokerGroupStatus,
		"topics":         topicStatus,
		"cluster_metrics": bcm.monitor.metrics,
		"alerts":         bcm.monitor.alerts,
	}
}

// Stop 停止 Broker 集群管理器
func (bcm *BrokerClusterManager) Stop() error {
	log.Println("停止 Broker 集群管理器...")
	
	bcm.running = false
	bcm.monitor.running = false
	
	// 停止所有 Broker 组
	for groupName, groupMgr := range bcm.brokerGroups {
		log.Printf("停止 Broker 组: %s", groupName)
		
		// 关闭生产者
		for i, producer := range groupMgr.producers {
			log.Printf("关闭 Broker 组 %s 生产者 %d", groupName, i)
			producer.Shutdown()
		}
		
		// 关闭消费者
		for i, consumer := range groupMgr.consumers {
			log.Printf("关闭 Broker 组 %s 消费者 %d", groupName, i)
			consumer.Stop()
		}
	}
	
	// 停止集群管理器
	bcm.clusterMgr.Stop()
	
	log.Println("Broker 集群管理器已停止")
	return nil
}

func main() {
	// Broker 集群配置
	config := &BrokerClusterConfig{
		ClusterName:     "broker-cluster",
		NameServerAddrs: []string{"127.0.0.1:9876"},
		BrokerGroups: []*BrokerGroup{
			{
				GroupName:      "group1",
				MasterAddr:     "127.0.0.1:10911",
				SlaveAddrs:     []string{"127.0.0.1:10912"},
				Role:           "MASTER",
				Weight:         100,
				MaxConnections: 1000,
			},
			{
				GroupName:      "group2",
				MasterAddr:     "127.0.0.1:10913",
				SlaveAddrs:     []string{"127.0.0.1:10914"},
				Role:           "MASTER",
				Weight:         100,
				MaxConnections: 1000,
			},
		},
		TopicConfig: []*TopicConfig{
			{
				TopicName:    "broker_cluster_test_topic",
				QueueNum:     8,
				Permission:   "READ_WRITE",
				BrokerGroups: []string{"group1", "group2"},
			},
		},
		LoadBalance: LoadBalanceConfig{
			Strategy:      "ROUND_ROBIN",
			StickySession: false,
			HealthCheck:   true,
		},
		Monitoring: MonitoringConfig{
			Enabled:         true,
			MetricsInterval: 10 * time.Second,
			AlertThreshold: AlertThreshold{
				CpuUsage:    80.0,
				MemoryUsage: 85.0,
				DiskUsage:   90.0,
				QueueDepth:  10000,
				Tps:         1000,
			},
		},
	}
	
	// 创建 Broker 集群管理器
	brokerClusterMgr, err := NewBrokerClusterManager(config)
	if err != nil {
		log.Fatalf("创建 Broker 集群管理器失败: %v", err)
	}
	
	// 启动 Broker 集群管理器
	if err := brokerClusterMgr.Start(); err != nil {
		log.Fatalf("启动 Broker 集群管理器失败: %v", err)
	}
	defer brokerClusterMgr.Stop()
	
	// 运行 Broker 集群测试
	go func() {
		time.Sleep(2 * time.Second)
		if err := brokerClusterMgr.TestBrokerCluster(); err != nil {
			log.Printf("Broker 集群测试失败: %v", err)
		}
	}()
	
	// 定期打印集群状态
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	
	for i := 0; i < 4; i++ {
		select {
		case <-ticker.C:
			status := brokerClusterMgr.GetClusterStatus()
			log.Printf("Broker 集群状态: %+v", status)
		}
	}
	
	log.Println("Broker 集群示例运行完成")
}