package nameserver

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"go-rocketmq/pkg/acl"
	"go-rocketmq/pkg/common"
	"go-rocketmq/pkg/protocol"
)

// NameServer NameServer服务器
type NameServer struct {
	config       *Config
	routeTable   *RouteInfoManager
	brokerLiveTable map[string]*BrokerLiveInfo
	filterServerTable map[string][]string
	aclMiddleware *acl.AclMiddleware
	mutex        sync.RWMutex
	listener     net.Listener
	shutdown     chan struct{}
}

// Config NameServer配置
type Config struct {
	ListenPort                int           `json:"listenPort"`
	ClusterTestEnable         bool          `json:"clusterTestEnable"`
	OrderMessageEnable        bool          `json:"orderMessageEnable"`
	ScanNotActiveBrokerInterval time.Duration `json:"scanNotActiveBrokerInterval"`
	AclEnable                 bool          `json:"aclEnable"`
	AclConfigFile             string        `json:"aclConfigFile"`
}

// BrokerLiveInfo Broker存活信息
type BrokerLiveInfo struct {
	LastUpdateTimestamp time.Time `json:"lastUpdateTimestamp"`
	DataVersion         *protocol.DataVersion `json:"dataVersion"`
	Channel             net.Conn  `json:"-"`
	HaServerAddr        string    `json:"haServerAddr"`
}

// RouteInfoManager 路由信息管理器
type RouteInfoManager struct {
	topicQueueTable   map[string][]*common.MessageQueue  // topic -> queues
	brokerAddrTable   map[string]map[int64]string        // brokerName -> {brokerId -> address}
	clusterAddrTable  map[string][]string                // clusterName -> brokerNames
	brokerLiveTable   map[string]*BrokerLiveInfo         // brokerAddr -> BrokerLiveInfo
	filterServerTable map[string][]string                // brokerAddr -> filterServerList
	mutex             sync.RWMutex
}

// NewNameServer 创建NameServer实例
func NewNameServer(config *Config) *NameServer {
	ns := &NameServer{
		config:            config,
		routeTable:        NewRouteInfoManager(),
		brokerLiveTable:   make(map[string]*BrokerLiveInfo),
		filterServerTable: make(map[string][]string),
		shutdown:          make(chan struct{}),
	}
	
	// 初始化ACL中间件
	if config.AclEnable {
		validator := acl.NewPlainAclValidator(config.AclConfigFile)
		ns.aclMiddleware = acl.NewAclMiddleware(validator, true)
	}
	
	return ns
}

// NewRouteInfoManager 创建路由信息管理器
func NewRouteInfoManager() *RouteInfoManager {
	return &RouteInfoManager{
		topicQueueTable:   make(map[string][]*common.MessageQueue),
		brokerAddrTable:   make(map[string]map[int64]string),
		clusterAddrTable:  make(map[string][]string),
		brokerLiveTable:   make(map[string]*BrokerLiveInfo),
		filterServerTable: make(map[string][]string),
	}
}

// Start 启动NameServer
func (ns *NameServer) Start() error {
	addr := fmt.Sprintf(":%d", ns.config.ListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	ns.listener = listener
	log.Printf("NameServer started on %s", addr)

	// 启动清理任务
	go ns.scanNotActiveBroker()

	// 处理连接
	go ns.handleConnections()

	return nil
}

// Stop 停止NameServer
func (ns *NameServer) Stop() {
	close(ns.shutdown)
	if ns.listener != nil {
		ns.listener.Close()
	}
	log.Println("NameServer stopped")
}

// handleConnections 处理客户端连接
func (ns *NameServer) handleConnections() {
	for {
		select {
		case <-ns.shutdown:
			return
		default:
			conn, err := ns.listener.Accept()
			if err != nil {
				select {
				case <-ns.shutdown:
					return
				default:
					log.Printf("Failed to accept connection: %v", err)
					continue
				}
			}
			go ns.handleConnection(conn)
		}
	}
}

// handleConnection 处理单个连接
func (ns *NameServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	
	remoteAddr := conn.RemoteAddr().String()
	log.Printf("New connection from %s", remoteAddr)
	
	// ACL权限验证
	if ns.config.AclEnable && ns.aclMiddleware != nil {
		if err := ns.validateConnection(conn, remoteAddr); err != nil {
			log.Printf("ACL validation failed for %s: %v", remoteAddr, err)
			return
		}
	}
	
	// TODO: 实现完整的协议处理逻辑
	// 这里应该实现RocketMQ的通信协议解析和处理
}

// RegisterBroker 注册Broker
func (ns *NameServer) RegisterBroker(
	clusterName string,
	brokerAddr string,
	brokerName string,
	brokerId int64,
	haServerAddr string,
	topicConfigWrapper *protocol.TopicConfigSerializeWrapper,
	filterServerList []string,
	channel net.Conn,
) *protocol.RegisterBrokerResult {
	
	// ACL权限验证
	if ns.config.AclEnable && ns.aclMiddleware != nil {
		remoteAddr := ""
		if channel != nil {
			remoteAddr = channel.RemoteAddr().String()
		}
		
		requestData := map[string]string{
			"clusterName": clusterName,
			"brokerName":  brokerName,
			"brokerAddr":  brokerAddr,
			"operation":   "REGISTER_BROKER",
		}
		
		if err := ns.ValidateTopicAccess(requestData, "BROKER_REGISTER", "ADMIN", remoteAddr); err != nil {
			log.Printf("ACL validation failed for broker registration from %s: %v", remoteAddr, err)
			return &protocol.RegisterBrokerResult{
				HaServerAddr: "",
				MasterAddr:   "",
			}
		}
	}
	
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	result := &protocol.RegisterBrokerResult{
		HaServerAddr: haServerAddr,
		MasterAddr:   brokerAddr,
	}

	// 更新集群信息
	ns.routeTable.mutex.Lock()
	brokerNames := ns.routeTable.clusterAddrTable[clusterName]
	if brokerNames == nil {
		brokerNames = make([]string, 0)
	}
	
	// 检查brokerName是否已存在
	found := false
	for _, name := range brokerNames {
		if name == brokerName {
			found = true
			break
		}
	}
	if !found {
		brokerNames = append(brokerNames, brokerName)
		ns.routeTable.clusterAddrTable[clusterName] = brokerNames
	}

	// 更新Broker地址表
	brokerAddrs := ns.routeTable.brokerAddrTable[brokerName]
	if brokerAddrs == nil {
		brokerAddrs = make(map[int64]string)
		ns.routeTable.brokerAddrTable[brokerName] = brokerAddrs
	}
	
	oldAddr := brokerAddrs[brokerId]
	if oldAddr != brokerAddr {
		brokerAddrs[brokerId] = brokerAddr
	}

	// 更新Topic配置
	if topicConfigWrapper != nil && topicConfigWrapper.TopicConfigTable != nil {
		for topic, topicConfig := range topicConfigWrapper.TopicConfigTable {
			ns.createAndUpdateQueueData(brokerName, topicConfig)
			log.Printf("Registered topic: %s for broker: %s", topic, brokerName)
		}
	}

	// 更新Broker存活信息
	brokerLiveInfo := &BrokerLiveInfo{
		LastUpdateTimestamp: time.Now(),
		DataVersion:         topicConfigWrapper.DataVersion,
		Channel:             channel,
		HaServerAddr:        haServerAddr,
	}
	ns.routeTable.brokerLiveTable[brokerAddr] = brokerLiveInfo

	// 更新过滤服务器列表
	if filterServerList != nil {
		ns.routeTable.filterServerTable[brokerAddr] = filterServerList
	}

	ns.routeTable.mutex.Unlock()

	log.Printf("Broker registered: cluster=%s, brokerName=%s, brokerId=%d, addr=%s", 
		clusterName, brokerName, brokerId, brokerAddr)

	return result
}

// createAndUpdateQueueData 创建和更新队列数据
func (ns *NameServer) createAndUpdateQueueData(brokerName string, topicConfig *protocol.TopicConfig) {
	queues := make([]*common.MessageQueue, 0, topicConfig.WriteQueueNums)
	
	for i := int32(0); i < topicConfig.WriteQueueNums; i++ {
		queue := &common.MessageQueue{
			Topic:      topicConfig.TopicName,
			BrokerName: brokerName,
			QueueId:    i,
		}
		queues = append(queues, queue)
	}
	
	ns.routeTable.topicQueueTable[topicConfig.TopicName] = queues
}

// GetRouteInfoByTopic 根据Topic获取路由信息
func (ns *NameServer) GetRouteInfoByTopic(topic string) *protocol.TopicRouteData {
	ns.routeTable.mutex.RLock()
	defer ns.routeTable.mutex.RUnlock()

	routeData := &protocol.TopicRouteData{
		OrderTopicConf: "",
		QueueDatas:     make([]*protocol.QueueData, 0),
		BrokerDatas:    make([]*protocol.BrokerData, 0),
		FilterServerTable: make(map[string][]string),
	}

	// 获取队列数据
	queues := ns.routeTable.topicQueueTable[topic]
	if queues == nil {
		return nil
	}

	// 按BrokerName分组队列
	brokerQueues := make(map[string][]*common.MessageQueue)
	for _, queue := range queues {
		brokerQueues[queue.BrokerName] = append(brokerQueues[queue.BrokerName], queue)
	}

	// 构建QueueData
	for brokerName, qs := range brokerQueues {
		queueData := &protocol.QueueData{
			BrokerName:     brokerName,
			ReadQueueNums:  int32(len(qs)),
			WriteQueueNums: int32(len(qs)),
			Perm:           6, // 读写权限
			TopicSysFlag:   0,
		}
		routeData.QueueDatas = append(routeData.QueueDatas, queueData)

		// 构建BrokerData
		brokerAddrs := ns.routeTable.brokerAddrTable[brokerName]
		if brokerAddrs != nil {
			// 从集群表中查找对应的集群名称
			clusterName := ""
			for cluster, brokers := range ns.routeTable.clusterAddrTable {
				for _, broker := range brokers {
					if broker == brokerName {
						clusterName = cluster
						break
					}
				}
				if clusterName != "" {
					break
				}
			}
			
			brokerData := &protocol.BrokerData{
				Cluster:     clusterName,
				BrokerName:  brokerName,
				BrokerAddrs: brokerAddrs,
			}
			routeData.BrokerDatas = append(routeData.BrokerDatas, brokerData)
		}
	}

	return routeData
}

// scanNotActiveBroker 扫描不活跃的Broker
func (ns *NameServer) scanNotActiveBroker() {
	ticker := time.NewTicker(ns.config.ScanNotActiveBrokerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ns.shutdown:
			return
		case <-ticker.C:
			ns.scanNotActiveBrokerInternal()
		}
	}
}

// scanNotActiveBrokerInternal 扫描不活跃Broker的内部实现
func (ns *NameServer) scanNotActiveBrokerInternal() {
	ns.mutex.Lock()
	ns.routeTable.mutex.Lock()
	defer ns.routeTable.mutex.Unlock()
	defer ns.mutex.Unlock()

	now := time.Now()
	toRemove := make([]string, 0)

	for brokerAddr, liveInfo := range ns.routeTable.brokerLiveTable {
		if now.Sub(liveInfo.LastUpdateTimestamp) > 2*time.Minute {
			toRemove = append(toRemove, brokerAddr)
			if liveInfo.Channel != nil {
				liveInfo.Channel.Close()
			}
		}
	}

	for _, brokerAddr := range toRemove {
		delete(ns.routeTable.brokerLiveTable, brokerAddr)
		delete(ns.routeTable.filterServerTable, brokerAddr)
		delete(ns.brokerLiveTable, brokerAddr)
		delete(ns.filterServerTable, brokerAddr)
		log.Printf("Removed inactive broker: %s", brokerAddr)
	}
}

// GetAllClusterInfo 获取所有集群信息
func (ns *NameServer) GetAllClusterInfo() *protocol.ClusterInfo {
	ns.routeTable.mutex.RLock()
	defer ns.routeTable.mutex.RUnlock()

	clusterInfo := &protocol.ClusterInfo{
		BrokerAddrTable:  make(map[string]map[int64]string),
		ClusterAddrTable: make(map[string][]string),
	}

	// 复制数据
	for k, v := range ns.routeTable.brokerAddrTable {
		clusterInfo.BrokerAddrTable[k] = make(map[int64]string)
		for brokerId, addr := range v {
			clusterInfo.BrokerAddrTable[k][brokerId] = addr
		}
	}

	for k, v := range ns.routeTable.clusterAddrTable {
		clusterInfo.ClusterAddrTable[k] = make([]string, len(v))
		copy(clusterInfo.ClusterAddrTable[k], v)
	}

	return clusterInfo
}

// validateConnection 验证连接的ACL权限
func (ns *NameServer) validateConnection(conn net.Conn, remoteAddr string) error {
	if ns.aclMiddleware == nil {
		return fmt.Errorf("ACL middleware not initialized")
	}
	
	// 简化的请求数据，实际应该从协议中解析
	requestData := map[string]string{
		"remoteAddress": remoteAddr,
		"operation":     "CONNECT",
	}
	
	// 进行管理员请求验证（连接验证）
	_, err := ns.aclMiddleware.ValidateAdminRequest(requestData, remoteAddr)
	return err
}

// ValidateTopicAccess 验证Topic访问权限
func (ns *NameServer) ValidateTopicAccess(requestData map[string]string, topicName, operation, remoteAddr string) error {
	if !ns.config.AclEnable || ns.aclMiddleware == nil {
		return nil
	}
	
	switch operation {
	case "PUB":
		_, err := ns.aclMiddleware.ValidateProducerRequest(requestData, topicName, remoteAddr)
		return err
	case "SUB":
		// 对于订阅，需要groupName，这里简化处理
		groupName := requestData["groupName"]
		if groupName == "" {
			groupName = "DEFAULT_GROUP"
		}
		_, err := ns.aclMiddleware.ValidateConsumerRequest(requestData, topicName, groupName, remoteAddr)
		return err
	default:
		_, err := ns.aclMiddleware.ValidateAdminRequest(requestData, remoteAddr)
		return err
	}
}

// IsAclEnabled 检查ACL是否启用
func (ns *NameServer) IsAclEnabled() bool {
	return ns.config.AclEnable && ns.aclMiddleware != nil
}

// ReloadAclConfig 重新加载ACL配置
func (ns *NameServer) ReloadAclConfig() error {
	if !ns.config.AclEnable || ns.aclMiddleware == nil {
		return fmt.Errorf("ACL is not enabled")
	}
	return ns.aclMiddleware.ReloadConfig()
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		ListenPort:                  9876,
		ClusterTestEnable:           false,
		OrderMessageEnable:          false,
		ScanNotActiveBrokerInterval: 5 * time.Second,
		AclEnable:                   false,
		AclConfigFile:               "config/plain_acl.yml",
	}
}