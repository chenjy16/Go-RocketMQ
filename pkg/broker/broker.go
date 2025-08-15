package broker

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"go-rocketmq/pkg/common"
	"go-rocketmq/pkg/protocol"
	"go-rocketmq/pkg/store"
	"go-rocketmq/pkg/ha"
	"go-rocketmq/pkg/cluster"
	"go-rocketmq/pkg/failover"
	"go-rocketmq/pkg/acl"
)

// Broker 代表一个消息代理
type Broker struct {
	config       *Config
	listener     net.Listener
	shutdown     chan struct{}
	wg           sync.WaitGroup
	mutex        sync.RWMutex
	
	// 消息存储
	messageStore *store.DefaultMessageStore
	
	// Topic配置
	topicConfigTable map[string]*protocol.TopicConfig
	
	// 消费者组信息
	consumerTable map[string]*ConsumerGroupInfo
	
	// 生产者信息
	producerTable map[string]*ProducerGroupInfo
	
	// 高可用性和集群组件
	haService        *ha.HAService
	clusterManager   *cluster.ClusterManager
	failoverService  *failover.FailoverService
	
	// ACL权限控制
	aclMiddleware    *acl.AclMiddleware
}

// Config Broker配置
type Config struct {
	BrokerName       string
	BrokerId         int64
	ClusterName      string
	ListenPort       int
	NameServerAddr   string
	StorePathRootDir string
	
	// 性能配置
	SendMessageThreadPoolNums int
	PullMessageThreadPoolNums int
	FlushDiskType            int // 0: ASYNC_FLUSH, 1: SYNC_FLUSH
	
	// 高可用配置
	BrokerRole       int // 0: ASYNC_MASTER, 1: SYNC_MASTER, 2: SLAVE
	HaListenPort     int
	HaMasterAddress  string
	ReplicationMode  int // 0: ASYNC_REPLICATION, 1: SYNC_REPLICATION
	
	// 集群配置
	EnableCluster    bool
	ClusterManagerPort int
	
	// 故障转移配置
	EnableFailover   bool
	AutoFailover     bool
	FailoverDelay    int // 故障转移延迟(秒)
	BackupBrokers    []string
	
	// ACL权限控制配置
	AclEnable        bool   // 是否启用ACL
	AclConfigFile    string // ACL配置文件路径
}

// ConsumerGroupInfo 消费者组信息
type ConsumerGroupInfo struct {
	GroupName        string
	ConsumeType      int // 0: CONSUME_ACTIVELY, 1: CONSUME_PASSIVELY
	MessageModel     int // 0: BROADCASTING, 1: CLUSTERING
	ConsumeFromWhere int
	Subscriptions    map[string]*protocol.SubscriptionData
	Channels         map[string]net.Conn
}

// ProducerGroupInfo 生产者组信息
type ProducerGroupInfo struct {
	GroupName string
	Channels  map[string]net.Conn
}

// NewBroker 创建新的Broker实例
func NewBroker(config *Config) *Broker {
	if config == nil {
		config = DefaultBrokerConfig()
	}
	
	// 创建存储配置
	storeConfig := &store.StoreConfig{
		StorePathRootDir:         config.StorePathRootDir,
		StorePathCommitLog:       config.StorePathRootDir + "/commitlog",
		StorePathConsumeQueue:    config.StorePathRootDir + "/consumequeue",
		StorePathIndex:           config.StorePathRootDir + "/index",
		MapedFileSizeCommitLog:   1024 * 1024 * 1024, // 1GB
		MapedFileSizeConsumeQueue: 1024 * 1024 * 6,   // 6MB
		FlushIntervalCommitLog:   500,  // 500ms
		FlushIntervalConsumeQueue: 1000, // 1s
		FlushDiskType:            store.FlushDiskType(config.FlushDiskType),
		FileReservedTime:         72, // 72小时
	}
	
	// 创建消息存储
	messageStore, err := store.NewDefaultMessageStore(storeConfig)
	if err != nil {
		panic(fmt.Sprintf("failed to create message store: %v", err))
	}
	
	broker := &Broker{
		config:           config,
		shutdown:         make(chan struct{}),
		messageStore:     messageStore,
		topicConfigTable: make(map[string]*protocol.TopicConfig),
		consumerTable:    make(map[string]*ConsumerGroupInfo),
		producerTable:    make(map[string]*ProducerGroupInfo),
	}
	
	// 初始化高可用性服务
	if config.BrokerRole != 2 || config.HaMasterAddress != "" { // 不是普通Slave或配置了Master地址
		haConfig := &ha.HAConfig{
			BrokerRole:          ha.BrokerRole(config.BrokerRole),
			ReplicationMode:     ha.ReplicationMode(config.ReplicationMode),
			HaListenPort:        config.HaListenPort,
			HaMasterAddress:     config.HaMasterAddress,
			HaHeartbeatInterval: 5000,  // 5秒
			HaConnectionTimeout: 3000,  // 3秒
			MaxTransferSize:     65536, // 64KB
			SyncFlushTimeout:    5000,  // 5秒
		}
		broker.haService = ha.NewHAService(haConfig, messageStore.GetCommitLog())
	}
	
	// 初始化集群管理器
	if config.EnableCluster {
		broker.clusterManager = cluster.NewClusterManager(config.ClusterName)
	}
	
	// 初始化故障转移服务
	if config.EnableFailover && broker.clusterManager != nil {
		broker.failoverService = failover.NewFailoverService(broker.clusterManager)
	}
	
	// 初始化ACL中间件
	if config.AclEnable {
		aclValidator := acl.NewPlainAclValidator(config.AclConfigFile)
		err := aclValidator.LoadConfig(config.AclConfigFile)
		if err != nil {
			log.Printf("Failed to load ACL config: %v", err)
		} else {
			broker.aclMiddleware = acl.NewAclMiddleware(aclValidator, true)
			log.Printf("ACL middleware initialized with config file: %s", config.AclConfigFile)
		}
	}
	
	return broker
}

// Start 启动Broker
func (b *Broker) Start() error {
	// 启动消息存储
	if err := b.messageStore.Start(); err != nil {
		return fmt.Errorf("failed to start message store: %v", err)
	}
	
	// 启动高可用性服务
	if b.haService != nil {
		if err := b.haService.Start(); err != nil {
			return fmt.Errorf("failed to start HA service: %v", err)
		}
		log.Printf("HA service started with role: %v", b.config.BrokerRole)
	}
	
	// 启动集群管理器
	if b.clusterManager != nil {
		if err := b.clusterManager.Start(); err != nil {
			return fmt.Errorf("failed to start cluster manager: %v", err)
		}
		
		// 注册当前Broker到集群
		brokerInfo := &cluster.BrokerInfo{
			BrokerName:     b.config.BrokerName,
			BrokerId:       b.config.BrokerId,
			ClusterName:    b.config.ClusterName,
			BrokerAddr:     fmt.Sprintf("localhost:%d", b.config.ListenPort),
			Version:        "1.0.0",
			DataVersion:    1,
			LastUpdateTime: time.Now().UnixMilli(),
			Role:           b.getBrokerRoleString(),
			Status:         cluster.ONLINE,
			Topics:         make(map[string]*cluster.TopicRouteInfo),
		}
		
		if err := b.clusterManager.RegisterBroker(brokerInfo); err != nil {
			return fmt.Errorf("failed to register broker to cluster: %v", err)
		}
		log.Printf("Broker registered to cluster: %s", b.config.ClusterName)
	}
	
	// 启动故障转移服务
	if b.failoverService != nil {
		if err := b.failoverService.Start(); err != nil {
			return fmt.Errorf("failed to start failover service: %v", err)
		}
		
		// 注册故障转移策略
		if b.config.AutoFailover {
			policy := &failover.FailoverPolicy{
				BrokerName:      b.config.BrokerName,
				FailoverType:    failover.AUTO_FAILOVER,
				BackupBrokers:   b.config.BackupBrokers,
				AutoFailover:    true,
				FailoverDelay:   time.Duration(b.config.FailoverDelay) * time.Second,
				HealthThreshold: 3,
				RecoveryPolicy:  failover.AUTO_RECOVERY,
				Notifications:   []failover.NotificationConfig{},
			}
			
			if err := b.failoverService.RegisterFailoverPolicy(policy); err != nil {
				return fmt.Errorf("failed to register failover policy: %v", err)
			}
			log.Printf("Auto failover enabled for broker: %s", b.config.BrokerName)
		}
	}
	
	addr := fmt.Sprintf(":%d", b.config.ListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}
	
	b.listener = listener
	log.Printf("Broker started on %s", addr)
	
	// 启动接受连接的goroutine
	b.wg.Add(1)
	go b.acceptConnections()
	
	// 注册到NameServer
	go b.registerToNameServer()
	
	// 启动心跳发送
	go b.sendHeartbeatToNameServer()
	
	log.Printf("Broker started successfully: %s", b.config.BrokerName)
	return nil
}

// Stop 停止Broker
func (b *Broker) Stop() error {
	close(b.shutdown)
	
	if b.listener != nil {
		b.listener.Close()
	}
	
	// 停止故障转移服务
	if b.failoverService != nil {
		b.failoverService.Stop()
		log.Printf("Failover service shutdown completed")
	}
	
	// 停止集群管理器
	if b.clusterManager != nil {
		// 注销当前Broker
		b.clusterManager.UnregisterBroker(b.config.BrokerName)
		b.clusterManager.Stop()
		log.Printf("Cluster manager shutdown completed")
	}
	
	// 停止高可用性服务
	if b.haService != nil {
		b.haService.Shutdown()
		log.Printf("HA service shutdown completed")
	}
	
	// 停止消息存储
	b.messageStore.Shutdown()
	
	b.wg.Wait()
	log.Printf("Broker stopped: %s", b.config.BrokerName)
	return nil
}

// acceptConnections 接受连接
func (b *Broker) acceptConnections() {
	defer b.wg.Done()
	
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.shutdown:
				return
			default:
				log.Printf("Failed to accept connection: %v", err)
				continue
			}
		}
		go b.handleConnection(conn)
	}
}

// handleConnection 处理连接
func (b *Broker) handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("New connection from %s", conn.RemoteAddr())
	
	// 读取请求头（4字节长度）
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		log.Printf("Failed to read request header: %v", err)
		return
	}
	
	// 解析请求长度
	requestLength := binary.BigEndian.Uint32(header)
	if requestLength > 1024*1024 { // 限制最大1MB
		log.Printf("Request too large: %d bytes", requestLength)
		return
	}
	
	// 读取请求数据
	requestData := make([]byte, requestLength)
	if _, err := io.ReadFull(conn, requestData); err != nil {
		log.Printf("Failed to read request data: %v", err)
		return
	}
	
	// 解析发送消息请求
	var requestWrapper map[string]interface{}
	if err := json.Unmarshal(requestData, &requestWrapper); err != nil {
		log.Printf("Failed to unmarshal request: %v", err)
		b.sendErrorResponse(conn, "Invalid request format")
		return
	}
	
	// 提取header和body
	headerData, _ := json.Marshal(requestWrapper["header"])
	var request protocol.SendMessageRequestHeader
	if err := json.Unmarshal(headerData, &request); err != nil {
		log.Printf("Failed to unmarshal request header: %v", err)
		b.sendErrorResponse(conn, "Invalid request header format")
		return
	}
	
	// 提取消息体
	var messageBody string
	if body, ok := requestWrapper["body"].(string); ok {
		messageBody = body
	}
	
	// 处理发送消息请求
	b.handleSendMessage(conn, &request, messageBody)
}

// registerToNameServer 注册到NameServer
func (b *Broker) registerToNameServer() {
	log.Printf("Registering broker %s to NameServer %s", b.config.BrokerName, b.config.NameServerAddr)
	
	// 构建Topic配置表
	topicConfigTable := make(map[string]*protocol.TopicConfig)
	b.mutex.RLock()
	for topicName, topicConfig := range b.topicConfigTable {
		topicConfigTable[topicName] = topicConfig
	}
	b.mutex.RUnlock()
	
	// 创建Topic配置包装器
	topicConfigWrapper := &protocol.TopicConfigSerializeWrapper{
		TopicConfigTable: topicConfigTable,
		DataVersion:      protocol.NewDataVersion(),
	}
	
	// 构建Broker地址
	brokerAddr := fmt.Sprintf("localhost:%d", b.config.ListenPort)
	haServerAddr := ""
	if b.config.HaListenPort > 0 {
		haServerAddr = fmt.Sprintf("localhost:%d", b.config.HaListenPort)
	}
	
	// 发送注册请求到NameServer
	if err := b.sendRegisterBrokerRequest(
		b.config.ClusterName,
		brokerAddr,
		b.config.BrokerName,
		b.config.BrokerId,
		haServerAddr,
		topicConfigWrapper,
		[]string{}, // filterServerList
	); err != nil {
		log.Printf("Failed to register broker to NameServer: %v", err)
		return
	}
	
	log.Printf("Successfully registered broker %s to NameServer", b.config.BrokerName)
}

// sendRegisterBrokerRequest 发送注册请求到NameServer
func (b *Broker) sendRegisterBrokerRequest(
	clusterName string,
	brokerAddr string,
	brokerName string,
	brokerId int64,
	haServerAddr string,
	topicConfigWrapper *protocol.TopicConfigSerializeWrapper,
	filterServerList []string,
) error {
	// 构建注册请求数据
	requestData := map[string]interface{}{
		"clusterName":         clusterName,
		"brokerAddr":          brokerAddr,
		"brokerName":          brokerName,
		"brokerId":            brokerId,
		"haServerAddr":        haServerAddr,
		"topicConfigWrapper":  topicConfigWrapper,
		"filterServerList":    filterServerList,
	}
	
	// 序列化请求数据
	requestBody, err := json.Marshal(requestData)
	if err != nil {
		return fmt.Errorf("failed to marshal request data: %v", err)
	}
	
	// 构建NameServer URL
	nameServerURL := fmt.Sprintf("http://%s/broker/register", b.config.NameServerAddr)
	
	// 发送HTTP POST请求
	resp, err := http.Post(nameServerURL, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("failed to send register request: %v", err)
	}
	defer resp.Body.Close()
	
	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register request failed with status: %d", resp.StatusCode)
	}
	
	// 读取响应体
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}
	
	// 解析响应
	var result protocol.RegisterBrokerResult
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return fmt.Errorf("failed to unmarshal response: %v", err)
	}
	
	log.Printf("Broker registration response: HaServerAddr=%s, MasterAddr=%s", 
		result.HaServerAddr, result.MasterAddr)
	
	return nil
}

// sendHeartbeatToNameServer 向NameServer发送心跳
func (b *Broker) sendHeartbeatToNameServer() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-b.shutdown:
			return
		case <-ticker.C:
			log.Printf("Sending heartbeat to NameServer")
			
			// 发送心跳请求
			if err := b.sendHeartbeatRequest(); err != nil {
				log.Printf("Failed to send heartbeat to NameServer: %v", err)
			} else {
				log.Printf("Successfully sent heartbeat to NameServer")
			}
		}
	}
}

// sendHeartbeatRequest 发送心跳请求到NameServer
func (b *Broker) sendHeartbeatRequest() error {
	// 构建心跳数据
	heartbeatData := map[string]interface{}{
		"brokerName":   b.config.BrokerName,
		"brokerId":     b.config.BrokerId,
		"brokerAddr":   fmt.Sprintf("localhost:%d", b.config.ListenPort),
		"clusterName":  b.config.ClusterName,
		"timestamp":    time.Now().UnixMilli(),
		"dataVersion":  protocol.NewDataVersion(),
	}
	
	// 序列化心跳数据
	heartbeatBody, err := json.Marshal(heartbeatData)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat data: %v", err)
	}
	
	// 构建NameServer URL
	nameServerURL := fmt.Sprintf("http://%s/broker/heartbeat", b.config.NameServerAddr)
	
	// 发送HTTP POST请求
	resp, err := http.Post(nameServerURL, "application/json", bytes.NewBuffer(heartbeatBody))
	if err != nil {
		return fmt.Errorf("failed to send heartbeat request: %v", err)
	}
	defer resp.Body.Close()
	
	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heartbeat request failed with status: %d", resp.StatusCode)
	}
	
	return nil
}

// getBrokerRoleString 获取Broker角色字符串
func (b *Broker) getBrokerRoleString() string {
	switch b.config.BrokerRole {
	case 0:
		return "ASYNC_MASTER"
	case 1:
		return "SYNC_MASTER"
	case 2:
		return "SLAVE"
	default:
		return "UNKNOWN"
	}
}

// PutMessage 存储消息
func (b *Broker) PutMessage(msg *common.Message) (*common.SendResult, error) {
	// 检查消息类型并路由到相应的处理方法
	
	// 检查是否为延迟消息
	if delayLevelStr := msg.GetProperty(store.PROPERTY_DELAY_TIME_LEVEL); delayLevelStr != "" {
		var delayLevel int32
		if _, err := fmt.Sscanf(delayLevelStr, "%d", &delayLevel); err == nil {
			return b.messageStore.PutDelayMessage(msg, delayLevel)
		}
	}
	
	// 检查是否为事务消息
	if store.IsTransactionMessage(msg) {
		producerGroup := msg.GetProperty(store.PROPERTY_PRODUCER_GROUP)
		transactionId := store.GetTransactionId(msg)
		if producerGroup != "" && transactionId != "" {
			return b.messageStore.PrepareMessage(msg, producerGroup, transactionId)
		}
	}
	
	// 检查是否为顺序消息
	if store.IsOrderedMessage(msg) {
		shardingKey := store.GetShardingKey(msg)
		if shardingKey != "" {
			return b.messageStore.PutOrderedMessage(msg, shardingKey)
		}
	}
	
	// 普通消息处理
	result, err := b.messageStore.PutMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to put message: %v", err)
	}
	
	// 使用默认队列ID 0
	queueId := int32(0)
	
	// 创建发送结果
	sendResult := &common.SendResult{
		SendStatus:  common.SendOK,
		MsgId:       fmt.Sprintf("%s_%d_%d", msg.Topic, queueId, result.QueueOffset),
		MessageQueue: &common.MessageQueue{
			Topic:      msg.Topic,
			BrokerName: b.config.BrokerName,
			QueueId:    queueId,
		},
		QueueOffset: result.QueueOffset,
	}
	
	log.Printf("Message stored: topic=%s, queueId=%d, offset=%d", 
		msg.Topic, queueId, result.QueueOffset)
	
	return sendResult, nil
}

// PullMessage 拉取消息
func (b *Broker) PullMessage(topic string, queueId int32, offset int64, maxNums int32) ([]*common.MessageExt, error) {
	return b.messageStore.GetMessage(topic, queueId, offset, maxNums)
}

// CreateTopic 创建Topic
func (b *Broker) CreateTopic(topic string, queueNums int32) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	
	topicConfig := &protocol.TopicConfig{
		TopicName:      topic,
		ReadQueueNums:  queueNums,
		WriteQueueNums: queueNums,
		Perm:           6, // 读写权限
		TopicSysFlag:   0,
		Order:          false,
	}
	
	b.topicConfigTable[topic] = topicConfig
	
	log.Printf("Topic created: %s with %d queues", topic, queueNums)
	return nil
}

// GetTopicConfig 获取Topic配置
func (b *Broker) GetTopicConfig(topic string) *protocol.TopicConfig {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	
	return b.topicConfigTable[topic]
}

// handleSendMessage 处理发送消息请求
func (b *Broker) handleSendMessage(conn net.Conn, request *protocol.SendMessageRequestHeader, messageBody string) {
	// ACL权限验证
	if b.aclMiddleware != nil && b.aclMiddleware.IsAclEnabled() {
		// 构造请求数据用于验证
		requestData := map[string]string{
			"topic":      request.Topic,
			"operation":  "PUB",
			"accessKey":  request.AccessKey,
			"signature":  request.Signature,
			"timestamp":  fmt.Sprintf("%d", request.Timestamp),
		}
		
		// 获取远程地址
		remoteAddr := conn.RemoteAddr().String()
		
		// 验证生产者权限
		_, err := b.aclMiddleware.ValidateProducerRequest(requestData, request.Topic, remoteAddr)
		if err != nil {
			log.Printf("ACL validation failed for producer: %v", err)
			b.sendErrorResponse(conn, fmt.Sprintf("Access denied: %v", err))
			return
		}
	}
	
	// 创建消息对象
	msg := &common.Message{
		Topic:      request.Topic,
		Properties: make(map[string]string),
		Body:       []byte(messageBody),
	}
	
	// 解析Properties
	if request.Properties != "" {
		var props map[string]string
		if err := json.Unmarshal([]byte(request.Properties), &props); err == nil {
			msg.Properties = props
		}
	}
	
	// 存储消息
	result, err := b.PutMessage(msg)
	if err != nil {
		log.Printf("Failed to store message: %v", err)
		b.sendErrorResponse(conn, fmt.Sprintf("Failed to store message: %v", err))
		return
	}
	
	// 构造响应
	response := &protocol.SendMessageResponseHeader{
		MsgId:       result.MsgId,
		QueueId:     result.MessageQueue.QueueId,
		QueueOffset: result.QueueOffset,
	}
	
	// 发送响应
	b.sendSuccessResponse(conn, response)
}

// sendErrorResponse 发送错误响应
func (b *Broker) sendErrorResponse(conn net.Conn, errorMsg string) {
	response := map[string]interface{}{
		"code":   1,
		"remark": errorMsg,
	}
	
	responseData, _ := json.Marshal(response)
	
	// 发送响应头
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(responseData)))
	conn.Write(header)
	conn.Write(responseData)
}

// sendSuccessResponse 发送成功响应
func (b *Broker) sendSuccessResponse(conn net.Conn, response *protocol.SendMessageResponseHeader) {
	responseData, err := json.Marshal(response)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		b.sendErrorResponse(conn, "Internal server error")
		return
	}
	
	// 发送响应头
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(responseData)))
	conn.Write(header)
	conn.Write(responseData)
	
	log.Printf("Sent success response: %s", string(responseData))
}

// DefaultBrokerConfig 返回默认Broker配置
// ========== 延迟消息相关方法 ==========

// PutDelayMessage 发送延迟消息
func (b *Broker) PutDelayMessage(msg *common.Message, delayLevel int32) (*common.SendResult, error) {
	return b.messageStore.PutDelayMessage(msg, delayLevel)
}

// ========== 事务消息相关方法 ==========

// RegisterTransactionListener 注册事务监听器
func (b *Broker) RegisterTransactionListener(producerGroup string, listener store.TransactionListener) {
	b.messageStore.RegisterTransactionListener(producerGroup, listener)
}

// PrepareMessage 准备事务消息
func (b *Broker) PrepareMessage(msg *common.Message, producerGroup string, transactionId string) (*common.SendResult, error) {
	return b.messageStore.PrepareMessage(msg, producerGroup, transactionId)
}

// CommitTransaction 提交事务
func (b *Broker) CommitTransaction(transactionId string) error {
	return b.messageStore.CommitTransaction(transactionId)
}

// RollbackTransaction 回滚事务
func (b *Broker) RollbackTransaction(transactionId string) error {
	return b.messageStore.RollbackTransaction(transactionId)
}

// ========== 顺序消息相关方法 ==========

// PutOrderedMessage 发送顺序消息
func (b *Broker) PutOrderedMessage(msg *common.Message, shardingKey string) (*common.SendResult, error) {
	return b.messageStore.PutOrderedMessage(msg, shardingKey)
}

// PullOrderedMessage 拉取顺序消息
func (b *Broker) PullOrderedMessage(topic string, queueId int32, consumerGroup string, maxNums int32) ([]*common.MessageExt, error) {
	return b.messageStore.PullOrderedMessage(topic, queueId, consumerGroup, maxNums)
}

// CommitConsumeOffset 提交消费进度
func (b *Broker) CommitConsumeOffset(topic string, queueId int32, consumerGroup string, offset int64) error {
	return b.messageStore.CommitConsumeOffset(topic, queueId, consumerGroup, offset)
}

// GetConsumeOffset 获取消费进度
func (b *Broker) GetConsumeOffset(topic string, queueId int32, consumerGroup string) int64 {
	return b.messageStore.GetConsumeOffset(topic, queueId, consumerGroup)
}

// IsAclEnabled 检查ACL是否启用
func (b *Broker) IsAclEnabled() bool {
	return b.config.AclEnable && b.aclMiddleware != nil
}

// SetAclEnabled 设置ACL启用状态
func (b *Broker) SetAclEnabled(enabled bool) {
	b.config.AclEnable = enabled
}

// ReloadAclConfig 重新加载ACL配置
func (b *Broker) ReloadAclConfig() error {
	if !b.config.AclEnable || b.aclMiddleware == nil {
		return fmt.Errorf("ACL is not enabled")
	}
	
	aclValidator := acl.NewPlainAclValidator(b.config.AclConfigFile)
	err := aclValidator.LoadConfig(b.config.AclConfigFile)
	if err != nil {
		return fmt.Errorf("failed to reload ACL config: %v", err)
	}
	
	b.aclMiddleware = acl.NewAclMiddleware(aclValidator, true)
	log.Printf("ACL config reloaded from: %s", b.config.AclConfigFile)
	return nil
}

// ValidateTopicAccess 验证Topic访问权限
func (b *Broker) ValidateTopicAccess(requestData map[string]string, topic, operation, remoteAddr string) error {
	if !b.IsAclEnabled() {
		return nil // ACL未启用，允许访问
	}
	
	// 先进行认证
	account, err := b.aclMiddleware.AuthenticateRequest(requestData, remoteAddr)
	if err != nil {
		return fmt.Errorf("authentication failed: %v", err)
	}
	
	// 检查Topic权限
	return b.aclMiddleware.CheckTopicPermission(account, topic, operation, remoteAddr)
}

func DefaultBrokerConfig() *Config {
	return &Config{
		BrokerName:       "DefaultBroker",
		BrokerId:         0,
		ClusterName:      "DefaultCluster",
		ListenPort:       10911,
		NameServerAddr:   "127.0.0.1:9876",
		StorePathRootDir: "/tmp/rocketmq-store",
		SendMessageThreadPoolNums: 16,
		PullMessageThreadPoolNums: 16,
		FlushDiskType:    0, // ASYNC_FLUSH
		BrokerRole:       0, // ASYNC_MASTER
		HaListenPort:     10912,
		ReplicationMode:  0, // ASYNC_REPLICATION
		EnableCluster:    true,
		ClusterManagerPort: 10913,
		EnableFailover:   true,
		AutoFailover:     false,
		FailoverDelay:    30,
		BackupBrokers:    []string{},
		AclEnable:        false,
		AclConfigFile:    "config/plain_acl.yml",
	}
}