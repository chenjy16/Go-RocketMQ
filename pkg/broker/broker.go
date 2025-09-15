package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	common "github.com/chenjy16/go-rocketmq-common"
	remoting "github.com/chenjy16/go-rocketmq-remoting"
	"github.com/chenjy16/go-rocketmq-remoting/command"
	"github.com/chenjy16/go-rocketmq-remoting/heartbeat"

	"go-rocketmq/pkg/acl"
	"go-rocketmq/pkg/cluster"
	"go-rocketmq/pkg/failover"
	"go-rocketmq/pkg/ha"
	"go-rocketmq/pkg/store"
)

// Broker 代表一个消息代理
type Broker struct {
	config         *Config
	listener       net.Listener
	remotingServer *remoting.RemotingServer
	shutdown       chan struct{}
	wg             sync.WaitGroup
	mutex          sync.RWMutex

	// 消息存储
	messageStore *store.DefaultMessageStore

	// Topic配置
	topicConfigTable map[string]*remoting.TopicConfig

	// 消费者组信息
	consumerTable map[string]*ConsumerGroupInfo

	// 生产者信息
	producerTable map[string]*ProducerGroupInfo

	// 高可用性和集群组件
	haService       *ha.HAService
	clusterManager  *cluster.ClusterManager
	failoverService *failover.FailoverService

	// ACL权限控制
	aclMiddleware *acl.AclMiddleware

	// 协议处理器
	protocolProcessor remoting.ProtocolProcessor
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
	FlushDiskType             int // 0: ASYNC_FLUSH, 1: SYNC_FLUSH

	// 高可用配置
	BrokerRole      int // 0: ASYNC_MASTER, 1: SYNC_MASTER, 2: SLAVE
	HaListenPort    int
	HaMasterAddress string
	ReplicationMode int // 0: ASYNC_REPLICATION, 1: SYNC_REPLICATION

	// 集群配置
	EnableCluster      bool
	ClusterManagerPort int

	// 故障转移配置
	EnableFailover bool
	AutoFailover   bool
	FailoverDelay  int // 故障转移延迟(秒)
	BackupBrokers  []string

	// ACL权限控制配置
	AclEnable     bool   // 是否启用ACL
	AclConfigFile string // ACL配置文件路径
}

// ConsumerGroupInfo 消费者组信息
type ConsumerGroupInfo struct {
	GroupName        string
	ConsumeType      int // 0: CONSUME_ACTIVELY, 1: CONSUME_PASSIVELY
	MessageModel     int // 0: BROADCASTING, 1: CLUSTERING
	ConsumeFromWhere int
	Subscriptions    map[string]*remoting.SubscriptionData
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
		StorePathRootDir:          config.StorePathRootDir,
		StorePathCommitLog:        config.StorePathRootDir + "/commitlog",
		StorePathConsumeQueue:     config.StorePathRootDir + "/consumequeue",
		StorePathIndex:            config.StorePathRootDir + "/index",
		MapedFileSizeCommitLog:    1024 * 1024 * 1024, // 1GB
		MapedFileSizeConsumeQueue: 1024 * 1024 * 6,    // 6MB
		FlushIntervalCommitLog:    500,                // 500ms
		FlushIntervalConsumeQueue: 1000,               // 1s
		FlushDiskType:             store.FlushDiskType(config.FlushDiskType),
		FileReservedTime:          72, // 72小时
	}

	// 创建消息存储
	messageStore, err := store.NewDefaultMessageStore(storeConfig)
	if err != nil {
		panic(fmt.Sprintf("failed to create message store: %v", err))
	}

	// 创建remoting服务器
	remotingServer := remoting.NewRemotingServer(config.ListenPort)

	// 创建协议处理器
	protocolProcessor := remoting.NewProtocolProcessor()

	broker := &Broker{
		config:            config,
		shutdown:          make(chan struct{}),
		remotingServer:    remotingServer,
		messageStore:      messageStore,
		topicConfigTable:  make(map[string]*remoting.TopicConfig),
		consumerTable:     make(map[string]*ConsumerGroupInfo),
		producerTable:     make(map[string]*ProducerGroupInfo),
		protocolProcessor: protocolProcessor,
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

	// 注册协议处理器
	broker.registerProtocolProcessors()

	return broker
}

// registerProtocolProcessors 注册协议处理器
func (b *Broker) registerProtocolProcessors() {
	// 注册发送消息处理器
	sendMessageProcessor := &SendMessageProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.SendMessage, sendMessageProcessor)

	// 注册发送消息V2处理器
	sendMessageV2Processor := &SendMessageV2Processor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.SendMessageV2, sendMessageV2Processor)

	// 注册发送批量消息处理器
	sendBatchMessageProcessor := &SendBatchMessageProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.SendBatchMessage, sendBatchMessageProcessor)

	// 注册拉取消息处理器
	pullMessageProcessor := &PullMessageProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.PullMessage, pullMessageProcessor)

	// 注册查询消息处理器
	queryMessageProcessor := &QueryMessageProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.QueryMessage, queryMessageProcessor)

	// 注册根据Key查询消息处理器
	queryMessageByKeyProcessor := &QueryMessageByKeyProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.QueryMessageByKey, queryMessageByKeyProcessor)

	// 注册查询路由信息处理器
	queryRouteProcessor := &QueryRouteProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.GetRouteInfoByTopic, queryRouteProcessor)

	// 注册创建/更新Topic处理器
	updateAndCreateTopicProcessor := &UpdateAndCreateTopicProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.UpdateAndCreateTopic, updateAndCreateTopicProcessor)

	// 注册获取Broker集群信息处理器
	getBrokerClusterInfoProcessor := &GetBrokerClusterInfoProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.GetBrokerClusterInfo, getBrokerClusterInfoProcessor)

	// 注册Broker注册处理器
	registerBrokerProcessor := &RegisterBrokerProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.RegisterBroker, registerBrokerProcessor)

	// 注册Broker注销处理器
	unregisterBrokerProcessor := &UnregisterBrokerProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.UnregisterBroker, unregisterBrokerProcessor)

	// 注册心跳处理器
	heartbeatProcessor := &HeartbeatProcessor{heartbeatProcessor: heartbeat.NewHeartbeatProcessor()}
	b.remotingServer.RegisterProcessor(remoting.HeartBeat, heartbeatProcessor)

	// 注册更新消费者偏移量处理器
	updateConsumerOffsetProcessor := &UpdateConsumerOffsetProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.UpdateConsumerOffset, updateConsumerOffsetProcessor)

	// 注册结束事务处理器
	endTransactionProcessor := &EndTransactionProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.EndTransaction, endTransactionProcessor)

	// 注册检查事务状态处理器
	checkTransactionStateProcessor := &CheckTransactionStateProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.CheckTransactionState, checkTransactionStateProcessor)

	// 注册更新Broker配置处理器
	updateBrokerConfigProcessor := &UpdateBrokerConfigProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.UpdateBrokerConfig, updateBrokerConfigProcessor)

	// 注册获取Broker配置处理器
	getBrokerConfigProcessor := &GetBrokerConfigProcessor{broker: b}
	b.remotingServer.RegisterProcessor(remoting.GetBrokerConfig, getBrokerConfigProcessor)

	log.Printf("Registered protocol processors for broker")
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

	// 启动remoting服务器
	if err := b.remotingServer.Start(); err != nil {
		return fmt.Errorf("failed to start remoting server: %v", err)
	}

	log.Printf("Broker started on port %d", b.config.ListenPort)

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

	// 停止remoting服务器
	if b.remotingServer != nil {
		b.remotingServer.Stop()
		log.Printf("Remoting server shutdown completed")
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

// registerToNameServer 注册到NameServer
func (b *Broker) registerToNameServer() {
	log.Printf("Registering broker %s to NameServer %s", b.config.BrokerName, b.config.NameServerAddr)

	// 构建Topic配置表
	topicConfigTable := make(map[string]*remoting.TopicConfig)
	b.mutex.RLock()
	for topicName, topicConfig := range b.topicConfigTable {
		topicConfigTable[topicName] = topicConfig
	}
	b.mutex.RUnlock()

	// 创建Topic配置包装器
	topicConfigWrapper := &remoting.TopicConfigSerializeWrapper{
		TopicConfigTable: topicConfigTable,
		DataVersion:      remoting.NewDataVersion(),
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
	topicConfigWrapper *remoting.TopicConfigSerializeWrapper,
	filterServerList []string,
) error {
	// 构建注册请求数据
	requestData := map[string]interface{}{
		"clusterName":        clusterName,
		"brokerAddr":         brokerAddr,
		"brokerName":         brokerName,
		"brokerId":           brokerId,
		"haServerAddr":       haServerAddr,
		"topicConfigWrapper": topicConfigWrapper,
		"filterServerList":   filterServerList,
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
	var result remoting.RegisterBrokerResult
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
		"brokerName":  b.config.BrokerName,
		"brokerId":    b.config.BrokerId,
		"brokerAddr":  fmt.Sprintf("localhost:%d", b.config.ListenPort),
		"clusterName": b.config.ClusterName,
		"timestamp":   time.Now().UnixMilli(),
		"dataVersion": remoting.NewDataVersion(),
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
		SendStatus: common.SendOK,
		MsgId:      fmt.Sprintf("%s_%d_%d", msg.Topic, queueId, result.QueueOffset),
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

	topicConfig := &remoting.TopicConfig{
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
func (b *Broker) GetTopicConfig(topic string) *remoting.TopicConfig {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.topicConfigTable[topic]
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

// CommitConsumeOffset 提交消费进度
func (b *Broker) CommitConsumeOffset(topic string, queueId int32, consumerGroup string, offset int64) error {
	return b.messageStore.CommitConsumeOffset(topic, queueId, consumerGroup, offset)
}

// GetConsumeOffset 获取消费进度
func (b *Broker) GetConsumeOffset(topic string, queueId int32, consumerGroup string) int64 {
	return b.messageStore.GetConsumeOffset(topic, queueId, consumerGroup)
}

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

// PutDelayMessage 存储延迟消息
func (b *Broker) PutDelayMessage(msg *common.Message, delayLevel int32) (*common.SendResult, error) {
	return b.messageStore.PutDelayMessage(msg, delayLevel)
}

// PutOrderedMessage 存储顺序消息
func (b *Broker) PutOrderedMessage(msg *common.Message, shardingKey string) (*common.SendResult, error) {
	return b.messageStore.PutOrderedMessage(msg, shardingKey)
}

// PullOrderedMessage 拉取顺序消息
func (b *Broker) PullOrderedMessage(topic string, queueId int32, consumerGroup string, maxNums int32) ([]*common.MessageExt, error) {
	return b.messageStore.PullOrderedMessage(topic, queueId, consumerGroup, maxNums)
}

// DefaultBrokerConfig 返回默认Broker配置
func DefaultBrokerConfig() *Config {
	return &Config{
		BrokerName:                "DefaultBroker",
		BrokerId:                  0,
		ClusterName:               "DefaultCluster",
		ListenPort:                10911,
		NameServerAddr:            "127.0.0.1:9876",
		StorePathRootDir:          "/tmp/rocketmq-store",
		SendMessageThreadPoolNums: 16,
		PullMessageThreadPoolNums: 16,
		FlushDiskType:             0, // ASYNC_FLUSH
		BrokerRole:                0, // ASYNC_MASTER
		HaListenPort:              10912,
		ReplicationMode:           0, // ASYNC_REPLICATION
		EnableCluster:             true,
		ClusterManagerPort:        10913,
		EnableFailover:            true,
		AutoFailover:              false,
		FailoverDelay:             30,
		BackupBrokers:             []string{},
		AclEnable:                 false,
		AclConfigFile:             "config/plain_acl.yml",
	}
}

// parseRequestHeader 解析请求头
func parseRequestHeader(request *remoting.RemotingCommand, header interface{}) error {
	if request.ExtFields != nil {
		headerData, _ := json.Marshal(request.ExtFields)
		if err := json.Unmarshal(headerData, header); err != nil {
			return err
		}
	}
	return nil
}

// SendMessageProcessor 发送消息处理器
type SendMessageProcessor struct {
	broker *Broker
}

// ProcessRequest 处理发送消息请求
func (p *SendMessageProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// ACL权限验证
	if p.broker.aclMiddleware != nil && p.broker.aclMiddleware.IsAclEnabled() {
		// 构造请求数据用于验证
		requestData := map[string]string{
			"topic":     request.ExtFields["topic"],
			"operation": "PUB",
			"accessKey": request.ExtFields["accessKey"],
			"signature": request.ExtFields["signature"],
			"timestamp": request.ExtFields["timestamp"],
		}

		// 验证生产者权限
		_, err := p.broker.aclMiddleware.ValidateProducerRequest(requestData, request.ExtFields["topic"], conn.GetRemoteAddr())
		if err != nil {
			log.Printf("ACL validation failed for producer: %v", err)
			return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Access denied: %v", err)), nil
		}
	}

	// 解析请求头
	var header remoting.SendMessageRequestHeader
	if request.ExtFields != nil {
		headerData, _ := json.Marshal(request.ExtFields)
		if err := json.Unmarshal(headerData, &header); err != nil {
			return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Failed to parse send message header: %v", err)), nil
		}
	}

	// 解析Properties字符串为map
	properties := make(map[string]string)
	if header.Properties != "" {
		// 简化实现：假设Properties是JSON格式
		json.Unmarshal([]byte(header.Properties), &properties)
	}

	// 创建消息
	msg := &common.Message{
		Topic:      header.Topic,
		Properties: properties,
		Body:       request.Body,
	}

	// 存储消息
	result, err := p.broker.PutMessage(msg)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Failed to store message: %v", err)), nil
	}

	// 创建响应
	responseHeader := &remoting.SendMessageResponseHeader{
		MsgId:         result.MsgId,
		QueueId:       result.MessageQueue.QueueId,
		QueueOffset:   result.QueueOffset,
		TransactionId: "", // 简化实现
	}

	extFields := make(map[string]string)
	headerData, _ := json.Marshal(responseHeader)
	json.Unmarshal(headerData, &extFields)

	response := remoting.CreateResponseCommand(remoting.Success, "")
	response.Opaque = request.Opaque
	response.ExtFields = extFields

	return response, nil
}

// PullMessageProcessor 拉取消息处理器
type PullMessageProcessor struct {
	broker *Broker
}

// ProcessRequest 处理拉取消息请求
func (p *PullMessageProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// 解析请求头
	var header remoting.PullMessageRequestHeader
	if request.ExtFields != nil {
		headerData, _ := json.Marshal(request.ExtFields)
		if err := json.Unmarshal(headerData, &header); err != nil {
			return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Failed to parse pull message header: %v", err)), nil
		}
	}

	// 拉取消息
	messages, err := p.broker.PullMessage(header.Topic, header.QueueId, header.QueueOffset, header.MaxMsgNums)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Failed to pull message: %v", err)), nil
	}

	// 创建响应
	responseHeader := &remoting.PullMessageResponseHeader{
		SuggestWhichBrokerId: 0,
		NextBeginOffset:      header.QueueOffset + int64(len(messages)),
		MinOffset:            0,
		MaxOffset:            1000, // 简化实现
	}

	extFields := make(map[string]string)
	headerData, _ := json.Marshal(responseHeader)
	json.Unmarshal(headerData, &extFields)

	// 编码消息体
	var responseBody []byte
	if len(messages) > 0 {
		// 简化实现：将消息序列化为JSON
		responseBody, _ = json.Marshal(messages)
	}

	response := remoting.CreateResponseCommand(remoting.Success, "")
	response.Opaque = request.Opaque
	response.ExtFields = extFields
	response.Body = responseBody

	return response, nil
}

// QueryRouteProcessor 查询路由处理器
type QueryRouteProcessor struct {
	broker *Broker
}

// ProcessRequest 处理路由查询请求
func (p *QueryRouteProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	topic := request.ExtFields["topic"]
	if topic == "" {
		return remoting.CreateResponseCommand(remoting.SystemError, "topic is required for route query"), nil
	}

	// 构造路由数据
	routeData := &remoting.TopicRouteData{
		QueueDatas: []*remoting.QueueData{
			{
				BrokerName:     p.broker.config.BrokerName,
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6, // 读写权限
			},
		},
		BrokerDatas: []*remoting.BrokerData{
			{
				Cluster:    p.broker.config.ClusterName,
				BrokerName: p.broker.config.BrokerName,
				BrokerAddrs: map[int64]string{
					0: fmt.Sprintf("127.0.0.1:%d", p.broker.config.ListenPort),
				},
			},
		},
	}

	data, err := json.Marshal(routeData)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("failed to marshal route data: %v", err)), nil
	}

	response := remoting.CreateResponseCommand(remoting.Success, "")
	response.Body = data
	return response, nil
}

// HeartbeatProcessor 心跳处理器
type HeartbeatProcessor struct {
	heartbeatProcessor *heartbeat.HeartbeatProcessor
}

// ProcessRequest 处理心跳请求
func (p *HeartbeatProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// Convert remoting command to server command
	serverRequest := &command.RemotingCommand{
		Code:      command.RequestCode(request.Code),
		Language:  request.Language,
		Version:   request.Version,
		Opaque:    request.Opaque,
		Flag:      request.Flag,
		Remark:    request.Remark,
		ExtFields: request.ExtFields,
		Body:      request.Body,
	}

	serverConn := &command.Connection{
		Addr:       conn.Addr,
		Conn:       conn.Conn,
		Reader:     conn.Reader,
		Writer:     conn.Writer,
		LastUsed:   conn.LastUsed,
		Closed:     conn.Closed,
		RemoteAddr: conn.RemoteAddr,
	}

	// Process the request
	response, err := p.heartbeatProcessor.ProcessRequest(ctx, serverRequest, serverConn)
	if err != nil {
		return nil, err
	}

	// Convert response back to remoting command
	if response != nil {
		remotingResponse := &remoting.RemotingCommand{
			Code:      remoting.RequestCode(response.Code),
			Language:  response.Language,
			Version:   response.Version,
			Opaque:    response.Opaque,
			Flag:      response.Flag,
			Remark:    response.Remark,
			ExtFields: response.ExtFields,
			Body:      response.Body,
		}
		return remotingResponse, nil
	}

	return nil, nil
}

// UpdateConsumerOffsetProcessor 更新消费者偏移量处理器
type UpdateConsumerOffsetProcessor struct {
	broker *Broker
}

// ProcessRequest 处理消费者偏移量更新请求
func (p *UpdateConsumerOffsetProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	consumerGroup := request.ExtFields["consumerGroup"]
	queueKey := request.ExtFields["queueKey"]
	commitOffsetStr := request.ExtFields["commitOffset"]

	if consumerGroup == "" || queueKey == "" || commitOffsetStr == "" {
		return remoting.CreateResponseCommand(remoting.SystemError, "consumerGroup, queueKey and commitOffset are required"), nil
	}

	commitOffset, err := strconv.ParseInt(commitOffsetStr, 10, 64)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("invalid commitOffset: %v", err)), nil
	}

	// 更新偏移量（简化实现）
	log.Printf("Update consumer offset - Group: %s, Queue: %s, Offset: %d", consumerGroup, queueKey, commitOffset)

	response := remoting.CreateResponseCommand(remoting.Success, "")
	return response, nil
}

// SendMessageV2Processor 发送消息V2处理器
type SendMessageV2Processor struct {
	broker *Broker
}

// ProcessRequest 处理V2版本消息发送请求
func (p *SendMessageV2Processor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// ACL权限检查
	if p.broker.aclMiddleware != nil && p.broker.aclMiddleware.IsAclEnabled() {
		// 构造请求数据用于验证
		requestData := map[string]string{
			"topic":     request.ExtFields["topic"],
			"operation": "PUB",
			"accessKey": request.ExtFields["accessKey"],
			"signature": request.ExtFields["signature"],
			"timestamp": request.ExtFields["timestamp"],
		}

		// 验证生产者权限
		_, err := p.broker.aclMiddleware.ValidateProducerRequest(requestData, request.ExtFields["topic"], conn.GetRemoteAddr())
		if err != nil {
			log.Printf("ACL validation failed for producer: %v", err)
			return remoting.CreateResponseCommand(remoting.ResponseCode(1), fmt.Sprintf("Access denied: %v", err)), nil
		}
	}

	var header remoting.SendMessageRequestHeader
	if err := parseRequestHeader(request, &header); err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Failed to parse send message header: %v", err)), nil
	}

	// 构造消息并存储
	message := &common.Message{
		Topic: header.Topic,
		Body:  request.Body,
	}

	if header.Properties != "" {
		props := make(map[string]string)
		if err := json.Unmarshal([]byte(header.Properties), &props); err == nil {
			message.Properties = props
		}
	}

	// 存储消息
	result, err := p.broker.messageStore.PutMessage(message)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Failed to store message: %v", err)), nil
	}

	// 构造响应
	responseHeader := &remoting.SendMessageResponseHeader{
		MsgId:       result.MsgId,
		QueueId:     result.MessageQueue.QueueId,
		QueueOffset: result.QueueOffset,
	}

	response := remoting.CreateResponseCommand(remoting.Success, "")
	response.ExtFields = map[string]string{
		"msgId":       responseHeader.MsgId,
		"queueId":     strconv.FormatInt(int64(responseHeader.QueueId), 10),
		"queueOffset": strconv.FormatInt(responseHeader.QueueOffset, 10),
	}
	return response, nil
}

// SendBatchMessageProcessor 发送批量消息处理器
type SendBatchMessageProcessor struct {
	broker *Broker
}

// ProcessRequest 处理批量消息发送请求
func (p *SendBatchMessageProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// ACL权限检查
	if p.broker.aclMiddleware != nil && p.broker.aclMiddleware.IsAclEnabled() {
		// 构造请求数据用于验证
		requestData := map[string]string{
			"topic":     request.ExtFields["topic"],
			"operation": "PUB",
			"accessKey": request.ExtFields["accessKey"],
			"signature": request.ExtFields["signature"],
			"timestamp": request.ExtFields["timestamp"],
		}

		// 验证生产者权限
		_, err := p.broker.aclMiddleware.ValidateProducerRequest(requestData, request.ExtFields["topic"], conn.GetRemoteAddr())
		if err != nil {
			log.Printf("ACL validation failed for producer: %v", err)
			return remoting.CreateResponseCommand(remoting.ResponseCode(1), fmt.Sprintf("Access denied: %v", err)), nil
		}
	}

	var header remoting.SendMessageRequestHeader
	if err := parseRequestHeader(request, &header); err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Failed to parse send message header: %v", err)), nil
	}

	// 解析批量消息
	messages := make([]*common.Message, 0)
	if err := json.Unmarshal(request.Body, &messages); err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Failed to parse batch messages: %v", err)), nil
	}

	// 存储批量消息
	var msgIds []string
	for _, message := range messages {
		result, err := p.broker.messageStore.PutMessage(message)
		if err != nil {
			return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("Failed to store message: %v", err)), nil
		}
		msgIds = append(msgIds, result.MsgId)
	}

	// 构造响应
	response := remoting.CreateResponseCommand(remoting.Success, "")
	response.ExtFields = map[string]string{
		"msgIds": strings.Join(msgIds, ","),
	}
	return response, nil
}

// QueryMessageProcessor 查询消息处理器
type QueryMessageProcessor struct {
	broker *Broker
}

// ProcessRequest 处理消息查询请求
func (p *QueryMessageProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	topic := request.ExtFields["topic"]
	key := request.ExtFields["key"]

	if topic == "" || key == "" {
		return remoting.CreateResponseCommand(remoting.SystemError, "topic and key are required for message query"), nil
	}

	// 查询消息（简化实现）
	log.Printf("Query message - Topic: %s, Key: %s", topic, key)

	response := remoting.CreateResponseCommand(remoting.Success, "")
	return response, nil
}

// QueryMessageByKeyProcessor 根据Key查询消息处理器
type QueryMessageByKeyProcessor struct {
	broker *Broker
}

// ProcessRequest 处理按键消息查询请求
func (p *QueryMessageByKeyProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	topic := request.ExtFields["topic"]
	key := request.ExtFields["key"]

	if topic == "" || key == "" {
		return remoting.CreateResponseCommand(remoting.SystemError, "topic and key are required for message query by key"), nil
	}

	// 按键查询消息（简化实现）
	log.Printf("Query message by key - Topic: %s, Key: %s", topic, key)

	response := remoting.CreateResponseCommand(remoting.Success, "")
	return response, nil
}

// UpdateAndCreateTopicProcessor 创建/更新Topic处理器
type UpdateAndCreateTopicProcessor struct {
	broker *Broker
}

// ProcessRequest 处理主题创建和更新请求
func (p *UpdateAndCreateTopicProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	topic := request.ExtFields["topic"]
	if topic == "" {
		return remoting.CreateResponseCommand(remoting.SystemError, "topic is required"), nil
	}

	// 创建或更新主题（简化实现）
	log.Printf("Update or create topic: %s", topic)

	response := remoting.CreateResponseCommand(remoting.Success, "")
	return response, nil
}

// GetBrokerClusterInfoProcessor 获取Broker集群信息处理器
type GetBrokerClusterInfoProcessor struct {
	broker *Broker
}

// ProcessRequest 处理获取Broker集群信息请求
func (p *GetBrokerClusterInfoProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// 构造集群信息
	clusterInfo := &remoting.ClusterInfo{
		BrokerAddrTable: map[string]map[int64]string{
			p.broker.config.BrokerName: {
				0: fmt.Sprintf("127.0.0.1:%d", p.broker.config.ListenPort),
			},
		},
		ClusterAddrTable: map[string][]string{
			p.broker.config.ClusterName: {p.broker.config.BrokerName},
		},
	}

	data, err := json.Marshal(clusterInfo)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("failed to marshal cluster info: %v", err)), nil
	}

	response := remoting.CreateResponseCommand(remoting.Success, "")
	response.Body = data
	return response, nil
}

// RegisterBrokerProcessor Broker注册处理器
type RegisterBrokerProcessor struct {
	broker *Broker
}

// ProcessRequest 处理Broker注册请求
func (p *RegisterBrokerProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	brokerName := request.ExtFields["brokerName"]
	brokerAddr := request.ExtFields["brokerAddr"]

	if brokerName == "" || brokerAddr == "" {
		return remoting.CreateResponseCommand(remoting.SystemError, "brokerName and brokerAddr are required"), nil
	}

	// 注册Broker（简化实现）
	log.Printf("Register broker - Name: %s, Address: %s", brokerName, brokerAddr)

	response := remoting.CreateResponseCommand(remoting.Success, "")
	return response, nil
}

// UnregisterBrokerProcessor Broker注销处理器
type UnregisterBrokerProcessor struct {
	broker *Broker
}

// ProcessRequest 处理Broker注销请求
func (p *UnregisterBrokerProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	brokerName := request.ExtFields["brokerName"]
	brokerAddr := request.ExtFields["brokerAddr"]

	if brokerName == "" || brokerAddr == "" {
		return remoting.CreateResponseCommand(remoting.SystemError, "brokerName and brokerAddr are required"), nil
	}

	// 注销Broker（简化实现）
	log.Printf("Unregister broker - Name: %s, Address: %s", brokerName, brokerAddr)

	response := remoting.CreateResponseCommand(remoting.Success, "")
	return response, nil
}

// EndTransactionProcessor 结束事务处理器
type EndTransactionProcessor struct {
	broker *Broker
}

// ProcessRequest 处理事务结束请求
func (p *EndTransactionProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// 处理事务结束（简化实现）
	log.Printf("End transaction")

	response := remoting.CreateResponseCommand(remoting.Success, "")
	return response, nil
}

// CheckTransactionStateProcessor 检查事务状态处理器
type CheckTransactionStateProcessor struct {
	broker *Broker
}

// ProcessRequest 处理事务状态检查请求
func (p *CheckTransactionStateProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// 检查事务状态（简化实现）
	log.Printf("Check transaction state")

	response := remoting.CreateResponseCommand(remoting.Success, "")
	return response, nil
}

// UpdateBrokerConfigProcessor 更新Broker配置处理器
type UpdateBrokerConfigProcessor struct {
	broker *Broker
}

// ProcessRequest 处理Broker配置更新请求
func (p *UpdateBrokerConfigProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// 更新Broker配置（简化实现）
	log.Printf("Update broker config")

	response := remoting.CreateResponseCommand(remoting.Success, "")
	return response, nil
}

// GetBrokerConfigProcessor 获取Broker配置处理器
type GetBrokerConfigProcessor struct {
	broker *Broker
}

// ProcessRequest 处理获取Broker配置请求
func (p *GetBrokerConfigProcessor) ProcessRequest(ctx context.Context, request *remoting.RemotingCommand, conn *remoting.ServerConnection) (*remoting.RemotingCommand, error) {
	// 获取Broker配置（简化实现）
	configData := map[string]string{
		"brokerName": p.broker.config.BrokerName,
		"brokerId":   strconv.FormatInt(p.broker.config.BrokerId, 10),
		"listenPort": strconv.Itoa(p.broker.config.ListenPort),
	}

	data, err := json.Marshal(configData)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.SystemError, fmt.Sprintf("failed to marshal config data: %v", err)), nil
	}

	response := remoting.CreateResponseCommand(remoting.Success, "")
	response.Body = data
	return response, nil
}
