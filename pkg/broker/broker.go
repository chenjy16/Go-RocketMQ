package broker

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"go-rocketmq/pkg/common"
	"go-rocketmq/pkg/protocol"
)

// Broker 代表一个消息代理
type Broker struct {
	config       *Config
	listener     net.Listener
	shutdown     chan struct{}
	wg           sync.WaitGroup
	mutex        sync.RWMutex
	
	// 消息存储
	messageStore *MessageStore
	
	// Topic配置
	topicConfigTable map[string]*protocol.TopicConfig
	
	// 消费者组信息
	consumerTable map[string]*ConsumerGroupInfo
	
	// 生产者信息
	producerTable map[string]*ProducerGroupInfo
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
}

// MessageStore 消息存储
type MessageStore struct {
	mutex    sync.RWMutex
	messages map[string][]*common.MessageExt // topic -> messages
	queues   map[string]map[int32][]*common.MessageExt // topic -> queueId -> messages
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
	
	return &Broker{
		config:           config,
		shutdown:         make(chan struct{}),
		messageStore:     NewMessageStore(),
		topicConfigTable: make(map[string]*protocol.TopicConfig),
		consumerTable:    make(map[string]*ConsumerGroupInfo),
		producerTable:    make(map[string]*ProducerGroupInfo),
	}
}

// NewMessageStore 创建新的消息存储
func NewMessageStore() *MessageStore {
	return &MessageStore{
		messages: make(map[string][]*common.MessageExt),
		queues:   make(map[string]map[int32][]*common.MessageExt),
	}
}

// Start 启动Broker
func (b *Broker) Start() error {
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
	// TODO: 实现向NameServer注册的逻辑
	log.Printf("Registering broker %s to NameServer %s", b.config.BrokerName, b.config.NameServerAddr)
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
			// TODO: 发送心跳到NameServer
			log.Printf("Sending heartbeat to NameServer")
		}
	}
}

// PutMessage 存储消息
func (b *Broker) PutMessage(msg *common.Message) (*common.SendResult, error) {
	b.messageStore.mutex.Lock()
	defer b.messageStore.mutex.Unlock()
	
	// 创建扩展消息
	msgExt := &common.MessageExt{
		Message:     msg,
		QueueId:     0, // 简化版本，使用固定队列ID
		StoreSize:   int32(len(msg.Body)),
		QueueOffset: 0,
		SysFlag:     0,
		BornTimestamp: time.Now(),
		StoreTimestamp: time.Now(),
		BornHost:    "127.0.0.1:0",
		StoreHost:   fmt.Sprintf("127.0.0.1:%d", b.config.ListenPort),
	}
	
	// 存储到topic消息列表
	messages := b.messageStore.messages[msg.Topic]
	messages = append(messages, msgExt)
	b.messageStore.messages[msg.Topic] = messages
	
	// 存储到队列
	if b.messageStore.queues[msg.Topic] == nil {
		b.messageStore.queues[msg.Topic] = make(map[int32][]*common.MessageExt)
	}
	queueMessages := b.messageStore.queues[msg.Topic][msgExt.QueueId]
	queueMessages = append(queueMessages, msgExt)
	b.messageStore.queues[msg.Topic][msgExt.QueueId] = queueMessages
	
	// 设置队列偏移量
	msgExt.QueueOffset = int64(len(queueMessages) - 1)
	
	result := &common.SendResult{
		SendStatus: common.SendOK,
		MsgId:      fmt.Sprintf("%s_%d_%d", msg.Topic, msgExt.QueueId, msgExt.QueueOffset),
		MessageQueue: &common.MessageQueue{
			Topic:      msg.Topic,
			BrokerName: b.config.BrokerName,
			QueueId:    msgExt.QueueId,
		},
		QueueOffset: msgExt.QueueOffset,
	}
	
	log.Printf("Message stored: topic=%s, queueId=%d, offset=%d", 
		msg.Topic, msgExt.QueueId, msgExt.QueueOffset)
	
	return result, nil
}

// PullMessage 拉取消息
func (b *Broker) PullMessage(topic string, queueId int32, offset int64, maxNums int32) ([]*common.MessageExt, error) {
	b.messageStore.mutex.RLock()
	defer b.messageStore.mutex.RUnlock()
	
	queueMessages := b.messageStore.queues[topic][queueId]
	if queueMessages == nil || offset >= int64(len(queueMessages)) {
		return nil, nil
	}
	
	start := int(offset)
	end := start + int(maxNums)
	if end > len(queueMessages) {
		end = len(queueMessages)
	}
	
	result := make([]*common.MessageExt, end-start)
	copy(result, queueMessages[start:end])
	
	log.Printf("Messages pulled: topic=%s, queueId=%d, offset=%d, count=%d", 
		topic, queueId, offset, len(result))
	
	return result, nil
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
	
	// 初始化队列
	if b.messageStore.queues[topic] == nil {
		b.messageStore.queues[topic] = make(map[int32][]*common.MessageExt)
		for i := int32(0); i < queueNums; i++ {
			b.messageStore.queues[topic][i] = make([]*common.MessageExt, 0)
		}
	}
	
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
	}
}