package client

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 常量定义
const (
	VERSION = 317
)

// 全局变量
var requestId int32

// SimpleConnectionPool 简单连接池
type SimpleConnectionPool struct {
	connections map[string]net.Conn
	mutex       sync.RWMutex
	maxIdle     time.Duration
}

// GetConnection 获取连接
func (pool *SimpleConnectionPool) GetConnection(addr string) (net.Conn, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	
	// 检查是否有可用连接
	if conn, exists := pool.connections[addr]; exists {
		// 简单检查连接是否有效
		if conn != nil {
			delete(pool.connections, addr)
			return conn, nil
		}
	}
	
	// 创建新连接
	return net.DialTimeout("tcp", addr, 5*time.Second)
}

// ReleaseConnection 释放连接
func (pool *SimpleConnectionPool) ReleaseConnection(addr string, conn net.Conn) {
	if conn == nil {
		return
	}
	
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	
	// 简单实现：直接存储连接（实际应该检查连接状态和池大小）
	pool.connections[addr] = conn
}

// Close 关闭连接池
func (pool *SimpleConnectionPool) Close() {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	
	for addr, conn := range pool.connections {
		if conn != nil {
			conn.Close()
		}
		delete(pool.connections, addr)
	}
}



// Consumer 消费者客户端
type Consumer struct {
	config          *ConsumerConfig
	nameServerAddrs []string
	subscriptions   map[string]*Subscription
	messageListener MessageListener
	mutex           sync.RWMutex
	started         bool
	shutdown        chan struct{}
	wg              sync.WaitGroup
	rebalanceService *RebalanceService
	loadBalanceStrategy LoadBalanceStrategy
	// 消息追踪管理器
	traceManager *TraceManager
	// ACL中间件
	aclMiddleware *ACLMiddleware
	// 队列选择器索引
	queueSelectorIndex int
	// 消费进度存储
	consumeOffsets map[string]int64
	offsetMutex    sync.RWMutex
	// 连接池管理器
	connectionPool *SimpleConnectionPool
	// 心跳管理
	heartbeatTicker *time.Ticker
	clientID       string
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	GroupName            string                      // 消费者组名
	NameServerAddr       string                      // NameServer地址
	ConsumeFromWhere     ConsumeFromWhere     // 消费起始位置
	MessageModel         MessageModel         // 消息模式
	ConsumeThreadMin     int                         // 最小消费线程数
	ConsumeThreadMax     int                         // 最大消费线程数
	PullInterval         time.Duration               // 拉取间隔
	PullBatchSize        int32                       // 批量拉取大小
	ConsumeTimeout       time.Duration               // 消费超时时间
	// ACL 配置字段
	AccessKey            string                      // ACL访问密钥
	SecretKey            string                      // ACL秘密密钥
	SecurityToken        string                      // ACL安全令牌
	SignatureMethod      string                      // 签名算法，默认HmacSHA1
	EnableACL            bool                        // 是否启用ACL认证
	ACLConfigPath        string                      // ACL配置文件路径
	ACLHotReload         bool                        // 是否启用ACL配置热重载
}

// Subscription 订阅信息
type Subscription struct {
	Topic      string
	SubExpression string
	Listener   MessageListener
	Filter     MessageFilter // 消息过滤器
}

// NewConsumer 创建新的消费者
func NewConsumer(config *ConsumerConfig) *Consumer {
	if config == nil {
		config = DefaultConsumerConfig()
	}
	
	consumer := &Consumer{
		config:        config,
		subscriptions: make(map[string]*Subscription),
		shutdown:      make(chan struct{}),
		loadBalanceStrategy: &AverageAllocateStrategy{}, // 默认使用平均分配策略
	}
	consumer.rebalanceService = NewRebalanceService(consumer)
	
	// 初始化消息追踪管理器（默认不启用）
	consumer.traceManager = NewTraceManager(nil)
	consumer.traceManager.SetEnabled(false)
	
	// 初始化ACL中间件（默认不启用）
	consumer.aclMiddleware = nil
	
	// 初始化连接池
	consumer.connectionPool = &SimpleConnectionPool{
		connections: make(map[string]net.Conn),
		maxIdle:     30 * time.Minute,
	}
	
	return consumer
}

// DefaultConsumerConfig 返回默认消费者配置
func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		GroupName:        "DefaultConsumerGroup",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: ConsumeFromFirstOffset,
		MessageModel:     Clustering,
		ConsumeThreadMin: 1,
		ConsumeThreadMax: 20,
		PullInterval:     1 * time.Second,
		PullBatchSize:    32,
		ConsumeTimeout:   15 * time.Minute,
	}
}

// SetNameServerAddr 设置NameServer地址
func (c *Consumer) SetNameServerAddr(addr string) {
	c.config.NameServerAddr = addr
}

// Subscribe 订阅Topic
func (c *Consumer) Subscribe(topic, subExpression string, listener MessageListener) error {
	return c.SubscribeWithFilter(topic, subExpression, listener, nil)
}

// SubscribeWithFilter 订阅主题并指定过滤器
func (c *Consumer) SubscribeWithFilter(topic, subExpression string, listener MessageListener, filter MessageFilter) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if c.started {
		return fmt.Errorf("consumer already started, cannot subscribe to new topics")
	}
	
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	
	if listener == nil {
		return fmt.Errorf("message listener cannot be nil")
	}
	
	// 检查是否已经订阅了该主题
	if _, exists := c.subscriptions[topic]; exists {
		return fmt.Errorf("topic %s already subscribed", topic)
	}
	
	// 如果没有指定过滤器，根据subExpression创建默认过滤器
	if filter == nil && subExpression != "" && subExpression != "*" {
		filter = NewTagFilterFromExpression(subExpression)
	}
	
	c.subscriptions[topic] = &Subscription{
		Topic:         topic,
		SubExpression: subExpression,
		Listener:      listener,
		Filter:        filter,
	}
	
	log.Printf("Subscribed to topic: %s with expression: %s", topic, subExpression)
	return nil
}

// SubscribeWithTagFilter 使用标签过滤器订阅主题
func (c *Consumer) SubscribeWithTagFilter(topic string, tags ...string) error {
	filter := NewTagFilter(tags...)
	return c.SubscribeWithFilter(topic, "", nil, filter)
}

// SubscribeWithSQLFilter 使用SQL92过滤器订阅主题
func (c *Consumer) SubscribeWithSQLFilter(topic, sqlExpression string, listener MessageListener) error {
	filter, err := NewSQL92Filter(sqlExpression)
	if err != nil {
		return fmt.Errorf("create SQL filter failed: %v", err)
	}
	return c.SubscribeWithFilter(topic, sqlExpression, listener, filter)
}

// Start 启动消费者
func (c *Consumer) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if c.started {
		return fmt.Errorf("consumer already started")
	}
	
	if len(c.subscriptions) == 0 {
		return fmt.Errorf("no subscriptions found, please subscribe to topics first")
	}
	
	// 解析NameServer地址
	c.nameServerAddrs = []string{c.config.NameServerAddr}
	
	// 启动重平衡服务
	c.rebalanceService.Start()
	
	// 启动消息追踪管理器
	if c.traceManager != nil {
		c.traceManager.Start()
	}
	
	// 初始化ACL中间件
	if err := c.initACLMiddleware(); err != nil {
		return fmt.Errorf("failed to initialize ACL middleware: %v", err)
	}
	
	// 启动心跳机制
	c.startHeartbeat()
	
	// 启动消费线程
	for i := 0; i < c.config.ConsumeThreadMax; i++ {
		c.wg.Add(1)
		go c.consumeLoop()
	}
	
	c.started = true
	log.Printf("Consumer started: group=%s, threads=%d", c.config.GroupName, c.config.ConsumeThreadMax)
	
	return nil
}

// Stop 停止消费者
func (c *Consumer) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if !c.started {
		return fmt.Errorf("consumer not started")
	}
	
	// 停止心跳机制
	c.stopHeartbeat()
	
	// 停止重平衡服务
	c.rebalanceService.Stop()
	
	// 停止消息追踪管理器
	if c.traceManager != nil {
		c.traceManager.Stop()
	}
	
	// 关闭连接池
	if c.connectionPool != nil {
		c.connectionPool.Close()
	}
	
	close(c.shutdown)
	c.wg.Wait()
	
	c.started = false
	log.Printf("Consumer stopped: group=%s", c.config.GroupName)
	
	return nil
}

// consumeLoop 消费循环
func (c *Consumer) consumeLoop() {
	defer c.wg.Done()
	
	ticker := time.NewTicker(c.config.PullInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-c.shutdown:
			return
		case <-ticker.C:
			c.pullAndConsumeMessages()
		}
	}
}

// pullAndConsumeMessages 拉取并消费消息
func (c *Consumer) pullAndConsumeMessages() {
	c.mutex.RLock()
	subscriptions := make(map[string]*Subscription)
	for k, v := range c.subscriptions {
		subscriptions[k] = v
	}
	c.mutex.RUnlock()
	
	for topic, subscription := range subscriptions {
		c.pullMessagesForTopic(topic, subscription)
	}
}

// pullMessagesForTopic 为指定Topic拉取消息
func (c *Consumer) pullMessagesForTopic(topic string, subscription *Subscription) {
	// 1. 从NameServer获取Topic路由信息
	routeData, err := c.getTopicRouteFromNameServer(topic)
	if err != nil {
		log.Printf("Failed to get route info for topic %s: %v", topic, err)
		return
	}
	
	if routeData == nil || len(routeData.QueueDatas) == 0 {
		log.Printf("No available queues for topic %s", topic)
		return
	}
	
	// 2. 选择消息队列
	mq := c.selectMessageQueue(routeData, topic)
	if mq == nil {
		log.Printf("Failed to select message queue for topic %s", topic)
		return
	}
	
	// 3. 获取消费进度
	offset, err := c.getConsumeOffset(mq)
	if err != nil {
		log.Printf("Failed to get consume offset for queue %v, using 0: %v", mq, err)
		offset = 0
	}
	
	// 4. 发送拉取请求到Broker
	messages, nextOffset, err := c.pullMessagesFromBroker(mq, offset)
	if err != nil {
		log.Printf("Failed to pull messages from broker: %v", err)
		return
	}
	
	// 5. 更新消费进度
	if len(messages) > 0 {
		c.updateConsumeOffset(mq, nextOffset)
		c.consumeMessagesWithFilter(messages, subscription)
	}
}

// getTopicRouteFromNameServer 从NameServer获取Topic路由信息
func (c *Consumer) getTopicRouteFromNameServer(topic string) (*TopicRouteData, error) {
	if c.config.NameServerAddr == "" {
		return nil, fmt.Errorf("nameserver address not set")
	}
	
	// 从连接池获取连接到NameServer
	conn, err := c.connectionPool.GetConnection(c.config.NameServerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection to nameserver %s: %v", c.config.NameServerAddr, err)
	}
	defer c.connectionPool.ReleaseConnection(c.config.NameServerAddr, conn)
	
	// 构造获取路由信息的请求
	request := &RemotingCommand{
		Code:     GET_ROUTEINTO_BY_TOPIC,
		Version:  VERSION,
		Opaque:   atomic.AddInt32(&requestId, 1),
		Flag:     0,
		Remark:   "",
		ExtFields: map[string]string{"topic": topic},
	}
	
	// 发送请求
	err = c.sendRequest(conn, request)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	
	// 接收响应
	response, err := c.receiveResponse(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to receive response: %v", err)
	}
	
	if response.Code != SUCCESS {
		return nil, fmt.Errorf("nameserver returned error: %s", response.Remark)
	}
	
	// 解析路由数据
	var routeData TopicRouteData
	if len(response.Body) > 0 {
		err = json.Unmarshal(response.Body, &routeData)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal route data: %v", err)
		}
	}
	
	return &routeData, nil
}

// selectMessageQueue 选择消息队列
func (c *Consumer) selectMessageQueue(routeData *TopicRouteData, topic string) *MessageQueue {
	if len(routeData.QueueDatas) == 0 {
		return nil
	}
	
	// 简单的轮询选择策略
	c.queueSelectorIndex++
	queueData := routeData.QueueDatas[c.queueSelectorIndex%len(routeData.QueueDatas)]
	
	// 选择一个可用的Broker
	var brokerAddr string
	for _, brokerData := range routeData.BrokerDatas {
		if brokerData.BrokerName == queueData.BrokerName {
			if addr, ok := brokerData.BrokerAddrs[0]; ok { // 选择Master Broker
				brokerAddr = addr
				break
			}
		}
	}
	
	if brokerAddr == "" {
		return nil
	}
	
	return &MessageQueue{
		Topic:      topic,
		BrokerName: queueData.BrokerName,
		QueueId:    0, // 简化：使用第一个队列
	}
}

// mockPullMessages 模拟拉取消息（用于测试）
func (c *Consumer) mockPullMessages(topic string) []*MessageExt {
	// 这是一个模拟实现，实际应该从Broker拉取
	return []*MessageExt{
		{
			Message: &Message{
				Topic: topic,
				Body:  []byte("mock message body"),
			},
			MsgId:     "mock_msg_id_001",
			QueueId:   0,
			StoreSize: 100,
		},
	}
}

// consumeMessages 消费消息
func (c *Consumer) consumeMessages(messages []*MessageExt, listener MessageListener) {
	if listener == nil {
		log.Printf("No listener found for messages, skipping %d messages", len(messages))
		return
	}
	
	// ACL验证：如果启用了ACL，验证消息权限
	if c.IsACLEnabled() {
		verifiedMessages := make([]*MessageExt, 0, len(messages))
		for _, msg := range messages {
			if c.verifyMessageACL(msg) {
				verifiedMessages = append(verifiedMessages, msg)
			} else {
				log.Printf("ACL verification failed for message: %s, topic: %s", msg.MsgId, msg.Topic)
			}
		}
		messages = verifiedMessages
	}
	
	// 如果没有通过ACL验证的消息，直接返回
	if len(messages) == 0 {
		log.Printf("No messages passed ACL verification")
		return
	}
	
	// 调用消息监听器处理消息
	result := listener.ConsumeMessage(messages)
	
	switch result {
	case ConsumeSuccess:
		log.Printf("Successfully consumed %d messages", len(messages))
		// 提交消费进度到Broker
		err := c.commitConsumeProgress(messages)
		if err != nil {
			log.Printf("Failed to commit consume progress: %v", err)
		}
	case ReconsumeLater:
		log.Printf("Failed to consume %d messages, will retry later", len(messages))
		// 将消息重新放回队列或延迟重试
		err := c.retryMessages(messages)
		if err != nil {
			log.Printf("Failed to retry messages: %v", err)
		}
	}
}

// consumeMessagesWithFilter 使用过滤器消费消息
func (c *Consumer) consumeMessagesWithFilter(messages []*MessageExt, subscription *Subscription) {
	if len(messages) == 0 || subscription == nil || subscription.Listener == nil {
		return
	}
	
	// 应用过滤器
	filteredMessages := make([]*MessageExt, 0, len(messages))
	for _, msg := range messages {
		if subscription.Filter == nil || subscription.Filter.Match(msg) {
			filteredMessages = append(filteredMessages, msg)
		} else {
			log.Printf("Message filtered out: %s, topic: %s, tags: %s", msg.MsgId, msg.Topic, msg.Tags)
		}
	}
	
	// 消费通过过滤器的消息
	if len(filteredMessages) > 0 {
		c.consumeWithTrace(filteredMessages, subscription)
	}
}

// consumeWithTrace 带追踪的消息消费
func (c *Consumer) consumeWithTrace(msgs []*MessageExt, subscription *Subscription) {
	for _, msg := range msgs {
		// 创建消费者追踪上下文
		var traceCtx *TraceContext
		if c.traceManager != nil && c.traceManager.IsEnabled() {
			traceCtx = CreateConsumeTraceContext(c.config.GroupName, msg)
			traceCtx.TimeStamp = time.Now().UnixMilli()
		}
		
		start := time.Now()
		
		// 消费消息
		result := subscription.Listener.ConsumeMessage([]*MessageExt{msg})
		
		// 更新追踪信息
		if traceCtx != nil {
			traceCtx.Success = (result == ConsumeSuccess)
			traceCtx.CostTime = time.Since(start).Milliseconds()
			traceCtx.ContextCode = int(result)
			c.traceManager.TraceMessage(traceCtx)
		}
	}
}

// GetSubscriptions 获取订阅信息
func (c *Consumer) GetSubscriptions() map[string]*Subscription {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	
	result := make(map[string]*Subscription)
	for k, v := range c.subscriptions {
		result[k] = v
	}
	
	return result
}

// SetLoadBalanceStrategy 设置负载均衡策略
func (c *Consumer) SetLoadBalanceStrategy(strategy LoadBalanceStrategy) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.loadBalanceStrategy = strategy
}

// GetLoadBalanceStrategy 获取负载均衡策略
func (c *Consumer) GetLoadBalanceStrategy() LoadBalanceStrategy {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.loadBalanceStrategy
}

// SetRebalanceInterval 设置重平衡间隔
func (c *Consumer) SetRebalanceInterval(interval time.Duration) {
	if c.rebalanceService != nil {
		c.rebalanceService.rebalanceInterval = interval
	}
}

// TriggerRebalance 手动触发重平衡
func (c *Consumer) TriggerRebalance() {
	if c.rebalanceService != nil {
		c.rebalanceService.doRebalance()
	}
}

// EnableTrace 启用消息追踪
func (c *Consumer) EnableTrace(nameServerAddr string, traceTopic string) error {
	if c.traceManager == nil {
		return fmt.Errorf("trace manager not initialized")
	}
	
	// 创建追踪分发器
	dispatcher := NewDefaultTraceDispatcher(nameServerAddr, traceTopic)
	c.traceManager = NewTraceManager(dispatcher)
	c.traceManager.SetEnabled(true)
	
	// 如果消费者已启动，启动追踪管理器
	if c.IsStarted() {
		return c.traceManager.Start()
	}
	
	return nil
}

// DisableTrace 禁用消息追踪
func (c *Consumer) DisableTrace() {
	if c.traceManager != nil {
		c.traceManager.SetEnabled(false)
	}
}

// AddTraceHook 添加追踪钩子
func (c *Consumer) AddTraceHook(hook TraceHook) {
	if c.traceManager != nil {
		c.traceManager.AddHook(hook)
	}
}

// RemoveTraceHook 移除追踪钩子
func (c *Consumer) RemoveTraceHook(hookName string) {
	if c.traceManager != nil {
		c.traceManager.RemoveHook(hookName)
	}
}

// IsStarted 检查消费者是否已启动
func (c *Consumer) IsStarted() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	
	return c.started
}

// UpdateNameServerAddr 更新NameServer地址
func (c *Consumer) UpdateNameServerAddr() error {
	if c.config.NameServerAddr == "" {
		return fmt.Errorf("nameserver address not set")
	}
	
	log.Printf("Updating route info from NameServer: %s", c.config.NameServerAddr)
	
	// 更新NameServer地址列表
	c.nameServerAddrs = []string{c.config.NameServerAddr}
	
	// 获取所有已订阅Topic的路由信息
	c.mutex.RLock()
	subscriptions := make(map[string]*Subscription)
	for topic, sub := range c.subscriptions {
		subscriptions[topic] = sub
	}
	c.mutex.RUnlock()
	
	// 为每个订阅的Topic更新路由信息
	for topic := range subscriptions {
		routeData, err := c.getTopicRouteFromNameServer(topic)
		if err != nil {
			log.Printf("Failed to get route info for topic %s: %v", topic, err)
			continue
		}
		
		if routeData != nil {
			log.Printf("Successfully updated route info for topic %s: %d queues, %d brokers", 
				topic, len(routeData.QueueDatas), len(routeData.BrokerDatas))
		}
	}
	
	log.Printf("Route info update completed for %d topics", len(subscriptions))
	return nil
}

// RebalanceQueue 重新平衡队列
func (c *Consumer) RebalanceQueue() error {
	if c.config.MessageModel != Clustering {
		// 广播模式不需要重平衡
		return nil
	}
	
	log.Printf("Rebalancing queues for consumer group: %s", c.config.GroupName)
	
	// 获取所有订阅的Topic
	c.mutex.RLock()
	subscriptions := make(map[string]*Subscription)
	for topic, sub := range c.subscriptions {
		subscriptions[topic] = sub
	}
	c.mutex.RUnlock()
	
	// 为每个Topic执行重平衡
	for topic := range subscriptions {
		err := c.rebalanceTopicQueues(topic)
		if err != nil {
			log.Printf("Failed to rebalance queues for topic %s: %v", topic, err)
			continue
		}
	}
	
	log.Printf("Queue rebalance completed for %d topics", len(subscriptions))
	return nil
}

// rebalanceTopicQueues 重新平衡指定Topic的队列
func (c *Consumer) rebalanceTopicQueues(topic string) error {
	// 获取Topic的路由信息
	routeData, err := c.getTopicRouteFromNameServer(topic)
	if err != nil {
		return fmt.Errorf("failed to get route data for topic %s: %v", topic, err)
	}
	
	if routeData == nil || len(routeData.QueueDatas) == 0 {
		return fmt.Errorf("no queue data found for topic %s", topic)
	}
	
	// 构建所有消息队列列表
	mqAll := make([]*MessageQueue, 0)
	for _, queueData := range routeData.QueueDatas {
		for i := int32(0); i < queueData.ReadQueueNums; i++ {
			mq := &MessageQueue{
				Topic:      topic,
				BrokerName: queueData.BrokerName,
				QueueId:    i,
			}
			mqAll = append(mqAll, mq)
		}
	}
	
	// 模拟获取消费者组中的所有消费者ID
	cidAll := c.getConsumerGroupMembers()
	
	// 使用负载均衡策略分配队列
	if c.loadBalanceStrategy != nil {
		currentCID := c.getConsumerID()
		allocatedQueues := c.loadBalanceStrategy.Allocate(c.config.GroupName, currentCID, mqAll, cidAll)
		
		log.Printf("Topic %s rebalanced: allocated %d queues out of %d total queues", 
			topic, len(allocatedQueues), len(mqAll))
		
		// 这里可以进一步处理分配的队列，比如更新本地队列分配信息
		for _, mq := range allocatedQueues {
			log.Printf("Allocated queue: %s-%s-%d", mq.Topic, mq.BrokerName, mq.QueueId)
		}
	} else {
		log.Printf("No load balance strategy set, using default allocation for topic %s", topic)
	}
	
	return nil
}

// getConsumerGroupMembers 获取消费者组成员列表
func (c *Consumer) getConsumerGroupMembers() []string {
	// 简化实现，实际应该从Broker获取消费者组的所有成员
	// 这里返回模拟数据
	return []string{c.getConsumerID(), "consumer-2", "consumer-3"}
}

// getConsumerID 获取当前消费者ID
func (c *Consumer) getConsumerID() string {
	// 简化实现，实际应该是唯一的消费者标识
	return fmt.Sprintf("%s@%s", c.config.GroupName, "localhost")
}

// initACLMiddleware 初始化ACL中间件
func (c *Consumer) initACLMiddleware() error {
	if !c.config.EnableACL {
		return nil
	}
	
	// 从配置文件加载ACL配置
	if c.config.ACLConfigPath != "" {
		aclMiddleware, err := NewACLMiddlewareFromConfig(c.config.ACLConfigPath, c.config.ACLHotReload)
		if err != nil {
			return fmt.Errorf("failed to load ACL config from file: %v", err)
		}
		c.aclMiddleware = aclMiddleware
	} else {
		// 从配置字段创建ACL中间件
		signatureMethod := HmacSHA1
		if c.config.SignatureMethod != "" {
			signatureMethod = ACLSignatureMethod(c.config.SignatureMethod)
		}
		c.aclMiddleware = NewACLMiddleware(c.config.AccessKey, c.config.SecretKey, signatureMethod, true)
	}
	
	return nil
}

// SetACLConfig 设置ACL配置
func (c *Consumer) SetACLConfig(accessKey, secretKey string, signatureMethod ACLSignatureMethod) error {
	c.aclMiddleware = NewACLMiddleware(accessKey, secretKey, signatureMethod, true)
	c.config.EnableACL = true
	c.config.AccessKey = accessKey
	c.config.SecretKey = secretKey
	c.config.SignatureMethod = string(signatureMethod)
	
	return nil
}

// EnableACL 启用ACL认证
func (c *Consumer) EnableACL(accessKey, secretKey string) error {
	return c.SetACLConfig(accessKey, secretKey, HmacSHA1)
}

// DisableACL 禁用ACL认证
func (c *Consumer) DisableACL() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	c.config.EnableACL = false
	c.aclMiddleware = nil
}

// IsACLEnabled 检查是否启用了ACL认证
func (c *Consumer) IsACLEnabled() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.config.EnableACL && c.aclMiddleware != nil
}

// verifyMessageACL 验证消息的ACL权限
func (c *Consumer) verifyMessageACL(msg *MessageExt) bool {
	if c.aclMiddleware == nil || !c.aclMiddleware.IsEnabled() {
		return true // 如果没有ACL中间件或未启用，默认通过
	}
	
	// 构造认证头信息进行验证
	authHeaders, err := c.aclMiddleware.GenerateAuthHeaders(msg.Topic, c.config.GroupName, msg.MsgId)
	if err != nil {
		log.Printf("Failed to generate auth headers for message verification: %v", err)
		return false
	}
	
	// 验证认证头信息
	err = c.aclMiddleware.VerifyAuthHeaders(authHeaders, msg.Topic, c.config.GroupName, msg.MsgId)
	if err != nil {
		log.Printf("ACL verification failed for message %s: %v", msg.MsgId, err)
		return false
	}
	
	return true
}

// getConsumeOffset 获取消费进度
func (c *Consumer) getConsumeOffset(mq *MessageQueue) (int64, error) {
	c.offsetMutex.RLock()
	defer c.offsetMutex.RUnlock()
	
	key := fmt.Sprintf("%s-%s-%d", mq.Topic, mq.BrokerName, mq.QueueId)
	if offset, ok := c.consumeOffsets[key]; ok {
		return offset, nil
	}
	return 0, nil
}

// updateConsumeOffset 更新消费进度
func (c *Consumer) updateConsumeOffset(mq *MessageQueue, offset int64) {
	c.offsetMutex.Lock()
	defer c.offsetMutex.Unlock()
	
	if c.consumeOffsets == nil {
		c.consumeOffsets = make(map[string]int64)
	}
	
	key := fmt.Sprintf("%s-%s-%d", mq.Topic, mq.BrokerName, mq.QueueId)
	c.consumeOffsets[key] = offset
}

// pullMessagesFromBroker 从Broker拉取消息
func (c *Consumer) pullMessagesFromBroker(mq *MessageQueue, offset int64) ([]*MessageExt, int64, error) {
	// 1. 获取Broker地址
	brokerAddr, err := c.getBrokerAddr(mq.BrokerName)
	if err != nil {
		return nil, offset, fmt.Errorf("failed to get broker address: %v", err)
	}
	
	// 2. 从连接池获取连接
	conn, err := c.connectionPool.GetConnection(brokerAddr)
	if err != nil {
		return nil, offset, fmt.Errorf("failed to get connection to broker: %v", err)
	}
	defer c.connectionPool.ReleaseConnection(brokerAddr, conn)
	
	// 3. 构造拉取消息请求
	request := &RemotingCommand{
		Code:      11, // PULL_MESSAGE
		Language:  "JAVA",
		Version:   VERSION,
		Opaque:    atomic.AddInt32(&requestId, 1),
		Flag:      0,
		Remark:    "",
		ExtFields: make(map[string]string),
		Body:      nil,
	}
	
	// 4. 设置请求头参数
	request.ExtFields["consumerGroup"] = c.config.GroupName
	request.ExtFields["topic"] = mq.Topic
	request.ExtFields["queueId"] = fmt.Sprintf("%d", mq.QueueId)
	request.ExtFields["queueOffset"] = fmt.Sprintf("%d", offset)
	request.ExtFields["maxMsgNums"] = fmt.Sprintf("%d", c.config.PullBatchSize)
	request.ExtFields["sysFlag"] = "0"
	request.ExtFields["commitOffset"] = fmt.Sprintf("%d", offset)
	request.ExtFields["suspendTimeoutMillis"] = "15000"
	request.ExtFields["subscription"] = "*"
	request.ExtFields["subVersion"] = fmt.Sprintf("%d", time.Now().Unix())
	request.ExtFields["expressionType"] = "TAG"
	
	// 5. 发送请求
	err = c.sendRequest(conn, request)
	if err != nil {
		return nil, offset, fmt.Errorf("failed to send pull request: %v", err)
	}
	
	// 6. 接收响应
	response, err := c.receiveResponse(conn)
	if err != nil {
		return nil, offset, fmt.Errorf("failed to receive pull response: %v", err)
	}
	
	// 7. 解析响应
	if response.Code != 0 { // SUCCESS
		// 如果是PullNotFound(19)，返回空消息列表
		if response.Code == 19 {
			return []*MessageExt{}, offset, nil
		}
		return nil, offset, fmt.Errorf("pull message failed with code %d: %s", response.Code, response.Remark)
	}
	
	// 8. 解析消息数据
	messages, nextOffset, err := c.parseMessageResponse(response, mq)
	if err != nil {
		return nil, offset, fmt.Errorf("failed to parse message response: %v", err)
	}
	
	return messages, nextOffset, nil
}

// getBrokerAddr 获取Broker地址
func (c *Consumer) getBrokerAddr(brokerName string) (string, error) {
	// 简化实现：直接返回默认的Broker地址
	// 实际实现应该从路由信息中获取
	if len(c.nameServerAddrs) > 0 {
		// 使用NameServer地址作为Broker地址（简化实现）
		return c.nameServerAddrs[0], nil
	}
	return "127.0.0.1:10911", nil // 默认Broker地址
}

// parseMessageResponse 解析消息响应
func (c *Consumer) parseMessageResponse(response *RemotingCommand, mq *MessageQueue) ([]*MessageExt, int64, error) {
	// 从响应头中获取nextBeginOffset
	nextOffsetStr, exists := response.ExtFields["nextBeginOffset"]
	if !exists {
		return nil, 0, fmt.Errorf("nextBeginOffset not found in response")
	}
	
	nextOffset := int64(0)
	if nextOffsetStr != "" {
		if parsed, err := fmt.Sscanf(nextOffsetStr, "%d", &nextOffset); err != nil || parsed != 1 {
			log.Printf("Failed to parse nextBeginOffset: %s", nextOffsetStr)
		}
	}
	
	// 如果没有消息体，返回空消息列表
	if response.Body == nil || len(response.Body) == 0 {
		return []*MessageExt{}, nextOffset, nil
	}
	
	// 尝试多种消息解析格式
	messages, err := c.parseMessagesFromBody(response.Body, mq)
	if err != nil {
		return nil, nextOffset, fmt.Errorf("failed to parse message body: %v", err)
	}
	
	return messages, nextOffset, nil
}

// parseMessagesFromBody 从响应体中解析消息，支持多种格式
func (c *Consumer) parseMessagesFromBody(body []byte, mq *MessageQueue) ([]*MessageExt, error) {
	// 尝试JSON格式解析
	if messages, err := c.parseJSONMessages(body); err == nil {
		return messages, nil
	}
	
	// 尝试二进制格式解析
	if messages, err := c.parseBinaryMessages(body, mq); err == nil {
		return messages, nil
	}
	
	// 尝试简单文本格式解析
	if messages, err := c.parseTextMessages(body, mq); err == nil {
		return messages, nil
	}
	
	return nil, fmt.Errorf("unsupported message format")
}

// parseJSONMessages 解析JSON格式的消息
func (c *Consumer) parseJSONMessages(body []byte) ([]*MessageExt, error) {
	var messages []*MessageExt
	err := json.Unmarshal(body, &messages)
	if err != nil {
		// 尝试解析单个消息
		var singleMessage MessageExt
		if singleErr := json.Unmarshal(body, &singleMessage); singleErr == nil {
			return []*MessageExt{&singleMessage}, nil
		}
		return nil, err
	}
	return messages, nil
}

// parseBinaryMessages 解析二进制格式的消息（类似RocketMQ的序列化格式）
func (c *Consumer) parseBinaryMessages(body []byte, mq *MessageQueue) ([]*MessageExt, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("binary data too short")
	}
	
	var messages []*MessageExt
	offset := 0
	
	for offset < len(body) {
		// 检查剩余数据长度
		if offset+4 > len(body) {
			break
		}
		
		// 读取消息长度（4字节）
		msgLen := int(body[offset])<<24 | int(body[offset+1])<<16 | int(body[offset+2])<<8 | int(body[offset+3])
		offset += 4
		
		// 检查消息长度是否有效
		if msgLen <= 0 || offset+msgLen > len(body) {
			break
		}
		
		// 提取消息数据
		msgData := body[offset : offset+msgLen]
		offset += msgLen
		
		// 解析消息
		msg, err := c.deserializeMessage(msgData, mq)
		if err != nil {
			continue // 跳过无法解析的消息
		}
		
		messages = append(messages, msg)
	}
	
	if len(messages) == 0 {
		return nil, fmt.Errorf("no valid messages found in binary data")
	}
	
	return messages, nil
}

// parseTextMessages 解析简单文本格式的消息
func (c *Consumer) parseTextMessages(body []byte, mq *MessageQueue) ([]*MessageExt, error) {
	// 将整个body作为一个消息处理
	msg := &MessageExt{
		Message: &Message{
			Topic:      mq.Topic,
			Body:       body,
			Properties: make(map[string]string),
		},
		MsgId:     fmt.Sprintf("text-msg-%d", time.Now().UnixNano()),
		QueueId:   mq.QueueId,
		StoreSize: int32(len(body)),
	}
	
	// 尝试从body中提取更多信息
	if len(body) > 0 {
		// 检查是否包含消息ID
		bodyStr := string(body)
		if strings.Contains(bodyStr, "msgId:") {
			lines := strings.Split(bodyStr, "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "msgId:") {
					msg.MsgId = strings.TrimSpace(strings.TrimPrefix(line, "msgId:"))
				}
			}
		}
	}
	
	return []*MessageExt{msg}, nil
}

// deserializeMessage 反序列化单个消息
func (c *Consumer) deserializeMessage(data []byte, mq *MessageQueue) (*MessageExt, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("message data too short")
	}
	
	// 简化的反序列化逻辑
	// 实际实现应该按照RocketMQ的消息格式进行解析
	offset := 0
	
	// 读取消息体长度（4字节）
	bodyLen := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4
	
	if bodyLen <= 0 || offset+bodyLen > len(data) {
		return nil, fmt.Errorf("invalid body length")
	}
	
	// 提取消息体
	body := data[offset : offset+bodyLen]
	offset += bodyLen
	
	// 创建消息对象
	msg := &MessageExt{
		Message: &Message{
			Topic:      mq.Topic,
			Body:       body,
			Properties: make(map[string]string),
		},
		MsgId:     fmt.Sprintf("binary-msg-%d", time.Now().UnixNano()),
		QueueId:   mq.QueueId,
		StoreSize: int32(len(data)),
	}
	
	// 尝试读取更多字段（如果数据足够）
	if offset+8 <= len(data) {
		// 读取队列偏移量（8字节）
		queueOffset := int64(data[offset])<<56 | int64(data[offset+1])<<48 |
			int64(data[offset+2])<<40 | int64(data[offset+3])<<32 |
			int64(data[offset+4])<<24 | int64(data[offset+5])<<16 |
			int64(data[offset+6])<<8 | int64(data[offset+7])
		msg.QueueOffset = queueOffset
	}
	
	return msg, nil
}

// sendRequest 发送请求到服务器
func (c *Consumer) sendRequest(conn net.Conn, request *RemotingCommand) error {
	// 序列化请求
	data, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}
	
	// 发送数据长度
	length := int32(len(data))
	lengthBytes := make([]byte, 4)
	lengthBytes[0] = byte(length >> 24)
	lengthBytes[1] = byte(length >> 16)
	lengthBytes[2] = byte(length >> 8)
	lengthBytes[3] = byte(length)
	
	_, err = conn.Write(lengthBytes)
	if err != nil {
		return fmt.Errorf("failed to write length: %v", err)
	}
	
	// 发送数据
	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}
	
	return nil
}

// receiveResponse 接收服务器响应
func (c *Consumer) receiveResponse(conn net.Conn) (*RemotingCommand, error) {
	// 读取响应长度
	lengthBytes := make([]byte, 4)
	_, err := conn.Read(lengthBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read length: %v", err)
	}
	
	length := int32(lengthBytes[0])<<24 | int32(lengthBytes[1])<<16 | int32(lengthBytes[2])<<8 | int32(lengthBytes[3])
	
	// 读取响应数据
	data := make([]byte, length)
	_, err = conn.Read(data)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %v", err)
	}
	
	// 反序列化响应
	var response RemotingCommand
	err = json.Unmarshal(data, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}
	
	return &response, nil
}

// commitConsumeProgress 提交消费进度到Broker
func (c *Consumer) commitConsumeProgress(messages []*MessageExt) error {
	if len(messages) == 0 {
		return nil
	}
	
	// 按照队列分组消息
	queueOffsets := make(map[string]int64)
	for _, msg := range messages {
		// 从消息中获取队列信息，这里简化处理，使用Topic和QueueId
		key := fmt.Sprintf("%s-%d", msg.Topic, msg.QueueId)
		if offset, ok := queueOffsets[key]; !ok || msg.QueueOffset > offset {
			queueOffsets[key] = msg.QueueOffset + 1 // 下一个要消费的偏移量
		}
	}
	
	// 为每个队列提交消费进度
	for key, offset := range queueOffsets {
		err := c.commitOffsetToBroker(key, offset)
		if err != nil {
			log.Printf("Failed to commit offset for queue %s: %v", key, err)
			// 继续处理其他队列，不因为一个失败而全部失败
		}
	}
	
	return nil
}

// commitOffsetToBroker 向Broker提交指定队列的消费进度
func (c *Consumer) commitOffsetToBroker(queueKey string, offset int64) error {
	if c.config.NameServerAddr == "" {
		return fmt.Errorf("nameserver address not set")
	}
	
	// 从连接池获取连接到Broker (这里简化为连接NameServer)
	conn, err := c.connectionPool.GetConnection(c.config.NameServerAddr)
	if err != nil {
		return fmt.Errorf("failed to get connection to broker: %v", err)
	}
	defer c.connectionPool.ReleaseConnection(c.config.NameServerAddr, conn)
	
	// 构造提交消费进度的请求
	request := &RemotingCommand{
		Code:     UPDATE_CONSUMER_OFFSET,
		Version:  VERSION,
		Opaque:   atomic.AddInt32(&requestId, 1),
		Flag:     0,
		Remark:   "",
		ExtFields: map[string]string{
			"consumerGroup": c.config.GroupName,
			"queueKey":     queueKey,
			"commitOffset": fmt.Sprintf("%d", offset),
		},
	}
	
	// 发送请求
	err = c.sendRequest(conn, request)
	if err != nil {
		return fmt.Errorf("failed to send commit offset request: %v", err)
	}
	
	// 接收响应
	response, err := c.receiveResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to receive commit offset response: %v", err)
	}
	
	if response.Code != SUCCESS {
		return fmt.Errorf("broker returned error when committing offset: %s", response.Remark)
	}
	
	log.Printf("Successfully committed offset %d for queue %s", offset, queueKey)
	return nil
}

// retryMessages 重试消息处理
func (c *Consumer) retryMessages(messages []*MessageExt) error {
	if len(messages) == 0 {
		return nil
	}
	
	for _, msg := range messages {
		// 增加重试次数
		msg.ReconsumeTimes++
		
		// 检查是否超过最大重试次数
		maxRetryTimes := int32(16) // RocketMQ默认最大重试16次
		if msg.ReconsumeTimes >= maxRetryTimes {
			log.Printf("Message %s exceeded max retry times (%d), sending to DLQ", msg.MsgId, maxRetryTimes)
			err := c.sendToDLQ(msg)
			if err != nil {
				log.Printf("Failed to send message %s to DLQ: %v", msg.MsgId, err)
			}
			continue
		}
		
		// 计算延迟时间 (指数退避)
		delayLevel := c.calculateDelayLevel(msg.ReconsumeTimes)
		delayTime := c.calculateDelayTime(delayLevel)
		
		log.Printf("Retrying message %s (attempt %d) with delay %v", msg.MsgId, msg.ReconsumeTimes, delayTime)
		
		// 延迟重新投递消息
		go func(message *MessageExt, delay time.Duration) {
			time.Sleep(delay)
			err := c.redeliverMessage(message)
			if err != nil {
				log.Printf("Failed to redeliver message %s: %v", message.MsgId, err)
			}
		}(msg, delayTime)
	}
	
	return nil
}

// calculateDelayLevel 计算延迟级别
func (c *Consumer) calculateDelayLevel(retryTimes int32) int32 {
	// RocketMQ延迟级别: 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
	if retryTimes <= 0 {
		return 1 // 1s
	}
	if retryTimes >= 18 {
		return 18 // 2h
	}
	return retryTimes
}

// calculateDelayTime 根据延迟级别计算延迟时间
func (c *Consumer) calculateDelayTime(delayLevel int32) time.Duration {
	delayTimes := []time.Duration{
		1 * time.Second,   // level 1
		5 * time.Second,   // level 2
		10 * time.Second,  // level 3
		30 * time.Second,  // level 4
		1 * time.Minute,   // level 5
		2 * time.Minute,   // level 6
		3 * time.Minute,   // level 7
		4 * time.Minute,   // level 8
		5 * time.Minute,   // level 9
		6 * time.Minute,   // level 10
		7 * time.Minute,   // level 11
		8 * time.Minute,   // level 12
		9 * time.Minute,   // level 13
		10 * time.Minute,  // level 14
		20 * time.Minute,  // level 15
		30 * time.Minute,  // level 16
		1 * time.Hour,     // level 17
		2 * time.Hour,     // level 18
	}
	
	if delayLevel <= 0 || int(delayLevel) > len(delayTimes) {
		return 1 * time.Second
	}
	return delayTimes[delayLevel-1]
}

// sendToDLQ 发送消息到死信队列
func (c *Consumer) sendToDLQ(msg *MessageExt) error {
	// 构造死信队列Topic名称
	dlqTopic := fmt.Sprintf("%%DLQ%%_%s", c.config.GroupName)
	
	// 创建死信消息
	dlqMsg := &Message{
		Topic:      dlqTopic,
		Body:       msg.Body,
		Properties: make(map[string]string),
	}
	
	// 复制原消息属性
	for k, v := range msg.Properties {
		dlqMsg.Properties[k] = v
	}
	
	// 添加死信队列相关属性
	dlqMsg.Properties["ORIGIN_TOPIC"] = msg.Topic
	dlqMsg.Properties["ORIGIN_MSG_ID"] = msg.MsgId
	dlqMsg.Properties["RETRY_TIMES"] = fmt.Sprintf("%d", msg.ReconsumeTimes)
	dlqMsg.Properties["DLQ_ORIGIN_QUEUE_ID"] = fmt.Sprintf("%d", msg.QueueId)
	dlqMsg.Properties["DLQ_ORIGIN_BROKER"] = msg.StoreHost
	
	log.Printf("Sending message %s to DLQ topic %s after %d retries", msg.MsgId, dlqTopic, msg.ReconsumeTimes)
	
	// 实现真实的DLQ发送逻辑
	return c.sendMessageToBroker(dlqMsg, dlqTopic)
}

// sendMessageToBroker 发送消息到Broker
func (c *Consumer) sendMessageToBroker(msg *Message, topic string) error {
	// 获取Topic路由信息
	routeData, err := c.getTopicRouteFromNameServer(topic)
	if err != nil {
		return fmt.Errorf("failed to get route for topic %s: %v", topic, err)
	}
	
	// 选择消息队列
	mq := c.selectMessageQueue(routeData, topic)
	if mq == nil {
		return fmt.Errorf("no available message queue for topic %s", topic)
	}
	
	// 获取Broker地址
	brokerAddr, err := c.getBrokerAddr(mq.BrokerName)
	if err != nil {
		return fmt.Errorf("failed to get broker address for %s: %v", mq.BrokerName, err)
	}
	
	// 构建发送请求
	request := &RemotingCommand{
		Code:      10, // SEND_MESSAGE
		Language:  "JAVA",
		Version:   VERSION,
		Opaque:    atomic.AddInt32(&requestId, 1),
		Flag:      0,
		ExtFields: make(map[string]string),
		Body:      msg.Body,
	}
	
	// 设置扩展字段
	request.ExtFields["topic"] = msg.Topic
	request.ExtFields["queueId"] = fmt.Sprintf("%d", mq.QueueId)
	request.ExtFields["sysFlag"] = "0"
	request.ExtFields["bornTimestamp"] = fmt.Sprintf("%d", time.Now().UnixMilli())
	request.ExtFields["flag"] = "0"
	request.ExtFields["properties"] = c.encodeProperties(msg.Properties)
	
	// 添加ACL认证头
	if c.aclMiddleware != nil {
		authHeaders, err := c.aclMiddleware.GenerateAuthHeaders(msg.Topic, c.config.GroupName, "")
		if err != nil {
			return fmt.Errorf("failed to generate ACL auth headers: %v", err)
		}
		for k, v := range authHeaders {
			request.ExtFields[k] = v
		}
	}
	
	// 发送请求到Broker
	conn, err := c.connectionPool.GetConnection(brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to get connection to broker %s: %v", brokerAddr, err)
	}
	defer c.connectionPool.ReleaseConnection(brokerAddr, conn)
	
	// 发送请求
	if err := c.sendRequest(conn, request); err != nil {
		return fmt.Errorf("failed to send request to broker: %v", err)
	}
	
	// 接收响应
	response, err := c.receiveResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to receive response from broker: %v", err)
	}
	
	// 检查响应状态
	if response.Code != 0 {
		return fmt.Errorf("broker returned error code %d: %s", response.Code, response.Remark)
	}
	
	log.Printf("Successfully sent message to DLQ topic %s", topic)
	return nil
}

// encodeProperties 编码消息属性
func (c *Consumer) encodeProperties(properties map[string]string) string {
	if len(properties) == 0 {
		return ""
	}
	
	var parts []string
	for k, v := range properties {
		parts = append(parts, fmt.Sprintf("%s%c%s", k, 1, v))
	}
	return strings.Join(parts, string(rune(2)))
}

// redeliverMessage 重新投递消息
func (c *Consumer) redeliverMessage(msg *MessageExt) error {
	log.Printf("Redelivering message %s (attempt %d)", msg.MsgId, msg.ReconsumeTimes)
	// 这里应该实现重新投递消息的逻辑
	// 简化实现：重新调用消息处理
	if subscription, ok := c.subscriptions[msg.Topic]; ok {
		if subscription.Listener != nil {
			result := subscription.Listener.ConsumeMessage([]*MessageExt{msg})
			if result == ConsumeSuccess {
				log.Printf("Message %s redelivered successfully", msg.MsgId)
				return c.commitConsumeProgress([]*MessageExt{msg})
			} else {
				log.Printf("Message %s redelivery failed, will retry again", msg.MsgId)
				return c.retryMessages([]*MessageExt{msg})
			}
		}
	}
	return fmt.Errorf("no listener found for topic %s", msg.Topic)
}

// retryMessageToQueue 重新投递消息到队列
func (c *Consumer) retryMessageToQueue(msg *MessageExt, mq *MessageQueue, delayLevel int) error {
	// 构造重试Topic名称
	retryTopic := fmt.Sprintf("%%RETRY%%_%s", c.config.GroupName)
	
	// 创建重试消息
	retryMsg := &Message{
		Topic:      retryTopic,
		Body:       msg.Body,
		Properties: make(map[string]string),
	}
	
	// 复制原消息属性
	for k, v := range msg.Properties {
		retryMsg.Properties[k] = v
	}
	
	// 添加重试相关属性
	retryMsg.Properties["ORIGIN_TOPIC"] = msg.Topic
	retryMsg.Properties["ORIGIN_MSG_ID"] = msg.MsgId
	retryMsg.Properties["RETRY_TIMES"] = fmt.Sprintf("%d", msg.ReconsumeTimes)
	retryMsg.Properties["DELAY_LEVEL"] = fmt.Sprintf("%d", delayLevel)
	
	log.Printf("Retrying message %s to topic %s with delay level %d (attempt %d)", 
		msg.MsgId, retryTopic, delayLevel, msg.ReconsumeTimes)
	
	// 这里简化实现，实际应该发送到Broker的重试队列
	// TODO: 实现真实的重试消息发送逻辑
	return nil
}

// HeartbeatData 心跳数据结构
type HeartbeatData struct {
	ClientID      string          `json:"clientID"`
	ProducerData  []ProducerData  `json:"producerDataSet"`
	ConsumerData  []ConsumerData  `json:"consumerDataSet"`
}

// ProducerData 生产者数据
type ProducerData struct {
	GroupName string `json:"groupName"`
}

// ConsumerData 消费者数据
type ConsumerData struct {
	GroupName         string             `json:"groupName"`
	ConsumeType       string             `json:"consumeType"`
	MessageModel      string             `json:"messageModel"`
	ConsumeFromWhere  string             `json:"consumeFromWhere"`
	SubscriptionData  []SubscriptionData `json:"subscriptionDataSet"`
	UnitMode          bool               `json:"unitMode"`
}



// startHeartbeat 启动心跳机制
func (c *Consumer) startHeartbeat() {
	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
	}
	
	// 每30秒发送一次心跳
	c.heartbeatTicker = time.NewTicker(30 * time.Second)
	
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer c.heartbeatTicker.Stop()
		
		for {
			select {
			case <-c.shutdown:
				return
			case <-c.heartbeatTicker.C:
				err := c.sendHeartbeatToBroker()
				if err != nil {
					log.Printf("Failed to send heartbeat: %v", err)
				}
			}
		}
	}()
	
	log.Printf("Heartbeat mechanism started for consumer group: %s", c.config.GroupName)
}

// stopHeartbeat 停止心跳机制
func (c *Consumer) stopHeartbeat() {
	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
		c.heartbeatTicker = nil
	}
	log.Printf("Heartbeat mechanism stopped for consumer group: %s", c.config.GroupName)
}

// sendHeartbeatToBroker 向Broker发送心跳
func (c *Consumer) sendHeartbeatToBroker() error {
	// 构造心跳数据
	heartbeatData := c.buildHeartbeatData()
	
	// 序列化心跳数据
	heartbeatBody, err := json.Marshal(heartbeatData)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat data: %v", err)
	}
	
	// 获取Broker地址列表
	brokerAddrs := c.getAllBrokerAddrs()
	if len(brokerAddrs) == 0 {
		return fmt.Errorf("no broker addresses available")
	}
	
	// 向所有Broker发送心跳
	var lastErr error
	for _, brokerAddr := range brokerAddrs {
		err := c.sendHeartbeatToBrokerAddr(brokerAddr, heartbeatBody)
		if err != nil {
			log.Printf("Failed to send heartbeat to broker %s: %v", brokerAddr, err)
			lastErr = err
		} else {
			log.Printf("Successfully sent heartbeat to broker %s", brokerAddr)
		}
	}
	
	return lastErr
}

// buildHeartbeatData 构造心跳数据
func (c *Consumer) buildHeartbeatData() *HeartbeatData {
	// 构造消费者数据
	consumerData := ConsumerData{
		GroupName:        c.config.GroupName,
		ConsumeType:      "CONSUME_PASSIVELY", // 被动消费
		MessageModel:     string(c.config.MessageModel),
		ConsumeFromWhere: string(c.config.ConsumeFromWhere),
		SubscriptionData: c.buildSubscriptionData(),
		UnitMode:         false,
	}
	
	// 构造心跳数据
	heartbeatData := &HeartbeatData{
		ClientID:     c.getClientID(),
		ProducerData: []ProducerData{}, // 消费者没有生产者数据
		ConsumerData: []ConsumerData{consumerData},
	}
	
	return heartbeatData
}

// buildSubscriptionData 构造订阅数据
func (c *Consumer) buildSubscriptionData() []SubscriptionData {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	
	subscriptionData := make([]SubscriptionData, 0, len(c.subscriptions))
	for topic, subscription := range c.subscriptions {
		data := SubscriptionData{
			Topic:           topic,
			SubString:       subscription.SubExpression,
			ClassFilterMode: false,
			TagsSet:         c.parseTagsFromExpression(subscription.SubExpression),
			CodeSet:         []int32{},
			SubVersion:      time.Now().Unix(),
			ExpressionType:  "TAG",
		}
		subscriptionData = append(subscriptionData, data)
	}
	
	return subscriptionData
}

// parseTagsFromExpression 从表达式中解析标签
func (c *Consumer) parseTagsFromExpression(expression string) []string {
	if expression == "" || expression == "*" {
		return []string{"*"}
	}
	
	// 简单的标签解析，支持 "tag1||tag2||tag3" 格式
	tags := make([]string, 0)
	if expression != "" {
		// 按 "||" 分割标签
		tagList := strings.Split(expression, "||")
		for _, tag := range tagList {
			tag = strings.TrimSpace(tag)
			if tag != "" {
				tags = append(tags, tag)
			}
		}
	}
	
	if len(tags) == 0 {
		tags = []string{"*"}
	}
	
	return tags
}

// sendHeartbeatToBrokerAddr 向指定Broker地址发送心跳
func (c *Consumer) sendHeartbeatToBrokerAddr(brokerAddr string, heartbeatBody []byte) error {
	// 从连接池获取连接
	conn, err := c.connectionPool.GetConnection(brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to get connection to broker %s: %v", brokerAddr, err)
	}
	defer c.connectionPool.ReleaseConnection(brokerAddr, conn)
	
	// 构造心跳请求
	request := &RemotingCommand{
		Code:      HEART_BEAT,
		Language:  "JAVA",
		Version:   VERSION,
		Opaque:    atomic.AddInt32(&requestId, 1),
		Flag:      0,
		Remark:    "",
		ExtFields: make(map[string]string),
		Body:      heartbeatBody,
	}
	
	// 添加客户端ID到扩展字段
	request.ExtFields["clientID"] = c.getClientID()
	
	// 发送请求
	err = c.sendRequest(conn, request)
	if err != nil {
		return fmt.Errorf("failed to send heartbeat request: %v", err)
	}
	
	// 接收响应
	response, err := c.receiveResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to receive heartbeat response: %v", err)
	}
	
	if response.Code != SUCCESS {
		return fmt.Errorf("broker returned error for heartbeat: %s", response.Remark)
	}
	
	return nil
}

// getAllBrokerAddrs 获取所有Broker地址
func (c *Consumer) getAllBrokerAddrs() []string {
	// 简化实现：返回NameServer地址作为Broker地址
	// 实际实现应该从路由信息中获取所有Broker地址
	if len(c.nameServerAddrs) > 0 {
		return c.nameServerAddrs
	}
	return []string{"127.0.0.1:10911"} // 默认Broker地址
}

// getClientID 获取客户端ID
func (c *Consumer) getClientID() string {
	if c.clientID == "" {
		// 生成唯一的客户端ID
		hostname, _ := os.Hostname()
		pid := os.Getpid()
		timestamp := time.Now().UnixNano()
		c.clientID = fmt.Sprintf("%s@%d@%d", hostname, pid, timestamp)
	}
	return c.clientID
}