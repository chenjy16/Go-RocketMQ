package client

import (
	"fmt"
	"log"
	"sync"
	"time"
)

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
	
	// 停止重平衡服务
	c.rebalanceService.Stop()
	
	// 停止消息追踪管理器
	if c.traceManager != nil {
		c.traceManager.Stop()
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
	// TODO: 实现从Broker拉取消息的逻辑
	// 这里应该：
	// 1. 从NameServer获取Topic的路由信息
	// 2. 选择合适的Broker和队列
	// 3. 发送拉取请求到Broker
	// 4. 处理返回的消息
	
	// 简化版本：模拟拉取到消息
	messages := c.mockPullMessages(topic)
	
	if len(messages) > 0 {
		c.consumeMessagesWithFilter(messages, subscription)
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
	
	// 调用消息监听器处理消息
	result := listener.ConsumeMessage(messages)
	
	switch result {
	case ConsumeSuccess:
		log.Printf("Successfully consumed %d messages", len(messages))
		// TODO: 提交消费进度到Broker
	case ReconsumeLater:
		log.Printf("Failed to consume %d messages, will retry later", len(messages))
		// TODO: 将消息重新放回队列或延迟重试
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
	// TODO: 实现从NameServer更新路由信息的逻辑
	log.Printf("Updating route info from NameServer: %s", c.config.NameServerAddr)
	return nil
}

// RebalanceQueue 重新平衡队列
func (c *Consumer) RebalanceQueue() error {
	// TODO: 实现队列重新平衡逻辑
	// 这在集群模式下很重要，需要在消费者组内分配队列
	log.Printf("Rebalancing queues for consumer group: %s", c.config.GroupName)
	return nil
}