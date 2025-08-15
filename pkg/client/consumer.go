package client

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go-rocketmq/pkg/common"
)

// Consumer 消费者客户端
type Consumer struct {
	config          *ConsumerConfig
	nameServerAddrs []string
	subscriptions   map[string]*Subscription
	messageListener common.MessageListener
	mutex           sync.RWMutex
	started         bool
	shutdown        chan struct{}
	wg              sync.WaitGroup
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	GroupName            string                      // 消费者组名
	NameServerAddr       string                      // NameServer地址
	ConsumeFromWhere     common.ConsumeFromWhere     // 消费起始位置
	MessageModel         common.MessageModel         // 消息模式
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
	Listener   common.MessageListener
}

// NewConsumer 创建新的消费者
func NewConsumer(config *ConsumerConfig) *Consumer {
	if config == nil {
		config = DefaultConsumerConfig()
	}
	
	return &Consumer{
		config:        config,
		subscriptions: make(map[string]*Subscription),
		shutdown:      make(chan struct{}),
	}
}

// DefaultConsumerConfig 返回默认消费者配置
func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		GroupName:        "DefaultConsumerGroup",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: common.ConsumeFromFirstOffset,
		MessageModel:     common.Clustering,
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
func (c *Consumer) Subscribe(topic, subExpression string, listener common.MessageListener) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if c.started {
		return fmt.Errorf("consumer already started, cannot subscribe new topic")
	}
	
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	
	if listener == nil {
		return fmt.Errorf("listener cannot be nil")
	}
	
	if _, exists := c.subscriptions[topic]; exists {
		return fmt.Errorf("topic %s already subscribed", topic)
	}
	
	subscription := &Subscription{
		Topic:         topic,
		SubExpression: subExpression,
		Listener:      listener,
	}
	
	c.subscriptions[topic] = subscription
	log.Printf("Subscribed to topic: %s with expression: %s", topic, subExpression)
	
	return nil
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
		c.consumeMessages(messages, subscription.Listener)
	}
}

// mockPullMessages 模拟拉取消息（用于测试）
func (c *Consumer) mockPullMessages(topic string) []*common.MessageExt {
	// 这是一个模拟实现，实际应该从Broker拉取
	return []*common.MessageExt{
		{
			Message: &common.Message{
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
func (c *Consumer) consumeMessages(messages []*common.MessageExt, listener common.MessageListener) {
	if listener == nil {
		log.Printf("No listener found for messages, skipping %d messages", len(messages))
		return
	}
	
	// 调用消息监听器处理消息
	result := listener.ConsumeMessage(messages)
	
	switch result {
	case common.ConsumeSuccess:
		log.Printf("Successfully consumed %d messages", len(messages))
		// TODO: 提交消费进度到Broker
	case common.ReconsumeLater:
		log.Printf("Failed to consume %d messages, will retry later", len(messages))
		// TODO: 将消息重新放回队列或延迟重试
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