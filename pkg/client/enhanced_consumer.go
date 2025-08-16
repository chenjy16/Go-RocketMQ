package client

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// DLQMode 死信队列模式：Mock 或 Real Broker
type DLQMode int

const (
	DLQModeMock DLQMode = iota
	DLQModeReal
)

// PushConsumer Push模式消费者
type PushConsumer struct {
	*Consumer
	messageListener     MessageListener
	consumeType         ConsumeType
	pullInterval        time.Duration
	pullBatchSize       int32
	concurrentlyMaxSpan int32
	consumeFromWhere    ConsumeFromWhere
	consumeTimestamp    string
	messageModel        MessageModel
	filter              MessageFilter
	loadBalanceStrategy LoadBalanceStrategy
	retryPolicy         *RetryPolicy
	consumeThreadMin    int32
	consumeThreadMax    int32
	// DLQ 模式：默认使用 Mock，必要时可切换到真实 Broker 交互
	dlqMode             DLQMode
}

// NewPushConsumer 创建Push消费者
func NewPushConsumer(groupName string) *PushConsumer {
	config := &ConsumerConfig{
		GroupName: groupName,
	}
	consumer := NewConsumer(config)
	return &PushConsumer{
		Consumer:            consumer,
		consumeType:         ConsumeConcurrently,
		pullInterval:        1 * time.Second,
		pullBatchSize:       32,
		concurrentlyMaxSpan: 2000,
		consumeFromWhere:    ConsumeFromLastOffset,
		messageModel:        Clustering,
		loadBalanceStrategy: &AverageAllocateStrategy{},
		retryPolicy: &RetryPolicy{
			MaxRetryTimes:   16,
			RetryDelayLevel: DelayLevel1s,
			RetryInterval:   1 * time.Second,
			EnableRetry:     true,
		},
		consumeThreadMin: 10,
		consumeThreadMax: 20,
		dlqMode:          DLQModeMock,
	}
}

// RegisterMessageListener 注册消息监听器
func (pc *PushConsumer) RegisterMessageListener(listener MessageListener) {
	pc.messageListener = listener
}

// SetMessageListener 设置消息监听器
func (pc *PushConsumer) SetMessageListener(listener MessageListener) {
	pc.messageListener = listener
}

// SetConsumeFromWhere 设置消费起始位置
func (pc *PushConsumer) SetConsumeFromWhere(consumeFromWhere ConsumeFromWhere) {
	pc.consumeFromWhere = consumeFromWhere
}

// SetConsumeTimestamp 设置消费时间戳
func (pc *PushConsumer) SetConsumeTimestamp(timestamp string) {
	pc.consumeTimestamp = timestamp
}

// SetMessageModel 设置消息模式
func (pc *PushConsumer) SetMessageModel(messageModel MessageModel) {
	pc.messageModel = messageModel
}

// SetConsumeThreadCount 设置消费线程数
func (pc *PushConsumer) SetConsumeThreadCount(min, max int32) {
	pc.consumeThreadMin = min
	pc.consumeThreadMax = max
}

// SetConcurrentlyMaxSpan 设置并发消费最大跨度
func (pc *PushConsumer) SetConcurrentlyMaxSpan(span int32) {
	pc.concurrentlyMaxSpan = span
}

// SetMessageFilter 设置消息过滤器
func (pc *PushConsumer) SetMessageFilter(filter MessageFilter) {
	pc.filter = filter
}

// SetLoadBalanceStrategy 设置负载均衡策略
func (pc *PushConsumer) SetLoadBalanceStrategy(strategy LoadBalanceStrategy) {
	pc.loadBalanceStrategy = strategy
}

// SetRetryPolicy 设置重试策略
func (pc *PushConsumer) SetRetryPolicy(policy *RetryPolicy) {
	pc.retryPolicy = policy
}

// SetConsumeType 设置消费类型
func (pc *PushConsumer) SetConsumeType(consumeType ConsumeType) {
	pc.consumeType = consumeType
}

// SetPullInterval 设置拉取间隔
func (pc *PushConsumer) SetPullInterval(interval time.Duration) {
	pc.pullInterval = interval
}

// SetPullBatchSize 设置批量拉取大小
func (pc *PushConsumer) SetPullBatchSize(batchSize int32) {
	pc.pullBatchSize = batchSize
}

// SetDLQMode 设置DLQ工作模式（Mock 或 Real Broker）
func (pc *PushConsumer) SetDLQMode(mode DLQMode) {
	pc.dlqMode = mode
}

// GetDLQMode 获取当前DLQ工作模式
func (pc *PushConsumer) GetDLQMode() DLQMode {
	return pc.dlqMode
}

// Start 启动消费者
func (pc *PushConsumer) Start() error {
	if pc.started {
		return fmt.Errorf("consumer already started")
	}
	pc.started = true
	pc.wg.Add(1)
	go pc.startPullAndConsumeLoop()
	return nil
}

func (pc *PushConsumer) startPullAndConsumeLoop() {
	defer pc.wg.Done()
	for pc.started {
		pc.pullAndConsumeMessages()
		time.Sleep(pc.pullInterval)
	}
}

// pullAndConsumeMessages 拉取并消费消息
func (pc *PushConsumer) pullAndConsumeMessages() {
	pc.Consumer.mutex.RLock()
	subscriptions := make(map[string]*Subscription)
	for topic, sub := range pc.Consumer.subscriptions {
		subscriptions[topic] = sub
	}
	pc.Consumer.mutex.RUnlock()
	
	for topic := range subscriptions {
		messages := pc.pullMessages(topic)
		if len(messages) > 0 {
			pc.consumeMessages(messages)
		}
	}
}

// pullMessages 拉取消息
func (pc *PushConsumer) pullMessages(topic string) []*MessageExt {
	// 如果是 DLQ Topic
	if strings.HasPrefix(topic, "%DLQ%_") {
		// 真实DLQ模式：走真实Broker拉取
		if pc.dlqMode == DLQModeReal {
			// 1. 路由
			routeData, err := pc.Consumer.getTopicRouteFromNameServer(topic)
			if err != nil {
				log.Printf("[DLQ-Real] 获取路由失败 topic=%s: %v", topic, err)
				return nil
			}
			if routeData == nil || len(routeData.QueueDatas) == 0 {
				log.Printf("[DLQ-Real] 无可用队列 topic=%s", topic)
				return nil
			}
			// 2. 选择队列
			mq := pc.Consumer.selectMessageQueue(routeData, topic)
			if mq == nil {
				log.Printf("[DLQ-Real] 选择队列失败 topic=%s", topic)
				return nil
			}
			// 3. 读取offset
			offset, err := pc.Consumer.getConsumeOffset(mq)
			if err != nil {
				log.Printf("[DLQ-Real] 获取offset失败，使用0: %v", err)
				offset = 0
			}
			// 4. 拉取
			msgs, nextOffset, err := pc.Consumer.pullMessagesFromBroker(mq, offset)
			if err != nil {
				log.Printf("[DLQ-Real] 从Broker拉取失败: %v", err)
				return nil
			}
			// 5. 更新offset
			if len(msgs) > 0 {
				pc.Consumer.updateConsumeOffset(mq, nextOffset)
			}
			return msgs
		}
		
		// Mock DLQ 模式：返回包含DLQ属性的模拟消息
		return []*MessageExt{
			{
				Message: &Message{
					Topic: topic,
					Tags:  "TagA",
					Body:  []byte(fmt.Sprintf("fail_message_auto_dlq - 原始业务失败消息 - %s - %d", topic, time.Now().Unix())),
					Properties: map[string]string{
						"ORIGIN_TOPIC":         "RetryTestTopic",
						"ORIGIN_MSG_ID":        fmt.Sprintf("dlq_msg_%d", time.Now().UnixNano()),
						"RETRY_TIMES":          "3",
						"DLQ_ORIGIN_QUEUE_ID":  "0",
						"DLQ_ORIGIN_BROKER":    "localhost:10911",
					},
				},
				MsgId:         fmt.Sprintf("dlq_msg_%d", time.Now().UnixNano()),
				QueueId:       0,
				StoreSize:     100,
				QueueOffset:   time.Now().Unix(),
				SysFlag:       0,
				BornTimestamp: time.Now(),
				StoreTimestamp: time.Now(),
				ReconsumeTimes: 3,
				StoreHost:     "localhost:10911",
			},
		}
	}
	
	// 普通消息，返回原有逻辑的模拟消息
	return []*MessageExt{
		{
			Message: &Message{
				Topic: topic,
				Tags:  "TagA",
				Body:  []byte(fmt.Sprintf("Push消息内容 - %s - %d", topic, time.Now().Unix())),
				Properties: make(map[string]string), // 初始化空的Properties
			},
			MsgId:        fmt.Sprintf("msg_%d", time.Now().UnixNano()),
			QueueId:      0,
			StoreSize:    100,
			QueueOffset:  time.Now().Unix(),
			SysFlag:      0,
			BornTimestamp: time.Now(),
			StoreTimestamp: time.Now(),
			ReconsumeTimes: 0, // 新消息重试次数为0
			StoreHost:     "localhost:10911",
		},
	}
}

// consumeMessages 消费消息
func (pc *PushConsumer) consumeMessages(messages []*MessageExt) {
	switch pc.consumeType {
	case ConsumeConcurrently:
		pc.consumeConcurrently(messages)
	case ConsumeOrderly:
		pc.consumeOrderly(messages)
	}
}

// consumeConcurrently 并发消费
func (pc *PushConsumer) consumeConcurrently(messages []*MessageExt) {
	if pc.messageListener != nil {
		result := pc.messageListener.ConsumeMessage(messages)
		if result != ConsumeSuccess {
			fmt.Printf("消费失败，需要重试: %v\n", result)
		}
	}
}

// consumeOrderly 顺序消费
func (pc *PushConsumer) consumeOrderly(messages []*MessageExt) {
	if pc.messageListener == nil {
		return
	}
	// 简化：按队列分组并顺序处理
	queues := make(map[int32][]*MessageExt)
	for _, msg := range messages {
		queues[msg.QueueId] = append(queues[msg.QueueId], msg)
	}
	for qid, msgs := range queues {
		pc.processQueueOrderly(qid, msgs)
	}
}

func (pc *PushConsumer) processQueueOrderly(queueId int32, msgs []*MessageExt) {
	if pc.messageListener == nil {
		return
	}
	// 此处可根据是否有顺序监听器进行不同处理，这里简化为逐条处理
	if listener, ok := pc.messageListener.(MessageListenerOrderly); ok {
		for _, msg := range msgs {
			status := listener([]*MessageExt{msg}, &ConsumeOrderlyContext{AutoCommit: true})
			if status == ReconsumeLater {
				pc.handleOrderlyRetry(queueId, []*MessageExt{msg})
				break
			}
		}
	} else {
		// 使用普通监听器，但保证顺序
		for _, msg := range msgs {
			result := pc.messageListener.ConsumeMessage([]*MessageExt{msg})
			if result == ReconsumeLater {
				pc.handleRetryMessage(msg)
				break // 顺序消费中，如果一条消息失败，后续消息不处理
			}
		}
	}
}

// handleOrderlyRetry 处理顺序消费重试
func (pc *PushConsumer) handleOrderlyRetry(queueId int32, msgs []*MessageExt) {
	if pc.retryPolicy == nil || !pc.retryPolicy.EnableRetry {
		fmt.Printf("队列%d顺序消费失败，重试已禁用\n", queueId)
		return
	}
	
	// 在实际实现中，这里应该暂停该队列的消费一段时间
	fmt.Printf("队列%d将暂停%v后重试\n", queueId, pc.retryPolicy.RetryInterval)
	// 可以使用定时器来实现暂停逻辑
}

// handleRetryMessage 处理重试消息
func (pc *PushConsumer) handleRetryMessage(msg *MessageExt) {
	if pc.retryPolicy == nil || !pc.retryPolicy.EnableRetry {
		fmt.Printf("消息消费失败，重试已禁用: %s\n", msg.MsgId)
		return
	}
	
	// 增加重试次数
	msg.ReconsumeTimes++
	
	if msg.ReconsumeTimes >= pc.retryPolicy.MaxRetryTimes {
		fmt.Printf("消息重试次数超限，进入死信队列: %s (重试次数: %d)\n", msg.MsgId, msg.ReconsumeTimes)
		// 根据模式选择：真实发送到DLQ或仅用于Mock演示
		if pc.dlqMode == DLQModeReal {
			// 调用基类的 sendToDLQ 方法，确保DLQ消息属性正确设置并发送到Broker
			if err := pc.Consumer.sendToDLQ(msg); err != nil {
				fmt.Printf("发送消息到DLQ失败: %v\n", err)
			}
		} else {
			// Mock 模式下不进行真实网络发送，由DLQ消费端的Mock拉取逻辑提供演示数据
			log.Printf("[DLQ-Mock] 已模拟投递至DLQ，MsgId=%s", msg.MsgId)
		}
		return
	}
	
	fmt.Printf("消息消费失败，稍后重试: %s, 重试次数: %d\n", msg.MsgId, msg.ReconsumeTimes)
	// 在实际实现中，这里应该将消息重新投递
	// 这里暂时只是打印日志，但实际应该调用 retryMessages 方法
}


// PullConsumer Pull模式消费者
type PullConsumer struct {
	*Consumer
	pullTimeout time.Duration
}

// NewPullConsumer 创建Pull消费者
func NewPullConsumer(groupName string) *PullConsumer {
	config := &ConsumerConfig{
		GroupName: groupName,
	}
	consumer := NewConsumer(config)
	return &PullConsumer{
		Consumer:    consumer,
		pullTimeout: 10 * time.Second,
	}
}

// SetPullTimeout 设置拉取超时时间
func (pc *PullConsumer) SetPullTimeout(timeout time.Duration) {
	pc.pullTimeout = timeout
}

// PullBlockIfNotFound 阻塞拉取消息
func (pc *PullConsumer) PullBlockIfNotFound(mq *MessageQueue, subExpression string, offset int64, maxNums int32) (*PullResult, error) {
	if !pc.started {
		return nil, fmt.Errorf("pull consumer not started")
	}
	
	// 简化实现，实际应该通过网络协议从Broker拉取
	messages := make([]*MessageExt, 0)
	
	// 模拟拉取到的消息
	for i := int32(0); i < maxNums && i < 5; i++ {
		msg := &MessageExt{
			Message: &Message{
				Topic: mq.Topic,
				Tags:  "TagA",
				Body:  []byte(fmt.Sprintf("Pull消息内容 - %d", offset+int64(i))),
			},
			MsgId:        fmt.Sprintf("pull_msg_%d_%d", offset, i),
			QueueId:      mq.QueueId,
			StoreSize:    100,
			QueueOffset:  offset + int64(i),
			SysFlag:      0,
			BornTimestamp: time.Now(),
			StoreTimestamp: time.Now(),
		}
		messages = append(messages, msg)
	}
	
	result := &PullResult{
		PullStatus:      PullFound,
		NextBeginOffset: offset + int64(len(messages)),
		MinOffset:       0,
		MaxOffset:       1000,
		MsgFoundList:    messages,
	}
	
	return result, nil
}

// PullNoHangup 非阻塞拉取
func (pc *PullConsumer) PullNoHangup(mq *MessageQueue, subExpression string, offset int64, maxNums int32) (*PullResult, error) {
	if !pc.started {
		return nil, fmt.Errorf("pull consumer not started")
	}
	return &PullResult{PullStatus: PullNoNewMsg, NextBeginOffset: offset, MinOffset: 0, MaxOffset: offset, MsgFoundList: []*MessageExt{}}, nil
}

// SimpleConsumer 简化消费者
type SimpleConsumer struct {
	*Consumer
	awaitDuration     time.Duration
	invisibleDuration time.Duration
	maxMessageNum     int32
	retryPolicy       *RetryPolicy
}

// RetryPolicy 重试策略
type RetryPolicy struct {
	MaxRetryTimes    int32         `json:"maxRetryTimes"`    // 最大重试次数
	RetryDelayLevel  int32         `json:"retryDelayLevel"`  // 重试延时级别
	RetryInterval    time.Duration `json:"retryInterval"`    // 重试间隔
	EnableRetry      bool          `json:"enableRetry"`      // 是否启用重试
}

// ConsumeProgress 消费进度
type ConsumeProgress struct {
	Topic       string `json:"topic"`       // 主题
	QueueId     int32  `json:"queueId"`     // 队列ID
	Offset      int64  `json:"offset"`      // 消费偏移量
	UpdateTime  int64  `json:"updateTime"`  // 更新时间
}

// NewSimpleConsumer 创建Simple消费者
func NewSimpleConsumer(groupName string) *SimpleConsumer {
	config := &ConsumerConfig{
		GroupName: groupName,
	}
	consumer := NewConsumer(config)
	return &SimpleConsumer{
		Consumer:          consumer,
		awaitDuration:     5 * time.Second,
		invisibleDuration: 30 * time.Second,
		maxMessageNum:     16,
		retryPolicy: &RetryPolicy{
			MaxRetryTimes:   16,
			RetryDelayLevel: DelayLevel1s,
			RetryInterval:   1 * time.Second,
			EnableRetry:     true,
		},
	}
}

// SetAwaitDuration 设置等待时长
func (sc *SimpleConsumer) SetAwaitDuration(duration time.Duration) {
	sc.awaitDuration = duration
}

// SetInvisibleDuration 设置不可见时长
func (sc *SimpleConsumer) SetInvisibleDuration(duration time.Duration) {
	sc.invisibleDuration = duration
}

// SetMaxMessageNum 设置最大消息数
func (sc *SimpleConsumer) SetMaxMessageNum(maxNum int32) {
	sc.maxMessageNum = maxNum
}

// ReceiveMessage 接收消息
func (sc *SimpleConsumer) ReceiveMessage(maxMessageNum int32, invisibleDuration time.Duration) ([]*MessageExt, error) {
	if !sc.started {
		return nil, fmt.Errorf("simple consumer not started")
	}
	// 简化：不实现真实拉取
	return []*MessageExt{}, nil
}

// AckMessage 确认消息
func (sc *SimpleConsumer) AckMessage(messageExt *MessageExt) error {
	if !sc.started {
		return fmt.Errorf("simple consumer not started")
	}
	return nil
}

// ChangeInvisibleDuration 修改不可见时长
func (sc *SimpleConsumer) ChangeInvisibleDuration(messageExt *MessageExt, invisibleDuration time.Duration) error {
	if !sc.started {
		return fmt.Errorf("simple consumer not started")
	}
	return nil
}

// SetRetryPolicy 设置重试策略
func (sc *SimpleConsumer) SetRetryPolicy(policy *RetryPolicy) {
	sc.retryPolicy = policy
}

// GetRetryPolicy 获取重试策略
func (sc *SimpleConsumer) GetRetryPolicy() *RetryPolicy {
	return sc.retryPolicy
}

// RetryMessage 重试消息
func (sc *SimpleConsumer) RetryMessage(messageExt *MessageExt) error {
	if !sc.started {
		return fmt.Errorf("simple consumer not started")
	}
	return nil
}

// UpdateConsumeProgress 更新消费进度
func (sc *SimpleConsumer) UpdateConsumeProgress(topic string, queueId int32, offset int64) error {
	if !sc.started {
		return fmt.Errorf("simple consumer not started")
	}
	return nil
}

// GetConsumeProgress 获取消费进度
func (sc *SimpleConsumer) GetConsumeProgress(topic string, queueId int32) (*ConsumeProgress, error) {
	if !sc.started {
		return nil, fmt.Errorf("simple consumer not started")
	}
	return &ConsumeProgress{Topic: topic, QueueId: queueId, Offset: 0, UpdateTime: time.Now().Unix()}, nil
}

// MessageFilter 消息过滤器类型
type MessageFilter interface {
	// Match 判断消息是否匹配过滤条件
	Match(msg *MessageExt) bool
}

// TagFilter 标签过滤器
type TagFilter struct {
	tags []string
}

// NewTagFilter 创建标签过滤器
func NewTagFilter(tags ...string) *TagFilter {
	return &TagFilter{tags: tags}
}

// NewTagFilterFromExpression 通过表达式创建标签过滤器
func NewTagFilterFromExpression(expression string) *TagFilter {
	return &TagFilter{tags: parseTags(expression)}
}

func parseTags(expression string) []string {
	parts := strings.Split(expression, "||")
	var tags []string
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			tags = append(tags, t)
		}
	}
	return tags
}

// Match 匹配实现
func (tf *TagFilter) Match(msg *MessageExt) bool {
	if len(tf.tags) == 0 {
		return true
	}
	for _, tag := range tf.tags {
		if msg.Tags == tag {
			return true
		}
	}
	return false
}

// SQL92Filter SQL92表达式过滤器
type SQL92Filter struct {
	expression string
	compiled   *SQLExpression
}

// NewSQL92Filter 创建SQL92过滤器
func NewSQL92Filter(expression string) (*SQL92Filter, error) {
	compiled, err := compileSQLExpression(expression)
	if err != nil {
		return nil, err
	}
	return &SQL92Filter{expression: expression, compiled: compiled}, nil
}

// Match SQL92过滤器匹配实现
func (sf *SQL92Filter) Match(msg *MessageExt) bool {
	if sf.compiled == nil {
		return true
	}
	return sf.compiled.Evaluate(msg)
}

// SQLExpression 伪实现
type SQLExpression struct {
	expression string
}

func (se *SQLExpression) Evaluate(msg *MessageExt) bool {
	// 这里是一个简单的模拟实现，实际应解析并执行表达式
	return true
}

func compileSQLExpression(expression string) (*SQLExpression, error) {
	// 这里简单返回表达式包装对象
	return &SQLExpression{expression: expression}, nil
}

func evaluateSimpleSQL(expression string, msg *MessageExt) bool {
	// 简化评估逻辑
	return true
}

// LoadBalanceStrategy 负载均衡策略接口
type LoadBalanceStrategy interface {
	Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue
	GetName() string
}

type AverageAllocateStrategy struct{}

func (aas *AverageAllocateStrategy) Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue {
	// 简化的平均分配策略实现（伪）
	return mqAll
}

func (aas *AverageAllocateStrategy) GetName() string { return "AverageAllocate" }

type RoundRobinAllocateStrategy struct{}

func (rras *RoundRobinAllocateStrategy) Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue {
	// 简化的轮询分配策略实现（伪）
	return mqAll
}

func (rras *RoundRobinAllocateStrategy) GetName() string { return "RoundRobinAllocate" }

type ConsistentHashAllocateStrategy struct {
	virtualNodeCount int // 虚拟节点数量
}

func NewConsistentHashAllocateStrategy(virtualNodeCount int) *ConsistentHashAllocateStrategy {
	return &ConsistentHashAllocateStrategy{virtualNodeCount: virtualNodeCount}
}

func (chas *ConsistentHashAllocateStrategy) Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue {
	// 简化：直接返回
	return mqAll
}

func (chas *ConsistentHashAllocateStrategy) GetName() string { return "ConsistentHashAllocate" }

type MachineRoomAllocateStrategy struct {
	machineRoomResolver func(brokerName string) string // 解析Broker所在机房
}

func NewMachineRoomAllocateStrategy(resolver func(brokerName string) string) *MachineRoomAllocateStrategy {
	return &MachineRoomAllocateStrategy{machineRoomResolver: resolver}
}

func (mras *MachineRoomAllocateStrategy) Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue {
	// 简化：直接返回
	return mqAll
}

func (mras *MachineRoomAllocateStrategy) getCurrentConsumerRoom(cid string) string { return "default" }

func (mras *MachineRoomAllocateStrategy) GetName() string { return "MachineRoomAllocate" }

type RebalanceService struct {
	consumer           *Consumer
	rebalanceInterval  time.Duration
	mutex              sync.RWMutex
	lastRebalanceTime  time.Time
}

func NewRebalanceService(consumer *Consumer) *RebalanceService {
	return &RebalanceService{consumer: consumer, rebalanceInterval: 30 * time.Second}
}

func (rs *RebalanceService) Start()  {}
func (rs *RebalanceService) Stop()   {}

func (rs *RebalanceService) rebalanceLoop() {}

func (rs *RebalanceService) doRebalance() {}

func (rs *RebalanceService) rebalanceTopic(topic string) {}

func (rs *RebalanceService) getTopicMessageQueues(topic string) []*MessageQueue { return []*MessageQueue{} }

func (rs *RebalanceService) getConsumerGroupMembers() []string { return []string{} }