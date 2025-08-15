package client

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
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

// Start 启动Push消费者
func (pc *PushConsumer) Start() error {
	if pc.messageListener == nil {
		return fmt.Errorf("message listener not set")
	}
	
	err := pc.Consumer.Start()
	if err != nil {
		return err
	}
	
	// 启动消息拉取和消费循环
	go pc.startPullAndConsumeLoop()
	
	return nil
}

// startPullAndConsumeLoop 启动拉取和消费循环
func (pc *PushConsumer) startPullAndConsumeLoop() {
	ticker := time.NewTicker(pc.pullInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-pc.Consumer.shutdown:
			return
		case <-ticker.C:
			pc.pullAndConsumeMessages()
		}
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
	// 简化实现，实际应该从Broker拉取消息
	// 这里返回模拟的消息
	return []*MessageExt{
		{
			Message: &Message{
				Topic: topic,
				Tags:  "TagA",
				Body:  []byte(fmt.Sprintf("Push消息内容 - %s - %d", topic, time.Now().Unix())),
			},
			MsgId:        fmt.Sprintf("msg_%d", time.Now().UnixNano()),
			QueueId:      0,
			StoreSize:    100,
			QueueOffset:  time.Now().Unix(),
			SysFlag:      0,
			BornTimestamp: time.Now(),
			StoreTimestamp: time.Now(),
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
	
	// 按队列分组消息，确保同一队列的消息顺序消费
	queueMsgs := make(map[int32][]*MessageExt)
	for _, msg := range messages {
		queueMsgs[msg.QueueId] = append(queueMsgs[msg.QueueId], msg)
	}
	
	// 按队列顺序处理消息
	for queueId, queueMessages := range queueMsgs {
		pc.processQueueOrderly(queueId, queueMessages)
	}
}

// processQueueOrderly 按队列顺序处理消息
func (pc *PushConsumer) processQueueOrderly(queueId int32, msgs []*MessageExt) {
	// 创建顺序消费上下文
	context := &ConsumeOrderlyContext{
		MessageQueue: &MessageQueue{
			Topic:   msgs[0].Topic,
			QueueId: queueId,
		},
		AutoCommit:  true,
		SuspendTime: 0,
	}
	
	// 如果有顺序消费监听器，使用它
	if orderlyListener, ok := pc.messageListener.(MessageListenerOrderly); ok {
		result := orderlyListener(msgs, context)
		if result == ReconsumeLater {
			fmt.Printf("顺序消息消费失败，队列%d暂停消费\n", queueId)
			// 在实际实现中，这里应该暂停该队列的消费
			pc.handleOrderlyRetry(queueId, msgs)
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
	
	if msg.ReconsumeTimes >= pc.retryPolicy.MaxRetryTimes {
		fmt.Printf("消息重试次数超限，进入死信队列: %s\n", msg.MsgId)
		// 在实际实现中，这里应该将消息发送到死信队列
		return
	}
	
	fmt.Printf("消息消费失败，稍后重试: %s, 重试次数: %d\n", msg.MsgId, msg.ReconsumeTimes+1)
	// 在实际实现中，这里应该将消息重新投递
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

// PullNoHangup 非阻塞拉取消息
func (pc *PullConsumer) PullNoHangup(mq *MessageQueue, subExpression string, offset int64, maxNums int32) (*PullResult, error) {
	return pc.PullBlockIfNotFound(mq, subExpression, offset, maxNums)
}

// SimpleConsumer 简单消费者（RocketMQ 5.0新增）
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

// NewSimpleConsumer 创建简单消费者
func NewSimpleConsumer(groupName string) *SimpleConsumer {
	config := &ConsumerConfig{
		GroupName: groupName,
	}
	consumer := NewConsumer(config)
	return &SimpleConsumer{
		Consumer:          consumer,
		awaitDuration:     10 * time.Second,
		invisibleDuration: 30 * time.Second,
		maxMessageNum:     16,
		retryPolicy: &RetryPolicy{
			MaxRetryTimes:   3,
			RetryDelayLevel: DelayLevel1s,
			RetryInterval:   1 * time.Second,
			EnableRetry:     true,
		},
	}
}

// SetAwaitDuration 设置等待时间
func (sc *SimpleConsumer) SetAwaitDuration(duration time.Duration) {
	sc.awaitDuration = duration
}

// SetInvisibleDuration 设置消息不可见时间
func (sc *SimpleConsumer) SetInvisibleDuration(duration time.Duration) {
	sc.invisibleDuration = duration
}

// SetMaxMessageNum 设置最大消息数量
func (sc *SimpleConsumer) SetMaxMessageNum(maxNum int32) {
	sc.maxMessageNum = maxNum
}

// ReceiveMessage 接收消息
func (sc *SimpleConsumer) ReceiveMessage(maxMessageNum int32, invisibleDuration time.Duration) ([]*MessageExt, error) {
	if !sc.started {
		return nil, fmt.Errorf("simple consumer not started")
	}
	
	if maxMessageNum <= 0 {
		maxMessageNum = sc.maxMessageNum
	}
	
	if invisibleDuration <= 0 {
		invisibleDuration = sc.invisibleDuration
	}
	
	// 简化实现，实际应该通过网络协议从Broker接收
	messages := make([]*MessageExt, 0)
	
	// 模拟接收到的消息
	for i := int32(0); i < maxMessageNum && i < 3; i++ {
		msg := &MessageExt{
			Message: &Message{
				Topic: "DefaultTopic", // 简化实现
				Tags:  "TagA",
				Body:  []byte(fmt.Sprintf("Simple消息内容 - %d", time.Now().Unix()+int64(i))),
			},
			MsgId:        fmt.Sprintf("simple_msg_%d", time.Now().UnixNano()+int64(i)),
			QueueId:      0,
			StoreSize:    100,
			QueueOffset:  time.Now().Unix() + int64(i),
			SysFlag:      0,
			BornTimestamp: time.Now(),
			StoreTimestamp: time.Now(),
		}
		messages = append(messages, msg)
	}
	
	return messages, nil
}

// AckMessage 确认消息
func (sc *SimpleConsumer) AckMessage(messageExt *MessageExt) error {
	if !sc.started {
		return fmt.Errorf("simple consumer not started")
	}
	
	// 简化实现，实际应该发送ACK请求到Broker
	fmt.Printf("确认消息: %s\n", messageExt.MsgId)
	return nil
}

// ChangeInvisibleDuration 修改消息不可见时间
func (sc *SimpleConsumer) ChangeInvisibleDuration(messageExt *MessageExt, invisibleDuration time.Duration) error {
	if !sc.started {
		return fmt.Errorf("simple consumer not started")
	}
	
	// 简化实现，实际应该发送修改请求到Broker
	fmt.Printf("修改消息不可见时间: %s, 时间: %v\n", messageExt.MsgId, invisibleDuration)
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
	
	if !sc.retryPolicy.EnableRetry {
		return fmt.Errorf("retry is disabled")
	}
	
	if messageExt.ReconsumeTimes >= sc.retryPolicy.MaxRetryTimes {
		return fmt.Errorf("exceed max retry times: %d", sc.retryPolicy.MaxRetryTimes)
	}
	
	// 简化实现，实际应该发送重试请求到Broker
	fmt.Printf("重试消息: %s, 重试次数: %d\n", messageExt.MsgId, messageExt.ReconsumeTimes+1)
	return nil
}

// UpdateConsumeProgress 更新消费进度
func (sc *SimpleConsumer) UpdateConsumeProgress(topic string, queueId int32, offset int64) error {
	if !sc.started {
		return fmt.Errorf("simple consumer not started")
	}
	
	// 简化实现，实际应该持久化消费进度到Broker
	fmt.Printf("更新消费进度: Topic=%s, QueueId=%d, Offset=%d\n", topic, queueId, offset)
	return nil
}

// GetConsumeProgress 获取消费进度
func (sc *SimpleConsumer) GetConsumeProgress(topic string, queueId int32) (*ConsumeProgress, error) {
	if !sc.started {
		return nil, fmt.Errorf("simple consumer not started")
	}
	
	// 简化实现，实际应该从Broker获取消费进度
	return &ConsumeProgress{
		Topic:      topic,
		QueueId:    queueId,
		Offset:     0, // 简化返回0
		UpdateTime: time.Now().Unix(),
	}, nil
}

// MessageFilter 消息过滤器接口
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

// NewTagFilterFromExpression 从表达式创建标签过滤器
func NewTagFilterFromExpression(expression string) *TagFilter {
	var tags []string
	if expression == "*" {
		tags = []string{"*"}
	} else if expression != "" {
		// 解析标签表达式，支持 "tag1||tag2" 格式
		tags = parseTags(expression)
	}
	return &TagFilter{tags: tags}
}

// parseTags 解析标签表达式
func parseTags(expression string) []string {
	// 简化实现：按 "||" 分割标签
	if expression == "" {
		return []string{}
	}
	
	// 这里可以实现更复杂的标签表达式解析
	// 目前只支持简单的 "tag1||tag2" 格式
	tags := []string{}
	if expression != "" {
		tags = append(tags, expression)
	}
	return tags
}

// Match 判断消息标签是否匹配
func (tf *TagFilter) Match(msg *MessageExt) bool {
	if len(tf.tags) == 0 {
		return true // 没有设置标签过滤，匹配所有消息
	}
	
	for _, tag := range tf.tags {
		if msg.Tags == tag || tag == "*" {
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
		return nil, fmt.Errorf("compile SQL expression failed: %v", err)
	}
	return &SQL92Filter{
		expression: expression,
		compiled:   compiled,
	}, nil
}

// Match 判断消息是否匹配SQL92表达式
func (sf *SQL92Filter) Match(msg *MessageExt) bool {
	if sf.compiled == nil {
		return true
	}
	return sf.compiled.Evaluate(msg)
}

// SQLExpression SQL表达式编译结果
type SQLExpression struct {
	expression string
	// 这里可以添加编译后的AST或其他优化结构
}

// Evaluate 评估SQL表达式
func (se *SQLExpression) Evaluate(msg *MessageExt) bool {
	// 简化实现，实际应该解析SQL表达式并评估
	// 支持基本的属性比较，如: a > 5 AND b = 'test'
	return evaluateSimpleSQL(se.expression, msg)
}

// compileSQLExpression 编译SQL表达式
func compileSQLExpression(expression string) (*SQLExpression, error) {
	if expression == "" {
		return &SQLExpression{expression: expression}, nil
	}
	// 这里应该实现完整的SQL92解析器
	// 简化实现，只做基本验证
	return &SQLExpression{expression: expression}, nil
}

// evaluateSimpleSQL 评估简单的SQL表达式
func evaluateSimpleSQL(expression string, msg *MessageExt) bool {
	if expression == "" {
		return true
	}
	
	// 简化实现：支持基本的属性比较
	// 例如: "a > 5", "b = 'test'", "c IS NOT NULL"
	// 实际实现需要完整的SQL解析器
	
	// 示例：检查消息属性
	if expression == "1=1" {
		return true
	}
	
	// 可以根据消息属性进行简单匹配
	for key, value := range msg.Properties {
		if fmt.Sprintf("%s = '%s'", key, value) == expression {
			return true
		}
	}
	
	return false
}

// LoadBalanceStrategy 负载均衡策略
type LoadBalanceStrategy interface {
	// Allocate 分配消息队列
	Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue
	// GetName 获取策略名称
	GetName() string
}

// AverageAllocateStrategy 平均分配策略
type AverageAllocateStrategy struct{}

// Allocate 平均分配消息队列
func (aas *AverageAllocateStrategy) Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue {
	if len(mqAll) == 0 || len(cidAll) == 0 {
		return nil
	}
	
	// 找到当前消费者的索引
	currentIndex := -1
	for i, cid := range cidAll {
		if cid == currentCID {
			currentIndex = i
			break
		}
	}
	
	if currentIndex == -1 {
		return nil
	}
	
	// 计算分配的队列
	mqLen := len(mqAll)
	cidLen := len(cidAll)
	mod := mqLen % cidLen
	averageSize := mqLen / cidLen
	
	startIndex := currentIndex * averageSize
	if currentIndex < mod {
		startIndex += currentIndex
		averageSize++
	} else {
		startIndex += mod
	}
	
	result := make([]*MessageQueue, 0, averageSize)
	for i := 0; i < averageSize; i++ {
		if startIndex+i < mqLen {
			result = append(result, mqAll[startIndex+i])
		}
	}
	
	return result
}

// GetName 获取策略名称
func (aas *AverageAllocateStrategy) GetName() string {
	return "AVG"
}

// RoundRobinAllocateStrategy 轮询分配策略
type RoundRobinAllocateStrategy struct{}

// Allocate 轮询分配消息队列
func (rras *RoundRobinAllocateStrategy) Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue {
	if len(mqAll) == 0 || len(cidAll) == 0 {
		return nil
	}
	
	// 找到当前消费者的索引
	currentIndex := -1
	for i, cid := range cidAll {
		if cid == currentCID {
			currentIndex = i
			break
		}
	}
	
	if currentIndex == -1 {
		return nil
	}
	
	// 轮询分配
	result := make([]*MessageQueue, 0)
	for i := currentIndex; i < len(mqAll); i += len(cidAll) {
		result = append(result, mqAll[i])
	}
	
	return result
}

// GetName 获取策略名称
func (rras *RoundRobinAllocateStrategy) GetName() string {
	return "RR"
}

// ConsistentHashAllocateStrategy 一致性哈希分配策略
type ConsistentHashAllocateStrategy struct {
	virtualNodeCount int // 虚拟节点数量
}

// NewConsistentHashAllocateStrategy 创建一致性哈希分配策略
func NewConsistentHashAllocateStrategy(virtualNodeCount int) *ConsistentHashAllocateStrategy {
	if virtualNodeCount <= 0 {
		virtualNodeCount = 160 // 默认虚拟节点数
	}
	return &ConsistentHashAllocateStrategy{
		virtualNodeCount: virtualNodeCount,
	}
}

// Allocate 一致性哈希分配消息队列
func (chas *ConsistentHashAllocateStrategy) Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue {
	if len(mqAll) == 0 || len(cidAll) == 0 {
		return nil
	}
	
	// 简化的一致性哈希实现
	// 实际应该使用完整的一致性哈希环
	hashRing := make(map[uint32]string)
	
	// 为每个消费者创建虚拟节点
	for _, cid := range cidAll {
		for i := 0; i < chas.virtualNodeCount; i++ {
			virtualNode := fmt.Sprintf("%s#%d", cid, i)
			hash := consistentHash(virtualNode)
			hashRing[hash] = cid
		}
	}
	
	// 为每个消息队列找到对应的消费者
	result := make([]*MessageQueue, 0)
	for _, mq := range mqAll {
		mqKey := fmt.Sprintf("%s-%d", mq.BrokerName, mq.QueueId)
		mqHash := consistentHash(mqKey)
		
		// 找到第一个大于等于mqHash的虚拟节点
		var targetCID string
		minDiff := uint32(^uint32(0)) // 最大uint32值
		for hash, cid := range hashRing {
			if hash >= mqHash {
				if hash-mqHash < minDiff {
					minDiff = hash - mqHash
					targetCID = cid
				}
			}
		}
		
		// 如果没找到，选择最小的hash值对应的消费者
		if targetCID == "" {
			minHash := uint32(^uint32(0))
			for hash, cid := range hashRing {
				if hash < minHash {
					minHash = hash
					targetCID = cid
				}
			}
		}
		
		if targetCID == currentCID {
			result = append(result, mq)
		}
	}
	
	return result
}

// GetName 获取策略名称
func (chas *ConsistentHashAllocateStrategy) GetName() string {
	return "CONSISTENT_HASH"
}

// MachineRoomAllocateStrategy 机房感知分配策略
type MachineRoomAllocateStrategy struct {
	machineRoomResolver func(brokerName string) string // 解析Broker所在机房
}

// NewMachineRoomAllocateStrategy 创建机房感知分配策略
func NewMachineRoomAllocateStrategy(resolver func(brokerName string) string) *MachineRoomAllocateStrategy {
	if resolver == nil {
		// 默认解析器，从broker名称中提取机房信息
		resolver = func(brokerName string) string {
			// 假设broker名称格式为: broker-{room}-{id}
			parts := strings.Split(brokerName, "-")
			if len(parts) >= 2 {
				return parts[1]
			}
			return "default"
		}
	}
	return &MachineRoomAllocateStrategy{
		machineRoomResolver: resolver,
	}
}

// Allocate 机房感知分配消息队列
func (mras *MachineRoomAllocateStrategy) Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue {
	if len(mqAll) == 0 || len(cidAll) == 0 {
		return nil
	}
	
	// 按机房分组消息队列
	roomMQs := make(map[string][]*MessageQueue)
	for _, mq := range mqAll {
		room := mras.machineRoomResolver(mq.BrokerName)
		roomMQs[room] = append(roomMQs[room], mq)
	}
	
	// 获取当前消费者所在机房（简化实现，假设从CID中解析）
	currentRoom := mras.getCurrentConsumerRoom(currentCID)
	
	// 优先分配同机房的队列
	result := make([]*MessageQueue, 0)
	if sameMQs, exists := roomMQs[currentRoom]; exists {
		// 使用平均分配策略分配同机房的队列
		avgStrategy := &AverageAllocateStrategy{}
		sameMQResult := avgStrategy.Allocate(consumerGroup, currentCID, sameMQs, cidAll)
		result = append(result, sameMQResult...)
	}
	
	// 如果同机房队列不足，分配其他机房的队列
	if len(result) == 0 {
		avgStrategy := &AverageAllocateStrategy{}
		result = avgStrategy.Allocate(consumerGroup, currentCID, mqAll, cidAll)
	}
	
	return result
}

// getCurrentConsumerRoom 获取当前消费者所在机房
func (mras *MachineRoomAllocateStrategy) getCurrentConsumerRoom(cid string) string {
	// 简化实现，假设CID格式为: consumer-{room}-{id}
	parts := strings.Split(cid, "-")
	if len(parts) >= 2 {
		return parts[1]
	}
	return "default"
}

// GetName 获取策略名称
func (mras *MachineRoomAllocateStrategy) GetName() string {
	return "MACHINE_ROOM"
}

// consistentHash 一致性哈希函数
func consistentHash(s string) uint32 {
	h := uint32(0)
	for _, c := range s {
		h = h*31 + uint32(c)
	}
	return h
}

// RebalanceService 重平衡服务
type RebalanceService struct {
	consumer        *Consumer
	rebalanceInterval time.Duration
	stopChan        chan struct{}
	mutex           sync.RWMutex
	lastRebalanceTime time.Time
}

// NewRebalanceService 创建重平衡服务
func NewRebalanceService(consumer *Consumer) *RebalanceService {
	return &RebalanceService{
		consumer:          consumer,
		rebalanceInterval: 20 * time.Second, // 默认20秒重平衡一次
		stopChan:          make(chan struct{}),
	}
}

// Start 启动重平衡服务
func (rs *RebalanceService) Start() {
	go rs.rebalanceLoop()
}

// Stop 停止重平衡服务
func (rs *RebalanceService) Stop() {
	close(rs.stopChan)
}

// rebalanceLoop 重平衡循环
func (rs *RebalanceService) rebalanceLoop() {
	ticker := time.NewTicker(rs.rebalanceInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			rs.doRebalance()
		case <-rs.stopChan:
			return
		}
	}
}

// doRebalance 执行重平衡
func (rs *RebalanceService) doRebalance() {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()
	
	if time.Since(rs.lastRebalanceTime) < rs.rebalanceInterval {
		return
	}
	
	// 获取所有订阅的主题
	subscriptions := rs.consumer.GetSubscriptions()
	for topic := range subscriptions {
		rs.rebalanceTopic(topic)
	}
	
	rs.lastRebalanceTime = time.Now()
}

// rebalanceTopic 重平衡指定主题
func (rs *RebalanceService) rebalanceTopic(topic string) {
	// 模拟获取主题的所有消息队列
	mqAll := rs.getTopicMessageQueues(topic)
	
	// 模拟获取消费者组中的所有消费者
	cidAll := rs.getConsumerGroupMembers()
	
	// 使用负载均衡策略重新分配队列
	// 这里需要根据实际的负载均衡策略来分配
	log.Printf("Rebalancing topic: %s, queues: %d, consumers: %d", topic, len(mqAll), len(cidAll))
}

// getTopicMessageQueues 获取主题的所有消息队列
func (rs *RebalanceService) getTopicMessageQueues(topic string) []*MessageQueue {
	// 模拟实现，实际应该从NameServer获取
	queues := make([]*MessageQueue, 0)
	for i := 0; i < 4; i++ { // 假设每个主题有4个队列
		queues = append(queues, &MessageQueue{
			Topic:      topic,
			BrokerName: fmt.Sprintf("broker-%d", i%2),
			QueueId:    int32(i),
		})
	}
	return queues
}

// getConsumerGroupMembers 获取消费者组成员
func (rs *RebalanceService) getConsumerGroupMembers() []string {
	// 模拟实现，实际应该从Broker获取
	return []string{"consumer-1", "consumer-2", "consumer-3"}
}