package client

import (
	"fmt"
	"sync"
	"time"
)

// TransactionProducer 事务消息生产者
type TransactionProducer struct {
	*Producer
	transactionListener TransactionListener
	checkExecutor       *TransactionCheckExecutor
}

// TransactionCheckExecutor 事务检查执行器
type TransactionCheckExecutor struct {
	listener TransactionListener
	mutex    sync.RWMutex
	transactionStateTable map[string]*TransactionState
}

// TransactionState 事务状态
type TransactionState struct {
	MsgId         string
	TransactionId string
	State         LocalTransactionState
	CreateTime    time.Time
	UpdateTime    time.Time
	CheckTimes    int32
}

// NewTransactionProducer 创建事务生产者
func NewTransactionProducer(groupName string, listener TransactionListener) *TransactionProducer {
	producer := NewProducer(groupName)
	checkExecutor := &TransactionCheckExecutor{
		listener:              listener,
		transactionStateTable: make(map[string]*TransactionState),
	}
	
	return &TransactionProducer{
		Producer:            producer,
		transactionListener: listener,
		checkExecutor:       checkExecutor,
	}
}

// SendMessageInTransaction 发送事务消息
func (tp *TransactionProducer) SendMessageInTransaction(msg *Message, arg interface{}) (*SendResult, error) {
	if !tp.started {
		return nil, fmt.Errorf("transaction producer not started")
	}
	
	if tp.transactionListener == nil {
		return nil, fmt.Errorf("transaction listener is nil")
	}
	
	// 1. 发送半消息（Prepare消息）
	prepareMsg := *msg
	prepareMsg.SetProperty("TRAN_MSG", "true")
	prepareMsg.SetProperty("PREPARE_MESSAGE", "true")
	
	result, err := tp.Producer.SendSync(&prepareMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to send prepare message: %v", err)
	}
	
	// 2. 执行本地事务
	localTransactionState := tp.transactionListener.ExecuteLocalTransaction(msg, arg)
	
	// 3. 根据本地事务执行结果，提交或回滚事务
	err = tp.endTransaction(result.MsgId, localTransactionState)
	if err != nil {
		return nil, fmt.Errorf("failed to end transaction: %v", err)
	}
	
	return result, nil
}

// endTransaction 结束事务
func (tp *TransactionProducer) endTransaction(msgId string, state LocalTransactionState) error {
	// 构造事务结束请求
	var transactionStatus TransactionStatus
	switch state {
	case CommitMessage:
		transactionStatus = CommitTransaction
	case RollbackMessage:
		transactionStatus = RollbackTransaction
	default:
		transactionStatus = UnknownTransaction
	}
	
	// 这里应该发送事务结束请求到Broker
	// 简化实现，实际需要通过网络协议发送
	fmt.Printf("Ending transaction for msgId: %s, status: %v\n", msgId, transactionStatus)
	
	return nil
}

// CheckTransactionState 检查事务状态（由Broker回调）
func (tp *TransactionProducer) CheckTransactionState(msgExt *MessageExt) LocalTransactionState {
	if tp.transactionListener == nil {
		return UnknownMessage
	}
	
	return tp.checkExecutor.CheckTransactionState(msgExt)
}

// CheckTransactionState 检查事务状态
func (tce *TransactionCheckExecutor) CheckTransactionState(msgExt *MessageExt) LocalTransactionState {
	tce.mutex.Lock()
	defer tce.mutex.Unlock()
	
	// 获取或创建事务状态
	transactionState := tce.getOrCreateTransactionState(msgExt.MsgId, msgExt.TransactionId)
	transactionState.CheckTimes++
	transactionState.UpdateTime = time.Now()
	
	// 调用用户的事务检查逻辑
	state := tce.listener.CheckLocalTransaction(msgExt)
	transactionState.State = state
	
	return state
}

// getOrCreateTransactionState 获取或创建事务状态
func (tce *TransactionCheckExecutor) getOrCreateTransactionState(msgId, transactionId string) *TransactionState {
	if state, exists := tce.transactionStateTable[msgId]; exists {
		return state
	}
	
	state := &TransactionState{
		MsgId:         msgId,
		TransactionId: transactionId,
		State:         UnknownMessage,
		CreateTime:    time.Now(),
		UpdateTime:    time.Now(),
		CheckTimes:    0,
	}
	tce.transactionStateTable[msgId] = state
	return state
}

// RemoveTransactionState 移除事务状态
func (tce *TransactionCheckExecutor) RemoveTransactionState(msgId string) {
	tce.mutex.Lock()
	defer tce.mutex.Unlock()
	delete(tce.transactionStateTable, msgId)
}

// GetTransactionState 获取事务状态
func (tce *TransactionCheckExecutor) GetTransactionState(msgId string) *TransactionState {
	tce.mutex.RLock()
	defer tce.mutex.RUnlock()
	return tce.transactionStateTable[msgId]
}

// SendOrderedMessage 发送顺序消息
func (p *Producer) SendOrderedMessage(msg *Message, selector MessageQueueSelector, arg interface{}) (*SendResult, error) {
	if !p.started {
		return nil, fmt.Errorf("producer not started")
	}
	
	if selector == nil {
		return nil, fmt.Errorf("message queue selector is nil")
	}
	
	// 获取Topic路由信息
	routeData := p.getTopicRouteData(msg.Topic)
	if routeData == nil {
		return nil, fmt.Errorf("no route data for topic: %s", msg.Topic)
	}
	
	// 构造消息队列列表
	var messageQueues []*MessageQueue
	for _, queueData := range routeData.QueueDatas {
		for i := int32(0); i < queueData.WriteQueueNums; i++ {
			mq := &MessageQueue{
				Topic:      msg.Topic,
				BrokerName: queueData.BrokerName,
				QueueId:    i,
			}
			messageQueues = append(messageQueues, mq)
		}
	}
	
	// 使用选择器选择消息队列
	selectedQueue := selector.Select(messageQueues, msg, arg)
	if selectedQueue == nil {
		return nil, fmt.Errorf("no message queue selected")
	}
	
	// 标记为顺序消息
	msg.SetProperty("ORDERED_MESSAGE", "true")
	
	// 发送到指定队列
	return p.sendMessageToQueue(msg, selectedQueue, p.config.SendMsgTimeout)
}

// SendDelayMessage 发送延时消息
func (p *Producer) SendDelayMessage(msg *Message, delayLevel int32) (*SendResult, error) {
	if !p.started {
		return nil, fmt.Errorf("producer not started")
	}
	
	if delayLevel < 1 || delayLevel > 18 {
		return nil, fmt.Errorf("invalid delay level: %d, should be 1-18", delayLevel)
	}
	
	// 设置延时级别
	msg.SetDelayTimeLevel(delayLevel)
	msg.SetProperty("DELAY_MESSAGE", "true")
	
	return p.SendSync(msg)
}

// SendScheduledMessage 发送定时消息
func (p *Producer) SendScheduledMessage(msg *Message, deliverTime time.Time) (*SendResult, error) {
	if !p.started {
		return nil, fmt.Errorf("producer not started")
	}
	
	if deliverTime.Before(time.Now()) {
		return nil, fmt.Errorf("deliver time should be in the future")
	}
	
	// 设置开始投递时间
	msg.SetStartDeliverTime(deliverTime.UnixMilli())
	msg.SetProperty("SCHEDULED_MESSAGE", "true")
	
	return p.SendSync(msg)
}

// SendBatchMessages 发送批量消息
func (p *Producer) SendBatchMessages(messages []*Message) (*SendResult, error) {
	if !p.started {
		return nil, fmt.Errorf("producer not started")
	}
	
	if len(messages) == 0 {
		return nil, fmt.Errorf("messages list is empty")
	}
	
	// 验证所有消息都属于同一个Topic
	topic := messages[0].Topic
	for _, msg := range messages {
		if msg.Topic != topic {
			return nil, fmt.Errorf("all messages in batch must have the same topic")
		}
	}
	
	// 计算总大小
	totalSize := 0
	for _, msg := range messages {
		totalSize += len(msg.Body)
	}
	
	if totalSize > int(p.config.MaxMessageSize) {
		return nil, fmt.Errorf("batch messages size exceeds limit: %d", p.config.MaxMessageSize)
	}
	
	// 创建批量消息
	batchMsg := &Message{
		Topic:      topic,
		Properties: make(map[string]string),
	}
	batchMsg.SetProperty("BATCH_MESSAGE", "true")
	batchMsg.SetProperty("BATCH_SIZE", fmt.Sprintf("%d", len(messages)))
	
	// 简化实现：将所有消息体合并
	// 实际实现中应该使用更复杂的编码格式
	var batchBody []byte
	for i, msg := range messages {
		if i > 0 {
			batchBody = append(batchBody, '|') // 使用|作为分隔符
		}
		batchBody = append(batchBody, msg.Body...)
	}
	batchMsg.Body = batchBody
	
	return p.SendSync(batchMsg)
}

// DefaultMessageQueueSelector 默认消息队列选择器
type DefaultMessageQueueSelector struct{}

// Select 选择消息队列（基于哈希）
func (s *DefaultMessageQueueSelector) Select(mqs []*MessageQueue, msg *Message, arg interface{}) *MessageQueue {
	if len(mqs) == 0 {
		return nil
	}
	
	// 如果有分片键，使用分片键的哈希值
	if msg.ShardingKey != "" {
		hash := simpleHash(msg.ShardingKey)
		return mqs[hash%len(mqs)]
	}
	
	// 如果传入了参数，使用参数的哈希值
	if arg != nil {
		hash := simpleHash(fmt.Sprintf("%v", arg))
		return mqs[hash%len(mqs)]
	}
	
	// 默认使用消息键的哈希值
	if msg.Keys != "" {
		hash := simpleHash(msg.Keys)
		return mqs[hash%len(mqs)]
	}
	
	// 最后使用轮询方式
	return mqs[0]
}

// OrderedMessageQueueSelector 顺序消息队列选择器
type OrderedMessageQueueSelector struct{}

// Select 选择消息队列（确保相同的key选择相同的队列）
func (s *OrderedMessageQueueSelector) Select(mqs []*MessageQueue, msg *Message, arg interface{}) *MessageQueue {
	if len(mqs) == 0 {
		return nil
	}
	
	// 优先使用分片键
	if msg.ShardingKey != "" {
		hash := simpleHash(msg.ShardingKey)
		return mqs[hash%len(mqs)]
	}
	
	// 使用传入的参数作为分片键
	if arg != nil {
		hash := simpleHash(fmt.Sprintf("%v", arg))
		return mqs[hash%len(mqs)]
	}
	
	// 使用消息键
	if msg.Keys != "" {
		hash := simpleHash(msg.Keys)
		return mqs[hash%len(mqs)]
	}
	
	// 如果没有任何键，返回第一个队列
	return mqs[0]
}

// RoundRobinMessageQueueSelector 轮询消息队列选择器
type RoundRobinMessageQueueSelector struct {
	counter int
	mutex   sync.Mutex
}

// Select 轮询选择消息队列
func (s *RoundRobinMessageQueueSelector) Select(mqs []*MessageQueue, msg *Message, arg interface{}) *MessageQueue {
	if len(mqs) == 0 {
		return nil
	}
	
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	selected := mqs[s.counter%len(mqs)]
	s.counter++
	return selected
}

// simpleHash 简单哈希函数
func simpleHash(s string) int {
	hash := 0
	for _, c := range s {
		hash = hash*31 + int(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}