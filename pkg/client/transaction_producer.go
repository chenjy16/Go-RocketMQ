package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
	"math/rand"
	"encoding/json"
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

// EndTransactionRequest 事务结束请求
type EndTransactionRequest struct {
	MsgId             string
	TransactionStatus TransactionStatus
	ProducerGroup     string
	Timestamp         int64
}

// 事务相关常量
const (
	END_TRANSACTION_CODE = 37
	TRANSACTION_GOLANG   = "GOLANG"
	TRANSACTION_VERSION  = 1
	TRANSACTION_SUCCESS  = 0
)

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
	
	// 发送事务结束请求到Broker
	return tp.sendEndTransactionRequest(msgId, transactionStatus)
}

// sendEndTransactionRequest 发送事务结束请求
func (tp *TransactionProducer) sendEndTransactionRequest(msgId string, status TransactionStatus) error {
	// 获取Broker地址
	brokerAddr, err := tp.getBrokerAddr()
	if err != nil {
		return fmt.Errorf("failed to get broker address: %v", err)
	}
	
	// 构建事务结束请求
	request := &EndTransactionRequest{
		MsgId:             msgId,
		TransactionStatus: status,
		ProducerGroup:     tp.getProducerGroup(),
		Timestamp:         time.Now().UnixMilli(),
	}
	
	// 发送请求
	return tp.sendTransactionRequest(brokerAddr, request)
}

// getBrokerAddr 获取Broker地址
func (tp *TransactionProducer) getBrokerAddr() (string, error) {
	// 从路由信息中获取Broker地址
	if len(tp.Producer.nameServers) == 0 {
		return "", fmt.Errorf("no name servers configured")
	}
	
	// 简化实现：使用第一个NameServer地址作为Broker地址
	// 实际应该通过路由发现获取真实的Broker地址
	return tp.Producer.nameServers[0], nil
}

// sendTransactionRequest 发送事务请求
func (tp *TransactionProducer) sendTransactionRequest(brokerAddr string, request *EndTransactionRequest) error {
	// 序列化请求
	body, err := tp.encodeEndTransactionRequest(request)
	if err != nil {
		return fmt.Errorf("failed to encode request: %v", err)
	}
	
	// 构建RemotingCommand
	cmd := &RemotingCommand{
		Code:     END_TRANSACTION_CODE,
		Language: TRANSACTION_GOLANG,
		Version:  TRANSACTION_VERSION,
		Opaque:   tp.generateOpaque(),
		Flag:     0,
		Body:     body,
	}
	
	// 添加请求头
	cmd.ExtFields = map[string]string{
		"msgId":             request.MsgId,
		"transactionStatus": fmt.Sprintf("%d", request.TransactionStatus),
		"producerGroup":     request.ProducerGroup,
		"timestamp":         fmt.Sprintf("%d", request.Timestamp),
	}
	
	// 发送请求并等待响应
	response, err := tp.sendTransactionSyncRequest(brokerAddr, cmd)
	if err != nil {
		return fmt.Errorf("failed to send transaction request: %v", err)
	}
	
	// 检查响应状态
	if response.Code != TRANSACTION_SUCCESS {
		return fmt.Errorf("transaction request failed with code: %d, remark: %s", response.Code, response.Remark)
	}
	
	return nil
}

// encodeEndTransactionRequest 编码事务结束请求
func (tp *TransactionProducer) encodeEndTransactionRequest(request *EndTransactionRequest) ([]byte, error) {
	return json.Marshal(request)
}

// generateOpaque 生成请求ID
func (tp *TransactionProducer) generateOpaque() int32 {
	return rand.Int31()
}

// sendTransactionSyncRequest 发送同步事务请求
func (tp *TransactionProducer) sendTransactionSyncRequest(brokerAddr string, cmd *RemotingCommand) (*RemotingCommand, error) {
	// 简化实现：模拟发送请求
	// 实际应该通过网络连接发送到Broker
	fmt.Printf("Sending transaction request to broker: %s, msgId: %s\n", brokerAddr, cmd.ExtFields["msgId"])
	
	// 返回成功响应
	return &RemotingCommand{
		Code:   TRANSACTION_SUCCESS,
		Remark: "Transaction completed successfully",
	}, nil
}

// getProducerGroup 获取生产者组名
func (tp *TransactionProducer) getProducerGroup() string {
	// 从嵌入的Producer获取组名
	if tp.Producer != nil {
		return tp.Producer.config.GroupName
	}
	return "DEFAULT_TRANSACTION_PRODUCER"
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
	
	// 使用复杂的批量消息编码格式
	batchBody, err := p.encodeBatchMessages(messages)
	if err != nil {
		return nil, fmt.Errorf("failed to encode batch messages: %v", err)
	}
	
	if len(batchBody) > int(p.config.MaxMessageSize) {
		return nil, fmt.Errorf("batch messages size exceeds limit: %d", p.config.MaxMessageSize)
	}
	
	// 创建批量消息
	batchMsg := &Message{
		Topic:      topic,
		Properties: make(map[string]string),
		Body:       batchBody,
	}
	batchMsg.SetProperty("BATCH_MESSAGE", "true")
	batchMsg.SetProperty("BATCH_SIZE", fmt.Sprintf("%d", len(messages)))
	batchMsg.SetProperty("BATCH_ENCODING", "ROCKETMQ_V1")
	
	return p.SendSync(batchMsg)
}

// encodeBatchMessages 编码批量消息，使用类似RocketMQ的复杂编码格式
func (p *Producer) encodeBatchMessages(messages []*Message) ([]byte, error) {
	var buffer bytes.Buffer
	
	// 写入批量消息头部信息
	header := &BatchMessageHeader{
		Magic:        0x12345678, // 魔数
		Version:      1,          // 版本号
		MessageCount: int32(len(messages)),
		Timestamp:    time.Now().UnixMilli(),
	}
	
	if err := p.writeBatchHeader(&buffer, header); err != nil {
		return nil, fmt.Errorf("failed to write batch header: %v", err)
	}
	
	// 编码每个消息
	for i, msg := range messages {
		encodedMsg, err := p.encodeMessage(msg, int32(i))
		if err != nil {
			return nil, fmt.Errorf("failed to encode message %d: %v", i, err)
		}
		
		// 写入消息长度（4字节）
		if err := binary.Write(&buffer, binary.BigEndian, int32(len(encodedMsg))); err != nil {
			return nil, fmt.Errorf("failed to write message length: %v", err)
		}
		
		// 写入消息数据
		if _, err := buffer.Write(encodedMsg); err != nil {
			return nil, fmt.Errorf("failed to write message data: %v", err)
		}
	}
	
	return buffer.Bytes(), nil
}

// BatchMessageHeader 批量消息头部
type BatchMessageHeader struct {
	Magic        int32 // 魔数，用于识别批量消息格式
	Version      int32 // 版本号
	MessageCount int32 // 消息数量
	Timestamp    int64 // 时间戳
}

// writeBatchHeader 写入批量消息头部
func (p *Producer) writeBatchHeader(buffer *bytes.Buffer, header *BatchMessageHeader) error {
	if err := binary.Write(buffer, binary.BigEndian, header.Magic); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.Version); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.MessageCount); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.Timestamp); err != nil {
		return err
	}
	return nil
}

// encodeMessage 编码单个消息
func (p *Producer) encodeMessage(msg *Message, index int32) ([]byte, error) {
	var buffer bytes.Buffer
	
	// 消息头部
	msgHeader := &MessageHeader{
		Index:      index,
		BodyLength: int32(len(msg.Body)),
		Flag:       p.calculateMessageFlag(msg),
		Timestamp:  time.Now().UnixMilli(),
	}
	
	// 写入消息头部
	if err := p.writeMessageHeader(&buffer, msgHeader); err != nil {
		return nil, fmt.Errorf("failed to write message header: %v", err)
	}
	
	// 写入Topic长度和内容
	topicBytes := []byte(msg.Topic)
	if err := binary.Write(&buffer, binary.BigEndian, int16(len(topicBytes))); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(topicBytes); err != nil {
		return nil, err
	}
	
	// 写入Tags（如果存在）
	tags := msg.Tags
	tagsBytes := []byte(tags)
	if err := binary.Write(&buffer, binary.BigEndian, int16(len(tagsBytes))); err != nil {
		return nil, err
	}
	if len(tagsBytes) > 0 {
		if _, err := buffer.Write(tagsBytes); err != nil {
			return nil, err
		}
	}
	
	// 写入Keys（如果存在）
	keys := msg.Keys
	keysBytes := []byte(keys)
	if err := binary.Write(&buffer, binary.BigEndian, int16(len(keysBytes))); err != nil {
		return nil, err
	}
	if len(keysBytes) > 0 {
		if _, err := buffer.Write(keysBytes); err != nil {
			return nil, err
		}
	}
	
	// 写入Properties
	propertiesData, err := p.encodeProperties(msg.Properties)
	if err != nil {
		return nil, fmt.Errorf("failed to encode properties: %v", err)
	}
	if err := binary.Write(&buffer, binary.BigEndian, int32(len(propertiesData))); err != nil {
		return nil, err
	}
	if len(propertiesData) > 0 {
		if _, err := buffer.Write(propertiesData); err != nil {
			return nil, err
		}
	}
	
	// 写入消息体
	if _, err := buffer.Write(msg.Body); err != nil {
		return nil, err
	}
	
	return buffer.Bytes(), nil
}

// MessageHeader 消息头部
type MessageHeader struct {
	Index      int32 // 消息在批次中的索引
	BodyLength int32 // 消息体长度
	Flag       int32 // 消息标志
	Timestamp  int64 // 时间戳
}

// writeMessageHeader 写入消息头部
func (p *Producer) writeMessageHeader(buffer *bytes.Buffer, header *MessageHeader) error {
	if err := binary.Write(buffer, binary.BigEndian, header.Index); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.BodyLength); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.Flag); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.Timestamp); err != nil {
		return err
	}
	return nil
}

// calculateMessageFlag 计算消息标志
func (p *Producer) calculateMessageFlag(msg *Message) int32 {
	var flag int32 = 0
	
	// 检查消息类型
	if msg.IsTransactionMessage() {
		flag |= 0x01 // 事务消息标志
	}
	if msg.IsDelayMessage() {
		flag |= 0x02 // 延迟消息标志
	}
	if msg.IsOrderedMessage() {
		flag |= 0x04 // 顺序消息标志
	}
	
	// 检查压缩
	if len(msg.Body) > 4096 { // 大于4KB的消息考虑压缩
		flag |= 0x08 // 压缩标志
	}
	
	return flag
}

// encodeProperties 编码消息属性
func (p *Producer) encodeProperties(properties map[string]string) ([]byte, error) {
	if len(properties) == 0 {
		return nil, nil
	}
	
	var buffer bytes.Buffer
	
	// 写入属性数量
	if err := binary.Write(&buffer, binary.BigEndian, int32(len(properties))); err != nil {
		return nil, err
	}
	
	// 写入每个属性
	for key, value := range properties {
		// 写入key长度和内容
		keyBytes := []byte(key)
		if err := binary.Write(&buffer, binary.BigEndian, int16(len(keyBytes))); err != nil {
			return nil, err
		}
		if _, err := buffer.Write(keyBytes); err != nil {
			return nil, err
		}
		
		// 写入value长度和内容
		valueBytes := []byte(value)
		if err := binary.Write(&buffer, binary.BigEndian, int16(len(valueBytes))); err != nil {
			return nil, err
		}
		if _, err := buffer.Write(valueBytes); err != nil {
			return nil, err
		}
	}
	
	return buffer.Bytes(), nil
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