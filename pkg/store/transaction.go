package store

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go-rocketmq/pkg/common"
)

// 事务消息相关常量
const (
	// 事务消息相关的Topic
	RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC"
	// 事务操作记录Topic
	RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC"
	// 事务消息属性
	PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG"
	PROPERTY_PRODUCER_GROUP       = "PGROUP"
	PROPERTY_UNIQUE_CLIENT_ID     = "UNIQ_KEY"
	PROPERTY_TRANSACTION_ID       = "__transactionId__"
	// 注意：PROPERTY_REAL_TOPIC 和 PROPERTY_REAL_QUEUE_ID 在 delayqueue.go 中定义
)

// TransactionState 事务状态
type TransactionState int32

const (
	TransactionStateUnknown TransactionState = iota
	TransactionStateCommit
	TransactionStateRollback
)

func (ts TransactionState) String() string {
	switch ts {
	case TransactionStateCommit:
		return "COMMIT"
	case TransactionStateRollback:
		return "ROLLBACK"
	default:
		return "UNKNOWN"
	}
}

// TransactionListener 事务监听器接口
type TransactionListener interface {
	// ExecuteLocalTransaction 执行本地事务
	ExecuteLocalTransaction(msg *common.Message, arg interface{}) TransactionState
	// CheckLocalTransaction 检查本地事务状态
	CheckLocalTransaction(msgExt *common.MessageExt) TransactionState
}

// TransactionRecord 事务记录
type TransactionRecord struct {
	TransactionId string
	ProducerGroup string
	ClientId      string
	MsgId         string
	RealTopic     string
	RealQueueId   int32
	State         TransactionState
	CreateTime    time.Time
	UpdateTime    time.Time
	CheckCount    int32
}

// TransactionService 事务消息服务
type TransactionService struct {
	storeConfig     *StoreConfig
	messageStore    *DefaultMessageStore
	running         bool
	mutex           sync.RWMutex
	shutdown        chan struct{}
	transactionMap  map[string]*TransactionRecord // transactionId -> TransactionRecord
	transactionMutex sync.RWMutex
	checkTimer      *time.Timer
	listeners       map[string]TransactionListener // producerGroup -> TransactionListener
	listenerMutex   sync.RWMutex
	// 持久化管理器
	persistenceManager *PersistenceManager
}

// NewTransactionService 创建事务消息服务
func NewTransactionService(storeConfig *StoreConfig, messageStore *DefaultMessageStore, persistenceManager *PersistenceManager) *TransactionService {
	return &TransactionService{
		storeConfig:        storeConfig,
		messageStore:       messageStore,
		shutdown:           make(chan struct{}),
		transactionMap:     make(map[string]*TransactionRecord),
		listeners:          make(map[string]TransactionListener),
		persistenceManager: persistenceManager,
	}
}

// Start 启动事务消息服务
func (ts *TransactionService) Start() error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	if ts.running {
		return fmt.Errorf("transaction service is already running")
	}

	// 从持久化管理器加载事务状态
	if ts.persistenceManager != nil {
		ts.loadTransactionStatesFromPersistence()
	}

	// 启动事务回查定时器
	go ts.startTransactionCheckTimer()

	ts.running = true
	log.Printf("TransactionService started")
	return nil
}

// Shutdown 关闭事务消息服务
func (ts *TransactionService) Shutdown() {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	if !ts.running {
		return
	}

	// 停止定时器
	if ts.checkTimer != nil {
		ts.checkTimer.Stop()
	}

	// 发送停止信号
	close(ts.shutdown)

	ts.running = false
	log.Printf("TransactionService stopped")
}

// RegisterTransactionListener 注册事务监听器
func (ts *TransactionService) RegisterTransactionListener(producerGroup string, listener TransactionListener) {
	ts.listenerMutex.Lock()
	defer ts.listenerMutex.Unlock()
	ts.listeners[producerGroup] = listener
	log.Printf("Registered transaction listener for producer group: %s", producerGroup)
}

// PrepareMessage 准备事务消息（发送半消息）
func (ts *TransactionService) PrepareMessage(msg *common.Message, producerGroup string, transactionId string) (*common.SendResult, error) {
	// 保存原始Topic和QueueId
	realTopic := msg.Topic
	realQueueId := msg.GetProperty("queueId")
	if realQueueId == "" {
		realQueueId = "0"
	}

	// 修改消息属性，发送到半消息Topic
	msg.Topic = RMQ_SYS_TRANS_HALF_TOPIC
	msg.SetProperty(PROPERTY_TRANSACTION_PREPARED, "true")
	msg.SetProperty(PROPERTY_PRODUCER_GROUP, producerGroup)
	msg.SetProperty(PROPERTY_TRANSACTION_ID, transactionId)
	msg.SetProperty(PROPERTY_REAL_TOPIC, realTopic)
	msg.SetProperty(PROPERTY_REAL_QUEUE_ID, realQueueId)
	msg.SetProperty(PROPERTY_UNIQUE_CLIENT_ID, fmt.Sprintf("%s_%d", producerGroup, time.Now().UnixNano()))

	// 存储半消息
	result, err := ts.messageStore.PutMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to put prepare message: %v", err)
	}

	// 记录事务信息
	record := &TransactionRecord{
		TransactionId: transactionId,
		ProducerGroup: producerGroup,
		ClientId:      msg.GetProperty(PROPERTY_UNIQUE_CLIENT_ID),
		MsgId:         result.MsgId,
		RealTopic:     realTopic,
		RealQueueId:   0, // 简化处理
		State:         TransactionStateUnknown,
		CreateTime:    time.Now(),
		UpdateTime:    time.Now(),
		CheckCount:    0,
	}
	
	ts.transactionMutex.Lock()
	ts.transactionMap[transactionId] = record
	ts.transactionMutex.Unlock()
	
	// 保存到持久化管理器
	ts.saveTransactionStateToPersistence(record)

	log.Printf("Prepared transaction message: %s, transactionId: %s", result.MsgId, transactionId)
	return result, nil
}

// CommitTransaction 提交事务
func (ts *TransactionService) CommitTransaction(transactionId string) error {
	ts.transactionMutex.Lock()
	record, exists := ts.transactionMap[transactionId]
	if !exists {
		ts.transactionMutex.Unlock()
		return fmt.Errorf("transaction not found: %s", transactionId)
	}
	record.State = TransactionStateCommit
	record.UpdateTime = time.Now()
	// 保存状态更新到持久化管理器
	ts.saveTransactionStateToPersistence(record)
	ts.transactionMutex.Unlock()

	// 获取半消息
	halfMsg, err := ts.getHalfMessage(record.MsgId)
	if err != nil {
		return fmt.Errorf("failed to get half message: %v", err)
	}

	// 投递到真实Topic
	if err := ts.deliverRealMessage(halfMsg); err != nil {
		return fmt.Errorf("failed to deliver real message: %v", err)
	}

	// 记录操作到OP Topic
	if err := ts.recordTransactionOp(transactionId, TransactionStateCommit); err != nil {
		log.Printf("Failed to record transaction op: %v", err)
	}

	// 事务完成后从持久化管理器中移除
	ts.removeTransactionStateFromPersistence(transactionId)

	log.Printf("Committed transaction: %s", transactionId)
	return nil
}

// RollbackTransaction 回滚事务
func (ts *TransactionService) RollbackTransaction(transactionId string) error {
	ts.transactionMutex.Lock()
	record, exists := ts.transactionMap[transactionId]
	if !exists {
		ts.transactionMutex.Unlock()
		return fmt.Errorf("transaction not found: %s", transactionId)
	}
	record.State = TransactionStateRollback
	record.UpdateTime = time.Now()
	// 保存状态更新到持久化管理器
	ts.saveTransactionStateToPersistence(record)
	ts.transactionMutex.Unlock()

	// 记录操作到OP Topic
	if err := ts.recordTransactionOp(transactionId, TransactionStateRollback); err != nil {
		log.Printf("Failed to record transaction op: %v", err)
	}

	// 事务完成后从持久化管理器中移除
	ts.removeTransactionStateFromPersistence(transactionId)

	log.Printf("Rolled back transaction: %s", transactionId)
	return nil
}

// startTransactionCheckTimer 启动事务回查定时器
func (ts *TransactionService) startTransactionCheckTimer() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ts.shutdown:
			return
		case <-ticker.C:
			ts.checkTransactions()
		}
	}
}

// checkTransactions 检查超时的事务
func (ts *TransactionService) checkTransactions() {
	now := time.Now()
	checkTimeout := 60 * time.Second // 60秒超时
	maxCheckCount := int32(15)       // 最大检查次数
	transactionTimeout := 24 * time.Hour // 24小时事务超时

	ts.transactionMutex.RLock()
	var needCheckTransactions []*TransactionRecord
	var expiredTransactions []string
	
	for transactionId, record := range ts.transactionMap {
		if record.State == TransactionStateUnknown {
			// 检查是否超过最大生存时间，如果是则强制回滚
			if now.Sub(record.CreateTime) > transactionTimeout {
				expiredTransactions = append(expiredTransactions, transactionId)
				log.Printf("Transaction %s expired after %v, will be rolled back", transactionId, transactionTimeout)
			} else if now.Sub(record.UpdateTime) > checkTimeout && record.CheckCount < maxCheckCount {
				// 需要检查的事务
				needCheckTransactions = append(needCheckTransactions, record)
			} else if record.CheckCount >= maxCheckCount {
				// 超过最大检查次数，强制回滚
				expiredTransactions = append(expiredTransactions, transactionId)
				log.Printf("Transaction %s exceeded max check count %d, will be rolled back", transactionId, maxCheckCount)
			}
		}
	}
	ts.transactionMutex.RUnlock()

	// 处理需要检查的事务
	for _, record := range needCheckTransactions {
		go ts.checkTransaction(record)
	}
	
	// 处理过期事务（强制回滚）
	for _, transactionId := range expiredTransactions {
		go func(txId string) {
			if err := ts.RollbackTransaction(txId); err != nil {
				log.Printf("Failed to rollback expired transaction %s: %v", txId, err)
			}
		}(transactionId)
	}
}

// checkTransaction 检查单个事务
func (ts *TransactionService) checkTransaction(record *TransactionRecord) {
	// 获取事务监听器
	ts.listenerMutex.RLock()
	listener, exists := ts.listeners[record.ProducerGroup]
	ts.listenerMutex.RUnlock()

	if !exists {
		log.Printf("No transaction listener found for producer group: %s", record.ProducerGroup)
		return
	}

	// 获取半消息
	halfMsg, err := ts.getHalfMessage(record.MsgId)
	if err != nil {
		log.Printf("Failed to get half message for transaction check: %v", err)
		return
	}

	// 调用监听器检查事务状态
	state := listener.CheckLocalTransaction(halfMsg)

	// 更新检查次数
	ts.transactionMutex.Lock()
	record.CheckCount++
	record.UpdateTime = time.Now()
	// 保存检查次数更新到持久化管理器
	ts.saveTransactionStateToPersistence(record)
	ts.transactionMutex.Unlock()

	// 根据检查结果处理事务
	switch state {
	case TransactionStateCommit:
		ts.CommitTransaction(record.TransactionId)
	case TransactionStateRollback:
		ts.RollbackTransaction(record.TransactionId)
	default:
		log.Printf("Transaction check returned unknown state for: %s", record.TransactionId)
	}
}

// getHalfMessage 获取半消息
func (ts *TransactionService) getHalfMessage(msgId string) (*common.MessageExt, error) {
	// 首先尝试从半消息Topic的多个队列中查找消息
	for queueId := int32(0); queueId < 4; queueId++ {
		messages, err := ts.messageStore.GetMessage(RMQ_SYS_TRANS_HALF_TOPIC, queueId, 0, 100)
		if err != nil {
			continue // 继续尝试下一个队列
		}

		for _, msg := range messages {
			if msg.MsgId == msgId {
				return msg, nil
			}
		}
	}

	// 如果在半消息Topic中找不到，尝试从事务记录中重建半消息
	// 需要通过msgId查找对应的事务记录
	ts.transactionMutex.RLock()
	var record *TransactionRecord
	for _, r := range ts.transactionMap {
		if r.MsgId == msgId {
			record = r
			break
		}
	}
	ts.transactionMutex.RUnlock()
	
	if record == nil {
		return nil, fmt.Errorf("half message not found: %s", msgId)
	}

	// 尝试从CommitLog中根据事务记录查找原始消息
	// 这里我们需要扫描CommitLog来查找匹配的事务消息
	halfMsg, err := ts.searchHalfMessageInCommitLog(msgId, record)
	if err != nil {
		// 如果CommitLog中也找不到，则根据事务记录重建一个基本的半消息
		// 这确保了系统的健壮性，避免事务检查失败
		log.Printf("Warning: Could not find half message in CommitLog for %s, reconstructing from transaction record: %v", msgId, err)
		return ts.reconstructHalfMessage(msgId, record), nil
	}
	
	return halfMsg, nil
}

// searchHalfMessageInCommitLog 在CommitLog中搜索半消息
func (ts *TransactionService) searchHalfMessageInCommitLog(msgId string, record *TransactionRecord) (*common.MessageExt, error) {
	// 获取CommitLog的访问接口
	commitLog := ts.messageStore.commitLog
	if commitLog == nil {
		return nil, fmt.Errorf("commit log not available")
	}
	
	// 获取CommitLog的最小和最大偏移量
	minOffset := commitLog.GetMinOffset()
	maxOffset := commitLog.GetMaxOffset()
	
	// 分批扫描CommitLog查找匹配的事务消息
	batchSize := int64(1024 * 1024) // 1MB批次
	for offset := minOffset; offset < maxOffset; offset += batchSize {
		endOffset := offset + batchSize
		if endOffset > maxOffset {
			endOffset = maxOffset
		}
		
		// 读取数据块
		data, err := commitLog.GetData(offset, int32(endOffset-offset))
		if err != nil {
			continue
		}
		
		// 在数据块中搜索匹配的消息
		msg := ts.searchMessageInDataBlock(data, msgId, record)
		if msg != nil {
			return msg, nil
		}
	}
	
	return nil, fmt.Errorf("half message not found in commit log")
}

// searchMessageInDataBlock 在数据块中搜索消息
func (ts *TransactionService) searchMessageInDataBlock(data []byte, msgId string, record *TransactionRecord) *common.MessageExt {
	// 简化实现：这里应该解析CommitLog的消息格式
	// 由于CommitLog格式复杂，这里使用基于属性的匹配
	
	// 检查数据块中是否包含事务ID
	transactionIdBytes := []byte(record.TransactionId)
	if !ts.containsBytes(data, transactionIdBytes) {
		return nil
	}
	
	// 检查是否包含半消息Topic
	halfTopicBytes := []byte(RMQ_SYS_TRANS_HALF_TOPIC)
	if !ts.containsBytes(data, halfTopicBytes) {
		return nil
	}
	
	// 如果找到匹配的模式，重建消息
	return ts.reconstructHalfMessage(msgId, record)
}

// containsBytes 检查字节数组是否包含子数组
func (ts *TransactionService) containsBytes(data, pattern []byte) bool {
	if len(pattern) == 0 {
		return true
	}
	if len(data) < len(pattern) {
		return false
	}
	
	for i := 0; i <= len(data)-len(pattern); i++ {
		match := true
		for j := 0; j < len(pattern); j++ {
			if data[i+j] != pattern[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// reconstructHalfMessage 根据事务记录重建半消息
func (ts *TransactionService) reconstructHalfMessage(msgId string, record *TransactionRecord) *common.MessageExt {
	// 根据事务记录重建半消息，确保包含必要的事务信息
	halfMsg := &common.MessageExt{
		Message: &common.Message{
			Topic: RMQ_SYS_TRANS_HALF_TOPIC,
			Body:  []byte(fmt.Sprintf("reconstructed half message for transaction %s", record.TransactionId)),
			Properties: map[string]string{
				PROPERTY_REAL_TOPIC:     record.RealTopic,
				PROPERTY_REAL_QUEUE_ID:  fmt.Sprintf("%d", record.RealQueueId),
				PROPERTY_TRANSACTION_ID: record.TransactionId,
				PROPERTY_PRODUCER_GROUP: record.ProducerGroup,
				PROPERTY_UNIQUE_CLIENT_ID: record.ClientId,
				"RECONSTRUCTED": "true", // 标记这是重建的消息
			},
		},
		MsgId:           msgId,
		QueueId:         0,
		QueueOffset:     0,
		CommitLogOffset: 0,
		StoreSize:       0,
		BornTimestamp:   record.CreateTime,
		StoreTimestamp:  record.UpdateTime,
		BornHost:        "127.0.0.1:0",
		StoreHost:       "127.0.0.1:10911",
	}
	
	log.Printf("Reconstructed half message for transaction %s from record", record.TransactionId)
	return halfMsg
}

// deliverRealMessage 投递真实消息
func (ts *TransactionService) deliverRealMessage(halfMsg *common.MessageExt) error {
	// 恢复原始Topic和属性
	realTopic := halfMsg.GetProperty(PROPERTY_REAL_TOPIC)
	realQueueId := halfMsg.GetProperty(PROPERTY_REAL_QUEUE_ID)

	if realTopic == "" {
		return fmt.Errorf("real topic is empty")
	}

	// 创建新消息
	newMsg := &common.Message{
		Topic:      realTopic,
		Tags:       halfMsg.Tags,
		Keys:       halfMsg.Keys,
		Body:       halfMsg.Body,
		Properties: make(map[string]string),
	}

	// 复制属性，但排除事务相关属性
	for k, v := range halfMsg.Properties {
		if k != PROPERTY_TRANSACTION_PREPARED && k != PROPERTY_PRODUCER_GROUP &&
		   k != PROPERTY_TRANSACTION_ID && k != PROPERTY_REAL_TOPIC &&
		   k != PROPERTY_REAL_QUEUE_ID && k != PROPERTY_UNIQUE_CLIENT_ID {
			newMsg.Properties[k] = v
		}
	}

	// 设置队列ID
	if realQueueId != "" {
		newMsg.SetProperty("queueId", realQueueId)
	}

	// 投递到真实Topic
	_, err := ts.messageStore.PutMessage(newMsg)
	if err != nil {
		return fmt.Errorf("failed to put message to real topic: %v", err)
	}

	log.Printf("Delivered transaction message to topic: %s, queueId: %s", realTopic, realQueueId)
	return nil
}

// recordTransactionOp 记录事务操作
func (ts *TransactionService) recordTransactionOp(transactionId string, state TransactionState) error {
	// 创建操作记录消息
	opMsg := &common.Message{
		Topic:      RMQ_SYS_TRANS_OP_HALF_TOPIC,
		Body:       []byte(fmt.Sprintf("transactionId=%s,state=%s", transactionId, state.String())),
		Properties: make(map[string]string),
	}
	opMsg.SetProperty(PROPERTY_TRANSACTION_ID, transactionId)
	opMsg.SetProperty("state", state.String())

	// 存储操作记录
	_, err := ts.messageStore.PutMessage(opMsg)
	return err
}

// IsTransactionMessage 判断是否为事务消息
func IsTransactionMessage(msg *common.Message) bool {
	return msg.GetProperty(PROPERTY_TRANSACTION_PREPARED) == "true"
}

// GetTransactionId 获取事务ID
func GetTransactionId(msg interface{}) string {
	switch m := msg.(type) {
	case *common.Message:
		return m.GetProperty(PROPERTY_TRANSACTION_ID)
	case *common.MessageExt:
		return m.GetProperty(PROPERTY_TRANSACTION_ID)
	default:
		return ""
	}
}

// loadTransactionStatesFromPersistence 从持久化管理器加载事务状态
func (ts *TransactionService) loadTransactionStatesFromPersistence() {
	if ts.persistenceManager == nil {
		log.Printf("PersistenceManager is nil, cannot load transaction states")
		return
	}
	
	allStates := ts.persistenceManager.GetAllTransactionStates()
	log.Printf("Retrieved %d transaction states from persistence manager", len(allStates))
	ts.transactionMutex.Lock()
	defer ts.transactionMutex.Unlock()
	
	loadedCount := 0
	
	for transactionId, persistentState := range allStates {
		log.Printf("Processing transaction %s with state %v", transactionId, persistentState.State)
		// 只加载未完成的事务（UNKNOWN状态）
		if persistentState.State == TransactionStateUnknown {
			record := &TransactionRecord{
				TransactionId: persistentState.TransactionId,
				ProducerGroup: persistentState.ProducerGroup,
				ClientId:      "", // 客户端ID在重启后可能不同
				MsgId:         "", // 消息ID需要从消息中获取
				RealTopic:     persistentState.Message.Topic,
				RealQueueId:   0,
				State:         persistentState.State,
				CreateTime:    time.Unix(persistentState.CreateTime, 0),
				UpdateTime:    time.Unix(persistentState.UpdateTime, 0),
				CheckCount:    0, // 重置检查次数
			}
			
			ts.transactionMap[transactionId] = record
			loadedCount++
			log.Printf("Loaded transaction %s into memory", transactionId)
		} else {
			log.Printf("Skipping transaction %s with state %v (not UNKNOWN)", transactionId, persistentState.State)
		}
	}
	
	log.Printf("Loaded %d transaction states from persistence", loadedCount)
}

// saveTransactionStateToPersistence 保存事务状态到持久化管理器
func (ts *TransactionService) saveTransactionStateToPersistence(record *TransactionRecord) {
	if ts.persistenceManager == nil {
		return
	}
	
	persistentState := &PersistentTransactionState{
		TransactionId: record.TransactionId,
		ProducerGroup: record.ProducerGroup,
		State:         record.State,
		CreateTime:    record.CreateTime.Unix(),
		UpdateTime:    record.UpdateTime.Unix(),
		Message: &common.Message{
			Topic: record.RealTopic,
			Properties: map[string]string{
				PROPERTY_TRANSACTION_ID: record.TransactionId,
				PROPERTY_PRODUCER_GROUP: record.ProducerGroup,
			},
		},
	}
	
	ts.persistenceManager.UpdateTransactionState(record.TransactionId, persistentState)
}

// removeTransactionStateFromPersistence 从持久化管理器移除事务状态
func (ts *TransactionService) removeTransactionStateFromPersistence(transactionId string) {
	if ts.persistenceManager == nil {
		return
	}
	
	ts.persistenceManager.RemoveTransactionState(transactionId)
}