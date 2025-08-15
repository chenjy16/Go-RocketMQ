package store

import (
	"fmt"
	"os"
	"sync"
	"time"

	"go-rocketmq/pkg/common"
)

// StoreConfig 存储配置
type StoreConfig struct {
	// 存储根目录
	StorePathRootDir string
	// CommitLog存储目录
	StorePathCommitLog string
	// ConsumeQueue存储目录
	StorePathConsumeQueue string
	// Index存储目录
	StorePathIndex string
	
	// 文件大小配置
	MapedFileSizeCommitLog   int64 // CommitLog文件大小，默认1GB
	MapedFileSizeConsumeQueue int64 // ConsumeQueue文件大小，默认300万条记录
	MapedFileSizeIndexFile   int64 // IndexFile文件大小，默认400万条记录
	
	// 刷盘配置
	FlushDiskType            FlushDiskType // 刷盘方式
	FlushIntervalCommitLog   int           // CommitLog刷盘间隔(ms)
	FlushCommitLogLeastPages int           // CommitLog刷盘最少页数
	FlushConsumeQueueLeastPages int        // ConsumeQueue刷盘最少页数
	FlushIntervalConsumeQueue int          // ConsumeQueue刷盘间隔(ms)
	
	// 文件保留配置
	FileReservedTime int // 文件保留时间(小时)
	DeleteWhen       string // 删除文件的时间点
	DiskMaxUsedSpaceRatio int // 磁盘最大使用比例
	
	// 其他配置
	TransientStorePoolEnable bool // 是否启用堆外内存
	TransientStorePoolSize   int  // 堆外内存池大小
	FastFailIfNoBufferInStorePool bool // 如果内存池没有缓冲区是否快速失败
}

// FlushDiskType 刷盘类型
type FlushDiskType int

const (
	// ASYNC_FLUSH 异步刷盘
	ASYNC_FLUSH FlushDiskType = iota
	// SYNC_FLUSH 同步刷盘
	SYNC_FLUSH
)

// DefaultMessageStore 默认消息存储实现
type DefaultMessageStore struct {
	storeConfig *StoreConfig
	
	// 核心存储组件
	commitLog     *CommitLog
	consumeQueueTable map[string]*ConsumeQueue // topic -> ConsumeQueue
	indexService  *IndexService
	
	// 新增功能服务
	delayQueueService    *DelayQueueService
	transactionService   *TransactionService
	orderedQueueService  *OrderedQueueService
	
	// 运行状态
	running bool
	mutex   sync.RWMutex
	
	// 停止信号
	shutdown chan struct{}
}

// NewDefaultMessageStore 创建默认消息存储
func NewDefaultMessageStore(config *StoreConfig) (*DefaultMessageStore, error) {
	if config == nil {
		config = NewDefaultStoreConfig()
	}
	
	// 创建存储目录
	if err := createStoreDirectories(config); err != nil {
		return nil, fmt.Errorf("failed to create store directories: %v", err)
	}
	
	store := &DefaultMessageStore{
		storeConfig:       config,
		consumeQueueTable: make(map[string]*ConsumeQueue),
		shutdown:          make(chan struct{}),
	}
	
	// 初始化CommitLog
	commitLog, err := NewCommitLog(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create commit log: %v", err)
	}
	store.commitLog = commitLog
	
	// 初始化IndexService
	store.indexService = NewIndexService(config.StorePathIndex)
	
	// 初始化延迟队列服务
	store.delayQueueService = NewDelayQueueService(config, store)
	
	// 初始化事务消息服务
	store.transactionService = NewTransactionService(config, store)
	
	// 初始化顺序队列服务
	store.orderedQueueService = NewOrderedQueueService(config, store)
	
	return store, nil
}

// NewDefaultStoreConfig 创建默认存储配置
func NewDefaultStoreConfig() *StoreConfig {
	return &StoreConfig{
		StorePathRootDir:      "./store",
		StorePathCommitLog:    "./store/commitlog",
		StorePathConsumeQueue: "./store/consumequeue",
		StorePathIndex:        "./store/index",
		
		MapedFileSizeCommitLog:   1024 * 1024 * 1024, // 1GB
		MapedFileSizeConsumeQueue: 300000 * 20,        // 300万条记录 * 20字节
		MapedFileSizeIndexFile:   400000 * 400,        // 400万条记录 * 400字节
		
		FlushDiskType:               ASYNC_FLUSH,
		FlushIntervalCommitLog:      500,  // 500ms
		FlushCommitLogLeastPages:    4,    // 4页
		FlushConsumeQueueLeastPages: 2,    // 2页
		FlushIntervalConsumeQueue:   1000, // 1000ms
		
		FileReservedTime:      72,   // 72小时
		DeleteWhen:            "04", // 凌晨4点
		DiskMaxUsedSpaceRatio: 75,   // 75%
		
		TransientStorePoolEnable:      false,
		TransientStorePoolSize:        5,
		FastFailIfNoBufferInStorePool: false,
	}
}

// createStoreDirectories 创建存储目录
func createStoreDirectories(config *StoreConfig) error {
	dirs := []string{
		config.StorePathRootDir,
		config.StorePathCommitLog,
		config.StorePathConsumeQueue,
		config.StorePathIndex,
	}
	
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}
	
	return nil
}

// Start 启动消息存储
func (store *DefaultMessageStore) Start() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	
	if store.running {
		return fmt.Errorf("message store is already running")
	}
	
	// 启动CommitLog
	if err := store.commitLog.Start(); err != nil {
		return fmt.Errorf("failed to start commit log: %v", err)
	}
	
	// 启动IndexService
	if err := store.indexService.Start(); err != nil {
		return fmt.Errorf("failed to start index service: %v", err)
	}
	
	// 启动延迟队列服务
	if err := store.delayQueueService.Start(); err != nil {
		return fmt.Errorf("failed to start delay queue service: %v", err)
	}
	
	// 启动事务消息服务
	if err := store.transactionService.Start(); err != nil {
		return fmt.Errorf("failed to start transaction service: %v", err)
	}
	
	// 启动顺序队列服务
	if err := store.orderedQueueService.Start(); err != nil {
		return fmt.Errorf("failed to start ordered queue service: %v", err)
	}
	
	store.running = true
	return nil
}

// Shutdown 关闭消息存储
func (store *DefaultMessageStore) Shutdown() {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	
	if !store.running {
		return
	}
	
	// 发送停止信号
	close(store.shutdown)
	
	// 停止新增服务
	store.delayQueueService.Shutdown()
	store.transactionService.Shutdown()
	store.orderedQueueService.Shutdown()
	
	// 停止IndexService
	store.indexService.Shutdown()
	
	// 停止CommitLog
	store.commitLog.Shutdown()
	
	// 停止所有ConsumeQueue
	for _, cq := range store.consumeQueueTable {
		cq.Shutdown()
	}
	
	store.running = false
}

// PutMessage 存储消息
func (store *DefaultMessageStore) PutMessage(msg *common.Message) (*common.SendResult, error) {
	if !store.running {
		return nil, fmt.Errorf("message store is not running")
	}
	
	if msg == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}
	
	if msg.Topic == "" {
		return nil, fmt.Errorf("message topic cannot be empty")
	}
	
	// 构建消息扩展信息
	msgExt := &common.MessageExt{
		Message:        msg,
		QueueId:        0, // 简化版本，使用固定队列ID
		StoreSize:      0, // 将在CommitLog中计算
		QueueOffset:    0, // 将在ConsumeQueue中计算
		SysFlag:        0,
		BornTimestamp:  time.Now(),
		StoreTimestamp: time.Now(),
		BornHost:       "127.0.0.1:0",
		StoreHost:      "127.0.0.1:10911",
	}
	
	// 存储到CommitLog
	result, err := store.commitLog.PutMessage(msgExt)
	if err != nil {
		return nil, fmt.Errorf("failed to put message to commit log: %v", err)
	}
	
	// 更新ConsumeQueue
	if err := store.updateConsumeQueue(msgExt, result); err != nil {
		return nil, fmt.Errorf("failed to update consume queue: %v", err)
	}
	
	// 更新Index
	if err := store.updateIndex(msgExt, result); err != nil {
		// Index更新失败不影响消息存储
		fmt.Printf("Warning: failed to build index: %v\n", err)
	}
	
	return result, nil
}

// updateIndex 更新索引
func (store *DefaultMessageStore) updateIndex(msgExt *common.MessageExt, result *common.SendResult) error {
	// 构建索引key
	keys := make([]string, 0)
	
	// 添加消息Key
	if msgExt.Keys != "" {
		keys = append(keys, msgExt.Keys)
	}
	
	// 添加UniqKey
	if uniqKey := msgExt.GetProperty("UNIQ_KEY"); uniqKey != "" {
		keys = append(keys, uniqKey)
	}
	
	// 构建索引
	for _, key := range keys {
		store.indexService.BuildIndex(key, msgExt.CommitLogOffset, msgExt.StoreTimestamp.UnixMilli())
	}
	
	return nil
}

// tagsString2tagsCode 将标签字符串转换为哈希码
func tagsString2tagsCode(tags string) uint32 {
	// 简单的哈希算法
	hash := uint32(0)
	for _, c := range tags {
		hash = hash*31 + uint32(c)
	}
	return hash
}

// updateConsumeQueue 更新ConsumeQueue
func (store *DefaultMessageStore) updateConsumeQueue(msgExt *common.MessageExt, result *common.SendResult) error {
	// 获取或创建ConsumeQueue
	cq := store.getOrCreateConsumeQueue(msgExt.Topic, msgExt.QueueId)
	if cq == nil {
		return fmt.Errorf("failed to get consume queue for topic %s, queueId %d", msgExt.Topic, msgExt.QueueId)
	}
	
	// 计算Tag哈希码
	tagsCode := int64(0)
	if msgExt.Tags != "" {
		tagsCode = int64(tagsString2tagsCode(msgExt.Tags))
	}
	
	// 添加到ConsumeQueue
	return cq.PutMessagePositionInfo(msgExt.CommitLogOffset, msgExt.StoreSize, tagsCode)
}

// getOrCreateConsumeQueue 获取或创建ConsumeQueue
func (store *DefaultMessageStore) getOrCreateConsumeQueue(topic string, queueId int32) *ConsumeQueue {
	key := fmt.Sprintf("%s-%d", topic, queueId)
	
	store.mutex.RLock()
	cq, exists := store.consumeQueueTable[key]
	store.mutex.RUnlock()
	
	if exists {
		return cq
	}
	
	store.mutex.Lock()
	defer store.mutex.Unlock()
	
	// 双重检查
	if cq, exists = store.consumeQueueTable[key]; exists {
		return cq
	}
	
	// 创建新的ConsumeQueue
	cq = NewConsumeQueue(topic, queueId, store.storeConfig.StorePathConsumeQueue, store.storeConfig.MapedFileSizeConsumeQueue)
	
	store.consumeQueueTable[key] = cq
	return cq
}

// GetMessage 获取消息
func (store *DefaultMessageStore) GetMessage(topic string, queueId int32, offset int64, maxMsgNums int32) ([]*common.MessageExt, error) {
	if !store.running {
		return nil, fmt.Errorf("message store is not running")
	}
	
	// 获取ConsumeQueue
	cq := store.getOrCreateConsumeQueue(topic, queueId)
	if cq == nil {
		return nil, fmt.Errorf("consume queue not found for topic %s, queueId %d", topic, queueId)
	}
	
	var messages []*common.MessageExt
	for i := int32(0); i < maxMsgNums; i++ {
		// 从ConsumeQueue获取消息位置信息
		position, err := cq.GetIndexBuffer(offset + int64(i))
		if err != nil {
			break
		}
		
		// 从CommitLog读取消息
		msg, err := store.commitLog.GetMessage(position.Offset, position.Size)
		if err != nil {
			continue
		}
		
		messages = append(messages, msg)
	}
	
	return messages, nil
}

// GetMaxOffsetInQueue 获取队列中的最大偏移量
func (store *DefaultMessageStore) GetMaxOffsetInQueue(topic string, queueId int32) int64 {
	cq := store.getOrCreateConsumeQueue(topic, queueId)
	if cq == nil {
		return 0
	}
	return cq.GetMaxOffsetInQueue()
}

// GetMinOffsetInQueue 获取队列中的最小偏移量
func (store *DefaultMessageStore) GetMinOffsetInQueue(topic string, queueId int32) int64 {
	cq := store.getOrCreateConsumeQueue(topic, queueId)
	if cq == nil {
		return 0
	}
	return cq.GetMinOffsetInQueue()
}

// ========== 延迟消息相关方法 ==========

// PutDelayMessage 存储延迟消息
func (store *DefaultMessageStore) PutDelayMessage(msg *common.Message, delayLevel int32) (*common.SendResult, error) {
	return store.delayQueueService.PutDelayMessage(msg, delayLevel)
}

// ========== 事务消息相关方法 ==========

// RegisterTransactionListener 注册事务监听器
func (store *DefaultMessageStore) RegisterTransactionListener(producerGroup string, listener TransactionListener) {
	store.transactionService.RegisterTransactionListener(producerGroup, listener)
}

// PrepareMessage 准备事务消息
func (store *DefaultMessageStore) PrepareMessage(msg *common.Message, producerGroup string, transactionId string) (*common.SendResult, error) {
	return store.transactionService.PrepareMessage(msg, producerGroup, transactionId)
}

// CommitTransaction 提交事务
func (store *DefaultMessageStore) CommitTransaction(transactionId string) error {
	return store.transactionService.CommitTransaction(transactionId)
}

// RollbackTransaction 回滚事务
func (store *DefaultMessageStore) RollbackTransaction(transactionId string) error {
	return store.transactionService.RollbackTransaction(transactionId)
}

// ========== 顺序消息相关方法 ==========

// PutOrderedMessage 存储顺序消息
func (store *DefaultMessageStore) PutOrderedMessage(msg *common.Message, shardingKey string) (*common.SendResult, error) {
	return store.orderedQueueService.PutOrderedMessage(msg, shardingKey)
}

// PullOrderedMessage 拉取顺序消息
func (store *DefaultMessageStore) PullOrderedMessage(topic string, queueId int32, consumerGroup string, maxNums int32) ([]*common.MessageExt, error) {
	return store.orderedQueueService.PullOrderedMessage(topic, queueId, consumerGroup, maxNums)
}

// CommitConsumeOffset 提交消费进度
func (store *DefaultMessageStore) CommitConsumeOffset(topic string, queueId int32, consumerGroup string, offset int64) error {
	return store.orderedQueueService.CommitConsumeOffset(topic, queueId, consumerGroup, offset)
}

// GetConsumeOffset 获取消费进度
func (store *DefaultMessageStore) GetConsumeOffset(topic string, queueId int32, consumerGroup string) int64 {
	return store.orderedQueueService.GetConsumeOffset(topic, queueId, consumerGroup)
}

// GetCommitLog 获取CommitLog实例
func (store *DefaultMessageStore) GetCommitLog() *CommitLog {
	return store.commitLog
}