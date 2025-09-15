package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	common "github.com/chenjy16/go-rocketmq-common"
)

// MessageTrace 消息轨迹结构
type MessageTrace struct {
	MsgId      string            `json:"msgId"`
	Topic      string            `json:"topic"`
	Tags       string            `json:"tags"`
	Keys       string            `json:"keys"`
	QueueId    int32             `json:"queueId"`
	Offset     int64             `json:"offset"`
	StoreTime  int64             `json:"storeTime"`
	BodySize   int32             `json:"bodySize"`
	Status     string            `json:"status"`
	Properties map[string]string `json:"properties"`
}

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
	MapedFileSizeCommitLog    int64 // CommitLog文件大小，默认1GB
	MapedFileSizeConsumeQueue int64 // ConsumeQueue文件大小，默认300万条记录
	MapedFileSizeIndexFile    int64 // IndexFile文件大小，默认400万条记录

	// 刷盘配置
	FlushDiskType               FlushDiskType // 刷盘方式
	FlushIntervalCommitLog      int           // CommitLog刷盘间隔(ms)
	FlushCommitLogLeastPages    int           // CommitLog刷盘最少页数
	FlushConsumeQueueLeastPages int           // ConsumeQueue刷盘最少页数
	FlushIntervalConsumeQueue   int           // ConsumeQueue刷盘间隔(ms)

	// 文件保留配置
	FileReservedTime      int    // 文件保留时间(小时)
	DeleteWhen            string // 删除文件的时间点
	DiskMaxUsedSpaceRatio int    // 磁盘最大使用比例

	// 其他配置
	TransientStorePoolEnable      bool // 是否启用堆外内存
	TransientStorePoolSize        int  // 堆外内存池大小
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

// StoreStats 存储统计信息
type StoreStats struct {
	// 消息统计
	TotalMessagesPut int64         // 总写入消息数
	TotalMessagesGet int64         // 总读取消息数
	TotalPutLatency  time.Duration // 总写入延迟
	TotalGetLatency  time.Duration // 总读取延迟
	AvgPutLatency    time.Duration // 平均写入延迟
	AvgGetLatency    time.Duration // 平均读取延迟

	// 磁盘统计
	TotalDiskWriteBytes int64 // 总磁盘写入字节数
	TotalDiskReadBytes  int64 // 总磁盘读取字节数
	DiskUsagePercent    int64 // 磁盘使用百分比

	// 队列统计
	ActiveQueues     int64            // 活跃队列数
	TotalQueues      int64            // 总队列数
	MessagesPerQueue map[string]int64 // 每个队列的消息数

	// 错误统计
	TotalPutErrors int64     // 总写入错误数
	TotalGetErrors int64     // 总读取错误数
	LastErrorTime  time.Time // 最后错误时间
	LastError      string    // 最后错误信息

	// 时间戳
	StartTime      time.Time // 启动时间
	LastUpdateTime time.Time // 最后更新时间

	mutex sync.RWMutex
}

// PerformanceMonitor 性能监控器
type PerformanceMonitor struct {
	stats *StoreStats
}

// NewPerformanceMonitor 创建性能监控器
func NewPerformanceMonitor() *PerformanceMonitor {
	return &PerformanceMonitor{
		stats: &StoreStats{
			StartTime:        time.Now(),
			MessagesPerQueue: make(map[string]int64),
		},
	}
}

// UpdatePutStats 更新写入统计
func (pm *PerformanceMonitor) UpdatePutStats(latency time.Duration, messageSize int, err error) {
	pm.stats.mutex.Lock()
	defer pm.stats.mutex.Unlock()

	pm.stats.TotalMessagesPut++
	pm.stats.TotalPutLatency += latency
	pm.stats.AvgPutLatency = pm.stats.TotalPutLatency / time.Duration(pm.stats.TotalMessagesPut)
	pm.stats.TotalDiskWriteBytes += int64(messageSize)
	pm.stats.LastUpdateTime = time.Now()

	if err != nil {
		pm.stats.TotalPutErrors++
		pm.stats.LastErrorTime = time.Now()
		pm.stats.LastError = err.Error()
	}
}

// UpdateGetStats 更新读取统计
func (pm *PerformanceMonitor) UpdateGetStats(latency time.Duration, messageSize int, err error) {
	pm.stats.mutex.Lock()
	defer pm.stats.mutex.Unlock()

	pm.stats.TotalMessagesGet++
	pm.stats.TotalGetLatency += latency
	pm.stats.AvgGetLatency = pm.stats.TotalGetLatency / time.Duration(pm.stats.TotalMessagesGet)
	pm.stats.TotalDiskReadBytes += int64(messageSize)
	pm.stats.LastUpdateTime = time.Now()

	if err != nil {
		pm.stats.TotalGetErrors++
		pm.stats.LastErrorTime = time.Now()
		pm.stats.LastError = err.Error()
	}
}

// UpdateQueueStats 更新队列统计
func (pm *PerformanceMonitor) UpdateQueueStats(topic string, queueId int32, messageCount int64) {
	pm.stats.mutex.Lock()
	defer pm.stats.mutex.Unlock()

	queueKey := fmt.Sprintf("%s-%d", topic, queueId)
	pm.stats.MessagesPerQueue[queueKey] = messageCount
	pm.stats.TotalQueues = int64(len(pm.stats.MessagesPerQueue))

	// 计算活跃队列数（有消息的队列）
	var activeQueues int64
	for _, count := range pm.stats.MessagesPerQueue {
		if count > 0 {
			activeQueues++
		}
	}
	pm.stats.ActiveQueues = activeQueues
}

// UpdateDiskStats 更新磁盘统计
func (pm *PerformanceMonitor) UpdateDiskStats(usagePercent int64) {
	pm.stats.mutex.Lock()
	defer pm.stats.mutex.Unlock()

	pm.stats.DiskUsagePercent = usagePercent
	pm.stats.LastUpdateTime = time.Now()
}

// GetStats 获取统计信息
func (pm *PerformanceMonitor) GetStats() *StoreStats {
	pm.stats.mutex.RLock()
	defer pm.stats.mutex.RUnlock()

	// 创建统计信息副本
	statsCopy := &StoreStats{
		TotalMessagesPut:    pm.stats.TotalMessagesPut,
		TotalMessagesGet:    pm.stats.TotalMessagesGet,
		TotalPutLatency:     pm.stats.TotalPutLatency,
		TotalGetLatency:     pm.stats.TotalGetLatency,
		AvgPutLatency:       pm.stats.AvgPutLatency,
		AvgGetLatency:       pm.stats.AvgGetLatency,
		TotalDiskWriteBytes: pm.stats.TotalDiskWriteBytes,
		TotalDiskReadBytes:  pm.stats.TotalDiskReadBytes,
		DiskUsagePercent:    pm.stats.DiskUsagePercent,
		ActiveQueues:        pm.stats.ActiveQueues,
		TotalQueues:         pm.stats.TotalQueues,
		TotalPutErrors:      pm.stats.TotalPutErrors,
		TotalGetErrors:      pm.stats.TotalGetErrors,
		LastErrorTime:       pm.stats.LastErrorTime,
		LastError:           pm.stats.LastError,
		StartTime:           pm.stats.StartTime,
		LastUpdateTime:      pm.stats.LastUpdateTime,
		MessagesPerQueue:    make(map[string]int64),
	}

	// 复制队列消息数
	for k, v := range pm.stats.MessagesPerQueue {
		statsCopy.MessagesPerQueue[k] = v
	}

	return statsCopy
}

// ResetStats 重置统计信息
func (pm *PerformanceMonitor) ResetStats() {
	pm.stats.mutex.Lock()
	defer pm.stats.mutex.Unlock()

	pm.stats.TotalMessagesPut = 0
	pm.stats.TotalMessagesGet = 0
	pm.stats.TotalPutLatency = 0
	pm.stats.TotalGetLatency = 0
	pm.stats.AvgPutLatency = 0
	pm.stats.AvgGetLatency = 0
	pm.stats.TotalDiskWriteBytes = 0
	pm.stats.TotalDiskReadBytes = 0
	pm.stats.DiskUsagePercent = 0
	pm.stats.ActiveQueues = 0
	pm.stats.TotalQueues = 0
	pm.stats.TotalPutErrors = 0
	pm.stats.TotalGetErrors = 0
	pm.stats.LastErrorTime = time.Time{}
	pm.stats.LastError = ""
	pm.stats.MessagesPerQueue = make(map[string]int64)
}

// DefaultMessageStore 默认消息存储实现
type DefaultMessageStore struct {
	storeConfig *StoreConfig

	// 存储组件
	commitLog         *CommitLog
	consumeQueueTable map[string]*ConsumeQueue // topic -> ConsumeQueue
	indexService      *IndexService

	// 高级功能服务
	delayQueueService   *DelayQueueService
	transactionService  *TransactionService
	orderedQueueService *OrderedQueueService
	persistenceManager  *PersistenceManager

	// 控制字段
	running      bool
	shutdownOnce sync.Once
	mutex        sync.RWMutex

	// 停止信号
	shutdown chan struct{}

	// 队列选择计数器
	queueSelector int32

	// 性能监控
	performanceMonitor *PerformanceMonitor
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
		storeConfig:        config,
		consumeQueueTable:  make(map[string]*ConsumeQueue),
		shutdown:           make(chan struct{}),
		performanceMonitor: NewPerformanceMonitor(),
	}

	// 初始化CommitLog
	commitLog, err := NewCommitLog(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create commit log: %v", err)
	}
	store.commitLog = commitLog

	// 初始化IndexService
	store.indexService = NewIndexService(config.StorePathIndex)

	// 初始化持久化管理器（必须在其他服务之前初始化）
	store.persistenceManager = NewPersistenceManager(config)

	// 初始化延迟队列服务
	store.delayQueueService = NewDelayQueueService(config, store)

	// 初始化事务消息服务
	store.transactionService = NewTransactionService(config, store, store.persistenceManager)

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

		MapedFileSizeCommitLog:    1024 * 1024 * 1024, // 1GB
		MapedFileSizeConsumeQueue: 300000 * 20,        // 300万条记录 * 20字节
		MapedFileSizeIndexFile:    400000 * 400,       // 400万条记录 * 400字节

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

	// 启动持久化管理器（必须在其他服务之前启动）
	if err := store.persistenceManager.Start(); err != nil {
		return fmt.Errorf("failed to start persistence manager: %v", err)
	}

	// 恢复ConsumeQueue
	if err := store.recoverConsumeQueues(); err != nil {
		return fmt.Errorf("failed to recover consume queues: %v", err)
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
	store.shutdownOnce.Do(func() {
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

		// 停止持久化管理器
		store.persistenceManager.Stop()

		// 停止IndexService
		store.indexService.Shutdown()

		// 停止CommitLog
		store.commitLog.Shutdown()

		// 停止所有ConsumeQueue
		for _, cq := range store.consumeQueueTable {
			cq.Shutdown()
		}

		store.running = false
	})
}

// PutMessage 存储消息
func (store *DefaultMessageStore) PutMessage(msg *common.Message) (*common.SendResult, error) {
	startTime := time.Now()

	// 选择队列ID（轮询方式）
	queueId := atomic.AddInt32(&store.queueSelector, 1) % 4 // 使用4个队列
	result, err := store.PutMessageToQueue(msg, queueId)

	// 更新性能统计
	latency := time.Since(startTime)
	messageSize := 0
	if msg != nil {
		messageSize = len(msg.Body)
	}
	store.performanceMonitor.UpdatePutStats(latency, messageSize, err)

	// 更新队列统计
	if result != nil {
		store.performanceMonitor.UpdateQueueStats(msg.Topic, queueId, 1) // 简化处理，实际应该获取队列中的消息数
	}

	return result, err
}

// PutMessageToQueue 将消息存储到指定队列
func (store *DefaultMessageStore) PutMessageToQueue(msg *common.Message, queueId int32) (*common.SendResult, error) {
	if !store.running {
		err := fmt.Errorf("message store is not running")
		store.performanceMonitor.UpdatePutStats(0, 0, err)
		return nil, err
	}

	if msg == nil {
		err := fmt.Errorf("message cannot be nil")
		store.performanceMonitor.UpdatePutStats(0, 0, err)
		return nil, err
	}

	if msg.Topic == "" {
		err := fmt.Errorf("message topic cannot be empty")
		store.performanceMonitor.UpdatePutStats(0, 0, err)
		return nil, err
	}

	// 构建消息扩展信息
	msgExt := &common.MessageExt{
		Message:        msg,
		QueueId:        queueId,
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
		err = fmt.Errorf("failed to put message to commit log: %v", err)
		store.performanceMonitor.UpdatePutStats(0, 0, err)
		return nil, err
	}

	// 更新ConsumeQueue
	if err := store.updateConsumeQueue(msgExt, result); err != nil {
		err = fmt.Errorf("failed to update consume queue: %v", err)
		store.performanceMonitor.UpdatePutStats(0, 0, err)
		return nil, err
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

	// 添加消息ID作为索引key
	if result.MsgId != "" {
		keys = append(keys, result.MsgId)
	}

	// 构建索引
	for _, key := range keys {
		store.indexService.BuildIndex(key, msgExt.CommitLogOffset, msgExt.StoreTimestamp.UnixMilli())

		// 同时添加到持久化管理器的消息索引
		msgIndex := &MessageIndex{
			MessageKey: key,
			Topic:      msgExt.Topic,
			QueueId:    msgExt.QueueId,
			Offset:     msgExt.QueueOffset,
			StoreTime:  msgExt.StoreTimestamp.UnixMilli(),
			Tags:       msgExt.Tags,
		}
		store.persistenceManager.AddMessageIndex(key, msgIndex)
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
	startTime := time.Now()

	messages, err := store.getMessageInternal(topic, queueId, offset, maxMsgNums)

	// 更新性能统计
	latency := time.Since(startTime)
	messageSize := 0
	for _, msg := range messages {
		if msg != nil {
			messageSize += len(msg.Body)
		}
	}
	store.performanceMonitor.UpdateGetStats(latency, messageSize, err)

	return messages, err
}

// getMessageInternal 内部获取消息方法
func (store *DefaultMessageStore) getMessageInternal(topic string, queueId int32, offset int64, maxMsgNums int32) ([]*common.MessageExt, error) {
	if !store.running {
		return nil, fmt.Errorf("message store is not running")
	}

	// 检查ConsumeQueue是否存在（不自动创建）
	store.mutex.RLock()
	key := fmt.Sprintf("%s-%d", topic, queueId)
	cq, exists := store.consumeQueueTable[key]
	store.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic %s with queueId %d not found", topic, queueId)
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
	// 更新持久化管理器中的消费进度
	store.persistenceManager.UpdateConsumeProgress(topic, queueId, consumerGroup, offset)
	// 同时更新顺序队列服务中的消费进度
	return store.orderedQueueService.CommitConsumeOffset(topic, queueId, consumerGroup, offset)
}

// GetConsumeOffset 获取消费进度
func (store *DefaultMessageStore) GetConsumeOffset(topic string, queueId int32, consumerGroup string) int64 {
	// 优先从持久化管理器获取消费进度
	offset := store.persistenceManager.GetConsumeProgress(topic, queueId, consumerGroup)
	if offset != -1 {
		return offset
	}
	// 如果持久化管理器中没有，则从顺序队列服务获取
	return store.orderedQueueService.GetConsumeOffset(topic, queueId, consumerGroup)
}

// GetCommitLog 获取CommitLog实例
func (store *DefaultMessageStore) GetCommitLog() *CommitLog {
	return store.commitLog
}

// ========== 索引查询相关方法 ==========

// QueryMessageByKey 根据Key查询消息
func (store *DefaultMessageStore) QueryMessageByKey(topic, key string, maxNum int32, begin, end int64) ([]*common.MessageExt, error) {
	// 从IndexService查询物理偏移量
	offsets, err := store.indexService.QueryOffset(topic, key, maxNum, begin, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query offsets by key: %v", err)
	}

	var messages []*common.MessageExt
	for _, offset := range offsets {
		// 从CommitLog读取消息，需要先获取消息大小
		// 这里简化处理，假设消息大小不超过64KB
		msg, err := store.commitLog.GetMessage(offset, 65536)
		if err != nil {
			continue // 跳过读取失败的消息
		}

		// 验证消息是否匹配条件
		if msg.Topic == topic && (msg.Keys == key || msg.GetProperty("UNIQ_KEY") == key) {
			// 检查时间范围
			if msg.StoreTimestamp.UnixMilli() >= begin && msg.StoreTimestamp.UnixMilli() <= end {
				messages = append(messages, msg)
			}
		}

		if int32(len(messages)) >= maxNum {
			break
		}
	}

	return messages, nil
}

// QueryMessageByTimeRange 根据时间范围查询消息
func (store *DefaultMessageStore) QueryMessageByTimeRange(topic string, startTime, endTime int64, maxNum int32) ([]*common.MessageExt, error) {
	// 从持久化管理器查询消息索引
	indexes := store.persistenceManager.QueryMessagesByTimeRange(startTime, endTime)

	var messages []*common.MessageExt
	for _, index := range indexes {
		// 过滤指定topic
		if topic != "" && index.Topic != topic {
			continue
		}

		// 根据队列偏移量获取消息
		msg, err := store.GetMessage(index.Topic, index.QueueId, index.Offset, 1)
		if err != nil || len(msg) == 0 {
			continue
		}

		messages = append(messages, msg[0])
		if int32(len(messages)) >= maxNum {
			break
		}
	}

	return messages, nil
}

// QueryMessageTrace 查询消息轨迹
func (store *DefaultMessageStore) QueryMessageTrace(msgId string) (*MessageTrace, error) {
	// 首先尝试从消息索引中查找
	msgIndex := store.persistenceManager.GetMessageIndex(msgId)
	if msgIndex == nil {
		return nil, fmt.Errorf("message trace not found for msgId: %s", msgId)
	}

	// 构建消息轨迹
	trace := &MessageTrace{
		MsgId:     msgId,
		Topic:     msgIndex.Topic,
		Tags:      msgIndex.Tags,
		QueueId:   msgIndex.QueueId,
		Offset:    msgIndex.Offset,
		StoreTime: msgIndex.StoreTime,
		Status:    "STORED",
	}

	// 尝试获取完整消息信息
	msg, err := store.GetMessage(msgIndex.Topic, msgIndex.QueueId, msgIndex.Offset, 1)
	if err == nil && len(msg) > 0 {
		trace.Keys = msg[0].Keys
		trace.BodySize = int32(len(msg[0].Body))
		trace.Properties = msg[0].Properties
	}

	return trace, nil
}

// GetMessageIndex 获取消息索引
func (store *DefaultMessageStore) GetMessageIndex(messageKey string) *MessageIndex {
	return store.persistenceManager.GetMessageIndex(messageKey)
}

// QueryMessagesByKey 根据Key查询消息索引
func (store *DefaultMessageStore) QueryMessagesByKey(messageKey string) []*MessageIndex {
	return store.persistenceManager.QueryMessagesByKey(messageKey)
}

// GetStats 获取存储统计信息
func (store *DefaultMessageStore) GetStats() *StoreStats {
	return store.performanceMonitor.GetStats()
}

// ResetStats 重置统计信息
func (store *DefaultMessageStore) ResetStats() {
	store.performanceMonitor.ResetStats()
}

// recoverConsumeQueues 恢复ConsumeQueue
func (store *DefaultMessageStore) recoverConsumeQueues() error {
	// 扫描ConsumeQueue目录
	consumeQueueDir := store.storeConfig.StorePathConsumeQueue
	topicDirs, err := os.ReadDir(consumeQueueDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 目录不存在，跳过恢复
		}
		return fmt.Errorf("failed to read consume queue directory: %v", err)
	}

	for _, topicDir := range topicDirs {
		if !topicDir.IsDir() {
			continue
		}

		topicName := topicDir.Name()
		topicPath := filepath.Join(consumeQueueDir, topicName)

		queueDirs, err := os.ReadDir(topicPath)
		if err != nil {
			continue
		}

		for _, queueDir := range queueDirs {
			if !queueDir.IsDir() {
				continue
			}

			// 解析queueId
			queueId, err := strconv.ParseInt(queueDir.Name(), 10, 32)
			if err != nil {
				continue
			}

			// 创建ConsumeQueue并恢复
			cq := NewConsumeQueue(topicName, int32(queueId), store.storeConfig.StorePathConsumeQueue, store.storeConfig.MapedFileSizeConsumeQueue)
			cq.Recover()

			// 添加到表中
			key := fmt.Sprintf("%s-%d", topicName, queueId)
			store.consumeQueueTable[key] = cq
		}
	}

	return nil
}
