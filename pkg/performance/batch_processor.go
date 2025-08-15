package performance

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// BatchProcessor 批量处理器
type BatchProcessor struct {
	batchSize     int           // 批量大小
	flushInterval time.Duration // 刷新间隔
	processor     BatchHandler  // 处理函数
	buffer        []interface{} // 缓冲区
	mutex         sync.Mutex    // 互斥锁
	ticker        *time.Ticker  // 定时器
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	metrics       *BatchMetrics
	running       int32 // 运行状态
}

// BatchHandler 批量处理函数接口
type BatchHandler interface {
	Process(items []interface{}) error
}

// BatchHandlerFunc 批量处理函数类型
type BatchHandlerFunc func(items []interface{}) error

// Process 实现BatchHandler接口
func (f BatchHandlerFunc) Process(items []interface{}) error {
	return f(items)
}

// BatchMetrics 批量处理指标
type BatchMetrics struct {
	TotalItems      int64 // 总处理项目数
	TotalBatches    int64 // 总批次数
	SuccessfulItems int64 // 成功处理项目数
	FailedItems     int64 // 失败处理项目数
	AvgBatchSize    float64 // 平均批次大小
	AvgProcessTime  time.Duration // 平均处理时间
	mutex           sync.RWMutex
}

// BatchConfig 批量处理配置
type BatchConfig struct {
	BatchSize     int           // 批量大小
	FlushInterval time.Duration // 刷新间隔
	MaxRetries    int           // 最大重试次数
	RetryDelay    time.Duration // 重试延迟
	BufferSize    int           // 缓冲区大小
}

// DefaultBatchConfig 默认批量处理配置
var DefaultBatchConfig = BatchConfig{
	BatchSize:     100,
	FlushInterval: 100 * time.Millisecond,
	MaxRetries:    3,
	RetryDelay:    50 * time.Millisecond,
	BufferSize:    1000,
}

// NewBatchProcessor 创建批量处理器
func NewBatchProcessor(config BatchConfig, handler BatchHandler) *BatchProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &BatchProcessor{
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		processor:     handler,
		buffer:        make([]interface{}, 0, config.BufferSize),
		ctx:           ctx,
		cancel:        cancel,
		metrics:       &BatchMetrics{},
	}
}

// Start 启动批量处理器
func (bp *BatchProcessor) Start() error {
	if !atomic.CompareAndSwapInt32(&bp.running, 0, 1) {
		return errors.New("batch processor is already running")
	}
	
	bp.ticker = time.NewTicker(bp.flushInterval)
	bp.wg.Add(1)
	
	go bp.run()
	return nil
}

// Stop 停止批量处理器
func (bp *BatchProcessor) Stop() error {
	if !atomic.CompareAndSwapInt32(&bp.running, 1, 0) {
		return errors.New("batch processor is not running")
	}
	
	bp.cancel()
	if bp.ticker != nil {
		bp.ticker.Stop()
	}
	bp.wg.Wait()
	
	// 处理剩余的项目
	bp.flush()
	return nil
}

// Add 添加项目到批量处理器
func (bp *BatchProcessor) Add(item interface{}) error {
	if atomic.LoadInt32(&bp.running) == 0 {
		return errors.New("batch processor is not running")
	}
	
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	
	// 检查缓冲区是否已满
	if len(bp.buffer) >= cap(bp.buffer) {
		return errors.New("buffer is full")
	}
	
	bp.buffer = append(bp.buffer, item)
	
	// 如果达到批量大小，立即处理
	if len(bp.buffer) >= bp.batchSize {
		go bp.processBatch(bp.buffer)
		bp.buffer = make([]interface{}, 0, cap(bp.buffer))
	}
	
	return nil
}

// run 运行批量处理器
func (bp *BatchProcessor) run() {
	defer bp.wg.Done()
	
	for {
		select {
		case <-bp.ctx.Done():
			return
		case <-bp.ticker.C:
			bp.flush()
		}
	}
}

// flush 刷新缓冲区
func (bp *BatchProcessor) flush() {
	bp.mutex.Lock()
	if len(bp.buffer) == 0 {
		bp.mutex.Unlock()
		return
	}
	
	items := make([]interface{}, len(bp.buffer))
	copy(items, bp.buffer)
	bp.buffer = bp.buffer[:0]
	bp.mutex.Unlock()
	
	bp.processBatch(items)
}

// processBatch 处理批次
func (bp *BatchProcessor) processBatch(items []interface{}) {
	if len(items) == 0 {
		return
	}
	
	start := time.Now()
	err := bp.processor.Process(items)
	processTime := time.Since(start)
	
	// 更新指标
	bp.updateMetrics(len(items), processTime, err == nil)
}

// updateMetrics 更新指标
func (bp *BatchProcessor) updateMetrics(itemCount int, processTime time.Duration, success bool) {
	bp.metrics.mutex.Lock()
	defer bp.metrics.mutex.Unlock()
	
	bp.metrics.TotalItems += int64(itemCount)
	bp.metrics.TotalBatches++
	
	if success {
		bp.metrics.SuccessfulItems += int64(itemCount)
	} else {
		bp.metrics.FailedItems += int64(itemCount)
	}
	
	// 计算平均批次大小
	bp.metrics.AvgBatchSize = float64(bp.metrics.TotalItems) / float64(bp.metrics.TotalBatches)
	
	// 计算平均处理时间
	if bp.metrics.TotalBatches == 1 {
		bp.metrics.AvgProcessTime = processTime
	} else {
		bp.metrics.AvgProcessTime = (bp.metrics.AvgProcessTime*time.Duration(bp.metrics.TotalBatches-1) + processTime) / time.Duration(bp.metrics.TotalBatches)
	}
}

// GetMetrics 获取指标
func (bp *BatchProcessor) GetMetrics() BatchMetrics {
	bp.metrics.mutex.RLock()
	defer bp.metrics.mutex.RUnlock()
	return *bp.metrics
}

// MessageBatchProcessor 消息批量处理器
type MessageBatchProcessor struct {
	*BatchProcessor
	maxMessageSize int
	maxBatchBytes  int
	currentBytes   int
	messagePool    *MessagePool
}

// NewMessageBatchProcessor 创建消息批量处理器
func NewMessageBatchProcessor(config BatchConfig, handler BatchHandler) *MessageBatchProcessor {
	return &MessageBatchProcessor{
		BatchProcessor: NewBatchProcessor(config, handler),
		maxMessageSize: 4 * 1024 * 1024,  // 4MB
		maxBatchBytes:  32 * 1024 * 1024, // 32MB
		messagePool:    NewMessagePool(),
	}
}

// AddMessage 添加消息到批量处理器
func (mbp *MessageBatchProcessor) AddMessage(msg *Message) error {
	msgSize := len(msg.Body) + len(msg.Topic) + len(msg.Tags) + len(msg.Keys)
	for k, v := range msg.Properties {
		msgSize += len(k) + len(v)
	}
	
	// 检查消息大小
	if msgSize > mbp.maxMessageSize {
		return errors.New("message size exceeds maximum limit")
	}
	
	// 检查批次大小
	if mbp.currentBytes+msgSize > mbp.maxBatchBytes {
		// 先处理当前批次
		mbp.flush()
		mbp.currentBytes = 0
	}
	
	mbp.currentBytes += msgSize
	return mbp.Add(msg)
}

// ConsumerBatchProcessor 消费者批量处理器
type ConsumerBatchProcessor struct {
	*BatchProcessor
	consumeFunc func([]*Message) error
	maxConsumeSize int
}

// NewConsumerBatchProcessor 创建消费者批量处理器
func NewConsumerBatchProcessor(config BatchConfig, consumeFunc func([]*Message) error) *ConsumerBatchProcessor {
	cbp := &ConsumerBatchProcessor{
		consumeFunc:    consumeFunc,
		maxConsumeSize: config.BatchSize,
	}
	
	handler := BatchHandlerFunc(func(items []interface{}) error {
		messages := make([]*Message, len(items))
		for i, item := range items {
			messages[i] = item.(*Message)
		}
		return cbp.consumeFunc(messages)
	})
	
	cbp.BatchProcessor = NewBatchProcessor(config, handler)
	return cbp
}

// ConsumeMessage 消费消息
func (cbp *ConsumerBatchProcessor) ConsumeMessage(msg *Message) error {
	return cbp.Add(msg)
}

// StoreBatchProcessor 存储批量处理器
type StoreBatchProcessor struct {
	*BatchProcessor
	storeFunc func([]interface{}) error
	maxStoreSize int
}

// NewStoreBatchProcessor 创建存储批量处理器
func NewStoreBatchProcessor(config BatchConfig, storeFunc func([]interface{}) error) *StoreBatchProcessor {
	sbp := &StoreBatchProcessor{
		storeFunc:    storeFunc,
		maxStoreSize: config.BatchSize,
	}
	
	handler := BatchHandlerFunc(storeFunc)
	sbp.BatchProcessor = NewBatchProcessor(config, handler)
	return sbp
}

// Store 存储数据
func (sbp *StoreBatchProcessor) Store(data interface{}) error {
	return sbp.Add(data)
}

// BatchSender 批量发送器
type BatchSender struct {
	processor *MessageBatchProcessor
	sendFunc  func([]*Message) error
	config    BatchConfig
}

// NewBatchSender 创建批量发送器
func NewBatchSender(config BatchConfig, sendFunc func([]*Message) error) *BatchSender {
	handler := BatchHandlerFunc(func(items []interface{}) error {
		messages := make([]*Message, len(items))
		for i, item := range items {
			messages[i] = item.(*Message)
		}
		return sendFunc(messages)
	})
	
	return &BatchSender{
		processor: NewMessageBatchProcessor(config, handler),
		sendFunc:  sendFunc,
		config:    config,
	}
}

// Start 启动批量发送器
func (bs *BatchSender) Start() error {
	return bs.processor.Start()
}

// Stop 停止批量发送器
func (bs *BatchSender) Stop() error {
	return bs.processor.Stop()
}

// Send 发送消息
func (bs *BatchSender) Send(msg *Message) error {
	return bs.processor.AddMessage(msg)
}

// GetMetrics 获取发送指标
func (bs *BatchSender) GetMetrics() BatchMetrics {
	return bs.processor.GetMetrics()
}

// BatchReceiver 批量接收器
type BatchReceiver struct {
	processor   *ConsumerBatchProcessor
	receiveFunc func([]*Message) error
	config      BatchConfig
}

// NewBatchReceiver 创建批量接收器
func NewBatchReceiver(config BatchConfig, receiveFunc func([]*Message) error) *BatchReceiver {
	return &BatchReceiver{
		processor:   NewConsumerBatchProcessor(config, receiveFunc),
		receiveFunc: receiveFunc,
		config:      config,
	}
}

// Start 启动批量接收器
func (br *BatchReceiver) Start() error {
	return br.processor.Start()
}

// Stop 停止批量接收器
func (br *BatchReceiver) Stop() error {
	return br.processor.Stop()
}

// Receive 接收消息
func (br *BatchReceiver) Receive(msg *Message) error {
	return br.processor.ConsumeMessage(msg)
}

// GetMetrics 获取接收指标
func (br *BatchReceiver) GetMetrics() BatchMetrics {
	return br.processor.GetMetrics()
}

// BatchManager 批量管理器
type BatchManager struct {
	senders   map[string]*BatchSender
	receivers map[string]*BatchReceiver
	mutex     sync.RWMutex
}

// NewBatchManager 创建批量管理器
func NewBatchManager() *BatchManager {
	return &BatchManager{
		senders:   make(map[string]*BatchSender),
		receivers: make(map[string]*BatchReceiver),
	}
}

// RegisterSender 注册批量发送器
func (bm *BatchManager) RegisterSender(name string, sender *BatchSender) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	bm.senders[name] = sender
}

// RegisterReceiver 注册批量接收器
func (bm *BatchManager) RegisterReceiver(name string, receiver *BatchReceiver) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	bm.receivers[name] = receiver
}

// GetSender 获取批量发送器
func (bm *BatchManager) GetSender(name string) *BatchSender {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	return bm.senders[name]
}

// GetReceiver 获取批量接收器
func (bm *BatchManager) GetReceiver(name string) *BatchReceiver {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	return bm.receivers[name]
}

// StartAll 启动所有处理器
func (bm *BatchManager) StartAll() error {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	
	for _, sender := range bm.senders {
		if err := sender.Start(); err != nil {
			return err
		}
	}
	
	for _, receiver := range bm.receivers {
		if err := receiver.Start(); err != nil {
			return err
		}
	}
	
	return nil
}

// StopAll 停止所有处理器
func (bm *BatchManager) StopAll() error {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	
	for _, sender := range bm.senders {
		if err := sender.Stop(); err != nil {
			return err
		}
	}
	
	for _, receiver := range bm.receivers {
		if err := receiver.Stop(); err != nil {
			return err
		}
	}
	
	return nil
}

// GetAllMetrics 获取所有指标
func (bm *BatchManager) GetAllMetrics() map[string]BatchMetrics {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	
	metrics := make(map[string]BatchMetrics)
	
	for name, sender := range bm.senders {
		metrics["sender_"+name] = sender.GetMetrics()
	}
	
	for name, receiver := range bm.receivers {
		metrics["receiver_"+name] = receiver.GetMetrics()
	}
	
	return metrics
}

// 全局批量管理器
var (
	GlobalBatchManager *BatchManager
	batchOnce          sync.Once
)

// InitGlobalBatchManager 初始化全局批量管理器
func InitGlobalBatchManager() {
	batchOnce.Do(func() {
		GlobalBatchManager = NewBatchManager()
	})
}

// GetGlobalBatchManager 获取全局批量管理器
func GetGlobalBatchManager() *BatchManager {
	InitGlobalBatchManager()
	return GlobalBatchManager
}