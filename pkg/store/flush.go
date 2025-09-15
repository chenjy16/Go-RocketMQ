package store

import (
	"sync"
	"sync/atomic"
	"time"
)

// FlushService 刷盘服务接口
type FlushService interface {
	Start()
	Shutdown()
	Run()
}

// AdaptiveFlushConfig 自适应刷盘配置
type AdaptiveFlushConfig struct {
	EnableAdaptiveFlush     bool          // 是否启用自适应刷盘
	BaseFlushInterval       time.Duration // 基础刷盘间隔
	MinFlushInterval        time.Duration // 最小刷盘间隔
	MaxFlushInterval        time.Duration // 最大刷盘间隔
	FlushIntervalAdjustStep time.Duration // 刷盘间隔调整步长
	WriteRateThreshold      int64         // 写入速率阈值（消息数/秒）
	LatencyThreshold        time.Duration // 延迟阈值
}

// DefaultAdaptiveFlushConfig 默认自适应刷盘配置
func DefaultAdaptiveFlushConfig() *AdaptiveFlushConfig {
	return &AdaptiveFlushConfig{
		EnableAdaptiveFlush:     true,
		BaseFlushInterval:       500 * time.Millisecond,
		MinFlushInterval:        100 * time.Millisecond,
		MaxFlushInterval:        2 * time.Second,
		FlushIntervalAdjustStep: 100 * time.Millisecond,
		WriteRateThreshold:      1000, // 1000消息/秒
		LatencyThreshold:        10 * time.Millisecond,
	}
}

// FlushMetrics 刷盘指标
type FlushMetrics struct {
	TotalFlushCount     int64         // 总刷盘次数
	TotalFlushBytes     int64         // 总刷盘字节数
	AvgFlushLatency     time.Duration // 平均刷盘延迟
	LastFlushTime       time.Time     // 最后刷盘时间
	CurrentWriteRate    int64         // 当前写入速率（消息/秒）
	CurrentFlushLatency time.Duration // 当前刷盘延迟
	mutex               sync.RWMutex
}

// FlushCommitLogService CommitLog刷盘服务
type FlushCommitLogService struct {
	commitLog *CommitLog
	config    *StoreConfig
	running   bool
	mutex     sync.Mutex
	shutdown  chan struct{}

	// 自适应刷盘相关
	adaptiveConfig  *AdaptiveFlushConfig
	metrics         *FlushMetrics
	currentInterval time.Duration
	lastFlushTime   time.Time
	writeCounter    int64 // 写入计数器
}

// NewFlushCommitLogService 创建CommitLog刷盘服务
func NewFlushCommitLogService(commitLog *CommitLog, config *StoreConfig) *FlushCommitLogService {
	service := &FlushCommitLogService{
		commitLog:       commitLog,
		config:          config,
		shutdown:        make(chan struct{}),
		adaptiveConfig:  DefaultAdaptiveFlushConfig(),
		metrics:         &FlushMetrics{},
		currentInterval: time.Duration(config.FlushIntervalCommitLog) * time.Millisecond,
		lastFlushTime:   time.Now(),
	}

	// 如果配置中禁用了自适应刷盘，则使用固定间隔
	if config.FlushDiskType == SYNC_FLUSH {
		service.adaptiveConfig.EnableAdaptiveFlush = false
	}

	return service
}

// Start 启动刷盘服务
func (f *FlushCommitLogService) Start() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.running {
		return
	}

	f.running = true
	go f.Run()
}

// Shutdown 关闭刷盘服务
func (f *FlushCommitLogService) Shutdown() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if !f.running {
		return
	}

	f.running = false
	close(f.shutdown)
}

// Run 运行刷盘服务
func (f *FlushCommitLogService) Run() {
	ticker := time.NewTicker(f.currentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.shutdown:
			return
		case <-ticker.C:
			f.doFlush()
			// 重新计算刷盘间隔
			if f.adaptiveConfig.EnableAdaptiveFlush {
				f.adjustFlushInterval()
				// 重置ticker
				ticker.Reset(f.currentInterval)
			}
		}
	}
}

// doFlush 执行刷盘
func (f *FlushCommitLogService) doFlush() {
	startTime := time.Now()

	if f.config.FlushDiskType == SYNC_FLUSH {
		// 同步刷盘
		f.commitLog.flush()
	} else {
		// 异步刷盘
		f.commitLog.flush()
	}

	// 更新指标
	f.updateMetrics(time.Since(startTime))
}

// updateMetrics 更新刷盘指标
func (f *FlushCommitLogService) updateMetrics(latency time.Duration) {
	f.metrics.mutex.Lock()
	defer f.metrics.mutex.Unlock()

	f.metrics.TotalFlushCount++
	f.metrics.LastFlushTime = time.Now()
	f.metrics.CurrentFlushLatency = latency

	// 计算平均刷盘延迟
	if f.metrics.AvgFlushLatency == 0 {
		f.metrics.AvgFlushLatency = latency
	} else {
		f.metrics.AvgFlushLatency = (f.metrics.AvgFlushLatency + latency) / 2
	}

	// 计算写入速率（消息/秒）
	if time.Since(f.lastFlushTime) > 0 {
		writeCount := atomic.SwapInt64(&f.writeCounter, 0)
		duration := time.Since(f.lastFlushTime)
		f.metrics.CurrentWriteRate = writeCount * int64(time.Second) / int64(duration)
	}

	f.lastFlushTime = time.Now()
}

// adjustFlushInterval 调整刷盘间隔
func (f *FlushCommitLogService) adjustFlushInterval() {
	f.metrics.mutex.RLock()
	writeRate := f.metrics.CurrentWriteRate
	latency := f.metrics.CurrentFlushLatency
	f.metrics.mutex.RUnlock()

	// 根据写入速率和延迟调整刷盘间隔
	if writeRate > f.adaptiveConfig.WriteRateThreshold {
		// 高写入速率，减少刷盘间隔
		if f.currentInterval > f.adaptiveConfig.MinFlushInterval {
			f.currentInterval -= f.adaptiveConfig.FlushIntervalAdjustStep
			if f.currentInterval < f.adaptiveConfig.MinFlushInterval {
				f.currentInterval = f.adaptiveConfig.MinFlushInterval
			}
		}
	} else if latency > f.adaptiveConfig.LatencyThreshold {
		// 高延迟，减少刷盘间隔
		if f.currentInterval > f.adaptiveConfig.MinFlushInterval {
			f.currentInterval -= f.adaptiveConfig.FlushIntervalAdjustStep
			if f.currentInterval < f.adaptiveConfig.MinFlushInterval {
				f.currentInterval = f.adaptiveConfig.MinFlushInterval
			}
		}
	} else {
		// 正常情况，逐步恢复到基础刷盘间隔
		if f.currentInterval < f.adaptiveConfig.BaseFlushInterval {
			f.currentInterval += f.adaptiveConfig.FlushIntervalAdjustStep
			if f.currentInterval > f.adaptiveConfig.BaseFlushInterval {
				f.currentInterval = f.adaptiveConfig.BaseFlushInterval
			}
		} else if f.currentInterval > f.adaptiveConfig.BaseFlushInterval {
			f.currentInterval -= f.adaptiveConfig.FlushIntervalAdjustStep
			if f.currentInterval < f.adaptiveConfig.BaseFlushInterval {
				f.currentInterval = f.adaptiveConfig.BaseFlushInterval
			}
		}
	}
}

// GetMetrics 获取刷盘指标
func (f *FlushCommitLogService) GetMetrics() *FlushMetrics {
	f.metrics.mutex.RLock()
	defer f.metrics.mutex.RUnlock()

	// 返回指标的副本
	return &FlushMetrics{
		TotalFlushCount:     f.metrics.TotalFlushCount,
		TotalFlushBytes:     f.metrics.TotalFlushBytes,
		AvgFlushLatency:     f.metrics.AvgFlushLatency,
		LastFlushTime:       f.metrics.LastFlushTime,
		CurrentWriteRate:    f.metrics.CurrentWriteRate,
		CurrentFlushLatency: f.metrics.CurrentFlushLatency,
	}
}

// IncrementWriteCounter 增加写入计数器
func (f *FlushCommitLogService) IncrementWriteCounter() {
	atomic.AddInt64(&f.writeCounter, 1)
}

// FlushConsumeQueueService ConsumeQueue刷盘服务
type FlushConsumeQueueService struct {
	consumeQueues map[string]*ConsumeQueue
	config        *StoreConfig
	running       bool
	mutex         sync.Mutex
	shutdown      chan struct{}

	// 自适应刷盘相关
	adaptiveConfig  *AdaptiveFlushConfig
	metrics         *FlushMetrics
	currentInterval time.Duration
	lastFlushTime   time.Time
}

// NewFlushConsumeQueueService 创建ConsumeQueue刷盘服务
func NewFlushConsumeQueueService(consumeQueues map[string]*ConsumeQueue, config *StoreConfig) *FlushConsumeQueueService {
	service := &FlushConsumeQueueService{
		consumeQueues:   consumeQueues,
		config:          config,
		shutdown:        make(chan struct{}),
		adaptiveConfig:  DefaultAdaptiveFlushConfig(),
		metrics:         &FlushMetrics{},
		currentInterval: time.Duration(config.FlushIntervalConsumeQueue) * time.Millisecond,
		lastFlushTime:   time.Now(),
	}

	return service
}

// Start 启动刷盘服务
func (f *FlushConsumeQueueService) Start() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.running {
		return
	}

	f.running = true
	go f.Run()
}

// Shutdown 关闭刷盘服务
func (f *FlushConsumeQueueService) Shutdown() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if !f.running {
		return
	}

	f.running = false
	close(f.shutdown)
}

// Run 运行刷盘服务
func (f *FlushConsumeQueueService) Run() {
	ticker := time.NewTicker(f.currentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.shutdown:
			return
		case <-ticker.C:
			f.doFlush()
			// 重新计算刷盘间隔
			if f.adaptiveConfig.EnableAdaptiveFlush {
				f.adjustFlushInterval()
				// 重置ticker
				ticker.Reset(f.currentInterval)
			}
		}
	}
}

// doFlush 执行刷盘
func (f *FlushConsumeQueueService) doFlush() {
	startTime := time.Now()

	for _, cq := range f.consumeQueues {
		cq.Flush(0)
	}

	// 更新指标
	f.updateMetrics(time.Since(startTime))
}

// updateMetrics 更新刷盘指标
func (f *FlushConsumeQueueService) updateMetrics(latency time.Duration) {
	f.metrics.mutex.Lock()
	defer f.metrics.mutex.Unlock()

	f.metrics.TotalFlushCount++
	f.metrics.LastFlushTime = time.Now()
	f.metrics.CurrentFlushLatency = latency

	// 计算平均刷盘延迟
	if f.metrics.AvgFlushLatency == 0 {
		f.metrics.AvgFlushLatency = latency
	} else {
		f.metrics.AvgFlushLatency = (f.metrics.AvgFlushLatency + latency) / 2
	}

	f.lastFlushTime = time.Now()
}

// adjustFlushInterval 调整刷盘间隔
func (f *FlushConsumeQueueService) adjustFlushInterval() {
	f.metrics.mutex.RLock()
	latency := f.metrics.CurrentFlushLatency
	f.metrics.mutex.RUnlock()

	// 根据延迟调整刷盘间隔
	if latency > f.adaptiveConfig.LatencyThreshold {
		// 高延迟，减少刷盘间隔
		if f.currentInterval > f.adaptiveConfig.MinFlushInterval {
			f.currentInterval -= f.adaptiveConfig.FlushIntervalAdjustStep
			if f.currentInterval < f.adaptiveConfig.MinFlushInterval {
				f.currentInterval = f.adaptiveConfig.MinFlushInterval
			}
		}
	} else {
		// 正常情况，逐步恢复到基础刷盘间隔
		if f.currentInterval < f.adaptiveConfig.BaseFlushInterval {
			f.currentInterval += f.adaptiveConfig.FlushIntervalAdjustStep
			if f.currentInterval > f.adaptiveConfig.BaseFlushInterval {
				f.currentInterval = f.adaptiveConfig.BaseFlushInterval
			}
		} else if f.currentInterval > f.adaptiveConfig.BaseFlushInterval {
			f.currentInterval -= f.adaptiveConfig.FlushIntervalAdjustStep
			if f.currentInterval < f.adaptiveConfig.BaseFlushInterval {
				f.currentInterval = f.adaptiveConfig.BaseFlushInterval
			}
		}
	}
}

// GetMetrics 获取刷盘指标
func (f *FlushConsumeQueueService) GetMetrics() *FlushMetrics {
	f.metrics.mutex.RLock()
	defer f.metrics.mutex.RUnlock()

	// 返回指标的副本
	return &FlushMetrics{
		TotalFlushCount:     f.metrics.TotalFlushCount,
		TotalFlushBytes:     f.metrics.TotalFlushBytes,
		AvgFlushLatency:     f.metrics.AvgFlushLatency,
		LastFlushTime:       f.metrics.LastFlushTime,
		CurrentWriteRate:    f.metrics.CurrentWriteRate,
		CurrentFlushLatency: f.metrics.CurrentFlushLatency,
	}
}
