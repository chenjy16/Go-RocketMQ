package store

import (
	"sync"
	"time"
)



// FlushService 刷盘服务接口
type FlushService interface {
	Start()
	Shutdown()
	Run()
}

// FlushCommitLogService CommitLog刷盘服务
type FlushCommitLogService struct {
	commitLog *CommitLog
	config    *StoreConfig
	running   bool
	mutex     sync.Mutex
	shutdown  chan struct{}
}

// NewFlushCommitLogService 创建CommitLog刷盘服务
func NewFlushCommitLogService(commitLog *CommitLog, config *StoreConfig) *FlushCommitLogService {
	return &FlushCommitLogService{
		commitLog: commitLog,
		config:    config,
		shutdown:  make(chan struct{}),
	}
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
	ticker := time.NewTicker(time.Duration(f.config.FlushIntervalCommitLog) * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-f.shutdown:
			return
		case <-ticker.C:
			f.doFlush()
		}
	}
}

// doFlush 执行刷盘
func (f *FlushCommitLogService) doFlush() {
	if f.config.FlushDiskType == SYNC_FLUSH {
		// 同步刷盘
		f.commitLog.flush()
	} else {
		// 异步刷盘
		f.commitLog.flush()
	}
}

// FlushConsumeQueueService ConsumeQueue刷盘服务
type FlushConsumeQueueService struct {
	consumeQueues map[string]*ConsumeQueue
	config        *StoreConfig
	running       bool
	mutex         sync.Mutex
	shutdown      chan struct{}
}

// NewFlushConsumeQueueService 创建ConsumeQueue刷盘服务
func NewFlushConsumeQueueService(consumeQueues map[string]*ConsumeQueue, config *StoreConfig) *FlushConsumeQueueService {
	return &FlushConsumeQueueService{
		consumeQueues: consumeQueues,
		config:        config,
		shutdown:      make(chan struct{}),
	}
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
	ticker := time.NewTicker(time.Duration(f.config.FlushIntervalConsumeQueue) * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-f.shutdown:
			return
		case <-ticker.C:
			f.doFlush()
		}
	}
}

// doFlush 执行刷盘
func (f *FlushConsumeQueueService) doFlush() {
	for _, cq := range f.consumeQueues {
		cq.Flush(0)
	}
}