package store

import (
	"sync"
	"time"
)

// CleanService 清理服务接口
type CleanService interface {
	Start()
	Shutdown()
	Run()
}

// CleanCommitLogService CommitLog清理服务
type CleanCommitLogService struct {
	commitLog *CommitLog
	config    *StoreConfig
	running   bool
	mutex     sync.Mutex
	shutdown  chan struct{}
}

// NewCleanCommitLogService 创建CommitLog清理服务
func NewCleanCommitLogService(commitLog *CommitLog, config *StoreConfig) *CleanCommitLogService {
	return &CleanCommitLogService{
		commitLog: commitLog,
		config:    config,
		shutdown:  make(chan struct{}),
	}
}

// Start 启动清理服务
func (c *CleanCommitLogService) Start() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if c.running {
		return
	}
	
	c.running = true
	go c.Run()
}

// Shutdown 关闭清理服务
func (c *CleanCommitLogService) Shutdown() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if !c.running {
		return
	}
	
	c.running = false
	close(c.shutdown)
}

// Run 运行清理服务
func (c *CleanCommitLogService) Run() {
	ticker := time.NewTicker(10 * time.Second) // 默认10秒清理一次
	defer ticker.Stop()
	
	for {
		select {
		case <-c.shutdown:
			return
		case <-ticker.C:
			c.doClean()
		}
	}
}

// doClean 执行清理
func (c *CleanCommitLogService) doClean() {
	// 计算过期时间（默认保留72小时）
	expiredTime := time.Now().UnixMilli() - 72*60*60*1000
	
	// 清理过期文件
	c.commitLog.mapedFileQueue.DeleteExpiredFile(expiredTime, 100, 120*1000, false)
}

// CleanConsumeQueueService ConsumeQueue清理服务
type CleanConsumeQueueService struct {
	consumeQueues map[string]*ConsumeQueue
	config        *StoreConfig
	running       bool
	mutex         sync.Mutex
	shutdown      chan struct{}
}

// NewCleanConsumeQueueService 创建ConsumeQueue清理服务
func NewCleanConsumeQueueService(consumeQueues map[string]*ConsumeQueue, config *StoreConfig) *CleanConsumeQueueService {
	return &CleanConsumeQueueService{
		consumeQueues: consumeQueues,
		config:        config,
		shutdown:      make(chan struct{}),
	}
}

// Start 启动清理服务
func (c *CleanConsumeQueueService) Start() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if c.running {
		return
	}
	
	c.running = true
	go c.Run()
}

// Shutdown 关闭清理服务
func (c *CleanConsumeQueueService) Shutdown() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	if !c.running {
		return
	}
	
	c.running = false
	close(c.shutdown)
}

// Run 运行清理服务
func (c *CleanConsumeQueueService) Run() {
	ticker := time.NewTicker(10 * time.Second) // 默认10秒清理一次
	defer ticker.Stop()
	
	for {
		select {
		case <-c.shutdown:
			return
		case <-ticker.C:
			c.doClean()
		}
	}
}

// doClean 执行清理
func (c *CleanConsumeQueueService) doClean() {
	// 计算过期时间（默认保留72小时）
	expiredTime := time.Now().UnixMilli() - 72*60*60*1000
	
	// 清理过期文件
	for _, cq := range c.consumeQueues {
		cq.CleanExpiredFile(expiredTime)
	}
}