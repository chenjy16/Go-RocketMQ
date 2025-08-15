package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-rocketmq/pkg/common"
)

// TestCleanCommitLogService 测试CommitLog清理服务
func TestCleanCommitLogService(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-clean-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建存储配置
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024, // 1MB
		FlushDiskType:         ASYNC_FLUSH,
		FileReservedTime:      72, // 72小时
	}

	// 创建CommitLog
	commitLog, err := NewCommitLog(config)
	if err != nil {
		t.Fatalf("Failed to create commit log: %v", err)
	}
	defer commitLog.Shutdown()

	// 创建清理服务
	cleanService := NewCleanCommitLogService(commitLog, config)
	if cleanService == nil {
		t.Fatal("CleanCommitLogService should not be nil")
	}

	// 测试启动和关闭
	cleanService.Start()
	if !cleanService.running {
		t.Error("CleanService should be running after Start()")
	}

	// 测试重复启动
	cleanService.Start()
	if !cleanService.running {
		t.Error("CleanService should still be running after duplicate Start()")
	}

	// 测试关闭
	cleanService.Shutdown()
	if cleanService.running {
		t.Error("CleanService should not be running after Shutdown()")
	}

	// 测试重复关闭
	cleanService.Shutdown()
	if cleanService.running {
		t.Error("CleanService should still not be running after duplicate Shutdown()")
	}
}

// TestCleanCommitLogServiceDoClean 测试CommitLog清理功能
func TestCleanCommitLogServiceDoClean(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-clean-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建存储配置
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024, // 1MB
		FlushDiskType:         ASYNC_FLUSH,
		FileReservedTime:      72, // 72小时
	}

	// 创建CommitLog
	commitLog, err := NewCommitLog(config)
	if err != nil {
		t.Fatalf("Failed to create commit log: %v", err)
	}
	defer commitLog.Shutdown()

	// 创建清理服务
	cleanService := NewCleanCommitLogService(commitLog, config)

	// 测试doClean方法（这个方法应该不会出错）
	cleanService.doClean()

	// 验证清理服务的配置
	if cleanService.commitLog != commitLog {
		t.Error("CleanService commitLog should match the provided commitLog")
	}
	if cleanService.config != config {
		t.Error("CleanService config should match the provided config")
	}
}

// TestCleanConsumeQueueService 测试ConsumeQueue清理服务
func TestCleanConsumeQueueService(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-clean-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建存储配置
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog:   1024 * 1024, // 1MB
		MapedFileSizeConsumeQueue: 300000 * 20, // 300万条记录
		FlushDiskType:            ASYNC_FLUSH,
		FileReservedTime:         72, // 72小时
	}

	// 创建ConsumeQueue映射
	consumeQueues := make(map[string]*ConsumeQueue)

	// 创建测试ConsumeQueue
	testTopic := "TestTopic"
	consumeQueue := NewConsumeQueue(testTopic, 0, config.StorePathConsumeQueue, config.MapedFileSizeConsumeQueue)
	consumeQueues[testTopic] = consumeQueue
	defer consumeQueue.Shutdown()

	// 创建清理服务
	cleanService := NewCleanConsumeQueueService(consumeQueues, config)
	if cleanService == nil {
		t.Fatal("CleanConsumeQueueService should not be nil")
	}

	// 测试启动和关闭
	cleanService.Start()
	if !cleanService.running {
		t.Error("CleanService should be running after Start()")
	}

	// 测试重复启动
	cleanService.Start()
	if !cleanService.running {
		t.Error("CleanService should still be running after duplicate Start()")
	}

	// 测试关闭
	cleanService.Shutdown()
	if cleanService.running {
		t.Error("CleanService should not be running after Shutdown()")
	}

	// 测试重复关闭
	cleanService.Shutdown()
	if cleanService.running {
		t.Error("CleanService should still not be running after duplicate Shutdown()")
	}
}

// TestCleanConsumeQueueServiceDoClean 测试ConsumeQueue清理功能
func TestCleanConsumeQueueServiceDoClean(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-clean-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建存储配置
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog:   1024 * 1024, // 1MB
		MapedFileSizeConsumeQueue: 300000 * 20, // 300万条记录
		FlushDiskType:            ASYNC_FLUSH,
		FileReservedTime:         72, // 72小时
	}

	// 创建ConsumeQueue映射
	consumeQueues := make(map[string]*ConsumeQueue)

	// 创建多个测试ConsumeQueue
	topics := []string{"Topic1", "Topic2", "Topic3"}
	for _, topic := range topics {
		consumeQueue := NewConsumeQueue(topic, 0, config.StorePathConsumeQueue, config.MapedFileSizeConsumeQueue)
		consumeQueues[topic] = consumeQueue
		defer consumeQueue.Shutdown()
	}

	// 创建清理服务
	cleanService := NewCleanConsumeQueueService(consumeQueues, config)

	// 测试doClean方法
	cleanService.doClean()

	// 验证清理服务的配置
	if len(cleanService.consumeQueues) != len(topics) {
		t.Errorf("Expected %d consume queues, got %d", len(topics), len(cleanService.consumeQueues))
	}
	if cleanService.config != config {
		t.Error("CleanService config should match the provided config")
	}
}

// TestCleanServiceIntegration 测试清理服务集成
func TestCleanServiceIntegration(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-clean-integration-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建存储配置
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024, // 1MB
		FlushDiskType:         ASYNC_FLUSH,
		FileReservedTime:      1, // 1小时（用于测试）
	}

	// 创建消息存储
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}

	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()

	// 存储一些测试消息
	testMessages := []*common.Message{
		{Topic: "CleanTestTopic1", Body: []byte("test message 1")},
		{Topic: "CleanTestTopic2", Body: []byte("test message 2")},
		{Topic: "CleanTestTopic3", Body: []byte("test message 3")},
	}

	for _, msg := range testMessages {
		result, err := store.PutMessage(msg)
		if err != nil {
			t.Errorf("Failed to put message for topic %s: %v", msg.Topic, err)
			continue
		}
		if result == nil {
			t.Errorf("Put message result should not be nil for topic %s", msg.Topic)
		}
	}

	// 创建清理服务并测试
	commitLogCleanService := NewCleanCommitLogService(store.commitLog, config)
	commitLogCleanService.Start()
	time.Sleep(100 * time.Millisecond) // 让清理服务运行一小段时间
	commitLogCleanService.Shutdown()

	consumeQueueCleanService := NewCleanConsumeQueueService(store.consumeQueueTable, config)
	consumeQueueCleanService.Start()
	time.Sleep(100 * time.Millisecond) // 让清理服务运行一小段时间
	consumeQueueCleanService.Shutdown()

	// 验证清理服务正常工作（没有崩溃）
	t.Log("Clean service integration test completed successfully")
}

// TestCleanServiceConcurrency 测试清理服务并发安全性
func TestCleanServiceConcurrency(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-clean-concurrency-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建存储配置
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024, // 1MB
		FlushDiskType:         ASYNC_FLUSH,
	}

	// 创建CommitLog
	commitLog, err := NewCommitLog(config)
	if err != nil {
		t.Fatalf("Failed to create commit log: %v", err)
	}
	defer commitLog.Shutdown()

	// 创建清理服务
	cleanService := NewCleanCommitLogService(commitLog, config)

	// 并发测试启动和关闭
	for i := 0; i < 10; i++ {
		go func() {
			cleanService.Start()
			time.Sleep(10 * time.Millisecond)
			cleanService.Shutdown()
		}()
	}

	// 等待所有goroutine完成
	time.Sleep(200 * time.Millisecond)

	// 验证最终状态
	if cleanService.running {
		t.Error("CleanService should not be running after concurrent operations")
	}

	t.Log("Clean service concurrency test completed successfully")
}