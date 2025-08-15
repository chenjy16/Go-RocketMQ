package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go-rocketmq/pkg/common"
)

// TestFlushDiskType 测试刷盘类型
func TestFlushDiskType(t *testing.T) {
	if ASYNC_FLUSH != 0 {
		t.Errorf("Expected ASYNC_FLUSH to be 0, got %d", ASYNC_FLUSH)
	}
	
	if SYNC_FLUSH != 1 {
		t.Errorf("Expected SYNC_FLUSH to be 1, got %d", SYNC_FLUSH)
	}
}

// TestNewDefaultStoreConfig 测试默认存储配置
func TestNewDefaultStoreConfig(t *testing.T) {
	config := NewDefaultStoreConfig()
	
	if config == nil {
		t.Fatal("Expected config to be created")
	}
	
	if config.StorePathRootDir == "" {
		t.Error("Expected StorePathRootDir to be set")
	}
	
	if config.MapedFileSizeCommitLog != 1024*1024*1024 {
		t.Errorf("Expected MapedFileSizeCommitLog to be 1GB, got %d", config.MapedFileSizeCommitLog)
	}
	
	if config.MapedFileSizeConsumeQueue != 300000*20 {
		t.Errorf("Expected MapedFileSizeConsumeQueue to be 6MB, got %d", config.MapedFileSizeConsumeQueue)
	}
	
	if config.FlushDiskType != ASYNC_FLUSH {
		t.Errorf("Expected FlushDiskType to be ASYNC_FLUSH, got %d", config.FlushDiskType)
	}
	
	if config.FlushIntervalCommitLog != 500 {
		t.Errorf("Expected FlushIntervalCommitLog to be 500ms, got %d", config.FlushIntervalCommitLog)
	}
	
	if config.FileReservedTime != 72 {
		t.Errorf("Expected FileReservedTime to be 72 hours, got %d", config.FileReservedTime)
	}
	
	if config.DeleteWhen != "04" {
		t.Errorf("Expected DeleteWhen to be '04', got %s", config.DeleteWhen)
	}
	
	if config.DiskMaxUsedSpaceRatio != 75 {
		t.Errorf("Expected DiskMaxUsedSpaceRatio to be 75, got %d", config.DiskMaxUsedSpaceRatio)
	}
}

// TestStoreConfig 测试存储配置
func TestStoreConfig(t *testing.T) {
	config := &StoreConfig{
		StorePathRootDir:         "/tmp/rocketmq-test",
		StorePathCommitLog:       "/tmp/rocketmq-test/commitlog",
		StorePathConsumeQueue:    "/tmp/rocketmq-test/consumequeue",
		StorePathIndex:           "/tmp/rocketmq-test/index",
		MapedFileSizeCommitLog:   1024 * 1024 * 1024,
		MapedFileSizeConsumeQueue: 300000 * 20,
		MapedFileSizeIndexFile:   4000000 * 20,
		FlushDiskType:            SYNC_FLUSH,
		FlushIntervalCommitLog:   1000,
		FlushCommitLogLeastPages: 4,
		FlushConsumeQueueLeastPages: 2,
		FlushIntervalConsumeQueue: 1000,
		FileReservedTime:         48,
		DeleteWhen:               "02",
		DiskMaxUsedSpaceRatio:    80,
		TransientStorePoolEnable: true,
		TransientStorePoolSize:   5,
		FastFailIfNoBufferInStorePool: false,
	}
	
	// 测试JSON序列化
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}
	
	// 测试JSON反序列化
	var newConfig StoreConfig
	err = json.Unmarshal(data, &newConfig)
	if err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}
	
	if newConfig.StorePathRootDir != config.StorePathRootDir {
		t.Errorf("Expected StorePathRootDir %s, got %s", config.StorePathRootDir, newConfig.StorePathRootDir)
	}
	
	if newConfig.FlushDiskType != config.FlushDiskType {
		t.Errorf("Expected FlushDiskType %d, got %d", config.FlushDiskType, newConfig.FlushDiskType)
	}
	
	if newConfig.MapedFileSizeCommitLog != config.MapedFileSizeCommitLog {
		t.Errorf("Expected MapedFileSizeCommitLog %d, got %d", config.MapedFileSizeCommitLog, newConfig.MapedFileSizeCommitLog)
	}
}

// TestNewDefaultMessageStore 测试创建默认消息存储
func TestNewDefaultMessageStore(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		MapedFileSizeConsumeQueue: 6000,
		FlushDiskType:         ASYNC_FLUSH,
		FlushIntervalCommitLog: 500,
		FileReservedTime:      72,
		DeleteWhen:            "04",
		DiskMaxUsedSpaceRatio: 75,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	if store == nil {
		t.Fatal("Expected store to be created")
	}
	
	if store.storeConfig != config {
		t.Error("Expected store config to match")
	}
	
	if store.consumeQueueTable == nil {
		t.Error("Expected consumeQueueTable to be initialized")
	}
	
	if store.running {
		t.Error("Expected store to not be running initially")
	}
	
	// 测试目录创建
	if _, err := os.Stat(config.StorePathCommitLog); os.IsNotExist(err) {
		t.Error("Expected commitlog directory to be created")
	}
	
	if _, err := os.Stat(config.StorePathConsumeQueue); os.IsNotExist(err) {
		t.Error("Expected consumequeue directory to be created")
	}
	
	if _, err := os.Stat(config.StorePathIndex); os.IsNotExist(err) {
		t.Error("Expected index directory to be created")
	}
}

// TestMessageStoreStartShutdown 测试消息存储启动和关闭
func TestMessageStoreStartShutdown(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	// 测试启动
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	if !store.running {
		t.Error("Expected store to be running after start")
	}
	
	// 测试重复启动
	err = store.Start()
	if err == nil {
		t.Error("Expected error for duplicate start")
	}
	
	// 测试关闭
	store.Shutdown()
	
	if store.running {
		t.Error("Expected store to be stopped after shutdown")
	}
}

// TestPutMessage 测试存储消息
func TestPutMessage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()
	
	// 创建测试消息
	msg := &common.Message{
		Topic: "TestTopic",
		Body:  []byte("test message body"),
		Properties: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}
	
	// 测试存储消息
	result, err := store.PutMessage(msg)
	if err != nil {
		t.Fatalf("Failed to put message: %v", err)
	}
	
	if result == nil {
		t.Fatal("Expected result to be returned")
	}
	
	if result.SendStatus != common.SendOK {
		t.Errorf("Expected SendStatus to be SendOK, got %v", result.SendStatus)
	}
	
	if result.MsgId == "" {
		t.Error("Expected MsgId to be set")
	}
	
	if result.QueueOffset < 0 {
		t.Errorf("Expected QueueOffset to be >= 0, got %d", result.QueueOffset)
	}
	
	// 测试存储空消息
	_, err = store.PutMessage(nil)
	if err == nil {
		t.Error("Expected error for nil message")
	} else {
		t.Logf("Correctly got error for nil message: %v", err)
	}
	
	// 测试存储空主题消息
	emptyTopicMsg := &common.Message{
		Topic: "",
		Body:  []byte("test"),
	}
	_, err = store.PutMessage(emptyTopicMsg)
	if err == nil {
		t.Error("Expected error for empty topic")
	}
}

// TestGetMessage 测试获取消息
func TestGetMessage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()
	
	// 先存储一些消息
	for i := 0; i < 5; i++ {
		msg := &common.Message{
			Topic: "TestTopic",
			Body:  []byte(fmt.Sprintf("test message %d", i)),
		}
		_, err := store.PutMessage(msg)
		if err != nil {
			t.Fatalf("Failed to put message %d: %v", i, err)
		}
	}
	
	// 测试获取消息
	messages, err := store.GetMessage("TestTopic", 0, 0, 3)
	if err != nil {
		t.Fatalf("Failed to get messages: %v", err)
	}
	
	if len(messages) == 0 {
		t.Error("Expected to get some messages")
	}
	
	// 测试获取不存在的主题
	messages, err = store.GetMessage("NonExistentTopic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Failed to get messages for non-existent topic: %v", err)
	}
	
	if len(messages) != 0 {
		t.Errorf("Expected 0 messages for non-existent topic, got %d", len(messages))
	}
}

// TestGetMaxMinOffsetInQueue 测试获取队列最大最小偏移量
func TestGetMaxMinOffsetInQueue(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()
	
	// 测试空队列的偏移量
	maxOffset := store.GetMaxOffsetInQueue("TestTopic", 0)
	if maxOffset != 0 {
		t.Errorf("Expected max offset to be 0 for empty queue, got %d", maxOffset)
	}
	
	minOffset := store.GetMinOffsetInQueue("TestTopic", 0)
	if minOffset != 0 {
		t.Errorf("Expected min offset to be 0 for empty queue, got %d", minOffset)
	}
	
	// 存储一些消息
	for i := 0; i < 3; i++ {
		msg := &common.Message{
			Topic: "TestTopic",
			Body:  []byte(fmt.Sprintf("test message %d", i)),
		}
		_, err := store.PutMessage(msg)
		if err != nil {
			t.Fatalf("Failed to put message %d: %v", i, err)
		}
	}
	
	// 再次测试偏移量
	maxOffset = store.GetMaxOffsetInQueue("TestTopic", 0)
	if maxOffset <= 0 {
		t.Errorf("Expected max offset to be > 0 after storing messages, got %d", maxOffset)
	}
	
	minOffset = store.GetMinOffsetInQueue("TestTopic", 0)
	if minOffset != 0 {
		t.Errorf("Expected min offset to be 0, got %d", minOffset)
	}
}

// TestGetOrCreateConsumeQueue 测试获取或创建消费队列
func TestGetOrCreateConsumeQueue(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	// 测试创建新的消费队列
	cq1 := store.getOrCreateConsumeQueue("TestTopic", 0)
	if cq1 == nil {
		t.Fatal("Expected consume queue to be created")
	}
	
	// 测试获取已存在的消费队列
	cq2 := store.getOrCreateConsumeQueue("TestTopic", 0)
	if cq2 != cq1 {
		t.Error("Expected to get the same consume queue instance")
	}
	
	// 测试创建不同队列ID的消费队列
	cq3 := store.getOrCreateConsumeQueue("TestTopic", 1)
	if cq3 == nil {
		t.Fatal("Expected consume queue to be created for different queue ID")
	}
	
	if cq3 == cq1 {
		t.Error("Expected different consume queue instances for different queue IDs")
	}
	
	// 测试创建不同主题的消费队列
	cq4 := store.getOrCreateConsumeQueue("AnotherTopic", 0)
	if cq4 == nil {
		t.Fatal("Expected consume queue to be created for different topic")
	}
	
	if cq4 == cq1 {
		t.Error("Expected different consume queue instances for different topics")
	}
}

// TestPutDelayMessage 测试存储延迟消息
func TestPutDelayMessage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()
	
	msg := &common.Message{
		Topic: "DelayTopic",
		Body:  []byte("delay message"),
	}
	
	// 测试存储延迟消息
	result, err := store.PutDelayMessage(msg, 3) // 延迟级别3
	if err != nil {
		t.Fatalf("Failed to put delay message: %v", err)
	}
	
	if result == nil {
		t.Fatal("Expected result to be returned")
	}
}

// TestTransactionMessage 测试事务消息
func TestTransactionMessage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()
	
	msg := &common.Message{
		Topic: "TransactionTopic",
		Body:  []byte("transaction message"),
	}
	
	// 测试准备事务消息
	result, err := store.PrepareMessage(msg, "TestProducerGroup", "tx123")
	if err != nil {
		t.Fatalf("Failed to prepare transaction message: %v", err)
	}
	
	if result == nil {
		t.Fatal("Expected result to be returned")
	}
	
	// 测试提交事务
	err = store.CommitTransaction("tx123")
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	
	// 测试回滚事务
	result2, err := store.PrepareMessage(msg, "TestProducerGroup", "tx456")
	if err != nil {
		t.Fatalf("Failed to prepare second transaction message: %v", err)
	}
	
	if result2 == nil {
		t.Fatal("Expected result to be returned for second transaction")
	}
	
	err = store.RollbackTransaction("tx456")
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}
}

// TestOrderedMessage 测试顺序消息
func TestOrderedMessage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()
	
	msg := &common.Message{
		Topic: "OrderedTopic",
		Body:  []byte("ordered message"),
	}
	
	// 测试存储顺序消息
	result, err := store.PutOrderedMessage(msg, "shardingKey1")
	if err != nil {
		t.Fatalf("Failed to put ordered message: %v", err)
	}
	
	if result == nil {
		t.Fatal("Expected result to be returned")
	}
	
	// 测试拉取顺序消息
	messages, err := store.PullOrderedMessage("OrderedTopic", 0, "TestConsumerGroup", 10)
	if err != nil {
		t.Fatalf("Failed to pull ordered messages: %v", err)
	}
	
	if messages == nil {
		t.Error("Expected messages to be returned")
	}
}

// TestConsumeOffset 测试消费偏移量管理
func TestConsumeOffset(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()
	
	// 测试获取初始偏移量
	offset := store.GetConsumeOffset("TestTopic", 0, "TestConsumerGroup")
	if offset != -1 {
		t.Errorf("Expected initial offset to be -1, got %d", offset)
	}
	
	// 测试提交偏移量
	err = store.CommitConsumeOffset("TestTopic", 0, "TestConsumerGroup", 100)
	if err != nil {
		t.Fatalf("Failed to commit consume offset: %v", err)
	}
	
	// 测试获取已提交的偏移量
	offset = store.GetConsumeOffset("TestTopic", 0, "TestConsumerGroup")
	if offset != 100 {
		t.Errorf("Expected offset to be 100, got %d", offset)
	}
	
	// 测试更新偏移量
	err = store.CommitConsumeOffset("TestTopic", 0, "TestConsumerGroup", 200)
	if err != nil {
		t.Fatalf("Failed to update consume offset: %v", err)
	}
	
	offset = store.GetConsumeOffset("TestTopic", 0, "TestConsumerGroup")
	if offset != 200 {
		t.Errorf("Expected updated offset to be 200, got %d", offset)
	}
}

// TestGetCommitLog 测试获取CommitLog
func TestGetCommitLog(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}
	
	commitLog := store.GetCommitLog()
	if commitLog == nil {
		t.Fatal("Expected CommitLog to be returned")
	}
	
	if commitLog != store.commitLog {
		t.Error("Expected returned CommitLog to match internal instance")
	}
}

// Benchmark tests

// BenchmarkNewDefaultMessageStore 基准测试创建消息存储
func BenchmarkNewDefaultMessageStore(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "rocketmq-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewDefaultMessageStore(config)
	}
}

// BenchmarkPutMessage 基准测试存储消息
func BenchmarkPutMessage(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "rocketmq-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		b.Fatalf("Failed to create message store: %v", err)
	}
	
	err = store.Start()
	if err != nil {
		b.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()
	
	msg := &common.Message{
		Topic: "BenchmarkTopic",
		Body:  []byte("benchmark message body"),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.PutMessage(msg)
	}
}

// BenchmarkGetMessage 基准测试获取消息
func BenchmarkGetMessage(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "rocketmq-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		b.Fatalf("Failed to create message store: %v", err)
	}
	
	err = store.Start()
	if err != nil {
		b.Fatalf("Failed to start store: %v", err)
	}
	defer store.Shutdown()
	
	// 预先存储一些消息
	for i := 0; i < 100; i++ {
		msg := &common.Message{
			Topic: "BenchmarkTopic",
			Body:  []byte(fmt.Sprintf("benchmark message %d", i)),
		}
		_, _ = store.PutMessage(msg)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.GetMessage("BenchmarkTopic", 0, int64(i%100), 1)
	}
}

// BenchmarkGetOrCreateConsumeQueue 基准测试获取或创建消费队列
func BenchmarkGetOrCreateConsumeQueue(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "rocketmq-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024,
		FlushDiskType:         ASYNC_FLUSH,
	}
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		b.Fatalf("Failed to create message store: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic := fmt.Sprintf("Topic%d", i%10)
		queueId := int32(i % 4)
		_ = store.getOrCreateConsumeQueue(topic, queueId)
	}
}