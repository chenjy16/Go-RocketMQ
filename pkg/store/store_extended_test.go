package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go-rocketmq/pkg/common"
)

// TestStoreConfigValidation 测试存储配置验证
func TestStoreConfigValidation(t *testing.T) {
	// 测试无效配置
	config := &StoreConfig{
		StorePathRootDir: "",
		MapedFileSizeCommitLog: -1,
		FlushDiskType: FlushDiskType(999),
	}
	
	_, err := NewDefaultMessageStore(config)
	if err == nil {
		t.Error("Expected error for invalid config")
	}
	
	// 测试有效配置
	validConfig := NewDefaultStoreConfig()
	validConfig.StorePathRootDir = "/tmp/rocketmq-test-validation"
	
	store, err := NewDefaultMessageStore(validConfig)
	if err != nil {
		t.Fatalf("Failed to create store with valid config: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(validConfig.StorePathRootDir)
	}()
}

// TestMessageStoreConcurrentOperations 测试消息存储并发操作
func TestMessageStoreConcurrentOperations(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-concurrent"
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 并发写入消息
	var wg sync.WaitGroup
	const numGoroutines = 10
	const messagesPerGoroutine = 100
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineId int) {
			defer wg.Done()
			
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := &common.Message{
					Topic: "TestTopic",
					Body:  []byte(fmt.Sprintf("Message from routine %d, msg %d", routineId, j)),
				}
				
				_, err := store.PutMessage(msg)
				if err != nil {
					t.Errorf("Failed to put message: %v", err)
					return
				}
			}
		}(i)
	}
	
	wg.Wait()
	
	// 验证消息数量
	for queueId := 0; queueId < 4; queueId++ {
		maxOffset := store.GetMaxOffsetInQueue("TestTopic", int32(queueId))
		if maxOffset <= 0 {
			t.Errorf("Expected messages in queue %d, got max offset %d", queueId, maxOffset)
		}
	}
}

// TestCommitLogErrorHandling 测试CommitLog错误处理
func TestCommitLogErrorHandling(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-commitlog-error"
	
	// 创建无效的存储路径
	config.StorePathCommitLog = "/invalid/path/that/does/not/exist"
	
	_, err := NewDefaultMessageStore(config)
	if err == nil {
		t.Error("Expected error for invalid commit log path")
	}
	
	// 测试正常路径
	config.StorePathCommitLog = filepath.Join(config.StorePathRootDir, "commitlog")
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 测试空消息
	_, err = store.PutMessage(nil)
	if err == nil {
		t.Error("Expected error for nil message")
	}
	
	// 测试无效消息
	invalidMsg := &common.Message{
		Topic: "", // 空topic
		Body:  []byte("test"),
	}
	
	_, err = store.PutMessage(invalidMsg)
	if err == nil {
		t.Error("Expected error for invalid message")
	}
}

// TestConsumeQueueOperations 测试ConsumeQueue操作
func TestConsumeQueueOperations(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-consumequeue"
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 测试多个主题，每个主题写入多条消息
	topics := []string{"Topic1", "Topic2", "Topic3"}
	
	for _, topic := range topics {
		// 为每个主题写入16条消息，确保所有队列都有消息
		for i := 0; i < 16; i++ {
			msg := &common.Message{
				Topic: topic,
				Body:  []byte(fmt.Sprintf("Message %d for %s", i, topic)),
			}
			
			_, err := store.PutMessage(msg)
			if err != nil {
				t.Errorf("Failed to put message: %v", err)
			}
		}
		
		// 验证每个队列都有消息
		for queueId := int32(0); queueId < 4; queueId++ {
			maxOffset := store.GetMaxOffsetInQueue(topic, queueId)
			minOffset := store.GetMinOffsetInQueue(topic, queueId)
			
			if maxOffset <= minOffset {
				t.Errorf("Invalid offset range for %s queue %d: min=%d, max=%d", 
					topic, queueId, minOffset, maxOffset)
			}
		}
	}
}

// TestIndexFileOperations 测试IndexFile操作
func TestIndexFileOperations(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-index"
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 写入带有不同key的消息
	keys := []string{"key1", "key2", "key3", "key1"} // key1重复
	
	for i, key := range keys {
		msg := &common.Message{
			Topic: "TestTopic",
			Body:  []byte(fmt.Sprintf("Message %d", i)),
			Keys:  key,
		}
		
		_, err := store.PutMessage(msg)
		if err != nil {
			t.Errorf("Failed to put message with key %s: %v", key, err)
		}
	}
	
	// 等待索引构建
	time.Sleep(100 * time.Millisecond)
}

// TestStoreRecovery 测试存储恢复
func TestStoreRecovery(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-recovery"
	
	// 第一次启动，写入数据
	store1, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	err = store1.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 写入一些消息
	for i := 0; i < 10; i++ {
		msg := &common.Message{
			Topic: "RecoveryTopic",
			Body:  []byte(fmt.Sprintf("Recovery message %d", i)),
		}
		
		_, err := store1.PutMessage(msg)
		if err != nil {
			t.Errorf("Failed to put message: %v", err)
		}
	}
	
	// 计算所有队列的总消息数
	totalMessages1 := int64(0)
	for queueId := int32(0); queueId < 4; queueId++ {
		totalMessages1 += store1.GetMaxOffsetInQueue("RecoveryTopic", queueId)
	}
	
	store1.Shutdown()
	
	// 第二次启动，验证恢复
	store2, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store for recovery: %v", err)
	}
	
	defer func() {
		store2.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store2.Start()
	if err != nil {
		t.Fatalf("Failed to start store for recovery: %v", err)
	}
	
	// 计算恢复后所有队列的总消息数
	totalMessages2 := int64(0)
	for queueId := int32(0); queueId < 4; queueId++ {
		totalMessages2 += store2.GetMaxOffsetInQueue("RecoveryTopic", queueId)
	}
	
	if totalMessages2 != totalMessages1 {
		t.Errorf("Recovery failed: expected total messages %d, got %d", totalMessages1, totalMessages2)
	}
}

// TestStoreFlushOperations 测试存储刷盘操作
func TestStoreFlushOperations(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-flush"
	config.FlushDiskType = SYNC_FLUSH // 同步刷盘
	config.FlushIntervalCommitLog = 100 // 100ms刷盘间隔
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 写入消息并验证刷盘
	msg := &common.Message{
		Topic: "FlushTopic",
		Body:  []byte("Flush test message"),
	}
	
	result, err := store.PutMessage(msg)
	if err != nil {
		t.Fatalf("Failed to put message: %v", err)
	}
	
	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	
	// 验证消息可以读取（检查所有队列）
	var allMessages []*common.MessageExt
	for queueId := int32(0); queueId < 4; queueId++ {
		messages, err := store.GetMessage("FlushTopic", queueId, 0, 10)
		if err == nil && len(messages) > 0 {
			allMessages = append(allMessages, messages...)
		}
	}
	
	if len(allMessages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(allMessages))
	}
}

// TestStoreEdgeCases 测试存储边界情况
func TestStoreEdgeCases(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-edge"
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 测试获取不存在的消息（使用唯一的topic名称）
	_, err = store.GetMessage("UniqueNonExistentTopic12345", 0, 0, 1)
	if err == nil {
		t.Error("Expected error for non-existent topic")
	}
	
	// 测试无效的队列ID
	maxOffset := store.GetMaxOffsetInQueue("TestTopic", -1)
	if maxOffset != 0 {
		t.Errorf("Expected 0 for invalid queue ID, got %d", maxOffset)
	}
	
	// 测试超大消息体
	largeBody := make([]byte, 1024*1024) // 1MB
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}
	
	largeMsg := &common.Message{
		Topic: "LargeTopic",
		Body:  largeBody,
	}
	
	_, err = store.PutMessage(largeMsg)
	if err != nil {
		t.Errorf("Failed to put large message: %v", err)
	}
}

// TestDelayQueueOperations 测试延迟队列操作
func TestDelayQueueOperations(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-delay"
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 测试延迟消息
	delayMsg := &common.Message{
		Topic: "DelayTopic",
		Body:  []byte("Delay message"),
	}
	
	_, err = store.PutDelayMessage(delayMsg, 1) // 延迟级别1
	if err != nil {
		t.Errorf("Failed to put delay message: %v", err)
	}
}

// TestTransactionOperations 测试事务操作
func TestTransactionOperations(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-transaction"
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 测试事务消息
	txMsg := &common.Message{
		Topic: "TransactionTopic",
		Body:  []byte("Transaction message"),
	}
	
	_, err = store.PrepareMessage(txMsg, "testGroup", "tx123")
	if err != nil {
		t.Errorf("Failed to prepare transaction message: %v", err)
	}
	
	// 提交事务
	err = store.CommitTransaction("tx123")
	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}
}

// TestOrderedQueueOperations 测试顺序队列操作
func TestOrderedQueueOperations(t *testing.T) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-test-ordered"
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start store: %v", err)
	}
	
	// 测试顺序消息
	orderedMsg := &common.Message{
		Topic: "OrderedTopic",
		Body:  []byte("Ordered message"),
	}
	
	_, err = store.PutOrderedMessage(orderedMsg, "shardingKey1")
	if err != nil {
		t.Errorf("Failed to put ordered message: %v", err)
	}
	
	// 计算shardingKey1对应的队列ID（简单哈希算法）
	hash := uint32(0)
	for _, b := range []byte("shardingKey1") {
		hash = hash*31 + uint32(b)
	}
	queueId := int32(hash % 4) // 4个队列
	
	// 拉取顺序消息（从正确的队列拉取）
	messages, err := store.PullOrderedMessage("OrderedTopic", queueId, "testGroup", 10)
	if err != nil {
		t.Errorf("Failed to pull ordered message: %v", err)
	}
	
	if len(messages) == 0 {
		t.Error("Expected at least one ordered message")
	}
}

// BenchmarkStoreOperations 存储操作基准测试
func BenchmarkStoreOperations(b *testing.B) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-bench-store"
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		b.Fatalf("Failed to start store: %v", err)
	}
	
	msg := &common.Message{
		Topic: "BenchTopic",
		Body:  []byte("Benchmark message"),
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := store.PutMessage(msg)
			if err != nil {
				b.Errorf("Failed to put message: %v", err)
			}
		}
	})
}

// BenchmarkGetMessageStore 获取消息基准测试
func BenchmarkGetMessageStore(b *testing.B) {
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = "/tmp/rocketmq-bench-get"
	
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	
	defer func() {
		store.Shutdown()
		os.RemoveAll(config.StorePathRootDir)
	}()
	
	err = store.Start()
	if err != nil {
		b.Fatalf("Failed to start store: %v", err)
	}
	
	// 预先写入一些消息
	for i := 0; i < 1000; i++ {
		msg := &common.Message{
			Topic: "BenchGetTopic",
			Body:  []byte(fmt.Sprintf("Message %d", i)),
		}
		
		_, err := store.PutMessage(msg)
		if err != nil {
			b.Fatalf("Failed to put message: %v", err)
		}
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := store.GetMessage("BenchGetTopic", 0, 0, 1)
			if err != nil {
				b.Errorf("Failed to get message: %v", err)
			}
		}
	})
}