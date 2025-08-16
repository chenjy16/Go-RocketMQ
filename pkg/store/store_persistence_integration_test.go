package store

import (
	"os"
	"testing"
	"time"

	"go-rocketmq/pkg/common"
)

func TestStorePersistenceIntegration(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-store-persistence-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建存储配置
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    tempDir + "/commitlog",
		StorePathConsumeQueue: tempDir + "/consumequeue",
		StorePathIndex:        tempDir + "/index",
		MapedFileSizeCommitLog:   1024 * 1024 * 10, // 10MB
		MapedFileSizeConsumeQueue: 1024 * 1024,     // 1MB
		MapedFileSizeIndexFile:   1024 * 1024,     // 1MB
		FlushDiskType:            ASYNC_FLUSH,
		FlushIntervalCommitLog:   1000,
		FlushCommitLogLeastPages: 4,
		FlushConsumeQueueLeastPages: 2,
		FlushIntervalConsumeQueue:   1000,
		FileReservedTime:         72,
		DeleteWhen:               "04",
		DiskMaxUsedSpaceRatio:    75,
	}

	// 创建消息存储
	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}

	// 启动存储
	err = store.Start()
	if err != nil {
		t.Fatalf("Failed to start message store: %v", err)
	}
	defer store.Shutdown()

	// 测试消息存储和索引
	t.Run("MessageStoreAndIndex", func(t *testing.T) {
		testMessageStoreAndIndex(t, store)
	})

	// 测试消费进度持久化
	t.Run("ConsumeProgressPersistence", func(t *testing.T) {
		testConsumeProgressPersistence(t, store)
	})

	// 测试延迟队列进度持久化
	t.Run("DelayProgressPersistence", func(t *testing.T) {
		testDelayProgressPersistence(t, store)
	})
}

func testMessageStoreAndIndex(t *testing.T, store *DefaultMessageStore) {
	// 创建测试消息
	msg := &common.Message{
		Topic: "TestTopic",
		Tags:  "TagA",
		Keys:  "msg_key_123",
		Body:  []byte("Hello RocketMQ with Persistence"),
	}
	msg.SetProperty("UNIQ_KEY", "uniq_key_456")

	// 存储消息
	result, err := store.PutMessage(msg)
	if err != nil {
		t.Fatalf("Failed to put message: %v", err)
	}

	if result.SendStatus != common.SendOK {
		t.Errorf("Expected send status OK, got %v", result.SendStatus)
	}

	// 等待一段时间确保索引构建完成
	time.Sleep(100 * time.Millisecond)

	// 验证消息索引是否添加到持久化管理器
	msgIndex := store.persistenceManager.GetMessageIndex("msg_key_123")
	if msgIndex == nil {
		t.Error("Expected message index for msg_key_123, got nil")
	} else {
		if msgIndex.Topic != "TestTopic" {
			t.Errorf("Expected topic TestTopic, got %s", msgIndex.Topic)
		}
		if msgIndex.Tags != "TagA" {
			t.Errorf("Expected tags TagA, got %s", msgIndex.Tags)
		}
	}

	// 验证UNIQ_KEY索引
	uniqIndex := store.persistenceManager.GetMessageIndex("uniq_key_456")
	if uniqIndex == nil {
		t.Error("Expected message index for uniq_key_456, got nil")
	}

	// 测试按Key查询消息
	queryResults := store.persistenceManager.QueryMessagesByKey("msg_key_123")
	if len(queryResults) == 0 {
		t.Error("Expected at least one query result for msg_key_123")
	}

	// 测试按时间范围查询消息
	startTime := time.Now().Add(-1 * time.Hour).Unix()
	endTime := time.Now().Add(1 * time.Hour).Unix()
	timeResults := store.persistenceManager.QueryMessagesByTimeRange(startTime, endTime)
	if len(timeResults) == 0 {
		t.Error("Expected at least one query result in time range")
	}
}

func testConsumeProgressPersistence(t *testing.T, store *DefaultMessageStore) {
	topic := "TestTopic"
	queueId := int32(0)
	consumerGroup := "TestConsumerGroup"
	offset := int64(100)

	// 提交消费进度
	err := store.CommitConsumeOffset(topic, queueId, consumerGroup, offset)
	if err != nil {
		t.Fatalf("Failed to commit consume offset: %v", err)
	}

	// 获取消费进度
	retrievedOffset := store.GetConsumeOffset(topic, queueId, consumerGroup)
	if retrievedOffset != offset {
		t.Errorf("Expected consume offset %d, got %d", offset, retrievedOffset)
	}

	// 验证持久化管理器中的消费进度
	persistentOffset := store.persistenceManager.GetConsumeProgress(topic, queueId, consumerGroup)
	if persistentOffset != offset {
		t.Errorf("Expected persistent consume offset %d, got %d", offset, persistentOffset)
	}

	// 更新消费进度
	newOffset := int64(200)
	err = store.CommitConsumeOffset(topic, queueId, consumerGroup, newOffset)
	if err != nil {
		t.Fatalf("Failed to commit new consume offset: %v", err)
	}

	// 验证更新后的消费进度
	updatedOffset := store.GetConsumeOffset(topic, queueId, consumerGroup)
	if updatedOffset != newOffset {
		t.Errorf("Expected updated consume offset %d, got %d", newOffset, updatedOffset)
	}
}

func testDelayProgressPersistence(t *testing.T, store *DefaultMessageStore) {
	// 测试延迟队列进度（通过延迟队列服务间接测试）
	delayLevel := int32(1)
	offset := int64(50)

	// 直接更新持久化管理器中的延迟队列进度
	store.persistenceManager.UpdateDelayProgress(delayLevel, offset)

	// 验证延迟队列进度
	retrievedOffset := store.persistenceManager.GetDelayProgress(delayLevel)
	if retrievedOffset != offset {
		t.Errorf("Expected delay progress %d, got %d", offset, retrievedOffset)
	}

	// 更新延迟队列进度
	newOffset := int64(100)
	store.persistenceManager.UpdateDelayProgress(delayLevel, newOffset)

	// 验证更新后的延迟队列进度
	updatedOffset := store.persistenceManager.GetDelayProgress(delayLevel)
	if updatedOffset != newOffset {
		t.Errorf("Expected updated delay progress %d, got %d", newOffset, updatedOffset)
	}
}

func TestStorePersistenceRestart(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-store-restart-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建存储配置
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    tempDir + "/commitlog",
		StorePathConsumeQueue: tempDir + "/consumequeue",
		StorePathIndex:        tempDir + "/index",
		MapedFileSizeCommitLog:   1024 * 1024 * 10, // 10MB
		MapedFileSizeConsumeQueue: 1024 * 1024,     // 1MB
		MapedFileSizeIndexFile:   1024 * 1024,     // 1MB
		FlushDiskType:            ASYNC_FLUSH,
		FlushIntervalCommitLog:   1000,
		FlushCommitLogLeastPages: 4,
		FlushConsumeQueueLeastPages: 2,
		FlushIntervalConsumeQueue:   1000,
		FileReservedTime:         72,
		DeleteWhen:               "04",
		DiskMaxUsedSpaceRatio:    75,
	}

	// 第一次启动：创建数据
	store1, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create first message store: %v", err)
	}

	err = store1.Start()
	if err != nil {
		t.Fatalf("Failed to start first message store: %v", err)
	}

	// 存储测试数据
	msg := &common.Message{
		Topic: "RestartTestTopic",
		Tags:  "RestartTag",
		Keys:  "restart_key_123",
		Body:  []byte("Restart test message"),
	}
	_, err = store1.PutMessage(msg)
	if err != nil {
		t.Fatalf("Failed to put message in first store: %v", err)
	}

	// 提交消费进度
	err = store1.CommitConsumeOffset("RestartTestTopic", 0, "RestartGroup", 999)
	if err != nil {
		t.Fatalf("Failed to commit consume offset in first store: %v", err)
	}

	// 等待数据持久化
	time.Sleep(200 * time.Millisecond)

	// 关闭第一个存储
	store1.Shutdown()

	// 第二次启动：验证数据恢复
	store2, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create second message store: %v", err)
	}

	err = store2.Start()
	if err != nil {
		t.Fatalf("Failed to start second message store: %v", err)
	}
	defer store2.Shutdown()

	// 验证消费进度是否恢复
	restoredOffset := store2.GetConsumeOffset("RestartTestTopic", 0, "RestartGroup")
	if restoredOffset != 999 {
		t.Errorf("Expected restored consume offset 999, got %d", restoredOffset)
	}

	// 验证消息索引是否恢复
	restoredIndex := store2.persistenceManager.GetMessageIndex("restart_key_123")
	if restoredIndex == nil {
		t.Error("Expected restored message index for restart_key_123, got nil")
	} else {
		if restoredIndex.Topic != "RestartTestTopic" {
			t.Errorf("Expected restored topic RestartTestTopic, got %s", restoredIndex.Topic)
		}
		if restoredIndex.Tags != "RestartTag" {
			t.Errorf("Expected restored tags RestartTag, got %s", restoredIndex.Tags)
		}
	}
}