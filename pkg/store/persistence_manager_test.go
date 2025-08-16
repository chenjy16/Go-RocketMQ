package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-rocketmq/pkg/common"
)

func TestPersistenceManager(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-persistence-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建配置
	config := &StoreConfig{
		StorePathRootDir: tempDir,
	}

	// 创建持久化管理器
	pm := NewPersistenceManager(config)

	// 启动
	err = pm.Start()
	if err != nil {
		t.Fatalf("Failed to start persistence manager: %v", err)
	}
	defer pm.Stop()

	// 测试消费进度
	t.Run("ConsumeProgress", func(t *testing.T) {
		testConsumeProgress(t, pm)
	})

	// 测试延迟队列进度
	t.Run("DelayProgress", func(t *testing.T) {
		testDelayProgress(t, pm)
	})

	// 测试事务状态
	t.Run("TransactionState", func(t *testing.T) {
		testTransactionState(t, pm)
	})

	// 测试消息索引
	t.Run("MessageIndex", func(t *testing.T) {
		testMessageIndex(t, pm)
	})
}

func testConsumeProgress(t *testing.T, pm *PersistenceManager) {
	// 测试更新和获取消费进度
	topic := "TestTopic"
	queueId := int32(0)
	consumerGroup := "TestGroup"
	offset := int64(100)

	// 更新消费进度
	pm.UpdateConsumeProgress(topic, queueId, consumerGroup, offset)

	// 获取消费进度
	result := pm.GetConsumeProgress(topic, queueId, consumerGroup)
	if result != offset {
		t.Errorf("Expected offset %d, got %d", offset, result)
	}

	// 测试不存在的消费进度
	result = pm.GetConsumeProgress("NonExistent", 0, "NonExistent")
	if result != -1 {
		t.Errorf("Expected -1 for non-existent progress, got %d", result)
	}

	// 测试获取所有消费进度
	allProgress := pm.GetAllConsumeProgress()
	if len(allProgress) == 0 {
		t.Error("Expected at least one consume progress entry")
	}
}

func testDelayProgress(t *testing.T, pm *PersistenceManager) {
	// 测试更新和获取延迟队列进度
	delayLevel := int32(1)
	offset := int64(50)

	// 更新延迟队列进度
	pm.UpdateDelayProgress(delayLevel, offset)

	// 获取延迟队列进度
	result := pm.GetDelayProgress(delayLevel)
	if result != offset {
		t.Errorf("Expected delay offset %d, got %d", offset, result)
	}

	// 测试不存在的延迟队列进度
	result = pm.GetDelayProgress(999)
	if result != 0 {
		t.Errorf("Expected 0 for non-existent delay progress, got %d", result)
	}

	// 测试获取所有延迟队列进度
	allDelayProgress := pm.GetAllDelayProgress()
	if len(allDelayProgress) == 0 {
		t.Error("Expected at least one delay progress entry")
	}
}

func testTransactionState(t *testing.T, pm *PersistenceManager) {
	// 测试更新和获取事务状态
	transactionId := "tx_123"
	state := &PersistentTransactionState{
		TransactionId: transactionId,
		ProducerGroup: "TestProducerGroup",
		State:         TransactionStateCommit,
		CreateTime:    time.Now().Unix(),
		Message: &common.Message{
			Topic: "TestTopic",
			Body:  []byte("test message"),
		},
	}

	// 更新事务状态
	pm.UpdateTransactionState(transactionId, state)

	// 获取事务状态
	result := pm.GetTransactionState(transactionId)
	if result == nil {
		t.Error("Expected transaction state, got nil")
		return
	}
	if result.TransactionId != transactionId {
		t.Errorf("Expected transaction ID %s, got %s", transactionId, result.TransactionId)
	}
	if result.State != TransactionStateCommit {
		t.Errorf("Expected state %v, got %v", TransactionStateCommit, result.State)
	}

	// 测试不存在的事务状态
	result = pm.GetTransactionState("non_existent")
	if result != nil {
		t.Error("Expected nil for non-existent transaction state")
	}

	// 测试移除事务状态
	pm.RemoveTransactionState(transactionId)
	result = pm.GetTransactionState(transactionId)
	if result != nil {
		t.Error("Expected nil after removing transaction state")
	}

	// 重新添加用于测试获取所有事务状态
	pm.UpdateTransactionState(transactionId, state)
	allStates := pm.GetAllTransactionStates()
	if len(allStates) == 0 {
		t.Error("Expected at least one transaction state")
	}
}

func testMessageIndex(t *testing.T, pm *PersistenceManager) {
	// 测试添加和获取消息索引
	messageKey := "msg_key_123"
	index := &MessageIndex{
		MessageKey: messageKey,
		Topic:      "TestTopic",
		QueueId:    0,
		Offset:     100,
		StoreTime:  time.Now().Unix(),
		Tags:       "TagA",
	}

	// 添加消息索引
	pm.AddMessageIndex(messageKey, index)

	// 获取消息索引
	result := pm.GetMessageIndex(messageKey)
	if result == nil {
		t.Error("Expected message index, got nil")
		return
	}
	if result.MessageKey != messageKey {
		t.Errorf("Expected message key %s, got %s", messageKey, result.MessageKey)
	}
	if result.Topic != "TestTopic" {
		t.Errorf("Expected topic TestTopic, got %s", result.Topic)
	}

	// 测试根据Key查询消息
	results := pm.QueryMessagesByKey(messageKey)
	if len(results) == 0 {
		t.Error("Expected at least one message index result")
	}

	// 测试根据时间范围查询消息
	startTime := time.Now().Add(-1 * time.Hour).Unix()
	endTime := time.Now().Add(1 * time.Hour).Unix()
	timeResults := pm.QueryMessagesByTimeRange(startTime, endTime)
	if len(timeResults) == 0 {
		t.Error("Expected at least one message index result in time range")
	}

	// 测试不存在的消息索引
	result = pm.GetMessageIndex("non_existent")
	if result != nil {
		t.Error("Expected nil for non-existent message index")
	}
}

func TestPersistenceManagerPersistence(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-persistence-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建配置
	config := &StoreConfig{
		StorePathRootDir: tempDir,
	}

	// 第一次启动，添加数据
	pm1 := NewPersistenceManager(config)
	err = pm1.Start()
	if err != nil {
		t.Fatalf("Failed to start first persistence manager: %v", err)
	}

	// 添加测试数据
	pm1.UpdateConsumeProgress("TestTopic", 0, "TestGroup", 100)
	pm1.UpdateDelayProgress(1, 50)
	txState := &PersistentTransactionState{
		TransactionId: "tx_persist_test",
		ProducerGroup: "TestGroup",
		State:         TransactionStateCommit,
		CreateTime:    time.Now().Unix(),
		Message: &common.Message{
			Topic: "TestTopic",
			Body:  []byte("persist test"),
		},
	}
	pm1.UpdateTransactionState("tx_persist_test", txState)
	msgIndex := &MessageIndex{
		MessageKey: "persist_msg_key",
		Topic:      "TestTopic",
		QueueId:    0,
		Offset:     200,
		StoreTime:  time.Now().Unix(),
		Tags:       "PersistTag",
	}
	pm1.AddMessageIndex("persist_msg_key", msgIndex)

	// 停止第一个管理器（触发保存）
	err = pm1.Stop()
	if err != nil {
		t.Fatalf("Failed to stop first persistence manager: %v", err)
	}

	// 验证文件是否创建
	persistenceDir := filepath.Join(tempDir, "persistence")
	files := []string{
		"consume_progress.json",
		"delay_progress.json",
		"transaction_state.json",
		"message_index.json",
	}
	for _, file := range files {
		filePath := filepath.Join(persistenceDir, file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Expected persistence file %s to exist", file)
		}
	}

	// 第二次启动，验证数据是否恢复
	pm2 := NewPersistenceManager(config)
	err = pm2.Start()
	if err != nil {
		t.Fatalf("Failed to start second persistence manager: %v", err)
	}
	defer pm2.Stop()

	// 验证消费进度
	consumeOffset := pm2.GetConsumeProgress("TestTopic", 0, "TestGroup")
	if consumeOffset != 100 {
		t.Errorf("Expected consume offset 100, got %d", consumeOffset)
	}

	// 验证延迟队列进度
	delayOffset := pm2.GetDelayProgress(1)
	if delayOffset != 50 {
		t.Errorf("Expected delay offset 50, got %d", delayOffset)
	}

	// 验证事务状态
	restoredTxState := pm2.GetTransactionState("tx_persist_test")
	if restoredTxState == nil {
		t.Error("Expected restored transaction state, got nil")
	} else {
		if restoredTxState.TransactionId != "tx_persist_test" {
			t.Errorf("Expected transaction ID tx_persist_test, got %s", restoredTxState.TransactionId)
		}
		if restoredTxState.State != TransactionStateCommit {
			t.Errorf("Expected state %v, got %v", TransactionStateCommit, restoredTxState.State)
		}
	}

	// 验证消息索引
	restoredIndex := pm2.GetMessageIndex("persist_msg_key")
	if restoredIndex == nil {
		t.Error("Expected restored message index, got nil")
	} else {
		if restoredIndex.MessageKey != "persist_msg_key" {
			t.Errorf("Expected message key persist_msg_key, got %s", restoredIndex.MessageKey)
		}
		if restoredIndex.Offset != 200 {
			t.Errorf("Expected offset 200, got %d", restoredIndex.Offset)
		}
	}
}

func TestPersistenceManagerAutoSave(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-persistence-autosave-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建配置
	config := &StoreConfig{
		StorePathRootDir: tempDir,
	}

	// 创建持久化管理器
	pm := NewPersistenceManager(config)

	// 启动
	err = pm.Start()
	if err != nil {
		t.Fatalf("Failed to start persistence manager: %v", err)
	}
	defer pm.Stop()

	// 修改自动保存间隔为较短时间（仅用于测试）
	pm.autoSaveTicker.Stop()
	pm.autoSaveTicker = time.NewTicker(100 * time.Millisecond)

	// 添加数据
	pm.UpdateConsumeProgress("AutoSaveTopic", 0, "AutoSaveGroup", 999)

	// 等待自动保存触发
	time.Sleep(200 * time.Millisecond)

	// 验证文件是否存在
	persistenceDir := filepath.Join(tempDir, "persistence")
	consumeProgressFile := filepath.Join(persistenceDir, "consume_progress.json")
	if _, err := os.Stat(consumeProgressFile); os.IsNotExist(err) {
		t.Error("Expected auto-saved consume progress file to exist")
	}
}