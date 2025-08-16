package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-rocketmq/pkg/common"
)

// TestTransactionPersistence 测试事务消息持久化
func TestTransactionPersistence(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq-transaction-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建配置
	config := &StoreConfig{
		StorePathRootDir:      tempDir,
		StorePathCommitLog:    filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue: filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:        filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog:   1024 * 1024, // 1MB
		MapedFileSizeConsumeQueue: 6000,
		FlushDiskType:         ASYNC_FLUSH,
		FlushIntervalCommitLog: 500,
		FileReservedTime:      72,
		DeleteWhen:            "04",
		DiskMaxUsedSpaceRatio: 75,
	}

	// 创建消息存储
	messageStore, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}

	// 启动消息存储
	err = messageStore.Start()
	if err != nil {
		t.Fatalf("Failed to start message store: %v", err)
	}
	defer messageStore.Shutdown()

	// 测试事务消息准备
	testTransactionPrepare(t, messageStore)

	// 测试事务状态持久化
	testTransactionStatePersistence(t, messageStore, config)

	// 测试事务超时处理
	testTransactionTimeout(t, messageStore)
}

func testTransactionPrepare(t *testing.T, messageStore *DefaultMessageStore) {
	// 创建测试消息
	msg := &common.Message{
		Topic: "TestTransactionTopic",
		Tags:  "TestTag",
		Keys:  "TestKey",
		Body:  []byte("test transaction message"),
		Properties: make(map[string]string),
	}

	producerGroup := "TestProducerGroup"
	transactionId := "tx_test_123"

	// 注册事务监听器
	listener := &TestTransactionListener{}
	messageStore.RegisterTransactionListener(producerGroup, listener)

	// 准备事务消息
	result, err := messageStore.PrepareMessage(msg, producerGroup, transactionId)
	if err != nil {
		t.Fatalf("Failed to prepare transaction message: %v", err)
	}

	if result.SendStatus != common.SendOK {
		t.Errorf("Expected send status OK, got %v", result.SendStatus)
	}

	// 验证事务状态是否保存到持久化管理器
	txState := messageStore.persistenceManager.GetTransactionState(transactionId)
	if txState == nil {
		t.Error("Transaction state not found in persistence manager")
		return
	}

	if txState.TransactionId != transactionId {
		t.Errorf("Expected transaction ID %s, got %s", transactionId, txState.TransactionId)
	}

	if txState.State != TransactionStateUnknown {
		t.Errorf("Expected transaction state UNKNOWN, got %v", txState.State)
	}

	// 提交事务
	err = messageStore.CommitTransaction(transactionId)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// 验证事务状态是否从持久化管理器中移除
	txState = messageStore.persistenceManager.GetTransactionState(transactionId)
	if txState != nil {
		t.Error("Transaction state should be removed after commit")
	}
}

func testTransactionStatePersistence(t *testing.T, messageStore *DefaultMessageStore, config *StoreConfig) {
	// 创建测试消息
	msg := &common.Message{
		Topic: "TestPersistenceTopic",
		Tags:  "PersistTag",
		Keys:  "PersistKey",
		Body:  []byte("test persistence message"),
		Properties: make(map[string]string),
	}

	producerGroup := "TestPersistenceGroup"
	transactionId := "tx_persist_456"

	// 注册事务监听器
	listener := &TestTransactionListener{}
	messageStore.RegisterTransactionListener(producerGroup, listener)

	// 准备事务消息
	_, err := messageStore.PrepareMessage(msg, producerGroup, transactionId)
	if err != nil {
		t.Fatalf("Failed to prepare transaction message: %v", err)
	}

	// 验证事务状态是否已保存到持久化管理器
	txState := messageStore.persistenceManager.GetTransactionState(transactionId)
	if txState == nil {
		t.Fatalf("Transaction state not saved to persistence manager after prepare")
	}
	t.Logf("Transaction state saved: %+v", txState)

	// 手动触发持久化保存
	err = messageStore.persistenceManager.saveAll()
	if err != nil {
		t.Fatalf("Failed to save persistence data: %v", err)
	}
	t.Logf("Persistence data saved successfully")
	
	// 停止消息存储
	messageStore.Shutdown()

	// 重新创建消息存储
	newMessageStore, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create new message store: %v", err)
	}

	// 启动新的消息存储
	err = newMessageStore.Start()
	if err != nil {
		t.Fatalf("Failed to start new message store: %v", err)
	}
	defer newMessageStore.Shutdown()

	// 验证事务状态是否恢复
	txState = newMessageStore.persistenceManager.GetTransactionState(transactionId)
	if txState == nil {
		t.Error("Transaction state not restored from persistence")
		return
	}

	if txState.TransactionId != transactionId {
		t.Errorf("Expected transaction ID %s, got %s", transactionId, txState.TransactionId)
	}

	if txState.State != TransactionStateUnknown {
		t.Errorf("Expected transaction state UNKNOWN, got %v", txState.State)
	}

	// 验证事务服务中的内存状态是否恢复
	txService := newMessageStore.transactionService
	txService.transactionMutex.RLock()
	record, exists := txService.transactionMap[transactionId]
	txService.transactionMutex.RUnlock()

	if !exists {
		t.Error("Transaction record not restored in transaction service")
		return
	}

	if record.TransactionId != transactionId {
		t.Errorf("Expected transaction ID %s, got %s", transactionId, record.TransactionId)
	}

	// 清理：回滚事务
	err = newMessageStore.RollbackTransaction(transactionId)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}
}

func testTransactionTimeout(t *testing.T, messageStore *DefaultMessageStore) {
	// 确保消息存储正在运行
	if !messageStore.running {
		err := messageStore.Start()
		if err != nil {
			t.Fatalf("Failed to start message store: %v", err)
		}
	}
	
	// 创建测试消息
	msg := &common.Message{
		Topic: "TestTimeoutTopic",
		Tags:  "TimeoutTag",
		Keys:  "TimeoutKey",
		Body:  []byte("test timeout message"),
		Properties: make(map[string]string),
	}

	producerGroup := "TestTimeoutGroup"
	transactionId := "tx_timeout_789"

	// 注册事务监听器
	listener := &TestTransactionListener{
		checkResult: TransactionStateUnknown, // 模拟检查返回未知状态
	}
	messageStore.RegisterTransactionListener(producerGroup, listener)

	// 准备事务消息
	_, err := messageStore.PrepareMessage(msg, producerGroup, transactionId)
	if err != nil {
		t.Fatalf("Failed to prepare transaction message: %v", err)
	}

	// 获取事务服务
	txService := messageStore.transactionService

	// 模拟超过最大检查次数
	txService.transactionMutex.Lock()
	record := txService.transactionMap[transactionId]
	if record != nil {
		record.CheckCount = 16 // 超过最大检查次数15
		record.UpdateTime = time.Now().Add(-2 * time.Minute) // 设置为2分钟前
	}
	txService.transactionMutex.Unlock()

	// 手动触发事务检查
	txService.checkTransactions()

	// 等待异步回滚完成
	time.Sleep(100 * time.Millisecond)

	// 验证事务是否被自动回滚
	txState := messageStore.persistenceManager.GetTransactionState(transactionId)
	if txState != nil {
		t.Error("Transaction should be removed after automatic rollback")
	}
}

// TestTransactionListener 测试事务监听器
type TestTransactionListener struct {
	checkResult TransactionState
}

func (l *TestTransactionListener) ExecuteLocalTransaction(msg *common.Message, arg interface{}) TransactionState {
	return TransactionStateUnknown
}

func (l *TestTransactionListener) CheckLocalTransaction(msgExt *common.MessageExt) TransactionState {
	if l.checkResult != 0 {
		return l.checkResult
	}
	return TransactionStateCommit
}