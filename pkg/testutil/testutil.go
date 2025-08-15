package testutil

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go-rocketmq/pkg/broker"
	"go-rocketmq/pkg/common"
	"go-rocketmq/pkg/nameserver"
	"go-rocketmq/pkg/performance"
	"go-rocketmq/pkg/store"
)

// TestEnvironment 测试环境
type TestEnvironment struct {
	NameServer     *nameserver.NameServer
	Broker         *broker.Broker
	TempDir        string
	NameServerPort int
	CleanupFns     []func()
	mu             sync.Mutex
}

// NewTestEnvironment 创建测试环境
func NewTestEnvironment(t *testing.T) *TestEnvironment {
	tempDir := CreateTempDir(t, "rocketmq_test")
	
	env := &TestEnvironment{
		TempDir:    tempDir,
		CleanupFns: make([]func(), 0),
	}
	
	// 注册清理函数
	t.Cleanup(func() {
		env.Cleanup()
	})
	
	return env
}

// StartNameServer 启动NameServer
func (env *TestEnvironment) StartNameServer(t *testing.T) {
	env.NameServerPort = GetFreePort()
	config := &nameserver.Config{
		ListenPort:                  env.NameServerPort,
		ClusterTestEnable:           false,
		OrderMessageEnable:          false,
		ScanNotActiveBrokerInterval: 5 * time.Second,
	}
	
	env.NameServer = nameserver.NewNameServer(config)
	if err := env.NameServer.Start(); err != nil {
		t.Fatalf("Failed to start nameserver: %v", err)
	}
	
	env.AddCleanup(func() {
		env.NameServer.Stop()
	})
	
	// 等待启动
	time.Sleep(100 * time.Millisecond)
}

// StartBroker 启动Broker
func (env *TestEnvironment) StartBroker(t *testing.T) {
	if env.NameServer == nil {
		t.Fatal("NameServer must be started before Broker")
	}
	
	config := &broker.Config{
		BrokerName:       "TestBroker",
		ListenPort:       GetFreePort(),
		NameServerAddr:   fmt.Sprintf("127.0.0.1:%d", env.NameServerPort),
		StorePathRootDir: filepath.Join(env.TempDir, "broker"),
	}
	
	env.Broker = broker.NewBroker(config)
	if err := env.Broker.Start(); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	
	env.AddCleanup(func() {
		env.Broker.Stop()
	})
	
	// 等待启动
	time.Sleep(200 * time.Millisecond)
}

// AddCleanup 添加清理函数
func (env *TestEnvironment) AddCleanup(fn func()) {
	env.mu.Lock()
	defer env.mu.Unlock()
	env.CleanupFns = append(env.CleanupFns, fn)
}

// Cleanup 清理资源
func (env *TestEnvironment) Cleanup() {
	env.mu.Lock()
	defer env.mu.Unlock()
	
	// 逆序执行清理函数
	for i := len(env.CleanupFns) - 1; i >= 0; i-- {
		env.CleanupFns[i]()
	}
	
	// 清理临时目录
	if env.TempDir != "" {
		os.RemoveAll(env.TempDir)
	}
}

// CreateTempDir 创建临时目录
func CreateTempDir(t *testing.T, prefix string) string {
	tempDir, err := os.MkdirTemp("", prefix)
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return tempDir
}

// GetFreePort 获取空闲端口
func GetFreePort() int {
	// 简单的端口分配策略，实际应用中可能需要更复杂的逻辑
	return 10000 + rand.Intn(50000)
}

// CreateTestMessage 创建测试消息
func CreateTestMessage(topic, body string) *common.Message {
	return &common.Message{
		Topic: topic,
		Body:  []byte(body),
	}
}

// CreateTestMessages 创建多个测试消息
func CreateTestMessages(topic string, count int) []*common.Message {
	messages := make([]*common.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = CreateTestMessage(topic, fmt.Sprintf("Test message %d", i))
	}
	return messages
}

// WaitForCondition 等待条件满足
func WaitForCondition(t *testing.T, condition func() bool, timeout time.Duration, message string) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	
	timeoutCh := time.After(timeout)
	
	for {
		select {
		case <-ticker.C:
			if condition() {
				return
			}
		case <-timeoutCh:
			t.Fatalf("Timeout waiting for condition: %s", message)
		}
	}
}

// AssertEventually 断言条件最终满足
func AssertEventually(t *testing.T, condition func() bool, timeout time.Duration, message string) {
	WaitForCondition(t, condition, timeout, message)
}

// AssertNoError 断言没有错误
func AssertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

// AssertError 断言有错误
func AssertError(t *testing.T, err error, message string) {
	if err == nil {
		t.Fatalf("%s: expected error but got nil", message)
	}
}

// AssertEqual 断言相等
func AssertEqual(t *testing.T, expected, actual interface{}, message string) {
	if expected != actual {
		t.Fatalf("%s: expected %v, got %v", message, expected, actual)
	}
}

// AssertNotEqual 断言不相等
func AssertNotEqual(t *testing.T, expected, actual interface{}, message string) {
	if expected == actual {
		t.Fatalf("%s: expected %v to not equal %v", message, expected, actual)
	}
}

// AssertTrue 断言为真
func AssertTrue(t *testing.T, condition bool, message string) {
	if !condition {
		t.Fatalf("%s: expected true but got false", message)
	}
}

// AssertFalse 断言为假
func AssertFalse(t *testing.T, condition bool, message string) {
	if condition {
		t.Fatalf("%s: expected false but got true", message)
	}
}

// PerformanceTestHelper 性能测试辅助工具
type PerformanceTestHelper struct {
	Monitor        *performance.PerformanceMonitor
	BatchProcessor *performance.BatchProcessor
	StartTime      time.Time
}

// NewPerformanceTestHelper 创建性能测试辅助工具
func NewPerformanceTestHelper(t *testing.T) *PerformanceTestHelper {
	monitorConfig := performance.MonitorConfig{
		CollectInterval: 1 * time.Second,
		HTTPPort:        GetFreePort(),
		EnableHTTP:      false, // 测试时不启用HTTP服务
		MetricsPath:     "/metrics",
	}
	monitor := performance.NewPerformanceMonitor(monitorConfig)
	monitor.Start()
	
	config := performance.BatchConfig{
		BatchSize:     10,
		BufferSize:    100,
		FlushInterval: 100 * time.Millisecond,
	}
	
	batchProcessor := performance.NewBatchProcessor(config, performance.BatchHandlerFunc(func(items []interface{}) error {
		// 默认处理函数
		return nil
	}))
	batchProcessor.Start()
	
	helper := &PerformanceTestHelper{
		Monitor:        monitor,
		BatchProcessor: batchProcessor,
		StartTime:      time.Now(),
	}
	
	t.Cleanup(func() {
		helper.Cleanup()
	})
	
	return helper
}

// Cleanup 清理性能测试资源
func (h *PerformanceTestHelper) Cleanup() {
	if h.BatchProcessor != nil {
		h.BatchProcessor.Stop()
	}
	if h.Monitor != nil {
		h.Monitor.Stop()
	}
}

// GetElapsedTime 获取经过的时间
func (h *PerformanceTestHelper) GetElapsedTime() time.Duration {
	return time.Since(h.StartTime)
}

// StoreTestHelper 存储测试辅助工具
type StoreTestHelper struct {
	Store   *store.DefaultMessageStore
	TempDir string
}

// NewStoreTestHelper 创建存储测试辅助工具
func NewStoreTestHelper(t *testing.T) *StoreTestHelper {
	tempDir := CreateTempDir(t, "store_test")
	
	config := &store.StoreConfig{
		StorePathRootDir:       tempDir,
		StorePathCommitLog:     filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue:  filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:         filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog: 1024 * 1024, // 1MB
		FlushDiskType:          store.ASYNC_FLUSH,
	}
	
	msgStore, err := store.NewDefaultMessageStore(config)
	AssertNoError(t, err, "Failed to create message store")
	
	err = msgStore.Start()
	AssertNoError(t, err, "Failed to start message store")
	
	helper := &StoreTestHelper{
		Store:   msgStore,
		TempDir: tempDir,
	}
	
	t.Cleanup(func() {
		helper.Cleanup()
	})
	
	return helper
}

// Cleanup 清理存储测试资源
func (h *StoreTestHelper) Cleanup() {
	if h.Store != nil {
		h.Store.Shutdown()
	}
	if h.TempDir != "" {
		os.RemoveAll(h.TempDir)
	}
}

// PutTestMessage 存储测试消息
func (h *StoreTestHelper) PutTestMessage(t *testing.T, topic, body string) {
	msg := CreateTestMessage(topic, body)
	result, err := h.Store.PutMessage(msg)
	AssertNoError(t, err, "Failed to put message")
	AssertNotEqual(t, "", result.MsgId, "Message ID should not be empty")
}

// MockHandler 模拟处理器
type MockHandler struct {
	ProcessFunc func(items []interface{}) error
	CallCount   int
	mu          sync.Mutex
}

// NewMockHandler 创建模拟处理器
func NewMockHandler(processFunc func(items []interface{}) error) *MockHandler {
	return &MockHandler{
		ProcessFunc: processFunc,
	}
}

// Process 处理项目
func (m *MockHandler) Process(items []interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CallCount++
	if m.ProcessFunc != nil {
		return m.ProcessFunc(items)
	}
	return nil
}

// GetCallCount 获取调用次数
func (m *MockHandler) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.CallCount
}

// ResetCallCount 重置调用次数
func (m *MockHandler) ResetCallCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CallCount = 0
}