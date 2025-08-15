package ha

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// ExtendedMockCommitLog 扩展的模拟CommitLog，支持更多功能
type ExtendedMockCommitLog struct {
	mutex     sync.RWMutex
	maxOffset int64
	data      map[int64][]byte
	errorMode bool // 用于模拟错误情况
}

func NewExtendedMockCommitLog() *ExtendedMockCommitLog {
	return &ExtendedMockCommitLog{
		maxOffset: 0,
		data:      make(map[int64][]byte),
		errorMode: false,
	}
}

func (emcl *ExtendedMockCommitLog) GetMaxOffset() int64 {
	emcl.mutex.RLock()
	defer emcl.mutex.RUnlock()
	return emcl.maxOffset
}

func (emcl *ExtendedMockCommitLog) GetData(offset int64, size int32) ([]byte, error) {
	emcl.mutex.RLock()
	defer emcl.mutex.RUnlock()
	
	if emcl.errorMode {
		return nil, fmt.Errorf("mock error: failed to get data")
	}
	
	if data, exists := emcl.data[offset]; exists {
		if int32(len(data)) <= size {
			return data, nil
		}
		return data[:size], nil
	}
	return nil, nil
}

func (emcl *ExtendedMockCommitLog) AppendData(data []byte) (int64, error) {
	emcl.mutex.Lock()
	defer emcl.mutex.Unlock()
	
	if emcl.errorMode {
		return -1, fmt.Errorf("mock error: failed to append data")
	}
	
	offset := emcl.maxOffset
	emcl.data[offset] = make([]byte, len(data))
	copy(emcl.data[offset], data)
	emcl.maxOffset += int64(len(data))
	return offset, nil
}

func (emcl *ExtendedMockCommitLog) SetErrorMode(enabled bool) {
	emcl.mutex.Lock()
	defer emcl.mutex.Unlock()
	emcl.errorMode = enabled
}

// TestHAServiceRoleConstants 测试角色常量
func TestHAServiceRoleConstants(t *testing.T) {
	if ASYNC_MASTER != 0 {
		t.Errorf("ASYNC_MASTER should be 0, got %d", ASYNC_MASTER)
	}
	if SYNC_MASTER != 1 {
		t.Errorf("SYNC_MASTER should be 1, got %d", SYNC_MASTER)
	}
	if SLAVE != 2 {
		t.Errorf("SLAVE should be 2, got %d", SLAVE)
	}
}

// TestHAServiceReplicationModeConstants 测试复制模式常量
func TestHAServiceReplicationModeConstants(t *testing.T) {
	if ASYNC_REPLICATION != 0 {
		t.Errorf("ASYNC_REPLICATION should be 0, got %d", ASYNC_REPLICATION)
	}
	if SYNC_REPLICATION != 1 {
		t.Errorf("SYNC_REPLICATION should be 1, got %d", SYNC_REPLICATION)
	}
}

// TestHAServiceConfigValidation 测试配置验证
func TestHAServiceConfigValidation(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	
	// 测试nil配置
	haService := NewHAService(nil, commitLog)
	if haService == nil {
		t.Fatal("HAService should not be nil even with nil config")
	}
	if haService.config == nil {
		t.Error("Config should be set to default when nil is provided")
	}
	
	// 测试有效配置
	validConfig := &HAConfig{
		BrokerRole:          SYNC_MASTER,
		ReplicationMode:     SYNC_REPLICATION,
		HaListenPort:        10913,
		HaMasterAddress:     "127.0.0.1:10912",
		HaHeartbeatInterval: 3000,
		HaConnectionTimeout: 5000,
		MaxTransferSize:     32768,
		SyncFlushTimeout:    3000,
	}
	
	haService2 := NewHAService(validConfig, commitLog)
	if haService2 == nil {
		t.Fatal("HAService should not be nil with valid config")
	}
	if haService2.config.BrokerRole != SYNC_MASTER {
		t.Error("Config should match provided values")
	}
}

// TestHAServiceDataReplication 测试数据复制功能
func TestHAServiceDataReplication(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	
	config := &HAConfig{
		BrokerRole:          ASYNC_MASTER,
		ReplicationMode:     ASYNC_REPLICATION,
		HaListenPort:        0, // 自动分配端口
		HaHeartbeatInterval: 1000,
		HaConnectionTimeout: 2000,
		MaxTransferSize:     1024,
		SyncFlushTimeout:    2000,
	}
	
	haService := NewHAService(config, commitLog)
	
	// 启动服务
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start HA service: %v", err)
	}
	defer haService.Shutdown()
	
	// 模拟数据写入
	testData := []byte("test message for replication")
	offset, err := commitLog.AppendData(testData)
	if err != nil {
		t.Fatalf("Failed to append data: %v", err)
	}
	
	if offset < 0 {
		t.Error("Offset should be non-negative")
	}
	
	// 验证数据可以读取
	retrievedData, err := commitLog.GetData(offset, int32(len(testData)))
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}
	
	if !bytes.Equal(testData, retrievedData) {
		t.Error("Retrieved data should match original data")
	}
}

// TestHAServiceSlaveConnection 测试从节点连接管理
func TestHAServiceSlaveConnection(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	
	config := &HAConfig{
		BrokerRole:          SLAVE,
		ReplicationMode:     ASYNC_REPLICATION,
		HaListenPort:        0,
		HaMasterAddress:     "127.0.0.1:99999", // 不存在的地址
		HaHeartbeatInterval: 1000,
		HaConnectionTimeout: 1000, // 短超时时间
		MaxTransferSize:     1024,
		SyncFlushTimeout:    1000,
	}
	
	haService := NewHAService(config, commitLog)
	
	// 启动从节点服务（会尝试连接主节点但失败）
	err := haService.Start()
	if err != nil {
		t.Fatalf("Start should not return error even if master connection fails: %v", err)
	}
	
	// 验证服务状态
	if haService.running == 0 {
		t.Error("Service should be running even if master connection fails")
	}
	
	// 短暂等待连接尝试
	time.Sleep(100 * time.Millisecond)
	
	// 关闭服务
	haService.Shutdown()
	
	if haService.running != 0 {
		t.Error("Service should be stopped after shutdown")
	}
}

// TestHAServiceSyncReplication 测试同步复制
func TestHAServiceSyncReplication(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	
	config := &HAConfig{
		BrokerRole:          SYNC_MASTER,
		ReplicationMode:     SYNC_REPLICATION,
		HaListenPort:        0,
		HaHeartbeatInterval: 1000,
		HaConnectionTimeout: 2000,
		MaxTransferSize:     1024,
		SyncFlushTimeout:    500, // 短超时时间用于测试
	}
	
	haService := NewHAService(config, commitLog)
	
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start HA service: %v", err)
	}
	defer haService.Shutdown()
	
	// 测试同步等待（应该超时，因为没有从节点）
	testOffset := int64(100)
	err = haService.WaitForSlaveAck(testOffset, 200*time.Millisecond)
	if err == nil {
		t.Error("WaitForSlaveAck should timeout when no slaves are connected")
	}
}

// TestHAServiceErrorHandling 测试错误处理
func TestHAServiceErrorHandling(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	
	// 启用错误模式
	commitLog.SetErrorMode(true)
	
	config := &HAConfig{
		BrokerRole:          ASYNC_MASTER,
		ReplicationMode:     ASYNC_REPLICATION,
		HaListenPort:        0,
		HaHeartbeatInterval: 1000,
		HaConnectionTimeout: 2000,
		MaxTransferSize:     1024,
		SyncFlushTimeout:    1000,
	}
	
	haService := NewHAService(config, commitLog)
	
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start HA service: %v", err)
	}
	defer haService.Shutdown()
	
	// 测试在错误模式下的数据操作
	_, err = commitLog.AppendData([]byte("test data"))
	if err == nil {
		t.Error("AppendData should fail in error mode")
	}
	
	_, err = commitLog.GetData(0, 100)
	if err == nil {
		t.Error("GetData should fail in error mode")
	}
	
	// 禁用错误模式
	commitLog.SetErrorMode(false)
	
	// 现在操作应该成功
	_, err = commitLog.AppendData([]byte("test data"))
	if err != nil {
		t.Errorf("AppendData should succeed after disabling error mode: %v", err)
	}
}

// TestHAServiceMultipleStartStop 测试多次启动停止
func TestHAServiceMultipleStartStop(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	
	config := &HAConfig{
		BrokerRole:          ASYNC_MASTER,
		ReplicationMode:     ASYNC_REPLICATION,
		HaListenPort:        0,
		HaHeartbeatInterval: 1000,
		HaConnectionTimeout: 2000,
		MaxTransferSize:     1024,
		SyncFlushTimeout:    1000,
	}
	
	haService := NewHAService(config, commitLog)
	
	// 测试重复启动
	err := haService.Start()
	if err != nil {
		t.Fatalf("First start should succeed: %v", err)
	}
	
	err = haService.Start()
	if err == nil {
		t.Error("Second start should fail")
	}
	
	// 停止服务
	haService.Shutdown()
	
	// 测试重复停止
	haService.Shutdown() // 应该安全地处理重复停止
	
	// 再次启动应该成功
	err = haService.Start()
	if err != nil {
		t.Fatalf("Restart should succeed: %v", err)
	}
	
	haService.Shutdown()
}

// TestHAServiceReplicationStatusExtended 测试扩展的复制状态
func TestHAServiceReplicationStatusExtended(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	
	config := &HAConfig{
		BrokerRole:          SYNC_MASTER,
		ReplicationMode:     SYNC_REPLICATION,
		HaListenPort:        0,
		HaHeartbeatInterval: 2000,
		HaConnectionTimeout: 3000,
		MaxTransferSize:     2048,
		SyncFlushTimeout:    2000,
	}
	
	haService := NewHAService(config, commitLog)
	
	// 启动前获取状态
	status := haService.GetReplicationStatus()
	if status["running"] != false {
		t.Error("Service should not be running initially")
	}
	
	// 启动服务
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start HA service: %v", err)
	}
	defer haService.Shutdown()
	
	// 启动后获取状态
	status = haService.GetReplicationStatus()
	if status["running"] != true {
		t.Error("Service should be running after start")
	}
	if status["role"] != SYNC_MASTER {
		t.Error("Role should match configuration")
	}
	if status["mode"] != SYNC_REPLICATION {
		t.Error("Mode should match configuration")
	}
	
	// 验证其他状态字段
	if _, exists := status["slave_count"]; !exists {
		t.Error("Status should include slave_count")
	}
	if _, exists := status["slaves"]; !exists {
		t.Error("Status should include slaves")
	}
}

// TestHAServicePortAllocation 测试端口分配
func TestHAServicePortAllocation(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	
	// 测试自动端口分配
	config := &HAConfig{
		BrokerRole:          ASYNC_MASTER,
		ReplicationMode:     ASYNC_REPLICATION,
		HaListenPort:        0, // 自动分配
		HaHeartbeatInterval: 1000,
		HaConnectionTimeout: 2000,
		MaxTransferSize:     1024,
		SyncFlushTimeout:    1000,
	}
	
	haService := NewHAService(config, commitLog)
	
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start HA service with auto port: %v", err)
	}
	defer haService.Shutdown()
	
	// 验证监听器已创建
	if haService.haListener == nil {
		t.Error("HA listener should be created")
	}
	
	// 获取实际分配的端口
	addr := haService.haListener.Addr()
	if addr == nil {
		t.Error("Listener address should not be nil")
	}
	
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		t.Error("Address should be TCP address")
	}
	
	if tcpAddr.Port <= 0 {
		t.Error("Allocated port should be positive")
	}
}

// TestHAServiceBenchmark 基准测试
func BenchmarkHAServiceDataAppend(b *testing.B) {
	commitLog := NewExtendedMockCommitLog()
	testData := []byte("benchmark test message for HA service")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := commitLog.AppendData(testData)
		if err != nil {
			b.Fatalf("Failed to append data: %v", err)
		}
	}
}

func BenchmarkHAServiceDataRead(b *testing.B) {
	commitLog := NewExtendedMockCommitLog()
	testData := []byte("benchmark test message for HA service")
	
	// 预先写入一些数据
	for i := 0; i < 100; i++ {
		commitLog.AppendData(testData)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i % 100 * len(testData))
		_, err := commitLog.GetData(offset, int32(len(testData)))
		if err != nil {
			b.Fatalf("Failed to get data: %v", err)
		}
	}
}

func BenchmarkHAServiceStatusCheck(b *testing.B) {
	commitLog := NewExtendedMockCommitLog()
	config := DefaultHAConfig()
	haService := NewHAService(config, commitLog)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status := haService.GetReplicationStatus()
		if status == nil {
			b.Fatal("Status should not be nil")
		}
	}
}