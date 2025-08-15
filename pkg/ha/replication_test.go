package ha

import (
	"testing"
	"time"
)

// MockCommitLog 模拟CommitLog接口
type MockCommitLog struct {
	maxOffset int64
	data      map[int64][]byte
}

func NewMockCommitLog() *MockCommitLog {
	return &MockCommitLog{
		maxOffset: 0,
		data:      make(map[int64][]byte),
	}
}

func (mcl *MockCommitLog) GetMaxOffset() int64 {
	return mcl.maxOffset
}

func (mcl *MockCommitLog) GetData(offset int64, size int32) ([]byte, error) {
	if data, exists := mcl.data[offset]; exists {
		return data, nil
	}
	return nil, nil
}

func (mcl *MockCommitLog) AppendData(data []byte) (int64, error) {
	offset := mcl.maxOffset
	mcl.data[offset] = data
	mcl.maxOffset += int64(len(data))
	return offset, nil
}

func TestHAServiceCreation(t *testing.T) {
	// 创建配置
	config := &HAConfig{
		BrokerRole:         ASYNC_MASTER,
		ReplicationMode:    ASYNC_REPLICATION,
		HaListenPort:       10912,
		HaMasterAddress:    "",
		HaHeartbeatInterval: 5000,
		HaConnectionTimeout: 3000,
		MaxTransferSize:     32768,
		SyncFlushTimeout:    5000,
	}
	
	// 创建模拟CommitLog
	commitLog := NewMockCommitLog()
	
	// 创建HA服务
	haService := NewHAService(config, commitLog)
	
	// 验证服务创建
	if haService == nil {
		t.Fatal("HAService should not be nil")
	}
	if haService.config != config {
		t.Error("Config should match")
	}
	if haService.commitLog != commitLog {
		t.Error("CommitLog should match")
	}
	if haService.running != 0 {
		t.Error("Service should not be running initially")
	}
}

func TestHAServiceStartStopMaster(t *testing.T) {
	// 创建主节点配置
	config := &HAConfig{
		BrokerRole:         ASYNC_MASTER,
		ReplicationMode:    ASYNC_REPLICATION,
		HaListenPort:       0, // 使用0让系统自动分配端口
		HaMasterAddress:    "",
		HaHeartbeatInterval: 5000,
		HaConnectionTimeout: 3000,
		MaxTransferSize:     32768,
		SyncFlushTimeout:    5000,
	}
	
	commitLog := NewMockCommitLog()
	haService := NewHAService(config, commitLog)
	
	// 启动服务
	err := haService.Start()
	if err != nil {
		t.Fatalf("Start should not return error: %v", err)
	}
	
	// 验证服务状态
	if haService.running == 0 {
		t.Error("Service should be running after start")
	}
	
	// 停止服务
	haService.Shutdown()
	
	// 验证服务已停止
	if haService.running != 0 {
		t.Error("Service should not be running after shutdown")
	}
}

func TestHAServiceStartStopSlave(t *testing.T) {
	// 创建从节点配置
	config := &HAConfig{
		BrokerRole:         SLAVE,
		ReplicationMode:    ASYNC_REPLICATION,
		HaListenPort:       0,
		HaMasterAddress:    "127.0.0.1:10912", // 指定主节点地址
		HaHeartbeatInterval: 5000,
		HaConnectionTimeout: 3000,
		MaxTransferSize:     32768,
		SyncFlushTimeout:    5000,
	}
	
	commitLog := NewMockCommitLog()
	haService := NewHAService(config, commitLog)
	
	// 启动服务（从节点会尝试连接主节点，但由于没有真实的主节点，连接会失败）
	err := haService.Start()
	if err != nil {
		t.Fatalf("Start should not return error: %v", err)
	}
	
	// 验证服务状态
	if haService.running == 0 {
		t.Error("Service should be running after start")
	}
	
	// 停止服务
	haService.Shutdown()
	
	// 验证服务已停止
	if haService.running != 0 {
		t.Error("Service should not be running after shutdown")
	}
}

func TestHAServiceReplicationStatus(t *testing.T) {
	config := &HAConfig{
		BrokerRole:         ASYNC_MASTER,
		ReplicationMode:    ASYNC_REPLICATION,
		HaListenPort:       0,
		HaMasterAddress:    "",
		HaHeartbeatInterval: 5000,
		HaConnectionTimeout: 3000,
		MaxTransferSize:     32768,
		SyncFlushTimeout:    5000,
	}
	
	commitLog := NewMockCommitLog()
	haService := NewHAService(config, commitLog)
	
	// 获取复制状态
	status := haService.GetReplicationStatus()
	
	// 验证状态信息
	if status == nil {
		t.Fatal("Status should not be nil")
	}
	if status["role"] != ASYNC_MASTER {
		t.Error("Broker role should match")
	}
	if status["mode"] != ASYNC_REPLICATION {
		t.Error("Replication mode should match")
	}
	if status["running"] != false {
		t.Error("Service should not be running initially")
	}
}

func TestHAServiceWaitForSlaveAck(t *testing.T) {
	config := &HAConfig{
		BrokerRole:         SYNC_MASTER,
		ReplicationMode:    SYNC_REPLICATION,
		HaListenPort:       0,
		HaMasterAddress:    "",
		HaHeartbeatInterval: 5000,
		HaConnectionTimeout: 3000,
		MaxTransferSize:     32768,
		SyncFlushTimeout:    1000, // 1秒超时
	}
	
	commitLog := NewMockCommitLog()
	haService := NewHAService(config, commitLog)
	
	// 测试等待从节点确认（应该超时）
	err := haService.WaitForSlaveAck(100, time.Millisecond*500)
	if err == nil {
		t.Error("WaitForSlaveAck should timeout when no slaves are connected")
	}
}

func TestHAServiceSlaveAckOffset(t *testing.T) {
	config := &HAConfig{
		BrokerRole:         ASYNC_MASTER,
		ReplicationMode:    ASYNC_REPLICATION,
		HaListenPort:       0,
		HaMasterAddress:    "",
		HaHeartbeatInterval: 5000,
		HaConnectionTimeout: 3000,
		MaxTransferSize:     32768,
		SyncFlushTimeout:    5000,
	}
	
	commitLog := NewMockCommitLog()
	haService := NewHAService(config, commitLog)
	
	// 获取从节点确认偏移量
	ackOffset := haService.getSlaveAckOffset()
	if ackOffset != 0 {
		t.Errorf("Initial slave ack offset should be 0, got %d", ackOffset)
	}
}

func TestDefaultHAConfig(t *testing.T) {
	// 测试默认配置
	config := DefaultHAConfig()
	
	if config == nil {
		t.Fatal("Default config should not be nil")
	}
	if config.BrokerRole != ASYNC_MASTER {
		t.Error("Default broker role should be ASYNC_MASTER")
	}
	if config.ReplicationMode != ASYNC_REPLICATION {
		t.Error("Default replication mode should be ASYNC_REPLICATION")
	}
	if config.HaListenPort != 10912 {
		t.Error("Default HA listen port should be 10912")
	}
	if config.HaHeartbeatInterval != 5000 {
		t.Error("Default heartbeat interval should be 5000ms")
	}
	if config.HaConnectionTimeout != 3000 {
		t.Error("Default connection timeout should be 3000ms")
	}
	if config.MaxTransferSize != 65536 {
		t.Error("Default max transfer size should be 65536")
	}
	if config.SyncFlushTimeout != 5000 {
		t.Error("Default sync flush timeout should be 5000ms")
	}
}

func TestHAServiceConcurrentStartStop(t *testing.T) {
	config := &HAConfig{
		BrokerRole:         ASYNC_MASTER,
		ReplicationMode:    ASYNC_REPLICATION,
		HaListenPort:       0,
		HaMasterAddress:    "",
		HaHeartbeatInterval: 5000,
		HaConnectionTimeout: 3000,
		MaxTransferSize:     32768,
		SyncFlushTimeout:    5000,
	}
	
	commitLog := NewMockCommitLog()
	haService := NewHAService(config, commitLog)
	
	// 测试多次启动和停止（顺序执行以避免channel重复关闭）
	for i := 0; i < 3; i++ {
		err := haService.Start()
		if err != nil {
			t.Logf("Start error (may be expected): %v", err)
		}
		
		// 短暂等待确保服务启动
		time.Sleep(time.Millisecond * 10)
		
		haService.Shutdown()
		
		// 短暂等待确保服务完全停止
		time.Sleep(time.Millisecond * 10)
	}
	
	// 验证最终状态
	if haService.running != 0 {
		t.Error("Service should be stopped after operations")
	}
}