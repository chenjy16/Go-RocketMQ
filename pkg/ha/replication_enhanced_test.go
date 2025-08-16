package ha

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MockNetConn 模拟网络连接，支持错误注入
type MockNetConn struct {
	readBuffer  *bytes.Buffer
	writeBuffer *bytes.Buffer
	closed      bool
	readError   error
	writeError  error
	closeError  error
	readDelay   time.Duration
	writeDelay  time.Duration
	mutex       sync.Mutex
}

func NewMockNetConn() *MockNetConn {
	return &MockNetConn{
		readBuffer:  new(bytes.Buffer),
		writeBuffer: new(bytes.Buffer),
	}
}

func (m *MockNetConn) Read(b []byte) (n int, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	if m.closed {
		return 0, io.EOF
	}
	
	if m.readDelay > 0 {
		time.Sleep(m.readDelay)
	}
	
	if m.readError != nil {
		return 0, m.readError
	}
	
	return m.readBuffer.Read(b)
}

func (m *MockNetConn) Write(b []byte) (n int, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	
	if m.writeDelay > 0 {
		time.Sleep(m.writeDelay)
	}
	
	if m.writeError != nil {
		return 0, m.writeError
	}
	
	return m.writeBuffer.Write(b)
}

func (m *MockNetConn) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.closed = true
	return m.closeError
}

func (m *MockNetConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080}
}

func (m *MockNetConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8081}
}

func (m *MockNetConn) SetDeadline(t time.Time) error { return nil }
func (m *MockNetConn) SetReadDeadline(t time.Time) error { return nil }
func (m *MockNetConn) SetWriteDeadline(t time.Time) error { return nil }

func (m *MockNetConn) SetReadError(err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.readError = err
}

func (m *MockNetConn) SetWriteError(err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.writeError = err
}

func (m *MockNetConn) WriteData(data []byte) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.readBuffer.Write(data)
}

func (m *MockNetConn) GetWrittenData() []byte {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.writeBuffer.Bytes()
}

// TestHAServiceNetworkIOErrors 测试网络I/O错误处理
func TestHAServiceNetworkIOErrors(t *testing.T) {
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
	
	// 测试从节点请求读取时的网络错误
	mockConn := NewMockNetConn()
	mockConn.SetReadError(errors.New("network read error"))
	
	_, err := haService.readSlaveRequest(mockConn)
	if err == nil {
		t.Error("Expected error when reading slave request with network error")
	}
	
	// 测试数据包发送时的网络错误
	mockConn2 := NewMockNetConn()
	mockConn2.SetWriteError(errors.New("network write error"))
	
	err = haService.sendDataPacket(mockConn2, 100, []byte("test data"))
	if err == nil {
		t.Error("Expected error when sending data packet with network error")
	}
	
	// 测试从节点请求发送时的网络错误
	mockConn3 := NewMockNetConn()
	mockConn3.SetWriteError(errors.New("slave request write error"))
	
	err = haService.sendSlaveRequest(mockConn3, 100)
	if err == nil {
		t.Error("Expected error when sending slave request with network error")
	}
	
	// 测试连接关闭时的读取错误
	mockConn4 := NewMockNetConn()
	mockConn4.Close()
	
	_, err = haService.readDataPacket(mockConn4)
	if err == nil {
		t.Error("Expected error when reading from closed connection")
	}
	
	// 测试网络延迟情况
	mockConn5 := NewMockNetConn()
	mockConn5.readDelay = 100 * time.Millisecond
	mockConn5.WriteData([]byte{0x00, 0x00}) // 不完整数据
	
	start := time.Now()
	_, err = haService.readSlaveRequest(mockConn5)
	elapsed := time.Since(start)
	
	if elapsed < 100*time.Millisecond {
		t.Error("Expected delay to be applied")
	}
	if err == nil {
		t.Error("Expected error due to incomplete data")
	}
}

// TestHAServiceDataPacketProtocolErrors 测试数据包协议错误
func TestHAServiceDataPacketProtocolErrors(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	config := DefaultHAConfig()
	haService := NewHAService(config, commitLog)
	
	// 测试不完整的长度字段
	mockConn := NewMockNetConn()
	mockConn.WriteData([]byte{0x00, 0x00}) // 只有2字节，期望4字节
	
	_, err := haService.readSlaveRequest(mockConn)
	if err == nil {
		t.Error("Expected error when reading incomplete length field")
	}
	
	// 测试不完整的偏移量字段
	mockConn2 := NewMockNetConn()
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, 8)
	mockConn2.WriteData(lengthBytes)
	mockConn2.WriteData([]byte{0x00, 0x00, 0x00, 0x00}) // 只有4字节，期望8字节
	
	_, err = haService.readSlaveRequest(mockConn2)
	if err == nil {
		t.Error("Expected error when reading incomplete offset field")
	}
	
	// 测试数据包读取时的不完整长度字段
	mockConn3 := NewMockNetConn()
	mockConn3.WriteData([]byte{0x00, 0x00}) // 不完整的长度字段
	
	_, err = haService.readDataPacket(mockConn3)
	if err == nil {
		t.Error("Expected error when reading data packet with incomplete length")
	}
	
	// 测试数据包读取时的不完整数据
	mockConn4 := NewMockNetConn()
	packet := make([]byte, 12)
	binary.BigEndian.PutUint32(packet[0:4], 16) // 声明长度为16
	binary.BigEndian.PutUint64(packet[4:12], 100) // 偏移量
	mockConn4.WriteData(packet)
	mockConn4.WriteData([]byte{0x01, 0x02}) // 只提供2字节数据，期望8字节
	
	_, err = haService.readDataPacket(mockConn4)
	if err == nil {
		t.Error("Expected error when reading data packet with incomplete data")
	}
	
	// 测试零长度数据包
	mockConn5 := NewMockNetConn()
	zeroPacket := make([]byte, 12)
	binary.BigEndian.PutUint32(zeroPacket[0:4], 8) // 长度为8（只有offset）
	binary.BigEndian.PutUint64(zeroPacket[4:12], 200) // 偏移量
	mockConn5.WriteData(zeroPacket)
	
	packetResult, err := haService.readDataPacket(mockConn5)
	if err != nil {
		t.Errorf("Should handle zero-length data packet: %v", err)
	}
	if len(packetResult.Data) != 0 {
		t.Error("Expected zero-length data")
	}
	if packetResult.Offset != 200 {
		t.Error("Expected offset to be 200")
	}
	
	// 测试超大数据包长度
	mockConn6 := NewMockNetConn()
	largePacket := make([]byte, 12)
	binary.BigEndian.PutUint32(largePacket[0:4], 0xFFFFFFFF) // 最大uint32值
	binary.BigEndian.PutUint64(largePacket[4:12], 300)
	mockConn6.WriteData(largePacket)
	
	_, err = haService.readDataPacket(mockConn6)
	if err == nil {
		t.Error("Expected error when reading oversized data packet")
	}
}

// TestHAServiceMasterSlaveConnectionManagement 测试主从连接管理
func TestHAServiceMasterSlaveConnectionManagement(t *testing.T) {
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
	
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start HA service: %v", err)
	}
	defer haService.Shutdown()
	
	// 测试多个从节点连接
	slaveConn1 := &SlaveConnection{
		conn:               NewMockNetConn(),
		slaveAddr:          "slave1:8080",
		running:            true,
		slaveRequestOffset: 0,
	}
	
	slaveConn2 := &SlaveConnection{
		conn:               NewMockNetConn(),
		slaveAddr:          "slave2:8080",
		running:            true,
		slaveRequestOffset: 100,
	}
	
	// 添加从节点连接
	haService.mutex.Lock()
	haService.slaveConnections[slaveConn1.slaveAddr] = slaveConn1
	haService.slaveConnections[slaveConn2.slaveAddr] = slaveConn2
	haService.mutex.Unlock()
	
	// 验证连接状态
	status := haService.GetReplicationStatus()
	slaveCount := status["slave_count"].(int)
	if slaveCount != 2 {
		t.Errorf("Expected 2 slaves, got %d", slaveCount)
	}
	
	// 测试从节点断开连接
	slaveConn1.running = false
	slaveConn1.conn.Close()
	
	// 从连接映射中移除
	haService.mutex.Lock()
	delete(haService.slaveConnections, slaveConn1.slaveAddr)
	haService.mutex.Unlock()
	
	// 验证连接数量更新
	status = haService.GetReplicationStatus()
	slaveCount = status["slave_count"].(int)
	if slaveCount != 1 {
		t.Errorf("Expected 1 slave after disconnection, got %d", slaveCount)
	}
	
	// 测试从节点重连
	slaveConn3 := &SlaveConnection{
		conn:               NewMockNetConn(),
		slaveAddr:          "slave1:8080", // 重用相同地址
		running:            true,
		slaveRequestOffset: 200,
	}
	
	haService.mutex.Lock()
	haService.slaveConnections[slaveConn3.slaveAddr] = slaveConn3
	haService.mutex.Unlock()
	
	status = haService.GetReplicationStatus()
	slaveCount = status["slave_count"].(int)
	if slaveCount != 2 {
		t.Errorf("Expected 2 slaves after reconnection, got %d", slaveCount)
	}
}

// TestHAServiceSlaveReconnection 测试从节点重连机制
func TestHAServiceSlaveReconnection(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	config := &HAConfig{
		BrokerRole:          SLAVE,
		ReplicationMode:     ASYNC_REPLICATION,
		HaListenPort:        0,
		HaMasterAddress:     "127.0.0.1:99999", // 不存在的地址
		HaHeartbeatInterval: 500,
		HaConnectionTimeout: 1000,
		MaxTransferSize:     1024,
		SyncFlushTimeout:    1000,
	}
	
	haService := NewHAService(config, commitLog)
	
	// 启动从节点（会持续尝试重连）
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start slave HA service: %v", err)
	}
	
	// 等待一段时间让重连尝试发生
	time.Sleep(200 * time.Millisecond)
	
	// 验证服务仍在运行
	if atomic.LoadInt32(&haService.running) == 0 {
		t.Error("Service should still be running despite connection failures")
	}
	
	// 关闭服务
	haService.Shutdown()
	
	if atomic.LoadInt32(&haService.running) != 0 {
		t.Error("Service should be stopped after shutdown")
	}
}

// TestHAServiceDataReplicationWithErrors 测试数据复制时的错误处理
func TestHAServiceDataReplicationWithErrors(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	config := DefaultHAConfig()
	haService := NewHAService(config, commitLog)
	
	// 添加一些测试数据
	testData := []byte("test replication data")
	commitLog.AppendData(testData)
	
	// 测试当CommitLog出现错误时的处理
	commitLog.SetErrorMode(true)
	
	slaveConn := &SlaveConnection{
		conn:               NewMockNetConn(),
		slaveAddr:          "test-slave",
		running:            true,
		slaveRequestOffset: 0,
	}
	
	// 尝试发送数据给从节点，应该失败
	err := haService.sendDataToSlave(slaveConn, 0)
	if err == nil {
		t.Error("Expected error when CommitLog is in error mode")
	}
	
	// 恢复正常模式
	commitLog.SetErrorMode(false)
	
	// 测试网络写入错误
	mockConn := slaveConn.conn.(*MockNetConn)
	mockConn.SetWriteError(errors.New("network write error"))
	
	err = haService.sendDataToSlave(slaveConn, 0)
	if err == nil {
		t.Error("Expected error when network write fails")
	}
	
	// 测试心跳发送错误
	err = haService.sendHeartbeatToSlave(slaveConn)
	if err == nil {
		t.Error("Expected error when sending heartbeat with network error")
	}
	
	// 恢复网络连接
	mockConn.SetWriteError(nil)
	
	// 现在应该成功
	err = haService.sendHeartbeatToSlave(slaveConn)
	if err != nil {
		t.Errorf("Heartbeat should succeed after fixing network: %v", err)
	}
}

// TestHAServiceSyncReplicationTimeout 测试同步复制超时
func TestHAServiceSyncReplicationTimeout(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	config := &HAConfig{
		BrokerRole:          SYNC_MASTER,
		ReplicationMode:     SYNC_REPLICATION,
		HaListenPort:        0,
		HaHeartbeatInterval: 1000,
		HaConnectionTimeout: 2000,
		MaxTransferSize:     1024,
		SyncFlushTimeout:    100, // 短超时时间
	}
	
	haService := NewHAService(config, commitLog)
	
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start HA service: %v", err)
	}
	defer haService.Shutdown()
	
	// 测试没有从节点时的同步等待超时
	testOffset := int64(100)
	err = haService.WaitForSlaveAck(testOffset, 50*time.Millisecond)
	if err == nil {
		t.Error("WaitForSlaveAck should timeout when no slaves are connected")
	}
	
	// 添加一个从节点但不更新ack offset
	slaveConn := &SlaveConnection{
		conn:           NewMockNetConn(),
		slaveAddr:      "test-slave",
		running:        true,
		slaveAckOffset: 0, // 低于期望的offset
	}
	
	haService.mutex.Lock()
	haService.slaveConnections[slaveConn.slaveAddr] = slaveConn
	haService.mutex.Unlock()
	
	// 测试等待超时
	err = haService.WaitForSlaveAck(testOffset, 50*time.Millisecond)
	if err == nil {
		t.Error("WaitForSlaveAck should timeout when slave ack is insufficient")
	}
	
	// 测试异步复制不需要等待
	config.ReplicationMode = ASYNC_REPLICATION
	haService2 := NewHAService(config, commitLog)
	err = haService2.WaitForSlaveAck(testOffset, 50*time.Millisecond)
	if err != nil {
		t.Error("WaitForSlaveAck should not wait for async replication")
	}
}

// TestHAServiceConcurrentOperations 测试并发操作
func TestHAServiceConcurrentOperations(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	config := DefaultHAConfig()
	haService := NewHAService(config, commitLog)
	
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start HA service: %v", err)
	}
	defer haService.Shutdown()
	
	// 并发添加和移除从节点连接
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			slaveAddr := fmt.Sprintf("slave%d:8080", id)
			slaveConn := &SlaveConnection{
				conn:      NewMockNetConn(),
				slaveAddr: slaveAddr,
				running:   true,
			}
			
			// 添加连接
			haService.mutex.Lock()
			haService.slaveConnections[slaveAddr] = slaveConn
			haService.mutex.Unlock()
			
			// 短暂等待
			time.Sleep(10 * time.Millisecond)
			
			// 移除连接
			haService.mutex.Lock()
			delete(haService.slaveConnections, slaveAddr)
			haService.mutex.Unlock()
		}(i)
	}
	
	wg.Wait()
	
	// 验证最终状态
	status := haService.GetReplicationStatus()
	slaveCount := status["slave_count"].(int)
	if slaveCount != 0 {
		t.Errorf("Expected 0 slaves after concurrent operations, got %d", slaveCount)
	}
}

// TestHAServiceEdgeCases 测试边界情况
func TestHAServiceEdgeCases(t *testing.T) {
	// 测试nil配置
	haService := NewHAService(nil, nil)
	if haService.config == nil {
		t.Error("Config should be set to default when nil")
	}
	
	// 测试无效的broker role
	invalidConfig := &HAConfig{
		BrokerRole: BrokerRole(999), // 无效角色
	}
	commitLog := NewExtendedMockCommitLog()
	haService2 := NewHAService(invalidConfig, commitLog)
	err := haService2.Start()
	if err == nil {
		t.Error("Start should fail with invalid broker role")
	}
	
	// 测试空的主节点地址
	slaveConfig := &HAConfig{
		BrokerRole:      SLAVE,
		HaMasterAddress: "",
	}
	haService3 := NewHAService(slaveConfig, commitLog)
	err = haService3.Start()
	if err == nil {
		t.Error("Slave start should fail with empty master address")
	}
	
	// 测试重复启动
	validConfig := DefaultHAConfig()
	haService4 := NewHAService(validConfig, commitLog)
	
	err = haService4.Start()
	if err != nil {
		t.Fatalf("First start should succeed: %v", err)
	}
	
	err = haService4.Start()
	if err == nil {
		t.Error("Second start should fail")
	}
	
	haService4.Shutdown()
	
	// 测试重复关闭
	haService4.Shutdown() // 应该安全处理
	
	// 测试关闭后再启动
	err = haService4.Start()
	if err != nil {
		t.Errorf("Restart after shutdown should succeed: %v", err)
	}
	
	haService4.Shutdown()
}

// TestHAServiceHeartbeat 测试心跳机制
func TestHAServiceHeartbeat(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	config := &HAConfig{
		BrokerRole:          ASYNC_MASTER,
		ReplicationMode:     ASYNC_REPLICATION,
		HaListenPort:        0,
		HaHeartbeatInterval: 100, // 短心跳间隔
		HaConnectionTimeout: 2000,
		MaxTransferSize:     1024,
		SyncFlushTimeout:    1000,
	}
	
	haService := NewHAService(config, commitLog)
	
	// 测试心跳发送
	mockConn := NewMockNetConn()
	err := haService.sendHeartbeatToSlave(&SlaveConnection{
		conn:      mockConn,
		slaveAddr: "test-slave",
		running:   true,
	})
	
	if err != nil {
		t.Errorf("Heartbeat should succeed: %v", err)
	}
	
	// 验证心跳数据包格式
	writtenData := mockConn.GetWrittenData()
	if len(writtenData) != 12 { // 4字节长度 + 8字节offset
		t.Errorf("Expected 12 bytes for heartbeat, got %d", len(writtenData))
	}
	
	// 验证心跳包内容
	length := binary.BigEndian.Uint32(writtenData[0:4])
	if length != 8 {
		t.Errorf("Expected heartbeat length 8, got %d", length)
	}
	
	offset := binary.BigEndian.Uint64(writtenData[4:12])
	if offset != 0 {
		t.Errorf("Expected heartbeat offset 0, got %d", offset)
	}
	
	// 测试心跳发送失败
	mockConn2 := NewMockNetConn()
	mockConn2.SetWriteError(errors.New("heartbeat write error"))
	
	err = haService.sendHeartbeatToSlave(&SlaveConnection{
		conn:      mockConn2,
		slaveAddr: "test-slave2",
		running:   true,
	})
	
	if err == nil {
		t.Error("Heartbeat should fail with write error")
	}
}

// TestHAServiceMultipleSlaveManagement 测试多个从节点管理
func TestHAServiceMultipleSlaveManagement(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	// 添加一些测试数据，让commitLog有一个较大的maxOffset
	for i := 0; i < 10; i++ {
		testData := fmt.Sprintf("test message %d", i)
		commitLog.AppendData([]byte(testData))
	}
	
	maxOffset := commitLog.GetMaxOffset()
	
	config := DefaultHAConfig()
	haService := NewHAService(config, commitLog)
	
	err := haService.Start()
	if err != nil {
		t.Fatalf("Failed to start HA service: %v", err)
	}
	defer haService.Shutdown()
	
	// 创建多个从节点连接
	slaveConns := make([]*SlaveConnection, 5)
	for i := 0; i < 5; i++ {
		slaveConns[i] = &SlaveConnection{
			conn:               NewMockNetConn(),
			slaveAddr:          fmt.Sprintf("slave%d:8080", i),
			running:            true,
			slaveRequestOffset: int64(i * 100),
			slaveAckOffset:     int64(i * 50),
		}
		
		haService.mutex.Lock()
		haService.slaveConnections[slaveConns[i].slaveAddr] = slaveConns[i]
		haService.mutex.Unlock()
	}
	
	// 验证所有从节点都被添加
	status := haService.GetReplicationStatus()
	slaveCount := status["slave_count"].(int)
	if slaveCount != 5 {
		t.Errorf("Expected 5 slaves, got %d", slaveCount)
	}
	
	// 测试获取最小ack offset
	minAckOffset := haService.getSlaveAckOffset()
	expectedMin := int64(0) // 最小的ack offset (slave0的ack offset)
	if minAckOffset != expectedMin {
		t.Errorf("Expected min ack offset %d, got %d", expectedMin, minAckOffset)
	}
	
	// 更新一些从节点的ack offset
	for i := 0; i < 3; i++ {
		slaveConns[i].mutex.Lock()
		slaveConns[i].slaveAckOffset = 200
		slaveConns[i].mutex.Unlock()
	}
	
	// 最小ack offset应该仍然是最慢的从节点 (slave3: 150, slave4: 200)
	// 但是由于getSlaveAckOffset的逻辑，当所有从节点的ack offset都大于commitLog.GetMaxOffset()时，
	// 它会返回commitLog.GetMaxOffset()，所以这里应该是140
	minAckOffset = haService.getSlaveAckOffset()
	expectedMin = maxOffset // 应该返回commitLog.GetMaxOffset()
	if minAckOffset != expectedMin {
		t.Errorf("Expected min ack offset %d, got %d", expectedMin, minAckOffset)
	}
	
	// 断开一些从节点
	for i := 3; i < 5; i++ {
		slaveConns[i].running = false
		haService.mutex.Lock()
		delete(haService.slaveConnections, slaveConns[i].slaveAddr)
		haService.mutex.Unlock()
	}
	
	// 验证从节点数量更新
	status = haService.GetReplicationStatus()
	slaveCount = status["slave_count"].(int)
	if slaveCount != 3 {
		t.Errorf("Expected 3 slaves after disconnection, got %d", slaveCount)
	}
	
	// 现在最小ack offset应该仍然是commitLog.GetMaxOffset()，因为剩下的从节点ack offset都是200
	minAckOffset = haService.getSlaveAckOffset()
	expectedMin = maxOffset // 应该返回commitLog.GetMaxOffset()
	if minAckOffset != expectedMin {
		t.Errorf("Expected min ack offset %d after disconnection, got %d", expectedMin, minAckOffset)
	}
}

// TestHAServiceConnectionTimeout 测试连接超时处理
func TestHAServiceConnectionTimeout(t *testing.T) {
	commitLog := NewExtendedMockCommitLog()
	config := &HAConfig{
		BrokerRole:          SLAVE,
		ReplicationMode:     ASYNC_REPLICATION,
		HaListenPort:        0,
		HaMasterAddress:     "192.0.2.1:99999", // 不可达地址
		HaHeartbeatInterval: 1000,
		HaConnectionTimeout: 100, // 短超时时间
		MaxTransferSize:     1024,
		SyncFlushTimeout:    1000,
	}
	
	haService := NewHAService(config, commitLog)
	
	// 启动从节点，应该快速超时
	start := time.Now()
	err := haService.Start()
	if err != nil {
		t.Fatalf("Start should not fail immediately: %v", err)
	}
	
	// 等待连接尝试
	time.Sleep(200 * time.Millisecond)
	
	// 验证服务仍在运行（会持续重试）
	if atomic.LoadInt32(&haService.running) == 0 {
		t.Error("Service should still be running despite connection timeouts")
	}
	
	elapsed := time.Since(start)
	if elapsed > 1*time.Second {
		t.Error("Connection attempts should not block service startup")
	}
	
	haService.Shutdown()
}

// BenchmarkHAServiceNetworkOperations 网络操作基准测试
func BenchmarkHAServiceNetworkOperations(b *testing.B) {
	commitLog := NewExtendedMockCommitLog()
	config := DefaultHAConfig()
	haService := NewHAService(config, commitLog)
	
	mockConn := NewMockNetConn()
	testData := []byte("benchmark test data for HA service")
	
	b.ResetTimer()
	
	b.Run("SendDataPacket", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := haService.sendDataPacket(mockConn, int64(i), testData)
			if err != nil {
				b.Fatalf("SendDataPacket failed: %v", err)
			}
		}
	})
	
	b.Run("SendSlaveRequest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := haService.sendSlaveRequest(mockConn, int64(i))
			if err != nil {
				b.Fatalf("SendSlaveRequest failed: %v", err)
			}
		}
	})
	
	b.Run("SendHeartbeat", func(b *testing.B) {
		slaveConn := &SlaveConnection{
			conn:      mockConn,
			slaveAddr: "benchmark-slave",
			running:   true,
		}
		
		for i := 0; i < b.N; i++ {
			err := haService.sendHeartbeatToSlave(slaveConn)
			if err != nil {
				b.Fatalf("SendHeartbeat failed: %v", err)
			}
		}
	})
}