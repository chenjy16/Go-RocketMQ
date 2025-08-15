package ha

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ReplicationMode 复制模式
type ReplicationMode int

const (
	// ASYNC_REPLICATION 异步复制
	ASYNC_REPLICATION ReplicationMode = iota
	// SYNC_REPLICATION 同步复制
	SYNC_REPLICATION
)

// BrokerRole Broker角色
type BrokerRole int

const (
	// ASYNC_MASTER 异步主节点
	ASYNC_MASTER BrokerRole = iota
	// SYNC_MASTER 同步主节点
	SYNC_MASTER
	// SLAVE 从节点
	SLAVE
)

// HAConfig 高可用配置
type HAConfig struct {
	BrokerRole         BrokerRole
	ReplicationMode    ReplicationMode
	HaListenPort       int
	HaMasterAddress    string
	HaHeartbeatInterval int // 心跳间隔(ms)
	HaConnectionTimeout int // 连接超时(ms)
	MaxTransferSize     int // 最大传输大小
	SyncFlushTimeout    int // 同步刷盘超时(ms)
}

// HAService 高可用服务
type HAService struct {
	config         *HAConfig
	commitLog      CommitLogInterface
	running        int32
	mutex          sync.RWMutex
	shutdown       chan struct{}
	wg             sync.WaitGroup

	// Master相关
	haListener     net.Listener
	slaveConnections map[string]*SlaveConnection

	// Slave相关
	masterConnection *MasterConnection

	// 复制状态
	push2SlaveMaxOffset int64
	slaveAckOffset      int64
}

// CommitLogInterface CommitLog接口
type CommitLogInterface interface {
	GetMaxOffset() int64
	GetData(offset int64, size int32) ([]byte, error)
	AppendData(data []byte) (int64, error)
}

// SlaveConnection 从节点连接
type SlaveConnection struct {
	conn           net.Conn
	slaveAddr      string
	lastWriteTimestamp int64
	slaveRequestOffset int64
	slaveAckOffset     int64
	running        bool
	mutex          sync.Mutex
}

// MasterConnection 主节点连接
type MasterConnection struct {
	conn           net.Conn
	masterAddr     string
	currentReportedOffset int64
	lastWriteTimestamp    int64
	running        bool
	mutex          sync.Mutex
}

// NewHAService 创建高可用服务
func NewHAService(config *HAConfig, commitLog CommitLogInterface) *HAService {
	if config == nil {
		config = DefaultHAConfig()
	}

	return &HAService{
		config:           config,
		commitLog:        commitLog,
		shutdown:         make(chan struct{}),
		slaveConnections: make(map[string]*SlaveConnection),
	}
}

// Start 启动高可用服务
func (ha *HAService) Start() error {
	if !atomic.CompareAndSwapInt32(&ha.running, 0, 1) {
		return fmt.Errorf("HA service already running")
	}

	// 重新初始化shutdown channel
	ha.shutdown = make(chan struct{})

	log.Printf("Starting HA service with role: %v, mode: %v", ha.config.BrokerRole, ha.config.ReplicationMode)

	switch ha.config.BrokerRole {
	case ASYNC_MASTER, SYNC_MASTER:
		return ha.startMaster()
	case SLAVE:
		return ha.startSlave()
	default:
		return fmt.Errorf("unknown broker role: %v", ha.config.BrokerRole)
	}
}

// startMaster 启动主节点
func (ha *HAService) startMaster() error {
	addr := fmt.Sprintf(":%d", ha.config.HaListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	ha.haListener = listener
	log.Printf("HA Master listening on %s", addr)

	// 启动接受连接的goroutine
	ha.wg.Add(1)
	go ha.acceptSlaveConnections()

	// 启动数据推送goroutine
	ha.wg.Add(1)
	go ha.pushDataToSlaves()

	return nil
}

// startSlave 启动从节点
func (ha *HAService) startSlave() error {
	if ha.config.HaMasterAddress == "" {
		return fmt.Errorf("master address not configured for slave")
	}

	log.Printf("HA Slave connecting to master: %s", ha.config.HaMasterAddress)

	// 启动连接主节点的goroutine
	ha.wg.Add(1)
	go ha.connectToMaster()

	return nil
}

// acceptSlaveConnections 接受从节点连接
func (ha *HAService) acceptSlaveConnections() {
	defer ha.wg.Done()
	
	for {
		select {
		case <-ha.shutdown:
			return
		default:
			conn, err := ha.haListener.Accept()
			if err != nil {
				// 检查是否是因为服务关闭导致的错误
				select {
				case <-ha.shutdown:
					return
				default:
					log.Printf("Failed to accept slave connection: %v", err)
					continue
				}
			}

			slaveAddr := conn.RemoteAddr().String()
			log.Printf("Accepted slave connection from: %s", slaveAddr)

			slaveConn := &SlaveConnection{
				conn:      conn,
				slaveAddr: slaveAddr,
				running:   true,
			}

			ha.mutex.Lock()
			ha.slaveConnections[slaveAddr] = slaveConn
			ha.mutex.Unlock()

			// 启动处理从节点的goroutine
			go ha.handleSlaveConnection(slaveConn)
		}
	}
}

// handleSlaveConnection 处理从节点连接
func (ha *HAService) handleSlaveConnection(slaveConn *SlaveConnection) {
	defer func() {
		slaveConn.conn.Close()
		ha.mutex.Lock()
		delete(ha.slaveConnections, slaveConn.slaveAddr)
		ha.mutex.Unlock()
		log.Printf("Slave connection closed: %s", slaveConn.slaveAddr)
	}()

	for slaveConn.running {
		select {
		case <-ha.shutdown:
			return
		default:
			// 读取从节点的请求
			request, err := ha.readSlaveRequest(slaveConn.conn)
			if err != nil {
				log.Printf("Failed to read slave request: %v", err)
				return
			}

			// 更新从节点状态
			slaveConn.mutex.Lock()
			slaveConn.slaveRequestOffset = request.Offset
			slaveConn.lastWriteTimestamp = time.Now().UnixMilli()
			slaveConn.mutex.Unlock()

			// 发送数据给从节点
			if err := ha.sendDataToSlave(slaveConn, request.Offset); err != nil {
				log.Printf("Failed to send data to slave: %v", err)
				return
			}
		}
	}
}

// SlaveRequest 从节点请求
type SlaveRequest struct {
	Offset int64
}

// readSlaveRequest 读取从节点请求
func (ha *HAService) readSlaveRequest(conn net.Conn) (*SlaveRequest, error) {
	// 读取请求长度
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBytes); err != nil {
		return nil, err
	}
	_ = binary.BigEndian.Uint32(lengthBytes) // 长度字段，暂时不使用

	// 读取offset
	offsetBytes := make([]byte, 8)
	if _, err := io.ReadFull(conn, offsetBytes); err != nil {
		return nil, err
	}
	offset := int64(binary.BigEndian.Uint64(offsetBytes))

	return &SlaveRequest{Offset: offset}, nil
}

// sendDataToSlave 发送数据给从节点
func (ha *HAService) sendDataToSlave(slaveConn *SlaveConnection, requestOffset int64) error {
	maxOffset := ha.commitLog.GetMaxOffset()
	if requestOffset >= maxOffset {
		// 没有新数据，发送心跳
		return ha.sendHeartbeatToSlave(slaveConn)
	}

	// 计算要发送的数据大小
	sendSize := maxOffset - requestOffset
	if sendSize > int64(ha.config.MaxTransferSize) {
		sendSize = int64(ha.config.MaxTransferSize)
	}

	// 获取数据
	data, err := ha.commitLog.GetData(requestOffset, int32(sendSize))
	if err != nil {
		return fmt.Errorf("failed to get commit log data: %v", err)
	}

	// 发送数据
	return ha.sendDataPacket(slaveConn.conn, requestOffset, data)
}

// sendHeartbeatToSlave 发送心跳给从节点
func (ha *HAService) sendHeartbeatToSlave(slaveConn *SlaveConnection) error {
	return ha.sendDataPacket(slaveConn.conn, 0, nil)
}

// sendDataPacket 发送数据包
func (ha *HAService) sendDataPacket(conn net.Conn, offset int64, data []byte) error {
	// 构造数据包: [length(4)] + [offset(8)] + [data]
	dataLen := len(data)
	packet := make([]byte, 4+8+dataLen)

	// 写入长度
	binary.BigEndian.PutUint32(packet[0:4], uint32(8+dataLen))
	// 写入offset
	binary.BigEndian.PutUint64(packet[4:12], uint64(offset))
	// 写入数据
	if dataLen > 0 {
		copy(packet[12:], data)
	}

	// 发送数据包
	_, err := conn.Write(packet)
	return err
}

// connectToMaster 连接到主节点
func (ha *HAService) connectToMaster() {
	defer ha.wg.Done()
	
	for {
		select {
		case <-ha.shutdown:
			return
		default:
			if err := ha.doConnectToMaster(); err != nil {
				log.Printf("Failed to connect to master: %v, retrying in 5s", err)
				time.Sleep(5 * time.Second)
				continue
			}
		}
	}
}

// doConnectToMaster 执行连接主节点
func (ha *HAService) doConnectToMaster() error {
	conn, err := net.DialTimeout("tcp", ha.config.HaMasterAddress, 
		time.Duration(ha.config.HaConnectionTimeout)*time.Millisecond)
	if err != nil {
		return err
	}

	log.Printf("Connected to master: %s", ha.config.HaMasterAddress)

	masterConn := &MasterConnection{
		conn:       conn,
		masterAddr: ha.config.HaMasterAddress,
		running:    true,
	}

	ha.masterConnection = masterConn

	// 启动从主节点接收数据的goroutine
	go ha.receiveDataFromMaster(masterConn)

	// 启动向主节点发送请求的goroutine
	go ha.sendRequestToMaster(masterConn)

	return nil
}

// receiveDataFromMaster 从主节点接收数据
func (ha *HAService) receiveDataFromMaster(masterConn *MasterConnection) {
	defer func() {
		masterConn.conn.Close()
		masterConn.running = false
		log.Printf("Master connection closed: %s", masterConn.masterAddr)
	}()

	for masterConn.running {
		select {
		case <-ha.shutdown:
			return
		default:
			// 读取数据包
			packet, err := ha.readDataPacket(masterConn.conn)
			if err != nil {
				log.Printf("Failed to read data from master: %v", err)
				return
			}

			// 如果是心跳包，跳过
			if len(packet.Data) == 0 {
				continue
			}

			// 写入本地CommitLog
			if _, err := ha.commitLog.AppendData(packet.Data); err != nil {
				log.Printf("Failed to append data to commit log: %v", err)
				return
			}

			// 更新复制进度
			masterConn.mutex.Lock()
			masterConn.currentReportedOffset = packet.Offset + int64(len(packet.Data))
			masterConn.lastWriteTimestamp = time.Now().UnixMilli()
			masterConn.mutex.Unlock()
		}
	}
}

// DataPacket 数据包
type DataPacket struct {
	Offset int64
	Data   []byte
}

// readDataPacket 读取数据包
func (ha *HAService) readDataPacket(conn net.Conn) (*DataPacket, error) {
	// 读取长度
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBytes); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBytes)

	// 读取offset
	offsetBytes := make([]byte, 8)
	if _, err := io.ReadFull(conn, offsetBytes); err != nil {
		return nil, err
	}
	offset := int64(binary.BigEndian.Uint64(offsetBytes))

	// 读取数据
	dataLen := length - 8
	var data []byte
	if dataLen > 0 {
		data = make([]byte, dataLen)
		if _, err := io.ReadFull(conn, data); err != nil {
			return nil, err
		}
	}

	return &DataPacket{
		Offset: offset,
		Data:   data,
	}, nil
}

// sendRequestToMaster 向主节点发送请求
func (ha *HAService) sendRequestToMaster(masterConn *MasterConnection) {
	ticker := time.NewTicker(time.Duration(ha.config.HaHeartbeatInterval) * time.Millisecond)
	defer ticker.Stop()

	for masterConn.running {
		select {
		case <-ha.shutdown:
			return
		case <-ticker.C:
			// 发送请求
			offset := ha.commitLog.GetMaxOffset()
			if err := ha.sendSlaveRequest(masterConn.conn, offset); err != nil {
				log.Printf("Failed to send request to master: %v", err)
				return
			}
		}
	}
}

// sendSlaveRequest 发送从节点请求
func (ha *HAService) sendSlaveRequest(conn net.Conn, offset int64) error {
	// 构造请求: [length(4)] + [offset(8)]
	request := make([]byte, 12)
	binary.BigEndian.PutUint32(request[0:4], 8) // 长度
	binary.BigEndian.PutUint64(request[4:12], uint64(offset))

	_, err := conn.Write(request)
	return err
}

// pushDataToSlaves 推送数据到从节点
func (ha *HAService) pushDataToSlaves() {
	defer ha.wg.Done()
	
	ticker := time.NewTicker(100 * time.Millisecond) // 100ms检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ha.shutdown:
			return
		case <-ticker.C:
			ha.doPushDataToSlaves()
		}
	}
}

// doPushDataToSlaves 执行推送数据到从节点
func (ha *HAService) doPushDataToSlaves() {
	maxOffset := ha.commitLog.GetMaxOffset()
	if maxOffset <= ha.push2SlaveMaxOffset {
		return // 没有新数据
	}

	ha.mutex.RLock()
	slaveConns := make([]*SlaveConnection, 0, len(ha.slaveConnections))
	for _, conn := range ha.slaveConnections {
		slaveConns = append(slaveConns, conn)
	}
	ha.mutex.RUnlock()

	// 推送数据到所有从节点
	for _, slaveConn := range slaveConns {
		if !slaveConn.running {
			continue
		}

		slaveConn.mutex.Lock()
		requestOffset := slaveConn.slaveRequestOffset
		slaveConn.mutex.Unlock()

		if requestOffset < maxOffset {
			go func(conn *SlaveConnection) {
				if err := ha.sendDataToSlave(conn, requestOffset); err != nil {
					log.Printf("Failed to push data to slave %s: %v", conn.slaveAddr, err)
				}
			}(slaveConn)
		}
	}

	ha.push2SlaveMaxOffset = maxOffset
}

// WaitForSlaveAck 等待从节点确认（同步复制）
func (ha *HAService) WaitForSlaveAck(offset int64, timeout time.Duration) error {
	if ha.config.ReplicationMode != SYNC_REPLICATION {
		return nil // 异步复制不需要等待
	}

	start := time.Now()
	for time.Since(start) < timeout {
		if ha.getSlaveAckOffset() >= offset {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	return fmt.Errorf("wait for slave ack timeout")
}

// getSlaveAckOffset 获取从节点确认的偏移量
func (ha *HAService) getSlaveAckOffset() int64 {
	ha.mutex.RLock()
	defer ha.mutex.RUnlock()

	minAckOffset := ha.commitLog.GetMaxOffset()
	for _, slaveConn := range ha.slaveConnections {
		slaveConn.mutex.Lock()
		ackOffset := slaveConn.slaveAckOffset
		slaveConn.mutex.Unlock()

		if ackOffset < minAckOffset {
			minAckOffset = ackOffset
		}
	}

	return minAckOffset
}

// Shutdown 关闭高可用服务
func (ha *HAService) Shutdown() {
	if !atomic.CompareAndSwapInt32(&ha.running, 1, 0) {
		return
	}

	log.Printf("Shutting down HA service")
	
	// 先发送关闭信号
	close(ha.shutdown)
	
	// 然后关闭监听器，阻止新连接
	if ha.haListener != nil {
		ha.haListener.Close()
	}

	// 关闭所有从节点连接
	ha.mutex.Lock()
	for _, slaveConn := range ha.slaveConnections {
		slaveConn.running = false
		slaveConn.conn.Close()
	}
	ha.mutex.Unlock()

	// 关闭主节点连接
	if ha.masterConnection != nil {
		ha.masterConnection.running = false
		ha.masterConnection.conn.Close()
	}
	
	// 等待所有goroutine完成
	ha.wg.Wait()
	
	log.Printf("HA service shutdown completed")
}

// GetReplicationStatus 获取复制状态
func (ha *HAService) GetReplicationStatus() map[string]interface{} {
	status := make(map[string]interface{})
	status["role"] = ha.config.BrokerRole
	status["mode"] = ha.config.ReplicationMode
	status["running"] = atomic.LoadInt32(&ha.running) == 1

	if ha.config.BrokerRole == ASYNC_MASTER || ha.config.BrokerRole == SYNC_MASTER {
		ha.mutex.RLock()
		status["slave_count"] = len(ha.slaveConnections)
		slaveStatus := make([]map[string]interface{}, 0)
		for _, slaveConn := range ha.slaveConnections {
			slaveConn.mutex.Lock()
			slaveInfo := map[string]interface{}{
				"address":           slaveConn.slaveAddr,
				"request_offset":    slaveConn.slaveRequestOffset,
				"ack_offset":        slaveConn.slaveAckOffset,
				"last_write_time":   slaveConn.lastWriteTimestamp,
			}
			slaveConn.mutex.Unlock()
			slaveStatus = append(slaveStatus, slaveInfo)
		}
		status["slaves"] = slaveStatus
		ha.mutex.RUnlock()
	} else {
		if ha.masterConnection != nil {
			ha.masterConnection.mutex.Lock()
			status["master_address"] = ha.masterConnection.masterAddr
			status["current_offset"] = ha.masterConnection.currentReportedOffset
			status["last_write_time"] = ha.masterConnection.lastWriteTimestamp
			ha.masterConnection.mutex.Unlock()
		}
	}

	return status
}

// DefaultHAConfig 默认高可用配置
func DefaultHAConfig() *HAConfig {
	return &HAConfig{
		BrokerRole:          ASYNC_MASTER,
		ReplicationMode:     ASYNC_REPLICATION,
		HaListenPort:        10912,
		HaHeartbeatInterval: 5000,  // 5秒
		HaConnectionTimeout: 3000,  // 3秒
		MaxTransferSize:     65536, // 64KB
		SyncFlushTimeout:    5000,  // 5秒
	}
}