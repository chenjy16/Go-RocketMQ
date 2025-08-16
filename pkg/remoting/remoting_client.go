package remoting

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go-rocketmq/pkg/protocol"
)

// RemotingClient RocketMQ远程通信客户端
type RemotingClient struct {
	connections sync.Map // map[string]*Connection
	requestTable sync.Map // map[int32]*ResponseFuture
	opaque       int32    // 请求序列号
	closed       int32    // 关闭标志
	ctx          context.Context
	cancel       context.CancelFunc
}

// Connection TCP连接封装
type Connection struct {
	addr     string
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	mutex    sync.RWMutex
	lastUsed time.Time
	closed   bool
}

// ResponseFuture 响应Future
type ResponseFuture struct {
	Opaque       int32
	TimeoutMs    int64
	Callback     ResponseCallback
	BeginTime    time.Time
	Done         chan *protocol.RemotingCommand
	Semaphore    chan struct{}
}

// ResponseCallback 响应回调
type ResponseCallback func(*protocol.RemotingCommand)

// NewRemotingClient 创建远程通信客户端
func NewRemotingClient() *RemotingClient {
	ctx, cancel := context.WithCancel(context.Background())
	client := &RemotingClient{
		ctx:    ctx,
		cancel: cancel,
	}
	
	// 启动清理goroutine
	go client.cleanupRoutine()
	
	return client
}

// Connect 连接到指定地址
func (rc *RemotingClient) Connect(addr string) error {
	if atomic.LoadInt32(&rc.closed) == 1 {
		return fmt.Errorf("client is closed")
	}
	
	// 检查是否已存在连接
	if conn, exists := rc.connections.Load(addr); exists {
		if c := conn.(*Connection); !c.closed {
			c.lastUsed = time.Now()
			return nil
		}
	}
	
	// 建立新连接
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", addr, err)
	}
	
	connection := &Connection{
		addr:     addr,
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
		lastUsed: time.Now(),
		closed:   false,
	}
	
	rc.connections.Store(addr, connection)
	
	// 启动接收goroutine
	go rc.receiveResponse(connection)
	
	return nil
}

// SendSync 同步发送请求
func (rc *RemotingClient) SendSync(addr string, request *protocol.RemotingCommand, timeoutMs int64) (*protocol.RemotingCommand, error) {
	if atomic.LoadInt32(&rc.closed) == 1 {
		return nil, fmt.Errorf("client is closed")
	}
	
	// 确保连接存在
	if err := rc.Connect(addr); err != nil {
		return nil, err
	}
	
	// 设置请求ID
	opaque := atomic.AddInt32(&rc.opaque, 1)
	request.Opaque = opaque
	
	// 创建ResponseFuture
	future := &ResponseFuture{
		Opaque:    opaque,
		TimeoutMs: timeoutMs,
		BeginTime: time.Now(),
		Done:      make(chan *protocol.RemotingCommand, 1),
		Semaphore: make(chan struct{}, 1),
	}
	
	rc.requestTable.Store(opaque, future)
	defer rc.requestTable.Delete(opaque)
	
	// 发送请求
	if err := rc.sendRequest(addr, request); err != nil {
		return nil, err
	}
	
	// 等待响应
	select {
	case response := <-future.Done:
		return response, nil
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		return nil, fmt.Errorf("request timeout after %dms", timeoutMs)
	case <-rc.ctx.Done():
		return nil, fmt.Errorf("client context cancelled")
	}
}

// SendAsync 异步发送请求
func (rc *RemotingClient) SendAsync(addr string, request *protocol.RemotingCommand, timeoutMs int64, callback ResponseCallback) error {
	if atomic.LoadInt32(&rc.closed) == 1 {
		return fmt.Errorf("client is closed")
	}
	
	// 确保连接存在
	if err := rc.Connect(addr); err != nil {
		return err
	}
	
	// 设置请求ID
	opaque := atomic.AddInt32(&rc.opaque, 1)
	request.Opaque = opaque
	
	// 创建ResponseFuture
	future := &ResponseFuture{
		Opaque:    opaque,
		TimeoutMs: timeoutMs,
		Callback:  callback,
		BeginTime: time.Now(),
		Semaphore: make(chan struct{}, 1),
	}
	
	rc.requestTable.Store(opaque, future)
	
	// 发送请求
	return rc.sendRequest(addr, request)
}

// SendOneway 单向发送请求（不等待响应）
func (rc *RemotingClient) SendOneway(addr string, request *protocol.RemotingCommand) error {
	if atomic.LoadInt32(&rc.closed) == 1 {
		return fmt.Errorf("client is closed")
	}
	
	// 确保连接存在
	if err := rc.Connect(addr); err != nil {
		return err
	}
	
	// 设置单向标志
	request.Flag |= 1 // RPC_ONEWAY
	request.Opaque = atomic.AddInt32(&rc.opaque, 1)
	
	// 发送请求
	return rc.sendRequest(addr, request)
}

// sendRequest 发送请求到指定地址
func (rc *RemotingClient) sendRequest(addr string, request *protocol.RemotingCommand) error {
	connValue, exists := rc.connections.Load(addr)
	if !exists {
		return fmt.Errorf("connection to %s not found", addr)
	}
	
	connection := connValue.(*Connection)
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	
	if connection.closed {
		return fmt.Errorf("connection to %s is closed", addr)
	}
	
	// 序列化请求
	data, err := rc.encodeRemotingCommand(request)
	if err != nil {
		return fmt.Errorf("failed to encode request: %v", err)
	}
	
	// 发送数据
	if _, err := connection.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write request: %v", err)
	}
	
	if err := connection.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush request: %v", err)
	}
	
	connection.lastUsed = time.Now()
	return nil
}

// receiveResponse 接收响应
func (rc *RemotingClient) receiveResponse(connection *Connection) {
	defer func() {
		connection.mutex.Lock()
		connection.closed = true
		connection.conn.Close()
		connection.mutex.Unlock()
		rc.connections.Delete(connection.addr)
	}()
	
	for {
		select {
		case <-rc.ctx.Done():
			return
		default:
		}
		
		// 设置读取超时
		connection.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		
		// 读取响应
		response, err := rc.decodeRemotingCommand(connection.reader)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}
		
		// 处理响应
		rc.processResponse(response)
	}
}

// processResponse 处理响应
func (rc *RemotingClient) processResponse(response *protocol.RemotingCommand) {
	futureValue, exists := rc.requestTable.Load(response.Opaque)
	if !exists {
		return
	}
	
	future := futureValue.(*ResponseFuture)
	rc.requestTable.Delete(response.Opaque)
	
	if future.Callback != nil {
		// 异步回调
		go future.Callback(response)
	} else if future.Done != nil {
		// 同步响应
		select {
		case future.Done <- response:
		default:
		}
	}
}

// encodeRemotingCommand 编码RemotingCommand
func (rc *RemotingClient) encodeRemotingCommand(cmd *protocol.RemotingCommand) ([]byte, error) {
	// 序列化header
	headerData, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	
	headerLength := len(headerData)
	bodyLength := len(cmd.Body)
	totalLength := 4 + headerLength + bodyLength
	
	// 构建数据包
	buf := bytes.NewBuffer(make([]byte, 0, totalLength+4))
	
	// 写入总长度
	binary.Write(buf, binary.BigEndian, int32(totalLength))
	
	// 写入header长度和序列化类型
	headerLengthAndSerializeType := (headerLength << 8) | 0 // JSON序列化
	binary.Write(buf, binary.BigEndian, int32(headerLengthAndSerializeType))
	
	// 写入header数据
	buf.Write(headerData)
	
	// 写入body数据
	if bodyLength > 0 {
		buf.Write(cmd.Body)
	}
	
	return buf.Bytes(), nil
}

// decodeRemotingCommand 解码RemotingCommand
func (rc *RemotingClient) decodeRemotingCommand(reader *bufio.Reader) (*protocol.RemotingCommand, error) {
	// 读取总长度
	var totalLength int32
	if err := binary.Read(reader, binary.BigEndian, &totalLength); err != nil {
		return nil, err
	}
	
	if totalLength <= 0 || totalLength > 16*1024*1024 { // 16MB限制
		return nil, fmt.Errorf("invalid total length: %d", totalLength)
	}
	
	// 读取header长度和序列化类型
	var headerLengthAndSerializeType int32
	if err := binary.Read(reader, binary.BigEndian, &headerLengthAndSerializeType); err != nil {
		return nil, err
	}
	
	headerLength := (headerLengthAndSerializeType >> 8) & 0xFFFFFF
	serializeType := headerLengthAndSerializeType & 0xFF
	
	if headerLength <= 0 || headerLength > totalLength-4 {
		return nil, fmt.Errorf("invalid header length: %d", headerLength)
	}
	
	// 读取header数据
	headerData := make([]byte, headerLength)
	if _, err := io.ReadFull(reader, headerData); err != nil {
		return nil, err
	}
	
	// 读取body数据
	bodyLength := totalLength - 4 - headerLength
	var bodyData []byte
	if bodyLength > 0 {
		bodyData = make([]byte, bodyLength)
		if _, err := io.ReadFull(reader, bodyData); err != nil {
			return nil, err
		}
	}
	
	// 反序列化header
	var cmd protocol.RemotingCommand
	if serializeType == 0 { // JSON
		if err := json.Unmarshal(headerData, &cmd); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("unsupported serialize type: %d", serializeType)
	}
	
	cmd.Body = bodyData
	return &cmd, nil
}

// cleanupRoutine 清理过期连接和请求
func (rc *RemotingClient) cleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			rc.cleanupConnections()
			rc.cleanupRequests()
		case <-rc.ctx.Done():
			return
		}
	}
}

// cleanupConnections 清理过期连接
func (rc *RemotingClient) cleanupConnections() {
	now := time.Now()
	rc.connections.Range(func(key, value interface{}) bool {
		connection := value.(*Connection)
		connection.mutex.RLock()
		lastUsed := connection.lastUsed
		closed := connection.closed
		connection.mutex.RUnlock()
		
		// 清理5分钟未使用的连接
		if closed || now.Sub(lastUsed) > 5*time.Minute {
			connection.mutex.Lock()
			if !connection.closed {
				connection.closed = true
				connection.conn.Close()
			}
			connection.mutex.Unlock()
			rc.connections.Delete(key)
		}
		return true
	})
}

// cleanupRequests 清理超时请求
func (rc *RemotingClient) cleanupRequests() {
	now := time.Now()
	rc.requestTable.Range(func(key, value interface{}) bool {
		future := value.(*ResponseFuture)
		timeout := time.Duration(future.TimeoutMs) * time.Millisecond
		
		if now.Sub(future.BeginTime) > timeout {
			rc.requestTable.Delete(key)
			
			// 通知超时
			if future.Callback != nil {
				go future.Callback(nil)
			} else if future.Done != nil {
				select {
				case future.Done <- nil:
				default:
				}
			}
		}
		return true
	})
}

// Close 关闭客户端
func (rc *RemotingClient) Close() {
	if !atomic.CompareAndSwapInt32(&rc.closed, 0, 1) {
		return
	}
	
	rc.cancel()
	
	// 关闭所有连接
	rc.connections.Range(func(key, value interface{}) bool {
		connection := value.(*Connection)
		connection.mutex.Lock()
		if !connection.closed {
			connection.closed = true
			connection.conn.Close()
		}
		connection.mutex.Unlock()
		return true
	})
	
	// 清理所有请求
	rc.requestTable.Range(func(key, value interface{}) bool {
		rc.requestTable.Delete(key)
		return true
	})
}

// IsConnected 检查是否连接到指定地址
func (rc *RemotingClient) IsConnected(addr string) bool {
	connValue, exists := rc.connections.Load(addr)
	if !exists {
		return false
	}
	
	connection := connValue.(*Connection)
	connection.mutex.RLock()
	defer connection.mutex.RUnlock()
	
	return !connection.closed
}