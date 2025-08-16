package remoting

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go-rocketmq/pkg/protocol"
)

// RemotingServer RocketMQ远程通信服务端
type RemotingServer struct {
	listenPort    int
	listener      net.Listener
	processors    sync.Map // map[protocol.RequestCode]RequestProcessor
	connections   sync.Map // map[string]*ServerConnection
	closed        int32
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// ServerConnection 服务端连接
type ServerConnection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	mutex    sync.RWMutex
	lastUsed time.Time
	closed   bool
	remoteAddr string
}

// RequestProcessor 请求处理器接口
type RequestProcessor interface {
	ProcessRequest(ctx context.Context, request *protocol.RemotingCommand, conn *ServerConnection) (*protocol.RemotingCommand, error)
}

// RequestProcessorFunc 请求处理器函数类型
type RequestProcessorFunc func(ctx context.Context, request *protocol.RemotingCommand, conn *ServerConnection) (*protocol.RemotingCommand, error)

// ProcessRequest 实现RequestProcessor接口
func (f RequestProcessorFunc) ProcessRequest(ctx context.Context, request *protocol.RemotingCommand, conn *ServerConnection) (*protocol.RemotingCommand, error) {
	return f(ctx, request, conn)
}

// NewRemotingServer 创建远程通信服务端
func NewRemotingServer(listenPort int) *RemotingServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &RemotingServer{
		listenPort: listenPort,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// RegisterProcessor 注册请求处理器
func (rs *RemotingServer) RegisterProcessor(code protocol.RequestCode, processor RequestProcessor) {
	rs.processors.Store(code, processor)
}

// RegisterProcessorFunc 注册请求处理器函数
func (rs *RemotingServer) RegisterProcessorFunc(code protocol.RequestCode, processorFunc RequestProcessorFunc) {
	rs.processors.Store(code, processorFunc)
}

// Start 启动服务端
func (rs *RemotingServer) Start() error {
	if atomic.LoadInt32(&rs.closed) == 1 {
		return fmt.Errorf("server is closed")
	}
	
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", rs.listenPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", rs.listenPort, err)
	}
	
	rs.listener = listener
	
	// 启动清理goroutine
	rs.wg.Add(1)
	go rs.cleanupRoutine()
	
	// 启动接受连接的goroutine
	rs.wg.Add(1)
	go rs.acceptConnections()
	
	return nil
}

// acceptConnections 接受客户端连接
func (rs *RemotingServer) acceptConnections() {
	defer rs.wg.Done()
	
	for {
		select {
		case <-rs.ctx.Done():
			return
		default:
		}
		
		conn, err := rs.listener.Accept()
		if err != nil {
			if atomic.LoadInt32(&rs.closed) == 1 {
				return
			}
			continue
		}
		
		// 处理新连接
		rs.wg.Add(1)
		go rs.handleConnection(conn)
	}
}

// handleConnection 处理客户端连接
func (rs *RemotingServer) handleConnection(conn net.Conn) {
	defer rs.wg.Done()
	defer conn.Close()
	
	remoteAddr := conn.RemoteAddr().String()
	serverConn := &ServerConnection{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		lastUsed:   time.Now(),
		closed:     false,
		remoteAddr: remoteAddr,
	}
	
	rs.connections.Store(remoteAddr, serverConn)
	defer func() {
		serverConn.mutex.Lock()
		serverConn.closed = true
		serverConn.mutex.Unlock()
		rs.connections.Delete(remoteAddr)
	}()
	
	for {
		select {
		case <-rs.ctx.Done():
			return
		default:
		}
		
		// 设置读取超时
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		
		// 读取请求
		request, err := rs.decodeRemotingCommand(serverConn.reader)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}
		
		serverConn.lastUsed = time.Now()
		
		// 处理请求
		rs.wg.Add(1)
		go rs.processRequest(request, serverConn)
	}
}

// processRequest 处理请求
func (rs *RemotingServer) processRequest(request *protocol.RemotingCommand, conn *ServerConnection) {
	defer rs.wg.Done()
	
	// 检查是否为单向请求
	isOneway := (request.Flag & 1) != 0
	
	// 查找处理器
	processorValue, exists := rs.processors.Load(request.Code)
	if !exists {
		if !isOneway {
			// 发送不支持的请求码响应
			response := protocol.CreateResponseCommand(protocol.RequestCodeNotSupported, "request code not supported")
			response.Opaque = request.Opaque
			rs.sendResponse(conn, response)
		}
		return
	}
	
	processor := processorValue.(RequestProcessor)
	
	// 处理请求
	ctx, cancel := context.WithTimeout(rs.ctx, 30*time.Second)
	defer cancel()
	
	response, err := processor.ProcessRequest(ctx, request, conn)
	if err != nil {
		if !isOneway {
			// 发送错误响应
			errorResponse := protocol.CreateResponseCommand(protocol.SystemError, err.Error())
			errorResponse.Opaque = request.Opaque
			rs.sendResponse(conn, errorResponse)
		}
		return
	}
	
	// 发送响应（如果不是单向请求）
	if !isOneway && response != nil {
		response.Opaque = request.Opaque
		rs.sendResponse(conn, response)
	}
}

// sendResponse 发送响应
func (rs *RemotingServer) sendResponse(conn *ServerConnection, response *protocol.RemotingCommand) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	
	if conn.closed {
		return
	}
	
	// 编码响应
	data, err := rs.encodeRemotingCommand(response)
	if err != nil {
		return
	}
	
	// 发送响应
	conn.writer.Write(data)
	conn.writer.Flush()
	conn.lastUsed = time.Now()
}

// encodeRemotingCommand 编码RemotingCommand（与客户端相同的实现）
func (rs *RemotingServer) encodeRemotingCommand(cmd *protocol.RemotingCommand) ([]byte, error) {
	// 使用与客户端相同的编码逻辑
	client := &RemotingClient{}
	return client.encodeRemotingCommand(cmd)
}

// decodeRemotingCommand 解码RemotingCommand（与客户端相同的实现）
func (rs *RemotingServer) decodeRemotingCommand(reader *bufio.Reader) (*protocol.RemotingCommand, error) {
	// 使用与客户端相同的解码逻辑
	client := &RemotingClient{}
	return client.decodeRemotingCommand(reader)
}

// cleanupRoutine 清理过期连接
func (rs *RemotingServer) cleanupRoutine() {
	defer rs.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			rs.cleanupConnections()
		case <-rs.ctx.Done():
			return
		}
	}
}

// cleanupConnections 清理过期连接
func (rs *RemotingServer) cleanupConnections() {
	now := time.Now()
	rs.connections.Range(func(key, value interface{}) bool {
		conn := value.(*ServerConnection)
		conn.mutex.RLock()
		lastUsed := conn.lastUsed
		closed := conn.closed
		conn.mutex.RUnlock()
		
		// 清理10分钟未使用的连接
		if closed || now.Sub(lastUsed) > 10*time.Minute {
			conn.mutex.Lock()
			if !conn.closed {
				conn.closed = true
				conn.conn.Close()
			}
			conn.mutex.Unlock()
			rs.connections.Delete(key)
		}
		return true
	})
}

// Stop 停止服务端
func (rs *RemotingServer) Stop() {
	if !atomic.CompareAndSwapInt32(&rs.closed, 0, 1) {
		return
	}
	
	rs.cancel()
	
	if rs.listener != nil {
		rs.listener.Close()
	}
	
	// 关闭所有连接
	rs.connections.Range(func(key, value interface{}) bool {
		conn := value.(*ServerConnection)
		conn.mutex.Lock()
		if !conn.closed {
			conn.closed = true
			conn.conn.Close()
		}
		conn.mutex.Unlock()
		return true
	})
	
	// 等待所有goroutine结束
	rs.wg.Wait()
}

// GetListenPort 获取监听端口
func (rs *RemotingServer) GetListenPort() int {
	return rs.listenPort
}

// GetConnectionCount 获取连接数
func (rs *RemotingServer) GetConnectionCount() int {
	count := 0
	rs.connections.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// SendToClient 向指定客户端发送消息
func (rs *RemotingServer) SendToClient(remoteAddr string, command *protocol.RemotingCommand) error {
	connValue, exists := rs.connections.Load(remoteAddr)
	if !exists {
		return fmt.Errorf("connection to %s not found", remoteAddr)
	}
	
	conn := connValue.(*ServerConnection)
	rs.sendResponse(conn, command)
	return nil
}

// BroadcastToAllClients 向所有客户端广播消息
func (rs *RemotingServer) BroadcastToAllClients(command *protocol.RemotingCommand) {
	rs.connections.Range(func(key, value interface{}) bool {
		conn := value.(*ServerConnection)
		rs.sendResponse(conn, command)
		return true
	})
}