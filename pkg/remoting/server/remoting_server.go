package server

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
)

// ServerError 服务端错误
type ServerError struct {
	Code    int
	Message string
	Err     error
}

// Error 实现error接口
func (e *ServerError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("ServerError[code=%d, message=%s]: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("ServerError[code=%d, message=%s]", e.Code, e.Message)
}

// Unwrap 返回底层错误
func (e *ServerError) Unwrap() error {
	return e.Err
}

// Server error codes
const (
	ErrCodeServerStartFailed    = 2001
	ErrCodeServerStopFailed     = 2002
	ErrCodeServerListenFailed   = 2003
	ErrCodeServerProcessFailed  = 2004
	ErrCodeServerEncodeFailed   = 2005
	ErrCodeServerDecodeFailed   = 2006
	ErrCodeServerInvalidRequest = 2007
	ErrCodeServerNetworkError   = 2008
)

// NewServerError 创建服务端错误
func NewServerError(code int, message string, err error) *ServerError {
	return &ServerError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// RemotingServer RocketMQ远程通信服务端
type RemotingServer struct {
	listenPort  int
	listener    net.Listener
	processors  sync.Map // map[RequestCode]RequestProcessor
	connections sync.Map // map[string]*ServerConnection
	closed      int32
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	// 错误处理配置
	errorConfig *ServerErrorHandlingConfig

	// 指标收集
	metrics *ServerMetrics
}

// ServerErrorHandlingConfig 服务端错误处理配置
type ServerErrorHandlingConfig struct {
	MaxConcurrentConnections int           // 最大并发连接数
	ConnectionTimeout        time.Duration // 连接超时
	RequestTimeout           time.Duration // 请求超时
	EnableRequestLogging     bool          // 是否启用请求日志
}

// DefaultServerErrorHandlingConfig 默认服务端错误处理配置
var DefaultServerErrorHandlingConfig = &ServerErrorHandlingConfig{
	MaxConcurrentConnections: 10000,
	ConnectionTimeout:        60 * time.Second,
	RequestTimeout:           30 * time.Second,
	EnableRequestLogging:     true,
}

// ServerMetrics 服务端指标
type ServerMetrics struct {
	TotalConnections  int64         // 总连接数
	ActiveConnections int64         // 活跃连接数
	TotalRequests     int64         // 总请求数
	SuccessRequests   int64         // 成功请求数
	FailedRequests    int64         // 失败请求数
	AvgRequestTime    time.Duration // 平均请求时间
	AvgResponseSize   int64         // 平均响应大小
	ErrorCountByCode  map[int]int64 // 按错误代码统计
	mutex             sync.RWMutex
}

// RequestCode 请求码
type RequestCode int32

// ResponseCode 响应码
type ResponseCode int32

const (
	Success                   ResponseCode = 0
	SystemError               ResponseCode = 1
	SystemBusy                ResponseCode = 2
	RequestCodeNotSupported   ResponseCode = 3
	TransactionFailed         ResponseCode = 4
	FlushDiskTimeout          ResponseCode = 10
	SlaveNotAvailable         ResponseCode = 11
	FlushSlaveTimeout         ResponseCode = 12
	MessageIllegal            ResponseCode = 13
	ServiceNotAvailable       ResponseCode = 14
	VersionNotSupported       ResponseCode = 15
	NoPermission              ResponseCode = 16
	TopicNotExist             ResponseCode = 17
	TopicExistAlready         ResponseCode = 18
	PullNotFound              ResponseCode = 19
	PullRetryImmediately      ResponseCode = 20
	PullOffsetMoved           ResponseCode = 21
	QueryNotFound             ResponseCode = 22
	SubscriptionParseFailed   ResponseCode = 23
	SubscriptionNotExist      ResponseCode = 24
	SubscriptionNotLatest     ResponseCode = 25
	SubscriptionGroupNotExist ResponseCode = 26
)

// RemotingCommand 远程调用命令
type RemotingCommand struct {
	Code      int32             `json:"code"`
	Language  string            `json:"language"`
	Version   int32             `json:"version"`
	Opaque    int32             `json:"opaque"`
	Flag      int32             `json:"flag"`
	Remark    string            `json:"remark"`
	ExtFields map[string]string `json:"extFields"`
	Body      []byte            `json:"body"`
}

// CreateResponseCommand 创建响应命令
func CreateResponseCommand(code ResponseCode, remark string) *RemotingCommand {
	return &RemotingCommand{
		Code:      int32(code),
		Language:  "GO",
		Version:   1,
		Flag:      1, // 响应标志
		Remark:    remark,
		ExtFields: make(map[string]string),
	}
}

// ServerConnection 服务端连接
type ServerConnection struct {
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	mutex      sync.RWMutex
	lastUsed   time.Time
	closed     bool
	remoteAddr string
}

// GetRemoteAddr 获取远程地址
func (sc *ServerConnection) GetRemoteAddr() string {
	return sc.remoteAddr
}

// RequestProcessor 请求处理器接口
type RequestProcessor interface {
	ProcessRequest(ctx context.Context, request *RemotingCommand, conn *ServerConnection) (*RemotingCommand, error)
}

// RequestProcessorFunc 请求处理器函数类型
type RequestProcessorFunc func(ctx context.Context, request *RemotingCommand, conn *ServerConnection) (*RemotingCommand, error)

// ProcessRequest 实现RequestProcessor接口
func (f RequestProcessorFunc) ProcessRequest(ctx context.Context, request *RemotingCommand, conn *ServerConnection) (*RemotingCommand, error) {
	return f(ctx, request, conn)
}

// NewRemotingServer 创建远程通信服务端
func NewRemotingServer(listenPort int) *RemotingServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &RemotingServer{
		listenPort: listenPort,
		ctx:        ctx,
		cancel:     cancel,
		// 初始化错误处理配置
		errorConfig: DefaultServerErrorHandlingConfig,
		// 初始化指标
		metrics: &ServerMetrics{
			ErrorCountByCode: make(map[int]int64),
		},
	}
}

// RegisterProcessor 注册请求处理器
func (rs *RemotingServer) RegisterProcessor(code RequestCode, processor RequestProcessor) {
	rs.processors.Store(code, processor)
}

// RegisterProcessorFunc 注册请求处理器函数
func (rs *RemotingServer) RegisterProcessorFunc(code RequestCode, processorFunc RequestProcessorFunc) {
	rs.processors.Store(code, processorFunc)
}

// Start 启动服务端
func (rs *RemotingServer) Start() error {
	if atomic.LoadInt32(&rs.closed) == 1 {
		return NewServerError(ErrCodeServerStartFailed, "server is closed", nil)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", rs.listenPort))
	if err != nil {
		return NewServerError(ErrCodeServerListenFailed, fmt.Sprintf("failed to listen on port %d", rs.listenPort), err)
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

		// 检查连接数限制
		if rs.getConnectionCount() >= rs.errorConfig.MaxConcurrentConnections {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		conn, err := rs.listener.Accept()
		if err != nil {
			if atomic.LoadInt32(&rs.closed) == 1 {
				return
			}
			// 记录错误但继续接受连接
			rs.metrics.incrementError(ErrCodeServerNetworkError)
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
		conn.SetReadDeadline(time.Now().Add(rs.errorConfig.ConnectionTimeout))

		// 读取请求
		request, err := rs.decodeRemotingCommand(serverConn.reader)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// 连接超时，继续等待
				continue
			}
			// 记录错误并断开连接
			rs.metrics.incrementError(ErrCodeServerDecodeFailed)
			return
		}

		serverConn.lastUsed = time.Now()

		// 处理请求
		rs.wg.Add(1)
		go rs.processRequest(request, serverConn)
	}
}

// processRequest 处理请求
func (rs *RemotingServer) processRequest(request *RemotingCommand, conn *ServerConnection) {
	defer rs.wg.Done()

	startTime := time.Now()
	defer func() {
		rs.metrics.updateRequestTime(time.Since(startTime))
		rs.metrics.incrementTotalRequests()
	}()

	// 检查是否为单向请求
	isOneway := (request.Flag & 1) != 0

	// 记录请求日志
	if rs.errorConfig.EnableRequestLogging {
		rs.logRequest(request, conn.remoteAddr)
	}

	// 查找处理器
	processorValue, exists := rs.processors.Load(request.Code)
	if !exists {
		rs.metrics.incrementFailedRequests()
		rs.metrics.incrementError(ErrCodeServerProcessFailed)

		if !isOneway {
			// 发送不支持的请求码响应
			response := CreateResponseCommand(RequestCodeNotSupported, "request code not supported")
			response.Opaque = request.Opaque
			rs.sendResponse(conn, response)
		}
		return
	}

	processor := processorValue.(RequestProcessor)

	// 处理请求
	ctx, cancel := context.WithTimeout(rs.ctx, rs.errorConfig.RequestTimeout)
	defer cancel()

	response, err := processor.ProcessRequest(ctx, request, conn)
	if err != nil {
		rs.metrics.incrementFailedRequests()
		rs.metrics.incrementError(ErrCodeServerProcessFailed)

		if !isOneway {
			// 发送错误响应
			errorResponse := CreateResponseCommand(SystemError, err.Error())
			errorResponse.Opaque = request.Opaque
			rs.sendResponse(conn, errorResponse)
		}
		return
	}

	// 发送响应（如果不是单向请求）
	if !isOneway && response != nil {
		response.Opaque = request.Opaque
		rs.sendResponse(conn, response)
		rs.metrics.incrementSuccessRequests()
		if response.Body != nil {
			rs.metrics.updateResponseSize(int64(len(response.Body)))
		}
	} else if !isOneway {
		// 发送空响应
		emptyResponse := CreateResponseCommand(Success, "")
		emptyResponse.Opaque = request.Opaque
		rs.sendResponse(conn, emptyResponse)
		rs.metrics.incrementSuccessRequests()
	}
}

// sendResponse 发送响应
func (rs *RemotingServer) sendResponse(conn *ServerConnection, response *RemotingCommand) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	if conn.closed {
		return
	}

	// 编码响应
	data, err := rs.encodeRemotingCommand(response)
	if err != nil {
		rs.metrics.incrementError(ErrCodeServerEncodeFailed)
		return
	}

	// 发送响应
	conn.writer.Write(data)
	conn.writer.Flush()
	conn.lastUsed = time.Now()
}

// encodeRemotingCommand 编码RemotingCommand
func (rs *RemotingServer) encodeRemotingCommand(cmd *RemotingCommand) ([]byte, error) {
	// 序列化header
	headerData, err := json.Marshal(cmd)
	if err != nil {
		return nil, NewServerError(ErrCodeServerEncodeFailed, "failed to marshal command header", err)
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
func (rs *RemotingServer) decodeRemotingCommand(reader *bufio.Reader) (*RemotingCommand, error) {
	// 读取总长度
	var totalLength int32
	if err := binary.Read(reader, binary.BigEndian, &totalLength); err != nil {
		return nil, NewServerError(ErrCodeServerDecodeFailed, "failed to read total length", err)
	}

	if totalLength <= 0 || totalLength > 16*1024*1024 { // 16MB限制
		return nil, NewServerError(ErrCodeServerInvalidRequest, fmt.Sprintf("invalid total length: %d", totalLength), nil)
	}

	// 读取header长度和序列化类型
	var headerLengthAndSerializeType int32
	if err := binary.Read(reader, binary.BigEndian, &headerLengthAndSerializeType); err != nil {
		return nil, NewServerError(ErrCodeServerDecodeFailed, "failed to read header length", err)
	}

	headerLength := (headerLengthAndSerializeType >> 8) & 0xFFFFFF
	serializeType := headerLengthAndSerializeType & 0xFF

	if headerLength <= 0 || headerLength > totalLength-4 {
		return nil, NewServerError(ErrCodeServerInvalidRequest, fmt.Sprintf("invalid header length: %d", headerLength), nil)
	}

	// 读取header数据
	headerData := make([]byte, headerLength)
	if _, err := io.ReadFull(reader, headerData); err != nil {
		return nil, NewServerError(ErrCodeServerDecodeFailed, "failed to read header data", err)
	}

	// 读取body数据
	bodyLength := totalLength - 4 - headerLength
	var bodyData []byte
	if bodyLength > 0 {
		bodyData = make([]byte, bodyLength)
		if _, err := io.ReadFull(reader, bodyData); err != nil {
			return nil, NewServerError(ErrCodeServerDecodeFailed, "failed to read body data", err)
		}
	}

	// 反序列化header
	var cmd RemotingCommand
	if serializeType == 0 { // JSON
		if err := json.Unmarshal(headerData, &cmd); err != nil {
			return nil, NewServerError(ErrCodeServerDecodeFailed, "failed to unmarshal command header", err)
		}
	} else {
		return nil, NewServerError(ErrCodeServerDecodeFailed, fmt.Sprintf("unsupported serialize type: %d", serializeType), nil)
	}

	cmd.Body = bodyData
	return &cmd, nil
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
func (rs *RemotingServer) Stop() error {
	if !atomic.CompareAndSwapInt32(&rs.closed, 0, 1) {
		return NewServerError(ErrCodeServerStopFailed, "server is already closed", nil)
	}

	rs.cancel()

	if rs.listener != nil {
		if err := rs.listener.Close(); err != nil {
			return NewServerError(ErrCodeServerStopFailed, "failed to close listener", err)
		}
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

	return nil
}

// GetListenPort 获取监听端口
func (rs *RemotingServer) GetListenPort() int {
	return rs.listenPort
}

// getConnectionCount 获取连接数
func (rs *RemotingServer) getConnectionCount() int {
	count := 0
	rs.connections.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// SendToClient 向指定客户端发送消息
func (rs *RemotingServer) SendToClient(remoteAddr string, command *RemotingCommand) error {
	connValue, exists := rs.connections.Load(remoteAddr)
	if !exists {
		return NewServerError(ErrCodeServerNetworkError, fmt.Sprintf("connection to %s not found", remoteAddr), nil)
	}

	conn := connValue.(*ServerConnection)
	rs.sendResponse(conn, command)
	return nil
}

// BroadcastToAllClients 向所有客户端广播消息
func (rs *RemotingServer) BroadcastToAllClients(command *RemotingCommand) {
	rs.connections.Range(func(key, value interface{}) bool {
		conn := value.(*ServerConnection)
		rs.sendResponse(conn, command)
		return true
	})
}

// logRequest 记录请求日志
func (rs *RemotingServer) logRequest(request *RemotingCommand, remoteAddr string) {
	// 简化实现，实际应该使用日志库
	fmt.Printf("Processing request code %d from %s\n", request.Code, remoteAddr)
}

// Server metrics methods
func (sm *ServerMetrics) incrementTotalRequests() {
	sm.mutex.Lock()
	sm.TotalRequests++
	sm.mutex.Unlock()
}

func (sm *ServerMetrics) incrementSuccessRequests() {
	sm.mutex.Lock()
	sm.SuccessRequests++
	sm.mutex.Unlock()
}

func (sm *ServerMetrics) incrementFailedRequests() {
	sm.mutex.Lock()
	sm.FailedRequests++
	sm.mutex.Unlock()
}

func (sm *ServerMetrics) incrementError(code int) {
	sm.mutex.Lock()
	sm.ErrorCountByCode[code]++
	sm.mutex.Unlock()
}

func (sm *ServerMetrics) updateRequestTime(latency time.Duration) {
	sm.mutex.Lock()
	if sm.AvgRequestTime == 0 {
		sm.AvgRequestTime = latency
	} else {
		sm.AvgRequestTime = (sm.AvgRequestTime + latency) / 2
	}
	sm.mutex.Unlock()
}

func (sm *ServerMetrics) updateResponseSize(size int64) {
	sm.mutex.Lock()
	if sm.AvgResponseSize == 0 {
		sm.AvgResponseSize = size
	} else {
		sm.AvgResponseSize = (sm.AvgResponseSize + size) / 2
	}
	sm.mutex.Unlock()
}

// GetServerStats 获取服务端统计信息
func (sm *ServerMetrics) GetServerStats() map[string]interface{} {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	stats := map[string]interface{}{
		"total_connections":   sm.TotalConnections,
		"active_connections":  sm.ActiveConnections,
		"total_requests":      sm.TotalRequests,
		"success_requests":    sm.SuccessRequests,
		"failed_requests":     sm.FailedRequests,
		"avg_request_time_ms": sm.AvgRequestTime.Milliseconds(),
		"avg_response_size":   sm.AvgResponseSize,
	}

	// 添加错误统计
	errorStats := make(map[string]int64)
	for code, count := range sm.ErrorCountByCode {
		errorStats[fmt.Sprintf("error_%d", code)] = count
	}
	stats["errors"] = errorStats

	return stats
}
