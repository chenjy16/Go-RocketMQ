package performance

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionPool 连接池
type ConnectionPool struct {
	address     string
	maxConns    int
	maxIdleTime time.Duration
	connTimeout time.Duration
	readTimeout time.Duration
	writeTimeout time.Duration
	pool        chan *PooledConnection
	active      int32
	closed      int32
	metrics     *NetworkMetrics
	mutex       sync.RWMutex
	tlsConfig   *tls.Config
}

// PooledConnection 池化连接
type PooledConnection struct {
	conn       net.Conn
	lastUsed   time.Time
	pool       *ConnectionPool
	inUse      int32
	bufReader  *bufio.Reader
	bufWriter  *bufio.Writer
	compressor *gzip.Writer
	decompressor *gzip.Reader
}

// NetworkMetrics 网络指标
type NetworkMetrics struct {
	TotalConnections    int64         // 总连接数
	ActiveConnections   int64         // 活跃连接数
	IdleConnections     int64         // 空闲连接数
	ConnectionsCreated  int64         // 创建的连接数
	ConnectionsDestroyed int64        // 销毁的连接数
	BytesRead          int64         // 读取字节数
	BytesWritten       int64         // 写入字节数
	CompressedBytes    int64         // 压缩字节数
	DecompressedBytes  int64         // 解压字节数
	AvgLatency         time.Duration // 平均延迟
	ErrorCount         int64         // 错误计数
	mutex              sync.RWMutex
}

// ConnectionPoolConfig 连接池配置
type ConnectionPoolConfig struct {
	MaxConnections  int           // 最大连接数
	MaxIdleTime     time.Duration // 最大空闲时间
	ConnectTimeout  time.Duration // 连接超时
	ReadTimeout     time.Duration // 读取超时
	WriteTimeout    time.Duration // 写入超时
	KeepAlive       bool          // 保持连接
	TCPNoDelay      bool          // TCP无延迟
	EnableCompression bool        // 启用压缩
	TLSConfig       *tls.Config   // TLS配置
}

// DefaultPoolConfig 默认连接池配置
var DefaultPoolConfig = ConnectionPoolConfig{
	MaxConnections:    100,
	MaxIdleTime:       30 * time.Minute,
	ConnectTimeout:    5 * time.Second,
	ReadTimeout:       30 * time.Second,
	WriteTimeout:      30 * time.Second,
	KeepAlive:         true,
	TCPNoDelay:        true,
	EnableCompression: false,
}

// NewConnectionPool 创建连接池
func NewConnectionPool(address string, config ConnectionPoolConfig) *ConnectionPool {
	return &ConnectionPool{
		address:      address,
		maxConns:     config.MaxConnections,
		maxIdleTime:  config.MaxIdleTime,
		connTimeout:  config.ConnectTimeout,
		readTimeout:  config.ReadTimeout,
		writeTimeout: config.WriteTimeout,
		pool:         make(chan *PooledConnection, config.MaxConnections),
		metrics:      &NetworkMetrics{},
		tlsConfig:    config.TLSConfig,
	}
}

// Get 获取连接
func (cp *ConnectionPool) Get() (*PooledConnection, error) {
	if atomic.LoadInt32(&cp.closed) == 1 {
		return nil, errors.New("connection pool is closed")
	}
	
	// 尝试从池中获取连接
	select {
	case conn := <-cp.pool:
		if cp.isConnValid(conn) {
			atomic.StoreInt32(&conn.inUse, 1)
			conn.lastUsed = time.Now()
			cp.metrics.incrementActiveConnections()
			return conn, nil
		}
		// 连接无效，关闭并创建新连接
		conn.close()
	default:
		// 池中没有可用连接
	}
	
	// 检查是否可以创建新连接
	if atomic.LoadInt32(&cp.active) >= int32(cp.maxConns) {
		return nil, errors.New("connection pool is full")
	}
	
	// 创建新连接
	return cp.createConnection()
}

// Put 归还连接
func (cp *ConnectionPool) Put(conn *PooledConnection) {
	if conn == nil || atomic.LoadInt32(&cp.closed) == 1 {
		return
	}
	
	atomic.StoreInt32(&conn.inUse, 0)
	conn.lastUsed = time.Now()
	cp.metrics.decrementActiveConnections()
	
	// 检查连接是否有效
	if !cp.isConnValid(conn) {
		conn.close()
		return
	}
	
	// 尝试放回池中
	select {
	case cp.pool <- conn:
		// 成功放回池中
	default:
		// 池已满，关闭连接
		conn.close()
	}
}

// createConnection 创建新连接
func (cp *ConnectionPool) createConnection() (*PooledConnection, error) {
	atomic.AddInt32(&cp.active, 1)
	
	dialer := &net.Dialer{
		Timeout:   cp.connTimeout,
		KeepAlive: 30 * time.Second,
	}
	
	var conn net.Conn
	var err error
	
	if cp.tlsConfig != nil {
		conn, err = tls.DialWithDialer(dialer, "tcp", cp.address, cp.tlsConfig)
	} else {
		conn, err = dialer.Dial("tcp", cp.address)
	}
	
	if err != nil {
		atomic.AddInt32(&cp.active, -1)
		return nil, err
	}
	
	// 设置TCP选项
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}
	
	pooledConn := &PooledConnection{
		conn:      conn,
		lastUsed:  time.Now(),
		pool:      cp,
		bufReader: bufio.NewReaderSize(conn, 32*1024),
		bufWriter: bufio.NewWriterSize(conn, 32*1024),
	}
	
	atomic.StoreInt32(&pooledConn.inUse, 1)
	cp.metrics.incrementConnectionsCreated()
	cp.metrics.incrementActiveConnections()
	
	return pooledConn, nil
}

// isConnValid 检查连接是否有效
func (cp *ConnectionPool) isConnValid(conn *PooledConnection) bool {
	if conn == nil || conn.conn == nil {
		return false
	}
	
	// 检查空闲时间
	if time.Since(conn.lastUsed) > cp.maxIdleTime {
		return false
	}
	
	// 检查连接状态
	conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond))
	one := make([]byte, 1)
	if _, err := conn.conn.Read(one); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			// 超时是正常的，表示连接可用
			conn.conn.SetReadDeadline(time.Time{})
			return true
		}
		return false
	}
	
	conn.conn.SetReadDeadline(time.Time{})
	return true
}

// Close 关闭连接池
func (cp *ConnectionPool) Close() error {
	if !atomic.CompareAndSwapInt32(&cp.closed, 0, 1) {
		return errors.New("connection pool is already closed")
	}
	
	// 关闭所有连接
	for {
		select {
		case conn := <-cp.pool:
			conn.close()
		default:
			return nil
		}
	}
}

// Read 读取数据
func (pc *PooledConnection) Read(b []byte) (int, error) {
	if atomic.LoadInt32(&pc.inUse) == 0 {
		return 0, errors.New("connection is not in use")
	}
	
	pc.conn.SetReadDeadline(time.Now().Add(pc.pool.readTimeout))
	n, err := pc.bufReader.Read(b)
	if err == nil {
		pc.pool.metrics.addBytesRead(int64(n))
	}
	return n, err
}

// Write 写入数据
func (pc *PooledConnection) Write(b []byte) (int, error) {
	if atomic.LoadInt32(&pc.inUse) == 0 {
		return 0, errors.New("connection is not in use")
	}
	
	pc.conn.SetWriteDeadline(time.Now().Add(pc.pool.writeTimeout))
	n, err := pc.bufWriter.Write(b)
	if err == nil {
		pc.pool.metrics.addBytesWritten(int64(n))
	}
	return n, err
}

// Flush 刷新缓冲区
func (pc *PooledConnection) Flush() error {
	return pc.bufWriter.Flush()
}

// WriteCompressed 写入压缩数据
func (pc *PooledConnection) WriteCompressed(data []byte) error {
	if pc.compressor == nil {
		pc.compressor = gzip.NewWriter(pc.bufWriter)
	} else {
		pc.compressor.Reset(pc.bufWriter)
	}
	
	_, err := pc.compressor.Write(data)
	if err != nil {
		return err
	}
	
	err = pc.compressor.Close()
	if err != nil {
		return err
	}
	
	pc.pool.metrics.addCompressedBytes(int64(len(data)))
	return pc.Flush()
}

// ReadDecompressed 读取解压数据
func (pc *PooledConnection) ReadDecompressed() ([]byte, error) {
	if pc.decompressor == nil {
		var err error
		pc.decompressor, err = gzip.NewReader(pc.bufReader)
		if err != nil {
			return nil, err
		}
	} else {
		err := pc.decompressor.Reset(pc.bufReader)
		if err != nil {
			return nil, err
		}
	}
	
	var buf bytes.Buffer
	_, err := io.Copy(&buf, pc.decompressor)
	if err != nil {
		return nil, err
	}
	
	data := buf.Bytes()
	pc.pool.metrics.addDecompressedBytes(int64(len(data)))
	return data, nil
}

// close 关闭连接
func (pc *PooledConnection) close() {
	if pc.conn != nil {
		pc.conn.Close()
		atomic.AddInt32(&pc.pool.active, -1)
		pc.pool.metrics.incrementConnectionsDestroyed()
	}
	if pc.compressor != nil {
		pc.compressor.Close()
	}
	if pc.decompressor != nil {
		pc.decompressor.Close()
	}
}

// MultiplexedConnection 多路复用连接
type MultiplexedConnection struct {
	conn     *PooledConnection
	streams  map[uint32]*Stream
	nextID   uint32
	mutex    sync.RWMutex
	closed   int32
	metrics  *NetworkMetrics
}

// Stream 数据流
type Stream struct {
	id       uint32
	conn     *MultiplexedConnection
	inBuffer *bytes.Buffer
	outBuffer *bytes.Buffer
	closed   int32
	mutex    sync.Mutex
}

// NewMultiplexedConnection 创建多路复用连接
func NewMultiplexedConnection(conn *PooledConnection) *MultiplexedConnection {
	return &MultiplexedConnection{
		conn:    conn,
		streams: make(map[uint32]*Stream),
		nextID:  1,
		metrics: &NetworkMetrics{},
	}
}

// OpenStream 打开新流
func (mc *MultiplexedConnection) OpenStream() *Stream {
	if atomic.LoadInt32(&mc.closed) == 1 {
		return nil
	}
	
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	
	streamID := mc.nextID
	mc.nextID++
	
	stream := &Stream{
		id:       streamID,
		conn:     mc,
		inBuffer: &bytes.Buffer{},
		outBuffer: &bytes.Buffer{},
	}
	
	mc.streams[streamID] = stream
	return stream
}

// CloseStream 关闭流
func (mc *MultiplexedConnection) CloseStream(streamID uint32) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	
	if stream, exists := mc.streams[streamID]; exists {
		atomic.StoreInt32(&stream.closed, 1)
		delete(mc.streams, streamID)
	}
}

// Write 流写入数据
func (s *Stream) Write(data []byte) (int, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return 0, errors.New("stream is closed")
	}
	
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	return s.outBuffer.Write(data)
}

// Read 流读取数据
func (s *Stream) Read(b []byte) (int, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return 0, errors.New("stream is closed")
	}
	
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	return s.inBuffer.Read(b)
}

// AsyncIOManager 异步IO管理器
type AsyncIOManager struct {
	workerCount int
	workers     []*AsyncWorker
	taskQueue   chan *AsyncTask
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	metrics     *NetworkMetrics
}

// AsyncWorker 异步工作器
type AsyncWorker struct {
	id      int
	manager *AsyncIOManager
	metrics *NetworkMetrics
}

// AsyncTask 异步任务
type AsyncTask struct {
	Type     string
	Data     []byte
	Conn     *PooledConnection
	Callback func([]byte, error)
	Timeout  time.Duration
}

// NewAsyncIOManager 创建异步IO管理器
func NewAsyncIOManager(workerCount int) *AsyncIOManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &AsyncIOManager{
		workerCount: workerCount,
		workers:     make([]*AsyncWorker, workerCount),
		taskQueue:   make(chan *AsyncTask, 1000),
		ctx:         ctx,
		cancel:      cancel,
		metrics:     &NetworkMetrics{},
	}
}

// Start 启动异步IO管理器
func (aim *AsyncIOManager) Start() {
	for i := 0; i < aim.workerCount; i++ {
		worker := &AsyncWorker{
			id:      i,
			manager: aim,
			metrics: &NetworkMetrics{},
		}
		aim.workers[i] = worker
		aim.wg.Add(1)
		go worker.run()
	}
}

// Stop 停止异步IO管理器
func (aim *AsyncIOManager) Stop() {
	aim.cancel()
	aim.wg.Wait()
	close(aim.taskQueue)
}

// SubmitTask 提交异步任务
func (aim *AsyncIOManager) SubmitTask(task *AsyncTask) error {
	select {
	case aim.taskQueue <- task:
		return nil
	case <-aim.ctx.Done():
		return errors.New("async IO manager is stopped")
	default:
		return errors.New("task queue is full")
	}
}

// run 工作器运行
func (aw *AsyncWorker) run() {
	defer aw.manager.wg.Done()
	
	for {
		select {
		case task := <-aw.manager.taskQueue:
			aw.processTask(task)
		case <-aw.manager.ctx.Done():
			return
		}
	}
}

// processTask 处理任务
func (aw *AsyncWorker) processTask(task *AsyncTask) {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		aw.metrics.updateLatency(latency)
	}()
	
	switch task.Type {
	case "read":
		aw.handleRead(task)
	case "write":
		aw.handleWrite(task)
	case "write_compressed":
		aw.handleWriteCompressed(task)
	default:
		if task.Callback != nil {
			task.Callback(nil, errors.New("unknown task type"))
		}
	}
}

// handleRead 处理读取任务
func (aw *AsyncWorker) handleRead(task *AsyncTask) {
	buffer := make([]byte, len(task.Data))
	n, err := task.Conn.Read(buffer)
	if task.Callback != nil {
		task.Callback(buffer[:n], err)
	}
}

// handleWrite 处理写入任务
func (aw *AsyncWorker) handleWrite(task *AsyncTask) {
	_, err := task.Conn.Write(task.Data)
	if err == nil {
		err = task.Conn.Flush()
	}
	if task.Callback != nil {
		task.Callback(nil, err)
	}
}

// handleWriteCompressed 处理压缩写入任务
func (aw *AsyncWorker) handleWriteCompressed(task *AsyncTask) {
	err := task.Conn.WriteCompressed(task.Data)
	if task.Callback != nil {
		task.Callback(nil, err)
	}
}

// 网络指标方法
func (nm *NetworkMetrics) incrementActiveConnections() {
	nm.mutex.Lock()
	nm.ActiveConnections++
	nm.mutex.Unlock()
}

func (nm *NetworkMetrics) decrementActiveConnections() {
	nm.mutex.Lock()
	nm.ActiveConnections--
	nm.mutex.Unlock()
}

func (nm *NetworkMetrics) incrementConnectionsCreated() {
	nm.mutex.Lock()
	nm.ConnectionsCreated++
	nm.TotalConnections++
	nm.mutex.Unlock()
}

func (nm *NetworkMetrics) incrementConnectionsDestroyed() {
	nm.mutex.Lock()
	nm.ConnectionsDestroyed++
	nm.mutex.Unlock()
}

func (nm *NetworkMetrics) addBytesRead(bytes int64) {
	nm.mutex.Lock()
	nm.BytesRead += bytes
	nm.mutex.Unlock()
}

func (nm *NetworkMetrics) addBytesWritten(bytes int64) {
	nm.mutex.Lock()
	nm.BytesWritten += bytes
	nm.mutex.Unlock()
}

func (nm *NetworkMetrics) addCompressedBytes(bytes int64) {
	nm.mutex.Lock()
	nm.CompressedBytes += bytes
	nm.mutex.Unlock()
}

func (nm *NetworkMetrics) addDecompressedBytes(bytes int64) {
	nm.mutex.Lock()
	nm.DecompressedBytes += bytes
	nm.mutex.Unlock()
}

func (nm *NetworkMetrics) updateLatency(latency time.Duration) {
	nm.mutex.Lock()
	if nm.AvgLatency == 0 {
		nm.AvgLatency = latency
	} else {
		nm.AvgLatency = (nm.AvgLatency + latency) / 2
	}
	nm.mutex.Unlock()
}

// GetNetworkStats 获取网络统计信息
func (nm *NetworkMetrics) GetNetworkStats() map[string]interface{} {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	
	return map[string]interface{}{
		"total_connections":     nm.TotalConnections,
		"active_connections":    nm.ActiveConnections,
		"idle_connections":      nm.IdleConnections,
		"connections_created":   nm.ConnectionsCreated,
		"connections_destroyed": nm.ConnectionsDestroyed,
		"bytes_read":           nm.BytesRead,
		"bytes_written":        nm.BytesWritten,
		"compressed_bytes":     nm.CompressedBytes,
		"decompressed_bytes":   nm.DecompressedBytes,
		"avg_latency_ms":       nm.AvgLatency.Milliseconds(),
		"error_count":          nm.ErrorCount,
	}
}

// NetworkOptimizer 网络优化器
type NetworkOptimizer struct {
	connectionPools map[string]*ConnectionPool
	asyncManager    *AsyncIOManager
	metrics         *NetworkMetrics
	mutex           sync.RWMutex
}

// NewNetworkOptimizer 创建网络优化器
func NewNetworkOptimizer() *NetworkOptimizer {
	return &NetworkOptimizer{
		connectionPools: make(map[string]*ConnectionPool),
		asyncManager:    NewAsyncIOManager(10),
		metrics:         &NetworkMetrics{},
	}
}

// RegisterConnectionPool 注册连接池
func (no *NetworkOptimizer) RegisterConnectionPool(name, address string, config ConnectionPoolConfig) {
	no.mutex.Lock()
	defer no.mutex.Unlock()
	no.connectionPools[name] = NewConnectionPool(address, config)
}

// GetConnectionPool 获取连接池
func (no *NetworkOptimizer) GetConnectionPool(name string) *ConnectionPool {
	no.mutex.RLock()
	defer no.mutex.RUnlock()
	return no.connectionPools[name]
}

// Start 启动网络优化器
func (no *NetworkOptimizer) Start() {
	no.asyncManager.Start()
}

// Stop 停止网络优化器
func (no *NetworkOptimizer) Stop() {
	no.asyncManager.Stop()
	no.mutex.Lock()
	defer no.mutex.Unlock()
	for _, pool := range no.connectionPools {
		pool.Close()
	}
}

// 全局网络优化器
var (
	GlobalNetworkOptimizer *NetworkOptimizer
	networkOnce            sync.Once
)

// InitGlobalNetworkOptimizer 初始化全局网络优化器
func InitGlobalNetworkOptimizer() {
	networkOnce.Do(func() {
		GlobalNetworkOptimizer = NewNetworkOptimizer()
		GlobalNetworkOptimizer.Start()
	})
}

// GetGlobalNetworkOptimizer 获取全局网络优化器
func GetGlobalNetworkOptimizer() *NetworkOptimizer {
	InitGlobalNetworkOptimizer()
	return GlobalNetworkOptimizer
}