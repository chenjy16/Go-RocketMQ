package connection

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionPoolError 连接池错误
type ConnectionPoolError struct {
	Code    int
	Message string
	Err     error
}

// Error 实现error接口
func (e *ConnectionPoolError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("ConnectionPoolError[code=%d, message=%s]: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("ConnectionPoolError[code=%d, message=%s]", e.Code, e.Message)
}

// Unwrap 返回底层错误
func (e *ConnectionPoolError) Unwrap() error {
	return e.Err
}

// Connection pool error codes
const (
	ErrCodePoolClosed        = 3001
	ErrCodePoolFull          = 3002
	ErrCodeConnectionFailed  = 3003
	ErrCodeInvalidAddress    = 3004
	ErrCodeConnectionTimeout = 3005
	ErrCodePoolConfigInvalid = 3006
	ErrCodeInvalidResponse   = 3007
	ErrCodeEncodeFailed      = 3008
	ErrCodeDecodeFailed      = 3009
)

// NewConnectionPoolError 创建连接池错误
func NewConnectionPoolError(code int, message string, err error) *ConnectionPoolError {
	return &ConnectionPoolError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// ConnectionPool 连接池
type ConnectionPool struct {
	// client is managed by the RemotingClient
	connections    sync.Map // map[string]*PooledConnection
	maxConnections int32
	currentCount   int32
	maxIdleTime    time.Duration
	connectTimeout time.Duration
	requestTimeout time.Duration
	closed         int32
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup

	// 错误处理配置
	errorConfig *ConnectionPoolErrorConfig

	// 指标收集
	metrics *ConnectionPoolMetrics
}

// ConnectionPoolErrorConfig 连接池错误处理配置
type ConnectionPoolErrorConfig struct {
	MaxRetryAttempts        int           // 最大重试次数
	RetryInterval           time.Duration // 重试间隔
	EnableCircuitBreaker    bool          // 是否启用熔断器
	CircuitBreakerThreshold int           // 熔断器阈值
}

// DefaultConnectionPoolErrorConfig 默认连接池错误处理配置
var DefaultConnectionPoolErrorConfig = &ConnectionPoolErrorConfig{
	MaxRetryAttempts:        3,
	RetryInterval:           1 * time.Second,
	EnableCircuitBreaker:    true,
	CircuitBreakerThreshold: 5,
}

// ConnectionPoolMetrics 连接池指标
type ConnectionPoolMetrics struct {
	TotalConnections   int64         // 总连接数
	ActiveConnections  int64         // 活跃连接数
	FailedConnections  int64         // 失败连接数
	TimeoutConnections int64         // 超时连接数
	RetryConnections   int64         // 重试连接数
	AvgConnectionTime  time.Duration // 平均连接时间
	ErrorCountByCode   map[int]int64 // 按错误代码统计
	mutex              sync.RWMutex
}

// PooledConnection 池化连接
type PooledConnection struct {
	addr        string
	conn        net.Conn
	lastUsed    time.Time
	useCount    int64
	mutex       sync.RWMutex
	closed      bool
	createdTime time.Time

	// 连接状态
	lastError     error
	errorCount    int32
	lastErrorTime time.Time
}

// ConnectionPoolConfig 连接池配置
type ConnectionPoolConfig struct {
	MaxConnections int32         // 最大连接数
	MaxIdleTime    time.Duration // 最大空闲时间
	ConnectTimeout time.Duration // 连接超时
	RequestTimeout time.Duration // 请求超时
}

// DefaultConnectionPoolConfig 默认连接池配置
func DefaultConnectionPoolConfig() *ConnectionPoolConfig {
	return &ConnectionPoolConfig{
		MaxConnections: 100,
		MaxIdleTime:    5 * time.Minute,
		ConnectTimeout: 3 * time.Second,
		RequestTimeout: 30 * time.Second,
	}
}

// NewConnectionPool 创建连接池
func NewConnectionPool(config *ConnectionPoolConfig) *ConnectionPool {
	if config == nil {
		config = DefaultConnectionPoolConfig()
	}

	// 验证配置
	if config.MaxConnections <= 0 {
		// 使用默认值而不是返回错误，因为这是构造函数
		config.MaxConnections = 100
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 3 * time.Second
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 30 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool := &ConnectionPool{
		// client is managed by the connection pool
		maxConnections: config.MaxConnections,
		maxIdleTime:    config.MaxIdleTime,
		connectTimeout: config.ConnectTimeout,
		requestTimeout: config.RequestTimeout,
		ctx:            ctx,
		cancel:         cancel,
		// 初始化错误处理配置
		errorConfig: DefaultConnectionPoolErrorConfig,
		// 初始化指标
		metrics: &ConnectionPoolMetrics{
			ErrorCountByCode: make(map[int]int64),
		},
	}

	// 启动清理goroutine
	pool.wg.Add(1)
	go pool.cleanupRoutine()

	return pool
}

// GetConnection 获取连接
func (cp *ConnectionPool) GetConnection(addr string) (*PooledConnection, error) {
	if atomic.LoadInt32(&cp.closed) == 1 {
		return nil, NewConnectionPoolError(ErrCodePoolClosed, "connection pool is closed", nil)
	}

	// 验证地址
	if addr == "" {
		return nil, NewConnectionPoolError(ErrCodeInvalidAddress, "invalid address", nil)
	}

	startTime := time.Now()
	defer func() {
		cp.metrics.updateConnectionTime(time.Since(startTime))
		cp.metrics.incrementTotalConnections()
	}()

	// 重试机制
	var lastErr error
	for attempt := 0; attempt <= cp.errorConfig.MaxRetryAttempts; attempt++ {
		if attempt > 0 {
			// 记录重试
			cp.metrics.incrementRetryConnections()
			time.Sleep(cp.errorConfig.RetryInterval)
		}

		conn, err := cp.getConnectionWithCircuitBreaker(addr)
		if err == nil {
			return conn, nil
		}

		lastErr = err

		// 检查是否应该重试
		if !cp.shouldRetry(err, attempt) {
			break
		}
	}

	cp.metrics.incrementFailedConnections()
	return nil, lastErr
}

// getConnectionWithCircuitBreaker 带熔断器的获取连接
func (cp *ConnectionPool) getConnectionWithCircuitBreaker(addr string) (*PooledConnection, error) {
	// 检查熔断器
	// 这里简化实现，实际应该为每个地址维护一个熔断器

	// 尝试获取现有连接
	if connValue, exists := cp.connections.Load(addr); exists {
		conn := connValue.(*PooledConnection)
		conn.mutex.Lock()
		defer conn.mutex.Unlock()

		if !conn.closed {
			conn.lastUsed = time.Now()
			atomic.AddInt64(&conn.useCount, 1)
			return conn, nil
		}
	}

	// 检查连接数限制
	if atomic.LoadInt32(&cp.currentCount) >= cp.maxConnections {
		return nil, NewConnectionPoolError(ErrCodePoolFull, "connection pool is full", nil)
	}

	// 创建新连接
	return cp.createConnection(addr)
}

// createConnection 创建新连接
func (cp *ConnectionPool) createConnection(addr string) (*PooledConnection, error) {
	// Create a raw TCP connection
	conn, err := net.DialTimeout("tcp", addr, cp.connectTimeout)
	if err != nil {
		cp.metrics.incrementFailedConnections()
		cp.metrics.incrementError(ErrCodeConnectionFailed)
		return nil, NewConnectionPoolError(ErrCodeConnectionFailed, fmt.Sprintf("failed to connect to %s", addr), err)
	}

	// Create a pooled connection wrapper
	pooledConn := &PooledConnection{
		addr:        addr,
		conn:        conn,
		lastUsed:    time.Now(),
		useCount:    1,
		closed:      false,
		createdTime: time.Now(),
	}

	cp.connections.Store(addr, pooledConn)
	atomic.AddInt32(&cp.currentCount, 1)

	return pooledConn, nil
}

// SendSync 同步发送请求
// Removed to avoid circular dependency with RemotingClient

// SendAsync 异步发送请求
// Removed to avoid circular dependency with RemotingClient

// SendOneway 单向发送请求
// Removed to avoid circular dependency with RemotingClient

// shouldRetry 检查是否应该重试
func (cp *ConnectionPool) shouldRetry(err error, attempt int) bool {
	if attempt >= cp.errorConfig.MaxRetryAttempts {
		return false
	}

	// 检查错误类型
	if poolErr, ok := err.(*ConnectionPoolError); ok {
		// 某些错误不应该重试
		switch poolErr.Code {
		case ErrCodePoolClosed, ErrCodeInvalidAddress, ErrCodePoolConfigInvalid:
			return false
		case ErrCodeConnectionTimeout:
			return true
		}
	}

	return true
}

// IsConnected 检查是否连接到指定地址
func (cp *ConnectionPool) IsConnected(addr string) bool {
	connValue, exists := cp.connections.Load(addr)
	if !exists {
		return false
	}

	conn := connValue.(*PooledConnection)
	conn.mutex.RLock()
	defer conn.mutex.RUnlock()

	return !conn.closed && conn.conn != nil
}

// RemoveConnection 移除连接
func (cp *ConnectionPool) RemoveConnection(addr string) {
	connValue, exists := cp.connections.Load(addr)
	if !exists {
		return
	}

	conn := connValue.(*PooledConnection)
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	if !conn.closed {
		conn.closed = true
		if conn.conn != nil {
			conn.conn.Close()
		}
		cp.connections.Delete(addr)
		atomic.AddInt32(&cp.currentCount, -1)
	}
}

// cleanupRoutine 清理例程
func (cp *ConnectionPool) cleanupRoutine() {
	defer cp.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cp.cleanupIdleConnections()
		case <-cp.ctx.Done():
			return
		}
	}
}

// cleanupIdleConnections 清理空闲连接
func (cp *ConnectionPool) cleanupIdleConnections() {
	now := time.Now()
	var toRemove []string

	cp.connections.Range(func(key, value interface{}) bool {
		addr := key.(string)
		conn := value.(*PooledConnection)

		conn.mutex.RLock()
		lastUsed := conn.lastUsed
		closed := conn.closed
		conn.mutex.RUnlock()

		// 检查是否需要清理
		if closed || now.Sub(lastUsed) > cp.maxIdleTime {
			toRemove = append(toRemove, addr)
		}

		return true
	})

	// 移除空闲连接
	for _, addr := range toRemove {
		cp.RemoveConnection(addr)
	}
}

// GetConnectionStats 获取连接统计信息
func (cp *ConnectionPool) GetConnectionStats() map[string]interface{} {
	stats := make(map[string]interface{})
	stats["current_count"] = atomic.LoadInt32(&cp.currentCount)
	stats["max_connections"] = cp.maxConnections
	stats["max_idle_time"] = cp.maxIdleTime.String()
	stats["connect_timeout"] = cp.connectTimeout.String()
	stats["request_timeout"] = cp.requestTimeout.String()

	// 连接详情
	connections := make([]map[string]interface{}, 0)
	cp.connections.Range(func(key, value interface{}) bool {
		addr := key.(string)
		conn := value.(*PooledConnection)

		conn.mutex.RLock()
		connInfo := map[string]interface{}{
			"addr":         addr,
			"last_used":    conn.lastUsed.Format(time.RFC3339),
			"use_count":    atomic.LoadInt64(&conn.useCount),
			"closed":       conn.closed,
			"created_time": conn.createdTime.Format(time.RFC3339),
			"age":          time.Since(conn.createdTime).String(),
		}
		conn.mutex.RUnlock()

		connections = append(connections, connInfo)
		return true
	})
	stats["connections"] = connections

	return stats
}

// Close 关闭连接池
func (cp *ConnectionPool) Close() error {
	if !atomic.CompareAndSwapInt32(&cp.closed, 0, 1) {
		return NewConnectionPoolError(ErrCodePoolClosed, "connection pool is already closed", nil)
	}

	cp.cancel()

	// 关闭所有连接
	cp.connections.Range(func(key, value interface{}) bool {
		conn := value.(*PooledConnection)
		conn.mutex.Lock()
		if !conn.closed {
			conn.closed = true
			if conn.conn != nil {
				conn.conn.Close()
			}
		}
		conn.mutex.Unlock()
		return true
	})

	// 等待清理goroutine结束
	cp.wg.Wait()

	return nil
}

// GetConnectionCount 获取当前连接数
func (cp *ConnectionPool) GetConnectionCount() int32 {
	return atomic.LoadInt32(&cp.currentCount)
}

// GetMaxConnections 获取最大连接数
func (cp *ConnectionPool) GetMaxConnections() int32 {
	return cp.maxConnections
}

// SetMaxConnections 设置最大连接数
func (cp *ConnectionPool) SetMaxConnections(max int32) error {
	if max <= 0 {
		return NewConnectionPoolError(ErrCodePoolConfigInvalid, "max connections must be positive", nil)
	}
	atomic.StoreInt32(&cp.maxConnections, max)
	return nil
}

// GetMaxIdleTime 获取最大空闲时间
func (cp *ConnectionPool) GetMaxIdleTime() time.Duration {
	return cp.maxIdleTime
}

// SetMaxIdleTime 设置最大空闲时间
func (cp *ConnectionPool) SetMaxIdleTime(duration time.Duration) error {
	if duration <= 0 {
		return NewConnectionPoolError(ErrCodePoolConfigInvalid, "idle time must be positive", nil)
	}
	cp.maxIdleTime = duration
	return nil
}

// GetConnectTimeout 获取连接超时
func (cp *ConnectionPool) GetConnectTimeout() time.Duration {
	return cp.connectTimeout
}

// SetConnectTimeout 设置连接超时
func (cp *ConnectionPool) SetConnectTimeout(timeout time.Duration) error {
	if timeout <= 0 {
		return NewConnectionPoolError(ErrCodePoolConfigInvalid, "connect timeout must be positive", nil)
	}
	cp.connectTimeout = timeout
	return nil
}

// GetRequestTimeout 获取请求超时
func (cp *ConnectionPool) GetRequestTimeout() time.Duration {
	return cp.requestTimeout
}

// SetRequestTimeout 设置请求超时
func (cp *ConnectionPool) SetRequestTimeout(timeout time.Duration) error {
	if timeout <= 0 {
		return NewConnectionPoolError(ErrCodePoolConfigInvalid, "request timeout must be positive", nil)
	}
	cp.requestTimeout = timeout
	return nil
}

// TestConnection 测试连接
func (cp *ConnectionPool) TestConnection(addr string) error {
	conn, err := cp.GetConnection(addr)
	if err != nil {
		return err
	}

	// Test the connection by checking if it is still alive
	// For now, we just check if the connection is not nil and not closed
	if conn.conn == nil {
		return NewConnectionPoolError(ErrCodeConnectionFailed, "connection is nil", nil)
	}

	return nil
}

// WarmupConnections 预热连接
func (cp *ConnectionPool) WarmupConnections(addrs []string) error {
	var errors []error

	for _, addr := range addrs {
		if err := cp.TestConnection(addr); err != nil {
			errors = append(errors, fmt.Errorf("failed to warmup connection to %s: %v", addr, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("warmup failed for %d connections: %v", len(errors), errors)
	}

	return nil
}

// Connection pool metrics methods
func (cm *ConnectionPoolMetrics) incrementTotalConnections() {
	cm.mutex.Lock()
	cm.TotalConnections++
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) incrementFailedConnections() {
	cm.mutex.Lock()
	cm.FailedConnections++
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) incrementRetryConnections() {
	cm.mutex.Lock()
	cm.RetryConnections++
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) incrementError(code int) {
	cm.mutex.Lock()
	cm.ErrorCountByCode[code]++
	cm.mutex.Unlock()
}

func (cm *ConnectionPoolMetrics) updateConnectionTime(latency time.Duration) {
	cm.mutex.Lock()
	if cm.AvgConnectionTime == 0 {
		cm.AvgConnectionTime = latency
	} else {
		cm.AvgConnectionTime = (cm.AvgConnectionTime + latency) / 2
	}
	cm.mutex.Unlock()
}

// GetPoolStats 获取连接池统计信息
func (cm *ConnectionPoolMetrics) GetPoolStats() map[string]interface{} {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	stats := map[string]interface{}{
		"total_connections":      cm.TotalConnections,
		"active_connections":     cm.ActiveConnections,
		"failed_connections":     cm.FailedConnections,
		"timeout_connections":    cm.TimeoutConnections,
		"retry_connections":      cm.RetryConnections,
		"avg_connection_time_ms": cm.AvgConnectionTime.Milliseconds(),
	}

	// 添加错误统计
	errorStats := make(map[string]int64)
	for code, count := range cm.ErrorCountByCode {
		errorStats[fmt.Sprintf("error_%d", code)] = count
	}
	stats["errors"] = errorStats

	return stats
}
