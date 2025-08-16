package remoting

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go-rocketmq/pkg/protocol"
)

// ConnectionPool 连接池
type ConnectionPool struct {
	client          *RemotingClient
	connections     sync.Map // map[string]*PooledConnection
	maxConnections  int32
	currentCount    int32
	maxIdleTime     time.Duration
	connectTimeout  time.Duration
	requestTimeout  time.Duration
	closed          int32
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// PooledConnection 池化连接
type PooledConnection struct {
	addr        string
	client      *RemotingClient
	lastUsed    time.Time
	useCount    int64
	mutex       sync.RWMutex
	closed      bool
	createdTime time.Time
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
	
	ctx, cancel := context.WithCancel(context.Background())
	pool := &ConnectionPool{
		client:         NewRemotingClient(),
		maxConnections: config.MaxConnections,
		maxIdleTime:    config.MaxIdleTime,
		connectTimeout: config.ConnectTimeout,
		requestTimeout: config.RequestTimeout,
		ctx:            ctx,
		cancel:         cancel,
	}
	
	// 启动清理goroutine
	pool.wg.Add(1)
	go pool.cleanupRoutine()
	
	return pool
}

// GetConnection 获取连接
func (cp *ConnectionPool) GetConnection(addr string) (*PooledConnection, error) {
	if atomic.LoadInt32(&cp.closed) == 1 {
		return nil, fmt.Errorf("connection pool is closed")
	}
	
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
		return nil, fmt.Errorf("connection pool is full")
	}
	
	// 创建新连接
	return cp.createConnection(addr)
}

// createConnection 创建新连接
func (cp *ConnectionPool) createConnection(addr string) (*PooledConnection, error) {
	// 创建新的RemotingClient
	client := NewRemotingClient()
	
	// 建立连接
	if err := client.Connect(addr); err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", addr, err)
	}
	
	conn := &PooledConnection{
		addr:        addr,
		client:      client,
		lastUsed:    time.Now(),
		useCount:    1,
		closed:      false,
		createdTime: time.Now(),
	}
	
	cp.connections.Store(addr, conn)
	atomic.AddInt32(&cp.currentCount, 1)
	
	return conn, nil
}

// SendSync 同步发送请求
func (cp *ConnectionPool) SendSync(addr string, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	conn, err := cp.GetConnection(addr)
	if err != nil {
		return nil, err
	}
	
	return conn.client.SendSync(addr, request, int64(cp.requestTimeout/time.Millisecond))
}

// SendAsync 异步发送请求
func (cp *ConnectionPool) SendAsync(addr string, request *protocol.RemotingCommand, callback ResponseCallback) error {
	conn, err := cp.GetConnection(addr)
	if err != nil {
		return err
	}
	
	return conn.client.SendAsync(addr, request, int64(cp.requestTimeout/time.Millisecond), callback)
}

// SendOneway 单向发送请求
func (cp *ConnectionPool) SendOneway(addr string, request *protocol.RemotingCommand) error {
	conn, err := cp.GetConnection(addr)
	if err != nil {
		return err
	}
	
	return conn.client.SendOneway(addr, request)
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
	
	return !conn.closed && conn.client.IsConnected(addr)
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
		conn.client.Close()
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
func (cp *ConnectionPool) Close() {
	if !atomic.CompareAndSwapInt32(&cp.closed, 0, 1) {
		return
	}
	
	cp.cancel()
	
	// 关闭所有连接
	cp.connections.Range(func(key, value interface{}) bool {
		conn := value.(*PooledConnection)
		conn.mutex.Lock()
		if !conn.closed {
			conn.closed = true
			conn.client.Close()
		}
		conn.mutex.Unlock()
		return true
	})
	
	// 关闭主客户端
	cp.client.Close()
	
	// 等待清理goroutine结束
	cp.wg.Wait()
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
func (cp *ConnectionPool) SetMaxConnections(max int32) {
	atomic.StoreInt32(&cp.maxConnections, max)
}

// GetMaxIdleTime 获取最大空闲时间
func (cp *ConnectionPool) GetMaxIdleTime() time.Duration {
	return cp.maxIdleTime
}

// SetMaxIdleTime 设置最大空闲时间
func (cp *ConnectionPool) SetMaxIdleTime(duration time.Duration) {
	cp.maxIdleTime = duration
}

// GetConnectTimeout 获取连接超时
func (cp *ConnectionPool) GetConnectTimeout() time.Duration {
	return cp.connectTimeout
}

// SetConnectTimeout 设置连接超时
func (cp *ConnectionPool) SetConnectTimeout(timeout time.Duration) {
	cp.connectTimeout = timeout
}

// GetRequestTimeout 获取请求超时
func (cp *ConnectionPool) GetRequestTimeout() time.Duration {
	return cp.requestTimeout
}

// SetRequestTimeout 设置请求超时
func (cp *ConnectionPool) SetRequestTimeout(timeout time.Duration) {
	cp.requestTimeout = timeout
}

// TestConnection 测试连接
func (cp *ConnectionPool) TestConnection(addr string) error {
	conn, err := cp.GetConnection(addr)
	if err != nil {
		return err
	}
	
	// 发送心跳测试连接
	heartbeat := protocol.CreateRemotingCommand(protocol.RequestCode(34)) // HEART_BEAT
	heartbeat.Body = []byte(`{"clientID":"test"}`)
	
	_, err = conn.client.SendSync(addr, heartbeat, 5000) // 5秒超时
	return err
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