package performance

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestNewNetworkOptimizer 测试网络优化器创建
func TestNewNetworkOptimizer(t *testing.T) {
	optimizer := NewNetworkOptimizer()
	if optimizer == nil {
		t.Fatal("NewNetworkOptimizer should not return nil")
	}
	
	if optimizer.connectionPools == nil {
		t.Error("Connection pools should be initialized")
	}
	if optimizer.asyncManager == nil {
		t.Error("Async manager should be initialized")
	}
	if optimizer.metrics == nil {
		t.Error("Metrics should be initialized")
	}
}

// TestConnectionPool 测试连接池
func TestConnectionPool(t *testing.T) {
	config := ConnectionPoolConfig{
		MaxConnections: 3,
		ConnectTimeout: 1 * time.Second,
		MaxIdleTime:    100 * time.Millisecond,
	}
	
	pool := NewConnectionPool("127.0.0.1:8080", config)
	if pool == nil {
		t.Fatal("NewConnectionPool should not return nil")
	}
	
	// 由于无法实际连接到服务器，我们只测试池的基本属性
	if pool.maxConns != config.MaxConnections {
		t.Errorf("Expected MaxConnections %d, got %d", config.MaxConnections, pool.maxConns)
	}
	if pool.connTimeout != config.ConnectTimeout {
		t.Errorf("Expected ConnectTimeout %v, got %v", config.ConnectTimeout, pool.connTimeout)
	}
	if pool.maxIdleTime != config.MaxIdleTime {
		t.Errorf("Expected MaxIdleTime %v, got %v", config.MaxIdleTime, pool.maxIdleTime)
	}
}

// TestNetworkOptimizerRegisterPool 测试网络优化器注册连接池
func TestNetworkOptimizerRegisterPool(t *testing.T) {
	optimizer := NewNetworkOptimizer()
	
	config := ConnectionPoolConfig{
		MaxConnections: 2,
		ConnectTimeout: 100 * time.Millisecond,
	}
	
	// 注册连接池
	optimizer.RegisterConnectionPool("test-pool", "127.0.0.1:8080", config)
	
	// 获取连接池
	pool := optimizer.GetConnectionPool("test-pool")
	if pool == nil {
		t.Error("GetConnectionPool should not return nil")
	}
	
	if pool.maxConns != config.MaxConnections {
		t.Errorf("Expected MaxConnections %d, got %d", config.MaxConnections, pool.maxConns)
	}
	
	// 测试获取不存在的连接池
	nonExistentPool := optimizer.GetConnectionPool("non-existent")
	if nonExistentPool != nil {
		t.Error("GetConnectionPool for non-existent pool should return nil")
	}
}

// TestNetworkOptimizerStartStop 测试网络优化器启动停止
func TestNetworkOptimizerStartStop(t *testing.T) {
	optimizer := NewNetworkOptimizer()
	
	// 启动优化器
	optimizer.Start()
	
	// 检查异步管理器是否启动
	if optimizer.asyncManager == nil {
		t.Error("Async manager should be initialized after start")
	}
	
	// 停止优化器
	optimizer.Stop()
	
	// 验证停止操作不会panic
	// 实际的停止验证需要检查内部状态，这里只验证方法调用
}

// TestNetworkMetrics 测试网络指标
func TestNetworkMetrics(t *testing.T) {
	optimizer := NewNetworkOptimizer()
	
	// 检查指标初始化
	if optimizer.metrics == nil {
		t.Error("Metrics should be initialized")
	}
	
	// 测试指标的基本功能
	metrics := optimizer.metrics
	metrics.incrementActiveConnections()
	metrics.incrementConnectionsCreated()
	metrics.addBytesRead(1024)
	metrics.addBytesWritten(512)
	
	// 获取网络统计
	stats := metrics.GetNetworkStats()
	if stats == nil {
		t.Error("GetNetworkStats should not return nil")
	}
	
	// 检查统计数据包含预期的字段
	if _, exists := stats["active_connections"]; !exists {
		t.Error("Stats should contain active_connections")
	}
	if _, exists := stats["bytes_read"]; !exists {
		t.Error("Stats should contain bytes_read")
	}
	if _, exists := stats["bytes_written"]; !exists {
		t.Error("Stats should contain bytes_written")
	}
}

// TestDefaultPoolConfig 测试默认连接池配置
func TestDefaultPoolConfig(t *testing.T) {
	config := DefaultPoolConfig
	if config.MaxConnections <= 0 {
		t.Error("Default MaxConnections should be greater than 0")
	}
	if config.ConnectTimeout <= 0 {
		t.Error("Default ConnectTimeout should be greater than 0")
	}
	if config.MaxIdleTime <= 0 {
		t.Error("Default MaxIdleTime should be greater than 0")
	}
}

// TestAsyncIOManager 测试异步IO管理器
func TestAsyncIOManager(t *testing.T) {
	manager := NewAsyncIOManager(2)
	if manager == nil {
		t.Fatal("NewAsyncIOManager should not return nil")
	}
	
	if len(manager.workers) != 2 {
		t.Errorf("Expected 2 workers, got %d", len(manager.workers))
	}
	
	// 启动管理器
	manager.Start()
	
	// 停止管理器
	manager.Stop()
}

// TestConcurrentNetworkOperations 测试并发网络操作
func TestConcurrentNetworkOperations(t *testing.T) {
	optimizer := NewNetworkOptimizer()
	
	const numGoroutines = 10
	const operationsPerGoroutine = 10
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	// 并发注册连接池
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				poolName := fmt.Sprintf("pool-%d-%d", id, j)
				addr := fmt.Sprintf("127.0.0.1:%d", 8080+id*100+j)
				config := ConnectionPoolConfig{
					MaxConnections: 5,
					ConnectTimeout: 1 * time.Second,
				}
				optimizer.RegisterConnectionPool(poolName, addr, config)
				
				// 验证注册成功
				pool := optimizer.GetConnectionPool(poolName)
				if pool == nil {
					t.Errorf("Failed to register pool %s", poolName)
					return
				}
			}
		}(i)
	}
	
	wg.Wait()
}

// TestGlobalNetworkOptimizer 测试全局网络优化器
func TestGlobalNetworkOptimizer(t *testing.T) {
	// 初始化全局网络优化器
	InitGlobalNetworkOptimizer()
	
	// 获取全局网络优化器
	optimizer := GetGlobalNetworkOptimizer()
	if optimizer == nil {
		t.Fatal("GetGlobalNetworkOptimizer should not return nil")
	}
	
	// 测试全局变量
	if GlobalNetworkOptimizer == nil {
		t.Error("GlobalNetworkOptimizer should be initialized")
	}
	
	// 验证是同一个实例
	if optimizer != GlobalNetworkOptimizer {
		t.Error("GetGlobalNetworkOptimizer should return the same instance as GlobalNetworkOptimizer")
	}
}

// TestPooledConnectionMethods 测试池化连接方法
func TestPooledConnectionMethods(t *testing.T) {
	// 由于PooledConnection需要实际的网络连接，这里只测试结构体创建
	config := ConnectionPoolConfig{
		MaxConnections: 1,
		ConnectTimeout: 1 * time.Second,
	}
	
	pool := NewConnectionPool("127.0.0.1:8080", config)
	if pool == nil {
		t.Fatal("NewConnectionPool should not return nil")
	}
	
	// 测试池的基本属性
	if pool.address != "127.0.0.1:8080" {
		t.Errorf("Expected address 127.0.0.1:8080, got %s", pool.address)
	}
	
	// 测试关闭池
	err := pool.Close()
	if err != nil {
		t.Errorf("Pool.Close() should not return error: %v", err)
	}
}

// TestMultiplexedConnection 测试多路复用连接
func TestMultiplexedConnection(t *testing.T) {
	// 由于MultiplexedConnection需要实际的PooledConnection，这里只测试基本创建逻辑
	// 在实际环境中，这需要真实的网络连接
	
	// 测试NewMultiplexedConnection不会panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("NewMultiplexedConnection should not panic: %v", r)
		}
	}()
	
	// 传入nil测试
	mc := NewMultiplexedConnection(nil)
	if mc == nil {
		t.Error("NewMultiplexedConnection should handle nil gracefully")
	}
}