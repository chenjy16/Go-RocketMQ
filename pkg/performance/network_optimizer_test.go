package performance

import (
	"sync"
	"testing"
	"time"

	"github.com/chenjy16/go-rocketmq-remoting/connection"
)

// TestNewNetworkOptimizer 测试网络优化器创建
func TestNewNetworkOptimizer(t *testing.T) {
	config := NetworkOptimizerConfig{
		MaxConnections: 10,
		ConnectTimeout: 1 * time.Second,
		MaxIdleTime:    100 * time.Millisecond,
	}

	optimizer := NewNetworkOptimizer("127.0.0.1:8080", config)
	if optimizer == nil {
		t.Fatal("NewNetworkOptimizer should not return nil")
	}

	if optimizer.pool == nil {
		t.Error("Connection pool should be initialized")
	}
	if optimizer.metrics == nil {
		t.Error("Metrics should be initialized")
	}
}

// TestConnectionPool 测试连接池
func TestConnectionPool(t *testing.T) {
	poolConfig := &connection.ConnectionPoolConfig{
		MaxConnections: 3,
		ConnectTimeout: 1 * time.Second,
		MaxIdleTime:    100 * time.Millisecond,
	}

	pool := connection.NewConnectionPool(poolConfig)
	if pool == nil {
		t.Fatal("NewConnectionPool should not return nil")
	}

	// 由于无法实际连接到服务器，我们只测试池的基本属性
	// We can't directly access the config, but we can test that the pool was created
	// with the correct parameters by checking its behavior
	stats := pool.GetStats()
	if stats == nil {
		t.Error("GetStats should not return nil")
	}
}

// TestNetworkOptimizerRegisterPool 测试网络优化器获取连接
func TestNetworkOptimizerGetConnection(t *testing.T) {
	config := NetworkOptimizerConfig{
		MaxConnections: 2,
		ConnectTimeout: 100 * time.Millisecond,
	}

	// 创建网络优化器
	optimizer := NewNetworkOptimizer("127.0.0.1:8080", config)

	// 由于无法实际连接到服务器，我们只测试方法调用不会panic
	// 在实际环境中，这需要真实的网络连接
	conn, err := optimizer.GetConnection()
	if conn != nil || err == nil {
		// 如果有连接，需要归还
		if conn != nil {
			optimizer.ReturnConnection(conn)
		}
	}
}

// TestNetworkOptimizerStartStop 测试网络优化器关闭
func TestNetworkOptimizerClose(t *testing.T) {
	config := NetworkOptimizerConfig{
		MaxConnections: 2,
		ConnectTimeout: 100 * time.Millisecond,
	}

	optimizer := NewNetworkOptimizer("127.0.0.1:8080", config)

	// 关闭优化器
	err := optimizer.Close()
	if err != nil {
		t.Errorf("Close should not return error: %v", err)
	}
}

// TestNetworkMetrics 测试网络指标
func TestNetworkMetrics(t *testing.T) {
	config := NetworkOptimizerConfig{
		MaxConnections: 10,
		ConnectTimeout: 1 * time.Second,
		MaxIdleTime:    100 * time.Millisecond,
	}

	optimizer := NewNetworkOptimizer("127.0.0.1:8080", config)

	// 检查指标初始化
	if optimizer.metrics == nil {
		t.Error("Metrics should be initialized")
	}

	// 测试指标的基本功能
	metrics := optimizer.metrics
	metrics.incrementActiveConnections()
	metrics.decrementActiveConnections()
	metrics.incrementConnectionsCreated()

	// 获取网络统计
	stats := optimizer.GetStats()
	if stats == nil {
		t.Error("GetStats should not return nil")
	}

	// 检查统计数据包含预期的字段
	if _, exists := stats["bytes_read"]; !exists {
		t.Error("Stats should contain bytes_read")
	}
	if _, exists := stats["bytes_written"]; !exists {
		t.Error("Stats should contain bytes_written")
	}
}

// TestDefaultPoolConfig 测试默认连接池配置
func TestDefaultNetworkOptimizerConfig(t *testing.T) {
	config := DefaultNetworkOptimizerConfig
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

// TestConcurrentNetworkOperations 测试并发网络操作
func TestConcurrentNetworkOperations(t *testing.T) {
	config := NetworkOptimizerConfig{
		MaxConnections: 10,
		ConnectTimeout: 1 * time.Second,
		MaxIdleTime:    100 * time.Millisecond,
	}

	optimizer := NewNetworkOptimizer("127.0.0.1:8080", config)

	const numGoroutines = 10
	const operationsPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// 并发获取连接
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				// 获取连接
				conn, err := optimizer.GetConnection()
				if conn != nil || err == nil {
					// 如果有连接，需要归还
					if conn != nil {
						optimizer.ReturnConnection(conn)
					}
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
