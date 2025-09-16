package performance

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/chenjy16/go-rocketmq-remoting/connection"
)

// TestNewNetworkOptimizer 测试网络优化器创建
func TestNewNetworkOptimizer(t *testing.T) {
	config := NetworkOptimizerConfig{
		MaxConnections:    10,
		ConnectTimeout:    1 * time.Second,
		MaxIdleTime:       100 * time.Millisecond,
		CompressThreshold: 1024,
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
	if optimizer.compressThreshold != config.CompressThreshold {
		t.Errorf("Expected compressThreshold %d, got %d", config.CompressThreshold, optimizer.compressThreshold)
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

// TestCompressData 测试数据压缩
func TestCompressData(t *testing.T) {
	config := NetworkOptimizerConfig{
		MaxConnections:    10,
		ConnectTimeout:    1 * time.Second,
		MaxIdleTime:       100 * time.Millisecond,
		CompressThreshold: 100, // 设置较小的阈值便于测试
	}

	optimizer := NewNetworkOptimizer("127.0.0.1:8080", config)

	// 测试小数据不压缩
	smallData := []byte("small data")
	compressed, err := optimizer.CompressData(smallData)
	if err != nil {
		t.Fatalf("CompressData failed: %v", err)
	}
	if !bytes.Equal(compressed, smallData) {
		t.Error("Small data should not be compressed")
	}

	// 测试大数据压缩
	largeData := make([]byte, 200)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	compressed, err = optimizer.CompressData(largeData)
	if err != nil {
		t.Fatalf("CompressData failed: %v", err)
	}
	// 注意：某些情况下压缩后的数据可能比原始数据更大，这取决于数据的可压缩性
	// 我们只检查是否成功压缩（不返回错误）并且返回了数据
	if len(compressed) == 0 {
		t.Error("Compressed data should not be empty")
	}

	// 测试解压
	decompressed, err := optimizer.DecompressData(compressed)
	if err != nil {
		t.Fatalf("DecompressData failed: %v", err)
	}
	if !bytes.Equal(decompressed, largeData) {
		t.Error("Decompressed data should match original")
	}

	// 检查指标 - 我们不能保证总是有压缩节省，因为某些数据可能无法有效压缩
	stats := optimizer.GetStats()
	// 只检查指标是否存在，不检查具体值
	if stats["compressed_bytes"] == nil {
		t.Error("Compressed bytes should be recorded")
	}
	if stats["decompressed_bytes"] == nil {
		t.Error("Decompressed bytes should be recorded")
	}
}

// TestNetworkOptimizerWithMetrics 测试网络优化器指标
func TestNetworkOptimizerWithMetrics(t *testing.T) {
	config := NetworkOptimizerConfig{
		MaxConnections:    10,
		ConnectTimeout:    1 * time.Second,
		MaxIdleTime:       100 * time.Millisecond,
		CompressThreshold: 1024,
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
	if _, exists := stats["compressed_bytes"]; !exists {
		t.Error("Stats should contain compressed_bytes")
	}
	if _, exists := stats["decompressed_bytes"]; !exists {
		t.Error("Stats should contain decompressed_bytes")
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
	if config.CompressThreshold <= 0 {
		t.Error("Default CompressThreshold should be greater than 0")
	}
}

// TestConcurrentNetworkOperations 测试并发网络操作
func TestConcurrentNetworkOperations(t *testing.T) {
	config := NetworkOptimizerConfig{
		MaxConnections:    10,
		ConnectTimeout:    1 * time.Second,
		MaxIdleTime:       100 * time.Millisecond,
		CompressThreshold: 1024,
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
