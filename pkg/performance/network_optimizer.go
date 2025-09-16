package performance

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"sync"
	"time"

	"github.com/chenjy16/go-rocketmq-remoting/command"
	"github.com/chenjy16/go-rocketmq-remoting/connection"
)

// NetworkMetrics 网络指标
type NetworkMetrics struct {
	TotalConnections     int64         // 总连接数
	ActiveConnections    int64         // 活跃连接数
	IdleConnections      int64         // 空闲连接数
	ConnectionsCreated   int64         // 创建的连接数
	ConnectionsDestroyed int64         // 销毁的连接数
	BytesRead            int64         // 读取字节数
	BytesWritten         int64         // 写入字节数
	CompressedBytes      int64         // 压缩字节数
	DecompressedBytes    int64         // 解压字节数
	AvgLatency           time.Duration // 平均延迟
	ErrorCount           int64         // 错误计数
	mutex                sync.RWMutex
}

// NetworkOptimizer 网络优化器
type NetworkOptimizer struct {
	address string
	config  *connection.ConnectionPoolConfig
	pool    *connection.ConnectionPool
	metrics *NetworkMetrics
	// 压缩配置
	compressThreshold int // 压缩阈值（字节）
}

// NetworkOptimizerConfig 网络优化器配置
type NetworkOptimizerConfig struct {
	MaxConnections    int           // 最大连接数
	MaxIdleTime       time.Duration // 最大空闲时间
	ConnectTimeout    time.Duration // 连接超时
	RequestTimeout    time.Duration // 请求超时
	CompressThreshold int           // 压缩阈值（字节）
}

// DefaultNetworkOptimizerConfig 默认网络优化器配置
var DefaultNetworkOptimizerConfig = NetworkOptimizerConfig{
	MaxConnections:    100,
	MaxIdleTime:       30 * time.Minute,
	ConnectTimeout:    5 * time.Second,
	RequestTimeout:    30 * time.Second,
	CompressThreshold: 4096, // 默认4KB以上数据进行压缩
}

// NewNetworkOptimizer 创建网络优化器
func NewNetworkOptimizer(address string, config NetworkOptimizerConfig) *NetworkOptimizer {
	// Convert NetworkOptimizerConfig to ConnectionPoolConfig
	poolConfig := &connection.ConnectionPoolConfig{
		MaxConnections: int32(config.MaxConnections),
		MaxIdleTime:    config.MaxIdleTime,
		ConnectTimeout: config.ConnectTimeout,
		RequestTimeout: config.RequestTimeout,
	}

	pool := connection.NewConnectionPool(poolConfig)

	return &NetworkOptimizer{
		address:           address,
		config:            poolConfig,
		pool:              pool,
		metrics:           &NetworkMetrics{},
		compressThreshold: config.CompressThreshold,
	}
}

// GetConnection 获取连接
func (no *NetworkOptimizer) GetConnection() (*command.Connection, error) {
	return no.pool.GetConnection(no.address)
}

// ReturnConnection 归还连接
func (no *NetworkOptimizer) ReturnConnection(conn *command.Connection) {
	no.pool.ReturnConnection(conn)
}

// Close 关闭网络优化器
func (no *NetworkOptimizer) Close() error {
	return no.pool.Close()
}

// CompressData 压缩数据
func (no *NetworkOptimizer) CompressData(data []byte) ([]byte, error) {
	// 如果数据小于阈值，不进行压缩
	if len(data) < no.compressThreshold {
		return data, nil
	}

	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	compressedData := buf.Bytes()

	// 更新指标
	no.metrics.mutex.Lock()
	no.metrics.CompressedBytes += int64(len(data) - len(compressedData))
	no.metrics.mutex.Unlock()

	return compressedData, nil
}

// DecompressData 解压数据
func (no *NetworkOptimizer) DecompressData(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	decompressedData, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// 更新指标
	no.metrics.mutex.Lock()
	no.metrics.DecompressedBytes += int64(len(decompressedData) - len(data))
	no.metrics.mutex.Unlock()

	return decompressedData, nil
}

// GetStats 获取网络统计信息
func (no *NetworkOptimizer) GetStats() map[string]interface{} {
	poolStats := no.pool.GetStats()

	// Merge with our metrics
	stats := make(map[string]interface{})
	for k, v := range poolStats {
		stats[k] = v
	}

	no.metrics.mutex.RLock()
	stats["bytes_read"] = no.metrics.BytesRead
	stats["bytes_written"] = no.metrics.BytesWritten
	stats["compressed_bytes"] = no.metrics.CompressedBytes
	stats["decompressed_bytes"] = no.metrics.DecompressedBytes
	stats["avg_latency_ms"] = no.metrics.AvgLatency.Milliseconds()
	stats["error_count"] = no.metrics.ErrorCount
	no.metrics.mutex.RUnlock()

	return stats
}

// Network metrics methods
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
	nm.mutex.Unlock()
}

// Global variables
var (
	GlobalNetworkOptimizer *NetworkOptimizer
	networkOnce            sync.Once
)

// InitGlobalNetworkOptimizer 初始化全局网络优化器
func InitGlobalNetworkOptimizer() {
	networkOnce.Do(func() {
		config := DefaultNetworkOptimizerConfig
		GlobalNetworkOptimizer = NewNetworkOptimizer("localhost:9876", config) // Default address
	})
}

// GetGlobalNetworkOptimizer 获取全局网络优化器
func GetGlobalNetworkOptimizer() *NetworkOptimizer {
	InitGlobalNetworkOptimizer()
	return GlobalNetworkOptimizer
}
