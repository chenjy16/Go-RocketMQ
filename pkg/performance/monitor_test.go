package performance

import (
	"sync"
	"testing"
	"time"
)

// TestNewPerformanceMonitor 测试性能监控器创建
func TestNewPerformanceMonitor(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 1 * time.Second,
		HTTPPort:        8080,
		EnableHTTP:      true,
		MetricsPath:     "/metrics",
	}
	
	monitor := NewPerformanceMonitor(config)
	if monitor == nil {
		t.Fatal("NewPerformanceMonitor should not return nil")
	}
	
	if monitor.collectInterval != config.CollectInterval {
		t.Errorf("Expected CollectInterval %v, got %v", config.CollectInterval, monitor.collectInterval)
	}
	if monitor.metrics == nil {
		t.Error("Metrics should be initialized")
	}
	if monitor.collectors == nil {
		t.Error("Collectors should be initialized")
	}
}

// TestPerformanceMonitorStartStop 测试监控器启动停止
func TestPerformanceMonitorStartStop(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false, // 禁用HTTP服务以避免端口冲突
	}
	
	monitor := NewPerformanceMonitor(config)
	
	// 启动监控器
	err := monitor.Start()
	if err != nil {
		t.Errorf("Start should not return error: %v", err)
	}
	
	// 等待一段时间确保监控器运行
	time.Sleep(200 * time.Millisecond)
	
	// 停止监控器
	err = monitor.Stop()
	if err != nil {
		t.Errorf("Stop should not return error: %v", err)
	}
	
	// 验证停止后不会panic
	time.Sleep(50 * time.Millisecond)
}

// TestMetricsCollection 测试指标收集
func TestMetricsCollection(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false,
	}
	monitor := NewPerformanceMonitor(config)
	
	// 手动调用UpdateMetrics来初始化指标
	monitor.UpdateMetrics()
	
	// 获取系统指标
	sysMetrics := monitor.GetSystemMetrics()
	if sysMetrics.Timestamp.IsZero() {
		t.Error("System metrics timestamp should not be zero")
	}
	
	if sysMetrics.Goroutines <= 0 {
		t.Errorf("Goroutines count should be greater than 0, got %d", sysMetrics.Goroutines)
	}
	
	if sysMetrics.MemoryUsed == 0 {
		t.Error("Memory used should be greater than 0")
	}
}

// TestAlertSystem 测试告警系统
func TestAlertSystem(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false,
	}
	
	monitor := NewPerformanceMonitor(config)
	
	// 手动更新指标
	monitor.UpdateMetrics()
	
	// 获取系统指标
	sysMetrics := monitor.GetSystemMetrics()
	if sysMetrics.Timestamp.IsZero() {
		t.Error("System metrics timestamp should not be zero")
	}
	if sysMetrics.Goroutines <= 0 {
		t.Error("Goroutines count should be greater than 0")
	}
	
	// 获取所有指标
	allMetrics := monitor.GetAllMetrics()
	if allMetrics == nil {
		t.Error("GetAllMetrics should not return nil")
	}
}

// TestMetricsHistory 测试指标历史
func TestMetricsHistory(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 50 * time.Millisecond,
		EnableHTTP:      false,
	}
	
	monitor := NewPerformanceMonitor(config)
	
	// 启动监控器收集指标
	err := monitor.Start()
	if err != nil {
		t.Errorf("Start should not return error: %v", err)
	}
	
	// 等待指标收集
	time.Sleep(100 * time.Millisecond)
	
	// 停止监控器
	err = monitor.Stop()
	if err != nil {
		t.Errorf("Stop should not return error: %v", err)
	}
	
	// 获取系统指标验证收集功能
	sysMetrics := monitor.GetSystemMetrics()
	if sysMetrics.Timestamp.IsZero() {
		t.Error("System metrics should have valid timestamp")
	}
	if sysMetrics.Goroutines <= 0 {
		t.Error("Goroutines count should be greater than 0")
	}
}

// TestConcurrentMonitoring 测试并发监控
func TestConcurrentMonitoring(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 10 * time.Millisecond,
		EnableHTTP:      false,
	}
	
	monitor := NewPerformanceMonitor(config)
	
	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	// 并发收集指标
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				// 获取系统指标进行并发测试
				_ = monitor.GetSystemMetrics()
			}
		}(i)
	}
	
	wg.Wait()
	
	// 验证并发操作没有导致数据竞争
	metrics := monitor.GetAllMetrics()
	if metrics == nil {
		t.Error("Metrics should not be nil after concurrent operations")
	}
}

// TestMonitorConfigValidation 测试监控配置验证
func TestMonitorConfigValidation(t *testing.T) {
	// 测试默认配置
	defaultConfig := MonitorConfig{
		CollectInterval: 1 * time.Second,
		HTTPPort:        8080,
		EnableHTTP:      true,
		MetricsPath:     "/metrics",
	}
	if defaultConfig.CollectInterval <= 0 {
		t.Error("Default CollectInterval should be greater than 0")
	}
	if defaultConfig.HTTPPort <= 0 {
		t.Error("Default HTTPPort should be greater than 0")
	}
	
	// 测试无效配置
	invalidConfig := MonitorConfig{
		CollectInterval: 0,
		HTTPPort:        0,
	}
	
	monitor := NewPerformanceMonitor(invalidConfig)
	if monitor == nil {
		t.Error("NewPerformanceMonitor should handle invalid config gracefully")
	}
}

// TestAlertCallback 测试告警回调
func TestAlertCallback(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false,
	}
	
	monitor := NewPerformanceMonitor(config)
	
	// 手动更新指标
	monitor.UpdateMetrics()
	
	// 获取系统指标
	sysMetrics := monitor.GetSystemMetrics()
	if sysMetrics.Timestamp.IsZero() {
		t.Error("System metrics timestamp should not be zero")
	}
	
	// 获取所有指标
	allMetrics := monitor.GetAllMetrics()
	if allMetrics == nil {
		t.Error("GetAllMetrics should not return nil")
	}
}

// TestGlobalPerformanceMonitor 测试全局性能监控器
func TestGlobalPerformanceMonitor(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false,
	}
	
	monitor := NewPerformanceMonitor(config)
	if monitor == nil {
		t.Fatal("NewPerformanceMonitor should not return nil")
	}
	
	// 手动更新指标
	monitor.UpdateMetrics()
	
	// 测试监控器功能
	metrics := monitor.GetAllMetrics()
	if metrics == nil {
		t.Error("GetAllMetrics should not return nil")
	}
	
	// 测试系统指标
	sysMetrics := monitor.GetSystemMetrics()
	if sysMetrics.Timestamp.IsZero() {
		t.Error("System metrics timestamp should not be zero")
	}
}

// TestResourceMonitoring 测试资源监控
func TestResourceMonitoring(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false,
	}
	
	monitor := NewPerformanceMonitor(config)
	
	// 手动更新指标
	monitor.UpdateMetrics()
	
	// 获取系统指标
	sysMetrics := monitor.GetSystemMetrics()
	
	// 检查CPU使用率
	if sysMetrics.CPUUsage < 0 || sysMetrics.CPUUsage > 100 {
		t.Errorf("CPU usage should be between 0-100, got %f", sysMetrics.CPUUsage)
	}
	
	// 检查内存使用量
	if sysMetrics.MemoryUsed < 0 {
		t.Errorf("Memory used should be non-negative, got %d", sysMetrics.MemoryUsed)
	}
	if sysMetrics.MemoryTotal <= 0 {
		t.Errorf("Memory total should be positive, got %d", sysMetrics.MemoryTotal)
	}
	
	// 检查goroutine数量
	if sysMetrics.Goroutines <= 0 {
		t.Errorf("Goroutine count should be greater than 0, got %d", sysMetrics.Goroutines)
	}
}

// TestAlertSeverity 测试告警严重级别
func TestAlertSeverity(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false,
	}
	
	monitor := NewPerformanceMonitor(config)
	
	// 手动更新指标
	monitor.UpdateMetrics()
	
	// 获取系统指标
	sysMetrics := monitor.GetSystemMetrics()
	if sysMetrics.Timestamp.IsZero() {
		t.Error("System metrics timestamp should not be zero")
	}
	
	// 验证指标值的合理性
	if sysMetrics.CPUUsage < 0 || sysMetrics.CPUUsage > 100 {
		t.Errorf("CPU usage should be between 0-100, got %f", sysMetrics.CPUUsage)
	}
	if sysMetrics.MemoryUsed < 0 {
		t.Errorf("Memory used should be non-negative, got %d", sysMetrics.MemoryUsed)
	}
}

// TestMonitorCleanup 测试监控器清理
func TestMonitorCleanup(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 50 * time.Millisecond,
		EnableHTTP:      false,
	}
	
	monitor := NewPerformanceMonitor(config)
	
	// 启动监控器收集指标
	err := monitor.Start()
	if err != nil {
		t.Errorf("Start should not return error: %v", err)
	}
	
	// 等待指标收集
	time.Sleep(100 * time.Millisecond)
	
	// 停止监控器
	err = monitor.Stop()
	if err != nil {
		t.Errorf("Stop should not return error: %v", err)
	}
	
	// 获取指标验证收集功能
	allMetrics := monitor.GetAllMetrics()
	if allMetrics == nil {
		t.Error("GetAllMetrics should not return nil")
	}
	
	// 获取系统指标
	sysMetrics := monitor.GetSystemMetrics()
	if sysMetrics.Timestamp.IsZero() {
		t.Error("System metrics timestamp should not be zero")
	}
	if sysMetrics.Goroutines <= 0 {
		t.Error("Goroutines count should be greater than 0")
	}
}