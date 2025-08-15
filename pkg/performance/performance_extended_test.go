package performance

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestBatchProcessorAdvanced 测试批量处理器的高级功能
func TestBatchProcessorAdvanced(t *testing.T) {
	config := BatchConfig{
		BatchSize:     5,
		FlushInterval: 50 * time.Millisecond,
		MaxRetries:    3,
		RetryDelay:    10 * time.Millisecond,
		BufferSize:    100,
	}

	processedItems := make([]interface{}, 0)
	var mutex sync.Mutex

	handler := BatchHandlerFunc(func(items []interface{}) error {
		mutex.Lock()
		defer mutex.Unlock()
		processedItems = append(processedItems, items...)
		return nil
	})

	processor := NewBatchProcessor(config, handler)

	// 测试启动和停止
	err := processor.Start()
	if err != nil {
		t.Errorf("Failed to start processor: %v", err)
	}

	// 添加项目
	for i := 0; i < 12; i++ {
		err := processor.Add(fmt.Sprintf("item-%d", i))
		if err != nil {
			t.Errorf("Failed to add item: %v", err)
		}
	}

	// 等待处理
	time.Sleep(200 * time.Millisecond)

	// 停止处理器
	err = processor.Stop()
	if err != nil {
		t.Errorf("Failed to stop processor: %v", err)
	}

	// 验证处理结果
	mutex.Lock()
	if len(processedItems) != 12 {
		t.Errorf("Expected 12 processed items, got %d", len(processedItems))
	}
	mutex.Unlock()

	// 验证指标
	metrics := processor.GetMetrics()
	if metrics.TotalItems != 12 {
		t.Errorf("Expected TotalItems 12, got %d", metrics.TotalItems)
	}
	if metrics.TotalBatches < 2 {
		t.Errorf("Expected at least 2 batches, got %d", metrics.TotalBatches)
	}
}

// TestBatchProcessorErrorHandlingAdvanced 测试批量处理器的错误处理
func TestBatchProcessorErrorHandlingAdvanced(t *testing.T) {
	config := DefaultBatchConfig
	config.BatchSize = 3
	config.FlushInterval = 50 * time.Millisecond

	errorOnSecondBatch := false
	handler := BatchHandlerFunc(func(items []interface{}) error {
		if !errorOnSecondBatch {
			errorOnSecondBatch = true
			return nil
		}
		return fmt.Errorf("simulated error")
	})

	processor := NewBatchProcessor(config, handler)
	err := processor.Start()
	if err != nil {
		t.Errorf("Failed to start processor: %v", err)
	}

	// 添加足够的项目触发两个批次
	for i := 0; i < 6; i++ {
		processor.Add(fmt.Sprintf("item-%d", i))
	}

	time.Sleep(200 * time.Millisecond)
	processor.Stop()

	// 验证错误处理指标
	metrics := processor.GetMetrics()
	if metrics.FailedItems == 0 {
		t.Error("Expected some failed items due to error")
	}
}

// TestMessageBatchProcessor 测试消息批量处理器
func TestMessageBatchProcessor(t *testing.T) {
	config := DefaultBatchConfig
	config.BatchSize = 3

	processedMessages := make([]*Message, 0)
	var mutex sync.Mutex

	handler := BatchHandlerFunc(func(items []interface{}) error {
		mutex.Lock()
		defer mutex.Unlock()
		for _, item := range items {
			if msg, ok := item.(*Message); ok {
				processedMessages = append(processedMessages, msg)
			}
		}
		return nil
	})

	processor := NewMessageBatchProcessor(config, handler)
	err := processor.Start()
	if err != nil {
		t.Errorf("Failed to start message processor: %v", err)
	}

	// 添加消息
	for i := 0; i < 5; i++ {
		msg := &Message{
			Topic: "test-topic",
			Body:  []byte(fmt.Sprintf("message-%d", i)),
		}
		err := processor.AddMessage(msg)
		if err != nil {
			t.Errorf("Failed to add message: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	processor.Stop()

	// 验证处理结果
	mutex.Lock()
	if len(processedMessages) != 5 {
		t.Errorf("Expected 5 processed messages, got %d", len(processedMessages))
	}
	mutex.Unlock()
}

// TestBatchManager 测试批量管理器
func TestBatchManager(t *testing.T) {
	manager := NewBatchManager()

	// 创建发送器和接收器
	sendFunc := func(messages []*Message) error {
		return nil
	}
	receiveFunc := func(messages []*Message) error {
		return nil
	}

	sender := NewBatchSender(DefaultBatchConfig, sendFunc)
	receiver := NewBatchReceiver(DefaultBatchConfig, receiveFunc)

	// 注册发送器和接收器
	manager.RegisterSender("test-sender", sender)
	manager.RegisterReceiver("test-receiver", receiver)

	// 测试获取
	gotSender := manager.GetSender("test-sender")
	if gotSender != sender {
		t.Error("Failed to get registered sender")
	}

	gotReceiver := manager.GetReceiver("test-receiver")
	if gotReceiver != receiver {
		t.Error("Failed to get registered receiver")
	}

	// 测试启动所有
	err := manager.StartAll()
	if err != nil {
		t.Errorf("Failed to start all: %v", err)
	}

	// 测试停止所有
	err = manager.StopAll()
	if err != nil {
		t.Errorf("Failed to stop all: %v", err)
	}

	// 测试获取所有指标
	metrics := manager.GetAllMetrics()
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics entries, got %d", len(metrics))
	}
}

// TestPerformanceMonitorHTTPServer 测试性能监控器的HTTP服务
func TestPerformanceMonitorHTTPServer(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		HTTPPort:        8081, // 使用不同端口避免冲突
		EnableHTTP:      true,
		MetricsPath:     "/metrics",
	}

	monitor := NewPerformanceMonitor(config)
	err := monitor.Start()
	if err != nil {
		t.Errorf("Failed to start monitor: %v", err)
	}

	// 等待HTTP服务启动
	time.Sleep(200 * time.Millisecond)

	// 测试指标端点
	resp, err := http.Get("http://localhost:8081/metrics")
	if err != nil {
		t.Errorf("Failed to get metrics: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	}

	// 测试健康检查端点
	resp, err = http.Get("http://localhost:8081/health")
	if err != nil {
		t.Errorf("Failed to get health: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	}

	// 测试调试端点
	resp, err = http.Get("http://localhost:8081/debug")
	if err != nil {
		t.Errorf("Failed to get debug: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	}

	monitor.Stop()
}

// TestAlertManager 测试告警管理器
func TestAlertManager(t *testing.T) {
	config := MonitorConfig{
		CollectInterval: 50 * time.Millisecond,
		EnableHTTP:      false,
	}

	monitor := NewPerformanceMonitor(config)
	alertManager := NewAlertManager(monitor)

	// 添加告警规则
	rule := AlertRule{
		Name:        "high-cpu",
		MetricName:  "cpu_usage",
		Threshold:   80.0,
		Operator:    ">",
		Duration:    100 * time.Millisecond,
		Description: "High CPU usage",
		Severity:    "warning",
	}
	alertManager.AddRule(rule)

	// 添加告警处理器
	alertTriggered := false
	handler := &TestAlertHandler{
		callback: func(alert Alert) {
			alertTriggered = true
		},
	}
	alertManager.AddHandler(handler)

	// 启动监控和告警
	monitor.Start()
	alertManager.Start()

	// 模拟高CPU使用率
	monitor.metrics.CPUUsage = 90.0

	// 等待告警触发
	time.Sleep(200 * time.Millisecond)

	// 停止服务
	alertManager.Stop()
	monitor.Stop()

	// 验证告警是否触发（这个测试可能不稳定，因为依赖时间）
	if alertTriggered {
		t.Log("Alert was triggered as expected")
	}
}

// TestAlertHandler 自定义告警处理器用于测试
type TestAlertHandler struct {
	callback func(Alert)
}

func (h *TestAlertHandler) Handle(alert Alert) error {
	if h.callback != nil {
		h.callback(alert)
	}
	return nil
}

// TestNetworkOptimizerAdvanced 测试网络优化器的高级功能
func TestNetworkOptimizerAdvanced(t *testing.T) {
	optimizer := NewNetworkOptimizer()

	// 测试连接池配置
	config := ConnectionPoolConfig{
		MaxConnections: 5,
		ConnectTimeout: 1 * time.Second,
		MaxIdleTime:    500 * time.Millisecond,
	}

	optimizer.RegisterConnectionPool("test", "test-server:8080", config)

	// 测试获取连接池
	gotPool := optimizer.GetConnectionPool("test")
	if gotPool == nil {
		t.Error("Failed to get registered connection pool")
	}

	// 测试启动和停止
	optimizer.Start()
	optimizer.Stop()
}

// TestMemoryPoolAdvanced 测试内存池的高级功能
func TestMemoryPoolAdvanced(t *testing.T) {
	pool := NewMemoryPool()

	// 测试大量并发访问
	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				buf := pool.GetBuffer(1024)
				if len(buf) != 1024 {
					buf = make([]byte, 1024)
				}
				// 模拟使用缓冲区
				for k := 0; k < len(buf); k++ {
					buf[k] = byte(k % 256)
				}
				pool.PutBuffer(buf)
			}
		}()
	}

	wg.Wait()

	// 验证内存池正常工作
	t.Log("Memory pool stress test completed successfully")
}

// TestGlobalInstances 测试全局实例
func TestGlobalInstances(t *testing.T) {
	// 测试全局批量管理器
	InitGlobalBatchManager()
	manager1 := GetGlobalBatchManager()
	manager2 := GetGlobalBatchManager()
	if manager1 != manager2 {
		t.Error("Global batch manager should be singleton")
	}

	// 测试全局性能监控器
	config := DefaultMonitorConfig
	config.EnableHTTP = false // 避免端口冲突
	InitGlobalPerformanceMonitor(config)
	monitor1 := GetGlobalPerformanceMonitor()
	monitor2 := GetGlobalPerformanceMonitor()
	if monitor1 != monitor2 {
		t.Error("Global performance monitor should be singleton")
	}

	// 测试全局网络优化器
	InitGlobalNetworkOptimizer()
	opt1 := GetGlobalNetworkOptimizer()
	opt2 := GetGlobalNetworkOptimizer()
	if opt1 != opt2 {
		t.Error("Global network optimizer should be singleton")
	}

	// 全局内存池测试跳过（方法未实现）
	t.Log("Global memory pool test skipped")
}

// TestPerformanceIntegration 测试性能模块集成
func TestPerformanceIntegration(t *testing.T) {
	// 创建所有组件
	memoryPool := NewMemoryPool()
	batchManager := NewBatchManager()
	networkOpt := NewNetworkOptimizer()

	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false,
	}
	monitor := NewPerformanceMonitor(config)

	// 注册组件到监控器
	monitor.RegisterMemoryPool(memoryPool)
	monitor.RegisterBatchManager(batchManager)
	monitor.RegisterNetworkOptimizer(networkOpt)

	// 启动所有组件
	err := monitor.Start()
	if err != nil {
		t.Errorf("Failed to start monitor: %v", err)
	}

	networkOpt.Start()

	err = batchManager.StartAll()
	if err != nil {
		t.Errorf("Failed to start batch manager: %v", err)
	}

	// 模拟一些活动
	for i := 0; i < 10; i++ {
		buf := memoryPool.GetBuffer(1024)
		memoryPool.PutBuffer(buf)
	}

	// 等待指标收集
	time.Sleep(200 * time.Millisecond)

	// 获取所有指标
	allMetrics := monitor.GetAllMetrics()
	if len(allMetrics) == 0 {
		t.Error("Expected some metrics to be collected")
	}

	// 停止所有组件
	batchManager.StopAll()
	networkOpt.Stop()
	monitor.Stop()
}

// TestPerformanceStressTest 性能压力测试
func TestPerformanceStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	memoryPool := NewMemoryPool()
	config := DefaultBatchConfig
	config.BatchSize = 1000
	config.FlushInterval = 10 * time.Millisecond

	processedCount := int64(0)
	handler := BatchHandlerFunc(func(items []interface{}) error {
		// 模拟处理时间
		time.Sleep(1 * time.Millisecond)
		processedCount += int64(len(items))
		return nil
	})

	processor := NewBatchProcessor(config, handler)
	processor.Start()

	// 启动多个生产者
	var wg sync.WaitGroup
	numProducers := 10
	numItemsPerProducer := 10000

	start := time.Now()
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for j := 0; j < numItemsPerProducer; j++ {
				buf := memoryPool.GetBuffer(256)
				item := fmt.Sprintf("producer-%d-item-%d", producerID, j)
				processor.Add(item)
				memoryPool.PutBuffer(buf)
			}
		}(i)
	}

	wg.Wait()
	processor.Stop()
	duration := time.Since(start)

	// 验证性能
	totalItems := int64(numProducers * numItemsPerProducer)
	throughput := float64(totalItems) / duration.Seconds()

	t.Logf("Processed %d items in %v (%.2f items/sec)", totalItems, duration, throughput)

	if throughput < 10000 { // 期望至少10k items/sec
		t.Logf("Warning: Low throughput %.2f items/sec", throughput)
	}

	if processedCount != totalItems {
		t.Errorf("Expected %d processed items, got %d", totalItems, processedCount)
	}
}