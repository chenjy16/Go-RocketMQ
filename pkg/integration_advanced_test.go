package pkg

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go-rocketmq/pkg/broker"
	"go-rocketmq/pkg/common"
	"go-rocketmq/pkg/nameserver"
	"go-rocketmq/pkg/performance"
)

// TestAdvancedMessageFlow 测试高级消息流程
func TestAdvancedMessageFlow(t *testing.T) {
	// 初始化NameServer
	nameServerConfig := &nameserver.Config{
		ListenPort:                  9879,
		ClusterTestEnable:           true,
		OrderMessageEnable:          true,
		ScanNotActiveBrokerInterval: 2 * time.Second,
	}
	ns := nameserver.NewNameServer(nameServerConfig)
	if err := ns.Start(); err != nil {
		t.Fatalf("Failed to start nameserver: %v", err)
	}
	defer ns.Stop()

	time.Sleep(500 * time.Millisecond)

	// 初始化Broker
	brokerConfig := &broker.Config{
		BrokerName:       "AdvancedTestBroker",
		ListenPort:       10941,
		NameServerAddr:   "127.0.0.1:9879",
		StorePathRootDir: "/tmp/rocketmq_advanced_test",
	}
	br := broker.NewBroker(brokerConfig)
	if err := br.Start(); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer br.Stop()

	time.Sleep(1 * time.Second)

	// 测试批量消息处理
	t.Run("BatchMessageProcessing", func(t *testing.T) {
		testBatchMessageProcessing(t, br)
	})

	// 测试并发消息处理
	t.Run("ConcurrentMessageProcessing", func(t *testing.T) {
		testConcurrentMessageProcessing(t, br)
	})

	// 测试消息持久化
	t.Run("MessagePersistence", func(t *testing.T) {
		testMessagePersistence(t, br)
	})

	// 测试主题管理
	t.Run("TopicManagement", func(t *testing.T) {
		testTopicManagement(t, br)
	})
}

// TestPerformanceOptimization 测试性能优化功能
func TestPerformanceOptimization(t *testing.T) {
	// 初始化性能监控
	monitorConfig := performance.MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false, // 避免端口冲突
	}
	monitor := performance.NewPerformanceMonitor(monitorConfig)
	if err := monitor.Start(); err != nil {
		t.Fatalf("Failed to start performance monitor: %v", err)
	}
	defer monitor.Stop()

	// 初始化全局性能组件
	performance.InitGlobalPools()
	performance.InitGlobalBatchManager()

	// 测试内存池优化
	t.Run("MemoryPoolOptimization", func(t *testing.T) {
		testMemoryPoolOptimization(t)
	})

	// 测试批量处理优化
	t.Run("BatchProcessingOptimization", func(t *testing.T) {
		testBatchProcessingOptimization(t)
	})

	// 测试性能监控
	t.Run("PerformanceMonitoring", func(t *testing.T) {
		testPerformanceMonitoring(t, monitor)
	})
}

// TestStressScenarios 测试压力场景
func TestStressScenarios(t *testing.T) {
	// 初始化Broker
	brokerConfig := &broker.Config{
		BrokerName:       "StressBroker",
		ListenPort:       10951,
		NameServerAddr:   "127.0.0.1:9876", // 使用默认端口
		StorePathRootDir: "/tmp/rocketmq_stress_test",
	}
	br := broker.NewBroker(brokerConfig)
	if err := br.Start(); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer br.Stop()

	time.Sleep(500 * time.Millisecond)

	// 测试高并发写入
	t.Run("HighConcurrencyWrite", func(t *testing.T) {
		testHighConcurrencyWrite(t, br)
	})

	// 测试大量主题创建
	t.Run("MassiveTopicCreation", func(t *testing.T) {
		testMassiveTopicCreation(t, br)
	})

	// 测试内存压力
	t.Run("MemoryPressure", func(t *testing.T) {
		testMemoryPressure(t)
	})
}

// TestErrorRecovery 测试错误恢复场景
func TestErrorRecovery(t *testing.T) {
	// 初始化性能组件
	performance.InitGlobalPools()
	performance.InitGlobalBatchManager()

	// 测试批量处理错误恢复
	t.Run("BatchProcessingErrorRecovery", func(t *testing.T) {
		testBatchProcessingErrorRecovery(t)
	})

	// 测试内存池错误恢复
	t.Run("MemoryPoolErrorRecovery", func(t *testing.T) {
		testMemoryPoolErrorRecovery(t)
	})

	// 测试资源泄漏防护
	t.Run("ResourceLeakPrevention", func(t *testing.T) {
		testResourceLeakPrevention(t)
	})
}

// 辅助测试函数实现

func testBatchMessageProcessing(t *testing.T, br *broker.Broker) {
	topicName := "BatchTestTopic"
	if err := br.CreateTopic(topicName, 4); err != nil {
		t.Errorf("Failed to create topic: %v", err)
		return
	}

	// 批量发送消息
	batchSize := 50
	for i := 0; i < batchSize; i++ {
		msg := &common.Message{
			Topic: topicName,
			Body:  []byte(fmt.Sprintf("Batch message %d", i)),
		}
		if _, err := br.PutMessage(msg); err != nil {
			t.Errorf("Failed to put batch message %d: %v", i, err)
		}
	}

	// 验证消息存储
	for queueId := 0; queueId < 4; queueId++ {
		if _, err := br.PullMessage(topicName, int32(queueId), 0, 20); err != nil {
			t.Errorf("Failed to pull messages from queue %d: %v", queueId, err)
		}
	}

	t.Logf("Batch message processing completed with %d messages", batchSize)
}

func testConcurrentMessageProcessing(t *testing.T, br *broker.Broker) {
	topicName := "ConcurrentTestTopic"
	if err := br.CreateTopic(topicName, 8); err != nil {
		t.Errorf("Failed to create topic: %v", err)
		return
	}

	concurrency := 20
	messagesPerGoroutine := 50
	var wg sync.WaitGroup
	var successCount int64
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := &common.Message{
					Topic: topicName,
					Body:  []byte(fmt.Sprintf("Concurrent message from goroutine %d, msg %d", id, j)),
				}
				if _, err := br.PutMessage(msg); err == nil {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()

	expectedTotal := int64(concurrency * messagesPerGoroutine)
	mu.Lock()
	finalSuccessCount := successCount
	mu.Unlock()

	if finalSuccessCount < expectedTotal*8/10 { // 允许20%的失败率
		t.Errorf("Too many failures: expected at least %d successes, got %d", expectedTotal*8/10, finalSuccessCount)
	}

	t.Logf("Concurrent message processing: %d/%d messages succeeded", finalSuccessCount, expectedTotal)
}

func testMessagePersistence(t *testing.T, br *broker.Broker) {
	topicName := "PersistenceTestTopic"
	if err := br.CreateTopic(topicName, 2); err != nil {
		t.Errorf("Failed to create topic: %v", err)
		return
	}

	// 发送消息
	messageCount := 20
	for i := 0; i < messageCount; i++ {
		msg := &common.Message{
			Topic: topicName,
			Body:  []byte(fmt.Sprintf("Persistence test message %d", i)),
		}
		if _, err := br.PutMessage(msg); err != nil {
			t.Errorf("Failed to put message %d: %v", i, err)
		}
	}

	// 等待持久化
	time.Sleep(100 * time.Millisecond)

	// 验证消息可以被读取
	for queueId := 0; queueId < 2; queueId++ {
		if _, err := br.PullMessage(topicName, int32(queueId), 0, 15); err != nil {
			t.Errorf("Failed to pull persisted messages from queue %d: %v", queueId, err)
		}
	}

	t.Log("Message persistence test completed")
}

func testTopicManagement(t *testing.T, br *broker.Broker) {
	// 创建多个主题
	topicCount := 10
	for i := 0; i < topicCount; i++ {
		topicName := fmt.Sprintf("MgmtTopic_%d", i)
		queueCount := int32(2 + (i % 6)) // 2-7个队列
		if err := br.CreateTopic(topicName, queueCount); err != nil {
			t.Errorf("Failed to create topic %s: %v", topicName, err)
		}
	}

	// 向每个主题发送消息
	for i := 0; i < topicCount; i++ {
		topicName := fmt.Sprintf("MgmtTopic_%d", i)
		msg := &common.Message{
			Topic: topicName,
			Body:  []byte(fmt.Sprintf("Management test message for %s", topicName)),
		}
		if _, err := br.PutMessage(msg); err != nil {
			t.Errorf("Failed to put message to topic %s: %v", topicName, err)
		}
	}

	t.Logf("Topic management test completed with %d topics", topicCount)
}

func testMemoryPoolOptimization(t *testing.T) {
	// 测试消息池
	messageCount := 1000
	for i := 0; i < messageCount; i++ {
		msg := performance.GetMessage()
		msg.Topic = fmt.Sprintf("PoolTopic_%d", i%10)
		msg.Body = []byte(fmt.Sprintf("Pool test message %d", i))
		performance.PutMessage(msg)
	}

	// 测试缓冲区池
	bufferCount := 1000
	for i := 0; i < bufferCount; i++ {
		buf := performance.GetBuffer(512)
		for j := 0; j < len(buf); j++ {
			buf[j] = byte(j % 256)
		}
		performance.PutBuffer(buf)
	}

	t.Log("Memory pool optimization test completed")
}

func testBatchProcessingOptimization(t *testing.T) {
	config := performance.BatchConfig{
		BatchSize:     20,
		FlushInterval: 50 * time.Millisecond,
		BufferSize:    100,
	}

	var processedCount int64
	var mu sync.Mutex

	handler := performance.BatchHandlerFunc(func(items []interface{}) error {
		mu.Lock()
		processedCount += int64(len(items))
		mu.Unlock()
		return nil
	})

	processor := performance.NewBatchProcessor(config, handler)
	if err := processor.Start(); err != nil {
		t.Fatalf("Failed to start batch processor: %v", err)
	}
	defer processor.Stop()

	// 添加测试项目
	itemCount := 100
	for i := 0; i < itemCount; i++ {
		if err := processor.Add(fmt.Sprintf("optimization_item_%d", i)); err != nil {
			t.Errorf("Failed to add item %d: %v", i, err)
		}
	}

	// 等待处理完成
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	finalProcessedCount := processedCount
	mu.Unlock()

	if finalProcessedCount != int64(itemCount) {
		t.Errorf("Expected %d processed items, got %d", itemCount, finalProcessedCount)
	}

	t.Log("Batch processing optimization test completed")
}

func testPerformanceMonitoring(t *testing.T, monitor *performance.PerformanceMonitor) {
	// 更新指标
	monitor.UpdateMetrics()

	// 获取系统指标
	sysMetrics := monitor.GetSystemMetrics()
	if sysMetrics.Timestamp.IsZero() {
		t.Error("System metrics timestamp should not be zero")
	}
	if sysMetrics.Goroutines <= 0 {
		t.Error("Goroutines count should be greater than 0")
	}
	if sysMetrics.MemoryUsed <= 0 {
		t.Error("Memory used should be greater than 0")
	}

	// 获取所有指标
	allMetrics := monitor.GetAllMetrics()
	if allMetrics == nil {
		t.Error("GetAllMetrics should not return nil")
	}

	t.Log("Performance monitoring test completed")
}

func testHighConcurrencyWrite(t *testing.T, br *broker.Broker) {
	topicName := "HighConcurrencyTopic"
	if err := br.CreateTopic(topicName, 16); err != nil {
		t.Errorf("Failed to create topic: %v", err)
		return
	}

	concurrency := 50
	messagesPerGoroutine := 100
	var wg sync.WaitGroup
	var totalSuccess int64
	var mu sync.Mutex

	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			successCount := 0
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := &common.Message{
					Topic: topicName,
					Body:  []byte(fmt.Sprintf("High concurrency message %d-%d", id, j)),
				}
				if _, err := br.PutMessage(msg); err == nil {
					successCount++
				}
			}
			mu.Lock()
			totalSuccess += int64(successCount)
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	mu.Lock()
	finalSuccess := totalSuccess
	mu.Unlock()

	throughput := float64(finalSuccess) / duration.Seconds()
	t.Logf("High concurrency write: %d messages in %v, throughput: %.2f msg/s", finalSuccess, duration, throughput)

	expectedTotal := int64(concurrency * messagesPerGoroutine)
	if finalSuccess < expectedTotal*7/10 { // 允许30%的失败率
		t.Errorf("Too many failures in high concurrency test: %d/%d", finalSuccess, expectedTotal)
	}
}

func testMassiveTopicCreation(t *testing.T, br *broker.Broker) {
	topicCount := 100
	var successCount int

	for i := 0; i < topicCount; i++ {
		topicName := fmt.Sprintf("MassiveTopic_%d", i)
		if err := br.CreateTopic(topicName, 4); err == nil {
			successCount++
		}
	}

	if successCount < topicCount*8/10 { // 允许20%的失败率
		t.Errorf("Too many topic creation failures: %d/%d", successCount, topicCount)
	}

	t.Logf("Massive topic creation: %d/%d topics created successfully", successCount, topicCount)
}

func testMemoryPressure(t *testing.T) {
	// 测试内存池在压力下的表现
	bufferCount := 10000
	buffers := make([][]byte, bufferCount)

	// 获取大量缓冲区
	for i := 0; i < bufferCount; i++ {
		buffers[i] = performance.GetBuffer(1024)
	}

	// 使用缓冲区
	for i, buf := range buffers {
		for j := 0; j < len(buf); j++ {
			buf[j] = byte(i % 256)
		}
	}

	// 归还缓冲区
	for _, buf := range buffers {
		performance.PutBuffer(buf)
	}

	t.Log("Memory pressure test completed")
}

func testBatchProcessingErrorRecovery(t *testing.T) {
	config := performance.BatchConfig{
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		BufferSize:    5, // 小缓冲区容易触发错误
	}

	var errorCount int
	var mu sync.Mutex

	handler := performance.BatchHandlerFunc(func(items []interface{}) error {
		mu.Lock()
		errorCount++
		mu.Unlock()
		// 模拟处理延迟
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	processor := performance.NewBatchProcessor(config, handler)
	if err := processor.Start(); err != nil {
		t.Fatalf("Failed to start batch processor: %v", err)
	}
	defer processor.Stop()

	// 快速添加项目，可能触发缓冲区满错误
	for i := 0; i < 20; i++ {
		err := processor.Add(fmt.Sprintf("error_recovery_item_%d", i))
		if err != nil {
			t.Logf("Expected error at item %d: %v", i, err)
		}
	}

	// 等待处理完成
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	finalErrorCount := errorCount
	mu.Unlock()

	t.Logf("Batch processing error recovery: %d processing cycles completed", finalErrorCount)
}

func testMemoryPoolErrorRecovery(t *testing.T) {
	// 测试内存池的错误恢复能力
	bufferCount := 1000

	// 获取和归还大量缓冲区
	for i := 0; i < bufferCount; i++ {
		buf := performance.GetBuffer(512)
		if buf == nil {
			t.Errorf("Failed to get buffer at iteration %d", i)
			continue
		}
		// 使用缓冲区
		buf[0] = byte(i % 256)
		performance.PutBuffer(buf)
	}

	t.Log("Memory pool error recovery test completed")
}

func testResourceLeakPrevention(t *testing.T) {
	// 测试资源泄漏防护
	iterations := 1000

	for i := 0; i < iterations; i++ {
		// 获取消息对象
		msg := performance.GetMessage()
		msg.Topic = "LeakTestTopic"
		msg.Body = []byte(fmt.Sprintf("Leak test message %d", i))

		// 获取缓冲区
		buf := performance.GetBuffer(256)
		copy(buf, msg.Body)

		// 确保资源被正确归还
		performance.PutBuffer(buf)
		performance.PutMessage(msg)
	}

	t.Log("Resource leak prevention test completed")
}

// BenchmarkAdvancedIntegrationScenarios 高级集成场景基准测试
func BenchmarkAdvancedIntegrationScenarios(b *testing.B) {
	// 初始化性能组件
	performance.InitGlobalPools()
	performance.InitGlobalBatchManager()

	b.Run("FullMessageLifecycle", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// 完整的消息生命周期
				msg := performance.GetMessage()
				msg.Topic = "BenchmarkTopic"
				msg.Body = performance.GetBuffer(256)

				// 模拟消息处理
				for i := 0; i < len(msg.Body); i++ {
					msg.Body[i] = byte(i % 256)
				}

				// 归还资源
				performance.PutBuffer(msg.Body)
				performance.PutMessage(msg)
			}
		})
	})

	b.Run("BatchProcessingThroughput", func(b *testing.B) {
		config := performance.BatchConfig{
			BatchSize:     100,
			FlushInterval: 10 * time.Millisecond,
			BufferSize:    1000,
		}

		handler := performance.BatchHandlerFunc(func(items []interface{}) error {
			return nil
		})

		processor := performance.NewBatchProcessor(config, handler)
		processor.Start()
		defer processor.Stop()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			processor.Add(fmt.Sprintf("benchmark_item_%d", i))
		}
	})

	b.Run("MemoryPoolPerformance", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := performance.GetBuffer(512)
				// 模拟使用
				buf[0] = 1
				performance.PutBuffer(buf)
			}
		})
	})
}