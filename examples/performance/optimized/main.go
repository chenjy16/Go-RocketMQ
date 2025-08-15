package main

import (
	"fmt"
	"sync"
	"time"

	"go-rocketmq/pkg/performance"
)

func main() {
	fmt.Println("=== Go-RocketMQ 性能优化示例 ===")

	// 1. 初始化性能优化组件
	initPerformanceComponents()

	// 2. 运行优化的生产者示例
	runOptimizedProducer()

	// 3. 运行优化的消费者示例
	runOptimizedConsumer()

	// 4. 运行批量处理示例
	runBatchProcessingExample()

	// 5. 展示性能监控
	showPerformanceMetrics()
}

// 初始化性能优化组件
func initPerformanceComponents() {
	fmt.Println("\n1. 初始化性能优化组件...")

	// 初始化全局内存池
	performance.InitGlobalPools()
	fmt.Println("✓ 内存池已初始化")

	// 创建批量管理器
	performance.InitGlobalBatchManager()
	batchManager := performance.GetGlobalBatchManager()
	fmt.Println("✓ 批量管理器已启动")

	// 启动性能监控
	performance.InitGlobalPerformanceMonitor(performance.DefaultMonitorConfig)
	monitor := performance.GetGlobalPerformanceMonitor()
	monitor.RegisterMemoryPool(performance.GlobalMemoryPool)
	monitor.RegisterBatchManager(batchManager)
	monitor.Start()
	fmt.Println("✓ 性能监控已启动")
}

// 运行优化的生产者示例
func runOptimizedProducer() {
	fmt.Println("\n2. 运行优化的生产者示例...")

	// 发送消息
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// 使用内存池创建消息
			msg := performance.GetMessage()
			defer performance.PutMessage(msg)

			msg.Topic = "optimized_topic"
			msg.Body = []byte(fmt.Sprintf("优化消息 #%d", index))
			msg.Tags = "performance"

			// 模拟消息发送
			time.Sleep(time.Microsecond)
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	fmt.Printf("✓ 发送1000条消息耗时: %v (平均 %.2f msg/s)\n", duration, 1000.0/duration.Seconds())
}

// 运行优化的消费者示例
func runOptimizedConsumer() {
	fmt.Println("\n3. 运行优化的消费者示例...")

	// 模拟消费消息
	var processedCount int64
	var mu sync.Mutex

	// 批量处理消息
	for i := 0; i < 100; i++ {
		// 使用缓冲区处理消息
		buffer := performance.GetBuffer(1024)
		copy(buffer, []byte(fmt.Sprintf("消息内容 #%d", i)))

		// 处理消息逻辑
		_ = string(buffer)

		// 归还缓冲区
		performance.PutBuffer(buffer)

		mu.Lock()
		processedCount++
		mu.Unlock()
	}

	mu.Lock()
	count := processedCount
	mu.Unlock()
	fmt.Printf("✓ 已处理消息数量: %d\n", count)
}

// 运行批量处理示例
func runBatchProcessingExample() {
	fmt.Println("\n4. 运行批量处理示例...")

	// 创建批量处理器
	batchProcessor := performance.NewBatchProcessor(performance.BatchConfig{
		BatchSize:     50,
		FlushInterval: 100 * time.Millisecond,
		BufferSize:    1000,
	}, performance.BatchHandlerFunc(func(items []interface{}) error {
		fmt.Printf("批量处理 %d 个项目\n", len(items))
		// 模拟处理时间
		time.Sleep(10 * time.Millisecond)
		return nil
	}))

	// 启动批量处理器
	batchProcessor.Start()
	defer batchProcessor.Stop()

	// 提交任务
	startTime := time.Now()
	for i := 0; i < 500; i++ {
		batchProcessor.Add(fmt.Sprintf("任务 #%d", i))
	}

	// 等待处理完成
	time.Sleep(2 * time.Second)
	duration := time.Since(startTime)
	fmt.Printf("✓ 批量处理500个任务耗时: %v\n", duration)
}

// 展示性能监控
func showPerformanceMetrics() {
	fmt.Println("\n5. 性能监控指标...")

	// 获取内存池指标
	memoryPool := performance.GlobalMemoryPool
	if memoryPool != nil {
		memoryMetrics := memoryPool.GetMetrics()

		fmt.Printf("内存池指标:\n")
		fmt.Printf("  - 缓冲区分配次数: %d\n", memoryMetrics.BufferAllocations)
		fmt.Printf("  - 缓冲区释放次数: %d\n", memoryMetrics.BufferDeallocations)
		fmt.Printf("  - 对象分配次数: %d\n", memoryMetrics.ObjectAllocations)
		fmt.Printf("  - 对象释放次数: %d\n", memoryMetrics.ObjectDeallocations)
		fmt.Printf("  - 池命中次数: %d\n", memoryMetrics.PoolHits)
		fmt.Printf("  - 池未命中次数: %d\n", memoryMetrics.PoolMisses)
	}

	// 获取系统指标
	monitor := performance.GetGlobalPerformanceMonitor()
	if monitor != nil {
		systemMetrics := monitor.GetSystemMetrics()
		fmt.Printf("\n系统指标:\n")
		fmt.Printf("  - CPU使用率: %.2f%%\n", systemMetrics.CPUUsage)
		fmt.Printf("  - 内存使用量: %d bytes\n", systemMetrics.MemoryUsed)
		fmt.Printf("  - Goroutines数量: %d\n", systemMetrics.Goroutines)
		fmt.Printf("  - GC次数: %d\n", systemMetrics.GCCount)
	}

	fmt.Println("\n=== 性能优化示例完成 ===")
}