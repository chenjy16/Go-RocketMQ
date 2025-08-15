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

// BenchmarkSystemThroughput 系统吞吐量基准测试
func BenchmarkSystemThroughput(b *testing.B) {
	// 初始化性能组件
	performance.InitGlobalPools()
	performance.InitGlobalBatchManager()

	b.Run("MessageProduceThroughput", func(b *testing.B) {
		// 初始化Broker
		brokerConfig := &broker.Config{
			BrokerName:       "ThroughputBroker",
			ListenPort:       10961,
			NameServerAddr:   "127.0.0.1:9876",
			StorePathRootDir: "/tmp/rocketmq_throughput_test",
		}
		br := broker.NewBroker(brokerConfig)
		if err := br.Start(); err != nil {
			b.Fatalf("Failed to start broker: %v", err)
		}
		defer br.Stop()

		// 创建测试主题
		topicName := "ThroughputTestTopic"
		if err := br.CreateTopic(topicName, 8); err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				msg := &common.Message{
					Topic: topicName,
					Body:  []byte("Throughput test message"),
				}
				br.PutMessage(msg)
			}
		})
	})

	b.Run("MessageConsumeThroughput", func(b *testing.B) {
		// 初始化Broker
		brokerConfig := &broker.Config{
			BrokerName:       "ConsumeBroker",
			ListenPort:       10962,
			NameServerAddr:   "127.0.0.1:9876",
			StorePathRootDir: "/tmp/rocketmq_consume_test",
		}
		br := broker.NewBroker(brokerConfig)
		if err := br.Start(); err != nil {
			b.Fatalf("Failed to start broker: %v", err)
		}
		defer br.Stop()

		// 创建测试主题并预填充消息
		topicName := "ConsumeTestTopic"
		if err := br.CreateTopic(topicName, 4); err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}

		// 预填充消息
		for i := 0; i < 1000; i++ {
			msg := &common.Message{
				Topic: topicName,
				Body:  []byte(fmt.Sprintf("Pre-filled message %d", i)),
			}
			br.PutMessage(msg)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			queueId := int32(i % 4)
			br.PullMessage(topicName, queueId, 0, 10)
		}
	})

	b.Run("ConcurrentProduceConsume", func(b *testing.B) {
		// 初始化Broker
		brokerConfig := &broker.Config{
			BrokerName:       "ConcurrentBroker",
			ListenPort:       10963,
			NameServerAddr:   "127.0.0.1:9876",
			StorePathRootDir: "/tmp/rocketmq_concurrent_test",
		}
		br := broker.NewBroker(brokerConfig)
		if err := br.Start(); err != nil {
			b.Fatalf("Failed to start broker: %v", err)
		}
		defer br.Stop()

		// 创建测试主题
		topicName := "ConcurrentTestTopic"
		if err := br.CreateTopic(topicName, 8); err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// 50% 生产，50% 消费
				if pb.Next() {
					// 生产消息
					msg := &common.Message{
						Topic: topicName,
						Body:  []byte("Concurrent test message"),
					}
					br.PutMessage(msg)
				} else {
					// 消费消息
					queueId := int32(0)
					br.PullMessage(topicName, queueId, 0, 5)
				}
			}
		})
	})
}

// BenchmarkLatencyPerformance 延迟性能基准测试
func BenchmarkLatencyPerformance(b *testing.B) {
	performance.InitGlobalPools()
	performance.InitGlobalBatchManager()

	b.Run("MessageLatency", func(b *testing.B) {
		// 初始化Broker
		brokerConfig := &broker.Config{
			BrokerName:       "LatencyBroker",
			ListenPort:       10964,
			NameServerAddr:   "127.0.0.1:9876",
			StorePathRootDir: "/tmp/rocketmq_latency_test",
		}
		br := broker.NewBroker(brokerConfig)
		if err := br.Start(); err != nil {
			b.Fatalf("Failed to start broker: %v", err)
		}
		defer br.Stop()

		// 创建测试主题
		topicName := "LatencyTestTopic"
		if err := br.CreateTopic(topicName, 4); err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}

		var totalLatency time.Duration
		var latencyCount int64
		var mu sync.Mutex

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				start := time.Now()
				msg := &common.Message{
					Topic: topicName,
					Body:  []byte("Latency test message"),
				}
				br.PutMessage(msg)
				latency := time.Since(start)

				mu.Lock()
				totalLatency += latency
				latencyCount++
				mu.Unlock()
			}
		})

		mu.Lock()
		avgLatency := totalLatency / time.Duration(latencyCount)
		mu.Unlock()

		b.Logf("Average message latency: %v", avgLatency)
	})

	b.Run("BatchProcessingLatency", func(b *testing.B) {
		config := performance.BatchConfig{
			BatchSize:     50,
			FlushInterval: 10 * time.Millisecond,
			BufferSize:    1000,
		}

		var totalLatency time.Duration
		var latencyCount int64
		var mu sync.Mutex

		handler := performance.BatchHandlerFunc(func(items []interface{}) error {
			mu.Lock()
			latencyCount += int64(len(items))
			mu.Unlock()
			return nil
		})

		processor := performance.NewBatchProcessor(config, handler)
		processor.Start()
		defer processor.Stop()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			processor.Add(fmt.Sprintf("latency_item_%d", i))
			latency := time.Since(start)

			mu.Lock()
			totalLatency += latency
			mu.Unlock()
		}

		mu.Lock()
		avgLatency := totalLatency / time.Duration(b.N)
		mu.Unlock()

		b.Logf("Average batch processing latency: %v", avgLatency)
	})
}

// BenchmarkMemoryPerformance 内存性能基准测试
func BenchmarkMemoryPerformance(b *testing.B) {
	performance.InitGlobalPools()

	b.Run("MessagePoolPerformance", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				msg := performance.GetMessage()
				msg.Topic = "BenchmarkTopic"
				msg.Body = []byte("Benchmark message")
				performance.PutMessage(msg)
			}
		})
	})

	b.Run("BufferPoolPerformance", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := performance.GetBuffer(1024)
				// 模拟使用
				for i := 0; i < len(buf) && i < 100; i++ {
					buf[i] = byte(i % 256)
				}
				performance.PutBuffer(buf)
			}
		})
	})

	b.Run("MemoryAllocationComparison", func(b *testing.B) {
		b.Run("WithPool", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buf := performance.GetBuffer(512)
					performance.PutBuffer(buf)
				}
			})
		})

		b.Run("WithoutPool", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = make([]byte, 512)
				}
			})
		})
	})
}

// BenchmarkConcurrencyPerformance 并发性能基准测试
func BenchmarkConcurrencyPerformance(b *testing.B) {
	performance.InitGlobalPools()
	performance.InitGlobalBatchManager()

	b.Run("HighConcurrencyMessageProcessing", func(b *testing.B) {
		// 初始化Broker
		brokerConfig := &broker.Config{
			BrokerName:       "ConcurrencyBroker",
			ListenPort:       10965,
			NameServerAddr:   "127.0.0.1:9876",
			StorePathRootDir: "/tmp/rocketmq_concurrency_test",
		}
		br := broker.NewBroker(brokerConfig)
		if err := br.Start(); err != nil {
			b.Fatalf("Failed to start broker: %v", err)
		}
		defer br.Stop()

		// 创建测试主题
		topicName := "ConcurrencyTestTopic"
		if err := br.CreateTopic(topicName, 16); err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}

		b.SetParallelism(100) // 设置高并发度
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				msg := &common.Message{
					Topic: topicName,
					Body:  []byte("High concurrency message"),
				}
				br.PutMessage(msg)
			}
		})
	})

	b.Run("ConcurrentTopicOperations", func(b *testing.B) {
		// 初始化Broker
		brokerConfig := &broker.Config{
			BrokerName:       "TopicOpsBroker",
			ListenPort:       10966,
			NameServerAddr:   "127.0.0.1:9876",
			StorePathRootDir: "/tmp/rocketmq_topic_ops_test",
		}
		br := broker.NewBroker(brokerConfig)
		if err := br.Start(); err != nil {
			b.Fatalf("Failed to start broker: %v", err)
		}
		defer br.Stop()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				topicName := fmt.Sprintf("ConcurrentTopic_%d", i%100)
				br.CreateTopic(topicName, 4)
				i++
			}
		})
	})

	b.Run("ConcurrentBatchProcessing", func(b *testing.B) {
		config := performance.BatchConfig{
			BatchSize:     20,
			FlushInterval: 5 * time.Millisecond,
			BufferSize:    500,
		}

		handler := performance.BatchHandlerFunc(func(items []interface{}) error {
			// 模拟处理时间
			time.Sleep(time.Microsecond)
			return nil
		})

		processor := performance.NewBatchProcessor(config, handler)
		processor.Start()
		defer processor.Stop()

		b.SetParallelism(50)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				processor.Add(fmt.Sprintf("concurrent_batch_item_%d", i))
				i++
			}
		})
	})
}

// BenchmarkScalabilityPerformance 可扩展性性能基准测试
func BenchmarkScalabilityPerformance(b *testing.B) {
	performance.InitGlobalPools()
	performance.InitGlobalBatchManager()

	b.Run("ScalableMessageVolume", func(b *testing.B) {
		// 测试不同消息量级下的性能
		volumes := []int{100, 1000, 10000}

		for _, volume := range volumes {
			b.Run(fmt.Sprintf("Volume_%d", volume), func(b *testing.B) {
				// 初始化Broker
				brokerConfig := &broker.Config{
					BrokerName:       fmt.Sprintf("ScaleBroker_%d", volume),
					ListenPort:       10970 + volume/1000,
					NameServerAddr:   "127.0.0.1:9876",
					StorePathRootDir: fmt.Sprintf("/tmp/rocketmq_scale_test_%d", volume),
				}
				br := broker.NewBroker(brokerConfig)
				if err := br.Start(); err != nil {
					b.Fatalf("Failed to start broker: %v", err)
				}
				defer br.Stop()

				// 创建测试主题
				topicName := fmt.Sprintf("ScaleTestTopic_%d", volume)
				queueCount := int32(volume / 1000)
				if queueCount < 1 {
					queueCount = 1
				}
				if err := br.CreateTopic(topicName, queueCount); err != nil {
					b.Fatalf("Failed to create topic: %v", err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// 批量发送消息
					for j := 0; j < volume/100; j++ {
						msg := &common.Message{
							Topic: topicName,
							Body:  []byte(fmt.Sprintf("Scale test message %d-%d", i, j)),
						}
						br.PutMessage(msg)
					}
				}
			})
		}
	})

	b.Run("ScalableTopicCount", func(b *testing.B) {
		// 测试不同主题数量下的性能
		topicCounts := []int{10, 100, 500}

		for _, topicCount := range topicCounts {
			b.Run(fmt.Sprintf("Topics_%d", topicCount), func(b *testing.B) {
				// 初始化Broker
				brokerConfig := &broker.Config{
					BrokerName:       fmt.Sprintf("TopicScaleBroker_%d", topicCount),
					ListenPort:       10980 + topicCount/100,
					NameServerAddr:   "127.0.0.1:9876",
					StorePathRootDir: fmt.Sprintf("/tmp/rocketmq_topic_scale_test_%d", topicCount),
				}
				br := broker.NewBroker(brokerConfig)
				if err := br.Start(); err != nil {
					b.Fatalf("Failed to start broker: %v", err)
				}
				defer br.Stop()

				// 预创建主题
				for i := 0; i < topicCount; i++ {
					topicName := fmt.Sprintf("ScaleTopic_%d_%d", topicCount, i)
					br.CreateTopic(topicName, 4)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// 向随机主题发送消息
					topicIndex := i % topicCount
					topicName := fmt.Sprintf("ScaleTopic_%d_%d", topicCount, topicIndex)
					msg := &common.Message{
						Topic: topicName,
						Body:  []byte(fmt.Sprintf("Topic scale test message %d", i)),
					}
					br.PutMessage(msg)
				}
			})
		}
	})
}

// BenchmarkEndToEndPerformance 端到端性能基准测试
func BenchmarkEndToEndPerformance(b *testing.B) {
	b.Run("FullStackPerformance", func(b *testing.B) {
		// 初始化NameServer
		nameServerConfig := &nameserver.Config{
			ListenPort:                  9889,
			ClusterTestEnable:           true,
			OrderMessageEnable:          true,
			ScanNotActiveBrokerInterval: 2 * time.Second,
		}
		ns := nameserver.NewNameServer(nameServerConfig)
		if err := ns.Start(); err != nil {
			b.Fatalf("Failed to start nameserver: %v", err)
		}
		defer ns.Stop()

		time.Sleep(500 * time.Millisecond)

		// 初始化Broker
		brokerConfig := &broker.Config{
			BrokerName:       "E2EBroker",
			ListenPort:       10991,
			NameServerAddr:   "127.0.0.1:9889",
			StorePathRootDir: "/tmp/rocketmq_e2e_test",
		}
		br := broker.NewBroker(brokerConfig)
		if err := br.Start(); err != nil {
			b.Fatalf("Failed to start broker: %v", err)
		}
		defer br.Stop()

		// 初始化性能监控
		monitorConfig := performance.MonitorConfig{
			CollectInterval: 100 * time.Millisecond,
			EnableHTTP:      false,
		}
		monitor := performance.NewPerformanceMonitor(monitorConfig)
		if err := monitor.Start(); err != nil {
			b.Fatalf("Failed to start performance monitor: %v", err)
		}
		defer monitor.Stop()

		// 初始化性能组件
		performance.InitGlobalPools()
		performance.InitGlobalBatchManager()

		time.Sleep(1 * time.Second)

		// 创建测试主题
		topicName := "E2ETestTopic"
		if err := br.CreateTopic(topicName, 8); err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// 完整的端到端流程
				perfMsg := performance.GetMessage()
				perfMsg.Topic = topicName
				perfMsg.Body = performance.GetBuffer(256)
				copy(perfMsg.Body, []byte("E2E test message"))

				// 转换为common.Message
				msg := &common.Message{
					Topic: perfMsg.Topic,
					Body:  perfMsg.Body,
				}

				// 发送消息
				br.PutMessage(msg)

				// 拉取消息
				br.PullMessage(topicName, 0, 0, 1)

				// 更新性能指标
				monitor.UpdateMetrics()

				// 归还资源
				performance.PutBuffer(perfMsg.Body)
				performance.PutMessage(perfMsg)
			}
		})
	})
}

// BenchmarkResourceUtilization 资源利用率基准测试
func BenchmarkResourceUtilization(b *testing.B) {
	performance.InitGlobalPools()
	performance.InitGlobalBatchManager()

	b.Run("CPUUtilization", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// CPU密集型操作
				msg := performance.GetMessage()
				msg.Topic = "CPUTestTopic"
				msg.Body = performance.GetBuffer(1024)

				// 模拟CPU密集型处理
				for i := 0; i < len(msg.Body); i++ {
					msg.Body[i] = byte((i * 7) % 256)
				}

				performance.PutBuffer(msg.Body)
				performance.PutMessage(msg)
			}
		})
	})

	b.Run("MemoryUtilization", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// 内存密集型操作
				buffers := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					buffers[i] = performance.GetBuffer(512)
				}

				// 使用缓冲区
				for _, buf := range buffers {
					for j := 0; j < len(buf); j++ {
						buf[j] = byte(j % 256)
					}
				}

				// 归还缓冲区
				for _, buf := range buffers {
					performance.PutBuffer(buf)
				}
			}
		})
	})

	b.Run("IOUtilization", func(b *testing.B) {
		// 初始化Broker进行IO测试
		brokerConfig := &broker.Config{
			BrokerName:       "IOBroker",
			ListenPort:       10992,
			NameServerAddr:   "127.0.0.1:9876",
			StorePathRootDir: "/tmp/rocketmq_io_test",
		}
		br := broker.NewBroker(brokerConfig)
		if err := br.Start(); err != nil {
			b.Fatalf("Failed to start broker: %v", err)
		}
		defer br.Stop()

		// 创建测试主题
		topicName := "IOTestTopic"
		if err := br.CreateTopic(topicName, 4); err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// IO密集型操作
			msg := &common.Message{
				Topic: topicName,
				Body:  make([]byte, 2048), // 较大的消息体
			}
			// 填充消息体
			for j := 0; j < len(msg.Body); j++ {
				msg.Body[j] = byte(j % 256)
			}

			// 写入存储
			br.PutMessage(msg)

			// 读取存储
			br.PullMessage(topicName, int32(i%4), 0, 1)
		}
	})
}