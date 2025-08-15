package performance

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// BenchmarkBatchProcessorThroughput 批量处理器吞吐量基准测试
func BenchmarkBatchProcessorThroughput(b *testing.B) {
	config := DefaultBatchConfig
	config.BatchSize = 1000
	config.FlushInterval = 10 * time.Millisecond

	handler := BatchHandlerFunc(func(items []interface{}) error {
		// 模拟快速处理
		return nil
	})

	processor := NewBatchProcessor(config, handler)
	processor.Start()
	defer processor.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			processor.Add("test-item")
		}
	})
}

// BenchmarkBatchProcessorLatency 批量处理器延迟基准测试
func BenchmarkBatchProcessorLatency(b *testing.B) {
	config := DefaultBatchConfig
	config.BatchSize = 10
	config.FlushInterval = 1 * time.Millisecond

	processedChan := make(chan struct{}, b.N)
	handler := BatchHandlerFunc(func(items []interface{}) error {
		for range items {
			select {
			case processedChan <- struct{}{}:
			default:
			}
		}
		return nil
	})

	processor := NewBatchProcessor(config, handler)
	processor.Start()
	defer processor.Stop()

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		processor.Add(fmt.Sprintf("item-%d", i))
	}

	// 等待所有项目被处理
	for i := 0; i < b.N; i++ {
		<-processedChan
	}

	duration := time.Since(start)
	b.ReportMetric(float64(duration.Nanoseconds())/float64(b.N), "ns/item")
}

// BenchmarkMemoryPoolAllocation 内存池分配基准测试
func BenchmarkMemoryPoolAllocation(b *testing.B) {
	pool := NewMemoryPool()

	b.Run("MemoryPool_1KB", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := pool.GetBuffer(1024)
				pool.PutBuffer(buf)
			}
		})
	})

	b.Run("Native_1KB", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := make([]byte, 1024)
				_ = buf
			}
		})
	})

	b.Run("MemoryPool_64KB", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := pool.GetBuffer(65536)
				pool.PutBuffer(buf)
			}
		})
	})

	b.Run("Native_64KB", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := make([]byte, 65536)
				_ = buf
			}
		})
	})
}

// BenchmarkMessagePoolOperations 消息池操作基准测试
func BenchmarkMessagePoolOperations(b *testing.B) {
	pool := NewMessagePool()

	b.Run("Get", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				msg := pool.Get()
				pool.Put(msg)
			}
		})
	})

	b.Run("Get_WithData", func(b *testing.B) {
		data := make([]byte, 1024)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				msg := pool.Get()
				msg.Body = data
				msg.Topic = "test-topic"
				pool.Put(msg)
			}
		})
	})
}

// BenchmarkPerformanceMonitorOverhead 性能监控器开销基准测试
func BenchmarkPerformanceMonitorOverhead(b *testing.B) {
	config := MonitorConfig{
		CollectInterval: 100 * time.Millisecond,
		EnableHTTP:      false,
	}

	monitor := NewPerformanceMonitor(config)
	memoryPool := NewMemoryPool()
	monitor.RegisterMemoryPool(memoryPool)

	b.Run("WithMonitoring", func(b *testing.B) {
		monitor.Start()
		defer monitor.Stop()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := memoryPool.GetBuffer(1024)
				memoryPool.PutBuffer(buf)
			}
		})
	})

	b.Run("WithoutMonitoring", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := memoryPool.GetBuffer(1024)
				memoryPool.PutBuffer(buf)
			}
		})
	})
}

// BenchmarkConcurrentBatchProcessing 并发批量处理基准测试
func BenchmarkConcurrentBatchProcessing(b *testing.B) {
	config := DefaultBatchConfig
	config.BatchSize = 100
	config.FlushInterval = 5 * time.Millisecond

	var processedCount int64
	handler := BatchHandlerFunc(func(items []interface{}) error {
		// 模拟处理时间
		time.Sleep(100 * time.Microsecond)
		return nil
	})

	numProcessors := runtime.NumCPU()
	processors := make([]*BatchProcessor, numProcessors)

	for i := 0; i < numProcessors; i++ {
		processors[i] = NewBatchProcessor(config, handler)
		processors[i].Start()
		defer processors[i].Stop()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		processorIndex := 0
		for pb.Next() {
			processors[processorIndex%numProcessors].Add("test-item")
			processorIndex++
		}
	})

	// 等待处理完成
	time.Sleep(100 * time.Millisecond)

	// 计算总处理量
	for _, processor := range processors {
		metrics := processor.GetMetrics()
		processedCount += metrics.TotalItems
	}

	b.ReportMetric(float64(processedCount), "items-processed")
}

// BenchmarkNetworkOptimizerOverhead 网络优化器开销基准测试
func BenchmarkNetworkOptimizerOverhead(b *testing.B) {
	optimizer := NewNetworkOptimizer()
	config := ConnectionPoolConfig{
		MaxConnections: 10,
		ConnectTimeout: 1 * time.Second,
		MaxIdleTime:    100 * time.Millisecond,
	}

	optimizer.RegisterConnectionPool("test", "localhost:8080", config)
	optimizer.Start()
	defer optimizer.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 模拟网络操作
			pool := optimizer.GetConnectionPool("test")
			if pool != nil {
				// 模拟连接使用
				time.Sleep(1 * time.Microsecond)
			}
		}
	})
}

// BenchmarkGCPressureComparison GC压力对比基准测试
func BenchmarkGCPressureComparison(b *testing.B) {
	pool := NewMemoryPool()

	b.Run("WithMemoryPool", func(b *testing.B) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(1024)
			// 模拟使用
			for j := 0; j < len(buf); j++ {
				buf[j] = byte(j % 256)
			}
			pool.PutBuffer(buf)
		}
		b.StopTimer()

		runtime.GC()
		runtime.ReadMemStats(&m2)

		b.ReportMetric(float64(m2.NumGC-m1.NumGC), "gc-count")
		b.ReportMetric(float64(m2.PauseTotalNs-m1.PauseTotalNs)/1e6, "gc-pause-ms")
	})

	b.Run("WithoutMemoryPool", func(b *testing.B) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 1024)
			// 模拟使用
			for j := 0; j < len(buf); j++ {
				buf[j] = byte(j % 256)
			}
			_ = buf
		}
		b.StopTimer()

		runtime.GC()
		runtime.ReadMemStats(&m2)

		b.ReportMetric(float64(m2.NumGC-m1.NumGC), "gc-count")
		b.ReportMetric(float64(m2.PauseTotalNs-m1.PauseTotalNs)/1e6, "gc-pause-ms")
	})
}

// BenchmarkBatchSizeOptimization 批量大小优化基准测试
func BenchmarkBatchSizeOptimization(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500, 1000, 5000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			config := DefaultBatchConfig
			config.BatchSize = batchSize
			config.FlushInterval = 10 * time.Millisecond

			handler := BatchHandlerFunc(func(items []interface{}) error {
				// 模拟处理时间与批量大小成正比
				processTime := time.Duration(len(items)) * time.Microsecond
				time.Sleep(processTime)
				return nil
			})

			processor := NewBatchProcessor(config, handler)
			processor.Start()
			defer processor.Stop()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				processor.Add(fmt.Sprintf("item-%d", i))
			}

			// 等待处理完成
			time.Sleep(100 * time.Millisecond)

			metrics := processor.GetMetrics()
			b.ReportMetric(float64(metrics.TotalBatches), "batches")
			b.ReportMetric(metrics.AvgBatchSize, "avg-batch-size")
		})
	}
}

// BenchmarkMemoryUsagePattern 内存使用模式基准测试
func BenchmarkMemoryUsagePattern(b *testing.B) {
	pool := NewMemoryPool()

	b.Run("Sequential_Access", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(1024)
			pool.PutBuffer(buf)
		}
	})

	b.Run("Batch_Access", func(b *testing.B) {
		batchSize := 100
		buffers := make([][]byte, batchSize)

		b.ResetTimer()
		for i := 0; i < b.N; i += batchSize {
			// 批量获取
			for j := 0; j < batchSize && i+j < b.N; j++ {
				buffers[j] = pool.GetBuffer(1024)
			}
			// 批量归还
			for j := 0; j < batchSize && i+j < b.N; j++ {
				pool.PutBuffer(buffers[j])
			}
		}
	})

	b.Run("Random_Size_Access", func(b *testing.B) {
		sizes := []int{64, 256, 1024, 4096, 16384}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			size := sizes[i%len(sizes)]
			buf := pool.GetBuffer(size)
			pool.PutBuffer(buf)
		}
	})
}

// BenchmarkConcurrencyScaling 并发扩展性基准测试
func BenchmarkConcurrencyScaling(b *testing.B) {
	pool := NewMemoryPool()
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			b.SetParallelism(concurrency)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buf := pool.GetBuffer(1024)
					// 模拟一些工作
					for i := 0; i < 100; i++ {
						if i < len(buf) {
							buf[i] = byte(i)
						}
					}
					pool.PutBuffer(buf)
				}
			})
		})
	}
}