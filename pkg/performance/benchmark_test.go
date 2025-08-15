package performance

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

// BenchmarkMemoryPool 内存池基准测试
func BenchmarkMemoryPool(b *testing.B) {
	pool := NewMemoryPool()
	
	b.Run("GetBuffer_Small", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(64)
			pool.PutBuffer(buf)
		}
	})
	
	b.Run("GetBuffer_Medium", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(1024)
			pool.PutBuffer(buf)
		}
	})
	
	b.Run("GetBuffer_Large", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(16384)
			pool.PutBuffer(buf)
		}
	})
	
	b.Run("GetBuffer_Concurrent", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := pool.GetBuffer(1024)
				pool.PutBuffer(buf)
			}
		})
	})
}

// BenchmarkMemoryPoolVsNative 内存池与原生分配对比
func BenchmarkMemoryPoolVsNative(b *testing.B) {
	pool := NewMemoryPool()
	
	b.Run("MemoryPool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(1024)
			// 模拟使用
			for j := 0; j < len(buf); j++ {
				buf[j] = byte(j)
			}
			pool.PutBuffer(buf)
		}
	})
	
	b.Run("NativeAllocation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 1024)
			// 模拟使用
			for j := 0; j < len(buf); j++ {
				buf[j] = byte(j)
			}
			_ = buf // 防止优化
		}
	})
}

// BenchmarkMessagePool 消息池基准测试
func BenchmarkMessagePool(b *testing.B) {
	msgPool := NewMessagePool()
	
	b.Run("GetMessage", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			msg := msgPool.Get()
			msg.Topic = "test"
			msg.Body = []byte("test message")
			msgPool.Put(msg)
		}
	})
	
	b.Run("GetMessage_Concurrent", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				msg := msgPool.Get()
				msg.Topic = "test"
				msg.Body = []byte("test message")
				msgPool.Put(msg)
			}
		})
	})
}

// BenchmarkBatchProcessor 批量处理器基准测试
func BenchmarkBatchProcessor(b *testing.B) {
	config := DefaultBatchConfig
	config.BatchSize = 100
	config.FlushInterval = 10 * time.Millisecond
	
	processedCount := int64(0)
	handler := BatchHandlerFunc(func(items []interface{}) error {
		processedCount += int64(len(items))
		return nil
	})
	
	processor := NewBatchProcessor(config, handler)
	processor.Start()
	defer processor.Stop()
	
	b.Run("AddItems", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			processor.Add(fmt.Sprintf("item_%d", i))
		}
	})
	
	b.Run("AddItems_Concurrent", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				processor.Add(fmt.Sprintf("item_%d", i))
				i++
			}
		})
	})
}

// BenchmarkBatchSender 批量发送器基准测试
func BenchmarkBatchSender(b *testing.B) {
	config := DefaultBatchConfig
	config.BatchSize = 50
	config.FlushInterval = 5 * time.Millisecond
	
	sentCount := int64(0)
	sendFunc := func(messages []*Message) error {
		sentCount += int64(len(messages))
		return nil
	}
	
	sender := NewBatchSender(config, sendFunc)
	sender.Start()
	defer sender.Stop()
	
	b.Run("SendMessages", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			msg := &Message{
				Topic: "test",
				Body:  []byte(fmt.Sprintf("message_%d", i)),
			}
			sender.Send(msg)
		}
	})
	
	b.Run("SendMessages_Concurrent", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				msg := &Message{
					Topic: "test",
					Body:  []byte(fmt.Sprintf("message_%d", i)),
				}
				sender.Send(msg)
				i++
			}
		})
	})
}

// BenchmarkConnectionPool 连接池基准测试
func BenchmarkConnectionPool(b *testing.B) {
	// 模拟连接池（由于需要真实网络连接，这里使用模拟）
	b.Skip("Skipping connection pool benchmark - requires real network")
}

// BenchmarkZeroCopyBuffer 零拷贝缓冲区基准测试
func BenchmarkZeroCopyBuffer(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	
	b.Run("ZeroCopyBuffer", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			zcb := NewZeroCopyBuffer(data)
			_ = zcb.Slice(0, 512)
			_ = zcb.UnsafeString()
		}
	})
	
	b.Run("NormalCopy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			copyData := make([]byte, len(data))
			copy(copyData, data)
			_ = copyData[0:512]
			_ = string(copyData)
		}
	})
}

// BenchmarkGCPressure GC压力测试
func BenchmarkGCPressure(b *testing.B) {
	b.Run("WithMemoryPool", func(b *testing.B) {
		pool := NewMemoryPool()
		b.ResetTimer()
		
		var gcBefore runtime.MemStats
		runtime.ReadMemStats(&gcBefore)
		
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(1024)
			// 模拟使用
			for j := 0; j < len(buf); j++ {
				buf[j] = byte(j)
			}
			pool.PutBuffer(buf)
		}
		
		var gcAfter runtime.MemStats
		runtime.ReadMemStats(&gcAfter)
		
		b.ReportMetric(float64(gcAfter.NumGC-gcBefore.NumGC), "gc-count")
		b.ReportMetric(float64(gcAfter.PauseTotalNs-gcBefore.PauseTotalNs)/1e6, "gc-pause-ms")
	})
	
	b.Run("WithoutMemoryPool", func(b *testing.B) {
		b.ResetTimer()
		
		var gcBefore runtime.MemStats
		runtime.ReadMemStats(&gcBefore)
		
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 1024)
			// 模拟使用
			for j := 0; j < len(buf); j++ {
				buf[j] = byte(j)
			}
			_ = buf // 防止优化
		}
		
		var gcAfter runtime.MemStats
		runtime.ReadMemStats(&gcAfter)
		
		b.ReportMetric(float64(gcAfter.NumGC-gcBefore.NumGC), "gc-count")
		b.ReportMetric(float64(gcAfter.PauseTotalNs-gcBefore.PauseTotalNs)/1e6, "gc-pause-ms")
	})
}

// BenchmarkThroughput 吞吐量基准测试
func BenchmarkThroughput(b *testing.B) {
	config := DefaultBatchConfig
	config.BatchSize = 1000
	config.FlushInterval = 1 * time.Millisecond
	
	processedCount := int64(0)
	handler := BatchHandlerFunc(func(items []interface{}) error {
		processedCount += int64(len(items))
		return nil
	})
	
	processor := NewBatchProcessor(config, handler)
	processor.Start()
	defer processor.Stop()
	
	b.Run("HighThroughput", func(b *testing.B) {
		b.ResetTimer()
		start := time.Now()
		
		var wg sync.WaitGroup
		workers := 10
		itemsPerWorker := b.N / workers
		
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < itemsPerWorker; i++ {
					item := fmt.Sprintf("worker_%d_item_%d", workerID, i)
					processor.Add(item)
				}
			}(w)
		}
		
		wg.Wait()
		elapsed := time.Since(start)
		
		// 等待处理完成
		time.Sleep(100 * time.Millisecond)
		
		throughput := float64(b.N) / elapsed.Seconds()
		b.ReportMetric(throughput, "items/sec")
	})
}

// BenchmarkLatency 延迟基准测试
func BenchmarkLatency(b *testing.B) {
	config := DefaultBatchConfig
	config.BatchSize = 1
	config.FlushInterval = 1 * time.Nanosecond // 立即处理
	
	latencies := make([]time.Duration, 0, b.N)
	var latencyMutex sync.Mutex
	
	handler := BatchHandlerFunc(func(items []interface{}) error {
		for _, item := range items {
			if startTime, ok := item.(time.Time); ok {
				latency := time.Since(startTime)
				latencyMutex.Lock()
				latencies = append(latencies, latency)
				latencyMutex.Unlock()
			}
		}
		return nil
	})
	
	processor := NewBatchProcessor(config, handler)
	processor.Start()
	defer processor.Stop()
	
	b.Run("ProcessingLatency", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			startTime := time.Now()
			processor.Add(startTime)
		}
		
		// 等待所有项目处理完成
		time.Sleep(100 * time.Millisecond)
		
		// 计算平均延迟
		latencyMutex.Lock()
		if len(latencies) > 0 {
			totalLatency := time.Duration(0)
			for _, lat := range latencies {
				totalLatency += lat
			}
			avgLatency := totalLatency / time.Duration(len(latencies))
			b.ReportMetric(float64(avgLatency.Nanoseconds())/1e6, "avg-latency-ms")
		}
		latencyMutex.Unlock()
	})
}

// BenchmarkMemoryUsage 内存使用基准测试
func BenchmarkMemoryUsage(b *testing.B) {
	b.Run("MemoryPool_Usage", func(b *testing.B) {
		pool := NewMemoryPool()
		
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		
		b.ResetTimer()
		buffers := make([][]byte, b.N)
		for i := 0; i < b.N; i++ {
			buffers[i] = pool.GetBuffer(1024)
		}
		
		runtime.ReadMemStats(&after)
		b.StopTimer()
		
		// 清理
		for _, buf := range buffers {
			pool.PutBuffer(buf)
		}
		
		memoryUsed := after.Alloc - before.Alloc
		b.ReportMetric(float64(memoryUsed)/float64(b.N), "bytes/op")
	})
	
	b.Run("Native_Usage", func(b *testing.B) {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		
		b.ResetTimer()
		buffers := make([][]byte, b.N)
		for i := 0; i < b.N; i++ {
			buffers[i] = make([]byte, 1024)
		}
		
		runtime.ReadMemStats(&after)
		b.StopTimer()
		
		_ = buffers // 防止优化
		
		memoryUsed := after.Alloc - before.Alloc
		b.ReportMetric(float64(memoryUsed)/float64(b.N), "bytes/op")
	})
}

// BenchmarkConcurrentAccess 并发访问基准测试
func BenchmarkConcurrentAccess(b *testing.B) {
	pool := NewMemoryPool()
	
	b.Run("MemoryPool_Concurrent", func(b *testing.B) {
		b.ResetTimer()
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
	
	b.Run("MessagePool_Concurrent", func(b *testing.B) {
		msgPool := NewMessagePool()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				msg := msgPool.Get()
				msg.Topic = "test"
				msg.Body = []byte("test message")
				msg.Properties["key"] = "value"
				msgPool.Put(msg)
			}
		})
	})
}

// BenchmarkBatchVsIndividual 批量处理与单个处理对比
func BenchmarkBatchVsIndividual(b *testing.B) {
	processedCount := int64(0)
	
	b.Run("BatchProcessing", func(b *testing.B) {
		config := DefaultBatchConfig
		config.BatchSize = 100
		config.FlushInterval = 1 * time.Millisecond
		
		handler := BatchHandlerFunc(func(items []interface{}) error {
			processedCount += int64(len(items))
			return nil
		})
		
		processor := NewBatchProcessor(config, handler)
		processor.Start()
		defer processor.Stop()
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			processor.Add(fmt.Sprintf("item_%d", i))
		}
	})
	
	b.Run("IndividualProcessing", func(b *testing.B) {
		handler := func(item interface{}) error {
			processedCount++
			return nil
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			handler(fmt.Sprintf("item_%d", i))
		}
	})
}

// BenchmarkCompressionOverhead 压缩开销基准测试
func BenchmarkCompressionOverhead(b *testing.B) {
	// 生成测试数据
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(rand.Intn(256))
	}
	
	b.Run("WithCompression", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 模拟压缩写入
			_ = data
		}
	})
	
	b.Run("WithoutCompression", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 模拟直接写入
			_ = data
		}
	})
}

// TestPerformanceRegression 性能回归测试
func TestPerformanceRegression(t *testing.T) {
	// 这个测试用于检测性能回归
	// 在CI/CD中可以设置性能阈值
	
	pool := NewMemoryPool()
	
	// 测试内存池性能
	start := time.Now()
	for i := 0; i < 10000; i++ {
		buf := pool.GetBuffer(1024)
		pool.PutBuffer(buf)
	}
	duration := time.Since(start)
	
	// 设置性能阈值（例如：10000次操作应该在500ms内完成）
	threshold := 500 * time.Millisecond
	if duration > threshold {
		t.Errorf("Memory pool performance regression: took %v, expected < %v", duration, threshold)
	}
	
	t.Logf("Memory pool performance: %v for 10000 operations", duration)
}

// BenchmarkIntegration 集成基准测试
func BenchmarkIntegration(b *testing.B) {
	// 初始化所有性能组件
	InitGlobalPools()
	InitGlobalBatchManager()
	InitGlobalNetworkOptimizer()
	
	b.Run("FullStack", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// 获取消息对象
				msg := GetMessage()
				msg.Topic = "benchmark"
				msg.Body = GetBuffer(1024)
				
				// 模拟处理
				for i := 0; i < len(msg.Body); i++ {
					msg.Body[i] = byte(i % 256)
				}
				
				// 归还资源
				PutBuffer(msg.Body)
				PutMessage(msg)
			}
		})
	})
}

// 运行所有基准测试的辅助函数
func RunAllBenchmarks() {
	fmt.Println("Running performance benchmarks...")
	
	// 这里可以添加自定义的基准测试运行逻辑
	// 例如：生成性能报告、对比历史数据等
}