package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenjy16/go-rocketmq-client"
)

// 基准测试配置
type BenchmarkConfig struct {
	ProducerCount    int           // 生产者数量
	ConsumerCount    int           // 消费者数量
	MessageCount     int           // 每个生产者发送的消息数量
	MessageSize      int           // 消息大小（字节）
	TestDuration     time.Duration // 测试持续时间
	WarmupDuration   time.Duration // 预热时间
	ReportInterval   time.Duration // 报告间隔
	NameServerAddr   string        // NameServer地址
	Topic            string        // 测试Topic
}

// 性能统计
type PerformanceStats struct {
	SentCount     int64 // 发送消息数
	ReceivedCount int64 // 接收消息数
	SentBytes     int64 // 发送字节数
	ReceivedBytes int64 // 接收字节数
	ErrorCount    int64 // 错误数
	StartTime     time.Time
	EndTime       time.Time
}

var (
	globalStats = &PerformanceStats{}
	statsLock   sync.RWMutex
)

func main() {
	fmt.Println("=== Go-RocketMQ 性能基准测试 ===")
	fmt.Println("本测试将评估Go-RocketMQ在高并发场景下的性能表现")

	// 基准测试配置
	config := &BenchmarkConfig{
		ProducerCount:  4,                // 4个生产者
		ConsumerCount:  2,                // 2个消费者
		MessageCount:   1000,             // 每个生产者发送1000条消息
		MessageSize:    1024,             // 1KB消息
		TestDuration:   60 * time.Second, // 测试60秒
		WarmupDuration: 10 * time.Second, // 预热10秒
		ReportInterval: 5 * time.Second,  // 每5秒报告一次
		NameServerAddr: "127.0.0.1:9876",
		Topic:          "BenchmarkTopic",
	}

	printBenchmarkConfig(config)

	// 运行基准测试
	runBenchmark(config)
}

// 打印基准测试配置
func printBenchmarkConfig(config *BenchmarkConfig) {
	fmt.Printf("\n--- 基准测试配置 ---\n")
	fmt.Printf("生产者数量: %d\n", config.ProducerCount)
	fmt.Printf("消费者数量: %d\n", config.ConsumerCount)
	fmt.Printf("每个生产者消息数: %d\n", config.MessageCount)
	fmt.Printf("消息大小: %d 字节\n", config.MessageSize)
	fmt.Printf("测试持续时间: %v\n", config.TestDuration)
	fmt.Printf("预热时间: %v\n", config.WarmupDuration)
	fmt.Printf("NameServer: %s\n", config.NameServerAddr)
	fmt.Printf("测试Topic: %s\n", config.Topic)
	fmt.Printf("总消息数: %d\n", config.ProducerCount*config.MessageCount)
	fmt.Printf("预期总数据量: %.2f MB\n", float64(config.ProducerCount*config.MessageCount*config.MessageSize)/(1024*1024))
}

// 运行基准测试
func runBenchmark(config *BenchmarkConfig) {
	var wg sync.WaitGroup

	// 初始化统计
	globalStats.StartTime = time.Now()

	// 启动性能监控
	wg.Add(1)
	go func() {
		defer wg.Done()
		startPerformanceMonitor(config)
	}()

	// 启动消费者
	fmt.Println("\n--- 启动消费者 ---")
	for i := 0; i < config.ConsumerCount; i++ {
		wg.Add(1)
		go func(consumerIndex int) {
			defer wg.Done()
			startBenchmarkConsumer(config, consumerIndex)
		}(i)
	}

	// 等待消费者启动
	time.Sleep(2 * time.Second)

	// 预热阶段
	fmt.Printf("\n--- 预热阶段 (%v) ---\n", config.WarmupDuration)
	startWarmup(config)

	// 重置统计
	resetStats()

	// 正式测试阶段
	fmt.Printf("\n--- 正式测试阶段 (%v) ---\n", config.TestDuration)
	testStartTime := time.Now()

	// 启动生产者
	for i := 0; i < config.ProducerCount; i++ {
		wg.Add(1)
		go func(producerIndex int) {
			defer wg.Done()
			startBenchmarkProducer(config, producerIndex)
		}(i)
	}

	// 等待测试完成
	time.Sleep(config.TestDuration)
	globalStats.EndTime = time.Now()

	// 等待所有goroutine完成
	wg.Wait()

	// 打印最终结果
	printFinalResults(config, time.Since(testStartTime))
}

// 启动预热
func startWarmup(config *BenchmarkConfig) {
	producer := client.NewProducer("warmup_producer_group")
	producer.SetNameServers([]string{config.NameServerAddr})

	if err := producer.Start(); err != nil {
		log.Printf("预热生产者启动失败: %v", err)
		return
	}
	defer producer.Shutdown()

	// 发送预热消息
	warmupCount := 100
	for i := 0; i < warmupCount; i++ {
		msg := createBenchmarkMessage(config, "warmup", i)
		producer.SendSync(msg)
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Printf("预热完成，发送了 %d 条消息\n", warmupCount)
}

// 重置统计
func resetStats() {
	statsLock.Lock()
	defer statsLock.Unlock()

	atomic.StoreInt64(&globalStats.SentCount, 0)
	atomic.StoreInt64(&globalStats.ReceivedCount, 0)
	atomic.StoreInt64(&globalStats.SentBytes, 0)
	atomic.StoreInt64(&globalStats.ReceivedBytes, 0)
	atomic.StoreInt64(&globalStats.ErrorCount, 0)
	globalStats.StartTime = time.Now()
}

// 启动基准测试生产者
func startBenchmarkProducer(config *BenchmarkConfig, producerIndex int) {
	producerGroup := fmt.Sprintf("benchmark_producer_group_%d", producerIndex)
	fmt.Printf("[生产者-%d] 启动\n", producerIndex)

	// 创建生产者
	producer := client.NewProducer(producerGroup)
	producer.SetNameServers([]string{config.NameServerAddr})

	if err := producer.Start(); err != nil {
		log.Printf("[生产者-%d] 启动失败: %v", producerIndex, err)
		atomic.AddInt64(&globalStats.ErrorCount, 1)
		return
	}
	defer producer.Shutdown()

	fmt.Printf("[生产者-%d] 开始发送消息\n", producerIndex)

	// 发送消息
	for i := 0; i < config.MessageCount; i++ {
		msg := createBenchmarkMessage(config, fmt.Sprintf("producer_%d", producerIndex), i)

		start := time.Now()
		result, err := producer.SendSync(msg)
		duration := time.Since(start)

		if err != nil {
			atomic.AddInt64(&globalStats.ErrorCount, 1)
			log.Printf("[生产者-%d] 发送失败: %v", producerIndex, err)
			continue
		}

		// 更新统计
		atomic.AddInt64(&globalStats.SentCount, 1)
		atomic.AddInt64(&globalStats.SentBytes, int64(len(msg.Body)))

		// 记录延迟（可选）
		if duration > 100*time.Millisecond {
			fmt.Printf("[生产者-%d] 高延迟消息: %v, MsgId: %s\n", producerIndex, duration, result.MsgId)
		}

		// 控制发送速率（可选）
		if i%100 == 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}

	fmt.Printf("[生产者-%d] 发送完成，共发送 %d 条消息\n", producerIndex, config.MessageCount)
}

// 启动基准测试消费者
func startBenchmarkConsumer(config *BenchmarkConfig, consumerIndex int) {
	consumerGroup := "benchmark_consumer_group"
	fmt.Printf("[消费者-%d] 启动\n", consumerIndex)

	// 创建消费者配置
	consumerConfig := &client.ConsumerConfig{
		GroupName:        consumerGroup,
		NameServerAddr:   config.NameServerAddr,
		ConsumeFromWhere: client.ConsumeFromLastOffset,
		MessageModel:     client.Clustering,
		ConsumeThreadMin: 4,
		ConsumeThreadMax: 8,
		PullInterval:     10 * time.Millisecond,
		PullBatchSize:    32,
		ConsumeTimeout:   30 * time.Second,
	}

	// 创建消费者
	consumer := client.NewConsumer(consumerConfig)

	// 订阅Topic
	err := consumer.Subscribe(config.Topic, "*", &BenchmarkMessageListener{
		ConsumerIndex: consumerIndex,
	})
	if err != nil {
		log.Printf("[消费者-%d] 订阅失败: %v", consumerIndex, err)
		atomic.AddInt64(&globalStats.ErrorCount, 1)
		return
	}

	if err := consumer.Start(); err != nil {
		log.Printf("[消费者-%d] 启动失败: %v", consumerIndex, err)
		atomic.AddInt64(&globalStats.ErrorCount, 1)
		return
	}
	defer consumer.Stop()

	fmt.Printf("[消费者-%d] 启动成功\n", consumerIndex)

	// 运行直到测试结束
	time.Sleep(config.TestDuration + 10*time.Second)
	fmt.Printf("[消费者-%d] 停止\n", consumerIndex)
}

// 创建基准测试消息
func createBenchmarkMessage(config *BenchmarkConfig, prefix string, index int) *client.Message {
	// 创建指定大小的消息体
	body := make([]byte, config.MessageSize)
	msgContent := fmt.Sprintf("%s_msg_%d_%d", prefix, index, time.Now().UnixNano())
	copy(body, []byte(msgContent))

	// 填充剩余空间
	for i := len(msgContent); i < config.MessageSize; i++ {
		body[i] = byte('A' + (i % 26))
	}

	msg := client.NewMessage(
		config.Topic,
		body,
	).SetTags("BENCHMARK").SetKeys(fmt.Sprintf("%s_%d", prefix, index)).SetProperty("producer", prefix).SetProperty("index", fmt.Sprintf("%d", index)).SetProperty("timestamp", fmt.Sprintf("%d", time.Now().UnixNano()))

	return msg
}

// 基准测试消息监听器
type BenchmarkMessageListener struct {
	ConsumerIndex int
	messageCount  int64
}

func (l *BenchmarkMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	for _, msg := range msgs {
		// 更新统计
		atomic.AddInt64(&globalStats.ReceivedCount, 1)
		atomic.AddInt64(&globalStats.ReceivedBytes, int64(len(msg.Body)))
		atomic.AddInt64(&l.messageCount, 1)

		// 计算端到端延迟
		if timestampStr := msg.GetProperty("timestamp"); timestampStr != "" {
			// 这里可以计算延迟，但为了性能考虑，在基准测试中通常省略
		}

		// 最小化处理时间
		// 在实际基准测试中，通常不进行复杂的业务逻辑处理
	}

	return client.ConsumeSuccess
}

// 启动性能监控
func startPerformanceMonitor(config *BenchmarkConfig) {
	ticker := time.NewTicker(config.ReportInterval)
	defer ticker.Stop()

	lastSentCount := int64(0)
	lastReceivedCount := int64(0)
	lastTime := time.Now()

	for {
		select {
		case <-ticker.C:
			currentTime := time.Now()
			currentSentCount := atomic.LoadInt64(&globalStats.SentCount)
			currentReceivedCount := atomic.LoadInt64(&globalStats.ReceivedCount)
			currentSentBytes := atomic.LoadInt64(&globalStats.SentBytes)
			currentReceivedBytes := atomic.LoadInt64(&globalStats.ReceivedBytes)
			currentErrorCount := atomic.LoadInt64(&globalStats.ErrorCount)

			duration := currentTime.Sub(lastTime).Seconds()
			sendTPS := float64(currentSentCount-lastSentCount) / duration
			receiveTPS := float64(currentReceivedCount-lastReceivedCount) / duration

			fmt.Printf("\n--- 性能报告 (%s) ---\n", currentTime.Format("15:04:05"))
			fmt.Printf("发送: %d 条 (%.2f TPS)\n", currentSentCount, sendTPS)
			fmt.Printf("接收: %d 条 (%.2f TPS)\n", currentReceivedCount, receiveTPS)
			fmt.Printf("发送数据: %.2f MB\n", float64(currentSentBytes)/(1024*1024))
			fmt.Printf("接收数据: %.2f MB\n", float64(currentReceivedBytes)/(1024*1024))
			fmt.Printf("错误数: %d\n", currentErrorCount)
			fmt.Printf("消息积压: %d\n", currentSentCount-currentReceivedCount)

			lastSentCount = currentSentCount
			lastReceivedCount = currentReceivedCount
			lastTime = currentTime
		}
	}
}

// 打印最终结果
func printFinalResults(config *BenchmarkConfig, actualDuration time.Duration) {
	finalSentCount := atomic.LoadInt64(&globalStats.SentCount)
	finalReceivedCount := atomic.LoadInt64(&globalStats.ReceivedCount)
	finalSentBytes := atomic.LoadInt64(&globalStats.SentBytes)
	finalReceivedBytes := atomic.LoadInt64(&globalStats.ReceivedBytes)
	finalErrorCount := atomic.LoadInt64(&globalStats.ErrorCount)

	fmt.Printf("\n=== 基准测试最终结果 ===\n")
	fmt.Printf("测试持续时间: %v\n", actualDuration)
	fmt.Printf("\n--- 消息统计 ---\n")
	fmt.Printf("发送消息数: %d\n", finalSentCount)
	fmt.Printf("接收消息数: %d\n", finalReceivedCount)
	fmt.Printf("消息丢失数: %d\n", finalSentCount-finalReceivedCount)
	fmt.Printf("错误数: %d\n", finalErrorCount)
	fmt.Printf("成功率: %.2f%%\n", float64(finalSentCount-finalErrorCount)/float64(finalSentCount)*100)

	fmt.Printf("\n--- 吞吐量统计 ---\n")
	sendTPS := float64(finalSentCount) / actualDuration.Seconds()
	receiveTPS := float64(finalReceivedCount) / actualDuration.Seconds()
	fmt.Printf("发送TPS: %.2f 条/秒\n", sendTPS)
	fmt.Printf("接收TPS: %.2f 条/秒\n", receiveTPS)

	fmt.Printf("\n--- 数据量统计 ---\n")
	fmt.Printf("发送数据量: %.2f MB\n", float64(finalSentBytes)/(1024*1024))
	fmt.Printf("接收数据量: %.2f MB\n", float64(finalReceivedBytes)/(1024*1024))
	sendMBPS := float64(finalSentBytes) / (1024 * 1024) / actualDuration.Seconds()
	receiveMBPS := float64(finalReceivedBytes) / (1024 * 1024) / actualDuration.Seconds()
	fmt.Printf("发送带宽: %.2f MB/秒\n", sendMBPS)
	fmt.Printf("接收带宽: %.2f MB/秒\n", receiveMBPS)

	fmt.Printf("\n--- 性能评估 ---\n")
	if sendTPS > 10000 {
		fmt.Println("✓ 发送性能: 优秀 (>10K TPS)")
	} else if sendTPS > 5000 {
		fmt.Println("✓ 发送性能: 良好 (>5K TPS)")
	} else {
		fmt.Println("⚠ 发送性能: 需要优化 (<5K TPS)")
	}

	if receiveTPS > 10000 {
		fmt.Println("✓ 接收性能: 优秀 (>10K TPS)")
	} else if receiveTPS > 5000 {
		fmt.Println("✓ 接收性能: 良好 (>5K TPS)")
	} else {
		fmt.Println("⚠ 接收性能: 需要优化 (<5K TPS)")
	}

	if finalErrorCount == 0 {
		fmt.Println("✓ 可靠性: 优秀 (无错误)")
	} else if float64(finalErrorCount)/float64(finalSentCount) < 0.01 {
		fmt.Println("✓ 可靠性: 良好 (错误率<1%)")
	} else {
		fmt.Println("⚠ 可靠性: 需要关注 (错误率较高)")
	}

	fmt.Println("\n基准测试完成")
}