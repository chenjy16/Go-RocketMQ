package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go-rocketmq/pkg/client"
	"go-rocketmq/pkg/common"
)

var (
	nameServer   = flag.String("nameserver", "127.0.0.1:9876", "NameServer地址")
	topic        = flag.String("topic", "BenchmarkTopic", "测试Topic")
	messageCount = flag.Int("count", 10000, "消息数量")
	messageSize  = flag.Int("size", 1024, "消息大小(字节)")
	concurrency  = flag.Int("concurrency", 10, "并发数")
	mode         = flag.String("mode", "sync", "发送模式: sync, async, oneway")
)

func main() {
	flag.Parse()

	fmt.Printf("=== Go-RocketMQ 性能测试 ===\n")
	fmt.Printf("NameServer: %s\n", *nameServer)
	fmt.Printf("Topic: %s\n", *topic)
	fmt.Printf("消息数量: %d\n", *messageCount)
	fmt.Printf("消息大小: %d bytes\n", *messageSize)
	fmt.Printf("并发数: %d\n", *concurrency)
	fmt.Printf("发送模式: %s\n", *mode)
	fmt.Printf("=============================\n\n")

	// 创建生产者
	producer := client.NewProducer("benchmark-producer-group")
	producer.SetNameServers([]string{*nameServer})

	if err := producer.Start(); err != nil {
		log.Fatalf("启动生产者失败: %v", err)
	}
	defer producer.Shutdown()

	// 准备测试数据
	messageBody := make([]byte, *messageSize)
	for i := range messageBody {
		messageBody[i] = byte('A' + (i % 26))
	}

	// 性能统计
	var (
		successCount int64
		failureCount int64
		totalLatency int64
	)

	startTime := time.Now()

	// 创建工作协程
	var wg sync.WaitGroup
	messagesPerWorker := *messageCount / *concurrency

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < messagesPerWorker; j++ {
				msgStart := time.Now()
				
				// 创建消息
				msg := common.NewMessage(*topic, messageBody)
				msg.SetKeys(fmt.Sprintf("benchmark-key-%d-%d", workerID, j))
				msg.SetTags("benchmark")

				var err error
				switch *mode {
				case "sync":
					_, err = producer.SendSync(msg)
				case "async":
					err = producer.SendAsync(msg, func(result *common.SendResult, asyncErr error) {
						if asyncErr != nil {
							atomic.AddInt64(&failureCount, 1)
						} else {
							atomic.AddInt64(&successCount, 1)
						}
					})
				case "oneway":
					err = producer.SendOneway(msg)
				default:
					log.Fatalf("不支持的发送模式: %s", *mode)
				}

				if err != nil {
					atomic.AddInt64(&failureCount, 1)
				} else if *mode != "async" {
					atomic.AddInt64(&successCount, 1)
				}

				// 记录延迟（仅同步模式）
				if *mode == "sync" {
					latency := time.Since(msgStart).Nanoseconds()
					atomic.AddInt64(&totalLatency, latency)
				}
			}
		}(i)
	}

	// 等待所有协程完成
	wg.Wait()

	// 等待异步消息完成
	if *mode == "async" {
		time.Sleep(2 * time.Second)
	}

	duration := time.Since(startTime)

	// 计算统计数据
	totalMessages := atomic.LoadInt64(&successCount) + atomic.LoadInt64(&failureCount)
	tps := float64(atomic.LoadInt64(&successCount)) / duration.Seconds()
	
	var avgLatency float64
	if *mode == "sync" && atomic.LoadInt64(&successCount) > 0 {
		avgLatency = float64(atomic.LoadInt64(&totalLatency)) / float64(atomic.LoadInt64(&successCount)) / 1e6 // 转换为毫秒
	}

	// 输出结果
	fmt.Printf("=== 测试结果 ===\n")
	fmt.Printf("总耗时: %v\n", duration)
	fmt.Printf("总消息数: %d\n", totalMessages)
	fmt.Printf("成功数: %d\n", atomic.LoadInt64(&successCount))
	fmt.Printf("失败数: %d\n", atomic.LoadInt64(&failureCount))
	fmt.Printf("成功率: %.2f%%\n", float64(atomic.LoadInt64(&successCount))/float64(totalMessages)*100)
	fmt.Printf("TPS: %.2f msg/s\n", tps)
	
	if *mode == "sync" {
		fmt.Printf("平均延迟: %.2f ms\n", avgLatency)
	}
	
	fmt.Printf("吞吐量: %.2f MB/s\n", tps*float64(*messageSize)/1024/1024)
	fmt.Printf("================\n")
}