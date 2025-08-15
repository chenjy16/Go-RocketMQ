package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go-rocketmq/pkg/broker"
	"github.com/chenjy16/go-rocketmq-client"
	"go-rocketmq/pkg/nameserver"
)

// 集成测试配置
type IntegrationTestConfig struct {
	NameServerPort int
	BrokerPort     int
	TestDuration   time.Duration
	MessageCount   int
	Concurrency    int
}

// 测试结果
type TestResult struct {
	TotalSent     int
	TotalReceived int
	SuccessRate   float64
	AvgLatency    time.Duration
	Throughput    float64
	Errors        []error
}

// 消息监听器
type IntegrationTestListener struct {
	mu            sync.Mutex
	receivedCount int
	messages      []*client.MessageExt
	latencies     []time.Duration
}

func (l *IntegrationTestListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, msg := range msgs {
		l.receivedCount++
		l.messages = append(l.messages, msg)

		// 计算延迟
		if sendTimeStr := msg.GetProperty("sendTime"); sendTimeStr != "" {
			if sendTime, err := time.Parse(time.RFC3339Nano, sendTimeStr); err == nil {
				latency := time.Since(sendTime)
				l.latencies = append(l.latencies, latency)
			}
		}

		fmt.Printf("[消费者] 接收消息: %s (总计: %d)\n", string(msg.Body), l.receivedCount)
	}

	return client.ConsumeSuccess
}

func (l *IntegrationTestListener) GetReceivedCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.receivedCount
}

func (l *IntegrationTestListener) GetAverageLatency() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.latencies) == 0 {
		return 0
	}

	var total time.Duration
	for _, latency := range l.latencies {
		total += latency
	}
	return total / time.Duration(len(l.latencies))
}

func main() {
	fmt.Println("=== Go-RocketMQ 全栈集成测试 ===")
	fmt.Println("本测试将启动完整的RocketMQ集群并进行端到端测试")

	// 测试配置
	config := &IntegrationTestConfig{
		NameServerPort: 9876,
		BrokerPort:     10911,
		TestDuration:   30 * time.Second,
		MessageCount:   1000,
		Concurrency:    5,
	}

	printTestConfig(config)

	// 运行集成测试
	result, err := runIntegrationTest(config)
	if err != nil {
		log.Fatalf("集成测试失败: %v", err)
	}

	// 打印测试结果
	printTestResult(result)
}

// 打印测试配置
func printTestConfig(config *IntegrationTestConfig) {
	fmt.Println("\n=== 测试配置 ===")
	fmt.Printf("NameServer端口: %d\n", config.NameServerPort)
	fmt.Printf("Broker端口: %d\n", config.BrokerPort)
	fmt.Printf("测试时长: %v\n", config.TestDuration)
	fmt.Printf("消息数量: %d\n", config.MessageCount)
	fmt.Printf("并发数: %d\n", config.Concurrency)
}

// 运行集成测试
func runIntegrationTest(config *IntegrationTestConfig) (*TestResult, error) {
	result := &TestResult{
		Errors: make([]error, 0),
	}

	// 1. 启动NameServer
	fmt.Println("\n=== 步骤1: 启动NameServer ===")
	nameServerAddr := fmt.Sprintf("127.0.0.1:%d", config.NameServerPort)
	ns, err := startNameServer(config.NameServerPort)
	if err != nil {
		return nil, fmt.Errorf("启动NameServer失败: %v", err)
	}
	defer ns.Stop()
	fmt.Printf("✓ NameServer启动成功: %s\n", nameServerAddr)

	// 等待NameServer就绪
	time.Sleep(2 * time.Second)

	// 2. 启动Broker
	fmt.Println("\n=== 步骤2: 启动Broker ===")
	brokerAddr := fmt.Sprintf("127.0.0.1:%d", config.BrokerPort)
	br, err := startBroker(config.BrokerPort, nameServerAddr)
	if err != nil {
		return nil, fmt.Errorf("启动Broker失败: %v", err)
	}
	defer br.Stop()
	fmt.Printf("✓ Broker启动成功: %s\n", brokerAddr)

	// 等待Broker注册到NameServer
	time.Sleep(3 * time.Second)

	// 3. 创建Topic
	fmt.Println("\n=== 步骤3: 创建测试Topic ===")
	topicName := "IntegrationTestTopic"
	if err := createTestTopic(nameServerAddr, topicName); err != nil {
		return nil, fmt.Errorf("创建Topic失败: %v", err)
	}
	fmt.Printf("✓ Topic创建成功: %s\n", topicName)

	// 4. 启动消费者
	fmt.Println("\n=== 步骤4: 启动消费者 ===")
	listener := &IntegrationTestListener{
		messages:  make([]*client.MessageExt, 0),
		latencies: make([]time.Duration, 0),
	}
	consumer, err := startConsumer(nameServerAddr, topicName, listener)
	if err != nil {
		return nil, fmt.Errorf("启动消费者失败: %v", err)
	}
	defer consumer.Stop()
	fmt.Println("✓ 消费者启动成功")

	// 等待消费者就绪
	time.Sleep(2 * time.Second)

	// 5. 启动生产者并发送消息
	fmt.Println("\n=== 步骤5: 启动生产者并发送消息 ===")
	producer, err := startProducer(nameServerAddr)
	if err != nil {
		return nil, fmt.Errorf("启动生产者失败: %v", err)
	}
	defer producer.Shutdown()
	fmt.Println("✓ 生产者启动成功")

	// 6. 执行消息发送测试
	fmt.Println("\n=== 步骤6: 执行消息发送测试 ===")
	startTime := time.Now()
	sentCount, sendErrors := sendMessages(producer, topicName, config.MessageCount, config.Concurrency)
	sendDuration := time.Since(startTime)

	result.TotalSent = sentCount
	result.Errors = append(result.Errors, sendErrors...)

	fmt.Printf("✓ 消息发送完成: %d/%d\n", sentCount, config.MessageCount)

	// 7. 等待消息消费完成
	fmt.Println("\n=== 步骤7: 等待消息消费完成 ===")
	waitForConsumption(listener, config.MessageCount, 30*time.Second)

	result.TotalReceived = listener.GetReceivedCount()
	result.SuccessRate = float64(result.TotalReceived) / float64(result.TotalSent) * 100
	result.AvgLatency = listener.GetAverageLatency()
	result.Throughput = float64(result.TotalSent) / sendDuration.Seconds()

	fmt.Printf("✓ 消息消费完成: %d\n", result.TotalReceived)

	// 8. 验证消息完整性
	fmt.Println("\n=== 步骤8: 验证消息完整性 ===")
	if err := verifyMessageIntegrity(listener, config.MessageCount); err != nil {
		result.Errors = append(result.Errors, err)
		fmt.Printf("⚠️ 消息完整性验证失败: %v\n", err)
	} else {
		fmt.Println("✓ 消息完整性验证通过")
	}

	return result, nil
}

// 启动NameServer
func startNameServer(port int) (*nameserver.NameServer, error) {
	config := &nameserver.Config{
		ListenPort: port,
	}

	ns := nameserver.NewNameServer(config)
	if err := ns.Start(); err != nil {
		return nil, err
	}

	return ns, nil
}

// 启动Broker
func startBroker(port int, nameServerAddr string) (*broker.Broker, error) {
	config := &broker.Config{
		BrokerName:     "IntegrationTestBroker",
		ListenPort:     port,
		NameServerAddr: nameServerAddr,
	}

	br := broker.NewBroker(config)
	if err := br.Start(); err != nil {
		return nil, err
	}

	return br, nil
}

// 创建测试Topic
func createTestTopic(nameServerAddr, topicName string) error {
	// 这里应该实现实际的Topic创建逻辑
	// 目前返回nil表示创建成功
	fmt.Printf("模拟创建Topic: %s\n", topicName)
	return nil
}

// 启动消费者
func startConsumer(nameServerAddr, topicName string, listener *IntegrationTestListener) (*client.Consumer, error) {
	config := &client.ConsumerConfig{
		GroupName:      "IntegrationTestConsumerGroup",
		NameServerAddr: nameServerAddr,
		ConsumeFromWhere: client.ConsumeFromFirstOffset,
		MessageModel:   client.Clustering,
		PullInterval:   1 * time.Second,
	}

	consumer := client.NewConsumer(config)
	consumer.Subscribe(topicName, "*", listener)

	if err := consumer.Start(); err != nil {
		return nil, err
	}

	return consumer, nil
}

// 启动生产者
func startProducer(nameServerAddr string) (*client.Producer, error) {
	producer := client.NewProducer("IntegrationTestProducerGroup")
	producer.SetNameServers([]string{nameServerAddr})

	if err := producer.Start(); err != nil {
		return nil, err
	}

	return producer, nil
}

// 发送消息
func sendMessages(producer *client.Producer, topicName string, messageCount, concurrency int) (int, []error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	sentCount := 0
	errors := make([]error, 0)

	// 创建信号量控制并发
	sem := make(chan struct{}, concurrency)

	for i := 0; i < messageCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			sem <- struct{}{} // 获取信号量
			defer func() { <-sem }() // 释放信号量

			// 创建消息
			msg := client.NewMessage(
				topicName,
				[]byte(fmt.Sprintf("集成测试消息 #%d", index+1)),
			).SetTags("INTEGRATION_TEST").SetKeys(fmt.Sprintf("test_key_%d", index+1)).SetProperty("messageIndex", fmt.Sprintf("%d", index+1)).SetProperty("sendTime", time.Now().Format(time.RFC3339Nano))

			// 发送消息
			_, err := producer.SendSync(msg)

			mu.Lock()
			if err != nil {
				errors = append(errors, fmt.Errorf("发送消息 #%d 失败: %v", index+1, err))
			} else {
				sentCount++
				if sentCount%100 == 0 {
					fmt.Printf("[生产者] 已发送 %d 条消息\n", sentCount)
				}
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	return sentCount, errors
}

// 等待消息消费完成
func waitForConsumption(listener *IntegrationTestListener, expectedCount int, timeout time.Duration) {
	startTime := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			received := listener.GetReceivedCount()
			fmt.Printf("[等待消费] 已接收: %d/%d\n", received, expectedCount)

			if received >= expectedCount {
				fmt.Println("✓ 所有消息消费完成")
				return
			}

			if time.Since(startTime) > timeout {
				fmt.Printf("⚠️ 等待超时，已接收: %d/%d\n", received, expectedCount)
				return
			}
		}
	}
}

// 验证消息完整性
func verifyMessageIntegrity(listener *IntegrationTestListener, expectedCount int) error {
	received := listener.GetReceivedCount()
	if received != expectedCount {
		return fmt.Errorf("消息数量不匹配: 期望 %d, 实际 %d", expectedCount, received)
	}

	// 这里可以添加更多的完整性检查
	// 例如：检查消息内容、顺序、重复等

	return nil
}

// 打印测试结果
func printTestResult(result *TestResult) {
	fmt.Println("\n=== 集成测试结果 ===")
	fmt.Printf("发送消息数: %d\n", result.TotalSent)
	fmt.Printf("接收消息数: %d\n", result.TotalReceived)
	fmt.Printf("成功率: %.2f%%\n", result.SuccessRate)
	fmt.Printf("平均延迟: %v\n", result.AvgLatency)
	fmt.Printf("吞吐量: %.2f 条/秒\n", result.Throughput)

	if len(result.Errors) > 0 {
		fmt.Printf("\n错误数量: %d\n", len(result.Errors))
		fmt.Println("错误详情:")
		for i, err := range result.Errors {
			if i < 5 { // 只显示前5个错误
				fmt.Printf("  %d. %v\n", i+1, err)
			}
		}
		if len(result.Errors) > 5 {
			fmt.Printf("  ... 还有 %d 个错误\n", len(result.Errors)-5)
		}
	}

	// 评估测试结果
	fmt.Println("\n=== 测试评估 ===")
	if result.SuccessRate >= 99.0 {
		fmt.Println("✓ 测试结果: 优秀")
	} else if result.SuccessRate >= 95.0 {
		fmt.Println("✓ 测试结果: 良好")
	} else if result.SuccessRate >= 90.0 {
		fmt.Println("⚠️ 测试结果: 一般")
	} else {
		fmt.Println("✗ 测试结果: 需要改进")
	}

	if result.AvgLatency < 10*time.Millisecond {
		fmt.Println("✓ 延迟表现: 优秀")
	} else if result.AvgLatency < 50*time.Millisecond {
		fmt.Println("✓ 延迟表现: 良好")
	} else {
		fmt.Println("⚠️ 延迟表现: 需要优化")
	}

	if result.Throughput > 1000 {
		fmt.Println("✓ 吞吐量表现: 优秀")
	} else if result.Throughput > 500 {
		fmt.Println("✓ 吞吐量表现: 良好")
	} else {
		fmt.Println("⚠️ 吞吐量表现: 需要优化")
	}

	fmt.Println("\n=== 集成测试完成 ===")
}