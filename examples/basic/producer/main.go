package main

import (
	"fmt"
	"log"
	"time"

	"github.com/chenjy16/go-rocketmq-client"
)

func main() {
	fmt.Println("=== Go-RocketMQ Enhanced 生产者示例 ===")

	// 演示基础生产者功能
	demoBasicProducer()

	// 演示增强功能
	demoEnhancedFeatures()

	fmt.Println("\n所有演示完成")
}

// 基础生产者演示
func demoBasicProducer() {
	fmt.Println("\n--- 基础生产者演示 ---")

	// 创建生产者实例
	producer := client.NewProducer("example_producer_group")

	// 设置 NameServer 地址
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	if err := producer.Start(); err != nil {
		log.Fatalf("启动生产者失败: %v", err)
	}
	defer producer.Shutdown()

	fmt.Println("生产者启动成功")

	// 发送同步消息示例
	fmt.Println("\n--- 发送同步消息 ---")
	sendSyncMessages(producer)

	// 发送异步消息示例
	fmt.Println("\n--- 发送异步消息 ---")
	sendAsyncMessages(producer)

	// 发送单向消息示例
	fmt.Println("\n--- 发送单向消息 ---")
	sendOnewayMessages(producer)

	fmt.Println("基础生产者演示完成")
}

// 增强功能演示
func demoEnhancedFeatures() {
	fmt.Println("\n--- 增强功能演示 ---")

	// 创建增强生产者
	producer := client.NewProducer("enhanced_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启用消息追踪
	producer.EnableTrace("trace_topic", "trace_group")
	fmt.Println("启用消息追踪功能")

	if err := producer.Start(); err != nil {
		log.Fatalf("启动增强生产者失败: %v", err)
	}
	defer producer.Shutdown()

	// 发送延时消息
	fmt.Println("\n--- 发送延时消息 ---")
	sendDelayMessages(producer)

	// 发送批量消息
	fmt.Println("\n--- 发送批量消息 ---")
	sendBatchMessages(producer)

	// 发送顺序消息
	fmt.Println("\n--- 发送顺序消息 ---")
	sendOrderedMessages(producer)

	// 发送事务消息
	fmt.Println("\n--- 发送事务消息 ---")
	sendTransactionMessages(producer)
}

// 发送同步消息
func sendSyncMessages(producer *client.Producer) {
	for i := 0; i < 5; i++ {
		// 创建消息
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("同步消息内容 #%d - %s", i+1, time.Now().Format("2006-01-02 15:04:05"))),
		)

		// 发送同步消息
		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("发送同步消息失败: %v", err)
			continue
		}

		fmt.Printf("同步消息发送成功 - MsgId: %s, QueueOffset: %d\n",
			result.MsgId, result.QueueOffset)

		// 间隔发送
		time.Sleep(100 * time.Millisecond)
	}
}

// 发送异步消息
func sendAsyncMessages(producer *client.Producer) {
	for i := 0; i < 3; i++ {
		// 创建消息
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("异步消息内容 #%d - %s", i+1, time.Now().Format("2006-01-02 15:04:05"))),
		)

		// 发送异步消息
		err := producer.SendAsync(msg, func(result *client.SendResult, err error) {
			if err != nil {
				log.Printf("异步消息发送失败: %v", err)
				return
			}
			fmt.Printf("异步消息发送成功 - MsgId: %s, QueueOffset: %d\n",
				result.MsgId, result.QueueOffset)
		})

		if err != nil {
			log.Printf("提交异步消息失败: %v", err)
		}

		time.Sleep(50 * time.Millisecond)
	}

	// 等待异步消息处理完成
	time.Sleep(1 * time.Second)
}

// 发送单向消息
func sendOnewayMessages(producer *client.Producer) {
	for i := 0; i < 3; i++ {
		// 创建消息
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("单向消息内容 #%d - %s", i+1, time.Now().Format("2006-01-02 15:04:05"))),
		)

		// 发送单向消息
		err := producer.SendOneway(msg)
		if err != nil {
			log.Printf("发送单向消息失败: %v", err)
			continue
		}

		fmt.Printf("单向消息发送成功 #%d\n", i+1)
		time.Sleep(50 * time.Millisecond)
	}
}

// 发送带标签的消息
func sendTaggedMessages(producer *client.Producer) {
	tags := []string{"TagA", "TagB", "TagC"}

	for _, tag := range tags {
		// 创建带标签的消息
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("带标签的消息内容 - Tag: %s, Time: %s", tag, time.Now().Format("2006-01-02 15:04:05"))),
		).SetTags(tag)

		// 发送消息
		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("发送带标签消息失败: %v", err)
			continue
		}

		fmt.Printf("带标签消息发送成功 - Tag: %s, MsgId: %s\n", tag, result.MsgId)
		time.Sleep(100 * time.Millisecond)
	}
}

// 发送延时消息
func sendDelayMessages(producer *client.Producer) {
	for i := 0; i < 3; i++ {
		// 创建延时消息
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("延时消息内容 #%d - %s", i+1, time.Now().Format("2006-01-02 15:04:05"))),
		).SetDelayTimeLevel(3) // 延时10秒

		// 发送延时消息
		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("发送延时消息失败: %v", err)
			continue
		}

		fmt.Printf("延时消息发送成功 - MsgId: %s, 延时级别: %d\n",
			result.MsgId, 3)
		time.Sleep(100 * time.Millisecond)
	}
}

// 发送批量消息
func sendBatchMessages(producer *client.Producer) {
	// 创建批量消息
	var messages []*client.Message
	for i := 0; i < 5; i++ {
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("批量消息内容 #%d - %s", i+1, time.Now().Format("2006-01-02 15:04:05"))),
		).SetKeys(fmt.Sprintf("batch_key_%d", i+1))
		messages = append(messages, msg)
	}

	// 发送批量消息（逐个发送）
	for _, msg := range messages {
		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("发送批量消息失败: %v", err)
			continue
		}
		fmt.Printf("批量消息发送成功 - MsgId: %s\n", result.MsgId)
	}
	fmt.Printf("批量消息发送完成，总数量: %d\n", len(messages))
}

// 发送顺序消息
func sendOrderedMessages(producer *client.Producer) {
	orderId := "order_12345"
	for i := 0; i < 3; i++ {
		// 创建顺序消息
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("顺序消息内容 #%d - OrderId: %s - %s", i+1, orderId, time.Now().Format("2006-01-02 15:04:05"))),
		).SetKeys(orderId)

		// 发送顺序消息
		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("发送顺序消息失败: %v", err)
			continue
		}

		fmt.Printf("顺序消息发送成功 - MsgId: %s, QueueOffset: %d\n",
			result.MsgId, result.QueueOffset)
		time.Sleep(100 * time.Millisecond)
	}
}

// 发送事务消息
func sendTransactionMessages(producer *client.Producer) {
	// 创建事务生产者
	txProducer := client.NewTransactionProducer("tx_producer_group", &ExampleTransactionListener{})
	txProducer.SetNameServers([]string{"127.0.0.1:9876"})

	if err := txProducer.Start(); err != nil {
		log.Printf("启动事务生产者失败: %v", err)
		return
	}
	defer txProducer.Shutdown()

	for i := 0; i < 2; i++ {
		// 创建事务消息
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("事务消息内容 #%d - %s", i+1, time.Now().Format("2006-01-02 15:04:05"))),
		).SetKeys(fmt.Sprintf("tx_key_%d", i+1))

		// 发送事务消息
		result, err := txProducer.SendMessageInTransaction(msg, fmt.Sprintf("tx_arg_%d", i+1))
		if err != nil {
			log.Printf("发送事务消息失败: %v", err)
			continue
		}

		fmt.Printf("事务消息发送成功 - MsgId: %s, TransactionId: %s\n",
			result.MsgId, result.TransactionId)
		time.Sleep(500 * time.Millisecond)
	}
}

// 事务监听器示例
type ExampleTransactionListener struct{}

func (l *ExampleTransactionListener) ExecuteLocalTransaction(msg *client.Message, arg interface{}) client.LocalTransactionState {
	fmt.Printf("执行本地事务 - Keys: %s, Arg: %v\n", msg.Keys, arg)
	// 模拟本地事务执行
	time.Sleep(100 * time.Millisecond)
	// 这里可以执行实际的本地事务逻辑
	return client.CommitMessage
}

func (l *ExampleTransactionListener) CheckLocalTransaction(msg *client.MessageExt) client.LocalTransactionState {
	fmt.Printf("检查本地事务状态 - MsgId: %s\n", msg.MsgId)
	// 模拟检查本地事务状态
	// 这里应该检查实际的本地事务状态
	return client.CommitMessage
}