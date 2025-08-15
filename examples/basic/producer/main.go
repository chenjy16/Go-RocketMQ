package main

import (
	"fmt"
	"log"
	"time"

	"github.com/chenjy16/go-rocketmq-client"
)

func main() {
	fmt.Println("=== Go-RocketMQ 生产者示例 ===")

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

	// 发送带标签的消息
	fmt.Println("\n--- 发送带标签的消息 ---")
	sendTaggedMessages(producer)

	// 发送带属性的消息
	fmt.Println("\n--- 发送带属性的消息 ---")
	sendMessageWithProperties(producer)

	fmt.Println("\n所有消息发送完成")
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

// 发送带属性的消息
func sendMessageWithProperties(producer *client.Producer) {
	for i := 0; i < 3; i++ {
		// 创建带属性的消息
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("带属性的消息内容 #%d - %s", i+1, time.Now().Format("2006-01-02 15:04:05"))),
		).SetKeys(fmt.Sprintf("key_%d", i+1)).SetProperty("userId", fmt.Sprintf("user_%d", i+1)).SetProperty("orderId", fmt.Sprintf("order_%d", i+1)).SetProperty("priority", "high")

		// 发送消息
		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("发送带属性消息失败: %v", err)
			continue
		}

		fmt.Printf("带属性消息发送成功 - MsgId: %s, Keys: %s\n",
			result.MsgId, msg.Keys)
		time.Sleep(100 * time.Millisecond)
	}
}