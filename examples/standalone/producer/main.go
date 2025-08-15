package main

import (
	"fmt"
	"log"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
	fmt.Println("=== Go-RocketMQ 独立客户端生产者示例 ===")

	// 创建生产者实例
	producer := client.NewProducer("example_producer_group")

	// 设置 NameServer 地址
	producer.SetNameServers([]string{"localhost:9876"})

	// 启动生产者
	if err := producer.Start(); err != nil {
		log.Fatalf("启动生产者失败: %v", err)
	}
	defer producer.Shutdown()

	fmt.Println("生产者启动成功，开始发送消息...")

	// 发送同步消息
	sendSyncMessages(producer)

	// 发送异步消息
	sendAsyncMessages(producer)

	// 发送单向消息
	sendOnewayMessages(producer)

	fmt.Println("所有消息发送完成")
}

// 发送同步消息
func sendSyncMessages(producer *client.Producer) {
	fmt.Println("\n--- 发送同步消息 ---")

	for i := 0; i < 3; i++ {
		msg := &client.Message{
			Topic: "TestTopic",
			Body:  []byte(fmt.Sprintf("同步消息 #%d - %s", i+1, time.Now().Format("15:04:05"))),
		}

		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("发送同步消息失败: %v", err)
			continue
		}

		fmt.Printf("同步消息发送成功: MsgId=%s, QueueId=%d\n", result.MsgId, result.MessageQueue.QueueId)
		time.Sleep(100 * time.Millisecond)
	}
}

// 发送异步消息
func sendAsyncMessages(producer *client.Producer) {
	fmt.Println("\n--- 发送异步消息 ---")

	for i := 0; i < 3; i++ {
		msg := &client.Message{
			Topic: "TestTopic",
			Body:  []byte(fmt.Sprintf("异步消息 #%d - %s", i+1, time.Now().Format("15:04:05"))),
		}

		err := producer.SendAsync(msg, func(result *client.SendResult, err error) {
			if err != nil {
				log.Printf("异步消息发送失败: %v", err)
				return
			}
			fmt.Printf("异步消息发送成功: MsgId=%s, QueueId=%d\n", result.MsgId, result.MessageQueue.QueueId)
		})

		if err != nil {
			log.Printf("提交异步消息失败: %v", err)
		}
	}

	// 等待异步消息处理完成
	time.Sleep(2 * time.Second)
}

// 发送单向消息
func sendOnewayMessages(producer *client.Producer) {
	fmt.Println("\n--- 发送单向消息 ---")

	for i := 0; i < 3; i++ {
		msg := &client.Message{
			Topic: "TestTopic",
			Body:  []byte(fmt.Sprintf("单向消息 #%d - %s", i+1, time.Now().Format("15:04:05"))),
		}

		err := producer.SendOneway(msg)
		if err != nil {
			log.Printf("发送单向消息失败: %v", err)
			continue
		}

		fmt.Printf("单向消息 #%d 发送完成\n", i+1)
		time.Sleep(100 * time.Millisecond)
	}
}