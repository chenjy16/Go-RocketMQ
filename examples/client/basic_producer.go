//go:build basic_producer

package main

import (
	"log"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
	log.Println("=== 基础生产者示例 ===")

	// 创建生产者
	producer := client.NewProducer("basic_producer_group")

	// 设置NameServer地址
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	if err := producer.Start(); err != nil {
		log.Fatalf("启动生产者失败: %v", err)
	}
	defer producer.Shutdown()

	log.Println("生产者启动成功")

	// 示例1: 同步发送普通消息
	log.Println("\n--- 同步发送消息 ---")
	msg1 := client.NewMessage("TestTopic", []byte("Hello RocketMQ - 同步消息"))
	msg1.SetTags("TagA")
	msg1.SetKeys("OrderID_001")
	msg1.SetProperty("userId", "12345")
	msg1.SetProperty("source", "web")

	result, err := producer.SendSync(msg1)
	if err != nil {
		log.Printf("同步发送失败: %v", err)
	} else {
		log.Printf("同步发送成功: MsgId=%s, QueueOffset=%d", result.MsgId, result.QueueOffset)
	}

	// 示例2: 异步发送消息
	log.Println("\n--- 异步发送消息 ---")
	msg2 := client.NewMessage("TestTopic", []byte("Hello RocketMQ - 异步消息"))
	msg2.SetTags("TagB")
	msg2.SetKeys("OrderID_002")

	err = producer.SendAsync(msg2, func(result *client.SendResult, err error) {
		if err != nil {
			log.Printf("异步发送失败: %v", err)
		} else {
			log.Printf("异步发送成功: MsgId=%s, QueueOffset=%d", result.MsgId, result.QueueOffset)
		}
	})

	if err != nil {
		log.Printf("提交异步发送失败: %v", err)
	}

	// 示例3: 单向发送消息（不关心结果）
	log.Println("\n--- 单向发送消息 ---")
	msg3 := client.NewMessage("TestTopic", []byte("Hello RocketMQ - 单向消息"))
	msg3.SetTags("TagC")
	msg3.SetKeys("OrderID_003")

	err = producer.SendOneway(msg3)
	if err != nil {
		log.Printf("单向发送失败: %v", err)
	} else {
		log.Println("单向发送成功")
	}

	// 示例4: 发送延时消息
	log.Println("\n--- 发送延时消息 ---")
	msg4 := client.NewMessage("DelayTopic", []byte("Hello RocketMQ - 延时消息"))
	msg4.SetTags("DelayTag")
	msg4.SetKeys("DelayOrder_001")
	msg4.SetDelayTimeLevel(client.DelayLevel10s) // 延时10秒

	result, err = producer.SendSync(msg4)
	if err != nil {
		log.Printf("延时消息发送失败: %v", err)
	} else {
		log.Printf("延时消息发送成功: MsgId=%s", result.MsgId)
	}

	// 等待异步发送完成
	time.Sleep(2 * time.Second)
	log.Println("\n基础生产者示例完成")
}