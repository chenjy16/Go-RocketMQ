package main

import (
	"fmt"
	"log"
	"time"

	"go-rocketmq/pkg/client"
	"go-rocketmq/pkg/common"
)

func main() {
	// 创建生产者
	producer := client.NewProducer("test-producer-group")
	
	// 设置NameServer地址
	producer.SetNameServers([]string{"localhost:9876"})
	
	// 启动生产者
	if err := producer.Start(); err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()

	log.Println("Producer started successfully")

	// 发送消息示例
	for i := 0; i < 10; i++ {
		// 创建消息
		msg := common.NewMessage("TestTopic", []byte(fmt.Sprintf("Hello Go-RocketMQ %d", i)))
		msg.SetTags("test-tag")
		msg.SetKeys(fmt.Sprintf("key-%d", i))
		msg.SetProperty("orderId", fmt.Sprintf("order-%d", i))

		// 同步发送
		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("Failed to send message %d: %v", i, err)
			continue
		}

		log.Printf("Message %d sent successfully: msgId=%s, queueOffset=%d", 
			i, result.MsgId, result.QueueOffset)

		time.Sleep(1 * time.Second)
	}

	// 异步发送示例
	log.Println("Sending async messages...")
	for i := 0; i < 5; i++ {
		msg := common.NewMessage("TestTopic", []byte(fmt.Sprintf("Async message %d", i)))
		
		err := producer.SendAsync(msg, func(result *common.SendResult, err error) {
			if err != nil {
				log.Printf("Async message failed: %v", err)
			} else {
				log.Printf("Async message sent: msgId=%s", result.MsgId)
			}
		})
		
		if err != nil {
			log.Printf("Failed to send async message: %v", err)
		}
	}

	// 等待异步消息完成
	time.Sleep(3 * time.Second)

	// 单向发送示例
	log.Println("Sending oneway messages...")
	for i := 0; i < 3; i++ {
		msg := common.NewMessage("OrderTopic", []byte(fmt.Sprintf("Oneway message %d", i)))
		
		err := producer.SendOneway(msg)
		if err != nil {
			log.Printf("Failed to send oneway message: %v", err)
		} else {
			log.Printf("Oneway message %d sent", i)
		}
	}

	log.Println("All messages sent, producer will shutdown")
}