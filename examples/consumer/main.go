package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-rocketmq/pkg/client"
	"go-rocketmq/pkg/common"
)

// TestMessageListener 测试消息监听器
type TestMessageListener struct {
	name string
}

// ConsumeMessage 实现MessageListener接口
func (l *TestMessageListener) ConsumeMessage(msgs []*common.MessageExt) common.ConsumeResult {
	for _, msg := range msgs {
		log.Printf("[%s] Received message: Topic=%s, Tags=%s, Keys=%s, Body=%s", 
			l.name, msg.Topic, msg.Tags, msg.Keys, string(msg.Body))
		
		// 模拟消息处理
		time.Sleep(100 * time.Millisecond)
	}
	
	return common.ConsumeSuccess
}

func main() {
	fmt.Println("=== RocketMQ Consumer Example ===")
	
	// 创建消费者配置
	config := &client.ConsumerConfig{
		GroupName:        "TestConsumerGroup",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: common.ConsumeFromLastOffset,
		MessageModel:     common.Clustering,
		ConsumeThreadMin: 2,
		ConsumeThreadMax: 4,
		PullInterval:     2 * time.Second,
		PullBatchSize:    16,
		ConsumeTimeout:   10 * time.Minute,
	}
	
	// 创建消费者
	consumer := client.NewConsumer(config)
	
	// 创建消息监听器
	testListener := &TestMessageListener{name: "TestListener"}
	orderListener := &TestMessageListener{name: "OrderListener"}
	
	// 订阅Topic
	err := consumer.Subscribe("TestTopic", "*", testListener)
	if err != nil {
		log.Fatalf("Failed to subscribe TestTopic: %v", err)
	}
	
	err = consumer.Subscribe("OrderTopic", "order", orderListener)
	if err != nil {
		log.Fatalf("Failed to subscribe OrderTopic: %v", err)
	}
	
	// 启动消费者
	err = consumer.Start()
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}
	
	fmt.Printf("Consumer started successfully!\n")
	fmt.Printf("Consumer Group: %s\n", config.GroupName)
	fmt.Printf("NameServer: %s\n", config.NameServerAddr)
	fmt.Printf("Subscriptions:\n")
	
	subscriptions := consumer.GetSubscriptions()
	for topic, sub := range subscriptions {
		fmt.Printf("  - Topic: %s, Expression: %s\n", topic, sub.SubExpression)
	}
	
	fmt.Printf("Press Ctrl+C to stop...\n")
	
	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	fmt.Println("\nShutting down consumer...")
	
	// 停止消费者
	err = consumer.Stop()
	if err != nil {
		log.Printf("Error stopping consumer: %v", err)
	}
	
	fmt.Println("Consumer stopped")
}