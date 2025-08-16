//go:build basic_consumer

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

// 自定义消息监听器
type MyMessageListener struct{}

func (l *MyMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	for _, msg := range msgs {
		log.Printf("收到消息: topic=%s, tags=%s, keys=%s, body=%s", msg.Topic, msg.Tags, msg.Keys, string(msg.Body))
	}
	return client.ConsumeSuccess
}

func main() {
	log.Println("=== 基础消费者示例 (PushConsumer) ===")

	// 创建Push消费者（推荐）
	consumer := client.NewPushConsumer("basic_consumer_group")
	consumer.Consumer.SetNameServerAddr("127.0.0.1:9876")
	consumer.SetConsumeFromWhere(client.ConsumeFromLastOffset)
	consumer.SetMessageModel(client.Clustering)
	consumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})

	// 注册监听器
	consumer.RegisterMessageListener(&MyMessageListener{})

	// 订阅Topic
	if err := consumer.Consumer.Subscribe("TestTopic", "*", &MyMessageListener{}); err != nil {
		log.Fatalf("订阅失败: %v", err)
	}

	// 启动消费者
	if err := consumer.Start(); err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	defer consumer.Stop()
	log.Println("消费者已启动，等待消息...")

	// 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("准备退出，停止消费者...")
	time.Sleep(500 * time.Millisecond)
}