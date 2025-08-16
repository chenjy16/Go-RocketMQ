//go:build ordered_consumer

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
	log.Println("=== 顺序消费者示例 (PushConsumer - Orderly) ===")

	// 创建Push消费者
	consumer := client.NewPushConsumer("ordered_consumer_group")
	consumer.Consumer.SetNameServerAddr("127.0.0.1:9876")
	consumer.SetMessageModel(client.Clustering)
	consumer.SetConsumeFromWhere(client.ConsumeFromLastOffset)

	// 设置为顺序消费
	consumer.SetConsumeType(client.ConsumeOrderly)

	// 定义顺序消息监听器
	listener := client.MessageListenerOrderly(func(msgs []*client.MessageExt, ctx *client.ConsumeOrderlyContext) client.ConsumeResult {
		for _, msg := range msgs {
			log.Printf("[Orderly] 收到消息: topic=%s, queueId=%d, tags=%s, keys=%s, shardingKey=%s, body=%s", 
				msg.Topic, msg.QueueId, msg.Tags, msg.Keys, msg.ShardingKey, string(msg.Body))
		}
		// 模拟业务处理耗时
		time.Sleep(100 * time.Millisecond)
		return client.ConsumeSuccess
	})

	// 注册监听器
	consumer.RegisterMessageListener(listener)

	// 订阅与顺序生产者示例配套的主题
	if err := consumer.Consumer.Subscribe("OrderTopic", "*", listener); err != nil {
		log.Fatalf("订阅 OrderTopic 失败: %v", err)
	}
	if err := consumer.Consumer.Subscribe("UserActionTopic", "*", listener); err != nil {
		log.Fatalf("订阅 UserActionTopic 失败: %v", err)
	}
	if err := consumer.Consumer.Subscribe("GameEventTopic", "*", listener); err != nil {
		log.Fatalf("订阅 GameEventTopic 失败: %v", err)
	}

	// 启动消费者
	if err := consumer.Start(); err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	defer consumer.Stop()
	log.Println("顺序消费者已启动，等待消息...")

	// 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("准备退出，停止消费者...")
	time.Sleep(300 * time.Millisecond)
}