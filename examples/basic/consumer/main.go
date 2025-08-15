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

func main() {
	fmt.Println("=== Go-RocketMQ 消费者示例 ===")

	// 创建消费者配置
	config := &client.ConsumerConfig{
		GroupName:        "example_consumer_group",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: common.ConsumeFromLastOffset,
		MessageModel:     common.Clustering,
		ConsumeThreadMin: 5,
		ConsumeThreadMax: 10,
		PullInterval:     100 * time.Millisecond,
		PullBatchSize:    32,
		ConsumeTimeout:   15 * time.Second,
	}

	// 创建消费者实例
	consumer := client.NewConsumer(config)

	fmt.Println("消费者创建成功")

	// 订阅Topic - 消费所有消息
	err := consumer.Subscribe("TestTopic", "*", &AllMessageListener{})
	if err != nil {
		log.Fatalf("订阅Topic失败: %v", err)
	}
	fmt.Println("订阅 TestTopic 成功 (所有消息)")

	// 订阅Topic - 只消费TagA的消息
	err = consumer.Subscribe("TestTopic", "TagA", &TagAMessageListener{})
	if err != nil {
		log.Printf("订阅TagA失败: %v", err)
	}

	// 启动消费者
	if err := consumer.Start(); err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	defer consumer.Stop()

	fmt.Println("消费者启动成功，开始消费消息...")
	fmt.Println("按 Ctrl+C 停止消费者")

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 显示消费统计
	go showConsumeStats()

	<-sigChan
	fmt.Println("\n正在停止消费者...")
}

// 消费统计计数器
var (
	allMessageCount int64
	tagAMessageCount int64
)

// 显示消费统计
func showConsumeStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Printf("\n[统计] 总消息数: %d, TagA消息数: %d\n", allMessageCount, tagAMessageCount)
		}
	}
}

// AllMessageListener 处理所有消息的监听器
type AllMessageListener struct{}

func (l *AllMessageListener) ConsumeMessage(msgs []*common.MessageExt) common.ConsumeResult {
	for _, msg := range msgs {
		allMessageCount++
		fmt.Printf("[全部消息] 收到消息 - Topic: %s, Tags: %s, Keys: %s, MsgId: %s\n",
			msg.Topic, msg.Tags, msg.Keys, msg.MsgId)
		fmt.Printf("[全部消息] 消息内容: %s\n", string(msg.Body))
		fmt.Printf("[全部消息] 队列信息 - QueueId: %d, QueueOffset: %d\n",
			msg.QueueId, msg.QueueOffset)
		fmt.Printf("[全部消息] 时间信息 - Born: %s, Store: %s\n",
			msg.BornTimestamp.Format("2006-01-02 15:04:05"),
			msg.StoreTimestamp.Format("2006-01-02 15:04:05"))

		// 打印消息属性
		if len(msg.Properties) > 0 {
			fmt.Printf("[全部消息] 消息属性: ")
			for key, value := range msg.Properties {
				fmt.Printf("%s=%s ", key, value)
			}
			fmt.Println()
		}
		fmt.Println("---")

		// 模拟消息处理时间
		time.Sleep(10 * time.Millisecond)
	}

	return common.ConsumeSuccess
}

// TagAMessageListener 处理TagA消息的监听器
type TagAMessageListener struct{}

func (l *TagAMessageListener) ConsumeMessage(msgs []*common.MessageExt) common.ConsumeResult {
	for _, msg := range msgs {
		tagAMessageCount++
		fmt.Printf("[TagA消息] 收到特定标签消息 - MsgId: %s\n", msg.MsgId)
		fmt.Printf("[TagA消息] 消息内容: %s\n", string(msg.Body))
		fmt.Println("[TagA消息] 这是TagA的专用处理逻辑")
		fmt.Println("---")

		// TagA消息的特殊处理逻辑
		processTagAMessage(msg)
	}

	return common.ConsumeSuccess
}

// 处理TagA消息的特殊逻辑
func processTagAMessage(msg *common.MessageExt) {
	// 这里可以添加TagA消息的特殊处理逻辑
	// 例如：特殊的业务处理、数据库操作等
	fmt.Printf("[TagA处理] 执行TagA消息的特殊业务逻辑 - MsgId: %s\n", msg.MsgId)

	// 模拟业务处理时间
	time.Sleep(20 * time.Millisecond)
}