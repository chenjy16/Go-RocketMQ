package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chenjy16/go-rocketmq-client"
)

func main() {
	fmt.Println("=== Go-RocketMQ Enhanced 消费者示例 ===")

	// 演示不同类型的消费者
	demoBasicConsumer()
	demoEnhancedFeatures()
}

// 基础消费者演示
func demoBasicConsumer() {
	fmt.Println("\n--- 基础消费者演示 ---")

	// 创建消费者配置
	config := &client.ConsumerConfig{
		GroupName:        "example_consumer_group",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: client.ConsumeFromLastOffset,
		MessageModel:     client.Clustering,
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

	// 启动消费者
	if err := consumer.Start(); err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	defer consumer.Stop()

	fmt.Println("基础消费者启动成功")

	// 运行一段时间后停止
	time.Sleep(5 * time.Second)
	fmt.Println("基础消费者演示完成")
}

// 增强功能演示
func demoEnhancedFeatures() {
	fmt.Println("\n--- 增强功能演示 ---")

	// 创建增强消费者
	consumer2 := client.NewConsumer(&client.ConsumerConfig{
		GroupName:        "enhanced_consumer_group",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: client.ConsumeFromLastOffset,
		MessageModel:     client.Clustering,
		ConsumeThreadMin: 5,
		ConsumeThreadMax: 10,
		PullInterval:     100 * time.Millisecond,
		PullBatchSize:    32,
		ConsumeTimeout:   15 * time.Second,
	})

	// 设置负载均衡策略
	consumer2.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})
	fmt.Println("设置平均分配负载均衡策略")

	// 启用消息追踪
	consumer2.EnableTrace("trace_topic", "trace_group")
	fmt.Println("启用消息追踪功能")

	// 订阅消息
	err := consumer2.Subscribe("TestTopic", "*", &EnhancedMessageListener{})
	if err != nil {
		log.Fatalf("增强消费者订阅失败: %v", err)
	}

	// 启动增强消费者
	if err := consumer2.Start(); err != nil {
		log.Fatalf("启动增强消费者失败: %v", err)
	}
	defer consumer2.Stop()

	fmt.Println("增强消费者启动成功")

	// 运行一段时间
	time.Sleep(5 * time.Second)

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("\n按 Ctrl+C 停止所有消费者")
	<-sigChan
	fmt.Println("\n正在停止所有消费者...")
}

// EnhancedMessageListener 增强消息监听器
type EnhancedMessageListener struct{}

func (l *EnhancedMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	for _, msg := range msgs {
		fmt.Printf("[增强消费者] 收到消息 - Topic: %s, Tags: %s, MsgId: %s\n",
			msg.Topic, msg.Tags, msg.MsgId)
		fmt.Printf("[增强消费者] 消息内容: %s\n", string(msg.Body))
		fmt.Printf("[增强消费者] 队列信息 - QueueId: %d, QueueOffset: %d\n",
			msg.QueueId, msg.QueueOffset)

		// 模拟消息处理
		time.Sleep(10 * time.Millisecond)
	}

	return client.ConsumeSuccess
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

func (l *AllMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
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

	return client.ConsumeSuccess
}

// TagAMessageListener 处理TagA消息的监听器
type TagAMessageListener struct{}

func (l *TagAMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	for _, msg := range msgs {
		tagAMessageCount++
		fmt.Printf("[TagA消息] 收到特定标签消息 - MsgId: %s\n", msg.MsgId)
		fmt.Printf("[TagA消息] 消息内容: %s\n", string(msg.Body))
		fmt.Println("[TagA消息] 这是TagA的专用处理逻辑")
		fmt.Println("---")

		// TagA消息的特殊处理逻辑
		processTagAMessage(msg)
	}

	return client.ConsumeSuccess
}

// 处理TagA消息的特殊逻辑
func processTagAMessage(msg *client.MessageExt) {
	// 这里可以添加TagA消息的特殊处理逻辑
	// 例如：特殊的业务处理、数据库操作等
	fmt.Printf("[TagA处理] 执行TagA消息的特殊业务逻辑 - MsgId: %s\n", msg.MsgId)

	// 模拟业务处理时间
	time.Sleep(20 * time.Millisecond)
}