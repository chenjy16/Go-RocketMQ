package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/chenjy16/go-rocketmq-client"
)

func main() {
	fmt.Println("=== Go-RocketMQ Enhanced 简单演示 ===")
	fmt.Println("本演示将展示增强的生产者和消费者功能，包括消息追踪、负载均衡等特性")

	var wg sync.WaitGroup

	// 启动增强消费者
	wg.Add(1)
	go func() {
		defer wg.Done()
		startEnhancedConsumer()
	}()

	// 等待消费者启动
	time.Sleep(2 * time.Second)

	// 启动增强生产者
	wg.Add(1)
	go func() {
		defer wg.Done()
		startEnhancedProducer()
	}()

	// 等待所有goroutine完成
	wg.Wait()
	fmt.Println("\n增强演示完成")
}

// 启动增强生产者
func startEnhancedProducer() {
	fmt.Println("\n[增强生产者] 正在启动...")

	// 创建生产者
	producer := client.NewProducer("enhanced_demo_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启用消息追踪
	producer.EnableTrace("trace_topic", "trace_group")
	fmt.Println("[增强生产者] 启用消息追踪功能")

	if err := producer.Start(); err != nil {
		log.Printf("[增强生产者] 启动失败: %v", err)
		return
	}
	defer producer.Shutdown()

	fmt.Println("[增强生产者] 启动成功")

	// 发送不同类型的消息
	sendEnhancedDemoMessages(producer)

	fmt.Println("[增强生产者] 所有消息发送完成")
}

// 发送增强演示消息
func sendEnhancedDemoMessages(producer *client.Producer) {
	// 1. 发送普通消息
	fmt.Println("\n[生产者] 发送普通消息...")
	for i := 0; i < 3; i++ {
		msg := client.NewMessage(
			"DemoTopic",
			[]byte(fmt.Sprintf("普通消息 #%d: Hello Go-RocketMQ! Time: %s",
				i+1, time.Now().Format("15:04:05"))),
		)

		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("[生产者] 发送失败: %v", err)
			continue
		}

		fmt.Printf("[生产者] 普通消息发送成功 - MsgId: %s\n", result.MsgId)
		time.Sleep(500 * time.Millisecond)
	}

	// 2. 发送带标签的消息
	fmt.Println("\n[生产者] 发送带标签的消息...")
	tags := []string{"ORDER", "PAYMENT", "NOTIFICATION"}
	for i, tag := range tags {
		msg := client.NewMessage(
			"DemoTopic",
			[]byte(fmt.Sprintf("业务消息 #%d: %s处理 - %s",
				i+1, tag, time.Now().Format("15:04:05"))),
		).SetTags(tag)

		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("[生产者] 发送失败: %v", err)
			continue
		}

		fmt.Printf("[生产者] %s消息发送成功 - MsgId: %s\n", tag, result.MsgId)
		time.Sleep(500 * time.Millisecond)
	}

	// 3. 发送带属性的消息
	fmt.Println("\n[生产者] 发送带属性的消息...")
	for i := 0; i < 2; i++ {
		msg := client.NewMessage(
			"DemoTopic",
			[]byte(fmt.Sprintf("用户订单消息 #%d - %s",
				i+1, time.Now().Format("15:04:05"))),
		).SetKeys(fmt.Sprintf("order_%d", i+1)).SetProperty("userId", fmt.Sprintf("user_%d", i+1)).SetProperty("amount", fmt.Sprintf("%.2f", float64(100+i*50))).SetProperty("currency", "CNY")

		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("[生产者] 发送失败: %v", err)
			continue
		}

		fmt.Printf("[生产者] 订单消息发送成功 - MsgId: %s, Keys: %s\n",
			result.MsgId, msg.Keys)
		time.Sleep(500 * time.Millisecond)
	}

	// 4. 发送异步消息
	fmt.Println("\n[生产者] 发送异步消息...")
	for i := 0; i < 2; i++ {
		msg := client.NewMessage(
			"DemoTopic",
			[]byte(fmt.Sprintf("异步消息 #%d - %s",
				i+1, time.Now().Format("15:04:05"))),
		)

		err := producer.SendAsync(msg, func(result *client.SendResult, err error) {
			if err != nil {
				log.Printf("[生产者] 异步消息发送失败: %v", err)
				return
			}
			fmt.Printf("[生产者] 异步消息发送成功 - MsgId: %s\n", result.MsgId)
		})

		if err != nil {
			log.Printf("[生产者] 提交异步消息失败: %v", err)
		}

		time.Sleep(300 * time.Millisecond)
	}

	// 等待异步消息处理完成
	time.Sleep(2 * time.Second)
}

// 启动增强消费者
func startEnhancedConsumer() {
	fmt.Println("\n[增强消费者] 正在启动...")

	// 创建消费者配置
	config := &client.ConsumerConfig{
		GroupName:        "enhanced_demo_consumer_group",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: client.ConsumeFromLastOffset,
		MessageModel:     client.Clustering,
		ConsumeThreadMin: 3,
		ConsumeThreadMax: 5,
		PullInterval:     100 * time.Millisecond,
		PullBatchSize:    16,
		ConsumeTimeout:   10 * time.Second,
	}

	// 创建消费者
	consumer := client.NewConsumer(config)

	// 设置负载均衡策略
	consumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})
	fmt.Println("[增强消费者] 设置平均分配负载均衡策略")

	// 启用消息追踪
	consumer.EnableTrace("trace_topic", "trace_group")
	fmt.Println("[增强消费者] 启用消息追踪功能")

	// 订阅Topic
	err := consumer.Subscribe("DemoTopic", "*", &EnhancedDemoMessageListener{})
	if err != nil {
		log.Printf("[增强消费者] 订阅失败: %v", err)
		return
	}

	if err := consumer.Start(); err != nil {
		log.Printf("[增强消费者] 启动失败: %v", err)
		return
	}
	defer consumer.Stop()

	fmt.Println("[增强消费者] 启动成功，开始监听消息...")

	// 运行15秒后停止
	time.Sleep(15 * time.Second)
	fmt.Println("[增强消费者] 停止监听")
}

// EnhancedDemoMessageListener 增强演示消息监听器
type EnhancedDemoMessageListener struct{}

func (l *EnhancedDemoMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	for _, msg := range msgs {
		fmt.Printf("\n[增强消费者] 收到消息:\n")
		fmt.Printf("  Topic: %s\n", msg.Topic)
		fmt.Printf("  Tags: %s\n", msg.Tags)
		fmt.Printf("  Keys: %s\n", msg.Keys)
		fmt.Printf("  MsgId: %s\n", msg.MsgId)
		fmt.Printf("  内容: %s\n", string(msg.Body))
		fmt.Printf("  队列: QueueId=%d, Offset=%d\n", msg.QueueId, msg.QueueOffset)
		fmt.Printf("  时间: %s\n", msg.StoreTimestamp.Format("2006-01-02 15:04:05"))
		fmt.Printf("  [追踪] 消息已被追踪记录\n")

		// 打印消息属性
		if len(msg.Properties) > 0 {
			fmt.Printf("  属性: ")
			for key, value := range msg.Properties {
				fmt.Printf("%s=%s ", key, value)
			}
			fmt.Println()
		}

		// 根据消息标签进行不同处理
		switch msg.Tags {
		case "ORDER":
			fmt.Println("  [业务处理] 处理订单消息")
		case "PAYMENT":
			fmt.Println("  [业务处理] 处理支付消息")
		case "NOTIFICATION":
			fmt.Println("  [业务处理] 处理通知消息")
		default:
			fmt.Println("  [业务处理] 处理普通消息")
		}

		// 模拟消息处理时间
		time.Sleep(100 * time.Millisecond)
		fmt.Println("  [增强消费者] 消息处理完成，追踪信息已记录")
	}

	return client.ConsumeSuccess
}