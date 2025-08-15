package main

import (
	"fmt"
	"log"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

// 示例事务监听器
type ExampleTransactionListener struct{}

func (l *ExampleTransactionListener) ExecuteLocalTransaction(msg *client.Message, arg interface{}) client.LocalTransactionState {
	fmt.Printf("执行本地事务: Topic=%s, Body=%s\n", msg.Topic, string(msg.Body))
	// 模拟本地事务执行
	time.Sleep(100 * time.Millisecond)
	// 假设事务成功
	return client.CommitMessage
}

func (l *ExampleTransactionListener) CheckLocalTransaction(msgExt *client.MessageExt) client.LocalTransactionState {
	fmt.Printf("检查本地事务状态: MsgId=%s\n", msgExt.MsgId)
	// 模拟事务状态检查
	return client.CommitMessage
}

// 示例消息监听器
type ExampleMessageListener struct{}

func (l *ExampleMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	for _, msg := range msgs {
		fmt.Printf("消费消息: Topic=%s, Tags=%s, Body=%s, MsgId=%s\n",
			msg.Topic, msg.Tags, string(msg.Body), msg.MsgId)
	}
	return client.ConsumeSuccess
}

// 示例消息队列选择器
type ExampleMessageQueueSelector struct{}

func (s *ExampleMessageQueueSelector) Select(mqs []*client.MessageQueue, msg *client.Message, arg interface{}) *client.MessageQueue {
	if len(mqs) == 0 {
		return nil
	}
	// 根据消息的ShardingKey选择队列
	if msg.ShardingKey != "" {
		hash := simpleHash(msg.ShardingKey)
		return mqs[hash%len(mqs)]
	}
	// 默认选择第一个队列
	return mqs[0]
}

func simpleHash(s string) int {
	hash := 0
	for _, c := range s {
		hash = hash*31 + int(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}

func main() {
	fmt.Println("=== Go-RocketMQ 高级功能演示 ===")

	// 1. 演示普通生产者功能
	demoNormalProducer()

	// 2. 演示事务消息
	demoTransactionMessage()

	// 3. 演示顺序消息
	demoOrderedMessage()

	// 4. 演示延时消息
	demoDelayMessage()

	// 5. 演示批量消息
	demoBatchMessage()

	// 6. 演示Push消费者
	demoPushConsumer()

	// 7. 演示Pull消费者
	demoPullConsumer()

	// 8. 演示Simple消费者
	demoSimpleConsumer()

	fmt.Println("\n=== 演示完成 ===")
}

func demoNormalProducer() {
	fmt.Println("\n--- 普通生产者演示 ---")

	// 创建生产者
	producer := client.NewProducer("demo_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		log.Printf("启动生产者失败: %v", err)
		return
	}
	defer producer.Shutdown()

	// 发送普通消息
	msg := client.NewMessage("DemoTopic", []byte("Hello RocketMQ!"))
	msg.SetTags("TagA")
	msg.SetKeys("Key1")

	result, err := producer.SendSync(msg)
	if err != nil {
		log.Printf("发送消息失败: %v", err)
		return
	}

	fmt.Printf("发送成功: MsgId=%s, QueueId=%d\n", result.MsgId, result.MessageQueue.QueueId)
}

func demoTransactionMessage() {
	fmt.Println("\n--- 事务消息演示 ---")

	// 创建事务生产者
	txProducer := client.NewTransactionProducer("demo_transaction_producer_group", &ExampleTransactionListener{})
	txProducer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := txProducer.Start()
	if err != nil {
		log.Printf("启动生产者失败: %v", err)
		return
	}
	defer txProducer.Shutdown()

	// 发送事务消息
	msg := client.NewMessage("TransactionTopic", []byte("Transaction Message"))
	msg.SetTags("TxTag")

	result, err := txProducer.SendMessageInTransaction(msg, "transaction_arg")
	if err != nil {
		log.Printf("发送事务消息失败: %v", err)
		return
	}

	fmt.Printf("事务消息发送成功: MsgId=%s, TransactionId=%s\n", result.MsgId, result.TransactionId)
}

func demoOrderedMessage() {
	fmt.Println("\n--- 顺序消息演示 ---")

	// 创建生产者
	producer := client.NewProducer("demo_ordered_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		log.Printf("启动生产者失败: %v", err)
		return
	}
	defer producer.Shutdown()

	// 发送顺序消息
	for i := 0; i < 3; i++ {
		msg := client.NewMessage("OrderedTopic", []byte(fmt.Sprintf("Ordered Message %d", i)))
		msg.SetTags("OrderTag")
		msg.SetShardingKey("order_123") // 相同的ShardingKey保证顺序

		result, err := producer.SendOrderedMessage(msg, &ExampleMessageQueueSelector{}, "order_123")
		if err != nil {
			log.Printf("发送顺序消息失败: %v", err)
			continue
		}

		fmt.Printf("顺序消息发送成功: MsgId=%s, QueueId=%d\n", result.MsgId, result.MessageQueue.QueueId)
	}
}

func demoDelayMessage() {
	fmt.Println("\n--- 延时消息演示 ---")

	// 创建生产者
	producer := client.NewProducer("demo_delay_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		log.Printf("启动生产者失败: %v", err)
		return
	}
	defer producer.Shutdown()

	// 发送延时消息（延时5秒）
	msg := client.NewMessage("DelayTopic", []byte("Delay Message"))
	msg.SetTags("DelayTag")

	result, err := producer.SendDelayMessage(msg, client.DelayLevel5s)
	if err != nil {
		log.Printf("发送延时消息失败: %v", err)
		return
	}

	fmt.Printf("延时消息发送成功: MsgId=%s, DelayLevel=%d\n", result.MsgId, client.DelayLevel5s)

	// 发送定时消息（5秒后投递）
	scheduledMsg := client.NewMessage("ScheduledTopic", []byte("Scheduled Message"))
	scheduledMsg.SetTags("ScheduledTag")
	deliverTime := time.Now().Add(5 * time.Second)

	result2, err := producer.SendScheduledMessage(scheduledMsg, deliverTime)
	if err != nil {
		log.Printf("发送定时消息失败: %v", err)
		return
	}

	fmt.Printf("定时消息发送成功: MsgId=%s, DeliverTime=%d\n", result2.MsgId, deliverTime.UnixMilli())
}

func demoBatchMessage() {
	fmt.Println("\n--- 批量消息演示 ---")

	// 创建生产者
	producer := client.NewProducer("demo_batch_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		log.Printf("启动生产者失败: %v", err)
		return
	}
	defer producer.Shutdown()

	// 创建批量消息
	var messages []*client.Message
	for i := 0; i < 5; i++ {
		msg := client.NewMessage("BatchTopic", []byte(fmt.Sprintf("Batch Message %d", i)))
		msg.SetTags("BatchTag")
		messages = append(messages, msg)
	}

	// 发送批量消息
	result, err := producer.SendBatchMessages(messages)
	if err != nil {
		log.Printf("发送批量消息失败: %v", err)
		return
	}

	fmt.Printf("批量消息发送成功: MsgId=%s, 消息数量=%d\n", result.MsgId, len(messages))
}

func demoPushConsumer() {
	fmt.Println("\n--- Push消费者演示 ---")

	// 创建Push消费者
	consumer := client.NewPushConsumer("demo_push_consumer_group")
	consumer.Consumer.SetNameServerAddr("127.0.0.1:9876")
	consumer.Consumer.Subscribe("DemoTopic", "*", &ExampleMessageListener{})
	consumer.RegisterMessageListener(&ExampleMessageListener{})
	consumer.SetConsumeType(client.ConsumeConcurrently)

	// 启动消费者
	err := consumer.Start()
	if err != nil {
		log.Printf("启动Push消费者失败: %v", err)
		return
	}

	fmt.Println("Push消费者已启动，等待消息...")
	time.Sleep(2 * time.Second) // 模拟消费一段时间

	// 关闭消费者
	consumer.Consumer.Stop()
	fmt.Println("Push消费者已关闭")
}

func demoPullConsumer() {
	fmt.Println("\n--- Pull消费者演示 ---")

	// 创建Pull消费者
	consumer := client.NewPullConsumer("demo_pull_consumer_group")
	consumer.Consumer.SetNameServerAddr("127.0.0.1:9876")

	// 启动消费者
	err := consumer.Consumer.Start()
	if err != nil {
		log.Printf("启动Pull消费者失败: %v", err)
		return
	}
	defer consumer.Consumer.Stop()

	// 创建消息队列
	mq := &client.MessageQueue{
		Topic:      "DemoTopic",
		BrokerName: "broker-a",
		QueueId:    0,
	}

	// 拉取消息
	result, err := consumer.PullBlockIfNotFound(mq, "*", 0, 10)
	if err != nil {
		log.Printf("拉取消息失败: %v", err)
		return
	}

	fmt.Printf("Pull消费者拉取结果: Status=%d, 消息数量=%d\n", result.PullStatus, len(result.MsgFoundList))
	for _, msg := range result.MsgFoundList {
		fmt.Printf("拉取到消息: MsgId=%s, Body=%s\n", msg.MsgId, string(msg.Body))
	}
}

func demoSimpleConsumer() {
	fmt.Println("\n--- Simple消费者演示 ---")

	// 创建Simple消费者
	consumer := client.NewSimpleConsumer("demo_simple_consumer_group")
	consumer.Consumer.SetNameServerAddr("127.0.0.1:9876")
	consumer.SetAwaitDuration(5 * time.Second)

	// 启动消费者
	err := consumer.Consumer.Start()
	if err != nil {
		log.Printf("启动Simple消费者失败: %v", err)
		return
	}
	defer consumer.Consumer.Stop()

	// 接收消息
	messages, err := consumer.ReceiveMessage(10, 5*time.Second)
	if err != nil {
		log.Printf("接收消息失败: %v", err)
		return
	}

	fmt.Printf("Simple消费者接收到 %d 条消息\n", len(messages))
	for _, msg := range messages {
		fmt.Printf("接收到消息: MsgId=%s, Body=%s\n", msg.MsgId, string(msg.Body))
		
		// 确认消息
		err = consumer.AckMessage(msg)
		if err != nil {
			log.Printf("确认消息失败: %v", err)
		} else {
			fmt.Printf("消息确认成功: MsgId=%s\n", msg.MsgId)
		}
	}
}