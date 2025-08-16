//go:build ordered_retry_dlq

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
	log.Println("=== 顺序消费失败重试与DLQ验证示例 ===")

	// 启动消费者验证重试和DLQ
	go startRetryAndDLQConsumer()

	// 启动生产者发送测试消息
	go startRetryTestProducer()

	// 启动DLQ消费者监控死信队列
	go startDLQConsumer()

	// 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("准备退出程序...")
}

// startRetryAndDLQConsumer 启动主要的消费者，模拟失败重试
func startRetryAndDLQConsumer() {
	log.Println("启动重试与DLQ验证消费者...")

	// 创建Push消费者
	consumer := client.NewPushConsumer("retry_dlq_consumer_group")
	consumer.Consumer.SetNameServerAddr("127.0.0.1:9876")
	consumer.SetMessageModel(client.Clustering)
	consumer.SetConsumeFromWhere(client.ConsumeFromLastOffset)

	// 设置为顺序消费
	consumer.SetConsumeType(client.ConsumeOrderly)

	// 演示 DLQ 模式切换 - 默认为 Mock 模式，可以切换到 Real 模式与真实 Broker 交互
	// consumer.SetDLQMode(client.DLQModeReal) // 真实 DLQ 模式 - 需要真实 Broker 环境
	consumer.SetDLQMode(client.DLQModeMock) // Mock 模式 - 用于本地演示

	log.Printf("当前 DLQ 模式: %v (Mock=0, Real=1)", consumer.GetDLQMode())

	// 配置重试策略 - 降低重试次数以快速验证DLQ
	retryPolicy := &client.RetryPolicy{
		MaxRetryTimes:   3,                  // 最大重试3次（便于快速测试）
		RetryDelayLevel: client.DelayLevel1s, // 1秒延时重试
		RetryInterval:   2 * time.Second,     // 重试间隔2秒
		EnableRetry:     true,                // 启用重试
	}
	consumer.SetRetryPolicy(retryPolicy)

	// 定义顺序消息监听器 - 模拟处理失败
	listener := client.MessageListenerOrderly(func(msgs []*client.MessageExt, ctx *client.ConsumeOrderlyContext) client.ConsumeResult {
		for _, msg := range msgs {
			log.Printf("[Orderly] 收到消息: topic=%s, queueId=%d, msgId=%s, retryTimes=%d, body=%s",
				msg.Topic, msg.QueueId, msg.MsgId, msg.ReconsumeTimes, string(msg.Body))

			// 模拟业务逻辑：包含"fail_message"的消息在达到最大重试次数前均返回重试
			if strings.Contains(string(msg.Body), "fail_message") {
				if msg.ReconsumeTimes < retryPolicy.MaxRetryTimes {
					log.Printf("[Orderly] 消息处理失败，将触发重试: %s (已重试: %d / 最大: %d)",
						msg.MsgId, msg.ReconsumeTimes, retryPolicy.MaxRetryTimes)
					return client.ReconsumeLater
				}
				log.Printf("[Orderly] 已达到最大重试次数，消息将进入DLQ（实际由系统处理）: %s", msg.MsgId)
			}

			log.Printf("[Orderly] 消息处理成功: %s", msg.MsgId)
		}
		return client.ConsumeSuccess
	})

	// 注册监听器
	consumer.RegisterMessageListener(listener)

	// 订阅测试主题
	if err := consumer.Consumer.Subscribe("RetryTestTopic", "*", listener); err != nil {
		log.Fatalf("订阅 RetryTestTopic 失败: %v", err)
	}

	// 启动消费者
	if err := consumer.Start(); err != nil {
		log.Fatalf("启动重试验证消费者失败: %v", err)
	}
	defer consumer.Stop()

	log.Println("重试与DLQ验证消费者已启动，等待消息...")

	// 保持运行
	select {}
}

// startRetryTestProducer 启动生产者发送测试消息
func startRetryTestProducer() {
	time.Sleep(2 * time.Second) // 等待消费者启动

	log.Println("启动重试测试生产者...")

	// 创建普通生产者
	producer := client.NewProducer("retry_test_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	if err := producer.Start(); err != nil {
		log.Fatalf("启动重试测试生产者失败: %v", err)
	}
	defer producer.Shutdown()

	// 发送测试消息，用于验证重试和DLQ
	testMessages := []struct {
		body        string
		shardingKey string
		shouldFail  bool
	}{
		{"fail_message_1", "user_001", true},    // 故意失败
		{"fail_message_2", "user_002", true},    // 故意失败
		{"success_message_1", "user_003", false}, // 成功消息
		{"fail_message_3", "user_004", true},    // 故意失败
		{"success_message_2", "user_005", false}, // 成功消息
		{"fail_message_4", "user_006", true},    // 新增故意失败
		{"fail_message_5", "user_007", true},    // 新增故意失败
		{"fail_message_6", "user_008", true},    // 新增故意失败
		{"fail_message_dlq_real_test", "user_009", true}, // 真实 DLQ 测试消息
	}

	queueSelector := &client.OrderedMessageQueueSelector{}

	for i, testMsg := range testMessages {
		msg := client.NewMessage("RetryTestTopic", []byte(testMsg.body))
		msg.SetTags("retry_test")
		msg.SetKeys(fmt.Sprintf("test_msg_%d", i))
		msg.SetShardingKey(testMsg.shardingKey)
		msg.SetProperty("test_scenario", "retry_dlq_validation")
		msg.SetProperty("should_fail", strconv.FormatBool(testMsg.shouldFail))
		msg.SetProperty("message_index", strconv.Itoa(i))

		result, err := producer.SendOrderedMessage(msg, queueSelector, testMsg.shardingKey)
		if err != nil {
			log.Printf("发送测试消息失败: %v", err)
			continue
		}

		log.Printf("发送测试消息成功: msgId=%s, body=%s, shouldFail=%v, shardingKey=%s",
			result.MsgId, testMsg.body, testMsg.shouldFail, testMsg.shardingKey)

		// 间隔发送
		time.Sleep(1 * time.Second)
	}

	log.Println("重试测试消息发送完毕")
}

// startDLQConsumer 启动DLQ消费者监控死信队列
func startDLQConsumer() {
	time.Sleep(5 * time.Second) // 等待主消费者启动和处理

	log.Println("启动DLQ监控消费者...")

	// 创建DLQ消费者 - DLQ topic命名规则: %DLQ%_%s (消费者组名)
	dlqConsumer := client.NewPushConsumer("dlq_monitor_group")
	dlqConsumer.Consumer.SetNameServerAddr("127.0.0.1:9876")
	dlqConsumer.SetMessageModel(client.Clustering)
	dlqConsumer.SetConsumeFromWhere(client.ConsumeFromFirstOffset)

	// DLQ消费者使用并发消费即可
	dlqConsumer.SetConsumeType(client.ConsumeConcurrently)

	// 演示 DLQ 模式设置 - DLQ 监控消费者建议设置为与主消费者一致的模式
	dlqConsumer.SetDLQMode(client.DLQModeMock) // 与主消费者保持一致

	// 定义DLQ消息监听器
	dlqListener := client.MessageListenerConcurrently(func(msgs []*client.MessageExt) client.ConsumeResult {
		for _, msg := range msgs {
			log.Printf("[DLQ] 收到死信消息: topic=%s, originTopic=%s, originMsgId=%s, reconsumeTime=%d, body=%s",
				msg.Topic,
				msg.Properties["ORIGIN_TOPIC"],
				msg.Properties["ORIGIN_MSG_ID"],
				msg.ReconsumeTimes,
				string(msg.Body))

			// 分析死信消息
			analyzeDLQMessage(msg)
		}
		return client.ConsumeSuccess
	})

	// 订阅DLQ主题 - 监控retry_dlq_consumer_group的死信队列
	dlqTopic := "%DLQ%_retry_dlq_consumer_group"
	if err := dlqConsumer.Consumer.Subscribe(dlqTopic, "*", dlqListener); err != nil {
		log.Fatalf("订阅DLQ主题 %s 失败: %v", dlqTopic, err)
	}
	// 注册监听器
	dlqConsumer.RegisterMessageListener(dlqListener)

	// 启动DLQ消费者
	if err := dlqConsumer.Start(); err != nil {
		log.Fatalf("启动DLQ消费者失败: %v", err)
	}
	defer dlqConsumer.Stop()

	log.Printf("DLQ监控消费者已启动，监控主题: %s", dlqTopic)

	// 保持运行
	select {}
}

// analyzeDLQMessage 分析死信消息
func analyzeDLQMessage(msg *client.MessageExt) {
	log.Printf("=== DLQ消息分析 ===")
	log.Printf("原始Topic: %s", msg.Properties["ORIGIN_TOPIC"])
	log.Printf("原始消息ID: %s", msg.Properties["ORIGIN_MSG_ID"])
	log.Printf("重试次数: %s", msg.Properties["RETRY_TIMES"])
	log.Printf("原队列ID: %s", msg.Properties["DLQ_ORIGIN_QUEUE_ID"]) // 与客户端sendToDLQ保持一致
	log.Printf("原Broker: %s", msg.Properties["DLQ_ORIGIN_BROKER"])     // 与客户端sendToDLQ保持一致
	log.Printf("消息体: %s", string(msg.Body))
	
	// 检查是否为预期的失败消息
	if strings.Contains(string(msg.Body), "fail_message") {
		log.Printf("✓ 确认：这是预期的失败消息，重试机制正常工作")
	} else {
		log.Printf("⚠ 警告：非预期的消息进入DLQ: %s", string(msg.Body))
	}
	
	// DLQ 模式分析
	if strings.Contains(string(msg.Body), "dlq_real_test") {
		log.Printf("💡 提示：这是真实 DLQ 模式测试消息，如需验证真实 Broker 交互，请：")
		log.Printf("   1. 启动真实的 RocketMQ Broker 和 NameServer")
		log.Printf("   2. 在代码中切换为 consumer.SetDLQMode(client.DLQModeReal)")
		log.Printf("   3. 确保 NameServer 地址配置正确")
	}
	log.Printf("===================")
}