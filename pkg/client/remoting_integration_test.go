package client

import (
	"fmt"
	"testing"
	"time"
)

// TestRemotingProducerBasic 测试基于remoting的生产者基本功能
func TestRemotingProducerBasic(t *testing.T) {
	// 创建生产者
	producer := NewRemotingProducer("TestProducerGroup")
	producer.SetNameServers([]string{"127.0.0.1:9876"})
	
	// 启动生产者
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()
	
	// 检查启动状态
	if !producer.IsStarted() {
		t.Fatal("Producer should be started")
	}
	
	// 创建测试消息
	msg := &Message{
		Topic: "TestTopic",
		Body:  []byte("Hello RocketMQ with Remoting"),
		Properties: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}
	
	// 同步发送消息
	result, err := producer.SendSync(msg)
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}
	
	// 验证发送结果
	if result == nil {
		t.Fatal("Send result should not be nil")
	}
	if result.SendStatus != SendOK {
		t.Fatalf("Expected send status OK, got %v", result.SendStatus)
	}
	if result.MsgId == "" {
		t.Fatal("Message ID should not be empty")
	}
	
	fmt.Printf("Message sent successfully: %s\n", result.MsgId)
}

// TestRemotingProducerAsync 测试异步发送
func TestRemotingProducerAsync(t *testing.T) {
	producer := NewRemotingProducer("TestAsyncProducerGroup")
	producer.SetNameServers([]string{"127.0.0.1:9876"})
	
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()
	
	msg := &Message{
		Topic: "TestAsyncTopic",
		Body:  []byte("Async message with remoting"),
	}
	
	// 异步发送消息
	done := make(chan bool)
	err = producer.SendAsync(msg, func(result *SendResult, err error) {
		defer func() { done <- true }()
		
		if err != nil {
			t.Errorf("Async send failed: %v", err)
			return
		}
		
		if result == nil {
			t.Error("Async send result should not be nil")
			return
		}
		
		fmt.Printf("Async message sent: %s\n", result.MsgId)
	})
	
	if err != nil {
		t.Fatalf("Failed to send async message: %v", err)
	}
	
	// 等待异步回调
	select {
	case <-done:
		// 成功
	case <-time.After(5 * time.Second):
		t.Fatal("Async callback timeout")
	}
}

// TestRemotingConsumerBasic 测试基于remoting的消费者基本功能
func TestRemotingConsumerBasic(t *testing.T) {
	// 创建消费者配置
	config := &ConsumerConfig{
		GroupName:        "TestConsumerGroup",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: ConsumeFromFirstOffset,
		MessageModel:     Clustering,
		PullInterval:     1 * time.Second,
		PullBatchSize:    32,
	}
	
	// 创建消费者
	consumer := NewRemotingConsumer(config)
	
	// 订阅Topic
	messageReceived := make(chan bool)
	listener := &TestMessageListener{
		Callback: func(msgs []*MessageExt) ConsumeResult {
			for _, msg := range msgs {
				fmt.Printf("Received message: %s\n", string(msg.Body))
			}
			messageReceived <- true
			return ConsumeSuccess
		},
	}
	
	err := consumer.Subscribe("TestTopic", "*", listener)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	
	// 启动消费者
	err = consumer.Start()
	if err != nil {
		t.Fatalf("Failed to start consumer: %v", err)
	}
	defer consumer.Stop()
	
	// 检查启动状态
	if !consumer.IsStarted() {
		t.Fatal("Consumer should be started")
	}
	
	// 验证订阅信息
	subscriptions := consumer.GetSubscriptions()
	if len(subscriptions) != 1 {
		t.Fatalf("Expected 1 subscription, got %d", len(subscriptions))
	}
	
	if _, exists := subscriptions["TestTopic"]; !exists {
		t.Fatal("TestTopic subscription not found")
	}
	
	fmt.Println("Consumer started and subscribed successfully")
}

// TestRemotingProducerConsumerIntegration 测试生产者和消费者集成
func TestRemotingProducerConsumerIntegration(t *testing.T) {
	// 创建生产者
	producer := NewRemotingProducer("IntegrationProducerGroup")
	producer.SetNameServers([]string{"127.0.0.1:9876"})
	
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()
	
	// 创建消费者
	config := &ConsumerConfig{
		GroupName:        "IntegrationConsumerGroup",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: ConsumeFromFirstOffset,
		MessageModel:     Clustering,
		PullInterval:     500 * time.Millisecond,
		PullBatchSize:    10,
	}
	
	consumer := NewRemotingConsumer(config)
	
	// 设置消息监听器
	messageCount := 0
	messageReceived := make(chan bool, 10)
	listener := &TestMessageListener{
		Callback: func(msgs []*MessageExt) ConsumeResult {
			for _, msg := range msgs {
				messageCount++
				fmt.Printf("Integration test received message %d: %s\n", messageCount, string(msg.Body))
				messageReceived <- true
			}
			return ConsumeSuccess
		},
	}
	
	err = consumer.Subscribe("IntegrationTopic", "*", listener)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	
	err = consumer.Start()
	if err != nil {
		t.Fatalf("Failed to start consumer: %v", err)
	}
	defer consumer.Stop()
	
	// 发送多条测试消息
	for i := 0; i < 5; i++ {
		msg := &Message{
			Topic: "IntegrationTopic",
			Body:  []byte(fmt.Sprintf("Integration test message %d", i+1)),
			Properties: map[string]string{
				"index": fmt.Sprintf("%d", i+1),
			},
		}
		
		result, err := producer.SendSync(msg)
		if err != nil {
			t.Fatalf("Failed to send message %d: %v", i+1, err)
		}
		
		fmt.Printf("Sent integration message %d: %s\n", i+1, result.MsgId)
		time.Sleep(100 * time.Millisecond)
	}
	
	fmt.Println("Integration test completed successfully")
}

// TestMessageListener 测试消息监听器
type TestMessageListener struct {
	Callback func([]*MessageExt) ConsumeResult
}

func (tml *TestMessageListener) ConsumeMessage(msgs []*MessageExt) ConsumeResult {
	if tml.Callback != nil {
		return tml.Callback(msgs)
	}
	return ConsumeSuccess
}

// TestRemotingProducerMultipleStart 测试重复启动
func TestRemotingProducerMultipleStart(t *testing.T) {
	producer := NewRemotingProducer("MultiStartTestGroup")
	
	// 第一次启动
	err := producer.Start()
	if err != nil {
		t.Fatalf("First start failed: %v", err)
	}
	
	// 第二次启动应该返回错误
	err = producer.Start()
	if err == nil {
		t.Fatal("Second start should return error")
	}
	
	producer.Shutdown()
	
	// 关闭后应该可以重新启动
	err = producer.Start()
	if err != nil {
		t.Fatalf("Restart after shutdown failed: %v", err)
	}
	
	producer.Shutdown()
}

// TestRemotingConsumerMultipleStart 测试消费者重复启动
func TestRemotingConsumerMultipleStart(t *testing.T) {
	consumer := NewRemotingConsumer(nil)
	
	// 第一次启动
	err := consumer.Start()
	if err != nil {
		t.Fatalf("First start failed: %v", err)
	}
	
	// 第二次启动应该返回错误
	err = consumer.Start()
	if err == nil {
		t.Fatal("Second start should return error")
	}
	
	consumer.Stop()
	
	// 关闭后应该可以重新启动
	err = consumer.Start()
	if err != nil {
		t.Fatalf("Restart after stop failed: %v", err)
	}
	
	consumer.Stop()
}