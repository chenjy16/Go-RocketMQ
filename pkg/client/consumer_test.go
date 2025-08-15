package client

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go-rocketmq/pkg/common"
)

// MockMessageListener 模拟消息监听器
type MockMessageListener struct {
	consumedMessages []*common.MessageExt
	result           common.ConsumeResult
}

func (m *MockMessageListener) ConsumeMessage(messages []*common.MessageExt) common.ConsumeResult {
	m.consumedMessages = append(m.consumedMessages, messages...)
	return m.result
}

// TestDefaultConsumerConfig 测试默认消费者配置
func TestDefaultConsumerConfig(t *testing.T) {
	config := DefaultConsumerConfig()
	
	if config.GroupName != "DefaultConsumerGroup" {
		t.Errorf("Expected GroupName to be 'DefaultConsumerGroup', got %s", config.GroupName)
	}
	
	if config.ConsumeFromWhere != common.ConsumeFromFirstOffset {
		t.Errorf("Expected ConsumeFromWhere to be ConsumeFromFirstOffset, got %v", config.ConsumeFromWhere)
	}
	
	if config.MessageModel != common.Clustering {
		t.Errorf("Expected MessageModel to be Clustering, got %v", config.MessageModel)
	}
	
	if config.ConsumeThreadMin != 1 {
		t.Errorf("Expected ConsumeThreadMin to be 1, got %d", config.ConsumeThreadMin)
	}
	
	if config.ConsumeThreadMax != 20 {
		t.Errorf("Expected ConsumeThreadMax to be 20, got %d", config.ConsumeThreadMax)
	}
	
	if config.PullInterval != 1*time.Second {
		t.Errorf("Expected PullInterval to be 1s, got %v", config.PullInterval)
	}
	
	if config.PullBatchSize != 32 {
		t.Errorf("Expected PullBatchSize to be 32, got %d", config.PullBatchSize)
	}
	
	if config.ConsumeTimeout != 15*time.Minute {
		t.Errorf("Expected ConsumeTimeout to be 15m, got %v", config.ConsumeTimeout)
	}
}

// TestConsumerConfig 测试消费者配置
func TestConsumerConfig(t *testing.T) {
	config := &ConsumerConfig{
		GroupName:        "TestGroup",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: common.ConsumeFromLastOffset,
		MessageModel:     common.Broadcasting,
		ConsumeThreadMin: 2,
		ConsumeThreadMax: 10,
		PullInterval:     2 * time.Second,
		PullBatchSize:    16,
		ConsumeTimeout:   10 * time.Minute,
	}
	
	// 测试JSON序列化
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}
	
	// 测试JSON反序列化
	var newConfig ConsumerConfig
	err = json.Unmarshal(data, &newConfig)
	if err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}
	
	if newConfig.GroupName != config.GroupName {
		t.Errorf("Expected GroupName %s, got %s", config.GroupName, newConfig.GroupName)
	}
	
	if newConfig.NameServerAddr != config.NameServerAddr {
		t.Errorf("Expected NameServerAddr %s, got %s", config.NameServerAddr, newConfig.NameServerAddr)
	}
}

// TestSubscription 测试订阅信息
func TestSubscription(t *testing.T) {
	listener := &MockMessageListener{result: common.ConsumeSuccess}
	
	sub := &Subscription{
		Topic:         "TestTopic",
		SubExpression: "*",
		Listener:      listener,
	}
	
	if sub.Topic != "TestTopic" {
		t.Errorf("Expected Topic to be 'TestTopic', got %s", sub.Topic)
	}
	
	if sub.SubExpression != "*" {
		t.Errorf("Expected SubExpression to be '*', got %s", sub.SubExpression)
	}
	
	if sub.Listener != listener {
		t.Error("Expected Listener to match")
	}
}

// TestNewConsumer 测试创建新消费者
func TestNewConsumer(t *testing.T) {
	// 测试使用默认配置
	consumer := NewConsumer(nil)
	if consumer == nil {
		t.Fatal("Expected consumer to be created")
	}
	
	if consumer.config.GroupName != "DefaultConsumerGroup" {
		t.Errorf("Expected default group name, got %s", consumer.config.GroupName)
	}
	
	if consumer.subscriptions == nil {
		t.Error("Expected subscriptions map to be initialized")
	}
	
	if consumer.shutdown == nil {
		t.Error("Expected shutdown channel to be initialized")
	}
	
	// 测试使用自定义配置
	config := &ConsumerConfig{
		GroupName:      "CustomGroup",
		NameServerAddr: "127.0.0.1:9876",
	}
	
	consumer2 := NewConsumer(config)
	if consumer2.config.GroupName != "CustomGroup" {
		t.Errorf("Expected custom group name, got %s", consumer2.config.GroupName)
	}
}

// TestSetNameServerAddr 测试设置NameServer地址
func TestSetNameServerAddr(t *testing.T) {
	consumer := NewConsumer(nil)
	addr := "127.0.0.1:9876"
	
	consumer.SetNameServerAddr(addr)
	
	if consumer.config.NameServerAddr != addr {
		t.Errorf("Expected NameServerAddr to be %s, got %s", addr, consumer.config.NameServerAddr)
	}
}

// TestSubscribe 测试订阅主题
func TestSubscribe(t *testing.T) {
	consumer := NewConsumer(nil)
	listener := &MockMessageListener{result: common.ConsumeSuccess}
	
	// 测试正常订阅
	err := consumer.Subscribe("TestTopic", "*", listener)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	
	sub, exists := consumer.subscriptions["TestTopic"]
	if !exists {
		t.Error("Expected subscription to exist")
	}
	
	if sub.Topic != "TestTopic" {
		t.Errorf("Expected topic 'TestTopic', got %s", sub.Topic)
	}
	
	if sub.SubExpression != "*" {
		t.Errorf("Expected subExpression '*', got %s", sub.SubExpression)
	}
	
	// 测试重复订阅
	err = consumer.Subscribe("TestTopic", "tag1", listener)
	if err == nil {
		t.Error("Expected error for duplicate subscription")
	}
	
	// 测试空主题
	err = consumer.Subscribe("", "*", listener)
	if err == nil {
		t.Error("Expected error for empty topic")
	}
	
	// 测试空监听器
	err = consumer.Subscribe("TestTopic2", "*", nil)
	if err == nil {
		t.Error("Expected error for nil listener")
	}
}

// TestConsumerStartStop 测试消费者启动和停止
func TestConsumerStartStop(t *testing.T) {
	consumer := NewConsumer(nil)
	listener := &MockMessageListener{result: common.ConsumeSuccess}
	
	// 测试未订阅就启动
	err := consumer.Start()
	if err == nil {
		t.Error("Expected error when starting without subscriptions")
	}
	
	// 添加订阅
	err = consumer.Subscribe("TestTopic", "*", listener)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	
	// 测试启动
	err = consumer.Start()
	if err != nil {
		t.Fatalf("Failed to start consumer: %v", err)
	}
	
	if !consumer.IsStarted() {
		t.Error("Expected consumer to be started")
	}
	
	// 测试重复启动
	err = consumer.Start()
	if err == nil {
		t.Error("Expected error for duplicate start")
	}
	
	// 测试停止
	err = consumer.Stop()
	if err != nil {
		t.Fatalf("Failed to stop consumer: %v", err)
	}
	
	if consumer.IsStarted() {
		t.Error("Expected consumer to be stopped")
	}
	
	// 测试重复停止
	err = consumer.Stop()
	if err == nil {
		t.Error("Expected error for duplicate stop")
	}
}

// TestGetSubscriptions 测试获取订阅信息
func TestGetSubscriptions(t *testing.T) {
	consumer := NewConsumer(nil)
	listener := &MockMessageListener{result: common.ConsumeSuccess}
	
	// 测试空订阅
	subs := consumer.GetSubscriptions()
	if len(subs) != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", len(subs))
	}
	
	// 添加订阅
	err := consumer.Subscribe("TestTopic1", "*", listener)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	
	err = consumer.Subscribe("TestTopic2", "tag1", listener)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	
	subs = consumer.GetSubscriptions()
	if len(subs) != 2 {
		t.Errorf("Expected 2 subscriptions, got %d", len(subs))
	}
	
	if _, exists := subs["TestTopic1"]; !exists {
		t.Error("Expected TestTopic1 subscription to exist")
	}
	
	if _, exists := subs["TestTopic2"]; !exists {
		t.Error("Expected TestTopic2 subscription to exist")
	}
}

// TestConsumeMessages 测试消息消费
func TestConsumeMessages(t *testing.T) {
	consumer := NewConsumer(nil)
	listener := &MockMessageListener{result: common.ConsumeSuccess}
	
	// 创建测试消息
	messages := []*common.MessageExt{
		{
			Message: &common.Message{
				Topic: "TestTopic",
				Body:  []byte("test message 1"),
			},
			MsgId:     "msg1",
			QueueId:   0,
			StoreSize: 100,
		},
		{
			Message: &common.Message{
				Topic: "TestTopic",
				Body:  []byte("test message 2"),
			},
			MsgId:     "msg2",
			QueueId:   1,
			StoreSize: 100,
		},
	}
	
	// 测试消费消息
	consumer.consumeMessages(messages, listener)
	
	if len(listener.consumedMessages) != 2 {
		t.Errorf("Expected 2 consumed messages, got %d", len(listener.consumedMessages))
	}
	
	if string(listener.consumedMessages[0].Body) != "test message 1" {
		t.Errorf("Expected first message body 'test message 1', got %s", string(listener.consumedMessages[0].Body))
	}
	
	if string(listener.consumedMessages[1].Body) != "test message 2" {
		t.Errorf("Expected second message body 'test message 2', got %s", string(listener.consumedMessages[1].Body))
	}
}

// TestMockPullMessages 测试模拟拉取消息
func TestMockPullMessages(t *testing.T) {
	consumer := NewConsumer(nil)
	
	messages := consumer.mockPullMessages("TestTopic")
	
	if len(messages) == 0 {
		t.Error("Expected at least one message")
	}
	
	for _, msg := range messages {
		if msg.Topic != "TestTopic" {
			t.Errorf("Expected topic 'TestTopic', got %s", msg.Topic)
		}
		
		if len(msg.Body) == 0 {
			t.Error("Expected message body to be non-empty")
		}
		
		if msg.MsgId == "" {
			t.Error("Expected message ID to be non-empty")
		}
	}
}

// TestUpdateNameServerAddr 测试更新NameServer地址
func TestUpdateNameServerAddr(t *testing.T) {
	consumer := NewConsumer(nil)
	
	err := consumer.UpdateNameServerAddr()
	if err != nil {
		t.Fatalf("Failed to update NameServer address: %v", err)
	}
}

// TestRebalanceQueue 测试队列重平衡
func TestRebalanceQueue(t *testing.T) {
	consumer := NewConsumer(nil)
	
	err := consumer.RebalanceQueue()
	if err != nil {
		t.Fatalf("Failed to rebalance queue: %v", err)
	}
}

// Benchmark tests

// BenchmarkNewConsumer 基准测试创建消费者
func BenchmarkNewConsumer(b *testing.B) {
	config := &ConsumerConfig{
		GroupName:      "BenchmarkGroup",
		NameServerAddr: "127.0.0.1:9876",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewConsumer(config)
	}
}

// BenchmarkSubscribe 基准测试订阅主题
func BenchmarkSubscribe(b *testing.B) {
	consumer := NewConsumer(nil)
	listener := &MockMessageListener{result: common.ConsumeSuccess}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic := fmt.Sprintf("Topic%d", i)
		_ = consumer.Subscribe(topic, "*", listener)
	}
}

// BenchmarkConsumeMessages 基准测试消息消费
func BenchmarkConsumeMessages(b *testing.B) {
	consumer := NewConsumer(nil)
	listener := &MockMessageListener{result: common.ConsumeSuccess}
	
	messages := []*common.MessageExt{
		{
			Message: &common.Message{
				Topic: "BenchmarkTopic",
				Body:  []byte("benchmark message"),
			},
			MsgId:     "benchmark_msg",
			QueueId:   0,
			StoreSize: 100,
		},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		consumer.consumeMessages(messages, listener)
	}
}

// BenchmarkGetSubscriptions 基准测试获取订阅信息
func BenchmarkGetSubscriptions(b *testing.B) {
	consumer := NewConsumer(nil)
	listener := &MockMessageListener{result: common.ConsumeSuccess}
	
	// 添加一些订阅
	for i := 0; i < 10; i++ {
		topic := fmt.Sprintf("Topic%d", i)
		_ = consumer.Subscribe(topic, "*", listener)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = consumer.GetSubscriptions()
	}
}