package common

import (
	"testing"
	"time"
	"reflect"
)

// TestNewMessage 测试创建新消息
func TestNewMessage(t *testing.T) {
	topic := "TestTopic"
	body := []byte("Hello World")
	
	msg := NewMessage(topic, body)
	
	if msg.Topic != topic {
		t.Errorf("Expected topic %s, got %s", topic, msg.Topic)
	}
	
	if !reflect.DeepEqual(msg.Body, body) {
		t.Errorf("Expected body %v, got %v", body, msg.Body)
	}
	
	if msg.Properties == nil {
		t.Error("Properties should be initialized")
	}
}

// TestMessageSetTags 测试设置消息标签
func TestMessageSetTags(t *testing.T) {
	msg := NewMessage("TestTopic", []byte("test"))
	tags := "tag1"
	
	result := msg.SetTags(tags)
	
	if msg.Tags != tags {
		t.Errorf("Expected tags %s, got %s", tags, msg.Tags)
	}
	
	// 测试链式调用
	if result != msg {
		t.Error("SetTags should return the message itself for chaining")
	}
}

// TestMessageSetKeys 测试设置消息键
func TestMessageSetKeys(t *testing.T) {
	msg := NewMessage("TestTopic", []byte("test"))
	keys := "key1"
	
	result := msg.SetKeys(keys)
	
	if msg.Keys != keys {
		t.Errorf("Expected keys %s, got %s", keys, msg.Keys)
	}
	
	// 测试链式调用
	if result != msg {
		t.Error("SetKeys should return the message itself for chaining")
	}
}

// TestMessageSetProperty 测试设置消息属性
func TestMessageSetProperty(t *testing.T) {
	msg := NewMessage("TestTopic", []byte("test"))
	key := "orderId"
	value := "12345"
	
	result := msg.SetProperty(key, value)
	
	if msg.Properties[key] != value {
		t.Errorf("Expected property %s=%s, got %s", key, value, msg.Properties[key])
	}
	
	// 测试链式调用
	if result != msg {
		t.Error("SetProperty should return the message itself for chaining")
	}
}

// TestMessageGetProperty 测试获取消息属性
func TestMessageGetProperty(t *testing.T) {
	msg := NewMessage("TestTopic", []byte("test"))
	key := "orderId"
	value := "12345"
	
	msg.SetProperty(key, value)
	
	result := msg.GetProperty(key)
	if result != value {
		t.Errorf("Expected property value %s, got %s", value, result)
	}
	
	// 测试不存在的属性
	nonExistentValue := msg.GetProperty("nonExistent")
	if nonExistentValue != "" {
		t.Errorf("Expected empty string for non-existent property, got %s", nonExistentValue)
	}
}

// TestMessageChaining 测试方法链式调用
func TestMessageChaining(t *testing.T) {
	msg := NewMessage("TestTopic", []byte("test"))
	
	result := msg.SetTags("tag1").SetKeys("key1").SetProperty("orderId", "12345")
	
	if result != msg {
		t.Error("Chaining should return the same message instance")
	}
	
	if msg.Tags != "tag1" {
		t.Errorf("Expected tags 'tag1', got %s", msg.Tags)
	}
	
	if msg.Keys != "key1" {
		t.Errorf("Expected keys 'key1', got %s", msg.Keys)
	}
	
	if msg.GetProperty("orderId") != "12345" {
		t.Errorf("Expected property orderId='12345', got %s", msg.GetProperty("orderId"))
	}
}

// TestMessageQueue 测试消息队列
func TestMessageQueue(t *testing.T) {
	mq := &MessageQueue{
		Topic:      "TestTopic",
		BrokerName: "TestBroker",
		QueueId:    1,
	}
	
	expected := "MessageQueue{topic=TestTopic, brokerName=TestBroker, queueId=1}"
	result := mq.String()
	
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

// TestSendStatus 测试发送状态枚举
func TestSendStatus(t *testing.T) {
	tests := []struct {
		status   SendStatus
		expected int32
	}{
		{SendOK, 0},
		{SendFlushDiskTimeout, 1},
		{SendFlushSlaveTimeout, 2},
		{SendSlaveNotAvailable, 3},
	}
	
	for _, test := range tests {
		if int32(test.status) != test.expected {
			t.Errorf("Expected status %d, got %d", test.expected, int32(test.status))
		}
	}
}

// TestConsumeResult 测试消费结果枚举
func TestConsumeResult(t *testing.T) {
	tests := []struct {
		result   ConsumeResult
		expected int32
	}{
		{ConsumeSuccess, 0},
		{ReconsumeLater, 1},
	}
	
	for _, test := range tests {
		if int32(test.result) != test.expected {
			t.Errorf("Expected result %d, got %d", test.expected, int32(test.result))
		}
	}
}

// TestConsumeFromWhere 测试消费起始位置枚举
func TestConsumeFromWhere(t *testing.T) {
	tests := []struct {
		position ConsumeFromWhere
		expected int32
	}{
		{ConsumeFromLastOffset, 0},
		{ConsumeFromFirstOffset, 1},
		{ConsumeFromTimestamp, 2},
	}
	
	for _, test := range tests {
		if int32(test.position) != test.expected {
			t.Errorf("Expected position %d, got %d", test.expected, int32(test.position))
		}
	}
}

// TestMessageModel 测试消息模式枚举
func TestMessageModel(t *testing.T) {
	tests := []struct {
		model    MessageModel
		expected int32
	}{
		{Clustering, 0},
		{Broadcasting, 1},
	}
	
	for _, test := range tests {
		if int32(test.model) != test.expected {
			t.Errorf("Expected model %d, got %d", test.expected, int32(test.model))
		}
	}
}

// TestMessageExt 测试扩展消息结构
func TestMessageExt(t *testing.T) {
	msg := NewMessage("TestTopic", []byte("test"))
	now := time.Now()
	
	msgExt := &MessageExt{
		Message:             msg,
		MsgId:              "MSG_001",
		QueueId:            1,
		StoreSize:          100,
		QueueOffset:        10,
		SysFlag:            0,
		BornTimestamp:      now,
		BornHost:           "127.0.0.1:9876",
		StoreTimestamp:     now,
		StoreHost:          "127.0.0.1:10911",
		ReconsumeTimes:     0,
		PreparedTransaction: false,
		CommitLogOffset:    1000,
	}
	
	// 验证基础消息属性
	if msgExt.Topic != "TestTopic" {
		t.Errorf("Expected topic 'TestTopic', got %s", msgExt.Topic)
	}
	
	// 验证扩展属性
	if msgExt.MsgId != "MSG_001" {
		t.Errorf("Expected msgId 'MSG_001', got %s", msgExt.MsgId)
	}
	
	if msgExt.QueueId != 1 {
		t.Errorf("Expected queueId 1, got %d", msgExt.QueueId)
	}
	
	if msgExt.CommitLogOffset != 1000 {
		t.Errorf("Expected commitLogOffset 1000, got %d", msgExt.CommitLogOffset)
	}
}

// TestSendResult 测试发送结果结构
func TestSendResult(t *testing.T) {
	mq := &MessageQueue{
		Topic:      "TestTopic",
		BrokerName: "TestBroker",
		QueueId:    1,
	}
	
	result := &SendResult{
		SendStatus:    SendOK,
		MsgId:         "MSG_001",
		MessageQueue:  mq,
		QueueOffset:   100,
		TransactionId: "TXN_001",
		OffsetMsgId:   "OFFSET_MSG_001",
		RegionId:      "region-1",
		TraceOn:       true,
	}
	
	if result.SendStatus != SendOK {
		t.Errorf("Expected sendStatus SendOK, got %v", result.SendStatus)
	}
	
	if result.MsgId != "MSG_001" {
		t.Errorf("Expected msgId 'MSG_001', got %s", result.MsgId)
	}
	
	if result.MessageQueue.Topic != "TestTopic" {
		t.Errorf("Expected topic 'TestTopic', got %s", result.MessageQueue.Topic)
	}
	
	if result.QueueOffset != 100 {
		t.Errorf("Expected queueOffset 100, got %d", result.QueueOffset)
	}
}

// TestConsumeOrderlyContext 测试顺序消费上下文
func TestConsumeOrderlyContext(t *testing.T) {
	mq := &MessageQueue{
		Topic:      "TestTopic",
		BrokerName: "TestBroker",
		QueueId:    1,
	}
	
	context := &ConsumeOrderlyContext{
		MessageQueue: mq,
		AutoCommit:   true,
		SuspendTime:  5 * time.Second,
	}
	
	if context.MessageQueue.Topic != "TestTopic" {
		t.Errorf("Expected topic 'TestTopic', got %s", context.MessageQueue.Topic)
	}
	
	if !context.AutoCommit {
		t.Error("Expected autoCommit to be true")
	}
	
	if context.SuspendTime != 5*time.Second {
		t.Errorf("Expected suspendTime 5s, got %v", context.SuspendTime)
	}
}

// BenchmarkNewMessage 基准测试创建消息
func BenchmarkNewMessage(b *testing.B) {
	topic := "BenchmarkTopic"
	body := []byte("Benchmark message body")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewMessage(topic, body)
	}
}

// BenchmarkMessageSetProperty 基准测试设置属性
func BenchmarkMessageSetProperty(b *testing.B) {
	msg := NewMessage("TestTopic", []byte("test"))
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.SetProperty("key", "value")
	}
}

// BenchmarkMessageGetProperty 基准测试获取属性
func BenchmarkMessageGetProperty(b *testing.B) {
	msg := NewMessage("TestTopic", []byte("test"))
	msg.SetProperty("key", "value")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = msg.GetProperty("key")
	}
}