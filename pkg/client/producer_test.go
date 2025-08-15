package client

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go-rocketmq/pkg/common"
	"go-rocketmq/pkg/protocol"
)

// TestDefaultProducerConfig 测试默认生产者配置
func TestDefaultProducerConfig(t *testing.T) {
	config := DefaultProducerConfig()
	if config == nil {
		t.Fatal("DefaultProducerConfig should not return nil")
	}
	if config.GroupName == "" {
		t.Error("GroupName should not be empty")
	}
	if config.SendMsgTimeout <= 0 {
		t.Error("SendMsgTimeout should be greater than 0")
	}
	if config.MaxMessageSize <= 0 {
		t.Error("MaxMessageSize should be greater than 0")
	}
	if config.RetryTimesWhenSendFailed < 0 {
		t.Error("RetryTimesWhenSendFailed should not be negative")
	}
}

// TestProducerConfig 测试生产者配置结构体
func TestProducerConfig(t *testing.T) {
	config := &ProducerConfig{
		GroupName:                         "test-producer-group",
		NameServers:                       []string{"127.0.0.1:9876"},
		SendMsgTimeout:                    5 * time.Second,
		CompressMsgBodyOver:               4096,
		RetryTimesWhenSendFailed:          3,
		RetryTimesWhenSendAsyncFailed:     3,
		RetryAnotherBrokerWhenNotStoreOK:  true,
		MaxMessageSize:                    1024 * 1024 * 4,
	}

	// 测试JSON序列化
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal ProducerConfig: %v", err)
	}

	// 测试JSON反序列化
	var config2 ProducerConfig
	err = json.Unmarshal(data, &config2)
	if err != nil {
		t.Fatalf("Failed to unmarshal ProducerConfig: %v", err)
	}

	if config2.GroupName != config.GroupName {
		t.Errorf("GroupName mismatch, expected %s, got %s", config.GroupName, config2.GroupName)
	}
	if config2.SendMsgTimeout != config.SendMsgTimeout {
		t.Errorf("SendMsgTimeout mismatch, expected %v, got %v", config.SendMsgTimeout, config2.SendMsgTimeout)
	}
	if len(config2.NameServers) != len(config.NameServers) {
		t.Errorf("NameServers length mismatch, expected %d, got %d", len(config.NameServers), len(config2.NameServers))
	}
}

// TestNewProducer 测试生产者创建
func TestNewProducer(t *testing.T) {
	producer := NewProducer("test-producer-group")
	if producer == nil {
		t.Fatal("NewProducer should not return nil")
	}
	if producer.config == nil {
		t.Error("config should be initialized")
	}
	if producer.config.GroupName != "test-producer-group" {
		t.Errorf("GroupName mismatch, expected test-producer-group, got %s", producer.config.GroupName)
	}
	if producer.shutdown == nil {
		t.Error("shutdown channel should be initialized")
	}
	if producer.routeTable == nil {
		t.Error("routeTable should be initialized")
	}
	if producer.started {
		t.Error("Producer should not be started initially")
	}
}

// TestSetNameServers 测试设置NameServer地址
func TestSetNameServers(t *testing.T) {
	producer := NewProducer("test-producer-group")
	nameServers := []string{"127.0.0.1:9876", "127.0.0.1:9877"}

	producer.SetNameServers(nameServers)

	if len(producer.config.NameServers) != 2 {
		t.Errorf("NameServers length mismatch, expected 2, got %d", len(producer.config.NameServers))
	}
	if producer.config.NameServers[0] != "127.0.0.1:9876" {
		t.Errorf("First NameServer mismatch, expected 127.0.0.1:9876, got %s", producer.config.NameServers[0])
	}
	if producer.config.NameServers[1] != "127.0.0.1:9877" {
		t.Errorf("Second NameServer mismatch, expected 127.0.0.1:9877, got %s", producer.config.NameServers[1])
	}
}

// TestProducerStartShutdown 测试生产者启动和关闭
func TestProducerStartShutdown(t *testing.T) {
	producer := NewProducer("test-producer-group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 测试启动
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	if !producer.started {
		t.Error("Producer should be started")
	}

	// 测试重复启动
	err = producer.Start()
	if err == nil {
		t.Error("Starting an already started producer should return an error")
	}

	// 测试关闭
	producer.Shutdown()
	if producer.started {
		t.Error("Producer should be stopped after shutdown")
	}

	// 等待关闭完成
	time.Sleep(100 * time.Millisecond)
}

// TestProducerStartWithoutNameServers 测试没有NameServer的启动
func TestProducerStartWithoutNameServers(t *testing.T) {
	producer := NewProducer("test-producer-group")

	// 不设置NameServer，应该启动失败
	err := producer.Start()
	if err == nil {
		t.Error("Starting producer without NameServers should return an error")
	}
	if producer.started {
		t.Error("Producer should not be started when start fails")
	}
}

// TestGetTopicRouteData 测试获取Topic路由数据
func TestGetTopicRouteData(t *testing.T) {
	producer := NewProducer("test-producer-group")

	// 创建测试路由数据
	routeData := &protocol.TopicRouteData{
		OrderTopicConf: "",
		QueueDatas: []*protocol.QueueData{
			{
				BrokerName:     "broker-a",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
			},
		},
		BrokerDatas: []*protocol.BrokerData{
			{
				Cluster:     "DefaultCluster",
				BrokerName:  "broker-a",
				BrokerAddrs: map[int64]string{0: "127.0.0.1:10911"},
			},
		},
		FilterServerTable: make(map[string][]string),
	}

	// 手动设置路由数据
	producer.routeMutex.Lock()
	producer.routeTable["TestTopic"] = routeData
	producer.routeMutex.Unlock()

	// 获取路由数据
	retrievedRouteData := producer.getTopicRouteData("TestTopic")
	if retrievedRouteData == nil {
		t.Fatal("getTopicRouteData should return route data")
	}
	if len(retrievedRouteData.QueueDatas) != 1 {
		t.Errorf("QueueDatas length mismatch, expected 1, got %d", len(retrievedRouteData.QueueDatas))
	}
	if retrievedRouteData.QueueDatas[0].BrokerName != "broker-a" {
		t.Errorf("BrokerName mismatch, expected broker-a, got %s", retrievedRouteData.QueueDatas[0].BrokerName)
	}
}

// TestGetTopicRouteDataNotFound 测试获取不存在的Topic路由数据
func TestGetTopicRouteDataNotFound(t *testing.T) {
	producer := NewProducer("test-producer-group")

	// 获取不存在的Topic路由数据
	routeData := producer.getTopicRouteData("NonExistentTopic")
	if routeData != nil {
		t.Error("getTopicRouteData should return nil for non-existent topic")
	}
}

// TestSelectMessageQueue 测试消息队列选择
func TestSelectMessageQueue(t *testing.T) {
	producer := NewProducer("test-producer-group")

	// 创建测试路由数据
	routeData := &protocol.TopicRouteData{
		OrderTopicConf: "",
		QueueDatas: []*protocol.QueueData{
			{
				BrokerName:     "broker-a",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
			},
			{
				BrokerName:     "broker-b",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
			},
		},
		BrokerDatas: []*protocol.BrokerData{
			{
				Cluster:     "DefaultCluster",
				BrokerName:  "broker-a",
				BrokerAddrs: map[int64]string{0: "127.0.0.1:10911"},
			},
			{
				Cluster:     "DefaultCluster",
				BrokerName:  "broker-b",
				BrokerAddrs: map[int64]string{0: "127.0.0.1:10921"},
			},
		},
		FilterServerTable: make(map[string][]string),
	}

	// 选择消息队列
	mq := producer.selectMessageQueue(routeData, "TestTopic")
	if mq == nil {
		t.Fatal("selectMessageQueue should return a message queue")
	}
	if mq.Topic != "TestTopic" {
		t.Errorf("Topic mismatch, expected TestTopic, got %s", mq.Topic)
	}
	if mq.BrokerName == "" {
		t.Error("BrokerName should not be empty")
	}
	if mq.QueueId < 0 {
		t.Error("QueueId should not be negative")
	}
}

// TestSelectMessageQueueWithEmptyRouteData 测试空路由数据的队列选择
func TestSelectMessageQueueWithEmptyRouteData(t *testing.T) {
	producer := NewProducer("test-producer-group")

	// 创建空的路由数据
	routeData := &protocol.TopicRouteData{
		OrderTopicConf:    "",
		QueueDatas:        []*protocol.QueueData{},
		BrokerDatas:       []*protocol.BrokerData{},
		FilterServerTable: make(map[string][]string),
	}

	// 选择消息队列
	mq := producer.selectMessageQueue(routeData, "TestTopic")
	if mq != nil {
		t.Error("selectMessageQueue should return nil for empty route data")
	}
}

// TestSendSyncWithInvalidMessage 测试发送无效消息
func TestSendSyncWithInvalidMessage(t *testing.T) {
	producer := NewProducer("test-producer-group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()

	// 测试发送nil消息
	_, err = producer.SendSync(nil)
	if err == nil {
		t.Error("Sending nil message should return an error")
	}

	// 测试发送空Topic的消息
	msg := &common.Message{
		Topic: "",
		Body:  []byte("test message"),
	}
	_, err = producer.SendSync(msg)
	if err == nil {
		t.Error("Sending message with empty topic should return an error")
	}

	// 测试发送空Body的消息
	msg = &common.Message{
		Topic: "TestTopic",
		Body:  nil,
	}
	_, err = producer.SendSync(msg)
	if err == nil {
		t.Error("Sending message with nil body should return an error")
	}
}

// TestSendSyncWithTimeout 测试带超时的同步发送
func TestSendSyncWithTimeout(t *testing.T) {
	producer := NewProducer("test-producer-group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()

	// 创建测试消息
	msg := &common.Message{
		Topic:      "TestTopic",
		Body:       []byte("test message with timeout"),
		Properties: map[string]string{"TAGS": "TestTag"},
	}

	// 测试带超时的发送（由于没有真实的Broker，会超时失败）
	_, err = producer.SendSyncWithTimeout(msg, 1*time.Second)
	if err == nil {
		t.Error("SendSyncWithTimeout should fail without real broker")
	}
}

// TestSendOneway 测试单向发送
func TestSendOneway(t *testing.T) {
	producer := NewProducer("test-producer-group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()

	// 创建测试消息
	msg := &common.Message{
		Topic:      "TestTopic",
		Body:       []byte("oneway message"),
		Properties: map[string]string{"TAGS": "OnewayTag"},
	}

	// 测试单向发送（由于没有真实的Broker，会失败）
	err = producer.SendOneway(msg)
	if err == nil {
		t.Error("SendOneway should fail without real broker")
	}
}

// TestSendAsync 测试异步发送
func TestSendAsync(t *testing.T) {
	producer := NewProducer("test-producer-group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()

	// 创建测试消息
	msg := &common.Message{
		Topic:      "TestTopic",
		Body:       []byte("async message"),
		Properties: map[string]string{"TAGS": "AsyncTag"},
	}

	// 创建回调函数
	callbackCalled := false
	callback := func(result *common.SendResult, err error) {
		callbackCalled = true
		if err == nil {
			t.Error("Callback should receive an error without real broker")
		}
	}

	// 测试异步发送
	err = producer.SendAsync(msg, callback)
	if err != nil {
		t.Fatalf("SendAsync should not return immediate error: %v", err)
	}

	// 等待回调执行
	time.Sleep(100 * time.Millisecond)
	if !callbackCalled {
		t.Error("Callback should be called")
	}
}

// TestSendAsyncWithNilCallback 测试异步发送空回调
func TestSendAsyncWithNilCallback(t *testing.T) {
	producer := NewProducer("test-producer-group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()

	// 创建测试消息
	msg := &common.Message{
		Topic:      "TestTopic",
		Body:       []byte("async message with nil callback"),
		Properties: map[string]string{"TAGS": "AsyncTag"},
	}

	// 测试异步发送空回调
	err = producer.SendAsync(msg, nil)
	if err == nil {
		t.Error("SendAsync with nil callback should return an error")
	}
}

// TestConcurrentSend 测试并发发送
func TestConcurrentSend(t *testing.T) {
	producer := NewProducer("test-producer-group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	err := producer.Start()
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()

	// 并发发送消息
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			msg := &common.Message{
				Topic:      "TestTopic",
				Body:       []byte(fmt.Sprintf("concurrent message %d", id)),
				Properties: map[string]string{"TAGS": "ConcurrentTag"},
			}

			// 尝试发送（会失败，但不应该崩溃）
			_, err := producer.SendSync(msg)
			if err == nil {
				t.Errorf("SendSync should fail without real broker for goroutine %d", id)
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// BenchmarkNewProducer 基准测试生产者创建
func BenchmarkNewProducer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewProducer("benchmark-producer-group")
	}
}

// BenchmarkSetNameServers 基准测试设置NameServer
func BenchmarkSetNameServers(b *testing.B) {
	producer := NewProducer("benchmark-producer-group")
	nameServers := []string{"127.0.0.1:9876", "127.0.0.1:9877"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		producer.SetNameServers(nameServers)
	}
}

// BenchmarkGetTopicRouteData 基准测试获取路由数据
func BenchmarkGetTopicRouteData(b *testing.B) {
	producer := NewProducer("benchmark-producer-group")

	// 预先设置一些路由数据
	routeData := &protocol.TopicRouteData{
		OrderTopicConf: "",
		QueueDatas: []*protocol.QueueData{
			{
				BrokerName:     "broker-a",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
			},
		},
		BrokerDatas: []*protocol.BrokerData{
			{
				Cluster:     "DefaultCluster",
				BrokerName:  "broker-a",
				BrokerAddrs: map[int64]string{0: "127.0.0.1:10911"},
			},
		},
		FilterServerTable: make(map[string][]string),
	}

	producer.routeMutex.Lock()
	producer.routeTable["BenchmarkTopic"] = routeData
	producer.routeMutex.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = producer.getTopicRouteData("BenchmarkTopic")
	}
}

// BenchmarkSelectMessageQueue 基准测试消息队列选择
func BenchmarkSelectMessageQueue(b *testing.B) {
	producer := NewProducer("benchmark-producer-group")

	// 创建测试路由数据
	routeData := &protocol.TopicRouteData{
		OrderTopicConf: "",
		QueueDatas: []*protocol.QueueData{
			{
				BrokerName:     "broker-a",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
			},
			{
				BrokerName:     "broker-b",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
			},
		},
		BrokerDatas: []*protocol.BrokerData{
			{
				Cluster:     "DefaultCluster",
				BrokerName:  "broker-a",
				BrokerAddrs: map[int64]string{0: "127.0.0.1:10911"},
			},
			{
				Cluster:     "DefaultCluster",
				BrokerName:  "broker-b",
				BrokerAddrs: map[int64]string{0: "127.0.0.1:10921"},
			},
		},
		FilterServerTable: make(map[string][]string),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = producer.selectMessageQueue(routeData, "BenchmarkTopic")
	}
}