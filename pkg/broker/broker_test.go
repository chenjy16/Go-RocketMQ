package broker

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"go-rocketmq/pkg/common"
	"go-rocketmq/pkg/protocol"
	"go-rocketmq/pkg/store"
)

// TestDefaultBrokerConfig 测试默认Broker配置
func TestDefaultBrokerConfig(t *testing.T) {
	config := DefaultBrokerConfig()
	if config == nil {
		t.Fatal("DefaultBrokerConfig should not return nil")
	}
	if config.BrokerName == "" {
		t.Error("BrokerName should not be empty")
	}
	if config.ListenPort <= 0 {
		t.Error("ListenPort should be greater than 0")
	}
	if config.SendMessageThreadPoolNums <= 0 {
		t.Error("SendMessageThreadPoolNums should be greater than 0")
	}
	if config.PullMessageThreadPoolNums <= 0 {
		t.Error("PullMessageThreadPoolNums should be greater than 0")
	}
}

// TestConfig 测试Broker配置结构体
func TestConfig(t *testing.T) {
	config := &Config{
		BrokerName:                "test-broker",
		BrokerId:                   0,
		ClusterName:                "DefaultCluster",
		ListenPort:                 10911,
		NameServerAddr:             "127.0.0.1:9876",
		StorePathRootDir:           "/tmp/store",
		SendMessageThreadPoolNums:  16,
		PullMessageThreadPoolNums:  16,
		FlushDiskType:              0,
		BrokerRole:                 0,
		HaListenPort:               10912,
		HaMasterAddress:            "",
		ReplicationMode:            0,
		EnableCluster:              true,
		ClusterManagerPort:         10913,
		EnableFailover:             true,
		AutoFailover:               true,
		FailoverDelay:              30,
		BackupBrokers:              []string{"127.0.0.1:10921"},
	}

	// 测试JSON序列化
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal Config: %v", err)
	}

	// 测试JSON反序列化
	var config2 Config
	err = json.Unmarshal(data, &config2)
	if err != nil {
		t.Fatalf("Failed to unmarshal Config: %v", err)
	}

	if config2.BrokerName != config.BrokerName {
		t.Errorf("BrokerName mismatch, expected %s, got %s", config.BrokerName, config2.BrokerName)
	}
	if config2.ListenPort != config.ListenPort {
		t.Errorf("ListenPort mismatch, expected %d, got %d", config.ListenPort, config2.ListenPort)
	}
	if config2.EnableCluster != config.EnableCluster {
		t.Errorf("EnableCluster mismatch, expected %t, got %t", config.EnableCluster, config2.EnableCluster)
	}
}

// TestConsumerGroupInfo 测试消费者组信息
func TestConsumerGroupInfo(t *testing.T) {
	cgi := &ConsumerGroupInfo{
		GroupName:        "test-consumer-group",
		ConsumeType:      0,
		MessageModel:     1,
		ConsumeFromWhere: 0,
		Subscriptions:    make(map[string]*protocol.SubscriptionData),
		Channels:         make(map[string]net.Conn),
	}

	// 添加订阅数据
	subData := &protocol.SubscriptionData{
		Topic:          "TestTopic",
		SubString:      "*",
		TagsSet:        []string{"TagA"},
		CodeSet:        []int32{1},
		SubVersion:     1,
		ExpressionType: "TAG",
	}
	cgi.Subscriptions["TestTopic"] = subData

	if cgi.GroupName != "test-consumer-group" {
		t.Errorf("GroupName mismatch, expected test-consumer-group, got %s", cgi.GroupName)
	}
	if len(cgi.Subscriptions) != 1 {
		t.Errorf("Subscriptions length mismatch, expected 1, got %d", len(cgi.Subscriptions))
	}
	if cgi.Subscriptions["TestTopic"].Topic != "TestTopic" {
		t.Error("Subscription topic mismatch")
	}
}

// TestProducerGroupInfo 测试生产者组信息
func TestProducerGroupInfo(t *testing.T) {
	pgi := &ProducerGroupInfo{
		GroupName: "test-producer-group",
		Channels:  make(map[string]net.Conn),
	}

	if pgi.GroupName != "test-producer-group" {
		t.Errorf("GroupName mismatch, expected test-producer-group, got %s", pgi.GroupName)
	}
	if pgi.Channels == nil {
		t.Error("Channels should be initialized")
	}
}

// TestNewBroker 测试Broker创建
func TestNewBroker(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	if broker == nil {
		t.Fatal("NewBroker should not return nil")
	}
	if broker.config != config {
		t.Error("Config should be set correctly")
	}
	if broker.shutdown == nil {
		t.Error("shutdown channel should be initialized")
	}
	if broker.topicConfigTable == nil {
		t.Error("topicConfigTable should be initialized")
	}
	if broker.consumerTable == nil {
		t.Error("consumerTable should be initialized")
	}
	if broker.producerTable == nil {
		t.Error("producerTable should be initialized")
	}
	if broker.messageStore == nil {
		t.Error("messageStore should be initialized")
	}
	if broker.haService == nil {
		t.Error("haService should be initialized")
	}
	if broker.clusterManager == nil {
		t.Error("clusterManager should be initialized")
	}
	if broker.failoverService == nil {
		t.Error("failoverService should be initialized")
	}
}

// TestBrokerStartStop 测试Broker启动和停止
func TestBrokerStartStop(t *testing.T) {
	config := &Config{
		BrokerName:                "test-broker",
		BrokerId:                   0,
		ClusterName:                "DefaultCluster",
		ListenPort:                 0, // 使用随机端口
		NameServerAddr:             "127.0.0.1:9876",
		StorePathRootDir:           "/tmp/test-store",
		SendMessageThreadPoolNums:  4,
		PullMessageThreadPoolNums:  4,
		FlushDiskType:              0,
		BrokerRole:                 0,
		HaListenPort:               0,
		ReplicationMode:            0,
		EnableCluster:              false, // 禁用集群功能以简化测试
		EnableFailover:             false, // 禁用故障转移以简化测试
	}

	broker := NewBroker(config)

	// 测试启动
	err := broker.Start()
	if err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}

	// 验证监听器已创建
	if broker.listener == nil {
		t.Error("Listener should be created after start")
	}

	// 等待一小段时间确保服务启动
	time.Sleep(100 * time.Millisecond)

	// 测试停止
	err = broker.Stop()
	if err != nil {
		t.Fatalf("Failed to stop broker: %v", err)
	}

	// 等待停止完成
	time.Sleep(100 * time.Millisecond)
}

// TestCreateTopic 测试创建Topic
func TestCreateTopic(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 创建Topic
	err := broker.CreateTopic("TestTopic", 4)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 验证Topic配置
	topicConfig := broker.GetTopicConfig("TestTopic")
	if topicConfig == nil {
		t.Fatal("Topic config should not be nil")
	}
	if topicConfig.TopicName != "TestTopic" {
		t.Errorf("Topic name mismatch, expected TestTopic, got %s", topicConfig.TopicName)
	}
	if topicConfig.ReadQueueNums != 4 {
		t.Errorf("ReadQueueNums mismatch, expected 4, got %d", topicConfig.ReadQueueNums)
	}
	if topicConfig.WriteQueueNums != 4 {
		t.Errorf("WriteQueueNums mismatch, expected 4, got %d", topicConfig.WriteQueueNums)
	}
}

// TestGetTopicConfigNotFound 测试获取不存在的Topic配置
func TestGetTopicConfigNotFound(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 获取不存在的Topic配置
	topicConfig := broker.GetTopicConfig("NonExistentTopic")
	if topicConfig != nil {
		t.Error("GetTopicConfig should return nil for non-existent topic")
	}
}

// TestPutMessage 测试消息存储
func TestPutMessage(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 启动broker
	err := broker.Start()
	if err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 先创建Topic
	err = broker.CreateTopic("TestTopic", 4)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 创建测试消息
	msg := &common.Message{
		Topic:      "TestTopic",
		Body:       []byte("Hello, RocketMQ!"),
		Properties: map[string]string{"TAGS": "TagA"},
	}

	// 存储消息
	result, err := broker.PutMessage(msg)
	if err != nil {
		t.Fatalf("Failed to put message: %v", err)
	}
	if result == nil {
		t.Fatal("SendResult should not be nil")
	}
	if result.SendStatus != common.SendOK {
		t.Errorf("SendStatus should be SendOK, got %v", result.SendStatus)
	}
	if result.MsgId == "" {
		t.Error("MsgId should not be empty")
	}
}

// TestPutDelayMessage 测试延迟消息
func TestPutDelayMessage(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 启动broker
	err := broker.Start()
	if err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 先创建Topic
	err = broker.CreateTopic("TestTopic", 4)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 创建延迟消息
	msg := &common.Message{
		Topic:      "TestTopic",
		Body:       []byte("Delayed message"),
		Properties: map[string]string{"TAGS": "DelayTag"},
	}

	// 存储延迟消息（延迟级别1，通常是1秒）
	result, err := broker.PutDelayMessage(msg, 1)
	if err != nil {
		t.Fatalf("Failed to put delay message: %v", err)
	}
	if result == nil {
		t.Fatal("SendResult should not be nil")
	}
	if result.SendStatus != common.SendOK {
		t.Errorf("SendStatus should be SendOK, got %v", result.SendStatus)
	}
}

// TestPutOrderedMessage 测试顺序消息
func TestPutOrderedMessage(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 启动broker
	err := broker.Start()
	if err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 先创建Topic
	err = broker.CreateTopic("OrderTopic", 4)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 创建顺序消息
	msg := &common.Message{
		Topic:      "OrderTopic",
		Body:       []byte("Ordered message"),
		Properties: map[string]string{"TAGS": "OrderTag"},
	}

	// 存储顺序消息
	result, err := broker.PutOrderedMessage(msg, "order-key-1")
	if err != nil {
		t.Fatalf("Failed to put ordered message: %v", err)
	}
	if result == nil {
		t.Fatal("SendResult should not be nil")
	}
	if result.SendStatus != common.SendOK {
		t.Errorf("SendStatus should be SendOK, got %v", result.SendStatus)
	}
}

// TestTransactionMessage 测试事务消息
func TestTransactionMessage(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 启动broker
	err := broker.Start()
	if err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 先创建Topic
	err = broker.CreateTopic("TransactionTopic", 4)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 注册事务监听器
	listener := &MockTransactionListener{}
	broker.RegisterTransactionListener("test-producer-group", listener)

	// 创建事务消息
	msg := &common.Message{
		Topic:      "TransactionTopic",
		Body:       []byte("Transaction message"),
		Properties: map[string]string{"TAGS": "TxTag"},
	}

	// 准备事务消息
	result, err := broker.PrepareMessage(msg, "test-producer-group", "tx-001")
	if err != nil {
		t.Fatalf("Failed to prepare transaction message: %v", err)
	}
	if result == nil {
		t.Fatal("SendResult should not be nil")
	}

	// 提交事务
	err = broker.CommitTransaction("tx-001")
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// TestTransactionRollback 测试事务回滚
func TestTransactionRollback(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 启动broker
	err := broker.Start()
	if err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 先创建Topic
	err = broker.CreateTopic("TransactionTopic", 4)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 注册事务监听器
	listener := &MockTransactionListener{}
	broker.RegisterTransactionListener("test-producer-group", listener)

	// 创建事务消息
	msg := &common.Message{
		Topic:      "TransactionTopic",
		Body:       []byte("Transaction message to rollback"),
		Properties: map[string]string{"TAGS": "TxTag"},
	}

	// 准备事务消息
	result, err := broker.PrepareMessage(msg, "test-producer-group", "tx-002")
	if err != nil {
		t.Fatalf("Failed to prepare transaction message: %v", err)
	}
	if result == nil {
		t.Fatal("SendResult should not be nil")
	}

	// 回滚事务
	err = broker.RollbackTransaction("tx-002")
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}
}

// TestPullMessage 测试消息拉取
func TestPullMessage(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 启动broker
	err := broker.Start()
	if err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 先创建Topic
	err = broker.CreateTopic("TestTopic", 4)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 存储一些消息
	for i := 0; i < 5; i++ {
		msg := &common.Message{
			Topic:      "TestTopic",
			Body:       []byte(fmt.Sprintf("Message %d", i)),
			Properties: map[string]string{"TAGS": "TestTag"},
		}
		_, err := broker.PutMessage(msg)
		if err != nil {
			t.Fatalf("Failed to put message %d: %v", i, err)
		}
	}

	// 拉取消息
	messages, err := broker.PullMessage("TestTopic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Failed to pull messages: %v", err)
	}
	if len(messages) == 0 {
		t.Error("Should pull some messages")
	}
}

// TestConsumeOffset 测试消费位点管理
func TestConsumeOffset(t *testing.T) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 提交消费位点
	err := broker.CommitConsumeOffset("TestTopic", 0, "test-consumer-group", 100)
	if err != nil {
		t.Fatalf("Failed to commit consume offset: %v", err)
	}

	// 获取消费位点
	offset := broker.GetConsumeOffset("TestTopic", 0, "test-consumer-group")
	if offset != 100 {
		t.Errorf("Consume offset mismatch, expected 100, got %d", offset)
	}
}

// TestGetBrokerRoleString 测试Broker角色字符串
func TestGetBrokerRoleString(t *testing.T) {
	tests := []struct {
		role     int
		expected string
	}{
		{0, "ASYNC_MASTER"},
		{1, "SYNC_MASTER"},
		{2, "SLAVE"},
		{99, "UNKNOWN"},
	}

	for _, tt := range tests {
		config := DefaultBrokerConfig()
		config.BrokerRole = tt.role
		broker := NewBroker(config)

		roleStr := broker.getBrokerRoleString()
		if roleStr != tt.expected {
			t.Errorf("Role string mismatch for role %d, expected %s, got %s", tt.role, tt.expected, roleStr)
		}
	}
}

// MockTransactionListener 模拟事务监听器
type MockTransactionListener struct{}

func (m *MockTransactionListener) ExecuteLocalTransaction(msg *common.Message, arg interface{}) store.TransactionState {
	return store.TransactionStateCommit
}

func (m *MockTransactionListener) CheckLocalTransaction(msg *common.MessageExt) store.TransactionState {
	return store.TransactionStateCommit
}

// BenchmarkPutMessage 基准测试消息存储
func BenchmarkPutMessage(b *testing.B) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 创建Topic
	err := broker.CreateTopic("BenchmarkTopic", 4)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	msg := &common.Message{
		Topic:      "BenchmarkTopic",
		Body:       []byte("Benchmark message"),
		Properties: map[string]string{"TAGS": "BenchTag"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := broker.PutMessage(msg)
		if err != nil {
			b.Fatalf("Failed to put message: %v", err)
		}
	}
}

// BenchmarkCreateTopic 基准测试Topic创建
func BenchmarkCreateTopic(b *testing.B) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topicName := fmt.Sprintf("BenchmarkTopic-%d", i)
		err := broker.CreateTopic(topicName, 4)
		if err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}
	}
}

// BenchmarkGetTopicConfig 基准测试获取Topic配置
func BenchmarkGetTopicConfig(b *testing.B) {
	config := DefaultBrokerConfig()
	broker := NewBroker(config)

	// 预先创建一些Topic
	for i := 0; i < 100; i++ {
		topicName := fmt.Sprintf("Topic-%d", i)
		err := broker.CreateTopic(topicName, 4)
		if err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topicName := fmt.Sprintf("Topic-%d", i%100)
		_ = broker.GetTopicConfig(topicName)
	}
}