package nameserver

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go-rocketmq/pkg/protocol"
)

// TestDefaultConfig 测试默认配置
func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	if config == nil {
		t.Fatal("DefaultConfig should not return nil")
	}
	if config.ListenPort != 9876 {
		t.Errorf("Default ListenPort should be 9876, got %d", config.ListenPort)
	}
	if config.ScanNotActiveBrokerInterval != 5*time.Second {
		t.Errorf("Default ScanNotActiveBrokerInterval should be 5s, got %v", config.ScanNotActiveBrokerInterval)
	}
}

// TestConfig 测试配置结构体
func TestConfig(t *testing.T) {
	config := &Config{
		ListenPort:                  9876,
		ClusterTestEnable:           true,
		OrderMessageEnable:          true,
		ScanNotActiveBrokerInterval: 10 * time.Second,
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

	if config2.ListenPort != config.ListenPort {
		t.Errorf("ListenPort mismatch, expected %d, got %d", config.ListenPort, config2.ListenPort)
	}
	if config2.ClusterTestEnable != config.ClusterTestEnable {
		t.Errorf("ClusterTestEnable mismatch, expected %t, got %t", config.ClusterTestEnable, config2.ClusterTestEnable)
	}
}

// TestBrokerLiveInfo 测试BrokerLiveInfo结构体
func TestBrokerLiveInfo(t *testing.T) {
	info := &BrokerLiveInfo{
		LastUpdateTimestamp: time.Now(),
		DataVersion:         protocol.NewDataVersion(),
		HaServerAddr:        "127.0.0.1:10912",
	}

	// 测试JSON序列化（不包含Channel字段）
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Failed to marshal BrokerLiveInfo: %v", err)
	}

	// 测试JSON反序列化
	var info2 BrokerLiveInfo
	err = json.Unmarshal(data, &info2)
	if err != nil {
		t.Fatalf("Failed to unmarshal BrokerLiveInfo: %v", err)
	}

	if info2.HaServerAddr != info.HaServerAddr {
		t.Errorf("HaServerAddr mismatch, expected %s, got %s", info.HaServerAddr, info2.HaServerAddr)
	}
	if info2.DataVersion == nil {
		t.Error("DataVersion should not be nil after unmarshal")
	}
}

// TestNewRouteInfoManager 测试路由信息管理器创建
func TestNewRouteInfoManager(t *testing.T) {
	manager := NewRouteInfoManager()
	if manager == nil {
		t.Fatal("NewRouteInfoManager should not return nil")
	}
	if manager.topicQueueTable == nil {
		t.Error("topicQueueTable should be initialized")
	}
	if manager.brokerAddrTable == nil {
		t.Error("brokerAddrTable should be initialized")
	}
	if manager.clusterAddrTable == nil {
		t.Error("clusterAddrTable should be initialized")
	}
	if manager.brokerLiveTable == nil {
		t.Error("brokerLiveTable should be initialized")
	}
	if manager.filterServerTable == nil {
		t.Error("filterServerTable should be initialized")
	}
}

// TestNewNameServer 测试NameServer创建
func TestNewNameServer(t *testing.T) {
	config := DefaultConfig()
	ns := NewNameServer(config)
	if ns == nil {
		t.Fatal("NewNameServer should not return nil")
	}
	if ns.config != config {
		t.Error("Config should be set correctly")
	}
	if ns.routeTable == nil {
		t.Error("routeTable should be initialized")
	}
	if ns.brokerLiveTable == nil {
		t.Error("brokerLiveTable should be initialized")
	}
	if ns.filterServerTable == nil {
		t.Error("filterServerTable should be initialized")
	}
	if ns.shutdown == nil {
		t.Error("shutdown channel should be initialized")
	}
}

// TestNameServerStartStop 测试NameServer启动和停止
func TestNameServerStartStop(t *testing.T) {
	config := &Config{
		ListenPort:                  0, // 使用随机端口
		ClusterTestEnable:           false,
		OrderMessageEnable:          false,
		ScanNotActiveBrokerInterval: 1 * time.Second,
	}
	ns := NewNameServer(config)

	// 测试启动
	err := ns.Start()
	if err != nil {
		t.Fatalf("Failed to start NameServer: %v", err)
	}

	// 验证监听器已创建
	if ns.listener == nil {
		t.Error("Listener should be created after start")
	}

	// 等待一小段时间确保服务启动
	time.Sleep(100 * time.Millisecond)

	// 测试停止
	ns.Stop()

	// 验证监听器已关闭
	time.Sleep(100 * time.Millisecond)
}

// TestRegisterBroker 测试Broker注册
func TestRegisterBroker(t *testing.T) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	// 创建测试用的TopicConfig
	topicConfig := &protocol.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
	}

	topicConfigWrapper := &protocol.TopicConfigSerializeWrapper{
		TopicConfigTable: map[string]*protocol.TopicConfig{
			"TestTopic": topicConfig,
		},
		DataVersion: protocol.NewDataVersion(),
	}

	// 注册Broker
	result := ns.RegisterBroker(
		"DefaultCluster",
		"127.0.0.1:10911",
		"broker-a",
		0,
		"127.0.0.1:10912",
		topicConfigWrapper,
		[]string{},
		nil,
	)

	if result == nil {
		t.Fatal("RegisterBroker should return a result")
	}
	if result.HaServerAddr != "127.0.0.1:10912" {
		t.Errorf("HaServerAddr mismatch, expected 127.0.0.1:10912, got %s", result.HaServerAddr)
	}
	if result.MasterAddr != "127.0.0.1:10911" {
		t.Errorf("MasterAddr mismatch, expected 127.0.0.1:10911, got %s", result.MasterAddr)
	}

	// 验证Broker已注册到路由表
	ns.mutex.RLock()
	brokerAddrs, exists := ns.routeTable.brokerAddrTable["broker-a"]
	ns.mutex.RUnlock()

	if !exists {
		t.Error("Broker should be registered in brokerAddrTable")
	}
	if brokerAddrs[0] != "127.0.0.1:10911" {
		t.Errorf("Broker address mismatch, expected 127.0.0.1:10911, got %s", brokerAddrs[0])
	}
}

// TestGetRouteInfoByTopic 测试根据Topic获取路由信息
func TestGetRouteInfoByTopic(t *testing.T) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	// 先注册一个Broker
	topicConfig := &protocol.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
	}

	topicConfigWrapper := &protocol.TopicConfigSerializeWrapper{
		TopicConfigTable: map[string]*protocol.TopicConfig{
			"TestTopic": topicConfig,
		},
		DataVersion: protocol.NewDataVersion(),
	}

	ns.RegisterBroker(
		"DefaultCluster",
		"127.0.0.1:10911",
		"broker-a",
		0,
		"127.0.0.1:10912",
		topicConfigWrapper,
		[]string{},
		nil,
	)

	// 获取路由信息
	routeData := ns.GetRouteInfoByTopic("TestTopic")
	if routeData == nil {
		t.Fatal("GetRouteInfoByTopic should return route data")
	}

	if len(routeData.QueueDatas) == 0 {
		t.Error("QueueDatas should not be empty")
	}
	if len(routeData.BrokerDatas) == 0 {
		t.Error("BrokerDatas should not be empty")
	}

	// 验证QueueData
	queueData := routeData.QueueDatas[0]
	if queueData.BrokerName != "broker-a" {
		t.Errorf("BrokerName mismatch, expected broker-a, got %s", queueData.BrokerName)
	}
	if queueData.ReadQueueNums != 4 {
		t.Errorf("ReadQueueNums mismatch, expected 4, got %d", queueData.ReadQueueNums)
	}

	// 验证BrokerData
	brokerData := routeData.BrokerDatas[0]
	if brokerData.BrokerName != "broker-a" {
		t.Errorf("BrokerName mismatch, expected broker-a, got %s", brokerData.BrokerName)
	}
	if brokerData.Cluster != "DefaultCluster" {
		t.Errorf("Cluster mismatch, expected DefaultCluster, got %s", brokerData.Cluster)
	}
}

// TestGetRouteInfoByTopicNotFound 测试获取不存在Topic的路由信息
func TestGetRouteInfoByTopicNotFound(t *testing.T) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	// 获取不存在的Topic路由信息
	routeData := ns.GetRouteInfoByTopic("NonExistentTopic")
	if routeData != nil {
		t.Error("GetRouteInfoByTopic should return nil for non-existent topic")
	}
}

// TestGetAllClusterInfo 测试获取所有集群信息
func TestGetAllClusterInfo(t *testing.T) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	// 注册多个Broker
	topicConfig := &protocol.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
	}

	topicConfigWrapper := &protocol.TopicConfigSerializeWrapper{
		TopicConfigTable: map[string]*protocol.TopicConfig{
			"TestTopic": topicConfig,
		},
		DataVersion: protocol.NewDataVersion(),
	}

	// 注册第一个Broker
	ns.RegisterBroker(
		"DefaultCluster",
		"127.0.0.1:10911",
		"broker-a",
		0,
		"127.0.0.1:10912",
		topicConfigWrapper,
		[]string{},
		nil,
	)

	// 注册第二个Broker
	ns.RegisterBroker(
		"DefaultCluster",
		"127.0.0.1:10921",
		"broker-b",
		0,
		"127.0.0.1:10922",
		topicConfigWrapper,
		[]string{},
		nil,
	)

	// 获取集群信息
	clusterInfo := ns.GetAllClusterInfo()
	if clusterInfo == nil {
		t.Fatal("GetAllClusterInfo should return cluster info")
	}

	if len(clusterInfo.BrokerAddrTable) != 2 {
		t.Errorf("BrokerAddrTable should have 2 entries, got %d", len(clusterInfo.BrokerAddrTable))
	}
	if len(clusterInfo.ClusterAddrTable) != 1 {
		t.Errorf("ClusterAddrTable should have 1 entry, got %d", len(clusterInfo.ClusterAddrTable))
	}

	// 验证集群包含正确的Broker
	brokers, exists := clusterInfo.ClusterAddrTable["DefaultCluster"]
	if !exists {
		t.Error("DefaultCluster should exist in ClusterAddrTable")
	}
	if len(brokers) != 2 {
		t.Errorf("DefaultCluster should have 2 brokers, got %d", len(brokers))
	}
}

// TestCreateAndUpdateQueueData 测试队列数据创建和更新
func TestCreateAndUpdateQueueData(t *testing.T) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	topicConfig := &protocol.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
	}

	// 调用createAndUpdateQueueData
	ns.createAndUpdateQueueData("broker-a", topicConfig)

	// 验证队列数据已创建
	ns.routeTable.mutex.RLock()
	queues, exists := ns.routeTable.topicQueueTable["TestTopic"]
	ns.routeTable.mutex.RUnlock()

	if !exists {
		t.Error("Topic should exist in topicQueueTable")
	}
	if len(queues) != 4 {
		t.Errorf("Should have 4 queues, got %d", len(queues))
	}

	// 验证队列属性
	for i, queue := range queues {
		if queue.Topic != "TestTopic" {
			t.Errorf("Queue %d topic mismatch, expected TestTopic, got %s", i, queue.Topic)
		}
		if queue.BrokerName != "broker-a" {
			t.Errorf("Queue %d broker name mismatch, expected broker-a, got %s", i, queue.BrokerName)
		}
		if queue.QueueId != int32(i) {
			t.Errorf("Queue %d queue id mismatch, expected %d, got %d", i, i, queue.QueueId)
		}
	}
}

// TestScanNotActiveBrokerInternal 测试扫描非活跃Broker
func TestScanNotActiveBrokerInternal(t *testing.T) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	// 添加一个过期的Broker
	expiredTime := time.Now().Add(-10 * time.Minute)
	ns.mutex.Lock()
	ns.brokerLiveTable["127.0.0.1:10911"] = &BrokerLiveInfo{
		LastUpdateTimestamp: expiredTime,
		DataVersion:         protocol.NewDataVersion(),
		HaServerAddr:        "127.0.0.1:10912",
	}
	ns.mutex.Unlock()

	// 添加Broker到路由表
	ns.routeTable.mutex.Lock()
	ns.routeTable.brokerLiveTable["127.0.0.1:10911"] = &BrokerLiveInfo{
		LastUpdateTimestamp: expiredTime,
		DataVersion:         protocol.NewDataVersion(),
		HaServerAddr:        "127.0.0.1:10912",
	}
	ns.routeTable.mutex.Unlock()

	// 执行扫描
	ns.scanNotActiveBrokerInternal()

	// 验证过期Broker已被移除
	ns.mutex.RLock()
	_, exists := ns.brokerLiveTable["127.0.0.1:10911"]
	ns.mutex.RUnlock()

	if exists {
		t.Error("Expired broker should be removed from brokerLiveTable")
	}
}

// TestConcurrentBrokerRegistration 测试并发Broker注册
func TestConcurrentBrokerRegistration(t *testing.T) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	topicConfig := &protocol.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
	}

	topicConfigWrapper := &protocol.TopicConfigSerializeWrapper{
		TopicConfigTable: map[string]*protocol.TopicConfig{
			"TestTopic": topicConfig,
		},
		DataVersion: protocol.NewDataVersion(),
	}

	// 并发注册多个Broker
	const numBrokers = 10
	done := make(chan bool, numBrokers)

	for i := 0; i < numBrokers; i++ {
		go func(id int) {
			brokerName := fmt.Sprintf("broker-%d", id)
			brokerAddr := fmt.Sprintf("127.0.0.1:%d", 10911+id)
			haServerAddr := fmt.Sprintf("127.0.0.1:%d", 10912+id)

			result := ns.RegisterBroker(
				"DefaultCluster",
				brokerAddr,
				brokerName,
				0,
				haServerAddr,
				topicConfigWrapper,
				[]string{},
				nil,
			)

			if result == nil {
				t.Errorf("RegisterBroker failed for broker-%d", id)
			}
			done <- true
		}(i)
	}

	// 等待所有注册完成
	for i := 0; i < numBrokers; i++ {
		<-done
	}

	// 验证所有Broker都已注册
	ns.mutex.RLock()
	brokerCount := len(ns.routeTable.brokerAddrTable)
	ns.mutex.RUnlock()

	if brokerCount != numBrokers {
		t.Errorf("Expected %d brokers, got %d", numBrokers, brokerCount)
	}
}

// BenchmarkRegisterBroker 基准测试Broker注册
func BenchmarkRegisterBroker(b *testing.B) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	topicConfig := &protocol.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
	}

	topicConfigWrapper := &protocol.TopicConfigSerializeWrapper{
		TopicConfigTable: map[string]*protocol.TopicConfig{
			"TestTopic": topicConfig,
		},
		DataVersion: protocol.NewDataVersion(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brokerName := fmt.Sprintf("broker-%d", i)
		brokerAddr := fmt.Sprintf("127.0.0.1:%d", 10911+i)
		haServerAddr := fmt.Sprintf("127.0.0.1:%d", 10912+i)

		ns.RegisterBroker(
			"DefaultCluster",
			brokerAddr,
			brokerName,
			0,
			haServerAddr,
			topicConfigWrapper,
			[]string{},
			nil,
		)
	}
}

// BenchmarkGetRouteInfoByTopic 基准测试获取路由信息
func BenchmarkGetRouteInfoByTopic(b *testing.B) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	// 预先注册一些Broker
	topicConfig := &protocol.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
	}

	topicConfigWrapper := &protocol.TopicConfigSerializeWrapper{
		TopicConfigTable: map[string]*protocol.TopicConfig{
			"TestTopic": topicConfig,
		},
		DataVersion: protocol.NewDataVersion(),
	}

	for i := 0; i < 10; i++ {
		brokerName := fmt.Sprintf("broker-%d", i)
		brokerAddr := fmt.Sprintf("127.0.0.1:%d", 10911+i)
		haServerAddr := fmt.Sprintf("127.0.0.1:%d", 10912+i)

		ns.RegisterBroker(
			"DefaultCluster",
			brokerAddr,
			brokerName,
			0,
			haServerAddr,
			topicConfigWrapper,
			[]string{},
			nil,
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ns.GetRouteInfoByTopic("TestTopic")
	}
}

// BenchmarkGetAllClusterInfo 基准测试获取集群信息
func BenchmarkGetAllClusterInfo(b *testing.B) {
	config := DefaultConfig()
	ns := NewNameServer(config)

	// 预先注册一些Broker
	topicConfig := &protocol.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
	}

	topicConfigWrapper := &protocol.TopicConfigSerializeWrapper{
		TopicConfigTable: map[string]*protocol.TopicConfig{
			"TestTopic": topicConfig,
		},
		DataVersion: protocol.NewDataVersion(),
	}

	for i := 0; i < 10; i++ {
		brokerName := fmt.Sprintf("broker-%d", i)
		brokerAddr := fmt.Sprintf("127.0.0.1:%d", 10911+i)
		haServerAddr := fmt.Sprintf("127.0.0.1:%d", 10912+i)

		ns.RegisterBroker(
			"DefaultCluster",
			brokerAddr,
			brokerName,
			0,
			haServerAddr,
			topicConfigWrapper,
			[]string{},
			nil,
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ns.GetAllClusterInfo()
	}
}