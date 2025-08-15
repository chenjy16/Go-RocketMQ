package cluster

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// TestBrokerStatus 测试Broker状态枚举
func TestBrokerStatus(t *testing.T) {
	if ONLINE != 0 {
		t.Errorf("Expected ONLINE to be 0, got %d", ONLINE)
	}
	
	if OFFLINE != 1 {
		t.Errorf("Expected OFFLINE to be 1, got %d", OFFLINE)
	}
	
	if SUSPECT != 2 {
		t.Errorf("Expected SUSPECT to be 2, got %d", SUSPECT)
	}
}

// TestLoadBalanceStrategy 测试负载均衡策略枚举
func TestLoadBalanceStrategy(t *testing.T) {
	if ROUND_ROBIN != 0 {
		t.Errorf("Expected ROUND_ROBIN to be 0, got %d", ROUND_ROBIN)
	}
	
	if RANDOM != 1 {
		t.Errorf("Expected RANDOM to be 1, got %d", RANDOM)
	}
	
	if LEAST_ACTIVE != 2 {
		t.Errorf("Expected LEAST_ACTIVE to be 2, got %d", LEAST_ACTIVE)
	}
	
	if WEIGHTED_ROUND_ROBIN != 3 {
		t.Errorf("Expected WEIGHTED_ROUND_ROBIN to be 3, got %d", WEIGHTED_ROUND_ROBIN)
	}
}

// TestBrokerInfo 测试Broker信息结构
func TestBrokerInfo(t *testing.T) {
	brokerInfo := &BrokerInfo{
		BrokerName:     "TestBroker",
		BrokerId:       0,
		ClusterName:    "TestCluster",
		BrokerAddr:     "127.0.0.1:10911",
		Version:        "1.0.0",
		DataVersion:    1,
		LastUpdateTime: time.Now().Unix(),
		Role:           "MASTER",
		Status:         ONLINE,
		Metrics: &BrokerMetrics{
			CpuUsage:        50.0,
			MemoryUsage:     60.0,
			DiskUsage:       30.0,
			NetworkIn:       1000,
			NetworkOut:      2000,
			MessageCount:    10000,
			Tps:             100,
			QueueDepth:      500,
			ConnectionCount: 10,
		},
		Topics: make(map[string]*TopicRouteInfo),
	}
	
	// 测试JSON序列化
	data, err := json.Marshal(brokerInfo)
	if err != nil {
		t.Fatalf("Failed to marshal BrokerInfo: %v", err)
	}
	
	// 测试JSON反序列化
	var newBrokerInfo BrokerInfo
	err = json.Unmarshal(data, &newBrokerInfo)
	if err != nil {
		t.Fatalf("Failed to unmarshal BrokerInfo: %v", err)
	}
	
	if newBrokerInfo.BrokerName != brokerInfo.BrokerName {
		t.Errorf("Expected BrokerName %s, got %s", brokerInfo.BrokerName, newBrokerInfo.BrokerName)
	}
	
	if newBrokerInfo.BrokerId != brokerInfo.BrokerId {
		t.Errorf("Expected BrokerId %d, got %d", brokerInfo.BrokerId, newBrokerInfo.BrokerId)
	}
	
	if newBrokerInfo.Status != brokerInfo.Status {
		t.Errorf("Expected Status %d, got %d", brokerInfo.Status, newBrokerInfo.Status)
	}
}

// TestBrokerMetrics 测试Broker指标
func TestBrokerMetrics(t *testing.T) {
	metrics := &BrokerMetrics{
		CpuUsage:        75.5,
		MemoryUsage:     80.2,
		DiskUsage:       45.8,
		NetworkIn:       5000,
		NetworkOut:      8000,
		MessageCount:    50000,
		Tps:             200,
		QueueDepth:      1000,
		ConnectionCount: 25,
	}
	
	// 测试JSON序列化
	data, err := json.Marshal(metrics)
	if err != nil {
		t.Fatalf("Failed to marshal BrokerMetrics: %v", err)
	}
	
	// 测试JSON反序列化
	var newMetrics BrokerMetrics
	err = json.Unmarshal(data, &newMetrics)
	if err != nil {
		t.Fatalf("Failed to unmarshal BrokerMetrics: %v", err)
	}
	
	if newMetrics.CpuUsage != metrics.CpuUsage {
		t.Errorf("Expected CpuUsage %.2f, got %.2f", metrics.CpuUsage, newMetrics.CpuUsage)
	}
	
	if newMetrics.MessageCount != metrics.MessageCount {
		t.Errorf("Expected MessageCount %d, got %d", metrics.MessageCount, newMetrics.MessageCount)
	}
}

// TestTopicRouteInfo 测试Topic路由信息
func TestTopicRouteInfo(t *testing.T) {
	routeInfo := &TopicRouteInfo{
		TopicName: "TestTopic",
		QueueDatas: []*QueueData{
			{
				BrokerName:     "TestBroker",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
				TopicSynFlag:   0,
			},
		},
		BrokerDatas: []*BrokerData{
			{
				Cluster:    "TestCluster",
				BrokerName: "TestBroker",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10911",
				},
			},
		},
		OrderTopicConf: &OrderTopicConf{
			OrderConf: "queue:4",
		},
	}
	
	// 测试JSON序列化
	data, err := json.Marshal(routeInfo)
	if err != nil {
		t.Fatalf("Failed to marshal TopicRouteInfo: %v", err)
	}
	
	// 测试JSON反序列化
	var newRouteInfo TopicRouteInfo
	err = json.Unmarshal(data, &newRouteInfo)
	if err != nil {
		t.Fatalf("Failed to unmarshal TopicRouteInfo: %v", err)
	}
	
	if newRouteInfo.TopicName != routeInfo.TopicName {
		t.Errorf("Expected TopicName %s, got %s", routeInfo.TopicName, newRouteInfo.TopicName)
	}
	
	if len(newRouteInfo.QueueDatas) != len(routeInfo.QueueDatas) {
		t.Errorf("Expected %d QueueDatas, got %d", len(routeInfo.QueueDatas), len(newRouteInfo.QueueDatas))
	}
	
	if len(newRouteInfo.BrokerDatas) != len(routeInfo.BrokerDatas) {
		t.Errorf("Expected %d BrokerDatas, got %d", len(routeInfo.BrokerDatas), len(newRouteInfo.BrokerDatas))
	}
}

// TestQueueData 测试队列数据
func TestQueueData(t *testing.T) {
	queueData := &QueueData{
		BrokerName:     "TestBroker",
		ReadQueueNums:  8,
		WriteQueueNums: 8,
		Perm:           6,
		TopicSynFlag:   0,
	}
	
	if queueData.BrokerName != "TestBroker" {
		t.Errorf("Expected BrokerName 'TestBroker', got %s", queueData.BrokerName)
	}
	
	if queueData.ReadQueueNums != 8 {
		t.Errorf("Expected ReadQueueNums 8, got %d", queueData.ReadQueueNums)
	}
	
	if queueData.WriteQueueNums != 8 {
		t.Errorf("Expected WriteQueueNums 8, got %d", queueData.WriteQueueNums)
	}
}

// TestBrokerData 测试Broker数据
func TestBrokerData(t *testing.T) {
	brokerData := &BrokerData{
		Cluster:    "TestCluster",
		BrokerName: "TestBroker",
		BrokerAddrs: map[int64]string{
			0: "127.0.0.1:10911",
			1: "127.0.0.1:10912",
		},
	}
	
	if brokerData.Cluster != "TestCluster" {
		t.Errorf("Expected Cluster 'TestCluster', got %s", brokerData.Cluster)
	}
	
	if brokerData.BrokerName != "TestBroker" {
		t.Errorf("Expected BrokerName 'TestBroker', got %s", brokerData.BrokerName)
	}
	
	if len(brokerData.BrokerAddrs) != 2 {
		t.Errorf("Expected 2 broker addresses, got %d", len(brokerData.BrokerAddrs))
	}
	
	if brokerData.BrokerAddrs[0] != "127.0.0.1:10911" {
		t.Errorf("Expected master address '127.0.0.1:10911', got %s", brokerData.BrokerAddrs[0])
	}
}

// TestNewClusterManager 测试创建集群管理器
func TestNewClusterManager(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	if cm == nil {
		t.Fatal("Expected ClusterManager to be created")
	}
	
	if cm.clusterName != "TestCluster" {
		t.Errorf("Expected cluster name 'TestCluster', got %s", cm.clusterName)
	}
	
	if cm.brokers == nil {
		t.Error("Expected brokers map to be initialized")
	}
	
	if cm.topicRoutes == nil {
		t.Error("Expected topicRoutes map to be initialized")
	}
	
	if cm.healthChecker == nil {
		t.Error("Expected healthChecker to be initialized")
	}
	
	if cm.loadBalancer == nil {
		t.Error("Expected loadBalancer to be initialized")
	}
	
	if cm.running {
		t.Error("Expected cluster manager to not be running initially")
	}
	
	if cm.healthCheckInterval != 30*time.Second {
		t.Errorf("Expected health check interval 30s, got %v", cm.healthCheckInterval)
	}
	
	if cm.brokerTimeout != 60*time.Second {
		t.Errorf("Expected broker timeout 60s, got %v", cm.brokerTimeout)
	}
}

// TestClusterManagerStartStop 测试集群管理器启动和停止
func TestClusterManagerStartStop(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	// 测试启动
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	
	if !cm.running {
		t.Error("Expected cluster manager to be running after start")
	}
	
	// 测试重复启动
	err = cm.Start()
	if err == nil {
		t.Error("Expected error for duplicate start")
	}
	
	// 测试停止
	cm.Stop()
	
	if cm.running {
		t.Error("Expected cluster manager to be stopped after stop")
	}
}

// TestRegisterBroker 测试注册Broker
func TestRegisterBroker(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	brokerInfo := &BrokerInfo{
		BrokerName:     "TestBroker",
		BrokerId:       0,
		ClusterName:    "TestCluster",
		BrokerAddr:     "127.0.0.1:10911",
		Version:        "1.0.0",
		DataVersion:    1,
		LastUpdateTime: time.Now().Unix(),
		Role:           "MASTER",
		Status:         ONLINE,
	}
	
	// 测试注册Broker
	err := cm.RegisterBroker(brokerInfo)
	if err != nil {
		t.Fatalf("Failed to register broker: %v", err)
	}
	
	// 验证Broker已注册
	registeredBroker, exists := cm.GetBroker("TestBroker")
	if !exists {
		t.Error("Expected broker to be registered")
	}
	
	if registeredBroker.BrokerName != "TestBroker" {
		t.Errorf("Expected broker name 'TestBroker', got %s", registeredBroker.BrokerName)
	}
	
	// 测试注册空Broker
	err = cm.RegisterBroker(nil)
	if err == nil {
		t.Error("Expected error for nil broker")
	}
	
	// 测试注册空名称Broker
	emptyNameBroker := &BrokerInfo{
		BrokerName: "",
		BrokerId:   0,
	}
	err = cm.RegisterBroker(emptyNameBroker)
	if err == nil {
		t.Error("Expected error for empty broker name")
	}
}

// TestUnregisterBroker 测试注销Broker
func TestUnregisterBroker(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	brokerInfo := &BrokerInfo{
		BrokerName:  "TestBroker",
		BrokerId:    0,
		ClusterName: "TestCluster",
		BrokerAddr:  "127.0.0.1:10911",
		Status:      ONLINE,
	}
	
	// 先注册Broker
	err := cm.RegisterBroker(brokerInfo)
	if err != nil {
		t.Fatalf("Failed to register broker: %v", err)
	}
	
	// 验证Broker存在
	_, exists := cm.GetBroker("TestBroker")
	if !exists {
		t.Error("Expected broker to exist before unregistration")
	}
	
	// 测试注销Broker
	cm.UnregisterBroker("TestBroker")
	
	// 验证Broker已注销
	_, exists = cm.GetBroker("TestBroker")
	if exists {
		t.Error("Expected broker to be unregistered")
	}
	
	// 测试注销不存在的Broker
	cm.UnregisterBroker("NonExistentBroker") // 应该不会出错
}

// TestUpdateBrokerMetrics 测试更新Broker指标
func TestUpdateBrokerMetrics(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	brokerInfo := &BrokerInfo{
		BrokerName:  "TestBroker",
		BrokerId:    0,
		ClusterName: "TestCluster",
		BrokerAddr:  "127.0.0.1:10911",
		Status:      ONLINE,
	}
	
	// 先注册Broker
	err := cm.RegisterBroker(brokerInfo)
	if err != nil {
		t.Fatalf("Failed to register broker: %v", err)
	}
	
	metrics := &BrokerMetrics{
		CpuUsage:        80.0,
		MemoryUsage:     70.0,
		DiskUsage:       50.0,
		MessageCount:    20000,
		Tps:             150,
		ConnectionCount: 15,
	}
	
	// 测试更新指标
	err = cm.UpdateBrokerMetrics("TestBroker", metrics)
	if err != nil {
		t.Fatalf("Failed to update broker metrics: %v", err)
	}
	
	// 验证指标已更新
	updatedBroker, exists := cm.GetBroker("TestBroker")
	if !exists {
		t.Error("Expected broker to exist")
	}
	
	if updatedBroker.Metrics == nil {
		t.Fatal("Expected metrics to be set")
	}
	
	if updatedBroker.Metrics.CpuUsage != 80.0 {
		t.Errorf("Expected CPU usage 80.0, got %.2f", updatedBroker.Metrics.CpuUsage)
	}
	
	// 测试更新不存在的Broker指标
	err = cm.UpdateBrokerMetrics("NonExistentBroker", metrics)
	if err == nil {
		t.Error("Expected error for non-existent broker")
	}
}

// TestGetAllBrokers 测试获取所有Broker
func TestGetAllBrokers(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	// 测试空集群
	brokers := cm.GetAllBrokers()
	if len(brokers) != 0 {
		t.Errorf("Expected 0 brokers, got %d", len(brokers))
	}
	
	// 注册一些Broker
	for i := 0; i < 3; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("TestBroker%d", i),
			BrokerId:    int64(i),
			ClusterName: "TestCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:1091%d", i),
			Status:      ONLINE,
		}
		err := cm.RegisterBroker(brokerInfo)
		if err != nil {
			t.Fatalf("Failed to register broker %d: %v", i, err)
		}
	}
	
	// 测试获取所有Broker
	brokers = cm.GetAllBrokers()
	if len(brokers) != 3 {
		t.Errorf("Expected 3 brokers, got %d", len(brokers))
	}
	
	for i := 0; i < 3; i++ {
		brokerName := fmt.Sprintf("TestBroker%d", i)
		if _, exists := brokers[brokerName]; !exists {
			t.Errorf("Expected broker %s to exist", brokerName)
		}
	}
}

// TestGetOnlineBrokers 测试获取在线Broker
func TestGetOnlineBrokers(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	// 注册不同状态的Broker
	statuses := []BrokerStatus{ONLINE, OFFLINE, SUSPECT, ONLINE}
	for i, status := range statuses {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("TestBroker%d", i),
			BrokerId:    int64(i),
			ClusterName: "TestCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:1091%d", i),
			Status:      status,
		}
		err := cm.RegisterBroker(brokerInfo)
		if err != nil {
			t.Fatalf("Failed to register broker %d: %v", i, err)
		}
	}
	
	// 测试获取在线Broker
	onlineBrokers := cm.GetOnlineBrokers()
	if len(onlineBrokers) != 2 {
		t.Errorf("Expected 2 online brokers, got %d", len(onlineBrokers))
	}
	
	for _, broker := range onlineBrokers {
		if broker.Status != ONLINE {
			t.Errorf("Expected broker status to be ONLINE, got %d", broker.Status)
		}
	}
}

// TestRegisterTopicRoute 测试注册Topic路由
func TestRegisterTopicRoute(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	routeInfo := &TopicRouteInfo{
		TopicName: "TestTopic",
		QueueDatas: []*QueueData{
			{
				BrokerName:     "TestBroker",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
			},
		},
		BrokerDatas: []*BrokerData{
			{
				Cluster:    "TestCluster",
				BrokerName: "TestBroker",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10911",
				},
			},
		},
	}
	
	// 测试注册Topic路由
	cm.RegisterTopicRoute("TestTopic", routeInfo)
	
	// 验证路由已注册
	registeredRoute, exists := cm.GetTopicRoute("TestTopic")
	if !exists {
		t.Error("Expected topic route to be registered")
	}
	
	if registeredRoute.TopicName != "TestTopic" {
		t.Errorf("Expected topic name 'TestTopic', got %s", registeredRoute.TopicName)
	}
	
	if len(registeredRoute.QueueDatas) != 1 {
		t.Errorf("Expected 1 queue data, got %d", len(registeredRoute.QueueDatas))
	}
}

// TestGetTopicRoute 测试获取Topic路由
func TestGetTopicRoute(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	// 测试获取不存在的路由
	_, exists := cm.GetTopicRoute("NonExistentTopic")
	if exists {
		t.Error("Expected non-existent topic route to not exist")
	}
	
	// 注册路由
	routeInfo := &TopicRouteInfo{
		TopicName: "TestTopic",
		QueueDatas: []*QueueData{
			{
				BrokerName:     "TestBroker",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
			},
		},
	}
	cm.RegisterTopicRoute("TestTopic", routeInfo)
	
	// 测试获取已注册的路由
	retrievedRoute, exists := cm.GetTopicRoute("TestTopic")
	if !exists {
		t.Error("Expected topic route to exist")
	}
	
	if retrievedRoute.TopicName != "TestTopic" {
		t.Errorf("Expected topic name 'TestTopic', got %s", retrievedRoute.TopicName)
	}
}

// TestGetClusterStatus 测试获取集群状态
func TestGetClusterStatus(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	// 注册一些Broker
	for i := 0; i < 2; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("TestBroker%d", i),
			BrokerId:    int64(i),
			ClusterName: "TestCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:1091%d", i),
			Status:      ONLINE,
			Metrics: &BrokerMetrics{
				MessageCount: int64((i + 1) * 1000),
				Tps:          int64((i + 1) * 100),
			},
		}
		err := cm.RegisterBroker(brokerInfo)
		if err != nil {
			t.Fatalf("Failed to register broker %d: %v", i, err)
		}
	}
	
	// 测试获取集群状态
	status := cm.GetClusterStatus()
	if status == nil {
		t.Fatal("Expected cluster status to be returned")
	}
	
	if status["clusterName"] != "TestCluster" {
		t.Errorf("Expected cluster name 'TestCluster', got %v", status["clusterName"])
	}
	
	if status["totalBrokers"] != 2 {
		t.Errorf("Expected total brokers 2, got %v", status["totalBrokers"])
	}
	
	if status["onlineBrokers"] != 2 {
		t.Errorf("Expected online brokers 2, got %v", status["onlineBrokers"])
	}
	
	if status["totalMessages"] != int64(3000) {
		t.Errorf("Expected total messages 3000, got %v", status["totalMessages"])
	}
	
	if status["totalTps"] != int64(300) {
		t.Errorf("Expected total TPS 300, got %v", status["totalTps"])
	}
}

// TestGetBrokerMetrics 测试获取Broker指标
func TestGetBrokerMetrics(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	
	// 注册带指标的Broker
	for i := 0; i < 2; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("TestBroker%d", i),
			BrokerId:    int64(i),
			ClusterName: "TestCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:1091%d", i),
			Status:      ONLINE,
			Metrics: &BrokerMetrics{
				CpuUsage:     float64((i+1)*10 + 50),
				MemoryUsage:  float64((i+1)*5 + 60),
				MessageCount: int64((i+1)*1000),
			},
		}
		err := cm.RegisterBroker(brokerInfo)
		if err != nil {
			t.Fatalf("Failed to register broker %d: %v", i, err)
		}
	}
	
	// 测试获取Broker指标
	metrics := cm.GetBrokerMetrics()
	if len(metrics) != 2 {
		t.Errorf("Expected 2 broker metrics, got %d", len(metrics))
	}
	
	for i := 0; i < 2; i++ {
		brokerName := fmt.Sprintf("TestBroker%d", i)
		if metric, exists := metrics[brokerName]; exists {
			expectedCpu := float64((i+1)*10 + 50)
			if metric.CpuUsage != expectedCpu {
				t.Errorf("Expected CPU usage %.2f for %s, got %.2f", expectedCpu, brokerName, metric.CpuUsage)
			}
		} else {
			t.Errorf("Expected metrics for broker %s", brokerName)
		}
	}
}

// TestNewLoadBalancer 测试创建负载均衡器
func TestNewLoadBalancer(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	lb := NewLoadBalancer(cm)
	
	if lb == nil {
		t.Fatal("Expected LoadBalancer to be created")
	}
	
	if lb.clusterManager != cm {
		t.Error("Expected cluster manager to match")
	}
	
	if lb.strategy != ROUND_ROBIN {
		t.Errorf("Expected default strategy to be ROUND_ROBIN, got %d", lb.strategy)
	}
}

// TestLoadBalancerSetStrategy 测试设置负载均衡策略
func TestLoadBalancerSetStrategy(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	lb := NewLoadBalancer(cm)
	
	// 测试设置不同策略
	strategies := []LoadBalanceStrategy{RANDOM, LEAST_ACTIVE, WEIGHTED_ROUND_ROBIN, ROUND_ROBIN}
	for _, strategy := range strategies {
		lb.SetStrategy(strategy)
		if lb.strategy != strategy {
			t.Errorf("Expected strategy %d, got %d", strategy, lb.strategy)
		}
	}
}

// TestSelectBrokerForProducer 测试为生产者选择Broker
func TestSelectBrokerForProducer(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	lb := NewLoadBalancer(cm)
	
	// 测试没有Broker的情况
	_, err := lb.SelectBrokerForProducer("TestTopic")
	if err == nil {
		t.Error("Expected error when no brokers available")
	}
	
	// 注册一些在线的Master Broker
	for i := 0; i < 3; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("TestBroker%d", i),
			BrokerId:    0, // Master
			ClusterName: "TestCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:1091%d", i),
			Role:        "MASTER",
			Status:      ONLINE,
		}
		err := cm.RegisterBroker(brokerInfo)
		if err != nil {
			t.Fatalf("Failed to register broker %d: %v", i, err)
		}
	}
	
	// 测试选择Broker
	selectedBroker, err := lb.SelectBrokerForProducer("TestTopic")
	if err != nil {
		t.Fatalf("Failed to select broker for producer: %v", err)
	}
	
	if selectedBroker == nil {
		t.Fatal("Expected broker to be selected")
	}
	
	if selectedBroker.Role != "MASTER" {
		t.Errorf("Expected selected broker to be MASTER, got %s", selectedBroker.Role)
	}
	
	if selectedBroker.Status != ONLINE {
		t.Errorf("Expected selected broker to be ONLINE, got %d", selectedBroker.Status)
	}
}

// TestSelectBrokerForConsumer 测试为消费者选择Broker
func TestSelectBrokerForConsumer(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	lb := NewLoadBalancer(cm)
	
	// 注册一些Broker
	for i := 0; i < 2; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("TestBroker%d", i),
			BrokerId:    int64(i),
			ClusterName: "TestCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:1091%d", i),
			Status:      ONLINE,
		}
		err := cm.RegisterBroker(brokerInfo)
		if err != nil {
			t.Fatalf("Failed to register broker %d: %v", i, err)
		}
	}
	
	// 注册Topic路由信息
	routeInfo := &TopicRouteInfo{
		TopicName: "TestTopic",
		QueueDatas: []*QueueData{
			{
				BrokerName:     "TestBroker0",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
			},
			{
				BrokerName:     "TestBroker1",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
			},
		},
		BrokerDatas: []*BrokerData{
			{
				Cluster:    "TestCluster",
				BrokerName: "TestBroker0",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10910",
				},
			},
			{
				Cluster:    "TestCluster",
				BrokerName: "TestBroker1",
				BrokerAddrs: map[int64]string{
					1: "127.0.0.1:10911",
				},
			},
		},
	}
	cm.RegisterTopicRoute("TestTopic", routeInfo)
	
	// 测试选择Broker
	selectedBroker, err := lb.SelectBrokerForConsumer("TestTopic", 0)
	if err != nil {
		t.Fatalf("Failed to select broker for consumer: %v", err)
	}
	
	if selectedBroker == nil {
		t.Fatal("Expected broker to be selected")
	}
	
	if selectedBroker.Status != ONLINE {
		t.Errorf("Expected selected broker to be ONLINE, got %d", selectedBroker.Status)
	}
}

// TestNewHealthChecker 测试创建健康检查器
func TestNewHealthChecker(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	hc := NewHealthChecker(cm)
	
	if hc == nil {
		t.Fatal("Expected HealthChecker to be created")
	}
	
	if hc.clusterManager != cm {
		t.Error("Expected cluster manager to match")
	}
	
	if hc.running {
		t.Error("Expected health checker to not be running initially")
	}
	
	if hc.checkInterval != 30*time.Second {
		t.Errorf("Expected check interval 30s, got %v", hc.checkInterval)
	}
	
	if hc.timeout != 5*time.Second {
		t.Errorf("Expected timeout 5s, got %v", hc.timeout)
	}
	
	if hc.httpClient == nil {
		t.Error("Expected HTTP client to be initialized")
	}
	
	if hc.shutdown == nil {
		t.Error("Expected shutdown channel to be initialized")
	}
}

// TestHealthCheckerStartStop 测试健康检查器启动和停止
func TestHealthCheckerStartStop(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	hc := NewHealthChecker(cm)
	
	// 测试启动
	err := hc.Start()
	if err != nil {
		t.Fatalf("Failed to start health checker: %v", err)
	}
	
	if !hc.running {
		t.Error("Expected health checker to be running after start")
	}
	
	// 测试重复启动
	err = hc.Start()
	if err == nil {
		t.Error("Expected error for duplicate start")
	}
	
	// 测试停止
	hc.Stop()
	
	if hc.running {
		t.Error("Expected health checker to be stopped after stop")
	}
}

// TestHealthCheckerSetters 测试健康检查器设置方法
func TestHealthCheckerSetters(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	hc := NewHealthChecker(cm)
	
	// 测试设置检查间隔
	newInterval := 60 * time.Second
	hc.SetCheckInterval(newInterval)
	if hc.checkInterval != newInterval {
		t.Errorf("Expected check interval %v, got %v", newInterval, hc.checkInterval)
	}
	
	// 测试设置超时时间
	newTimeout := 10 * time.Second
	hc.SetTimeout(newTimeout)
	if hc.timeout != newTimeout {
		t.Errorf("Expected timeout %v, got %v", newTimeout, hc.timeout)
	}
	
	if hc.httpClient.Timeout != newTimeout {
		t.Errorf("Expected HTTP client timeout %v, got %v", newTimeout, hc.httpClient.Timeout)
	}
}

// TestHealthCheckResult 测试健康检查结果
func TestHealthCheckResult(t *testing.T) {
	result := &HealthCheckResult{
		BrokerName: "TestBroker",
		Healthy:    true,
		Latency:    50 * time.Millisecond,
		Error:      "",
		Timestamp:  time.Now().Unix(),
	}
	
	// 测试JSON序列化
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal HealthCheckResult: %v", err)
	}
	
	// 测试JSON反序列化
	var newResult HealthCheckResult
	err = json.Unmarshal(data, &newResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal HealthCheckResult: %v", err)
	}
	
	if newResult.BrokerName != result.BrokerName {
		t.Errorf("Expected BrokerName %s, got %s", result.BrokerName, newResult.BrokerName)
	}
	
	if newResult.Healthy != result.Healthy {
		t.Errorf("Expected Healthy %t, got %t", result.Healthy, newResult.Healthy)
	}
	
	if newResult.Latency != result.Latency {
		t.Errorf("Expected Latency %v, got %v", result.Latency, newResult.Latency)
	}
}

// Benchmark tests

// BenchmarkNewClusterManager 基准测试创建集群管理器
func BenchmarkNewClusterManager(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewClusterManager("BenchmarkCluster")
	}
}

// BenchmarkRegisterBroker 基准测试注册Broker
func BenchmarkRegisterBroker(b *testing.B) {
	cm := NewClusterManager("BenchmarkCluster")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("Broker%d", i),
			BrokerId:    int64(i % 10),
			ClusterName: "BenchmarkCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:%d", 10911+i%100),
			Status:      ONLINE,
		}
		_ = cm.RegisterBroker(brokerInfo)
	}
}

// BenchmarkGetBroker 基准测试获取Broker
func BenchmarkGetBroker(b *testing.B) {
	cm := NewClusterManager("BenchmarkCluster")
	
	// 预先注册一些Broker
	for i := 0; i < 100; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("Broker%d", i),
			BrokerId:    int64(i),
			ClusterName: "BenchmarkCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:%d", 10911+i),
			Status:      ONLINE,
		}
		_ = cm.RegisterBroker(brokerInfo)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brokerName := fmt.Sprintf("Broker%d", i%100)
		_, _ = cm.GetBroker(brokerName)
	}
}

// BenchmarkSelectBrokerForProducer 基准测试为生产者选择Broker
func BenchmarkSelectBrokerForProducer(b *testing.B) {
	cm := NewClusterManager("BenchmarkCluster")
	lb := NewLoadBalancer(cm)
	
	// 预先注册一些Master Broker
	for i := 0; i < 10; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("Broker%d", i),
			BrokerId:    0, // Master
			ClusterName: "BenchmarkCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:%d", 10911+i),
			Role:        "MASTER",
			Status:      ONLINE,
		}
		_ = cm.RegisterBroker(brokerInfo)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic := fmt.Sprintf("Topic%d", i%5)
		_, _ = lb.SelectBrokerForProducer(topic)
	}
}

// BenchmarkGetClusterStatus 基准测试获取集群状态
func BenchmarkGetClusterStatus(b *testing.B) {
	cm := NewClusterManager("BenchmarkCluster")
	
	// 预先注册一些Broker
	for i := 0; i < 50; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:  fmt.Sprintf("Broker%d", i),
			BrokerId:    int64(i % 5),
			ClusterName: "BenchmarkCluster",
			BrokerAddr:  fmt.Sprintf("127.0.0.1:%d", 10911+i),
			Status:      ONLINE,
			Metrics: &BrokerMetrics{
				MessageCount: int64(i * 1000),
				Tps:          int64(i * 10),
			},
		}
		_ = cm.RegisterBroker(brokerInfo)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cm.GetClusterStatus()
	}
}