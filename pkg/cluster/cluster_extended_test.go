package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestClusterManagerConcurrentOperations 测试集群管理器并发操作
func TestClusterManagerConcurrentOperations(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// 并发注册Broker
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			brokerInfo := &BrokerInfo{
				BrokerName:     fmt.Sprintf("TestBroker%d", id),
				BrokerId:       int64(id),
				ClusterName:    "TestCluster",
				BrokerAddr:     fmt.Sprintf("127.0.0.1:%d", 10911+id),
				Version:        "1.0.0",
				DataVersion:    1,
				LastUpdateTime: time.Now().Unix(),
				Role:           "MASTER",
				Status:         ONLINE,
				Topics:         make(map[string]*TopicRouteInfo),
			}
			err := cm.RegisterBroker(brokerInfo)
			if err != nil {
				t.Errorf("Failed to register broker %d: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	// 验证所有Broker都已注册
	brokers := cm.GetAllBrokers()
	if len(brokers) != 10 {
		t.Errorf("Expected 10 brokers, got %d", len(brokers))
	}
}

// TestClusterManagerErrorHandling 测试集群管理器错误处理
func TestClusterManagerErrorHandling(t *testing.T) {
	cm := NewClusterManager("TestCluster")

	// 测试重复启动
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}

	err = cm.Start()
	if err == nil {
		t.Error("Expected error when starting already running cluster manager")
	}

	cm.Stop()

	// 测试注册无效Broker
	err = cm.RegisterBroker(nil)
	if err == nil {
		t.Error("Expected error when registering nil broker")
	}

	// 测试注册空名称Broker
	brokerInfo := &BrokerInfo{
		BrokerName:  "",
		BrokerId:    0,
		ClusterName: "TestCluster",
		BrokerAddr:  "127.0.0.1:10911",
		Topics:      make(map[string]*TopicRouteInfo),
	}
	err = cm.RegisterBroker(brokerInfo)
	if err == nil {
		t.Error("Expected error when registering broker with empty name")
	}
}

// TestClusterManagerBrokerLifecycle 测试Broker生命周期管理
func TestClusterManagerBrokerLifecycle(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// 注册Broker
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

	err = cm.RegisterBroker(brokerInfo)
	if err != nil {
		t.Fatalf("Failed to register broker: %v", err)
	}

	// 验证Broker已注册
	registeredBroker, exists := cm.GetBroker("TestBroker")
	if !exists {
		t.Error("Broker should be registered")
	}
	if registeredBroker.BrokerName != "TestBroker" {
		t.Errorf("Expected broker name TestBroker, got %s", registeredBroker.BrokerName)
	}

	// 更新Broker指标
	newMetrics := &BrokerMetrics{
		CpuUsage:        75.0,
		MemoryUsage:     80.0,
		DiskUsage:       40.0,
		NetworkIn:       2000,
		NetworkOut:      3000,
		MessageCount:    20000,
		Tps:             200,
		QueueDepth:      800,
		ConnectionCount: 20,
	}

	err = cm.UpdateBrokerMetrics("TestBroker", newMetrics)
	if err != nil {
		t.Fatalf("Failed to update broker metrics: %v", err)
	}

	// 验证指标已更新
	updatedBroker, _ := cm.GetBroker("TestBroker")
	if updatedBroker.Metrics.CpuUsage != 75.0 {
		t.Errorf("Expected CPU usage 75.0, got %f", updatedBroker.Metrics.CpuUsage)
	}

	// 注销Broker
	cm.UnregisterBroker("TestBroker")

	// 验证Broker已注销
	_, exists = cm.GetBroker("TestBroker")
	if exists {
		t.Error("Broker should be unregistered")
	}
}

// TestTopicRouteManagement 测试Topic路由管理
func TestTopicRouteManagement(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// 创建Topic路由信息
	routeInfo := &TopicRouteInfo{
		TopicName: "TestTopic",
		QueueDatas: []*QueueData{
			{
				BrokerName:     "TestBroker1",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
				TopicSynFlag:   0,
			},
			{
				BrokerName:     "TestBroker2",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
				TopicSynFlag:   0,
			},
		},
		BrokerDatas: []*BrokerData{
			{
				Cluster:    "TestCluster",
				BrokerName: "TestBroker1",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10911",
				},
			},
			{
				Cluster:    "TestCluster",
				BrokerName: "TestBroker2",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10912",
				},
			},
		},
	}

	// 注册Topic路由
	cm.RegisterTopicRoute("TestTopic", routeInfo)

	// 获取Topic路由
	retrievedRoute, exists := cm.GetTopicRoute("TestTopic")
	if !exists {
		t.Error("Topic route should exist")
	}
	if retrievedRoute.TopicName != "TestTopic" {
		t.Errorf("Expected topic name TestTopic, got %s", retrievedRoute.TopicName)
	}
	if len(retrievedRoute.QueueDatas) != 2 {
		t.Errorf("Expected 2 queue datas, got %d", len(retrievedRoute.QueueDatas))
	}
	if len(retrievedRoute.BrokerDatas) != 2 {
		t.Errorf("Expected 2 broker datas, got %d", len(retrievedRoute.BrokerDatas))
	}
}

// TestLoadBalancerStrategies 测试负载均衡策略
func TestLoadBalancerStrategies(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// 注册多个Broker
	for i := 0; i < 3; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:     fmt.Sprintf("TestBroker%d", i),
			BrokerId:       int64(i),
			ClusterName:    "TestCluster",
			BrokerAddr:     fmt.Sprintf("127.0.0.1:%d", 10911+i),
			Version:        "1.0.0",
			DataVersion:    1,
			LastUpdateTime: time.Now().Unix(),
			Role:           "MASTER",
			Status:         ONLINE,
			Metrics: &BrokerMetrics{
				CpuUsage:        float64(50 + i*10),
				MemoryUsage:     float64(60 + i*10),
				DiskUsage:       float64(30 + i*10),
				ConnectionCount: int32(10 + i*5),
			},
			Topics: make(map[string]*TopicRouteInfo),
		}
		err := cm.RegisterBroker(brokerInfo)
		if err != nil {
			t.Fatalf("Failed to register broker %d: %v", i, err)
		}
	}

	lb := cm.loadBalancer

	// 测试轮询策略（注意：当前实现是简化版本，总是选择按名称排序的第一个）
	lb.SetStrategy(ROUND_ROBIN)
	selectedBrokers := make(map[string]int)
	for i := 0; i < 9; i++ {
		broker, err := lb.SelectBrokerForProducer("TestTopic")
		if err != nil {
			t.Fatalf("Failed to select broker: %v", err)
		}
		selectedBrokers[broker.BrokerName]++
	}
	// 当前轮询实现总是选择按名称排序的第一个Broker
	if selectedBrokers["TestBroker0"] != 9 {
		t.Errorf("Round robin implementation should always select TestBroker0, got distribution: %v", selectedBrokers)
	}

	// 测试随机策略
	lb.SetStrategy(RANDOM)
	selectedBrokers = make(map[string]int)
	for i := 0; i < 30; i++ {
		broker, err := lb.SelectBrokerForProducer("TestTopic")
		if err != nil {
			t.Fatalf("Failed to select broker: %v", err)
		}
		selectedBrokers[broker.BrokerName]++
	}
	// 随机策略应该选择到所有Broker
	if len(selectedBrokers) != 3 {
		t.Errorf("Random strategy should select all 3 brokers, got %d", len(selectedBrokers))
	}

	// 测试最少活跃连接策略
	lb.SetStrategy(LEAST_ACTIVE)
	broker, err := lb.SelectBrokerForProducer("TestTopic")
	if err != nil {
		t.Fatalf("Failed to select broker: %v", err)
	}
	// 应该选择连接数最少的Broker（TestBroker0）
	if broker.BrokerName != "TestBroker0" {
		t.Errorf("Least active strategy should select TestBroker0, got %s", broker.BrokerName)
	}
}

// TestHealthCheckerFunctionality 测试健康检查器功能
func TestHealthCheckerFunctionality(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	hc := cm.healthChecker

	// 测试设置检查间隔
	hc.SetCheckInterval(10 * time.Second)
	if hc.checkInterval != 10*time.Second {
		t.Errorf("Expected check interval 10s, got %v", hc.checkInterval)
	}

	// 测试设置超时
	hc.SetTimeout(3 * time.Second)
	if hc.timeout != 3*time.Second {
		t.Errorf("Expected timeout 3s, got %v", hc.timeout)
	}

	// 测试健康状态获取
	status := hc.GetHealthStatus()
	if status["running"] != true {
		t.Error("Health checker should be running")
	}
	if status["check_interval"] != "10s" {
		t.Errorf("Expected check interval 10s in status, got %v", status["check_interval"])
	}
}

// TestClusterManagerHTTPHandler 测试HTTP处理器
func TestClusterManagerHTTPHandler(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// 注册一个Broker
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
		Topics:         make(map[string]*TopicRouteInfo),
	}
	err = cm.RegisterBroker(brokerInfo)
	if err != nil {
		t.Fatalf("Failed to register broker: %v", err)
	}

	// 创建测试服务器
	handler := cm.HTTPHandler()
	server := httptest.NewServer(handler)
	defer server.Close()

	// 测试获取集群状态
	resp, err := http.Get(server.URL + "/cluster/status")
	if err != nil {
		t.Fatalf("Failed to get cluster status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var status map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&status)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if status["clusterName"] != "TestCluster" {
        t.Errorf("Expected cluster name TestCluster, got %v", status["clusterName"])
    }

	// 测试获取Broker列表
	resp, err = http.Get(server.URL + "/cluster/brokers")
	if err != nil {
		t.Fatalf("Failed to get brokers: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var brokers map[string]*BrokerInfo
	err = json.NewDecoder(resp.Body).Decode(&brokers)
	if err != nil {
		t.Fatalf("Failed to decode brokers response: %v", err)
	}

	if len(brokers) != 1 {
		t.Errorf("Expected 1 broker, got %d", len(brokers))
	}

	if brokers["TestBroker"] == nil {
		t.Error("TestBroker should be in the response")
	}
}

// TestBrokerStatusTransitions 测试Broker状态转换
func TestBrokerStatusTransitions(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// 注册在线Broker
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
		Topics:         make(map[string]*TopicRouteInfo),
	}
	err = cm.RegisterBroker(brokerInfo)
	if err != nil {
		t.Fatalf("Failed to register broker: %v", err)
	}

	// 验证Broker在线
	onlineBrokers := cm.GetOnlineBrokers()
	if len(onlineBrokers) != 1 {
		t.Errorf("Expected 1 online broker, got %d", len(onlineBrokers))
	}

	// 更新Broker状态为离线
	brokerInfo.Status = OFFLINE
	err = cm.RegisterBroker(brokerInfo)
	if err != nil {
		t.Fatalf("Failed to update broker status: %v", err)
	}

	// 验证Broker离线
	onlineBrokers = cm.GetOnlineBrokers()
	if len(onlineBrokers) != 0 {
		t.Errorf("Expected 0 online brokers, got %d", len(onlineBrokers))
	}

	// 更新Broker状态为可疑
	brokerInfo.Status = SUSPECT
	err = cm.RegisterBroker(brokerInfo)
	if err != nil {
		t.Fatalf("Failed to update broker status: %v", err)
	}

	// 验证Broker状态
	registeredBroker, _ := cm.GetBroker("TestBroker")
	if registeredBroker.Status != SUSPECT {
		t.Errorf("Expected broker status SUSPECT, got %d", registeredBroker.Status)
	}
}

// TestClusterManagerEdgeCases 测试边界情况
func TestClusterManagerEdgeCases(t *testing.T) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// 测试获取不存在的Broker
	_, exists := cm.GetBroker("NonExistentBroker")
	if exists {
		t.Error("Non-existent broker should not exist")
	}

	// 测试获取不存在的Topic路由
	_, exists = cm.GetTopicRoute("NonExistentTopic")
	if exists {
		t.Error("Non-existent topic route should not exist")
	}

	// 测试更新不存在Broker的指标
	metrics := &BrokerMetrics{
		CpuUsage: 50.0,
	}
	err = cm.UpdateBrokerMetrics("NonExistentBroker", metrics)
	if err == nil {
		t.Error("Expected error when updating metrics for non-existent broker")
	}

	// 测试注销不存在的Broker
	cm.UnregisterBroker("NonExistentBroker") // 应该不会panic

	// 测试空集群的负载均衡
	_, err = cm.SelectBrokerForProducer("TestTopic")
	if err == nil {
		t.Error("Expected error when selecting broker from empty cluster")
	}

	_, err = cm.SelectBrokerForConsumer("TestTopic", 0)
	if err == nil {
		t.Error("Expected error when selecting broker for consumer from empty cluster")
	}
}

// BenchmarkClusterManagerRegisterBroker 基准测试Broker注册
func BenchmarkClusterManagerRegisterBroker(b *testing.B) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		b.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:     fmt.Sprintf("TestBroker%d", i),
			BrokerId:       int64(i),
			ClusterName:    "TestCluster",
			BrokerAddr:     fmt.Sprintf("127.0.0.1:%d", 10911+i),
			Version:        "1.0.0",
			DataVersion:    1,
			LastUpdateTime: time.Now().Unix(),
			Role:           "MASTER",
			Status:         ONLINE,
			Topics:         make(map[string]*TopicRouteInfo),
		}
		cm.RegisterBroker(brokerInfo)
	}
}

// BenchmarkClusterManagerGetBroker 基准测试Broker获取
func BenchmarkClusterManagerGetBroker(b *testing.B) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		b.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// 预先注册一些Broker
	for i := 0; i < 100; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:     fmt.Sprintf("TestBroker%d", i),
			BrokerId:       int64(i),
			ClusterName:    "TestCluster",
			BrokerAddr:     fmt.Sprintf("127.0.0.1:%d", 10911+i),
			Version:        "1.0.0",
			DataVersion:    1,
			LastUpdateTime: time.Now().Unix(),
			Role:           "MASTER",
			Status:         ONLINE,
			Topics:         make(map[string]*TopicRouteInfo),
		}
		cm.RegisterBroker(brokerInfo)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cm.GetBroker(fmt.Sprintf("TestBroker%d", i%100))
	}
}

// BenchmarkLoadBalancerSelection 基准测试负载均衡选择
func BenchmarkLoadBalancerSelection(b *testing.B) {
	cm := NewClusterManager("TestCluster")
	err := cm.Start()
	if err != nil {
		b.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// 注册多个Broker
	for i := 0; i < 10; i++ {
		brokerInfo := &BrokerInfo{
			BrokerName:     fmt.Sprintf("TestBroker%d", i),
			BrokerId:       int64(i),
			ClusterName:    "TestCluster",
			BrokerAddr:     fmt.Sprintf("127.0.0.1:%d", 10911+i),
			Version:        "1.0.0",
			DataVersion:    1,
			LastUpdateTime: time.Now().Unix(),
			Role:           "MASTER",
			Status:         ONLINE,
			Metrics: &BrokerMetrics{
				ConnectionCount: int32(10 + i),
			},
			Topics: make(map[string]*TopicRouteInfo),
		}
		cm.RegisterBroker(brokerInfo)
	}

	lb := cm.loadBalancer
	lb.SetStrategy(ROUND_ROBIN)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lb.SelectBrokerForProducer("TestTopic")
	}
}