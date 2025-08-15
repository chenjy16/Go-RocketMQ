package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go-rocketmq/pkg/failover"
	"go-rocketmq/pkg/ha"
	"go-rocketmq/pkg/cluster"
)

// FailoverHADemo 演示故障转移和高可用功能的使用
func main() {
	fmt.Println("Go-RocketMQ Failover & HA Demo")
	fmt.Println("==============================")

	// 1. 演示故障转移服务
	demoFailoverService()

	// 2. 演示高可用服务
	demoHAService()

	fmt.Println("\n🎉 Demo completed successfully!")
}

// demoFailoverService 演示故障转移服务的基本使用
func demoFailoverService() {
	fmt.Println("\n1. Failover Service Demo")
	fmt.Println("--------------------------")

	// 创建集群管理器
	clusterManager := cluster.NewClusterManager("demo-cluster")
	if clusterManager == nil {
		log.Fatal("Failed to create cluster manager")
	}

	// 创建故障转移服务
	service := failover.NewFailoverService(clusterManager)
	if service == nil {
		log.Fatal("Failed to create failover service")
	}

	fmt.Println("✅ Failover service created successfully")

	// 启动服务
	err := service.Start()
	if err != nil {
		log.Fatalf("Failed to start failover service: %v", err)
	}
	fmt.Println("✅ Failover service started")

	// 创建故障转移策略
	policy := &failover.FailoverPolicy{
		BrokerName:      "broker-1",
		FailoverType:    failover.AUTO_FAILOVER,
		BackupBrokers:   []string{"broker-2", "broker-3"},
		AutoFailover:    true,
		FailoverDelay:   5 * time.Second,
		HealthThreshold: 3,
		RecoveryPolicy:  failover.AUTO_RECOVERY,
	}

	// 注册故障转移策略
	err = service.RegisterFailoverPolicy(policy)
	if err != nil {
		log.Printf("Failed to register policy: %v", err)
	} else {
		fmt.Println("✅ Failover policy registered for broker-1")
	}

	// 获取服务状态
	status := service.GetFailoverStatus()
	fmt.Printf("📊 Failover Status:\n")
	fmt.Printf("   - Running: %v\n", status["running"])
	fmt.Printf("   - Policy Count: %v\n", status["policy_count"])
	fmt.Printf("   - Event Count: %v\n", status["event_count"])
	fmt.Printf("   - Check Interval: %v\n", status["check_interval"])
	fmt.Printf("   - Failover Timeout: %v\n", status["failover_timeout"])

	// 模拟运行一段时间
	time.Sleep(2 * time.Second)

	// 演示手动故障转移
	fmt.Println("\n🔧 Demonstrating manual failover...")
	err = service.ManualFailover("broker-1", "broker-2", "Demo manual failover")
	if err != nil {
		log.Printf("Manual failover failed: %v", err)
	} else {
		fmt.Println("✅ Manual failover initiated")
	}

	// 等待一段时间让故障转移完成
	time.Sleep(1 * time.Second)

	// 获取故障转移历史
	history := service.GetFailoverHistory(5)
	fmt.Printf("📜 Recent failover events: %d\n", len(history))
	for i, event := range history {
		fmt.Printf("   %d. Broker: %s, Type: %d, Status: %d\n", 
			i+1, event.BrokerName, event.EventType, event.Status)
	}

	// 演示手动恢复
	fmt.Println("\n🔄 Demonstrating manual recovery...")
	err = service.ManualRecovery("broker-1", "Demo manual recovery")
	if err != nil {
		log.Printf("Manual recovery failed: %v", err)
	} else {
		fmt.Println("✅ Manual recovery initiated")
	}

	// 停止服务
	service.Stop()
	fmt.Println("✅ Failover service stopped")
}

// MockCommitLog 模拟的 CommitLog 实现
type MockCommitLog struct {
	data   []byte
	offset int64
	mutex  sync.Mutex
}

func (m *MockCommitLog) GetMaxOffset() int64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.offset
}

func (m *MockCommitLog) GetData(offset int64, size int32) ([]byte, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	if offset >= m.offset {
		return nil, fmt.Errorf("offset %d exceeds max offset %d", offset, m.offset)
	}
	
	start := offset
	end := offset + int64(size)
	if end > m.offset {
		end = m.offset
	}
	
	if start >= int64(len(m.data)) {
		return nil, nil
	}
	
	if end > int64(len(m.data)) {
		end = int64(len(m.data))
	}
	
	return m.data[start:end], nil
}

func (m *MockCommitLog) AppendData(data []byte) (int64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.data = append(m.data, data...)
	currentOffset := m.offset
	m.offset += int64(len(data))
	return currentOffset, nil
}

// demoHAService 演示高可用服务的基本使用
func demoHAService() {
	fmt.Println("\n2. HA Service Demo")
	fmt.Println("-------------------")

	// 创建模拟的 CommitLog
	mockLog := &MockCommitLog{
		data:   make([]byte, 0),
		offset: 0,
	}

	// 创建主节点配置
	masterConfig := &ha.HAConfig{
		BrokerRole:          ha.ASYNC_MASTER,
		ReplicationMode:     ha.ASYNC_REPLICATION,
		HaListenPort:        10912,
		MaxTransferSize:     65536,
		HaHeartbeatInterval: 5000, // 5秒
		HaConnectionTimeout: 30000, // 30秒
		SyncFlushTimeout:    5000,  // 5秒
	}

	// 创建HA服务
	haService := ha.NewHAService(masterConfig, mockLog)
	if haService == nil {
		log.Fatal("Failed to create HA service")
	}

	fmt.Println("✅ HA service created successfully")

	// 启动服务
	err := haService.Start()
	if err != nil {
		log.Fatalf("Failed to start HA service: %v", err)
	}
	fmt.Println("✅ HA service started as ASYNC_MASTER")

	// 获取复制状态
	status := haService.GetReplicationStatus()
	fmt.Printf("📊 HA Replication Status:\n")
	for key, value := range status {
		fmt.Printf("   - %s: %v\n", key, value)
	}

	// 模拟数据写入
	fmt.Println("\n📝 Simulating data writes...")
	for i := 0; i < 5; i++ {
		testData := fmt.Sprintf("Message %d - %s", i+1, time.Now().Format("15:04:05"))
		offset, err := mockLog.AppendData([]byte(testData))
		if err != nil {
			log.Printf("Failed to append data: %v", err)
			continue
		}
		fmt.Printf("✅ Data written at offset %d: %s\n", offset, testData)
		time.Sleep(500 * time.Millisecond)
	}

	// 获取最新状态
	fmt.Println("\n📊 Updated HA Status:")
	status = haService.GetReplicationStatus()
	for key, value := range status {
		fmt.Printf("   - %s: %v\n", key, value)
	}

	// 演示等待从节点确认（在实际环境中会有从节点）
	fmt.Println("\n⏳ Demonstrating slave ack wait (will timeout in demo)...")
	maxOffset := mockLog.GetMaxOffset()
	err = haService.WaitForSlaveAck(maxOffset, 2*time.Second)
	if err != nil {
		fmt.Printf("⚠️  Slave ack timeout (expected in demo): %v\n", err)
	} else {
		fmt.Println("✅ Slave acknowledged")
	}

	// 模拟运行一段时间
	time.Sleep(2 * time.Second)

	// 关闭服务
	haService.Shutdown()
	fmt.Println("✅ HA service shutdown")

	// 演示默认配置
	fmt.Println("\n⚙️  Default HA Configuration:")
	defaultConfig := ha.DefaultHAConfig()
	fmt.Printf("   - Broker Role: %d\n", defaultConfig.BrokerRole)
	fmt.Printf("   - Replication Mode: %d\n", defaultConfig.ReplicationMode)
	fmt.Printf("   - Listen Port: %d\n", defaultConfig.HaListenPort)
	fmt.Printf("   - Max Transfer Size: %d\n", defaultConfig.MaxTransferSize)
	fmt.Printf("   - Heartbeat Interval: %d ms\n", defaultConfig.HaHeartbeatInterval)
	fmt.Printf("   - Connection Timeout: %d ms\n", defaultConfig.HaConnectionTimeout)
}