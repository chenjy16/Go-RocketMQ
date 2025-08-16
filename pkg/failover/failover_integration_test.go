package failover

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go-rocketmq/pkg/cluster"
)

// TestFailoverIntegration 测试故障转移机制的完整集成功能
func TestFailoverIntegration(t *testing.T) {
	t.Run("AutoFailoverWithDataSync", testAutoFailoverWithDataSync)
	t.Run("MasterSlaveSwitch", testMasterSlaveSwitch)
	t.Run("FailoverRecovery", testFailoverRecovery)
	t.Run("DataSyncDuringFailover", testDataSyncDuringFailover)
}

// testAutoFailoverWithDataSync 测试自动故障转移与数据同步
func testAutoFailoverWithDataSync(t *testing.T) {
	// 创建集群管理器
	cm := cluster.NewClusterManager("test-cluster")
	if cm == nil {
		t.Fatal("Failed to create cluster manager")
	}

	// 注册主从Broker
	masterBroker := &cluster.BrokerInfo{
		BrokerId:   0,
		BrokerName: "test-broker",
		BrokerAddr: "127.0.0.1:10911",
		Role:       "SYNC_MASTER",
		Status:     cluster.ONLINE,
	}
	slaveBroker := &cluster.BrokerInfo{
		BrokerId:   1,
		BrokerName: "test-broker",
		BrokerAddr: "127.0.0.1:10912",
		Role:       "SLAVE",
		Status:     cluster.ONLINE,
	}

	err := cm.RegisterBroker(masterBroker)
	if err != nil {
		t.Fatalf("Failed to register master broker: %v", err)
	}
	err = cm.RegisterBroker(slaveBroker)
	if err != nil {
		t.Fatalf("Failed to register slave broker: %v", err)
	}

	// 创建故障转移服务
	fs := NewFailoverService(cm)
	if fs == nil {
		t.Fatal("Failed to create failover service")
	}

	// 启动故障转移服务
	err = fs.Start()
	if err != nil {
		t.Fatalf("Failed to start failover service: %v", err)
	}
	defer fs.Stop()

	// 模拟主Broker故障
	masterBroker.Status = cluster.OFFLINE

	// 等待自动故障转移
	time.Sleep(2 * time.Second)

	// 验证在线Broker
	onlineBrokers := cm.GetOnlineBrokers()
	if len(onlineBrokers) == 0 {
		t.Error("Expected at least one online broker")
	}

	// 验证数据同步服务状态
	if fs.dataSyncService == nil {
		t.Error("dataSyncService should not be nil")
	}
	fs.dataSyncService.mutex.RLock()
	running := fs.dataSyncService.running
	fs.dataSyncService.mutex.RUnlock()
	if !running {
		t.Error("dataSyncService should be running")
	}
}

// testMasterSlaveSwitch 测试主从切换功能
func testMasterSlaveSwitch(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster-2")
	if cm == nil {
		t.Fatal("Failed to create cluster manager")
	}

	// 注册主从Broker
	masterBroker := &cluster.BrokerInfo{
		BrokerId:   0,
		BrokerName: "test-broker-2",
		BrokerAddr: "127.0.0.1:10921",
		Role:       "SYNC_MASTER",
		Status:     cluster.ONLINE,
	}
	slaveBroker := &cluster.BrokerInfo{
		BrokerId:   1,
		BrokerName: "test-broker-2",
		BrokerAddr: "127.0.0.1:10922",
		Role:       "SLAVE",
		Status:     cluster.ONLINE,
	}

	err := cm.RegisterBroker(masterBroker)
	if err != nil {
		t.Fatalf("Failed to register master broker: %v", err)
	}
	err = cm.RegisterBroker(slaveBroker)
	if err != nil {
		t.Fatalf("Failed to register slave broker: %v", err)
	}

	fs := NewFailoverService(cm)
	if fs == nil {
		t.Fatal("Failed to create failover service")
	}

	err = fs.Start()
	if err != nil {
		t.Fatalf("Failed to start failover service: %v", err)
	}
	defer fs.Stop()

	// 注册故障转移策略
	policy := &FailoverPolicy{
		BrokerName:    "test-broker-2",
		FailoverType:  MASTER_SLAVE_SWITCH,
		BackupBrokers: []string{"test-broker-2"},
		AutoFailover:  true,
	}
	err = fs.RegisterFailoverPolicy(policy)
	if err != nil {
		t.Errorf("Failed to register failover policy: %v", err)
	}

	// 执行主从切换
	event := &FailoverEvent{
		BrokerName: "test-broker-2",
		EventType:  FAILOVER_START,
	}
	err = fs.executeFailover(policy, event)
	if err != nil {
		t.Errorf("executeFailover failed: %v", err)
	}

	// 验证切换结果
	onlineBrokers := cm.GetOnlineBrokers()
	if len(onlineBrokers) == 0 {
		t.Error("Expected online brokers")
	}

	// 手动创建同步任务来验证数据同步功能
	if fs.dataSyncService == nil {
		t.Error("dataSyncService should not be nil")
	} else {
		task, err := fs.dataSyncService.CreateSyncTask("test-broker-2", "test-broker-2", INCREMENTAL_SYNC)
		if err != nil {
			t.Errorf("Failed to create sync task: %v", err)
		} else {
			// 启动同步任务
			err = fs.dataSyncService.StartSyncTask(task.TaskId)
			if err != nil {
				t.Errorf("Failed to start sync task: %v", err)
			}
			// 等待同步完成
			time.Sleep(200 * time.Millisecond)
			// 验证任务状态
			fs.dataSyncService.mutex.RLock()
			taskStatus := fs.dataSyncService.syncTasks[task.TaskId].Status
			fs.dataSyncService.mutex.RUnlock()
			if taskStatus == SYNC_FAILED {
				t.Error("Sync task failed")
			}
		}
	}
}

// testFailoverRecovery 测试故障恢复功能
func testFailoverRecovery(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster-3")
	if cm == nil {
		t.Fatal("Failed to create cluster manager")
	}

	// 注册Broker
	broker := &cluster.BrokerInfo{
		BrokerId:   0,
		BrokerName: "test-broker-3",
		BrokerAddr: "127.0.0.1:10931",
		Role:       "SYNC_MASTER",
		Status:     cluster.OFFLINE, // 初始状态为离线
	}

	err := cm.RegisterBroker(broker)
	if err != nil {
		t.Fatalf("Failed to register broker: %v", err)
	}

	fs := NewFailoverService(cm)
	if fs == nil {
		t.Fatal("Failed to create failover service")
	}

	err = fs.Start()
	if err != nil {
		t.Fatalf("Failed to start failover service: %v", err)
	}
	defer fs.Stop()

	// 模拟Broker恢复在线
	broker.Status = cluster.ONLINE

	// 等待恢复检测
	time.Sleep(1 * time.Second)

	// 验证恢复状态
	onlineBrokers := cm.GetOnlineBrokers()
	if len(onlineBrokers) == 0 {
		t.Error("Expected at least one online broker")
	}
}

// testDataSyncDuringFailover 测试故障转移期间的数据同步
func testDataSyncDuringFailover(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster-4")
	if cm == nil {
		t.Fatal("Failed to create cluster manager")
	}

	// 注册多个Broker
	brokers := []*cluster.BrokerInfo{
		{
			BrokerId:   0,
			BrokerName: "test-broker-4",
			BrokerAddr: "127.0.0.1:10941",
			Role:       "SYNC_MASTER",
			Status:     cluster.ONLINE,
		},
		{
			BrokerId:   1,
			BrokerName: "test-broker-4",
			BrokerAddr: "127.0.0.1:10942",
			Role:       "SLAVE",
			Status:     cluster.ONLINE,
		},
		{
			BrokerId:   2,
			BrokerName: "test-broker-4",
			BrokerAddr: "127.0.0.1:10943",
			Role:       "SLAVE",
			Status:     cluster.ONLINE,
		},
	}

	for _, broker := range brokers {
		err := cm.RegisterBroker(broker)
		if err != nil {
			t.Fatalf("Failed to register broker: %v", err)
		}
	}

	fs := NewFailoverService(cm)
	if fs == nil {
		t.Fatal("Failed to create failover service")
	}

	err := fs.Start()
	if err != nil {
		t.Fatalf("Failed to start failover service: %v", err)
	}
	defer fs.Stop()

	// 创建数据同步任务
	task, err := fs.dataSyncService.CreateSyncTask("test-broker-4", "test-broker-4", FULL_SYNC)
	if err != nil {
		t.Fatalf("Failed to create sync task: %v", err)
	}
	if task == nil {
		t.Fatal("Sync task should not be nil")
	}

	// 启动同步任务
	err = fs.dataSyncService.StartSyncTask(task.TaskId)
	if err != nil {
		t.Errorf("Failed to start sync task: %v", err)
	}

	// 等待同步完成
	time.Sleep(1 * time.Second)

	// 验证同步状态
	fs.dataSyncService.mutex.RLock()
	taskStatus := fs.dataSyncService.syncTasks[task.TaskId].Status
	fs.dataSyncService.mutex.RUnlock()

	if taskStatus != SYNC_RUNNING && taskStatus != SYNC_SUCCESS {
		t.Errorf("Expected task to be running or successful, got: %v", taskStatus)
	}
}

// TestDataSyncService 测试数据同步服务
func TestDataSyncService(t *testing.T) {
	t.Run("CreateAndStartSyncTask", testCreateAndStartSyncTask)
	t.Run("SyncTaskLifecycle", testSyncTaskLifecycle)
	t.Run("ConcurrentSyncTasks", testConcurrentSyncTasks)
}

// testCreateAndStartSyncTask 测试创建和启动同步任务
func testCreateAndStartSyncTask(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster-sync")
	ds := NewDataSyncService(cm)
	if ds == nil {
		t.Fatal("Failed to create data sync service")
	}

	err := ds.Start()
	if err != nil {
		t.Fatalf("Failed to start data sync service: %v", err)
	}
	defer ds.Stop()

	// 注册测试Broker
	cm.RegisterBroker(&cluster.BrokerInfo{
		BrokerId:   0,
		BrokerName: "master-broker",
		BrokerAddr: "127.0.0.1:10911",
		Role:       "SYNC_MASTER",
		Status:     cluster.ONLINE,
	})
	cm.RegisterBroker(&cluster.BrokerInfo{
		BrokerId:   1,
		BrokerName: "slave-broker",
		BrokerAddr: "127.0.0.1:10912",
		Role:       "SLAVE",
		Status:     cluster.ONLINE,
	})

	// 创建同步任务
	task, err := ds.CreateSyncTask("master-broker", "slave-broker", INCREMENTAL_SYNC)
	if err != nil {
		t.Fatalf("Failed to create sync task: %v", err)
	}
	if task == nil {
		t.Fatal("Task should not be nil")
	}
	if task.MasterBroker != "master-broker" {
		t.Errorf("Expected master broker 'master-broker', got '%s'", task.MasterBroker)
	}
	if task.SlaveBroker != "slave-broker" {
		t.Errorf("Expected slave broker 'slave-broker', got '%s'", task.SlaveBroker)
	}
	if task.SyncType != INCREMENTAL_SYNC {
		t.Errorf("Expected sync type INCREMENTAL_SYNC, got %v", task.SyncType)
	}
	if task.Status != SYNC_PENDING {
		t.Errorf("Expected status SYNC_PENDING, got %v", task.Status)
	}

	// 启动同步任务
	err = ds.StartSyncTask(task.TaskId)
	if err != nil {
		t.Errorf("Failed to start sync task: %v", err)
	}

	// 验证任务状态
	ds.mutex.RLock()
	taskStatus := ds.syncTasks[task.TaskId].Status
	ds.mutex.RUnlock()
	if taskStatus != SYNC_RUNNING {
		t.Errorf("Expected status SYNC_RUNNING, got %v", taskStatus)
	}
}

// testSyncTaskLifecycle 测试同步任务生命周期
func testSyncTaskLifecycle(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster-lifecycle")
	ds := NewDataSyncService(cm)
	if ds == nil {
		t.Fatal("Failed to create data sync service")
	}

	err := ds.Start()
	if err != nil {
		t.Fatalf("Failed to start data sync service: %v", err)
	}
	defer ds.Stop()

	// 注册测试Broker
	for i := 0; i < 3; i++ {
		masterName := fmt.Sprintf("master-%d", i)
		slaveName := fmt.Sprintf("slave-%d", i)
		cm.RegisterBroker(&cluster.BrokerInfo{
			BrokerId:   int64(i * 2),
			BrokerName: masterName,
			BrokerAddr: fmt.Sprintf("127.0.0.1:%d", 10911+i*2),
			Role:       "SYNC_MASTER",
			Status:     cluster.ONLINE,
		})
		cm.RegisterBroker(&cluster.BrokerInfo{
			BrokerId:   int64(i*2 + 1),
			BrokerName: slaveName,
			BrokerAddr: fmt.Sprintf("127.0.0.1:%d", 10912+i*2),
			Role:       "SLAVE",
			Status:     cluster.ONLINE,
		})
	}

	// 创建多种类型的同步任务
	syncTypes := []SyncType{FULL_SYNC, INCREMENTAL_SYNC, REAL_TIME_SYNC}
	tasks := make([]*SyncTask, len(syncTypes))

	for i, syncType := range syncTypes {
		task, err := ds.CreateSyncTask(fmt.Sprintf("master-%d", i), fmt.Sprintf("slave-%d", i), syncType)
		if err != nil {
			t.Fatalf("Failed to create sync task %d: %v", i, err)
		}
		tasks[i] = task
	}

	// 启动所有任务
	for _, task := range tasks {
		err = ds.StartSyncTask(task.TaskId)
		if err != nil {
			t.Errorf("Failed to start sync task: %v", err)
		}
	}

	// 等待任务执行
	time.Sleep(500 * time.Millisecond)

	// 验证任务状态
	ds.mutex.RLock()
	for _, task := range tasks {
		status := ds.syncTasks[task.TaskId].Status
		if status != SYNC_RUNNING && status != SYNC_SUCCESS {
			t.Errorf("Expected task to be running or successful, got: %v", status)
		}
	}
	ds.mutex.RUnlock()
}

// testConcurrentSyncTasks 测试并发同步任务
func testConcurrentSyncTasks(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster-concurrent")
	ds := NewDataSyncService(cm)
	if ds == nil {
		t.Fatal("Failed to create data sync service")
	}

	err := ds.Start()
	if err != nil {
		t.Fatalf("Failed to start data sync service: %v", err)
	}
	defer ds.Stop()

	// 注册测试Broker
	for i := 0; i < 10; i++ {
		masterName := fmt.Sprintf("master-%d", i)
		slaveName := fmt.Sprintf("slave-%d", i)
		cm.RegisterBroker(&cluster.BrokerInfo{
			BrokerId:   int64(i * 2),
			BrokerName: masterName,
			BrokerAddr: fmt.Sprintf("127.0.0.1:%d", 10911+i*2),
			Role:       "SYNC_MASTER",
			Status:     cluster.ONLINE,
		})
		cm.RegisterBroker(&cluster.BrokerInfo{
			BrokerId:   int64(i*2 + 1),
			BrokerName: slaveName,
			BrokerAddr: fmt.Sprintf("127.0.0.1:%d", 10912+i*2),
			Role:       "SLAVE",
			Status:     cluster.ONLINE,
		})
	}

	// 并发创建和启动多个同步任务
	var wg sync.WaitGroup
	taskCount := 10
	tasks := make([]*SyncTask, taskCount)

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			task, err := ds.CreateSyncTask(fmt.Sprintf("master-%d", index), fmt.Sprintf("slave-%d", index), INCREMENTAL_SYNC)
			if err != nil {
				t.Errorf("Failed to create sync task %d: %v", index, err)
				return
			}
			tasks[index] = task
			if task != nil {
				ds.StartSyncTask(task.TaskId)
			}
		}(i)
	}

	wg.Wait()

	// 验证所有任务都被创建
	ds.mutex.RLock()
	actualTaskCount := len(ds.syncTasks)
	ds.mutex.RUnlock()

	if actualTaskCount != taskCount {
		t.Errorf("Expected %d tasks, got %d", taskCount, actualTaskCount)
	}

	// 验证任务状态
	time.Sleep(200 * time.Millisecond)
	ds.mutex.RLock()
	for _, task := range tasks {
		if task != nil {
			status := ds.syncTasks[task.TaskId].Status
			if status != SYNC_RUNNING && status != SYNC_SUCCESS {
				t.Errorf("Expected task to be running or successful, got: %v", status)
			}
		}
	}
	ds.mutex.RUnlock()
}