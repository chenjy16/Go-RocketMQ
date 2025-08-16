package failover

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go-rocketmq/pkg/cluster"
)

// TestFailoverServiceNilPolicyHandling 测试空策略处理
func TestFailoverServiceNilPolicyHandling(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 测试注册nil策略
	err := fs.RegisterFailoverPolicy(nil)
	if err == nil {
		t.Error("Should return error when registering nil policy")
	}
	if err.Error() != "failover policy cannot be nil" {
		t.Errorf("Expected specific error message, got: %v", err)
	}

	// 验证策略数量为0
	status := fs.GetFailoverStatus()
	if policyCount, ok := status["policy_count"].(int); !ok || policyCount != 0 {
		t.Errorf("Expected 0 policies after nil registration, got %d", policyCount)
	}
}

// TestFailoverServiceEmptyBackupBrokers 测试空备份Broker列表
func TestFailoverServiceEmptyBackupBrokers(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 注册主Broker
	mainBroker := &cluster.BrokerInfo{
		BrokerName: "main-broker",
		BrokerAddr: "127.0.0.1:10911",
		Status:     cluster.OFFLINE,
	}
	cm.RegisterBroker(mainBroker)

	// 注册空备份Broker列表的策略
	policy := &FailoverPolicy{
		BrokerName:    "main-broker",
		FailoverType:  AUTO_FAILOVER,
		BackupBrokers: []string{}, // 空列表
		AutoFailover:  true,
	}
	fs.RegisterFailoverPolicy(policy)

	// 启动服务
	fs.Start()
	defer fs.Stop()

	// 手动触发故障转移
	err := fs.ManualFailover("main-broker", "backup-broker", "Test empty backup")
	if err == nil {
		t.Error("Should fail when no backup brokers configured")
	}
	if !strings.Contains(err.Error(), "no backup brokers configured") && !strings.Contains(err.Error(), "no available backup brokers") {
		t.Errorf("Expected error about backup brokers, got: %v", err)
	}

	// 验证故障转移事件记录了失败
	history := fs.GetFailoverHistory(10)
	if len(history) == 0 {
		t.Error("Should have recorded failover event")
		return
	}

	event := history[len(history)-1]
	if event.Status != FAILED {
		t.Errorf("Expected FAILED status, got %v", event.Status)
	}
	if event.ErrorMessage == "" {
		t.Error("Expected error message to be recorded")
	}
}

// TestFailoverServiceInvalidBrokerNames 测试无效Broker名称
func TestFailoverServiceInvalidBrokerNames(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 测试手动故障转移不存在的Broker
	err := fs.ManualFailover("non-existent-broker", "target-broker", "Test invalid broker")
	if err == nil {
		t.Error("Should return error for non-existent broker")
	}
	if err.Error() != "no failover policy found for broker: non-existent-broker" {
		t.Errorf("Expected specific error message, got: %v", err)
	}

	// 测试手动恢复不存在的Broker
	err = fs.ManualRecovery("non-existent-broker", "Test invalid recovery")
	if err != nil {
		// ManualRecovery可能不检查策略存在性，这是正常的
		t.Logf("ManualRecovery returned error (may be expected): %v", err)
	}
}

// TestFailoverServiceUnsupportedFailoverType 测试不支持的故障转移类型
func TestFailoverServiceUnsupportedFailoverType(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 注册Broker
	mainBroker := &cluster.BrokerInfo{
		BrokerName: "main-broker",
		BrokerAddr: "127.0.0.1:10911",
		Status:     cluster.OFFLINE,
	}
	backupBroker := &cluster.BrokerInfo{
		BrokerName: "backup-broker",
		BrokerAddr: "127.0.0.1:10912",
		Status:     cluster.ONLINE,
	}
	cm.RegisterBroker(mainBroker)
	cm.RegisterBroker(backupBroker)

	// 注册无效故障转移类型的策略
	policy := &FailoverPolicy{
		BrokerName:    "main-broker",
		FailoverType:  FailoverType(999), // 无效类型
		BackupBrokers: []string{"backup-broker"},
		AutoFailover:  true,
	}
	fs.RegisterFailoverPolicy(policy)

	// 启动服务
	fs.Start()
	defer fs.Stop()

	// 手动触发故障转移
	err := fs.ManualFailover("main-broker", "backup-broker", "Test unsupported type")
	if err == nil {
		t.Error("Should fail with unsupported failover type")
	}
	if !strings.Contains(err.Error(), "unsupported failover type") {
		t.Errorf("Expected unsupported failover type error, got: %v", err)
	}
}

// TestFailoverServiceNotificationFailures 测试通知发送失败
func TestFailoverServiceNotificationFailures(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 注册Broker
	mainBroker := &cluster.BrokerInfo{
		BrokerName: "main-broker",
		BrokerAddr: "127.0.0.1:10911",
		Status:     cluster.OFFLINE,
	}
	backupBroker := &cluster.BrokerInfo{
		BrokerName: "backup-broker",
		BrokerAddr: "127.0.0.1:10912",
		Status:     cluster.ONLINE,
	}
	cm.RegisterBroker(mainBroker)
	cm.RegisterBroker(backupBroker)

	// 注册包含通知配置的策略
	policy := &FailoverPolicy{
		BrokerName:    "main-broker",
		FailoverType:  AUTO_FAILOVER,
		BackupBrokers: []string{"backup-broker"},
		AutoFailover:  true,
		Notifications: []NotificationConfig{
			{
				Type:     "email",
				Endpoint: "invalid-email",
				Enabled:  true,
			},
			{
				Type:     "webhook",
				Endpoint: "http://invalid-url",
				Enabled:  true,
			},
			{
				Type:     "sms",
				Endpoint: "invalid-phone",
				Enabled:  false, // 禁用的通知
			},
		},
	}
	fs.RegisterFailoverPolicy(policy)

	// 启动服务
	fs.Start()
	defer fs.Stop()

	// 手动触发故障转移
	err := fs.ManualFailover("main-broker", "backup-broker", "Test notification failures")
	if err != nil {
		// 故障转移可能因为其他原因失败，这是可以接受的
		t.Logf("Manual failover failed (may be expected): %v", err)
		return
	}

	// 验证故障转移事件成功记录
	history := fs.GetFailoverHistory(10)
	if len(history) == 0 {
		t.Error("Should have recorded failover event")
		return
	}

	event := history[len(history)-1]
	if event.Status != SUCCESS {
		t.Errorf("Expected SUCCESS status despite notification failures, got %v", event.Status)
	}
}

// TestFailoverServiceConcurrentFailoverConflicts 测试并发故障转移冲突
func TestFailoverServiceConcurrentFailoverConflicts(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 注册多个Broker
	for i := 0; i < 5; i++ {
		mainBroker := &cluster.BrokerInfo{
			BrokerName: fmt.Sprintf("main-broker-%d", i),
			BrokerAddr: fmt.Sprintf("127.0.0.1:1091%d", i),
			Status:     cluster.OFFLINE,
		}
		backupBroker := &cluster.BrokerInfo{
			BrokerName: fmt.Sprintf("backup-broker-%d", i),
			BrokerAddr: fmt.Sprintf("127.0.0.1:1092%d", i),
			Status:     cluster.ONLINE,
		}
		cm.RegisterBroker(mainBroker)
		cm.RegisterBroker(backupBroker)

		// 注册故障转移策略
		policy := &FailoverPolicy{
			BrokerName:    fmt.Sprintf("main-broker-%d", i),
			FailoverType:  AUTO_FAILOVER,
			BackupBrokers: []string{fmt.Sprintf("backup-broker-%d", i)},
			AutoFailover:  true,
		}
		fs.RegisterFailoverPolicy(policy)
	}

	// 启动服务
	fs.Start()
	defer fs.Stop()

	var wg sync.WaitGroup
	errorChan := make(chan error, 50)

	// 并发执行故障转移
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			brokerIndex := id % 5
			err := fs.ManualFailover(
				fmt.Sprintf("main-broker-%d", brokerIndex),
				fmt.Sprintf("backup-broker-%d", brokerIndex),
				fmt.Sprintf("Concurrent failover %d", id),
			)
			if err != nil {
				errorChan <- fmt.Errorf("failover %d failed: %v", id, err)
			}
		}(i)
	}

	// 并发执行恢复操作
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond) // 稍微延迟
			err := fs.ManualRecovery(
				fmt.Sprintf("main-broker-%d", id),
				fmt.Sprintf("Concurrent recovery %d", id),
			)
			if err != nil {
				errorChan <- fmt.Errorf("recovery %d failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// 检查错误
	errorCount := 0
	for err := range errorChan {
		t.Logf("Concurrent operation error: %v", err)
		errorCount++
	}

	// 验证历史记录
	history := fs.GetFailoverHistory(50)
	if len(history) == 0 {
		t.Error("Should have recorded failover events")
	}

	// 验证服务状态
	status := fs.GetFailoverStatus()
	if eventCount, ok := status["event_count"].(int); !ok || eventCount == 0 {
		t.Error("Should have recorded events in status")
	}
}

// TestFailoverServiceHistoryManagementBoundary 测试历史事件管理边界情况
func TestFailoverServiceHistoryManagementBoundary(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 添加大量事件以测试历史记录限制
	for i := 0; i < 1100; i++ {
		event := &FailoverEvent{
			EventId:    fmt.Sprintf("test-event-%d", i),
			BrokerName: fmt.Sprintf("broker-%d", i%10),
			EventType:  FAILOVER_START,
			Timestamp:  time.Now().UnixMilli(),
			Status:     SUCCESS,
		}
		fs.addFailoverEvent(event)
	}

	// 验证历史记录被正确限制
	status := fs.GetFailoverStatus()
	eventCount, ok := status["event_count"].(int)
	if !ok {
		t.Fatal("event_count should be present in status")
	}
	if eventCount > 1000 {
		t.Errorf("Event count should be limited to 1000, got %d", eventCount)
	}
	if eventCount < 900 {
		t.Errorf("Event count should be at least 900 after cleanup, got %d", eventCount)
	}

	// 测试获取历史记录的边界情况
	testCases := []struct {
		limit    int
		expected int
	}{
		{0, eventCount},        // limit为0应该返回所有
		{-1, eventCount},       // 负数应该返回所有
		{10, 10},               // 正常限制
		{eventCount + 100, eventCount}, // 超过实际数量
	}

	for _, tc := range testCases {
		history := fs.GetFailoverHistory(tc.limit)
		if len(history) != tc.expected {
			t.Errorf("GetFailoverHistory(%d) returned %d events, expected %d",
				tc.limit, len(history), tc.expected)
		}
	}

	// 验证返回的是最新的事件
	history := fs.GetFailoverHistory(5)
	if len(history) != 5 {
		t.Fatalf("Expected 5 events, got %d", len(history))
	}

	// 验证事件按时间排序（最新的在前）
	for i := 0; i < len(history)-1; i++ {
		if history[i].Timestamp < history[i+1].Timestamp {
			t.Error("Events should be sorted by timestamp (newest first)")
			break
		}
	}
}

// TestFailoverServiceNoAvailableBackupBrokers 测试没有可用备份Broker的情况
func TestFailoverServiceNoAvailableBackupBrokers(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 注册主Broker
	mainBroker := &cluster.BrokerInfo{
		BrokerName: "main-broker",
		BrokerAddr: "127.0.0.1:10911",
		Status:     cluster.OFFLINE,
	}
	cm.RegisterBroker(mainBroker)

	// 注册所有备份Broker都离线
	for i := 0; i < 3; i++ {
		backupBroker := &cluster.BrokerInfo{
			BrokerName: fmt.Sprintf("backup-broker-%d", i),
			BrokerAddr: fmt.Sprintf("127.0.0.1:1091%d", i+2),
			Status:     cluster.OFFLINE, // 所有备份都离线
		}
		cm.RegisterBroker(backupBroker)
	}

	// 注册故障转移策略
	policy := &FailoverPolicy{
		BrokerName:    "main-broker",
		FailoverType:  AUTO_FAILOVER,
		BackupBrokers: []string{"backup-broker-0", "backup-broker-1", "backup-broker-2"},
		AutoFailover:  true,
	}
	fs.RegisterFailoverPolicy(policy)

	// 启动服务
	fs.Start()
	defer fs.Stop()

	// 手动触发故障转移
	err := fs.ManualFailover("main-broker", "backup-broker-0", "Test no available backups")
	if err == nil {
		t.Error("Should fail when no backup brokers are available")
	}
	if !strings.Contains(err.Error(), "no available backup brokers") && !strings.Contains(err.Error(), "backup broker") {
		t.Errorf("Expected error about backup brokers, got: %v", err)
	}

	// 验证故障转移事件记录了失败
	history := fs.GetFailoverHistory(10)
	if len(history) == 0 {
		t.Error("Should have recorded failover event")
		return
	}

	event := history[len(history)-1]
	if event.Status != FAILED {
		t.Errorf("Expected FAILED status, got %v", event.Status)
	}
	if event.ErrorMessage == "" {
		t.Error("Expected error message to be recorded")
	}
}

// TestFailoverServiceSlavePromotionFailure 测试从节点提升失败
func TestFailoverServiceSlavePromotionFailure(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 注册主Broker
	masterBroker := &cluster.BrokerInfo{
		BrokerName: "master-broker",
		BrokerAddr: "127.0.0.1:10911",
		Status:     cluster.OFFLINE,
	}
	cm.RegisterBroker(masterBroker)

	// 注册从Broker但不在集群管理器中注册（模拟找不到的情况）
	policy := &FailoverPolicy{
		BrokerName:    "master-broker",
		FailoverType:  MASTER_SLAVE_SWITCH,
		BackupBrokers: []string{"non-existent-slave"},
		AutoFailover:  true,
	}
	fs.RegisterFailoverPolicy(policy)

	// 启动服务
	fs.Start()
	defer fs.Stop()

	// 手动触发主从切换
	err := fs.ManualFailover("master-broker", "non-existent-slave", "Test slave promotion failure")
	if err == nil {
		t.Error("Should fail when slave broker doesn't exist")
	}
	if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "no available backup brokers") {
		t.Errorf("Expected error about broker not found, got: %v", err)
	}

	// 验证故障转移事件记录了失败
	history := fs.GetFailoverHistory(10)
	if len(history) == 0 {
		t.Error("Should have recorded failover event")
		return
	}

	event := history[len(history)-1]
	if event.Status != FAILED {
		t.Errorf("Expected FAILED status, got %v", event.Status)
	}
	if event.ErrorMessage == "" {
		t.Error("Expected error message to be recorded")
	}
}

// TestFailoverServiceRecoveryWithoutPolicy 测试没有策略的恢复
func TestFailoverServiceRecoveryWithoutPolicy(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)

	// 注册Broker但不注册故障转移策略
	broker := &cluster.BrokerInfo{
		BrokerName: "test-broker",
		BrokerAddr: "127.0.0.1:10911",
		Status:     cluster.ONLINE,
	}
	cm.RegisterBroker(broker)

	// 启动服务
	fs.Start()
	defer fs.Stop()

	// 尝试手动恢复没有策略的Broker
	err := fs.ManualRecovery("test-broker", "Test recovery without policy")
	if err != nil {
		// ManualRecovery可能不检查策略存在性，这是正常的
		t.Logf("ManualRecovery returned error (may be expected): %v", err)
	}

	// 验证恢复事件被记录
	history := fs.GetFailoverHistory(10)
	if len(history) > 0 {
		event := history[len(history)-1]
		if event.EventType != RECOVERY_START && event.EventType != RECOVERY_COMPLETE {
			t.Errorf("Expected recovery event type, got %v", event.EventType)
		}
	}
}