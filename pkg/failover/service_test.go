package failover

import (
	"testing"
	"time"

	"go-rocketmq/pkg/cluster"
)

func TestFailoverServiceCreation(t *testing.T) {
	// 创建集群管理器
	cm := cluster.NewClusterManager("test-cluster")
	
	// 创建故障转移服务
	fs := NewFailoverService(cm)
	
	// 验证服务创建
	if fs == nil {
		t.Fatal("FailoverService should not be nil")
	}
	if fs.running != false {
		t.Error("Service should not be running initially")
	}
	if fs.failoverPolicies == nil {
		t.Error("Failover policies map should be initialized")
	}
	if fs.failoverHistory == nil {
		t.Error("Failover history should be initialized")
	}
}

func TestFailoverServiceStartStop(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)
	
	// 启动服务
	err := fs.Start()
	if err != nil {
		t.Fatalf("Start should not return error: %v", err)
	}
	if fs.running != true {
		t.Error("Service should be running after start")
	}
	
	// 停止服务
	fs.Stop()
	if fs.running != false {
		t.Error("Service should not be running after stop")
	}
}

func TestFailoverPolicyRegistration(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)
	
	// 创建故障转移策略
	policy := &FailoverPolicy{
		BrokerName:      "test-broker",
		FailoverType:    AUTO_FAILOVER,
		BackupBrokers:   []string{"backup-broker-1", "backup-broker-2"},
		AutoFailover:    true,
		FailoverDelay:   time.Second * 5,
		HealthThreshold: 3,
		RecoveryPolicy:  AUTO_RECOVERY,
	}
	
	// 注册策略
	err := fs.RegisterFailoverPolicy(policy)
	if err != nil {
		t.Fatalf("RegisterFailoverPolicy should not return error: %v", err)
	}
	
	// 验证策略已注册
	fs.mutex.RLock()
	registeredPolicy, exists := fs.failoverPolicies["test-broker"]
	fs.mutex.RUnlock()
	
	if !exists {
		t.Error("Policy should be registered")
	}
	if registeredPolicy.BrokerName != "test-broker" {
		t.Error("Broker name should match")
	}
	if registeredPolicy.FailoverType != AUTO_FAILOVER {
		t.Error("Failover type should match")
	}
	
	// 注销策略
	fs.UnregisterFailoverPolicy("test-broker")
	
	fs.mutex.RLock()
	_, exists = fs.failoverPolicies["test-broker"]
	fs.mutex.RUnlock()
	
	if exists {
		t.Error("Policy should be unregistered")
	}
}

func TestFailoverServiceStatus(t *testing.T) {
	cm := cluster.NewClusterManager("test-cluster")
	fs := NewFailoverService(cm)
	
	// 获取状态
	status := fs.GetFailoverStatus()
	
	// 验证状态信息
	if status == nil {
		t.Fatal("Status should not be nil")
	}
	if status["running"] != false {
		t.Error("Service should not be running")
	}
	if status["policy_count"] != 0 {
		t.Error("Should have no active policies")
	}
	if status["event_count"] != 0 {
		t.Error("Should have no events")
	}
}