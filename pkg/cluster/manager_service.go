package cluster

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go-rocketmq/pkg/ha"
)

// ClusterManagerService 集群管理服务
type ClusterManagerService struct {
	config          *ClusterManagerConfig
	clusterManager  *ClusterManager
	dataReplicator  *DataReplicator
	faultDetector   *FaultDetector
	haService       *ha.HAService
	mutex           sync.RWMutex
	running         bool
	shutdown        chan struct{}
}

// ClusterManagerConfig 集群管理服务配置
type ClusterManagerConfig struct {
	ClusterName          string
	EnableDataReplication bool
	EnableFaultDetection  bool
	EnableHA             bool
	
	// 子组件配置
	ReplicationConfig   *ReplicationConfig
	FaultDetectorConfig *FaultDetectorConfig
	HAConfig           *ha.HAConfig
}

// ClusterStatus 集群状态
type ClusterStatus struct {
	ClusterName     string
	Running         bool
	TotalNodes      int
	OnlineNodes     int
	OfflineNodes    int
	SuspectNodes    int
	ActiveFaults    int
	ReplicationMode string
	HARole          string
	LastUpdateTime  time.Time
}

// NewClusterManagerService 创建集群管理服务
func NewClusterManagerService(config *ClusterManagerConfig) *ClusterManagerService {
	// 创建基础集群管理器
	clusterManager := NewClusterManager(config.ClusterName)
	
	return &ClusterManagerService{
		config:         config,
		clusterManager: clusterManager,
		shutdown:       make(chan struct{}),
	}
}

// Start 启动集群管理服务
func (cms *ClusterManagerService) Start() error {
	if cms.running {
		return fmt.Errorf("cluster manager service already running")
	}

	cms.running = true
	log.Printf("Starting cluster manager service for cluster: %s", cms.config.ClusterName)

	// 启动基础集群管理器
	if err := cms.clusterManager.Start(); err != nil {
		return fmt.Errorf("failed to start cluster manager: %v", err)
	}

	// 启动数据复制服务（如果启用）
	if cms.config.EnableDataReplication && cms.dataReplicator != nil {
		if err := cms.dataReplicator.Start(); err != nil {
			return fmt.Errorf("failed to start data replicator: %v", err)
		}
		log.Printf("Data replication service started")
	}

	// 启动故障检测服务（如果启用）
	if cms.config.EnableFaultDetection && cms.faultDetector != nil {
		if err := cms.faultDetector.Start(); err != nil {
			return fmt.Errorf("failed to start fault detector: %v", err)
		}
		log.Printf("Fault detection service started")
	}

	// 启动HA服务（如果启用）
	if cms.config.EnableHA && cms.haService != nil {
		if err := cms.haService.Start(); err != nil {
			return fmt.Errorf("failed to start HA service: %v", err)
		}
		log.Printf("HA service started")
	}

	// 启动状态监控
	go cms.monitorClusterStatus()

	log.Printf("Cluster manager service started successfully")
	return nil
}

// Stop 停止集群管理服务
func (cms *ClusterManagerService) Stop() {
	if !cms.running {
		return
	}

	cms.running = false
	close(cms.shutdown)
	
	// 停止各子服务
	if cms.faultDetector != nil {
		cms.faultDetector.Stop()
	}
	
	if cms.dataReplicator != nil {
		cms.dataReplicator.Stop()
	}
	
	if cms.haService != nil {
		cms.haService.Shutdown()
	}
	
	cms.clusterManager.Stop()

	log.Printf("Cluster manager service stopped")
}

// InitializeComponents 初始化组件
func (cms *ClusterManagerService) InitializeComponents(commitLog ha.CommitLogInterface) error {
	// 初始化数据复制器
	if cms.config.EnableDataReplication && cms.config.ReplicationConfig != nil {
		cms.dataReplicator = NewDataReplicator(
			cms.config.ReplicationConfig,
			cms.clusterManager,
			commitLog,
		)
		log.Printf("Data replicator initialized")
	}

	// 初始化故障检测器
	if cms.config.EnableFaultDetection && cms.config.FaultDetectorConfig != nil {
		cms.faultDetector = NewFaultDetector(
			cms.config.FaultDetectorConfig,
			cms.clusterManager,
			cms.haService,
		)
		log.Printf("Fault detector initialized")
	}

	return nil
}

// SetHAService 设置HA服务
func (cms *ClusterManagerService) SetHAService(haService *ha.HAService) {
	cms.haService = haService
}

// monitorClusterStatus 监控集群状态
func (cms *ClusterManagerService) monitorClusterStatus() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cms.shutdown:
			return
		case <-ticker.C:
			cms.updateClusterStatus()
		}
	}
}

// updateClusterStatus 更新集群状态
func (cms *ClusterManagerService) updateClusterStatus() {
	status := cms.GetClusterStatus()
	log.Printf("Cluster Status - Name: %s, Nodes: %d/%d online, Active Faults: %d",
		status.ClusterName,
		status.OnlineNodes,
		status.TotalNodes,
		status.ActiveFaults)
}

// GetClusterStatus 获取集群状态
func (cms *ClusterManagerService) GetClusterStatus() *ClusterStatus {
	clusterStats := cms.clusterManager.GetClusterStatus()
	
	status := &ClusterStatus{
		ClusterName:    cms.config.ClusterName,
		Running:        cms.running,
		TotalNodes:     clusterStats["totalBrokers"].(int),
		OnlineNodes:    clusterStats["onlineBrokers"].(int),
		OfflineNodes:   clusterStats["offlineBrokers"].(int),
		SuspectNodes:   clusterStats["suspectBrokers"].(int),
		LastUpdateTime: time.Now(),
	}

	// 获取活跃故障数
	if cms.faultDetector != nil {
		activeFaults := cms.faultDetector.GetActiveFaults()
		status.ActiveFaults = len(activeFaults)
	}

	// 设置复制模式
	if cms.dataReplicator != nil {
		switch cms.config.ReplicationConfig.Mode {
		case ASYNC_REPLICATION:
			status.ReplicationMode = "ASYNC"
		case SYNC_REPLICATION:
			status.ReplicationMode = "SYNC"
		default:
			status.ReplicationMode = "UNKNOWN"
		}
	}

	// 设置HA角色
	if cms.haService != nil {
		haStatus := cms.haService.GetReplicationStatus()
		if role, ok := haStatus["role"]; ok {
			status.HARole = fmt.Sprintf("%v", role)
		}
	}

	return status
}

// GetDetailedClusterInfo 获取详细集群信息
func (cms *ClusterManagerService) GetDetailedClusterInfo() map[string]interface{} {
	info := make(map[string]interface{})
	
	// 基础集群信息
	info["cluster"] = cms.clusterManager.GetClusterStatus()
	
	// Broker信息
	info["brokers"] = cms.clusterManager.GetAllBrokers()
	
	// 复制状态
	if cms.dataReplicator != nil {
		info["replication"] = cms.dataReplicator.GetReplicationStatus()
	}
	
	// HA状态
	if cms.haService != nil {
		info["ha"] = cms.haService.GetReplicationStatus()
	}
	
	// 故障信息
	if cms.faultDetector != nil {
		info["faults"] = cms.faultDetector.GetFaultEvents()
		activeFaults := cms.faultDetector.GetActiveFaults()
		info["activeFaults"] = len(activeFaults)
	}
	
	return info
}

// RegisterBroker 注册Broker
func (cms *ClusterManagerService) RegisterBroker(brokerInfo *BrokerInfo) error {
	return cms.clusterManager.RegisterBroker(brokerInfo)
}

// UnregisterBroker 注销Broker
func (cms *ClusterManagerService) UnregisterBroker(brokerName string) {
	cms.clusterManager.UnregisterBroker(brokerName)
}

// UpdateBrokerMetrics 更新Broker指标
func (cms *ClusterManagerService) UpdateBrokerMetrics(brokerName string, metrics *BrokerMetrics) error {
	return cms.clusterManager.UpdateBrokerMetrics(brokerName, metrics)
}

// GetBroker 获取Broker信息
func (cms *ClusterManagerService) GetBroker(brokerName string) (*BrokerInfo, bool) {
	return cms.clusterManager.GetBroker(brokerName)
}

// GetAllBrokers 获取所有Broker
func (cms *ClusterManagerService) GetAllBrokers() map[string]*BrokerInfo {
	return cms.clusterManager.GetAllBrokers()
}

// GetOnlineBrokers 获取在线Broker
func (cms *ClusterManagerService) GetOnlineBrokers() []*BrokerInfo {
	return cms.clusterManager.GetOnlineBrokers()
}

// SelectBrokerForProducer 为生产者选择Broker
func (cms *ClusterManagerService) SelectBrokerForProducer(topicName string) (*BrokerInfo, error) {
	return cms.clusterManager.SelectBrokerForProducer(topicName)
}

// SelectBrokerForConsumer 为消费者选择Broker
func (cms *ClusterManagerService) SelectBrokerForConsumer(topicName string, queueId int32) (*BrokerInfo, error) {
	return cms.clusterManager.SelectBrokerForConsumer(topicName, queueId)
}

// AddFaultNotificationHook 添加故障通知钩子
func (cms *ClusterManagerService) AddFaultNotificationHook(hook NotificationHook) {
	if cms.faultDetector != nil {
		cms.faultDetector.AddNotificationHook(hook)
	}
}

// TriggerManualRecovery 触发手动恢复
func (cms *ClusterManagerService) TriggerManualRecovery(nodeAddress string, action string) error {
	// 这里应该实现手动恢复逻辑
	log.Printf("Manual recovery triggered for node %s: %s", nodeAddress, action)
	
	// 实际实现应该：
	// 1. 验证节点地址
	// 2. 执行恢复动作
	// 3. 更新故障状态
	
	return nil
}

// GetServiceStatus 获取各服务状态
func (cms *ClusterManagerService) GetServiceStatus() map[string]bool {
	status := make(map[string]bool)
	
	status["clusterManager"] = cms.clusterManager != nil
	status["dataReplicator"] = cms.dataReplicator != nil && cms.dataReplicator.running
	status["faultDetector"] = cms.faultDetector != nil && cms.faultDetector.running
	status["haService"] = cms.haService != nil
	
	return status
}

// DefaultClusterManagerConfig 默认集群管理服务配置
func DefaultClusterManagerConfig() *ClusterManagerConfig {
	return &ClusterManagerConfig{
		ClusterName:           "DEFAULT_CLUSTER",
		EnableDataReplication: true,
		EnableFaultDetection:  true,
		EnableHA:             true,
		ReplicationConfig:     DefaultReplicationConfig(),
		FaultDetectorConfig:   DefaultFaultDetectorConfig(),
		HAConfig:             ha.DefaultHAConfig(),
	}
}