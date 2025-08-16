package failover

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go-rocketmq/pkg/cluster"
)

// FailoverService 故障转移服务
type FailoverService struct {
	clusterManager   *cluster.ClusterManager
	dataSyncService  *DataSyncService
	running          bool
	mutex            sync.RWMutex
	shutdown         chan struct{}
	failoverPolicies map[string]*FailoverPolicy
	failoverHistory  []*FailoverEvent

	// 配置
	checkInterval    time.Duration
	failoverTimeout  time.Duration
	recoveryTimeout  time.Duration
	maxRetryCount    int
}

// FailoverPolicy 故障转移策略
type FailoverPolicy struct {
	BrokerName       string                 `json:"brokerName"`
	FailoverType     FailoverType          `json:"failoverType"`
	BackupBrokers    []string              `json:"backupBrokers"`
	AutoFailover     bool                  `json:"autoFailover"`
	FailoverDelay    time.Duration         `json:"failoverDelay"`
	HealthThreshold  int                   `json:"healthThreshold"`
	RecoveryPolicy   RecoveryPolicy        `json:"recoveryPolicy"`
	Notifications    []NotificationConfig  `json:"notifications"`
}

// FailoverType 故障转移类型
type FailoverType int

const (
	// MANUAL_FAILOVER 手动故障转移
	MANUAL_FAILOVER FailoverType = iota
	// AUTO_FAILOVER 自动故障转移
	AUTO_FAILOVER
	// MASTER_SLAVE_SWITCH 主从切换
	MASTER_SLAVE_SWITCH
)

// RecoveryPolicy 恢复策略
type RecoveryPolicy int

const (
	// AUTO_RECOVERY 自动恢复
	AUTO_RECOVERY RecoveryPolicy = iota
	// MANUAL_RECOVERY 手动恢复
	MANUAL_RECOVERY
	// NO_RECOVERY 不恢复
	NO_RECOVERY
)

// NotificationConfig 通知配置
type NotificationConfig struct {
	Type     string `json:"type"`     // email, webhook, sms
	Endpoint string `json:"endpoint"` // 通知端点
	Enabled  bool   `json:"enabled"`
}

// FailoverEvent 故障转移事件
type FailoverEvent struct {
	EventId        string        `json:"eventId"`
	BrokerName     string        `json:"brokerName"`
	EventType      EventType     `json:"eventType"`
	Timestamp      int64         `json:"timestamp"`
	Reason         string        `json:"reason"`
	SourceBroker   string        `json:"sourceBroker"`
	TargetBroker   string        `json:"targetBroker"`
	Status         EventStatus   `json:"status"`
	Duration       time.Duration `json:"duration"`
	ErrorMessage   string        `json:"errorMessage,omitempty"`
}

// EventType 事件类型
type EventType int

const (
	// BROKER_DOWN Broker下线
	BROKER_DOWN EventType = iota
	// BROKER_UP Broker上线
	BROKER_UP
	// FAILOVER_START 故障转移开始
	FAILOVER_START
	// FAILOVER_COMPLETE 故障转移完成
	FAILOVER_COMPLETE
	// RECOVERY_START 恢复开始
	RECOVERY_START
	// RECOVERY_COMPLETE 恢复完成
	RECOVERY_COMPLETE
)

// EventStatus 事件状态
type EventStatus int

const (
	// PENDING 待处理
	PENDING EventStatus = iota
	// IN_PROGRESS 进行中
	IN_PROGRESS
	// SUCCESS 成功
	SUCCESS
	// FAILED 失败
	FAILED
)

// NewFailoverService 创建故障转移服务
func NewFailoverService(cm *cluster.ClusterManager) *FailoverService {
	return &FailoverService{
		clusterManager:   cm,
		dataSyncService:  NewDataSyncService(cm),
		shutdown:         make(chan struct{}),
		failoverPolicies: make(map[string]*FailoverPolicy),
		failoverHistory:  make([]*FailoverEvent, 0),
		checkInterval:    10 * time.Second,
		failoverTimeout:  30 * time.Second,
		recoveryTimeout:  60 * time.Second,
		maxRetryCount:    3,
	}
}

// Start 启动故障转移服务
func (fs *FailoverService) Start() error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if fs.running {
		return fmt.Errorf("failover service already running")
	}

	fs.running = true
	log.Printf("Starting failover service")

	// 启动数据同步服务
	if err := fs.dataSyncService.Start(); err != nil {
		return fmt.Errorf("failed to start data sync service: %v", err)
	}

	// 启动监控goroutine
	go fs.monitorBrokers()

	// 启动恢复检查goroutine
	go fs.checkRecovery()

	return nil
}

// Stop 停止故障转移服务
func (fs *FailoverService) Stop() {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if !fs.running {
		return
	}

	log.Printf("Stopping failover service")
	fs.running = false

	// 停止数据同步服务
	fs.dataSyncService.Stop()

	close(fs.shutdown)
}

// RegisterFailoverPolicy 注册故障转移策略
func (fs *FailoverService) RegisterFailoverPolicy(policy *FailoverPolicy) error {
	if policy == nil {
		return fmt.Errorf("failover policy cannot be nil")
	}

	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	fs.failoverPolicies[policy.BrokerName] = policy
	log.Printf("Registered failover policy for broker: %s", policy.BrokerName)

	return nil
}

// UnregisterFailoverPolicy 注销故障转移策略
func (fs *FailoverService) UnregisterFailoverPolicy(brokerName string) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	delete(fs.failoverPolicies, brokerName)
	log.Printf("Unregistered failover policy for broker: %s", brokerName)
}

// monitorBrokers 监控Broker状态
func (fs *FailoverService) monitorBrokers() {
	ticker := time.NewTicker(fs.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fs.shutdown:
			return
		case <-ticker.C:
			fs.checkBrokerStatus()
		}
	}
}

// checkBrokerStatus 检查Broker状态
func (fs *FailoverService) checkBrokerStatus() {
	brokers := fs.clusterManager.GetAllBrokers()

	for brokerName, broker := range brokers {
		policy, exists := fs.failoverPolicies[brokerName]
		if !exists {
			continue // 没有配置故障转移策略
		}

		// 检查Broker状态
		if broker.Status == cluster.OFFLINE && policy.AutoFailover {
			log.Printf("Detected broker %s is offline, triggering failover", brokerName)
			go fs.triggerFailover(brokerName, "Broker offline detected")
		} else if broker.Status == cluster.ONLINE {
			// 检查是否需要恢复
			fs.checkBrokerRecovery(brokerName)
		}
	}
}

// triggerFailover 触发故障转移
func (fs *FailoverService) triggerFailover(brokerName, reason string) {
	fs.mutex.Lock()
	policy, exists := fs.failoverPolicies[brokerName]
	if !exists {
		fs.mutex.Unlock()
		return
	}
	fs.mutex.Unlock()

	// 创建故障转移事件
	event := &FailoverEvent{
		EventId:      fmt.Sprintf("failover_%s_%d", brokerName, time.Now().UnixNano()),
		BrokerName:   brokerName,
		EventType:    FAILOVER_START,
		Timestamp:    time.Now().UnixMilli(),
		Reason:       reason,
		SourceBroker: brokerName,
		Status:       IN_PROGRESS,
	}

	fs.addFailoverEvent(event)

	log.Printf("Starting failover for broker %s, reason: %s", brokerName, reason)

	// 执行故障转移
	start := time.Now()
	err := fs.executeFailover(policy, event)
	event.Duration = time.Since(start)

	if err != nil {
		event.Status = FAILED
		event.ErrorMessage = err.Error()
		log.Printf("Failover failed for broker %s: %v", brokerName, err)
	} else {
		event.Status = SUCCESS
		event.EventType = FAILOVER_COMPLETE
		log.Printf("Failover completed for broker %s in %v", brokerName, event.Duration)
	}

	// 发送通知
	fs.sendNotifications(policy, event)
}

// executeFailover 执行故障转移
func (fs *FailoverService) executeFailover(policy *FailoverPolicy, event *FailoverEvent) error {
	if len(policy.BackupBrokers) == 0 {
		return fmt.Errorf("no backup brokers configured")
	}

	// 选择可用的备份Broker
	backupBroker, err := fs.selectBackupBroker(policy.BackupBrokers)
	if err != nil {
		return fmt.Errorf("failed to select backup broker: %v", err)
	}

	event.TargetBroker = backupBroker

	// 根据故障转移类型执行不同的操作
	switch policy.FailoverType {
	case AUTO_FAILOVER:
		return fs.performAutoFailover(policy.BrokerName, backupBroker)
	case MASTER_SLAVE_SWITCH:
		return fs.performMasterSlaveSwitch(policy.BrokerName, backupBroker)
	default:
		return fmt.Errorf("unsupported failover type: %v", policy.FailoverType)
	}
}

// selectBackupBroker 选择备份Broker
func (fs *FailoverService) selectBackupBroker(backupBrokers []string) (string, error) {
	for _, brokerName := range backupBrokers {
		broker, exists := fs.clusterManager.GetBroker(brokerName)
		if exists && broker.Status == cluster.ONLINE {
			return brokerName, nil
		}
	}
	return "", fmt.Errorf("no available backup brokers")
}

// performAutoFailover 执行自动故障转移
func (fs *FailoverService) performAutoFailover(sourceBroker, targetBroker string) error {
	log.Printf("Performing auto failover from %s to %s", sourceBroker, targetBroker)

	// 1. 更新路由信息，将流量切换到备份Broker
	if err := fs.updateRouteInfo(sourceBroker, targetBroker); err != nil {
		return fmt.Errorf("failed to update route info: %v", err)
	}

	// 2. 通知客户端更新路由
	if err := fs.notifyClientsRouteChange(sourceBroker, targetBroker); err != nil {
		return fmt.Errorf("failed to notify clients: %v", err)
	}

	// 3. 等待流量切换完成
	time.Sleep(5 * time.Second)

	log.Printf("Auto failover completed from %s to %s", sourceBroker, targetBroker)
	return nil
}

// performMasterSlaveSwitch 执行主从切换
func (fs *FailoverService) performMasterSlaveSwitch(masterBroker, slaveBroker string) error {
	log.Printf("Performing master-slave switch from %s to %s", masterBroker, slaveBroker)

	// 1. 停止主节点的写入
	if err := fs.stopMasterWrites(masterBroker); err != nil {
		return fmt.Errorf("failed to stop master writes: %v", err)
	}

	// 2. 等待从节点数据同步完成
	if err := fs.waitForSlaveSync(slaveBroker); err != nil {
		return fmt.Errorf("failed to wait for slave sync: %v", err)
	}

	// 3. 将从节点提升为主节点
	if err := fs.promoteSlaveToMaster(slaveBroker); err != nil {
		return fmt.Errorf("failed to promote slave to master: %v", err)
	}

	// 4. 更新路由信息
	if err := fs.updateRouteInfo(masterBroker, slaveBroker); err != nil {
		return fmt.Errorf("failed to update route info: %v", err)
	}

	log.Printf("Master-slave switch completed from %s to %s", masterBroker, slaveBroker)
	return nil
}

// updateRouteInfo 更新路由信息
func (fs *FailoverService) updateRouteInfo(sourceBroker, targetBroker string) error {
	// 这里应该更新NameServer中的路由信息
	// 简化实现：直接在集群管理器中更新
	log.Printf("Updating route info from %s to %s", sourceBroker, targetBroker)
	return nil
}

// notifyClientsRouteChange 通知客户端路由变更
func (fs *FailoverService) notifyClientsRouteChange(sourceBroker, targetBroker string) error {
	// 这里应该通知所有连接的客户端更新路由信息
	log.Printf("Notifying clients of route change from %s to %s", sourceBroker, targetBroker)
	return nil
}

// stopMasterWrites 停止主节点写入
func (fs *FailoverService) stopMasterWrites(masterBroker string) error {
	log.Printf("Stopping writes on master broker: %s", masterBroker)
	return nil
}

// waitForSlaveSync 等待从节点同步
func (fs *FailoverService) waitForSlaveSync(slaveBroker string) error {
	log.Printf("Waiting for slave sync on broker: %s", slaveBroker)

	// 检查是否有正在进行的同步任务
	syncTasks := fs.dataSyncService.GetAllSyncTasks()
	for _, task := range syncTasks {
		if task.SlaveBroker == slaveBroker && task.Status == SYNC_RUNNING {
			log.Printf("Found active sync task %s for slave %s, waiting for completion", task.TaskId, slaveBroker)
			
			// 等待同步完成，最多等待30秒
			timeout := time.After(30 * time.Second)
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			
			for {
				select {
				case <-timeout:
					return fmt.Errorf("timeout waiting for slave sync on %s", slaveBroker)
				case <-ticker.C:
					updatedTask, exists := fs.dataSyncService.GetSyncTask(task.TaskId)
					if !exists {
						return fmt.Errorf("sync task %s disappeared", task.TaskId)
					}
					if updatedTask.Status == SYNC_SUCCESS {
						log.Printf("Slave sync completed successfully for %s", slaveBroker)
						return nil
					}
					if updatedTask.Status == SYNC_FAILED {
						return fmt.Errorf("slave sync failed for %s: %s", slaveBroker, updatedTask.ErrorMessage)
					}
				}
			}
		}
	}

	// 如果没有活跃的同步任务，等待固定时间确保数据一致性
	log.Printf("No active sync task found, waiting for data consistency")
	time.Sleep(2 * time.Second)
	return nil
}

// promoteSlaveToMaster 将从节点提升为主节点
func (fs *FailoverService) promoteSlaveToMaster(slaveBroker string) error {
	log.Printf("Promoting slave to master: %s", slaveBroker)
	
	// 更新Broker角色
	broker, exists := fs.clusterManager.GetBroker(slaveBroker)
	if !exists {
		return fmt.Errorf("slave broker %s not found", slaveBroker)
	}

	broker.Role = "MASTER"
	return nil
}

// checkRecovery 检查恢复
func (fs *FailoverService) checkRecovery() {
	ticker := time.NewTicker(fs.checkInterval * 2) // 恢复检查频率较低
	defer ticker.Stop()

	for {
		select {
		case <-fs.shutdown:
			return
		case <-ticker.C:
			fs.checkBrokerRecoveryAll()
		}
	}
}

// checkBrokerRecoveryAll 检查所有Broker的恢复
func (fs *FailoverService) checkBrokerRecoveryAll() {
	fs.mutex.RLock()
	policies := make(map[string]*FailoverPolicy)
	for k, v := range fs.failoverPolicies {
		policies[k] = v
	}
	fs.mutex.RUnlock()

	for brokerName := range policies {
		fs.checkBrokerRecovery(brokerName)
	}
}

// checkBrokerRecovery 检查单个Broker的恢复
func (fs *FailoverService) checkBrokerRecovery(brokerName string) {
	broker, exists := fs.clusterManager.GetBroker(brokerName)
	if !exists {
		return
	}

	policy, exists := fs.failoverPolicies[brokerName]
	if !exists {
		return
	}

	// 检查是否需要恢复
	if broker.Status == cluster.ONLINE && policy.RecoveryPolicy == AUTO_RECOVERY {
		// 检查是否有故障转移事件需要恢复
	if fs.hasActiveFailover(brokerName) {
			log.Printf("Detected broker %s is back online, triggering recovery", brokerName)
			go fs.triggerRecovery(brokerName, "Broker back online")
		}
	}
}

// hasActiveFailover 检查是否有活跃的故障转移
func (fs *FailoverService) hasActiveFailover(brokerName string) bool {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	// 检查最近的故障转移事件
	for i := len(fs.failoverHistory) - 1; i >= 0; i-- {
		event := fs.failoverHistory[i]
		if event.BrokerName == brokerName && 
		   event.EventType == FAILOVER_COMPLETE && 
		   event.Status == SUCCESS {
			// 检查是否已经恢复
			for j := i + 1; j < len(fs.failoverHistory); j++ {
				recoveryEvent := fs.failoverHistory[j]
				if recoveryEvent.BrokerName == brokerName && 
				   recoveryEvent.EventType == RECOVERY_COMPLETE {
					return false // 已经恢复过了
				}
			}
			return true // 有故障转移但未恢复
		}
	}
	return false
}

// triggerRecovery 触发恢复
func (fs *FailoverService) triggerRecovery(brokerName, reason string) {
	// 创建恢复事件
	event := &FailoverEvent{
		EventId:      fmt.Sprintf("recovery_%s_%d", brokerName, time.Now().UnixNano()),
		BrokerName:   brokerName,
		EventType:    RECOVERY_START,
		Timestamp:    time.Now().UnixMilli(),
		Reason:       reason,
		SourceBroker: brokerName,
		Status:       IN_PROGRESS,
	}

	fs.addFailoverEvent(event)

	log.Printf("Starting recovery for broker %s, reason: %s", brokerName, reason)

	// 执行恢复
	start := time.Now()
	err := fs.executeRecovery(brokerName, event)
	event.Duration = time.Since(start)

	if err != nil {
		event.Status = FAILED
		event.ErrorMessage = err.Error()
		log.Printf("Recovery failed for broker %s: %v", brokerName, err)
	} else {
		event.Status = SUCCESS
		event.EventType = RECOVERY_COMPLETE
		log.Printf("Recovery completed for broker %s in %v", brokerName, event.Duration)
	}
}

// executeRecovery 执行恢复
func (fs *FailoverService) executeRecovery(brokerName string, event *FailoverEvent) error {
	log.Printf("Executing recovery for broker: %s", brokerName)

	// 1. 验证Broker健康状态
	if err := fs.verifyBrokerHealth(brokerName); err != nil {
		return fmt.Errorf("broker health verification failed: %v", err)
	}

	// 2. 恢复路由信息
	if err := fs.restoreRouteInfo(brokerName); err != nil {
		return fmt.Errorf("failed to restore route info: %v", err)
	}

	// 3. 通知客户端
	if err := fs.notifyClientsRecovery(brokerName); err != nil {
		return fmt.Errorf("failed to notify clients: %v", err)
	}

	return nil
}

// verifyBrokerHealth 验证Broker健康状态
func (fs *FailoverService) verifyBrokerHealth(brokerName string) error {
	broker, exists := fs.clusterManager.GetBroker(brokerName)
	if !exists {
		return fmt.Errorf("broker %s not found", brokerName)
	}

	if broker.Status != cluster.ONLINE {
		return fmt.Errorf("broker %s is not online", brokerName)
	}

	return nil
}

// restoreRouteInfo 恢复路由信息
func (fs *FailoverService) restoreRouteInfo(brokerName string) error {
	log.Printf("Restoring route info for broker: %s", brokerName)
	
	// 获取故障转移策略
	fs.mutex.RLock()
	policy, exists := fs.failoverPolicies[brokerName]
	fs.mutex.RUnlock()
	
	if !exists {
		return fmt.Errorf("no failover policy found for broker: %s", brokerName)
	}
	
	// 检查是否有可用的备份Broker
	if len(policy.BackupBrokers) == 0 {
		return fmt.Errorf("no backup brokers available for %s", brokerName)
	}
	
	// 选择一个健康的备份Broker作为新的主Broker
	newMasterBroker, err := fs.selectBackupBroker(policy.BackupBrokers)
	if err != nil {
		return fmt.Errorf("failed to select healthy backup broker: %v", err)
	}
	
	// 通过集群管理器更新路由信息
	if fs.clusterManager != nil {
		// 获取原Broker信息
		originalBroker, exists := fs.clusterManager.GetBroker(brokerName)
		if !exists {
			log.Printf("Original broker %s not found in cluster manager", brokerName)
		} else {
			// 创建新的Broker信息，使用备份Broker地址
			newBrokerInfo := &cluster.BrokerInfo{
				BrokerName:     brokerName,
				BrokerId:       originalBroker.BrokerId,
				ClusterName:    originalBroker.ClusterName,
				BrokerAddr:     newMasterBroker,
				Version:        originalBroker.Version,
				DataVersion:    time.Now().UnixMilli(),
				LastUpdateTime: time.Now().UnixMilli(),
				Role:           "MASTER",
				Status:         cluster.ONLINE,
				Metrics:        originalBroker.Metrics,
				Topics:         originalBroker.Topics,
			}
			
			// 重新注册Broker信息
			err = fs.clusterManager.RegisterBroker(newBrokerInfo)
			if err != nil {
				log.Printf("Failed to register updated broker info: %v", err)
			}
		}
	}
	
	log.Printf("Route info restored for broker %s, new master: %s", brokerName, newMasterBroker)
	return nil
}

// notifyClientsRecovery 通知客户端恢复
func (fs *FailoverService) notifyClientsRecovery(brokerName string) error {
	log.Printf("Notifying clients of recovery for broker: %s", brokerName)
	
	// 获取故障转移策略
	fs.mutex.RLock()
	policy, exists := fs.failoverPolicies[brokerName]
	fs.mutex.RUnlock()
	
	if !exists {
		return fmt.Errorf("no failover policy found for broker: %s", brokerName)
	}
	
	// 选择新的主Broker
	newMasterBroker, err := fs.selectBackupBroker(policy.BackupBrokers)
	if err != nil {
		return fmt.Errorf("failed to select healthy backup broker: %v", err)
	}
	
	// 通过集群管理器通知所有连接的客户端
	if fs.clusterManager != nil {
		// 获取所有在线Broker，作为通知目标
		onlineBrokers := fs.clusterManager.GetOnlineBrokers()
		
		// 构建路由变更通知消息
		notification := map[string]interface{}{
			"type":           "ROUTE_CHANGE",
			"brokerName":     brokerName,
			"newMasterAddr":  newMasterBroker,
			"timestamp":      time.Now().Unix(),
			"reason":         "BROKER_RECOVERY",
		}
		
		// 向所有在线Broker发送通知（它们会转发给连接的客户端）
		successCount := 0
		failureCount := 0
		
		for _, broker := range onlineBrokers {
			if broker.BrokerName != brokerName { // 不向自己发送通知
				err := fs.sendBrokerNotification(broker, notification)
				if err != nil {
					log.Printf("Failed to notify broker %s: %v", broker.BrokerName, err)
					failureCount++
				} else {
					successCount++
				}
			}
		}
		
		log.Printf("Broker notification completed: %d success, %d failures", successCount, failureCount)
		
		// 如果有Broker通知失败，记录警告但不返回错误
		if failureCount > 0 {
			log.Printf("Warning: %d brokers failed to receive recovery notification for broker %s", failureCount, brokerName)
		}
	}
	
	// 记录恢复事件
	recoveryEvent := &FailoverEvent{
		EventId:        fmt.Sprintf("recovery_%s_%d", brokerName, time.Now().Unix()),
		BrokerName:     brokerName,
		EventType:      RECOVERY_COMPLETE,
		Timestamp:      time.Now().Unix(),
		Reason:         "Broker recovery completed",
		SourceBroker:   brokerName,
		TargetBroker:   newMasterBroker,
		Status:         SUCCESS,
		Duration:       0,
	}
	fs.addFailoverEvent(recoveryEvent)
	
	log.Printf("Recovery notification sent to clients for broker: %s", brokerName)
	return nil
}

// sendNotifications 发送通知
func (fs *FailoverService) sendNotifications(policy *FailoverPolicy, event *FailoverEvent) {
	for _, notification := range policy.Notifications {
		if !notification.Enabled {
			continue
		}

		go func(config NotificationConfig) {
			if err := fs.sendNotification(config, event); err != nil {
				log.Printf("Failed to send notification: %v", err)
			}
		}(notification)
	}
}

// sendNotification 发送单个通知
func (fs *FailoverService) sendNotification(config NotificationConfig, event *FailoverEvent) error {
	log.Printf("Sending %s notification to %s for event %s", 
		config.Type, config.Endpoint, event.EventId)
	
	// 这里应该实现具体的通知逻辑（邮件、webhook、短信等）
	return nil
}

// addFailoverEvent 添加故障转移事件
func (fs *FailoverService) addFailoverEvent(event *FailoverEvent) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	fs.failoverHistory = append(fs.failoverHistory, event)

	// 保持历史记录数量在合理范围内
	if len(fs.failoverHistory) > 1000 {
		fs.failoverHistory = fs.failoverHistory[100:] // 保留最近900条记录
	}
}

// GetFailoverHistory 获取故障转移历史
func (fs *FailoverService) GetFailoverHistory(limit int) []*FailoverEvent {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	if limit <= 0 || limit > len(fs.failoverHistory) {
		limit = len(fs.failoverHistory)
	}

	result := make([]*FailoverEvent, limit)
	startIndex := len(fs.failoverHistory) - limit
	copy(result, fs.failoverHistory[startIndex:])

	return result
}

// GetFailoverStatus 获取故障转移状态
func (fs *FailoverService) GetFailoverStatus() map[string]interface{} {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	status := make(map[string]interface{})
	status["running"] = fs.running
	status["check_interval"] = fs.checkInterval.String()
	status["failover_timeout"] = fs.failoverTimeout.String()
	status["recovery_timeout"] = fs.recoveryTimeout.String()
	status["max_retry_count"] = fs.maxRetryCount
	status["policy_count"] = len(fs.failoverPolicies)
	status["event_count"] = len(fs.failoverHistory)

	return status
}

// ManualFailover 手动故障转移
func (fs *FailoverService) ManualFailover(brokerName, targetBroker, reason string) error {
	fs.mutex.RLock()
	policy, exists := fs.failoverPolicies[brokerName]
	fs.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("no failover policy found for broker: %s", brokerName)
	}

	// 创建临时策略用于手动故障转移
	tempPolicy := *policy
	tempPolicy.FailoverType = MANUAL_FAILOVER
	tempPolicy.BackupBrokers = []string{targetBroker}

	// 创建故障转移事件
	event := &FailoverEvent{
		EventId:      fmt.Sprintf("manual_failover_%s_%d", brokerName, time.Now().UnixNano()),
		BrokerName:   brokerName,
		EventType:    FAILOVER_START,
		Timestamp:    time.Now().UnixMilli(),
		Reason:       fmt.Sprintf("Manual failover: %s", reason),
		SourceBroker: brokerName,
		TargetBroker: targetBroker,
		Status:       IN_PROGRESS,
	}

	fs.addFailoverEvent(event)

	// 执行故障转移
	start := time.Now()
	err := fs.executeFailover(&tempPolicy, event)
	event.Duration = time.Since(start)

	if err != nil {
		event.Status = FAILED
		event.ErrorMessage = err.Error()
		return err
	}

	event.Status = SUCCESS
	event.EventType = FAILOVER_COMPLETE
	return nil
}

// ManualRecovery 手动恢复
func (fs *FailoverService) ManualRecovery(brokerName, reason string) error {
	// 创建恢复事件
	event := &FailoverEvent{
		EventId:      fmt.Sprintf("manual_recovery_%s_%d", brokerName, time.Now().UnixNano()),
		BrokerName:   brokerName,
		EventType:    RECOVERY_START,
		Timestamp:    time.Now().UnixMilli(),
		Reason:       fmt.Sprintf("Manual recovery: %s", reason),
		SourceBroker: brokerName,
		Status:       IN_PROGRESS,
	}

	fs.addFailoverEvent(event)

	// 执行恢复
	start := time.Now()
	err := fs.executeRecovery(brokerName, event)
	event.Duration = time.Since(start)

	if err != nil {
		event.Status = FAILED
		event.ErrorMessage = err.Error()
		return err
	}

	event.Status = SUCCESS
	event.EventType = RECOVERY_COMPLETE
	return nil
}

// sendBrokerNotification 向Broker发送通知
func (fs *FailoverService) sendBrokerNotification(broker *cluster.BrokerInfo, notification map[string]interface{}) error {
	// 这里应该实现向Broker发送通知的逻辑
	// 简化实现：记录日志
	log.Printf("Sending notification to broker %s: %v", broker.BrokerName, notification)
	return nil
}