package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
	"go-rocketmq/pkg/cluster"
)

// HAConfig 高可用配置
type HAConfig struct {
	ClusterName          string        `yaml:"cluster_name"`
	NameServerAddrs      []string      `yaml:"nameserver_addrs"`
	MasterBrokers        []string      `yaml:"master_brokers"`
	SlaveBrokers         []string      `yaml:"slave_brokers"`
	ReplicationMode      string        `yaml:"replication_mode"` // SYNC, ASYNC
	FlushDiskType        string        `yaml:"flush_disk_type"` // SYNC_FLUSH, ASYNC_FLUSH
	HealthCheckInterval  time.Duration `yaml:"health_check_interval"`
	FailoverTimeout      time.Duration `yaml:"failover_timeout"`
	DataReplicationDelay time.Duration `yaml:"data_replication_delay"`
	BackupRetentionDays  int           `yaml:"backup_retention_days"`
}

// HAManager 高可用管理器
type HAManager struct {
	config           *HAConfig
	clusterMgr       *cluster.ClusterManager
	masterProducers  []*client.Producer
	slaveProducers   []*client.Producer
	consumers        []*client.Consumer
	healthMonitor    *HealthMonitor
	replicationMgr   *ReplicationManager
	backupMgr        *BackupManager
	running          bool
	mutex            sync.RWMutex
}

// HealthMonitor 健康监控器
type HealthMonitor struct {
	haManager        *HAManager
	masterStatus     map[string]BrokerHealthStatus
	slaveStatus      map[string]BrokerHealthStatus
	lastCheckTime    time.Time
	running          bool
	mutex            sync.RWMutex
}

// BrokerHealthStatus Broker 健康状态
type BrokerHealthStatus struct {
	BrokerAddr       string    `json:"broker_addr"`
	IsHealthy        bool      `json:"is_healthy"`
	LastHeartbeat    time.Time `json:"last_heartbeat"`
	ResponseTime     time.Duration `json:"response_time"`
	ErrorCount       int       `json:"error_count"`
	LastErrorTime    time.Time `json:"last_error_time"`
	LastErrorMessage string    `json:"last_error_message"`
}

// ReplicationManager 数据复制管理器
type ReplicationManager struct {
	haManager         *HAManager
	replicationStatus map[string]*ReplicationStatus
	running           bool
	mutex             sync.RWMutex
}

// ReplicationStatus 复制状态
type ReplicationStatus struct {
	MasterAddr        string    `json:"master_addr"`
	SlaveAddr         string    `json:"slave_addr"`
	ReplicationLag    int64     `json:"replication_lag"`
	LastSyncTime      time.Time `json:"last_sync_time"`
	SyncedMessages    int64     `json:"synced_messages"`
	PendingMessages   int64     `json:"pending_messages"`
	IsInSync          bool      `json:"is_in_sync"`
}

// BackupManager 备份管理器
type BackupManager struct {
	haManager     *HAManager
	backupStatus  map[string]*BackupStatus
	running       bool
	mutex         sync.RWMutex
}

// BackupStatus 备份状态
type BackupStatus struct {
	BrokerAddr      string    `json:"broker_addr"`
	LastBackupTime  time.Time `json:"last_backup_time"`
	BackupSize      int64     `json:"backup_size"`
	BackupPath      string    `json:"backup_path"`
	IsBackupHealthy bool      `json:"is_backup_healthy"`
}

// NewHAManager 创建高可用管理器
func NewHAManager(config *HAConfig) (*HAManager, error) {
	clusterMgr := cluster.NewClusterManager(config.ClusterName)
	
	healthMonitor := &HealthMonitor{
		masterStatus:  make(map[string]BrokerHealthStatus),
		slaveStatus:   make(map[string]BrokerHealthStatus),
		running:       false,
	}
	
	replicationMgr := &ReplicationManager{
		replicationStatus: make(map[string]*ReplicationStatus),
		running:           false,
	}
	
	backupMgr := &BackupManager{
		backupStatus: make(map[string]*BackupStatus),
		running:      false,
	}
	
	haManager := &HAManager{
		config:         config,
		clusterMgr:     clusterMgr,
		masterProducers: make([]*client.Producer, 0),
		slaveProducers:  make([]*client.Producer, 0),
		consumers:       make([]*client.Consumer, 0),
		healthMonitor:   healthMonitor,
		replicationMgr:  replicationMgr,
		backupMgr:       backupMgr,
		running:         false,
	}
	
	healthMonitor.haManager = haManager
	replicationMgr.haManager = haManager
	backupMgr.haManager = haManager
	
	return haManager, nil
}

// Start 启动高可用管理器
func (ha *HAManager) Start() error {
	log.Println("启动高可用管理器...")
	
	// 启动集群管理器
	if err := ha.clusterMgr.Start(); err != nil {
		return fmt.Errorf("启动集群管理器失败: %v", err)
	}
	
	// 创建主 Broker 生产者
	if err := ha.createMasterProducers(); err != nil {
		return fmt.Errorf("创建主 Broker 生产者失败: %v", err)
	}
	
	// 创建从 Broker 生产者
	if err := ha.createSlaveProducers(); err != nil {
		return fmt.Errorf("创建从 Broker 生产者失败: %v", err)
	}
	
	// 创建消费者
	if err := ha.createConsumers(); err != nil {
		return fmt.Errorf("创建消费者失败: %v", err)
	}
	
	// 启动健康监控
	go ha.startHealthMonitoring()
	
	// 启动数据复制监控
	go ha.startReplicationMonitoring()
	
	// 启动备份管理
	go ha.startBackupManagement()
	
	ha.running = true
	log.Println("高可用管理器启动完成")
	return nil
}

// createMasterProducers 创建主 Broker 生产者
func (ha *HAManager) createMasterProducers() error {
	log.Printf("创建 %d 个主 Broker 生产者...", len(ha.config.MasterBrokers))
	
	for i, brokerAddr := range ha.config.MasterBrokers {
		producer := client.NewProducer(fmt.Sprintf("ha_master_producer_group_%d", i))
		producer.SetNameServers(ha.config.NameServerAddrs)
		
		if err := producer.Start(); err != nil {
			return fmt.Errorf("启动主 Broker 生产者失败 %s: %v", brokerAddr, err)
		}
		
		ha.masterProducers = append(ha.masterProducers, producer)
		
		// 初始化健康状态
		ha.healthMonitor.masterStatus[brokerAddr] = BrokerHealthStatus{
			BrokerAddr:    brokerAddr,
			IsHealthy:     true,
			LastHeartbeat: time.Now(),
		}
	}
	
	return nil
}

// createSlaveProducers 创建从 Broker 生产者
func (ha *HAManager) createSlaveProducers() error {
	log.Printf("创建 %d 个从 Broker 生产者...", len(ha.config.SlaveBrokers))
	
	for i, brokerAddr := range ha.config.SlaveBrokers {
		producer := client.NewProducer(fmt.Sprintf("ha_slave_producer_group_%d", i))
		producer.SetNameServers(ha.config.NameServerAddrs)
		
		if err := producer.Start(); err != nil {
			return fmt.Errorf("创建从 Broker 生产者失败 %s: %v", brokerAddr, err)
		}
		
		if err := producer.Start(); err != nil {
			return fmt.Errorf("启动从 Broker 生产者失败 %s: %v", brokerAddr, err)
		}
		
		ha.slaveProducers = append(ha.slaveProducers, producer)
		
		// 初始化健康状态
		ha.healthMonitor.slaveStatus[brokerAddr] = BrokerHealthStatus{
			BrokerAddr:    brokerAddr,
			IsHealthy:     true,
			LastHeartbeat: time.Now(),
		}
	}
	
	return nil
}

// createConsumers 创建消费者
func (ha *HAManager) createConsumers() error {
	log.Println("创建高可用消费者...")
	
	consumer := client.NewConsumer(&client.ConsumerConfig{
		GroupName: "ha_consumer_group",
	})
	consumer.SetNameServerAddr(ha.config.NameServerAddrs[0])
	consumer.Subscribe("ha_test_topic", "*", client.MessageListenerConcurrently(func(msgs []*client.MessageExt) client.ConsumeResult {
		for _, msg := range msgs {
			log.Printf("收到消息: %s", string(msg.Body))
		}
		return client.ConsumeSuccess
	}))
	
	if err := consumer.Start(); err != nil {
		return fmt.Errorf("启动消费者失败: %v", err)
	}
	
	ha.consumers = append(ha.consumers, consumer)
	return nil
}

// startHealthMonitoring 启动健康监控
func (ha *HAManager) startHealthMonitoring() {
	ha.healthMonitor.running = true
	ticker := time.NewTicker(ha.config.HealthCheckInterval)
	defer ticker.Stop()
	
	for ha.healthMonitor.running {
		select {
		case <-ticker.C:
			ha.performHealthCheck()
		}
	}
}

// performHealthCheck 执行健康检查
func (ha *HAManager) performHealthCheck() {
	log.Println("执行高可用健康检查...")
	
	ha.healthMonitor.mutex.Lock()
	defer ha.healthMonitor.mutex.Unlock()
	
	// 检查主 Broker 健康状态
	for _, brokerAddr := range ha.config.MasterBrokers {
		ha.checkBrokerHealth(brokerAddr, true)
	}
	
	// 检查从 Broker 健康状态
	for _, brokerAddr := range ha.config.SlaveBrokers {
		ha.checkBrokerHealth(brokerAddr, false)
	}
	
	ha.healthMonitor.lastCheckTime = time.Now()
}

// checkBrokerHealth 检查 Broker 健康状态
func (ha *HAManager) checkBrokerHealth(brokerAddr string, isMaster bool) {
	start := time.Now()
	
	// 这里可以实现具体的健康检查逻辑
	// 例如：发送心跳请求、检查连接状态等
	log.Printf("检查 Broker %s 健康状态 (Master: %v)", brokerAddr, isMaster)
	
	// 模拟健康检查
	isHealthy := true // 假设健康检查成功
	responseTime := time.Since(start)
	
	var status BrokerHealthStatus
	if isMaster {
		status = ha.healthMonitor.masterStatus[brokerAddr]
	} else {
		status = ha.healthMonitor.slaveStatus[brokerAddr]
	}
	
	status.IsHealthy = isHealthy
	status.LastHeartbeat = time.Now()
	status.ResponseTime = responseTime
	
	if !isHealthy {
		status.ErrorCount++
		status.LastErrorTime = time.Now()
		status.LastErrorMessage = "Health check failed"
		
		// 触发故障转移
		if isMaster {
			log.Printf("主 Broker %s 健康检查失败，触发故障转移", brokerAddr)
			ha.triggerMasterFailover(brokerAddr)
		}
	} else {
		status.ErrorCount = 0
	}
	
	if isMaster {
		ha.healthMonitor.masterStatus[brokerAddr] = status
	} else {
		ha.healthMonitor.slaveStatus[brokerAddr] = status
	}
}

// triggerMasterFailover 触发主 Broker 故障转移
func (ha *HAManager) triggerMasterFailover(failedMasterAddr string) {
	log.Printf("开始主 Broker 故障转移: %s", failedMasterAddr)
	
	// 查找可用的从 Broker
	availableSlave := ha.findAvailableSlave()
	if availableSlave == "" {
		log.Printf("警告: 没有可用的从 Broker 进行故障转移")
		return
	}
	
	log.Printf("选择从 Broker %s 作为新的主 Broker", availableSlave)
	
	// 执行故障转移逻辑
	if err := ha.executeFailover(failedMasterAddr, availableSlave); err != nil {
		log.Printf("故障转移失败: %v", err)
		return
	}
	
	log.Printf("主 Broker 故障转移完成: %s -> %s", failedMasterAddr, availableSlave)
}

// findAvailableSlave 查找可用的从 Broker
func (ha *HAManager) findAvailableSlave() string {
	for brokerAddr, status := range ha.healthMonitor.slaveStatus {
		if status.IsHealthy {
			return brokerAddr
		}
	}
	return ""
}

// executeFailover 执行故障转移
func (ha *HAManager) executeFailover(failedMaster, newMaster string) error {
	log.Printf("执行故障转移: %s -> %s", failedMaster, newMaster)
	
	// 这里可以实现具体的故障转移逻辑
	// 例如：更新路由信息、重新配置生产者和消费者等
	
	// 模拟故障转移过程
	time.Sleep(200 * time.Millisecond)
	
	return nil
}

// startReplicationMonitoring 启动数据复制监控
func (ha *HAManager) startReplicationMonitoring() {
	ha.replicationMgr.running = true
	ticker := time.NewTicker(ha.config.DataReplicationDelay)
	defer ticker.Stop()
	
	for ha.replicationMgr.running {
		select {
		case <-ticker.C:
			ha.monitorReplication()
		}
	}
}

// monitorReplication 监控数据复制
func (ha *HAManager) monitorReplication() {
	log.Println("监控数据复制状态...")
	
	ha.replicationMgr.mutex.Lock()
	defer ha.replicationMgr.mutex.Unlock()
	
	// 检查每个主从复制对的状态
	for i, masterAddr := range ha.config.MasterBrokers {
		if i < len(ha.config.SlaveBrokers) {
			slaveAddr := ha.config.SlaveBrokers[i]
			ha.checkReplicationStatus(masterAddr, slaveAddr)
		}
	}
}

// checkReplicationStatus 检查复制状态
func (ha *HAManager) checkReplicationStatus(masterAddr, slaveAddr string) {
	key := fmt.Sprintf("%s-%s", masterAddr, slaveAddr)
	
	// 获取或创建复制状态
	status, exists := ha.replicationMgr.replicationStatus[key]
	if !exists {
		status = &ReplicationStatus{
			MasterAddr: masterAddr,
			SlaveAddr:  slaveAddr,
		}
		ha.replicationMgr.replicationStatus[key] = status
	}
	
	// 模拟复制状态检查
	status.LastSyncTime = time.Now()
	status.ReplicationLag = int64(time.Since(status.LastSyncTime).Milliseconds())
	status.IsInSync = status.ReplicationLag < 1000 // 1秒内认为同步
	
	log.Printf("复制状态 %s -> %s: 延迟=%dms, 同步=%v",
		masterAddr, slaveAddr, status.ReplicationLag, status.IsInSync)
}

// startBackupManagement 启动备份管理
func (ha *HAManager) startBackupManagement() {
	ha.backupMgr.running = true
	ticker := time.NewTicker(24 * time.Hour) // 每天备份一次
	defer ticker.Stop()
	
	for ha.backupMgr.running {
		select {
		case <-ticker.C:
			ha.performBackup()
		}
	}
}

// performBackup 执行备份
func (ha *HAManager) performBackup() {
	log.Println("执行数据备份...")
	
	ha.backupMgr.mutex.Lock()
	defer ha.backupMgr.mutex.Unlock()
	
	// 备份所有主 Broker 的数据
	for _, brokerAddr := range ha.config.MasterBrokers {
		ha.backupBrokerData(brokerAddr)
	}
	
	// 清理过期备份
	ha.cleanupExpiredBackups()
}

// backupBrokerData 备份 Broker 数据
func (ha *HAManager) backupBrokerData(brokerAddr string) {
	log.Printf("备份 Broker %s 数据...", brokerAddr)
	
	// 获取或创建备份状态
	status, exists := ha.backupMgr.backupStatus[brokerAddr]
	if !exists {
		status = &BackupStatus{
			BrokerAddr: brokerAddr,
		}
		ha.backupMgr.backupStatus[brokerAddr] = status
	}
	
	// 模拟备份过程
	status.LastBackupTime = time.Now()
	status.BackupSize = 1024 * 1024 * 100 // 100MB
	status.BackupPath = fmt.Sprintf("/backup/%s/%s", brokerAddr, status.LastBackupTime.Format("20060102-150405"))
	status.IsBackupHealthy = true
	
	log.Printf("Broker %s 备份完成: 大小=%dMB, 路径=%s",
		brokerAddr, status.BackupSize/(1024*1024), status.BackupPath)
}

// cleanupExpiredBackups 清理过期备份
func (ha *HAManager) cleanupExpiredBackups() {
	log.Printf("清理 %d 天前的过期备份...", ha.config.BackupRetentionDays)
	
	// 这里可以实现具体的备份清理逻辑
	// 例如：删除超过保留期限的备份文件
}

// TestHAScenarios 测试高可用场景
func (ha *HAManager) TestHAScenarios() error {
	log.Println("开始高可用场景测试...")
	
	// 测试正常消息发送
	if err := ha.testNormalMessageSending(); err != nil {
		return fmt.Errorf("正常消息发送测试失败: %v", err)
	}
	
	// 测试主 Broker 故障场景
	if err := ha.testMasterFailureScenario(); err != nil {
		return fmt.Errorf("主 Broker 故障测试失败: %v", err)
	}
	
	// 测试数据复制
	if err := ha.testDataReplication(); err != nil {
		return fmt.Errorf("数据复制测试失败: %v", err)
	}
	
	log.Println("高可用场景测试完成")
	return nil
}

// testNormalMessageSending 测试正常消息发送
func (ha *HAManager) testNormalMessageSending() error {
	log.Println("测试正常消息发送...")
	
	if len(ha.masterProducers) == 0 {
		return fmt.Errorf("没有可用的主生产者")
	}
	
	producer := ha.masterProducers[0]
	for i := 0; i < 5; i++ {
		msg := &client.Message{
			Topic: "ha_test_topic",
			Body:  []byte(fmt.Sprintf("HA test message %d", i)),
			Tags:  "ha_test",
		}
		
		result, err := producer.SendSync(msg)
		if err != nil {
			return fmt.Errorf("发送消息失败: %v", err)
		}
		
		log.Printf("发送消息成功: %s", result.MsgId)
		time.Sleep(100 * time.Millisecond)
	}
	
	return nil
}

// testMasterFailureScenario 测试主 Broker 故障场景
func (ha *HAManager) testMasterFailureScenario() error {
	log.Println("测试主 Broker 故障场景...")
	
	if len(ha.config.MasterBrokers) == 0 {
		return fmt.Errorf("没有配置主 Broker")
	}
	
	// 模拟主 Broker 故障
	failedMaster := ha.config.MasterBrokers[0]
	log.Printf("模拟主 Broker %s 故障", failedMaster)
	
	// 更新健康状态
	ha.healthMonitor.mutex.Lock()
	status := ha.healthMonitor.masterStatus[failedMaster]
	status.IsHealthy = false
	status.ErrorCount = 5
	status.LastErrorTime = time.Now()
	status.LastErrorMessage = "Simulated failure"
	ha.healthMonitor.masterStatus[failedMaster] = status
	ha.healthMonitor.mutex.Unlock()
	
	// 触发故障转移
	ha.triggerMasterFailover(failedMaster)
	
	// 等待故障转移完成
	time.Sleep(1 * time.Second)
	
	log.Println("主 Broker 故障场景测试完成")
	return nil
}

// testDataReplication 测试数据复制
func (ha *HAManager) testDataReplication() error {
	log.Println("测试数据复制...")
	
	// 检查复制状态
	ha.monitorReplication()
	
	// 验证复制延迟
	ha.replicationMgr.mutex.RLock()
	for key, status := range ha.replicationMgr.replicationStatus {
		log.Printf("复制对 %s: 延迟=%dms, 同步=%v",
			key, status.ReplicationLag, status.IsInSync)
	}
	ha.replicationMgr.mutex.RUnlock()
	
	return nil
}

// GetHAStatus 获取高可用状态
func (ha *HAManager) GetHAStatus() map[string]interface{} {
	ha.mutex.RLock()
	defer ha.mutex.RUnlock()
	
	return map[string]interface{}{
		"running":            ha.running,
		"cluster_name":       ha.config.ClusterName,
		"replication_mode":   ha.config.ReplicationMode,
		"flush_disk_type":    ha.config.FlushDiskType,
		"master_brokers":     ha.config.MasterBrokers,
		"slave_brokers":      ha.config.SlaveBrokers,
		"master_status":      ha.healthMonitor.masterStatus,
		"slave_status":       ha.healthMonitor.slaveStatus,
		"replication_status": ha.replicationMgr.replicationStatus,
		"backup_status":      ha.backupMgr.backupStatus,
	}
}

// Stop 停止高可用管理器
func (ha *HAManager) Stop() error {
	log.Println("停止高可用管理器...")
	
	ha.running = false
	ha.healthMonitor.running = false
	ha.replicationMgr.running = false
	ha.backupMgr.running = false
	
	// 关闭所有主生产者
	for i, producer := range ha.masterProducers {
		log.Printf("关闭主生产者 %d", i)
		producer.Shutdown()
	}
	
	// 关闭所有从生产者
	for i, producer := range ha.slaveProducers {
		log.Printf("关闭从生产者 %d", i)
		producer.Shutdown()
	}
	
	// 关闭所有消费者
	for i, consumer := range ha.consumers {
		log.Printf("关闭消费者 %d", i)
		consumer.Stop()
	}
	
	// 停止集群管理器
	ha.clusterMgr.Stop()
	
	log.Println("高可用管理器已停止")
	return nil
}

func main() {
	// 高可用配置
	config := &HAConfig{
		ClusterName:          "ha-cluster",
		NameServerAddrs:      []string{"127.0.0.1:9876"},
		MasterBrokers:        []string{"127.0.0.1:10911", "127.0.0.1:10913"},
		SlaveBrokers:         []string{"127.0.0.1:10912", "127.0.0.1:10914"},
		ReplicationMode:      "SYNC",
		FlushDiskType:        "SYNC_FLUSH",
		HealthCheckInterval:  5 * time.Second,
		FailoverTimeout:      30 * time.Second,
		DataReplicationDelay: 1 * time.Second,
		BackupRetentionDays:  7,
	}
	
	// 创建高可用管理器
	haManager, err := NewHAManager(config)
	if err != nil {
		log.Fatalf("创建高可用管理器失败: %v", err)
	}
	
	// 启动高可用管理器
	if err := haManager.Start(); err != nil {
		log.Fatalf("启动高可用管理器失败: %v", err)
	}
	defer haManager.Stop()
	
	// 运行高可用测试
	go func() {
		time.Sleep(2 * time.Second)
		if err := haManager.TestHAScenarios(); err != nil {
			log.Printf("高可用测试失败: %v", err)
		}
	}()
	
	// 定期打印状态信息
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	
	for i := 0; i < 4; i++ {
		select {
		case <-ticker.C:
			status := haManager.GetHAStatus()
			log.Printf("高可用状态: %+v", status)
		}
	}
	
	log.Println("高可用示例运行完成")
}