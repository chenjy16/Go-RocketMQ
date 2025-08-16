package main

import (
	"fmt"
	"log"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
	"go-rocketmq/pkg/cluster"
)

// FailoverConfig 故障转移配置
type FailoverConfig struct {
	ClusterName        string   `yaml:"cluster_name"`
	NameServerAddrs    []string `yaml:"nameserver_addrs"`
	BrokerAddrs        []string `yaml:"broker_addrs"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	FailoverThreshold  int      `yaml:"failover_threshold"`
	RetryInterval      time.Duration `yaml:"retry_interval"`
	MaxRetries         int      `yaml:"max_retries"`
}

// FailoverManager 故障转移管理器
type FailoverManager struct {
	config         *FailoverConfig
	clusterMgr     *cluster.ClusterManager
	producer       *client.Producer
	consumer       *client.Consumer
	healthChecker  *HealthChecker
	running        bool
	failoverCount  int
}

// HealthChecker 健康检查器
type HealthChecker struct {
	manager       *FailoverManager
	failedChecks  map[string]int
	lastCheckTime map[string]time.Time
	running       bool
}

// NewFailoverManager 创建故障转移管理器
func NewFailoverManager(config *FailoverConfig) (*FailoverManager, error) {
	clusterMgr := cluster.NewClusterManager(config.ClusterName)
	
	healthChecker := &HealthChecker{
		failedChecks:  make(map[string]int),
		lastCheckTime: make(map[string]time.Time),
		running:       false,
	}
	
	fm := &FailoverManager{
		config:        config,
		clusterMgr:    clusterMgr,
		healthChecker: healthChecker,
		running:       false,
		failoverCount: 0,
	}
	
	healthChecker.manager = fm
	return fm, nil
}

// Start 启动故障转移管理器
func (fm *FailoverManager) Start() error {
	log.Println("启动故障转移管理器...")
	
	// 启动集群管理器
	if err := fm.clusterMgr.Start(); err != nil {
		return fmt.Errorf("启动集群管理器失败: %v", err)
	}
	
	// 创建生产者
	producer := client.NewProducer("failover_producer_group")
	producer.SetNameServers(fm.config.NameServerAddrs)
	
	if err := producer.Start(); err != nil {
		return fmt.Errorf("启动生产者失败: %v", err)
	}
	fm.producer = producer
	
	// 创建消费者
	consumerConfig := &client.ConsumerConfig{
		GroupName:        "failover_consumer_group",
		NameServerAddr:   fm.config.NameServerAddrs[0],
		ConsumeFromWhere: client.ConsumeFromLastOffset,
		MessageModel:     client.Clustering,
	}
	consumer := client.NewConsumer(consumerConfig)
	
	if err := consumer.Start(); err != nil {
		return fmt.Errorf("启动消费者失败: %v", err)
	}
	fm.consumer = consumer
	
	// 启动健康检查
	go fm.startHealthCheck()
	
	fm.running = true
	log.Println("故障转移管理器启动完成")
	return nil
}

// startHealthCheck 启动健康检查
func (fm *FailoverManager) startHealthCheck() {
	fm.healthChecker.running = true
	ticker := time.NewTicker(fm.config.HealthCheckInterval)
	defer ticker.Stop()
	
	for fm.healthChecker.running {
		select {
		case <-ticker.C:
			fm.performHealthCheck()
		}
	}
}

// performHealthCheck 执行健康检查
func (fm *FailoverManager) performHealthCheck() {
	log.Println("执行健康检查...")
	
	// 获取所有在线 Broker
	brokers := fm.clusterMgr.GetOnlineBrokers()
	if len(brokers) == 0 {
		log.Println("警告: 没有在线的 Broker")
		return
	}
	
	// 检查每个 Broker 的健康状态
	for _, broker := range brokers {
		if err := fm.checkBrokerHealth(broker); err != nil {
			log.Printf("Broker %s 健康检查失败: %v", broker.BrokerName, err)
			fm.handleBrokerFailure(broker)
		} else {
			// 重置失败计数
			fm.healthChecker.failedChecks[broker.BrokerName] = 0
			fm.healthChecker.lastCheckTime[broker.BrokerName] = time.Now()
		}
	}
}

// checkBrokerHealth 检查 Broker 健康状态
func (fm *FailoverManager) checkBrokerHealth(broker *cluster.BrokerInfo) error {
	// 这里可以实现具体的健康检查逻辑
	// 例如：发送心跳消息、检查连接状态等
	log.Printf("检查 Broker %s 健康状态", broker.BrokerName)
	
	// 模拟健康检查
	if broker.Status == cluster.OFFLINE {
		return fmt.Errorf("broker is offline")
	}
	
	return nil
}

// handleBrokerFailure 处理 Broker 故障
func (fm *FailoverManager) handleBrokerFailure(broker *cluster.BrokerInfo) {
	fm.healthChecker.failedChecks[broker.BrokerName]++
	failedCount := fm.healthChecker.failedChecks[broker.BrokerName]
	
	log.Printf("Broker %s 故障次数: %d", broker.BrokerName, failedCount)
	
	if failedCount >= fm.config.FailoverThreshold {
		log.Printf("触发故障转移: Broker %s", broker.BrokerName)
		fm.triggerFailover(broker)
	}
}

// triggerFailover 触发故障转移
func (fm *FailoverManager) triggerFailover(failedBroker *cluster.BrokerInfo) {
	fm.failoverCount++
	log.Printf("开始故障转移 #%d: 从 %s 切换", fm.failoverCount, failedBroker.BrokerName)
	
	// 查找可用的备用 Broker
	availableBrokers := fm.findAvailableBrokers(failedBroker)
	if len(availableBrokers) == 0 {
		log.Printf("警告: 没有可用的备用 Broker")
		return
	}
	
	// 选择最佳的备用 Broker
	targetBroker := fm.selectBestBroker(availableBrokers)
	log.Printf("选择目标 Broker: %s", targetBroker.BrokerName)
	
	// 执行故障转移
	if err := fm.executeFailover(failedBroker, targetBroker); err != nil {
		log.Printf("故障转移失败: %v", err)
		return
	}
	
	log.Printf("故障转移完成: %s -> %s", failedBroker.BrokerName, targetBroker.BrokerName)
}

// findAvailableBrokers 查找可用的 Broker
func (fm *FailoverManager) findAvailableBrokers(failedBroker *cluster.BrokerInfo) []*cluster.BrokerInfo {
	allBrokers := fm.clusterMgr.GetOnlineBrokers()
	var available []*cluster.BrokerInfo
	
	for _, broker := range allBrokers {
		if broker.BrokerName != failedBroker.BrokerName && broker.Status == cluster.ONLINE {
			available = append(available, broker)
		}
	}
	
	return available
}

// selectBestBroker 选择最佳的 Broker
func (fm *FailoverManager) selectBestBroker(brokers []*cluster.BrokerInfo) *cluster.BrokerInfo {
	if len(brokers) == 0 {
		return nil
	}
	
	// 简单选择第一个可用的 Broker
	// 实际应用中可以根据负载、延迟等指标选择
	return brokers[0]
}

// executeFailover 执行故障转移
func (fm *FailoverManager) executeFailover(from, to *cluster.BrokerInfo) error {
	log.Printf("执行故障转移: %s -> %s", from.BrokerName, to.BrokerName)
	
	// 这里可以实现具体的故障转移逻辑
	// 例如：更新路由信息、重新连接等
	
	// 模拟故障转移过程
	time.Sleep(100 * time.Millisecond)
	
	return nil
}

// TestFailover 测试故障转移
func (fm *FailoverManager) TestFailover() error {
	log.Println("开始故障转移测试...")
	
	// 发送测试消息
	for i := 0; i < 10; i++ {
		msg := &client.Message{
			Topic: "failover_test_topic",
			Body:  []byte(fmt.Sprintf("failover test message %d", i)),
		}
		
		result, err := fm.producer.SendSync(msg)
		if err != nil {
			log.Printf("发送消息失败: %v", err)
		} else {
			log.Printf("发送消息成功: %s", result.MsgId)
		}
		
		time.Sleep(500 * time.Millisecond)
	}
	
	return nil
}

// GetFailoverStats 获取故障转移统计信息
func (fm *FailoverManager) GetFailoverStats() map[string]interface{} {
	return map[string]interface{}{
		"failover_count":    fm.failoverCount,
		"running":           fm.running,
		"health_check_running": fm.healthChecker.running,
		"failed_checks":     fm.healthChecker.failedChecks,
		"last_check_time":   fm.healthChecker.lastCheckTime,
	}
}

// Stop 停止故障转移管理器
func (fm *FailoverManager) Stop() error {
	log.Println("停止故障转移管理器...")
	
	// 停止健康检查
	fm.healthChecker.running = false
	
	// 关闭生产者
	if fm.producer != nil {
		fm.producer.Shutdown()
	}
	
	// 关闭消费者
	if fm.consumer != nil {
		fm.consumer.Stop()
	}
	
	// 停止集群管理器
	fm.clusterMgr.Stop()
	
	fm.running = false
	log.Println("故障转移管理器已停止")
	return nil
}

func main() {
	// 故障转移配置
	config := &FailoverConfig{
		ClusterName:         "test-cluster",
		NameServerAddrs:     []string{"127.0.0.1:9876"},
		BrokerAddrs:         []string{"127.0.0.1:10911", "127.0.0.1:10912"},
		HealthCheckInterval: 5 * time.Second,
		FailoverThreshold:   3,
		RetryInterval:       1 * time.Second,
		MaxRetries:          5,
	}
	
	// 创建故障转移管理器
	failoverMgr, err := NewFailoverManager(config)
	if err != nil {
		log.Fatalf("创建故障转移管理器失败: %v", err)
	}
	
	// 启动故障转移管理器
	if err := failoverMgr.Start(); err != nil {
		log.Fatalf("启动故障转移管理器失败: %v", err)
	}
	defer failoverMgr.Stop()
	
	// 运行故障转移测试
	go func() {
		time.Sleep(2 * time.Second)
		if err := failoverMgr.TestFailover(); err != nil {
			log.Printf("故障转移测试失败: %v", err)
		}
	}()
	
	// 定期打印统计信息
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for i := 0; i < 6; i++ {
		select {
		case <-ticker.C:
			stats := failoverMgr.GetFailoverStats()
			log.Printf("故障转移统计: %+v", stats)
		}
	}
	
	log.Println("故障转移示例运行完成")
}