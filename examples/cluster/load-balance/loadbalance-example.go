package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
	"go-rocketmq/pkg/cluster"
)

// LoadBalanceConfig 负载均衡配置
type LoadBalanceConfig struct {
	ClusterName     string                    `yaml:"cluster_name"`
	NameServerAddrs []string                  `yaml:"nameserver_addrs"`
	BrokerAddrs     []string                  `yaml:"broker_addrs"`
	Strategy        cluster.LoadBalanceStrategy `yaml:"strategy"`
	ProducerCount   int                       `yaml:"producer_count"`
	ConsumerCount   int                       `yaml:"consumer_count"`
	TestDuration    time.Duration             `yaml:"test_duration"`
	MessageRate     int                       `yaml:"message_rate"`
}

// LoadBalanceManager 负载均衡管理器
type LoadBalanceManager struct {
	config        *LoadBalanceConfig
	clusterMgr    *cluster.ClusterManager
	loadBalancer  *cluster.LoadBalancer
	producers     []*client.Producer
	consumers     []*client.Consumer
	running       bool
	statistics    *LoadBalanceStats
	mutex         sync.RWMutex
}

// LoadBalanceStats 负载均衡统计信息
type LoadBalanceStats struct {
	TotalMessages    int64                    `json:"total_messages"`
	SuccessMessages  int64                    `json:"success_messages"`
	FailedMessages   int64                    `json:"failed_messages"`
	BrokerStats      map[string]*BrokerStats  `json:"broker_stats"`
	ProducerStats    map[string]*ProducerStats `json:"producer_stats"`
	ConsumerStats    map[string]*ConsumerStats `json:"consumer_stats"`
	StartTime        time.Time                `json:"start_time"`
	LastUpdateTime   time.Time                `json:"last_update_time"`
	mutex            sync.RWMutex
}

// BrokerStats Broker 统计信息
type BrokerStats struct {
	BrokerName      string  `json:"broker_name"`
	MessageCount    int64   `json:"message_count"`
	SuccessCount    int64   `json:"success_count"`
	FailedCount     int64   `json:"failed_count"`
	AvgLatency      float64 `json:"avg_latency"`
	Throughput      float64 `json:"throughput"`
	LastActiveTime  time.Time `json:"last_active_time"`
}

// ProducerStats 生产者统计信息
type ProducerStats struct {
	ProducerID      string  `json:"producer_id"`
	MessagesSent    int64   `json:"messages_sent"`
	SuccessCount    int64   `json:"success_count"`
	FailedCount     int64   `json:"failed_count"`
	AvgLatency      float64 `json:"avg_latency"`
	Throughput      float64 `json:"throughput"`
}

// ConsumerStats 消费者统计信息
type ConsumerStats struct {
	ConsumerID       string  `json:"consumer_id"`
	MessagesReceived int64   `json:"messages_received"`
	SuccessCount     int64   `json:"success_count"`
	FailedCount      int64   `json:"failed_count"`
	AvgLatency       float64 `json:"avg_latency"`
	Throughput       float64 `json:"throughput"`
}

// NewLoadBalanceManager 创建负载均衡管理器
func NewLoadBalanceManager(config *LoadBalanceConfig) (*LoadBalanceManager, error) {
	clusterMgr := cluster.NewClusterManager(config.ClusterName)
	loadBalancer := cluster.NewLoadBalancer(clusterMgr)
	loadBalancer.SetStrategy(config.Strategy)
	
	stats := &LoadBalanceStats{
		BrokerStats:   make(map[string]*BrokerStats),
		ProducerStats: make(map[string]*ProducerStats),
		ConsumerStats: make(map[string]*ConsumerStats),
		StartTime:     time.Now(),
	}
	
	return &LoadBalanceManager{
		config:       config,
		clusterMgr:   clusterMgr,
		loadBalancer: loadBalancer,
		producers:    make([]*client.Producer, 0),
		consumers:    make([]*client.Consumer, 0),
		running:      false,
		statistics:   stats,
	}, nil
}

// Start 启动负载均衡管理器
func (lbm *LoadBalanceManager) Start() error {
	log.Println("启动负载均衡管理器...")
	
	// 启动集群管理器
	if err := lbm.clusterMgr.Start(); err != nil {
		return fmt.Errorf("启动集群管理器失败: %v", err)
	}
	
	// 创建生产者
	if err := lbm.createProducers(); err != nil {
		return fmt.Errorf("创建生产者失败: %v", err)
	}
	
	// 创建消费者
	if err := lbm.createConsumers(); err != nil {
		return fmt.Errorf("创建消费者失败: %v", err)
	}
	
	// 启动统计收集
	go lbm.startStatsCollection()
	
	lbm.running = true
	log.Println("负载均衡管理器启动完成")
	return nil
}

// createProducers 创建生产者
func (lbm *LoadBalanceManager) createProducers() error {
	log.Printf("创建 %d 个生产者...", lbm.config.ProducerCount)
	
	for i := 0; i < lbm.config.ProducerCount; i++ {
		producer := client.NewProducer(fmt.Sprintf("loadbalance_producer_group_%d", i))
		producer.SetNameServers(lbm.config.NameServerAddrs)
		
		if err := producer.Start(); err != nil {
			return fmt.Errorf("启动生产者 %d 失败: %v", i, err)
		}
		
		lbm.producers = append(lbm.producers, producer)
		
		// 初始化生产者统计信息
		producerID := fmt.Sprintf("producer_%d", i)
		lbm.statistics.ProducerStats[producerID] = &ProducerStats{
			ProducerID: producerID,
		}
	}
	
	return nil
}

// createConsumers 创建消费者
func (lbm *LoadBalanceManager) createConsumers() error {
	log.Printf("创建 %d 个消费者...", lbm.config.ConsumerCount)
	
	for i := 0; i < lbm.config.ConsumerCount; i++ {
		consumerConfig := &client.ConsumerConfig{
			GroupName:        fmt.Sprintf("loadbalance_consumer_group_%d", i),
			NameServerAddr:   lbm.config.NameServerAddrs[0],
			ConsumeFromWhere: client.ConsumeFromLastOffset,
			MessageModel:     client.Clustering,
		}
		
		consumer := client.NewConsumer(consumerConfig)
		
		if err := consumer.Start(); err != nil {
			return fmt.Errorf("启动消费者 %d 失败: %v", i, err)
		}
		
		lbm.consumers = append(lbm.consumers, consumer)
		
		// 初始化消费者统计信息
		consumerID := fmt.Sprintf("consumer_%d", i)
		lbm.statistics.ConsumerStats[consumerID] = &ConsumerStats{
			ConsumerID: consumerID,
		}
	}
	
	return nil
}

// startStatsCollection 启动统计信息收集
func (lbm *LoadBalanceManager) startStatsCollection() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for lbm.running {
		select {
		case <-ticker.C:
			lbm.collectStats()
		}
	}
}

// collectStats 收集统计信息
func (lbm *LoadBalanceManager) collectStats() {
	lbm.statistics.mutex.Lock()
	defer lbm.statistics.mutex.Unlock()
	
	// 更新 Broker 统计信息
	brokers := lbm.clusterMgr.GetOnlineBrokers()
	for _, broker := range brokers {
		if _, exists := lbm.statistics.BrokerStats[broker.BrokerName]; !exists {
			lbm.statistics.BrokerStats[broker.BrokerName] = &BrokerStats{
				BrokerName: broker.BrokerName,
			}
		}
		
		brokerStats := lbm.statistics.BrokerStats[broker.BrokerName]
		brokerStats.LastActiveTime = time.Now()
		
		// 这里可以添加更多的统计信息收集逻辑
		if broker.Metrics != nil {
			brokerStats.Throughput = float64(broker.Metrics.Tps)
		}
	}
	
	lbm.statistics.LastUpdateTime = time.Now()
}

// RunLoadBalanceTest 运行负载均衡测试
func (lbm *LoadBalanceManager) RunLoadBalanceTest() error {
	log.Println("开始负载均衡测试...")
	
	// 创建测试上下文
	ctx, cancel := context.WithTimeout(context.Background(), lbm.config.TestDuration)
	defer cancel()
	
	// 启动生产者测试
	var wg sync.WaitGroup
	for i, producer := range lbm.producers {
		wg.Add(1)
		go func(producerIndex int, p *client.Producer) {
			defer wg.Done()
			lbm.runProducerTest(ctx, producerIndex, p)
		}(i, producer)
	}
	
	// 启动消费者测试
	for i, consumer := range lbm.consumers {
		wg.Add(1)
		go func(consumerIndex int, c *client.Consumer) {
			defer wg.Done()
			lbm.runConsumerTest(ctx, consumerIndex, c)
		}(i, consumer)
	}
	
	// 等待测试完成
	wg.Wait()
	
	log.Println("负载均衡测试完成")
	return nil
}

// runProducerTest 运行生产者测试
func (lbm *LoadBalanceManager) runProducerTest(ctx context.Context, producerIndex int, producer *client.Producer) {
	producerID := fmt.Sprintf("producer_%d", producerIndex)
	log.Printf("启动生产者测试: %s", producerID)
	
	ticker := time.NewTicker(time.Second / time.Duration(lbm.config.MessageRate))
	defer ticker.Stop()
	
	messageCount := 0
	for {
		select {
		case <-ctx.Done():
			log.Printf("生产者 %s 测试结束，发送消息数: %d", producerID, messageCount)
			return
		case <-ticker.C:
			messageCount++
			msg := &client.Message{
				Topic: "loadbalance_test_topic",
				Body:  []byte(fmt.Sprintf("message from %s #%d", producerID, messageCount)),
				Tags:  "loadbalance",
			}
			
			start := time.Now()
			result, err := producer.SendSync(msg)
			latency := time.Since(start)
			
			// 更新统计信息
			lbm.updateProducerStats(producerID, err == nil, latency)
			
			if err != nil {
				log.Printf("生产者 %s 发送消息失败: %v", producerID, err)
			} else {
				log.Printf("生产者 %s 发送消息成功: %s, 延迟: %v", producerID, result.MsgId, latency)
			}
		}
	}
}

// runConsumerTest 运行消费者测试
func (lbm *LoadBalanceManager) runConsumerTest(ctx context.Context, consumerIndex int, consumer *client.Consumer) {
	consumerID := fmt.Sprintf("consumer_%d", consumerIndex)
	log.Printf("启动消费者测试: %s", consumerID)
	
	// 这里应该实现消费者的消息处理逻辑
	// 由于当前的 Consumer 接口可能不同，这里只是示例
	
	for {
		select {
		case <-ctx.Done():
			log.Printf("消费者 %s 测试结束", consumerID)
			return
		default:
			// 模拟消息消费
			time.Sleep(100 * time.Millisecond)
			
			// 随机模拟消费成功/失败
			success := rand.Float32() > 0.1 // 90% 成功率
			latency := time.Duration(rand.Intn(100)) * time.Millisecond
			
			// 更新统计信息
			lbm.updateConsumerStats(consumerID, success, latency)
		}
	}
}

// updateProducerStats 更新生产者统计信息
func (lbm *LoadBalanceManager) updateProducerStats(producerID string, success bool, latency time.Duration) {
	lbm.statistics.mutex.Lock()
	defer lbm.statistics.mutex.Unlock()
	
	stats := lbm.statistics.ProducerStats[producerID]
	stats.MessagesSent++
	
	if success {
		stats.SuccessCount++
		lbm.statistics.SuccessMessages++
	} else {
		stats.FailedCount++
		lbm.statistics.FailedMessages++
	}
	
	lbm.statistics.TotalMessages++
	
	// 更新平均延迟
	if stats.MessagesSent > 0 {
		stats.AvgLatency = (stats.AvgLatency*float64(stats.MessagesSent-1) + float64(latency.Milliseconds())) / float64(stats.MessagesSent)
	}
	
	// 计算吞吐量 (消息/秒)
	duration := time.Since(lbm.statistics.StartTime).Seconds()
	if duration > 0 {
		stats.Throughput = float64(stats.MessagesSent) / duration
	}
}

// updateConsumerStats 更新消费者统计信息
func (lbm *LoadBalanceManager) updateConsumerStats(consumerID string, success bool, latency time.Duration) {
	lbm.statistics.mutex.Lock()
	defer lbm.statistics.mutex.Unlock()
	
	stats := lbm.statistics.ConsumerStats[consumerID]
	stats.MessagesReceived++
	
	if success {
		stats.SuccessCount++
	} else {
		stats.FailedCount++
	}
	
	// 更新平均延迟
	if stats.MessagesReceived > 0 {
		stats.AvgLatency = (stats.AvgLatency*float64(stats.MessagesReceived-1) + float64(latency.Milliseconds())) / float64(stats.MessagesReceived)
	}
	
	// 计算吞吐量
	duration := time.Since(lbm.statistics.StartTime).Seconds()
	if duration > 0 {
		stats.Throughput = float64(stats.MessagesReceived) / duration
	}
}

// GetLoadBalanceStats 获取负载均衡统计信息
func (lbm *LoadBalanceManager) GetLoadBalanceStats() *LoadBalanceStats {
	lbm.statistics.mutex.RLock()
	defer lbm.statistics.mutex.RUnlock()
	
	// 返回统计信息的副本
	stats := &LoadBalanceStats{
		TotalMessages:   lbm.statistics.TotalMessages,
		SuccessMessages: lbm.statistics.SuccessMessages,
		FailedMessages:  lbm.statistics.FailedMessages,
		BrokerStats:     make(map[string]*BrokerStats),
		ProducerStats:   make(map[string]*ProducerStats),
		ConsumerStats:   make(map[string]*ConsumerStats),
		StartTime:       lbm.statistics.StartTime,
		LastUpdateTime:  lbm.statistics.LastUpdateTime,
	}
	
	// 复制 Broker 统计信息
	for k, v := range lbm.statistics.BrokerStats {
		stats.BrokerStats[k] = &BrokerStats{
			BrokerName:     v.BrokerName,
			MessageCount:   v.MessageCount,
			SuccessCount:   v.SuccessCount,
			FailedCount:    v.FailedCount,
			AvgLatency:     v.AvgLatency,
			Throughput:     v.Throughput,
			LastActiveTime: v.LastActiveTime,
		}
	}
	
	// 复制生产者统计信息
	for k, v := range lbm.statistics.ProducerStats {
		stats.ProducerStats[k] = &ProducerStats{
			ProducerID:   v.ProducerID,
			MessagesSent: v.MessagesSent,
			SuccessCount: v.SuccessCount,
			FailedCount:  v.FailedCount,
			AvgLatency:   v.AvgLatency,
			Throughput:   v.Throughput,
		}
	}
	
	// 复制消费者统计信息
	for k, v := range lbm.statistics.ConsumerStats {
		stats.ConsumerStats[k] = &ConsumerStats{
			ConsumerID:       v.ConsumerID,
			MessagesReceived: v.MessagesReceived,
			SuccessCount:     v.SuccessCount,
			FailedCount:      v.FailedCount,
			AvgLatency:       v.AvgLatency,
			Throughput:       v.Throughput,
		}
	}
	
	return stats
}

// PrintStats 打印统计信息
func (lbm *LoadBalanceManager) PrintStats() {
	stats := lbm.GetLoadBalanceStats()
	
	log.Println("=== 负载均衡统计信息 ===")
	log.Printf("总消息数: %d", stats.TotalMessages)
	log.Printf("成功消息数: %d", stats.SuccessMessages)
	log.Printf("失败消息数: %d", stats.FailedMessages)
	log.Printf("成功率: %.2f%%", float64(stats.SuccessMessages)/float64(stats.TotalMessages)*100)
	
	log.Println("\n=== Broker 统计 ===")
	for _, brokerStats := range stats.BrokerStats {
		log.Printf("Broker: %s, 吞吐量: %.2f msg/s, 最后活跃时间: %s",
			brokerStats.BrokerName,
			brokerStats.Throughput,
			brokerStats.LastActiveTime.Format("15:04:05"))
	}
	
	log.Println("\n=== 生产者统计 ===")
	for _, producerStats := range stats.ProducerStats {
		log.Printf("生产者: %s, 发送: %d, 成功: %d, 失败: %d, 平均延迟: %.2fms, 吞吐量: %.2f msg/s",
			producerStats.ProducerID,
			producerStats.MessagesSent,
			producerStats.SuccessCount,
			producerStats.FailedCount,
			producerStats.AvgLatency,
			producerStats.Throughput)
	}
	
	log.Println("\n=== 消费者统计 ===")
	for _, consumerStats := range stats.ConsumerStats {
		log.Printf("消费者: %s, 接收: %d, 成功: %d, 失败: %d, 平均延迟: %.2fms, 吞吐量: %.2f msg/s",
			consumerStats.ConsumerID,
			consumerStats.MessagesReceived,
			consumerStats.SuccessCount,
			consumerStats.FailedCount,
			consumerStats.AvgLatency,
			consumerStats.Throughput)
	}
}

// Stop 停止负载均衡管理器
func (lbm *LoadBalanceManager) Stop() error {
	log.Println("停止负载均衡管理器...")
	
	lbm.running = false
	
	// 关闭所有生产者
	for i, producer := range lbm.producers {
		log.Printf("关闭生产者 %d", i)
		producer.Shutdown()
	}
	
	// 关闭所有消费者
	for i, consumer := range lbm.consumers {
		log.Printf("关闭消费者 %d", i)
		consumer.Stop()
	}
	
	// 停止集群管理器
	lbm.clusterMgr.Stop()
	
	log.Println("负载均衡管理器已停止")
	return nil
}

func main() {
	// 负载均衡配置
	config := &LoadBalanceConfig{
		ClusterName:     "loadbalance-cluster",
		NameServerAddrs: []string{"127.0.0.1:9876"},
		BrokerAddrs:     []string{"127.0.0.1:10911", "127.0.0.1:10912", "127.0.0.1:10913"},
		Strategy:        cluster.ROUND_ROBIN,
		ProducerCount:   3,
		ConsumerCount:   2,
		TestDuration:    30 * time.Second,
		MessageRate:     10, // 每秒10条消息
	}
	
	// 创建负载均衡管理器
	lbMgr, err := NewLoadBalanceManager(config)
	if err != nil {
		log.Fatalf("创建负载均衡管理器失败: %v", err)
	}
	
	// 启动负载均衡管理器
	if err := lbMgr.Start(); err != nil {
		log.Fatalf("启动负载均衡管理器失败: %v", err)
	}
	defer lbMgr.Stop()
	
	// 运行负载均衡测试
	go func() {
		time.Sleep(2 * time.Second)
		if err := lbMgr.RunLoadBalanceTest(); err != nil {
			log.Printf("负载均衡测试失败: %v", err)
		}
	}()
	
	// 定期打印统计信息
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for i := 0; i < 4; i++ {
		select {
		case <-ticker.C:
			lbMgr.PrintStats()
		}
	}
	
	log.Println("负载均衡示例运行完成")
}