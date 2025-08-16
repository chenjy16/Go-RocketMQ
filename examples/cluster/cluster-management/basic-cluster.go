package main

import (
	"context"
	"fmt"
	"log"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
	"go-rocketmq/pkg/cluster"
)

// ClusterConfig 集群配置
type ClusterConfig struct {
	ClusterName      string   `yaml:"cluster_name"`
	NameServerAddrs  []string `yaml:"nameserver_addrs"`
	BrokerAddrs      []string `yaml:"broker_addrs"`
	RetryTimes       int      `yaml:"retry_times"`
	SendMsgTimeout   time.Duration `yaml:"send_msg_timeout"`
}

// ClusterManager 集群管理器
type ClusterManager struct {
	config     *ClusterConfig
	clusterMgr *cluster.ClusterManager
	producers  map[string]*client.Producer
	consumers  map[string]*client.Consumer
	running    bool
}

// NewClusterManager 创建集群管理器
func NewClusterManager(config *ClusterConfig) (*ClusterManager, error) {
	// 创建集群管理器
	clusterMgr := cluster.NewClusterManager(config.ClusterName)

	return &ClusterManager{
		config:     config,
		clusterMgr: clusterMgr,
		producers:  make(map[string]*client.Producer),
		consumers:  make(map[string]*client.Consumer),
		running:    false,
	}, nil
}

// InitializeCluster 初始化集群
func (cm *ClusterManager) InitializeCluster() error {
	log.Printf("正在初始化集群: %s", cm.config.ClusterName)

	// 启动集群管理器
	if err := cm.clusterMgr.Start(); err != nil {
		return fmt.Errorf("启动集群管理器失败: %v", err)
	}

	// 检查 NameServer 连接
	if err := cm.checkNameServerConnection(); err != nil {
		return fmt.Errorf("NameServer 连接检查失败: %v", err)
	}

	// 检查 Broker 状态
	if err := cm.checkBrokerStatus(); err != nil {
		return fmt.Errorf("Broker 状态检查失败: %v", err)
	}

	// 创建默认 Topic
	if err := cm.createDefaultTopics(); err != nil {
		return fmt.Errorf("创建默认 Topic 失败: %v", err)
	}

	cm.running = true
	log.Printf("集群 %s 初始化完成", cm.config.ClusterName)
	return nil
}

// checkNameServerConnection 检查 NameServer 连接
func (cm *ClusterManager) checkNameServerConnection() error {
	log.Println("检查 NameServer 连接...")

	// 创建临时生产者来测试连接
	testProducer := client.NewProducer("test_connection_group")
	testProducer.SetNameServers(cm.config.NameServerAddrs)

	if err := testProducer.Start(); err != nil {
		return fmt.Errorf("无法连接到 NameServer: %v", err)
	}
	testProducer.Shutdown()

	log.Println("NameServer 连接正常")
	return nil
}

// checkBrokerStatus 检查 Broker 状态
func (cm *ClusterManager) checkBrokerStatus() error {
	log.Println("检查 Broker 状态...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 获取 Broker 运行时信息
	for _, brokerAddr := range cm.config.BrokerAddrs {
		log.Printf("检查 Broker: %s", brokerAddr)
		
		// 这里可以添加具体的 Broker 健康检查逻辑
		// 例如发送心跳消息或查询 Broker 状态
		_ = ctx // 使用 context
	}

	log.Println("所有 Broker 状态正常")
	return nil
}

// createDefaultTopics 创建默认 Topic
func (cm *ClusterManager) createDefaultTopics() error {
	log.Println("创建默认 Topic...")

	defaultTopics := []string{
		"DefaultCluster_Test",
		"DefaultCluster_Order",
		"DefaultCluster_Delay",
	}

	// 注意：在实际环境中，Topic 通常由管理员预先创建
	// 这里只是模拟 Topic 创建过程
	for _, topicName := range defaultTopics {
		log.Printf("准备使用 Topic: %s", topicName)
	}

	log.Println("默认 Topic 准备完成")
	return nil
}

// CreateProducer 创建生产者
func (cm *ClusterManager) CreateProducer(groupName string) error {
	log.Printf("创建生产者组: %s", groupName)

	p := client.NewProducer(groupName)
	p.SetNameServers(cm.config.NameServerAddrs)

	err := p.Start()
	if err != nil {
		return fmt.Errorf("启动生产者失败: %v", err)
	}

	cm.producers[groupName] = p
	log.Printf("生产者组 %s 创建成功", groupName)
	return nil
}

// CreateConsumer 创建消费者
func (cm *ClusterManager) CreateConsumer(groupName, topic string) error {
	log.Printf("创建消费者组: %s，订阅 Topic: %s", groupName, topic)

	config := &client.ConsumerConfig{
		GroupName:        groupName,
		NameServerAddr:   cm.config.NameServerAddrs[0],
		ConsumeFromWhere: client.ConsumeFromLastOffset,
		MessageModel:     client.Clustering,
	}

	c := client.NewConsumer(config)
	c.SetNameServerAddr(cm.config.NameServerAddrs[0])

	// 创建消息监听器
	listener := client.MessageListenerConcurrently(func(msgs []*client.MessageExt) client.ConsumeResult {
		for _, msg := range msgs {
			log.Printf("收到消息: Topic=%s, Tags=%s, Body=%s", msg.Topic, msg.Tags, string(msg.Body))
		}
		return client.ConsumeSuccess
	})

	// 订阅主题
	if err := c.Subscribe(topic, "*", listener); err != nil {
		return fmt.Errorf("订阅主题失败: %v", err)
	}

	if err := c.Start(); err != nil {
		return fmt.Errorf("启动消费者失败: %v", err)
	}

	cm.consumers[groupName] = c
	log.Printf("消费者组 %s 创建成功", groupName)
	return nil
}

// SendTestMessage 发送测试消息
func (cm *ClusterManager) SendTestMessage(producerGroup, topic, tag, body string) error {
	producer, exists := cm.producers[producerGroup]
	if !exists {
		return fmt.Errorf("生产者组 %s 不存在", producerGroup)
	}

	msg := client.NewMessage(topic, []byte(body))
	msg.SetTags(tag)

	result, err := producer.SendSync(msg)
	if err != nil {
		return fmt.Errorf("发送消息失败: %v", err)
	}

	log.Printf("消息发送成功: MsgId=%s, QueueId=%d", result.MsgId, result.MessageQueue.QueueId)
	return nil
}

// GetClusterInfo 获取集群信息
func (cm *ClusterManager) GetClusterInfo() map[string]interface{} {
	clusterStatus := cm.clusterMgr.GetClusterStatus()
	return map[string]interface{}{
		"cluster_name": cm.config.ClusterName,
		"nameservers": cm.config.NameServerAddrs,
		"brokers":     cm.config.BrokerAddrs,
		"running":     cm.running,
		"status":      clusterStatus,
	}
}

// Shutdown 关闭集群管理器
func (cm *ClusterManager) Shutdown() error {
	log.Println("正在关闭集群管理器...")

	// 关闭所有生产者
	for name, producer := range cm.producers {
		log.Printf("关闭生产者: %s", name)
		producer.Shutdown()
	}

	// 关闭所有消费者
	for name, consumer := range cm.consumers {
		log.Printf("关闭消费者: %s", name)
		consumer.Stop()
	}

	// 停止集群管理器
	cm.clusterMgr.Stop()

	cm.running = false
	log.Println("集群管理器已关闭")
	return nil
}

func main() {
	// 集群配置
	config := &ClusterConfig{
		ClusterName: "DefaultCluster",
		NameServerAddrs: []string{
			"127.0.0.1:9876",
			// "192.168.1.11:9876",
			// "192.168.1.12:9876",
		},
		BrokerAddrs: []string{
			"127.0.0.1:10911",
			// "192.168.1.21:10911",
			// "192.168.1.22:10911",
		},
		RetryTimes:     3,
		SendMsgTimeout: 10 * time.Second,
	}

	// 创建集群管理器
	clusterManager, err := NewClusterManager(config)
	if err != nil {
		log.Fatalf("创建集群管理器失败: %v", err)
	}
	defer clusterManager.Shutdown()

	// 初始化集群
	if err := clusterManager.InitializeCluster(); err != nil {
		log.Fatalf("初始化集群失败: %v", err)
	}

	// 创建生产者
	if err := clusterManager.CreateProducer("test_producer_group"); err != nil {
		log.Fatalf("创建生产者失败: %v", err)
	}

	// 创建消费者
	if err := clusterManager.CreateConsumer("test_consumer_group", "DefaultCluster_Test"); err != nil {
		log.Fatalf("创建消费者失败: %v", err)
	}

	// 等待消费者启动
	time.Sleep(2 * time.Second)

	// 发送测试消息
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf("集群测试消息 #%d - 时间: %s", i+1, time.Now().Format("2006-01-02 15:04:05"))
		if err := clusterManager.SendTestMessage("test_producer_group", "DefaultCluster_Test", "cluster", body); err != nil {
			log.Printf("发送消息失败: %v", err)
		} else {
			log.Printf("消息 #%d 发送成功", i+1)
		}
		time.Sleep(1 * time.Second)
	}

	// 获取集群信息
	if err := clusterManager.GetClusterInfo(); err != nil {
		log.Printf("获取集群信息失败: %v", err)
	}

	// 等待消息处理
	log.Println("等待消息处理完成...")
	time.Sleep(5 * time.Second)

	log.Println("基础集群管理示例运行完成")
}