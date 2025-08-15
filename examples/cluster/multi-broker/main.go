package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/chenjy16/go-rocketmq-client"
)

// 集群配置
type ClusterConfig struct {
	NameServers []string
	Brokers     []BrokerInfo
}

type BrokerInfo struct {
	Name    string
	Address string
	Role    string // MASTER, SLAVE
}

func main() {
	fmt.Println("=== Go-RocketMQ 多Broker集群示例 ===")
	fmt.Println("本示例演示如何在多Broker集群环境中进行消息生产和消费")

	// 集群配置
	clusterConfig := &ClusterConfig{
		NameServers: []string{
			"127.0.0.1:9876",
			// "127.0.0.1:9877", // 可以配置多个NameServer
		},
		Brokers: []BrokerInfo{
			{Name: "broker-a", Address: "127.0.0.1:10911", Role: "MASTER"},
			{Name: "broker-a-s", Address: "127.0.0.1:10921", Role: "SLAVE"},
			{Name: "broker-b", Address: "127.0.0.1:10912", Role: "MASTER"},
			{Name: "broker-b-s", Address: "127.0.0.1:10922", Role: "SLAVE"},
		},
	}

	fmt.Printf("集群配置:\n")
	fmt.Printf("  NameServers: %v\n", clusterConfig.NameServers)
	fmt.Printf("  Brokers: %d个\n", len(clusterConfig.Brokers))
	for _, broker := range clusterConfig.Brokers {
		fmt.Printf("    - %s (%s): %s\n", broker.Name, broker.Role, broker.Address)
	}

	var wg sync.WaitGroup

	// 启动多个消费者（模拟集群消费）
	fmt.Println("\n--- 启动集群消费者 ---")
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(consumerIndex int) {
			defer wg.Done()
			startClusterConsumer(clusterConfig, consumerIndex)
		}(i)
	}

	// 等待消费者启动
	time.Sleep(3 * time.Second)

	// 启动多个生产者（模拟负载均衡）
	fmt.Println("\n--- 启动集群生产者 ---")
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(producerIndex int) {
			defer wg.Done()
			startClusterProducer(clusterConfig, producerIndex)
		}(i)
	}

	wg.Wait()
	fmt.Println("\n集群示例完成")
}

// 启动集群生产者
func startClusterProducer(config *ClusterConfig, producerIndex int) {
	producerGroup := fmt.Sprintf("cluster_producer_group_%d", producerIndex)
	fmt.Printf("\n[生产者-%d] 启动集群生产者: %s\n", producerIndex, producerGroup)

	// 创建生产者
	producer := client.NewProducer(producerGroup)
	producer.SetNameServers(config.NameServers)

	if err := producer.Start(); err != nil {
		log.Printf("[生产者-%d] 启动失败: %v", producerIndex, err)
		return
	}
	defer producer.Shutdown()

	fmt.Printf("[生产者-%d] 启动成功\n", producerIndex)

	// 发送消息到不同的Topic
	topics := []string{"ClusterTopicA", "ClusterTopicB", "ClusterTopicC"}

	for round := 0; round < 3; round++ {
		for topicIndex, topic := range topics {
			// 发送消息
			msg := client.NewMessage(
				topic,
				[]byte(fmt.Sprintf("集群消息 - Producer: %d, Topic: %s, Round: %d, Time: %s",
					producerIndex, topic, round+1, time.Now().Format("15:04:05"))),
			).SetTags(fmt.Sprintf("TAG_%d", topicIndex)).SetKeys(fmt.Sprintf("KEY_P%d_R%d_T%d", producerIndex, round+1, topicIndex)).SetProperty("producerId", fmt.Sprintf("%d", producerIndex)).SetProperty("round", fmt.Sprintf("%d", round+1)).SetProperty("topicIndex", fmt.Sprintf("%d", topicIndex))

			result, err := producer.SendSync(msg)
			if err != nil {
				log.Printf("[生产者-%d] 发送消息失败: %v", producerIndex, err)
				continue
			}

			fmt.Printf("[生产者-%d] 消息发送成功 - Topic: %s, MsgId: %s, Queue: %d\n",
				producerIndex, topic, result.MsgId, result.MessageQueue.QueueId)

			time.Sleep(200 * time.Millisecond)
		}
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Printf("[生产者-%d] 消息发送完成\n", producerIndex)
	time.Sleep(5 * time.Second) // 等待消费完成
}

// 启动集群消费者
func startClusterConsumer(config *ClusterConfig, consumerIndex int) {
	consumerGroup := "cluster_consumer_group" // 同一个消费者组实现负载均衡
	fmt.Printf("\n[消费者-%d] 启动集群消费者: %s\n", consumerIndex, consumerGroup)

	// 创建消费者配置
	consumerConfig := &client.ConsumerConfig{
		GroupName:        consumerGroup,
		NameServerAddr:   config.NameServers[0], // 使用第一个NameServer
		ConsumeFromWhere: client.ConsumeFromLastOffset,
		MessageModel:     client.Clustering, // 集群模式
		ConsumeThreadMin: 2,
		ConsumeThreadMax: 4,
		PullInterval:     100 * time.Millisecond,
		PullBatchSize:    16,
		ConsumeTimeout:   15 * time.Second,
	}

	// 创建消费者
	consumer := client.NewConsumer(consumerConfig)

	// 订阅多个Topic
	topics := []string{"ClusterTopicA", "ClusterTopicB", "ClusterTopicC"}
	for _, topic := range topics {
		err := consumer.Subscribe(topic, "*", &ClusterMessageListener{
			ConsumerIndex: consumerIndex,
			Topic:         topic,
		})
		if err != nil {
			log.Printf("[消费者-%d] 订阅Topic %s失败: %v", consumerIndex, topic, err)
			continue
		}
		fmt.Printf("[消费者-%d] 订阅Topic成功: %s\n", consumerIndex, topic)
	}

	if err := consumer.Start(); err != nil {
		log.Printf("[消费者-%d] 启动失败: %v", consumerIndex, err)
		return
	}
	defer consumer.Stop()

	fmt.Printf("[消费者-%d] 启动成功\n", consumerIndex)

	// 运行20秒
	time.Sleep(20 * time.Second)
	fmt.Printf("[消费者-%d] 停止消费\n", consumerIndex)
}

// 集群消息监听器
type ClusterMessageListener struct {
	ConsumerIndex int
	Topic         string
	messageCount  int64
	mutex         sync.Mutex
}

func (l *ClusterMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	for _, msg := range msgs {
		l.messageCount++

		fmt.Printf("\n[消费者-%d] 收到集群消息:\n", l.ConsumerIndex)
		fmt.Printf("  Topic: %s\n", msg.Topic)
		fmt.Printf("  Tags: %s\n", msg.Tags)
		fmt.Printf("  Keys: %s\n", msg.Keys)
		fmt.Printf("  MsgId: %s\n", msg.MsgId)
		fmt.Printf("  QueueId: %d, Offset: %d\n", msg.QueueId, msg.QueueOffset)
		fmt.Printf("  消息内容: %s\n", string(msg.Body))
		fmt.Printf("  消费者统计: 第%d条消息\n", l.messageCount)

		// 打印消息属性
		if len(msg.Properties) > 0 {
			fmt.Printf("  消息属性: ")
			for key, value := range msg.Properties {
				fmt.Printf("%s=%s ", key, value)
			}
			fmt.Println()
		}

		// 模拟不同的业务处理逻辑
		l.processClusterMessage(msg)

		// 模拟处理时间
		time.Sleep(50 * time.Millisecond)
	}

	return client.ConsumeSuccess
}

// 处理集群消息
func (l *ClusterMessageListener) processClusterMessage(msg *client.MessageExt) {
	producerId := msg.GetProperty("producerId")
	round := msg.GetProperty("round")
	topicIndex := msg.GetProperty("topicIndex")

	fmt.Printf("  [业务处理] 消费者-%d处理消息 - 来自生产者%s, 轮次%s, Topic索引%s\n",
		l.ConsumerIndex, producerId, round, topicIndex)

	// 根据Topic进行不同的业务处理
	switch msg.Topic {
	case "ClusterTopicA":
		fmt.Printf("  [业务处理] 处理TopicA消息 - 用户行为分析\n")
	case "ClusterTopicB":
		fmt.Printf("  [业务处理] 处理TopicB消息 - 订单处理\n")
	case "ClusterTopicC":
		fmt.Printf("  [业务处理] 处理TopicC消息 - 系统监控\n")
	default:
		fmt.Printf("  [业务处理] 处理未知Topic消息\n")
	}

	// 模拟负载均衡效果
	fmt.Printf("  [负载均衡] 消息被消费者-%d处理，实现了集群负载分担\n", l.ConsumerIndex)
}

// 集群健康检查
func checkClusterHealth(config *ClusterConfig) {
	fmt.Println("\n--- 集群健康检查 ---")

	// 检查NameServer连接
	for i, ns := range config.NameServers {
		fmt.Printf("检查NameServer-%d: %s\n", i+1, ns)
		// 这里应该实现实际的连接检查
		fmt.Printf("  状态: 正常\n")
	}

	// 检查Broker连接
	for i, broker := range config.Brokers {
		fmt.Printf("检查Broker-%d: %s (%s)\n", i+1, broker.Name, broker.Role)
		fmt.Printf("  地址: %s\n", broker.Address)
		// 这里应该实现实际的连接检查
		fmt.Printf("  状态: 正常\n")
	}

	fmt.Println("集群健康检查完成")
}