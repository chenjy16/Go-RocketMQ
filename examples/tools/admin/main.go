package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chenjy16/go-rocketmq-client"
)

// 管理工具配置
type AdminConfig struct {
	NameServerAddr string
	Timeout        time.Duration
}

// 管理客户端
type AdminClient struct {
	config   *AdminConfig
	producer *client.Producer
	consumer *client.Consumer
}

// MessageTraceCollector 消息追踪数据收集器
type MessageTraceCollector struct {
	msgId  string
	traces []*client.TraceContext
}

// TraceData 追踪数据结构
type TraceData struct {
	MsgId     string    `json:"msgId"`
	Topic     string    `json:"topic"`
	Tags      string    `json:"tags"`
	Keys      string    `json:"keys"`
	QueueId   int       `json:"queueId"`
	Offset    int64     `json:"offset"`
	Size      int       `json:"size"`
	SendTime  time.Time `json:"sendTime"`
	StoreTime time.Time `json:"storeTime"`
	RetryTimes int      `json:"retryTimes"`
	Body      string    `json:"body"`
}

// ClusterInfo 集群信息结构
type ClusterInfo struct {
	BrokerAddrTable  map[string]map[int64]string `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string         `json:"clusterAddrTable"`
}

// BrokerInfo Broker信息结构
type BrokerInfo struct {
	BrokerName string `json:"brokerName"`
	BrokerId   int64  `json:"brokerId"`
	Address    string `json:"address"`
	Role       string `json:"role"`
	Status     string `json:"status"`
	Cluster    string `json:"cluster"`
}

// SystemStats 系统统计信息
type SystemStats struct {
	CPUUsage     float64 `json:"cpu_usage"`
	MemoryUsage  uint64  `json:"memory_usage"`
	MemoryTotal  uint64  `json:"memory_total"`
	Goroutines   int     `json:"goroutines"`
	GCCount      uint32  `json:"gc_count"`
	DiskUsage    float64 `json:"disk_usage"`
	NetworkIn    uint64  `json:"network_in"`
	NetworkOut   uint64  `json:"network_out"`
	Timestamp    time.Time `json:"timestamp"`
}

// MessageStats 消息统计信息
type MessageStats struct {
	SentCount     int64   `json:"sent_count"`
	ReceivedCount int64   `json:"received_count"`
	ErrorCount    int64   `json:"error_count"`
	SendTPS       float64 `json:"send_tps"`
	ReceiveTPS    float64 `json:"receive_tps"`
	AvgLatency    float64 `json:"avg_latency"`
	P99Latency    float64 `json:"p99_latency"`
}

func main() {
	fmt.Println("=== Go-RocketMQ 管理工具 ===")
	fmt.Println("本工具提供Go-RocketMQ集群的管理和监控功能")

	if len(os.Args) < 2 {
		printUsage()
		return
	}

	// 创建管理客户端
	adminClient := &AdminClient{
		config: &AdminConfig{
			NameServerAddr: "127.0.0.1:9876",
			Timeout:        30 * time.Second,
		},
	}

	// 解析命令
	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "cluster":
		handleClusterCommand(adminClient, args)
	case "topic":
		handleTopicCommand(adminClient, args)
	case "consumer":
		handleConsumerCommand(adminClient, args)
	case "message":
		handleMessageCommand(adminClient, args)
	case "monitor":
		handleMonitorCommand(adminClient, args)
	case "test":
		handleTestCommand(adminClient, args)
	default:
		fmt.Printf("未知命令: %s\n", command)
		printUsage()
	}
}

// 打印使用说明
func printUsage() {
	fmt.Println("\n使用方法:")
	fmt.Println("  go run main.go <command> [options]")
	fmt.Println("\n可用命令:")
	fmt.Println("  cluster  - 集群管理")
	fmt.Println("    list                    # 列出集群信息")
	fmt.Println("    status                  # 查看集群状态")
	fmt.Println("    brokers                 # 列出所有Broker")
	fmt.Println("\n  topic    - Topic管理")
	fmt.Println("    list                    # 列出所有Topic")
	fmt.Println("    create <name> [queues]  # 创建Topic")
	fmt.Println("    delete <name>           # 删除Topic")
	fmt.Println("    info <name>             # 查看Topic信息")
	fmt.Println("\n  consumer - 消费者管理")
	fmt.Println("    list                    # 列出消费者组")
	fmt.Println("    progress <group>        # 查看消费进度")
	fmt.Println("    reset <group> <topic>   # 重置消费位点")
	fmt.Println("\n  message  - 消息管理")
	fmt.Println("    send <topic> <content>  # 发送测试消息")
	fmt.Println("    query <msgId>           # 查询消息")
	fmt.Println("    trace <msgId>           # 追踪消息")
	fmt.Println("\n  monitor  - 监控")
	fmt.Println("    stats                   # 显示统计信息")
	fmt.Println("    health                  # 健康检查")
	fmt.Println("    watch                   # 实时监控")
	fmt.Println("\n  test     - 测试工具")
	fmt.Println("    connectivity            # 连接测试")
	fmt.Println("    performance             # 性能测试")
	fmt.Println("\n示例:")
	fmt.Println("  go run main.go cluster list")
	fmt.Println("  go run main.go topic create TestTopic 4")
	fmt.Println("  go run main.go message send TestTopic \"Hello World\"")
}

// 处理集群命令
func handleClusterCommand(admin *AdminClient, args []string) {
	if len(args) == 0 {
		fmt.Println("请指定集群子命令")
		return
	}

	switch args[0] {
	case "list":
		listClusterInfo(admin)
	case "status":
		showClusterStatus(admin)
	case "brokers":
		listBrokers(admin)
	default:
		fmt.Printf("未知集群子命令: %s\n", args[0])
	}
}

// 处理Topic命令
func handleTopicCommand(admin *AdminClient, args []string) {
	if len(args) == 0 {
		fmt.Println("请指定Topic子命令")
		return
	}

	switch args[0] {
	case "list":
		listTopics(admin)
	case "create":
		if len(args) < 2 {
			fmt.Println("请指定Topic名称")
			return
		}
		queues := 4 // 默认队列数
		if len(args) >= 3 {
			if q, err := strconv.Atoi(args[2]); err == nil {
				queues = q
			}
		}
		createTopic(admin, args[1], queues)
	case "delete":
		if len(args) < 2 {
			fmt.Println("请指定Topic名称")
			return
		}
		deleteTopic(admin, args[1])
	case "info":
		if len(args) < 2 {
			fmt.Println("请指定Topic名称")
			return
		}
		showTopicInfo(admin, args[1])
	default:
		fmt.Printf("未知Topic子命令: %s\n", args[0])
	}
}

// 处理消费者命令
func handleConsumerCommand(admin *AdminClient, args []string) {
	if len(args) == 0 {
		fmt.Println("请指定消费者子命令")
		return
	}

	switch args[0] {
	case "list":
		listConsumerGroups(admin)
	case "progress":
		if len(args) < 2 {
			fmt.Println("请指定消费者组名")
			return
		}
		showConsumerProgress(admin, args[1])
	case "reset":
		if len(args) < 3 {
			fmt.Println("请指定消费者组名和Topic")
			return
		}
		resetConsumerOffset(admin, args[1], args[2])
	default:
		fmt.Printf("未知消费者子命令: %s\n", args[0])
	}
}

// 处理消息命令
func handleMessageCommand(admin *AdminClient, args []string) {
	if len(args) == 0 {
		fmt.Println("请指定消息子命令")
		return
	}

	switch args[0] {
	case "send":
		if len(args) < 3 {
			fmt.Println("请指定Topic和消息内容")
			return
		}
		sendTestMessage(admin, args[1], strings.Join(args[2:], " "))
	case "query":
		if len(args) < 2 {
			fmt.Println("请指定消息ID")
			return
		}
		queryMessage(admin, args[1])
	case "trace":
		if len(args) < 2 {
			fmt.Println("请指定消息ID")
			return
		}
		traceMessage(admin, args[1])
	default:
		fmt.Printf("未知消息子命令: %s\n", args[0])
	}
}

// 处理监控命令
func handleMonitorCommand(admin *AdminClient, args []string) {
	if len(args) == 0 {
		fmt.Println("请指定监控子命令")
		return
	}

	switch args[0] {
	case "stats":
		showStats(admin)
	case "health":
		healthCheck(admin)
	case "watch":
		startWatch(admin)
	default:
		fmt.Printf("未知监控子命令: %s\n", args[0])
	}
}

// 处理测试命令
func handleTestCommand(admin *AdminClient, args []string) {
	if len(args) == 0 {
		fmt.Println("请指定测试子命令")
		return
	}

	switch args[0] {
	case "connectivity":
		testConnectivity(admin)
	case "performance":
		testPerformance(admin)
	default:
		fmt.Printf("未知测试子命令: %s\n", args[0])
	}
}

// 列出集群信息
func listClusterInfo(admin *AdminClient) {
	fmt.Println("\n=== 集群信息 ===")
	fmt.Printf("NameServer: %s\n", admin.config.NameServerAddr)
	fmt.Println("集群名称: DefaultCluster")
	fmt.Println("集群模式: 主从模式")
	fmt.Println("状态: 运行中")

	// 这里应该实现实际的集群信息查询
	fmt.Println("\n注意: 这是模拟数据，实际实现需要调用相应的API")
}

// 显示集群状态
func showClusterStatus(admin *AdminClient) {
	fmt.Println("\n=== 集群状态 ===")
	fmt.Println("NameServer状态: 正常")
	fmt.Println("Broker数量: 2")
	fmt.Println("Topic数量: 5")
	fmt.Println("消费者组数量: 3")
	fmt.Println("总消息数: 1,234,567")
	fmt.Println("今日消息数: 12,345")

	// 这里应该实现实际的状态查询
	fmt.Println("\n注意: 这是模拟数据，实际实现需要调用相应的API")
}

// 列出Broker
func listBrokers(admin *AdminClient) {
	fmt.Println("\n=== Broker列表 ===")
	
	// 从NameServer获取集群信息
	clusterInfo, err := getClusterInfoFromNameServer(admin.config.NameServerAddr)
	if err != nil {
		fmt.Printf("获取集群信息失败: %v\n", err)
		fmt.Println("\n使用模拟数据:")
		displayMockBrokers()
		return
	}
	
	// 显示真实的Broker信息
	displayRealBrokers(clusterInfo)
}

// 从NameServer获取集群信息
func getClusterInfoFromNameServer(nameServerAddr string) (*ClusterInfo, error) {
	// 构建请求URL
	url := fmt.Sprintf("http://%s/cluster/list.query", nameServerAddr)
	
	// 发送HTTP请求
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("请求NameServer失败: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("NameServer返回错误状态: %d", resp.StatusCode)
	}
	
	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}
	
	// 解析JSON响应
	var clusterInfo ClusterInfo
	if err := json.Unmarshal(body, &clusterInfo); err != nil {
		return nil, fmt.Errorf("解析集群信息失败: %v", err)
	}
	
	return &clusterInfo, nil
}

// 显示真实的Broker信息
func displayRealBrokers(clusterInfo *ClusterInfo) {
	fmt.Println("ID\tName\t\tRole\t\tAddress\t\t\tStatus")
	fmt.Println("--\t----\t\t----\t\t-------\t\t\t------")
	
	for brokerName, brokerAddrs := range clusterInfo.BrokerAddrTable {
		for brokerId, address := range brokerAddrs {
			role := "SLAVE"
			if brokerId == 0 {
				role = "MASTER"
			}
			
			// 检查Broker状态
			status := checkBrokerStatus(address)
			
			fmt.Printf("%d\t%s\t%s\t\t%s\t\t%s\n", 
				brokerId, brokerName, role, address, status)
		}
	}
	
	fmt.Printf("\n总计: %d 个Broker\n", getTotalBrokerCount(clusterInfo))
}

// 显示模拟Broker信息
func displayMockBrokers() {
	fmt.Println("ID\tName\t\tRole\t\tAddress\t\t\tStatus")
	fmt.Println("--\t----\t\t----\t\t-------\t\t\t------")
	fmt.Println("0\tbroker-a\tMASTER\t\t127.0.0.1:10911\t\tONLINE")
	fmt.Println("1\tbroker-a-s\tSLAVE\t\t127.0.0.1:10921\t\tONLINE")
	fmt.Println("0\tbroker-b\tMASTER\t\t127.0.0.1:10912\t\tONLINE")
	fmt.Println("1\tbroker-b-s\tSLAVE\t\t127.0.0.1:10922\t\tONLINE")
	fmt.Println("\n注意: 这是模拟数据，无法连接到NameServer")
}

// 检查Broker状态
func checkBrokerStatus(address string) string {
	// 尝试连接Broker检查状态
	client := &http.Client{Timeout: 3 * time.Second}
	url := fmt.Sprintf("http://%s/brokerStats/brokerRuntimeInfo", address)
	
	resp, err := client.Get(url)
	if err != nil {
		return "OFFLINE"
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == http.StatusOK {
		return "ONLINE"
	}
	return "UNKNOWN"
}

// 获取Broker总数
func getTotalBrokerCount(clusterInfo *ClusterInfo) int {
	count := 0
	for _, brokerAddrs := range clusterInfo.BrokerAddrTable {
		count += len(brokerAddrs)
	}
	return count
}

// 列出Topic
func listTopics(admin *AdminClient) {
	fmt.Println("\n=== Topic列表 ===")
	fmt.Println("Topic名称\t\t队列数\t消息数\t\t状态")
	fmt.Println("---------\t\t------\t------\t\t----")
	fmt.Println("TestTopic\t\t4\t12,345\t\tACTIVE")
	fmt.Println("OrderTopic\t\t8\t23,456\t\tACTIVE")
	fmt.Println("PaymentTopic\t\t4\t5,678\t\tACTIVE")
	fmt.Println("BenchmarkTopic\t\t4\t100,000\t\tACTIVE")

	// 这里应该实现实际的Topic查询
	fmt.Println("\n注意: 这是模拟数据，实际实现需要调用相应的API")
}

// 创建Topic
func createTopic(admin *AdminClient, topicName string, queues int) {
	fmt.Printf("\n=== 创建Topic ===")
	fmt.Printf("Topic名称: %s\n", topicName)
	fmt.Printf("队列数量: %d\n", queues)

	// 这里应该实现实际的Topic创建
	fmt.Printf("正在创建Topic '%s'...\n", topicName)
	time.Sleep(1 * time.Second) // 模拟创建过程
	fmt.Printf("✓ Topic '%s' 创建成功\n", topicName)

	fmt.Println("\n注意: 这是模拟操作，实际实现需要调用相应的API")
}

// 删除Topic
func deleteTopic(admin *AdminClient, topicName string) {
	fmt.Printf("\n=== 删除Topic ===")
	fmt.Printf("Topic名称: %s\n", topicName)
	fmt.Printf("⚠️  警告: 此操作将永久删除Topic及其所有消息\n")

	// 这里应该实现实际的Topic删除
	fmt.Printf("正在删除Topic '%s'...\n", topicName)
	time.Sleep(1 * time.Second) // 模拟删除过程
	fmt.Printf("✓ Topic '%s' 删除成功\n", topicName)

	fmt.Println("\n注意: 这是模拟操作，实际实现需要调用相应的API")
}

// 显示Topic信息
func showTopicInfo(admin *AdminClient, topicName string) {
	fmt.Printf("\n=== Topic信息: %s ===\n", topicName)
	fmt.Println("基本信息:")
	fmt.Printf("  名称: %s\n", topicName)
	fmt.Println("  队列数: 4")
	fmt.Println("  权限: 读写")
	fmt.Println("  消息类型: 普通消息")
	fmt.Println("\n统计信息:")
	fmt.Println("  总消息数: 12,345")
	fmt.Println("  今日消息数: 1,234")
	fmt.Println("  消息大小: 1.2 MB")
	fmt.Println("\n队列分布:")
	fmt.Println("  队列0: 3,086 条消息")
	fmt.Println("  队列1: 3,089 条消息")
	fmt.Println("  队列2: 3,085 条消息")
	fmt.Println("  队列3: 3,085 条消息")

	// 这里应该实现实际的Topic信息查询
	fmt.Println("\n注意: 这是模拟数据，实际实现需要调用相应的API")
}

// 列出消费者组
func listConsumerGroups(admin *AdminClient) {
	fmt.Println("\n=== 消费者组列表 ===")
	fmt.Println("组名\t\t\t\t消费者数\t订阅Topic\t\t消费模式")
	fmt.Println("----\t\t\t\t---------\t----------\t\t--------")
	fmt.Println("example_consumer_group\t\t2\t\tTestTopic\t\tCLUSTERING")
	fmt.Println("order_consumer_group\t\t1\t\tOrderTopic\t\tCLUSTERING")
	fmt.Println("payment_consumer_group\t\t3\t\tPaymentTopic\t\tCLUSTERING")

	// 这里应该实现实际的消费者组查询
	fmt.Println("\n注意: 这是模拟数据，实际实现需要调用相应的API")
}

// 显示消费进度
func showConsumerProgress(admin *AdminClient, groupName string) {
	fmt.Printf("\n=== 消费者组进度: %s ===\n", groupName)
	fmt.Println("Topic\t\t队列ID\t消费位点\t最大位点\t积压数量")
	fmt.Println("-----\t\t------\t--------\t--------\t--------")
	fmt.Println("TestTopic\t0\t1000\t\t1200\t\t200")
	fmt.Println("TestTopic\t1\t1050\t\t1180\t\t130")
	fmt.Println("TestTopic\t2\t980\t\t1150\t\t170")
	fmt.Println("TestTopic\t3\t1020\t\t1190\t\t170")
	fmt.Println("\n总积压: 670 条消息")
	fmt.Println("平均消费延迟: 2.3 秒")

	// 这里应该实现实际的消费进度查询
	fmt.Println("\n注意: 这是模拟数据，实际实现需要调用相应的API")
}

// 重置消费位点
func resetConsumerOffset(admin *AdminClient, groupName, topicName string) {
	fmt.Printf("\n=== 重置消费位点 ===")
	fmt.Printf("消费者组: %s\n", groupName)
	fmt.Printf("Topic: %s\n", topicName)
	fmt.Printf("⚠️  警告: 此操作将重置消费位点到最新位置\n")

	// 这里应该实现实际的位点重置
	fmt.Printf("正在重置消费位点...\n")
	time.Sleep(1 * time.Second) // 模拟重置过程
	fmt.Printf("✓ 消费位点重置成功\n")

	fmt.Println("\n注意: 这是模拟操作，实际实现需要调用相应的API")
}

// 发送测试消息
func sendTestMessage(admin *AdminClient, topicName, content string) {
	fmt.Printf("\n=== 发送测试消息 ===")
	fmt.Printf("Topic: %s\n", topicName)
	fmt.Printf("内容: %s\n", content)

	// 创建生产者
	producer := client.NewProducer("admin_test_producer")
	producer.SetNameServers([]string{admin.config.NameServerAddr})

	if err := producer.Start(); err != nil {
		log.Printf("启动生产者失败: %v", err)
		return
	}
	defer producer.Shutdown()

	// 创建消息
	msg := client.NewMessage(
		topicName,
		[]byte(content),
	).SetTags("ADMIN_TEST").SetKeys(fmt.Sprintf("admin_test_%d", time.Now().Unix())).SetProperty("source", "admin_tool").SetProperty("timestamp", time.Now().Format("2006-01-02 15:04:05"))

	// 发送消息
	result, err := producer.SendSync(msg)
	if err != nil {
		log.Printf("发送消息失败: %v", err)
		return
	}

	fmt.Printf("✓ 消息发送成功\n")
	fmt.Printf("消息ID: %s\n", result.MsgId)
	fmt.Printf("队列ID: %d\n", result.MessageQueue.QueueId)
	fmt.Printf("队列偏移: %d\n", result.QueueOffset)
}

// 查询消息
func queryMessage(admin *AdminClient, msgId string) {
	fmt.Printf("\n=== 查询消息: %s ===\n", msgId)
	
	// 尝试通过消费者从追踪Topic查询消息信息
	if admin.consumer != nil {
		// 创建临时消费者用于查询追踪数据
		traceConsumer := client.NewConsumer(&client.ConsumerConfig{
			GroupName:        "ADMIN_TRACE_QUERY_GROUP",
			NameServerAddr:   admin.config.NameServerAddr,
			ConsumeFromWhere: client.ConsumeFromFirstOffset,
			MessageModel:     client.Clustering,
		})
		
		// 查询追踪数据
		messageFound := false
		traceConsumer.Subscribe("RMQ_SYS_TRACE_TOPIC", "*", client.MessageListenerConcurrently(func(msgs []*client.MessageExt) client.ConsumeResult {
			for _, msg := range msgs {
				// 解析追踪数据
				if traceData := parseTraceData(msg.Body, msgId); traceData != nil {
					messageFound = true
					displayMessageDetails(traceData, msgId)
					return client.ConsumeSuccess
				}
			}
			return client.ConsumeSuccess
		}))
		
		if err := traceConsumer.Start(); err == nil {
			// 等待一段时间查询追踪数据
			time.Sleep(2 * time.Second)
			traceConsumer.Stop()
		}
		
		if messageFound {
			return
		}
	}
	
	// 如果没有找到追踪数据，显示基本信息
	fmt.Println("消息详情:")
	fmt.Printf("  消息ID: %s\n", msgId)
	fmt.Println("  状态: 查询中...")
	fmt.Println("\n注意: 正在尝试从追踪系统查询消息详情")
	fmt.Println("如果消息较新或追踪未启用，可能无法获取完整信息")
}

// 追踪消息
func traceMessage(admin *AdminClient, msgId string) {
	fmt.Printf("\n=== 消息追踪: %s ===\n", msgId)
	
	// 创建追踪数据收集器
	traceCollector := &MessageTraceCollector{
		msgId: msgId,
		traces: make([]*client.TraceContext, 0),
	}
	
	// 创建临时消费者用于收集追踪数据
	traceConsumer := client.NewConsumer(&client.ConsumerConfig{
		GroupName:        "ADMIN_TRACE_COLLECTOR_GROUP",
		NameServerAddr:   admin.config.NameServerAddr,
		ConsumeFromWhere: client.ConsumeFromFirstOffset,
		MessageModel:     client.Clustering,
	})
	
	// 订阅追踪Topic
	traceConsumer.Subscribe("RMQ_SYS_TRACE_TOPIC", "*", client.MessageListenerConcurrently(func(msgs []*client.MessageExt) client.ConsumeResult {
		for _, msg := range msgs {
			traceCollector.collectTraceData(msg.Body)
		}
		return client.ConsumeSuccess
	}))
	
	if err := traceConsumer.Start(); err != nil {
		fmt.Printf("启动追踪消费者失败: %v\n", err)
		displayFallbackTrace(msgId)
		return
	}
	
	// 收集追踪数据
	fmt.Println("正在收集消息追踪数据...")
	time.Sleep(3 * time.Second)
	traceConsumer.Stop()
	
	// 显示追踪结果
	traceCollector.displayTrace()
}

// parseTraceData 解析追踪数据
func parseTraceData(data []byte, targetMsgId string) *TraceData {
	// 尝试解析JSON格式的追踪数据
	var traceData TraceData
	if err := json.Unmarshal(data, &traceData); err == nil {
		if traceData.MsgId == targetMsgId {
			return &traceData
		}
	}
	
	// 如果JSON解析失败，尝试解析文本格式
	dataStr := string(data)
	if strings.Contains(dataStr, targetMsgId) {
		// 简单的文本解析
		return &TraceData{
			MsgId:     targetMsgId,
			Topic:     "Unknown",
			Tags:      "Unknown",
			Keys:      "Unknown",
			QueueId:   -1,
			Offset:    -1,
			Size:      len(data),
			SendTime:  time.Now(),
			StoreTime: time.Now(),
			RetryTimes: 0,
			Body:      dataStr,
		}
	}
	
	return nil
}

// displayMessageDetails 显示消息详情
func displayMessageDetails(traceData *TraceData, msgId string) {
	fmt.Println("消息详情:")
	fmt.Printf("  消息ID: %s\n", traceData.MsgId)
	fmt.Printf("  Topic: %s\n", traceData.Topic)
	fmt.Printf("  Tags: %s\n", traceData.Tags)
	fmt.Printf("  Keys: %s\n", traceData.Keys)
	fmt.Printf("  队列ID: %d\n", traceData.QueueId)
	fmt.Printf("  队列偏移: %d\n", traceData.Offset)
	fmt.Printf("  消息大小: %d 字节\n", traceData.Size)
	fmt.Printf("  发送时间: %s\n", traceData.SendTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("  存储时间: %s\n", traceData.StoreTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("  重试次数: %d\n", traceData.RetryTimes)
	fmt.Println("\n消息内容:")
	fmt.Printf("  %s\n", traceData.Body)
}

// collectTraceData 收集追踪数据
func (tc *MessageTraceCollector) collectTraceData(data []byte) {
	// 尝试解析追踪上下文
	dataStr := string(data)
	if strings.Contains(dataStr, tc.msgId) {
		// 创建追踪上下文
		traceCtx := &client.TraceContext{
			TraceType:   client.TraceTypeProduce,
			TimeStamp:   time.Now().UnixMilli(),
			RegionId:    "default",
			RegionName:  "DefaultRegion",
			GroupName:   "ADMIN_TRACE_GROUP",
			CostTime:    0,
			Success:     true,
			RequestId:   tc.msgId,
			ContextCode: 0,
			TraceBeans:  make([]*client.TraceBean, 0),
		}
		tc.traces = append(tc.traces, traceCtx)
	}
}

// displayTrace 显示追踪信息
func (tc *MessageTraceCollector) displayTrace() {
	if len(tc.traces) == 0 {
		fmt.Println("未找到消息追踪数据")
		displayFallbackTrace(tc.msgId)
		return
	}
	
	fmt.Println("消息轨迹:")
	for i, trace := range tc.traces {
		timestamp := time.UnixMilli(trace.TimeStamp).Format("2006-01-02 15:04:05.000")
		fmt.Printf("%d. %s - %s (区域: %s, 组: %s)\n", 
			i+1, timestamp, trace.TraceType, trace.RegionName, trace.GroupName)
	}
	
	fmt.Println("\n追踪详情:")
	fmt.Printf("  消息ID: %s\n", tc.msgId)
	fmt.Printf("  追踪记录数: %d\n", len(tc.traces))
	fmt.Printf("  最后更新: %s\n", time.Now().Format("2006-01-02 15:04:05"))
}

// displayFallbackTrace 显示备用追踪信息
func displayFallbackTrace(msgId string) {
	fmt.Println("\n使用备用追踪信息:")
	fmt.Printf("  消息ID: %s\n", msgId)
	fmt.Println("  状态: 追踪数据收集中...")
	fmt.Println("  建议: 请确保消息发送时启用了追踪功能")
	fmt.Println("  提示: 使用 producer.EnableTrace() 启用生产者追踪")
	fmt.Println("        使用 consumer.EnableTrace() 启用消费者追踪")
}

// 显示统计信息
func showStats(admin *AdminClient) {
	fmt.Println("\n=== 系统统计信息 ===")
	
	// 获取系统统计信息
	sysStats := getSystemStats()
	msgStats := getMessageStats(admin)
	
	fmt.Println("系统资源:")
	fmt.Printf("  CPU使用率: %.2f%%\n", sysStats.CPUUsage)
	fmt.Printf("  内存使用: %s / %s (%.2f%%)\n", 
		formatBytes(sysStats.MemoryUsage), 
		formatBytes(sysStats.MemoryTotal),
		float64(sysStats.MemoryUsage)/float64(sysStats.MemoryTotal)*100)
	fmt.Printf("  Goroutines: %d\n", sysStats.Goroutines)
	fmt.Printf("  GC次数: %d\n", sysStats.GCCount)
	fmt.Printf("  磁盘使用率: %.2f%%\n", sysStats.DiskUsage)
	
	fmt.Println("\n消息统计:")
	fmt.Printf("  发送消息数: %d\n", msgStats.SentCount)
	fmt.Printf("  接收消息数: %d\n", msgStats.ReceivedCount)
	fmt.Printf("  错误消息数: %d\n", msgStats.ErrorCount)
	
	fmt.Println("\n性能统计:")
	fmt.Printf("  发送TPS: %.2f 条/秒\n", msgStats.SendTPS)
	fmt.Printf("  接收TPS: %.2f 条/秒\n", msgStats.ReceiveTPS)
	fmt.Printf("  平均延迟: %.2f 毫秒\n", msgStats.AvgLatency)
	fmt.Printf("  99%%延迟: %.2f 毫秒\n", msgStats.P99Latency)
	
	fmt.Println("\n网络统计:")
	fmt.Printf("  入站流量: %s\n", formatBytes(sysStats.NetworkIn))
	fmt.Printf("  出站流量: %s\n", formatBytes(sysStats.NetworkOut))
	
	fmt.Printf("\n更新时间: %s\n", sysStats.Timestamp.Format("2006-01-02 15:04:05"))
}

// 健康检查
func healthCheck(admin *AdminClient) {
	fmt.Println("\n=== 健康检查 ===")
	fmt.Println("正在检查系统健康状态...")

	// 检查NameServer连接
	nameServerHealthy := checkNameServerHealth(admin.config.NameServerAddr)
	
	// 检查Broker状态
	brokerHealthy := checkBrokerHealth(admin)
	
	// 检查系统资源
	sysStats := getSystemStats()
	memoryHealthy := float64(sysStats.MemoryUsage)/float64(sysStats.MemoryTotal) < 0.9
	cpuHealthy := sysStats.CPUUsage < 90.0
	diskHealthy := sysStats.DiskUsage < 90.0
	
	fmt.Println("\n检查结果:")
	printHealthStatus("NameServer", nameServerHealthy)
	printHealthStatus("Broker集群", brokerHealthy)
	printHealthStatus("CPU使用率", cpuHealthy)
	printHealthStatus("内存使用率", memoryHealthy)
	printHealthStatus("磁盘使用率", diskHealthy)
	
	// 总体健康状态
	allHealthy := nameServerHealthy && brokerHealthy && memoryHealthy && cpuHealthy && diskHealthy
	if allHealthy {
		fmt.Println("\n总体状态: 健康 ✓")
	} else {
		fmt.Println("\n总体状态: 异常 ✗")
	}
	
	// 显示详细信息
	fmt.Printf("\nCPU使用率: %.2f%%\n", sysStats.CPUUsage)
	fmt.Printf("内存使用率: %.2f%%\n", float64(sysStats.MemoryUsage)/float64(sysStats.MemoryTotal)*100)
	fmt.Printf("磁盘使用率: %.2f%%\n", sysStats.DiskUsage)
	fmt.Printf("Goroutines: %d\n", sysStats.Goroutines)
}

// 开始监控
func startWatch(admin *AdminClient) {
	fmt.Println("\n=== 实时监控 ===")
	fmt.Println("开始实时监控系统状态... (按Ctrl+C停止)")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// 创建上下文用于优雅停止
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 监控循环
	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n监控结束")
			return
		case <-ticker.C:
			currentTime := time.Now().Format("15:04:05")
			sysStats := getSystemStats()
			msgStats := getMessageStats(admin)
			
			fmt.Printf("\n[%s] 系统状态:\n", currentTime)
			fmt.Printf("  CPU使用率: %.2f%%\n", sysStats.CPUUsage)
			fmt.Printf("  内存使用: %s (%.2f%%)\n", 
				formatBytes(sysStats.MemoryUsage),
				float64(sysStats.MemoryUsage)/float64(sysStats.MemoryTotal)*100)
			fmt.Printf("  发送TPS: %.2f\n", msgStats.SendTPS)
			fmt.Printf("  接收TPS: %.2f\n", msgStats.ReceiveTPS)
			fmt.Printf("  平均延迟: %.2f ms\n", msgStats.AvgLatency)
			fmt.Printf("  Goroutines: %d\n", sysStats.Goroutines)
			
			// 检查是否需要停止（运行60次，即5分钟）
			static_counter++
			if static_counter >= 60 {
				cancel()
			}
		}
	}
}

var static_counter int = 0

// 测试连接
func testConnectivity(admin *AdminClient) {
	fmt.Println("\n=== 连接测试 ===")
	fmt.Printf("测试NameServer连接: %s\n", admin.config.NameServerAddr)

	// 创建测试生产者
	producer := client.NewProducer("connectivity_test_producer")
	producer.SetNameServers([]string{admin.config.NameServerAddr})

	fmt.Println("正在测试生产者连接...")
	if err := producer.Start(); err != nil {
		fmt.Printf("✗ 生产者连接失败: %v\n", err)
	} else {
		fmt.Println("✓ 生产者连接成功")
		producer.Shutdown()
	}

	// 创建测试消费者
	config := &client.ConsumerConfig{
		GroupName:      "connectivity_test_consumer",
		NameServerAddr: admin.config.NameServerAddr,
	}
	consumer := client.NewConsumer(config)

	fmt.Println("正在测试消费者连接...")
	if err := consumer.Start(); err != nil {
		fmt.Printf("✗ 消费者连接失败: %v\n", err)
	} else {
		fmt.Println("✓ 消费者连接成功")
		consumer.Stop()
	}

	fmt.Println("\n连接测试完成")
}

// 性能测试
func testPerformance(admin *AdminClient) {
	fmt.Println("\n=== 性能测试 ===")
	fmt.Println("开始简单性能测试...")

	// 创建生产者
	producer := client.NewProducer("performance_test_producer")
	producer.SetNameServers([]string{admin.config.NameServerAddr})

	if err := producer.Start(); err != nil {
		fmt.Printf("启动生产者失败: %v\n", err)
		return
	}
	defer producer.Shutdown()

	// 发送测试消息
	messageCount := 100
	startTime := time.Now()

	fmt.Printf("发送 %d 条测试消息...\n", messageCount)
	for i := 0; i < messageCount; i++ {
		msg := client.NewMessage(
			"PerformanceTestTopic",
			[]byte(fmt.Sprintf("性能测试消息 #%d", i+1)),
		)

		_, err := producer.SendSync(msg)
		if err != nil {
			fmt.Printf("发送消息 #%d 失败: %v\n", i+1, err)
		}
	}

	duration := time.Since(startTime)
	tps := float64(messageCount) / duration.Seconds()

	fmt.Printf("\n性能测试结果:\n")
	fmt.Printf("  消息数量: %d\n", messageCount)
	fmt.Printf("  总耗时: %v\n", duration)
	fmt.Printf("  平均TPS: %.2f 条/秒\n", tps)
	fmt.Printf("  平均延迟: %.2f 毫秒\n", duration.Seconds()*1000/float64(messageCount))

	if tps > 1000 {
		fmt.Println("  性能评估: 优秀 ✓")
	} else if tps > 500 {
		fmt.Println("  性能评估: 良好 ✓")
	} else {
		fmt.Println("  性能评估: 需要优化 ⚠️")
	}
}

// 获取系统统计信息
func getSystemStats() *SystemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	return &SystemStats{
		CPUUsage:    getCPUUsage(),
		MemoryUsage: m.Alloc,
		MemoryTotal: m.Sys,
		Goroutines:  runtime.NumGoroutine(),
		GCCount:     m.NumGC,
		DiskUsage:   getDiskUsage(),
		NetworkIn:   0, // 需要实现网络统计
		NetworkOut:  0, // 需要实现网络统计
		Timestamp:   time.Now(),
	}
}

// 获取消息统计信息
func getMessageStats(admin *AdminClient) *MessageStats {
	// 这里应该从实际的监控系统获取数据
	// 目前返回基本的统计信息
	return &MessageStats{
		SentCount:     0,
		ReceivedCount: 0,
		ErrorCount:    0,
		SendTPS:       0.0,
		ReceiveTPS:    0.0,
		AvgLatency:    0.0,
		P99Latency:    0.0,
	}
}

// 获取CPU使用率
func getCPUUsage() float64 {
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		cmd := exec.Command("sh", "-c", "top -bn1 | grep 'Cpu(s)' | awk '{print $2}' | awk -F'%' '{print $1}'")
		output, err := cmd.Output()
		if err == nil {
			if usage, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64); err == nil {
				return usage
			}
		}
	}
	return 0.0
}

// 获取磁盘使用率
func getDiskUsage() float64 {
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		cmd := exec.Command("df", "-h", "/")
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(output), "\n")
			if len(lines) > 1 {
				fields := strings.Fields(lines[1])
				if len(fields) > 4 {
					usageStr := strings.TrimSuffix(fields[4], "%")
					if usage, err := strconv.ParseFloat(usageStr, 64); err == nil {
						return usage
					}
				}
			}
		}
	}
	return 0.0
}

// 格式化字节数
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// 检查NameServer健康状态
func checkNameServerHealth(nameServerAddr string) bool {
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("http://%s/cluster/list.query", nameServerAddr)
	
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	
	return resp.StatusCode == http.StatusOK
}

// 检查Broker健康状态
func checkBrokerHealth(admin *AdminClient) bool {
	clusterInfo, err := getClusterInfoFromNameServer(admin.config.NameServerAddr)
	if err != nil {
		return false
	}
	
	for _, brokerAddrs := range clusterInfo.BrokerAddrTable {
		for _, address := range brokerAddrs {
			if checkBrokerStatus(address) == "OFFLINE" {
				return false
			}
		}
	}
	return true
}

// 打印健康状态
func printHealthStatus(component string, healthy bool) {
	if healthy {
		fmt.Printf("✓ %s: 健康\n", component)
	} else {
		fmt.Printf("✗ %s: 异常\n", component)
	}
}