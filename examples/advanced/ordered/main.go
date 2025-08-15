package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/chenjy16/go-rocketmq-client"
)

// 订单状态变更事件
type OrderEvent struct {
	OrderId   string
	EventType string
	Timestamp time.Time
	Data      map[string]interface{}
}

func main() {
	fmt.Println("=== Go-RocketMQ 顺序消息示例 ===")
	fmt.Println("本示例演示如何发送和消费顺序消息，确保同一订单的事件按顺序处理")

	var wg sync.WaitGroup

	// 启动消费者
	wg.Add(1)
	go func() {
		defer wg.Done()
		startOrderedConsumer()
	}()

	// 等待消费者启动
	time.Sleep(2 * time.Second)

	// 启动生产者
	wg.Add(1)
	go func() {
		defer wg.Done()
		startOrderedProducer()
	}()

	wg.Wait()
	fmt.Println("\n顺序消息示例完成")
}

// 启动顺序消息生产者
func startOrderedProducer() {
	fmt.Println("\n[生产者] 启动顺序消息生产者...")

	// 创建生产者
	producer := client.NewProducer("ordered_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启用消息追踪
	err := producer.EnableTrace("trace_topic", "trace_group")
	if err != nil {
		log.Printf("[生产者] 启用消息追踪失败: %v", err)
	}

	if err := producer.Start(); err != nil {
		log.Printf("[生产者] 启动失败: %v", err)
		return
	}
	defer producer.Shutdown()

	fmt.Println("[生产者] 顺序消息生产者启动成功")

	// 模拟多个订单的生命周期事件
	orders := []string{"ORDER_001", "ORDER_002", "ORDER_003"}

	for _, orderId := range orders {
		fmt.Printf("\n[生产者] 开始发送订单 %s 的生命周期事件\n", orderId)
		sendOrderLifecycleEvents(producer, orderId)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n[生产者] 所有顺序消息发送完成")
	time.Sleep(5 * time.Second) // 等待消费完成
}

// 发送订单生命周期事件
func sendOrderLifecycleEvents(producer *client.Producer, orderId string) {
	// 定义订单生命周期事件序列
	events := []OrderEvent{
		{
			OrderId:   orderId,
			EventType: "ORDER_CREATED",
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"userId":    "user_123",
				"amount":    299.99,
				"productId": "PROD_456",
			},
		},
		{
			OrderId:   orderId,
			EventType: "PAYMENT_RECEIVED",
			Timestamp: time.Now().Add(1 * time.Second),
			Data: map[string]interface{}{
				"paymentId":     "PAY_789",
				"paymentMethod": "CREDIT_CARD",
				"amount":        299.99,
			},
		},
		{
			OrderId:   orderId,
			EventType: "INVENTORY_RESERVED",
			Timestamp: time.Now().Add(2 * time.Second),
			Data: map[string]interface{}{
				"warehouseId": "WH_001",
				"quantity":    1,
			},
		},
		{
			OrderId:   orderId,
			EventType: "ORDER_SHIPPED",
			Timestamp: time.Now().Add(3 * time.Second),
			Data: map[string]interface{}{
				"trackingNumber": "TRK_" + orderId,
				"carrier":        "EXPRESS_DELIVERY",
				"estimatedDate":  time.Now().Add(24 * time.Hour).Format("2006-01-02"),
			},
		},
		{
			OrderId:   orderId,
			EventType: "ORDER_DELIVERED",
			Timestamp: time.Now().Add(4 * time.Second),
			Data: map[string]interface{}{
				"deliveryTime": time.Now().Add(24 * time.Hour).Format("2006-01-02 15:04:05"),
				"signature":    "Customer",
			},
		},
	}

	// 按顺序发送事件
	for i, event := range events {
		// 创建顺序消息
		msg := createOrderEventMessage(event)

		// 发送到指定队列（根据订单ID选择队列确保顺序）
		result, err := sendOrderedMessage(producer, msg, orderId)
		if err != nil {
			log.Printf("[生产者] 发送顺序消息失败: %v", err)
			continue
		}

		fmt.Printf("[生产者] 顺序消息发送成功 - OrderId: %s, Event: %s, MsgId: %s, Sequence: %d\n",
			orderId, event.EventType, result.MsgId, i+1)

		// 间隔发送以模拟真实场景
		time.Sleep(200 * time.Millisecond)
	}
}

// 创建订单事件消息
func createOrderEventMessage(event OrderEvent) *client.Message {
	// 构造消息体
	msgBody := fmt.Sprintf(`{
		"orderId": "%s",
		"eventType": "%s",
		"timestamp": "%s",
		"data": %v
	}`, event.OrderId, event.EventType, event.Timestamp.Format("2006-01-02 15:04:05"), formatData(event.Data))

	// 创建消息
	msg := client.NewMessage(
		"OrderEventTopic",
		[]byte(msgBody),
	).SetTags(event.EventType).SetKeys(event.OrderId).SetProperty("orderId", event.OrderId).SetProperty("eventType", event.EventType).SetProperty("sequence", "true") // 标记为顺序消息

	return msg
}

// 格式化数据为JSON字符串
func formatData(data map[string]interface{}) string {
	result := "{"
	count := 0
	for key, value := range data {
		if count > 0 {
			result += ", "
		}
		result += fmt.Sprintf(`"%s": "%v"`, key, value)
		count++
	}
	result += "}"
	return result
}

// 发送顺序消息（根据订单ID选择队列）
func sendOrderedMessage(producer *client.Producer, msg *client.Message, orderId string) (*client.SendResult, error) {
	// 这里简化处理，实际应该实现队列选择器
	// 确保同一个订单的消息发送到同一个队列
	fmt.Printf("[生产者] 发送顺序消息到指定队列 - OrderId: %s\n", orderId)

	// 使用同步发送确保顺序
	return producer.SendSync(msg)
}

// 启动顺序消息消费者
func startOrderedConsumer() {
	fmt.Println("\n[消费者] 启动顺序消息消费者...")

	// 创建消费者配置
	config := &client.ConsumerConfig{
		GroupName:        "ordered_consumer_group",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: client.ConsumeFromLastOffset,
		MessageModel:     client.Clustering,
		ConsumeThreadMin: 1, // 顺序消费使用单线程
		ConsumeThreadMax: 1, // 顺序消费使用单线程
		PullInterval:     100 * time.Millisecond,
		PullBatchSize:    1, // 顺序消费每次只拉取一条消息
		ConsumeTimeout:   30 * time.Second,
	}

	// 创建消费者
	consumer := client.NewConsumer(config)

	// 设置负载均衡策略
	consumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})

	// 启用消息追踪
	err := consumer.EnableTrace("trace_topic", "trace_group")
	if err != nil {
		log.Printf("[消费者] 启用消息追踪失败: %v", err)
	}

	// 订阅Topic
	err = consumer.Subscribe("OrderEventTopic", "*", &OrderedMessageListener{})
	if err != nil {
		log.Printf("[消费者] 订阅失败: %v", err)
		return
	}

	if err := consumer.Start(); err != nil {
		log.Printf("[消费者] 启动失败: %v", err)
		return
	}
	defer consumer.Stop()

	fmt.Println("[消费者] 顺序消息消费者启动成功")

	// 运行20秒
	time.Sleep(20 * time.Second)
	fmt.Println("[消费者] 停止顺序消息消费")
}

// 顺序消息监听器
type OrderedMessageListener struct {
	orderStates map[string][]string // 记录每个订单的事件序列
	mutex       sync.Mutex
}

func (l *OrderedMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	if l.orderStates == nil {
		l.orderStates = make(map[string][]string)
	}

	for _, msg := range msgs {
		l.mutex.Lock()

		orderId := msg.GetProperty("orderId")
		eventType := msg.GetProperty("eventType")

		fmt.Printf("\n[消费者] 收到顺序消息:\n")
		fmt.Printf("  OrderId: %s\n", orderId)
		fmt.Printf("  EventType: %s\n", eventType)
		fmt.Printf("  MsgId: %s\n", msg.MsgId)
		fmt.Printf("  QueueId: %d, Offset: %d\n", msg.QueueId, msg.QueueOffset)
		fmt.Printf("  消息内容: %s\n", string(msg.Body))

		// 记录事件序列
		if l.orderStates[orderId] == nil {
			l.orderStates[orderId] = make([]string, 0)
		}
		l.orderStates[orderId] = append(l.orderStates[orderId], eventType)

		// 验证事件顺序
		l.validateEventOrder(orderId, eventType)

		// 处理具体的业务逻辑
		l.processOrderEvent(orderId, eventType, msg)

		fmt.Printf("  [消费者] 当前订单事件序列: %v\n", l.orderStates[orderId])

		l.mutex.Unlock()

		// 模拟处理时间
		time.Sleep(100 * time.Millisecond)
	}

	return client.ConsumeSuccess
}

// 验证事件顺序
func (l *OrderedMessageListener) validateEventOrder(orderId, eventType string) {
	expectedOrder := []string{
		"ORDER_CREATED",
		"PAYMENT_RECEIVED",
		"INVENTORY_RESERVED",
		"ORDER_SHIPPED",
		"ORDER_DELIVERED",
	}

	currentEvents := l.orderStates[orderId]
	currentIndex := len(currentEvents) - 1

	if currentIndex < len(expectedOrder) && expectedOrder[currentIndex] == eventType {
		fmt.Printf("  [验证] ✓ 事件顺序正确: %s (位置: %d)\n", eventType, currentIndex+1)
	} else {
		fmt.Printf("  [验证] ✗ 事件顺序异常: 期望 %s, 实际 %s\n",
			getExpectedEvent(expectedOrder, currentIndex), eventType)
	}
}

// 获取期望的事件
func getExpectedEvent(expectedOrder []string, index int) string {
	if index >= 0 && index < len(expectedOrder) {
		return expectedOrder[index]
	}
	return "UNKNOWN"
}

// 处理订单事件
func (l *OrderedMessageListener) processOrderEvent(orderId, eventType string, msg *client.MessageExt) {
	switch eventType {
	case "ORDER_CREATED":
		fmt.Printf("  [业务处理] 处理订单创建事件 - OrderId: %s\n", orderId)
		// 初始化订单状态、发送确认邮件等

	case "PAYMENT_RECEIVED":
		fmt.Printf("  [业务处理] 处理支付接收事件 - OrderId: %s\n", orderId)
		// 更新订单状态、触发发货流程等

	case "INVENTORY_RESERVED":
		fmt.Printf("  [业务处理] 处理库存预留事件 - OrderId: %s\n", orderId)
		// 确认库存、准备发货等

	case "ORDER_SHIPPED":
		fmt.Printf("  [业务处理] 处理订单发货事件 - OrderId: %s\n", orderId)
		// 发送发货通知、更新物流信息等

	case "ORDER_DELIVERED":
		fmt.Printf("  [业务处理] 处理订单送达事件 - OrderId: %s\n", orderId)
		// 完成订单、发送评价邀请等

	default:
		fmt.Printf("  [业务处理] 处理未知事件类型: %s\n", eventType)
	}
}