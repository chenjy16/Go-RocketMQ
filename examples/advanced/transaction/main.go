package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/chenjy16/go-rocketmq-client"
)

// 模拟业务数据
type OrderInfo struct {
	OrderId   string
	UserId    string
	Amount    float64
	Status    string
	Timestamp time.Time
}

// 模拟数据库
var orderDB = make(map[string]*OrderInfo)

func main() {
	fmt.Println("=== Go-RocketMQ 事务消息示例 ===")
	fmt.Println("本示例演示如何使用事务消息确保消息发送与本地事务的一致性")

	// 创建事务生产者
	producer := client.NewProducer("transaction_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启用消息追踪
	err := producer.EnableTrace("trace_topic", "trace_group")
	if err != nil {
		log.Printf("启用消息追踪失败: %v", err)
	}

	if err := producer.Start(); err != nil {
		log.Fatalf("启动事务生产者失败: %v", err)
	}
	defer producer.Shutdown()

	fmt.Println("事务生产者启动成功")

	// 模拟订单创建场景
	fmt.Println("\n--- 模拟订单创建场景 ---")
	createOrderWithTransaction(producer)

	// 模拟支付场景
	fmt.Println("\n--- 模拟支付场景 ---")
	processPaymentWithTransaction(producer)

	// 模拟库存扣减场景
	fmt.Println("\n--- 模拟库存扣减场景 ---")
	deductInventoryWithTransaction(producer)

	fmt.Println("\n所有事务消息示例完成")
}

// 创建订单的事务消息
func createOrderWithTransaction(producer *client.Producer) {
	for i := 0; i < 3; i++ {
		orderId := fmt.Sprintf("ORDER_%d_%d", time.Now().Unix(), i+1)
		userId := fmt.Sprintf("USER_%d", i+1)
		amount := 100.0 + float64(i*50)

		fmt.Printf("\n[事务] 开始创建订单 - OrderId: %s, UserId: %s, Amount: %.2f\n",
			orderId, userId, amount)

		// 创建事务消息
		msg := client.NewMessage(
			"OrderTopic",
			[]byte(fmt.Sprintf(`{
				"orderId": "%s",
				"userId": "%s",
				"amount": %.2f,
				"action": "CREATE_ORDER",
				"timestamp": "%s"
			}`, orderId, userId, amount, time.Now().Format("2006-01-02 15:04:05"))),
		).SetTags("ORDER_CREATE").SetKeys(orderId).SetProperty("orderId", orderId).SetProperty("userId", userId).SetProperty("transactionType", "ORDER_CREATE")

		// 发送事务消息（这里简化为同步发送，实际应该使用事务发送）
		result, err := sendTransactionMessage(producer, msg, &OrderTransactionExecutor{
			OrderId: orderId,
			UserId:  userId,
			Amount:  amount,
		})

		if err != nil {
			log.Printf("[事务] 订单创建失败: %v", err)
			continue
		}

		fmt.Printf("[事务] 订单创建成功 - MsgId: %s\n", result.MsgId)
		time.Sleep(1 * time.Second)
	}
}

// 支付处理的事务消息
func processPaymentWithTransaction(producer *client.Producer) {
	for i := 0; i < 2; i++ {
		orderId := fmt.Sprintf("ORDER_%d_%d", time.Now().Unix()-100, i+1)
		paymentId := fmt.Sprintf("PAY_%d_%d", time.Now().Unix(), i+1)
		amount := 150.0 + float64(i*30)

		fmt.Printf("\n[事务] 开始处理支付 - OrderId: %s, PaymentId: %s, Amount: %.2f\n",
			orderId, paymentId, amount)

		// 创建支付事务消息
		msg := client.NewMessage(
			"PaymentTopic",
			[]byte(fmt.Sprintf(`{
				"orderId": "%s",
				"paymentId": "%s",
				"amount": %.2f,
				"action": "PROCESS_PAYMENT",
				"timestamp": "%s"
			}`, orderId, paymentId, amount, time.Now().Format("2006-01-02 15:04:05"))),
		).SetTags("PAYMENT_PROCESS").SetKeys(paymentId).SetProperty("orderId", orderId).SetProperty("paymentId", paymentId).SetProperty("transactionType", "PAYMENT_PROCESS")

		// 发送支付事务消息
		result, err := sendTransactionMessage(producer, msg, &PaymentTransactionExecutor{
			OrderId:   orderId,
			PaymentId: paymentId,
			Amount:    amount,
		})

		if err != nil {
			log.Printf("[事务] 支付处理失败: %v", err)
			continue
		}

		fmt.Printf("[事务] 支付处理成功 - MsgId: %s\n", result.MsgId)
		time.Sleep(1 * time.Second)
	}
}

// 库存扣减的事务消息
func deductInventoryWithTransaction(producer *client.Producer) {
	products := []string{"PRODUCT_A", "PRODUCT_B", "PRODUCT_C"}

	for i, product := range products {
		orderId := fmt.Sprintf("ORDER_%d_%d", time.Now().Unix()-200, i+1)
		quantity := i + 1

		fmt.Printf("\n[事务] 开始扣减库存 - OrderId: %s, Product: %s, Quantity: %d\n",
			orderId, product, quantity)

		// 创建库存扣减事务消息
		msg := client.NewMessage(
			"InventoryTopic",
			[]byte(fmt.Sprintf(`{
				"orderId": "%s",
				"productId": "%s",
				"quantity": %d,
				"action": "DEDUCT_INVENTORY",
				"timestamp": "%s"
			}`, orderId, product, quantity, time.Now().Format("2006-01-02 15:04:05"))),
		).SetTags("INVENTORY_DEDUCT").SetKeys(orderId).SetProperty("orderId", orderId).SetProperty("productId", product).SetProperty("transactionType", "INVENTORY_DEDUCT")

		// 发送库存扣减事务消息
		result, err := sendTransactionMessage(producer, msg, &InventoryTransactionExecutor{
			OrderId:   orderId,
			ProductId: product,
			Quantity:  quantity,
		})

		if err != nil {
			log.Printf("[事务] 库存扣减失败: %v", err)
			continue
		}

		fmt.Printf("[事务] 库存扣减成功 - MsgId: %s\n", result.MsgId)
		time.Sleep(1 * time.Second)
	}
}

// 事务执行器接口
type TransactionExecutor interface {
	ExecuteLocalTransaction() error
	CheckLocalTransaction() bool
	GetTransactionId() string
}

// 订单事务执行器
type OrderTransactionExecutor struct {
	OrderId string
	UserId  string
	Amount  float64
}

func (e *OrderTransactionExecutor) ExecuteLocalTransaction() error {
	fmt.Printf("[本地事务] 执行订单创建 - OrderId: %s\n", e.OrderId)

	// 模拟本地事务执行
	order := &OrderInfo{
		OrderId:   e.OrderId,
		UserId:    e.UserId,
		Amount:    e.Amount,
		Status:    "CREATED",
		Timestamp: time.Now(),
	}

	// 模拟数据库操作
	orderDB[e.OrderId] = order

	// 模拟可能的失败情况
	if rand.Float32() < 0.1 { // 10%的失败率
		return fmt.Errorf("订单创建失败: 数据库异常")
	}

	fmt.Printf("[本地事务] 订单创建成功 - OrderId: %s, Status: %s\n", e.OrderId, order.Status)
	return nil
}

func (e *OrderTransactionExecutor) CheckLocalTransaction() bool {
	order, exists := orderDB[e.OrderId]
	if !exists {
		return false
	}
	return order.Status == "CREATED"
}

func (e *OrderTransactionExecutor) GetTransactionId() string {
	return e.OrderId
}

// 支付事务执行器
type PaymentTransactionExecutor struct {
	OrderId   string
	PaymentId string
	Amount    float64
}

func (e *PaymentTransactionExecutor) ExecuteLocalTransaction() error {
	fmt.Printf("[本地事务] 执行支付处理 - PaymentId: %s\n", e.PaymentId)

	// 模拟支付处理逻辑
	time.Sleep(100 * time.Millisecond)

	// 模拟可能的失败情况
	if rand.Float32() < 0.15 { // 15%的失败率
		return fmt.Errorf("支付处理失败: 余额不足")
	}

	fmt.Printf("[本地事务] 支付处理成功 - PaymentId: %s, Amount: %.2f\n", e.PaymentId, e.Amount)
	return nil
}

func (e *PaymentTransactionExecutor) CheckLocalTransaction() bool {
	// 模拟检查支付状态
	return true
}

func (e *PaymentTransactionExecutor) GetTransactionId() string {
	return e.PaymentId
}

// 库存事务执行器
type InventoryTransactionExecutor struct {
	OrderId   string
	ProductId string
	Quantity  int
}

func (e *InventoryTransactionExecutor) ExecuteLocalTransaction() error {
	fmt.Printf("[本地事务] 执行库存扣减 - ProductId: %s, Quantity: %d\n", e.ProductId, e.Quantity)

	// 模拟库存检查和扣减
	time.Sleep(50 * time.Millisecond)

	// 模拟可能的失败情况
	if rand.Float32() < 0.2 { // 20%的失败率
		return fmt.Errorf("库存扣减失败: 库存不足")
	}

	fmt.Printf("[本地事务] 库存扣减成功 - ProductId: %s, Quantity: %d\n", e.ProductId, e.Quantity)
	return nil
}

func (e *InventoryTransactionExecutor) CheckLocalTransaction() bool {
	// 模拟检查库存扣减状态
	return true
}

func (e *InventoryTransactionExecutor) GetTransactionId() string {
	return fmt.Sprintf("%s_%s", e.OrderId, e.ProductId)
}

// 发送事务消息（简化版本）
func sendTransactionMessage(producer *client.Producer, msg *client.Message, executor TransactionExecutor) (*client.SendResult, error) {
	fmt.Printf("[事务消息] 开始发送事务消息 - TransactionId: %s\n", executor.GetTransactionId())

	// 第一阶段：发送Half消息
	msg.SetProperty("TRAN_MSG", "true")
	msg.SetProperty("TRANSACTION_ID", executor.GetTransactionId())

	result, err := producer.SendSync(msg)
	if err != nil {
		return nil, fmt.Errorf("发送Half消息失败: %v", err)
	}

	fmt.Printf("[事务消息] Half消息发送成功 - MsgId: %s\n", result.MsgId)

	// 第二阶段：执行本地事务
	err = executor.ExecuteLocalTransaction()
	if err != nil {
		fmt.Printf("[事务消息] 本地事务执行失败，回滚消息 - Error: %v\n", err)
		// 这里应该发送回滚消息给Broker
		return nil, err
	}

	// 第三阶段：提交事务消息
	fmt.Printf("[事务消息] 本地事务执行成功，提交消息\n")
	// 这里应该发送提交消息给Broker

	return result, nil
}