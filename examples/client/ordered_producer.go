//go:build ordered_producer

package main

import (
	"fmt"
	"log"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
	log.Println("=== 顺序消息生产者示例 ===")

	// 创建生产者
	producer := client.NewProducer("ordered_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	if err := producer.Start(); err != nil {
		log.Fatalf("启动生产者失败: %v", err)
	}
	defer producer.Shutdown()

	log.Println("顺序消息生产者启动成功")

	// 示例1: 使用OrderedMessageQueueSelector发送顺序消息
	log.Println("\n--- 使用OrderedMessageQueueSelector发送顺序消息 ---")
	selector := &client.OrderedMessageQueueSelector{}
	
	// 模拟订单流程：下单 -> 支付 -> 发货
	orderSteps := []string{"订单创建", "订单支付", "订单发货"}
	orderId := "ORDER_12345"
	
	for stepIndex, step := range orderSteps {
		msg := client.NewMessage("OrderTopic", []byte(fmt.Sprintf("订单%s: %s", orderId, step)))
		msg.SetTags("OrderFlow")
		msg.SetKeys(orderId)
		msg.SetShardingKey(orderId) // 使用订单ID作为分片键，确保同一订单的消息发送到同一队列
		msg.SetProperty("orderId", orderId)
		msg.SetProperty("step", fmt.Sprintf("%d", stepIndex+1))
		msg.SetProperty("stepName", step)

		// 发送顺序消息，使用订单ID作为选择参数
		result, err := producer.SendOrderedMessage(msg, selector, orderId)
		if err != nil {
			log.Printf("发送顺序消息失败: %v", err)
		} else {
			log.Printf("发送顺序消息成功: 订单=%s, 步骤=%s, MsgId=%s, Queue=%d", 
				orderId, step, result.MsgId, result.MessageQueue.QueueId)
		}
		
		// 间隔一点时间，模拟真实业务场景
		time.Sleep(500 * time.Millisecond)
	}

	// 示例2: 使用DefaultMessageQueueSelector（哈希选择）
	log.Println("\n--- 使用DefaultMessageQueueSelector发送顺序消息 ---")
	defaultSelector := &client.DefaultMessageQueueSelector{}
	
	// 模拟用户操作流程
	userActions := []string{"登录", "浏览商品", "加入购物车", "下单", "支付"}
	userId := "USER_67890"
	
	for actionIndex, action := range userActions {
		msg := client.NewMessage("UserActionTopic", []byte(fmt.Sprintf("用户%s: %s", userId, action)))
		msg.SetTags("UserFlow")
		msg.SetKeys(userId)
		msg.SetShardingKey(userId) // 使用用户ID作为分片键
		msg.SetProperty("userId", userId)
		msg.SetProperty("action", action)
		msg.SetProperty("sequence", fmt.Sprintf("%d", actionIndex+1))
		msg.SetProperty("timestamp", fmt.Sprintf("%d", time.Now().Unix()))

		// 发送顺序消息，使用用户ID作为选择参数
		result, err := producer.SendOrderedMessage(msg, defaultSelector, userId)
		if err != nil {
			log.Printf("发送用户行为消息失败: %v", err)
		} else {
			log.Printf("发送用户行为消息成功: 用户=%s, 行为=%s, MsgId=%s, Queue=%d", 
				userId, action, result.MsgId, result.MessageQueue.QueueId)
		}
		
		time.Sleep(300 * time.Millisecond)
	}

	// 示例3: 使用RoundRobinMessageQueueSelector
	log.Println("\n--- 使用RoundRobinMessageQueueSelector发送消息 ---")
	roundRobinSelector := &client.RoundRobinMessageQueueSelector{}
	
	// 发送多个游戏事件消息
	gameEvents := []string{"玩家上线", "开始游戏", "获得道具", "升级", "玩家下线"}
	playerId := "PLAYER_ABCDE"
	
	for eventIndex, event := range gameEvents {
		msg := client.NewMessage("GameEventTopic", []byte(fmt.Sprintf("玩家%s: %s", playerId, event)))
		msg.SetTags("GameEvent")
		msg.SetKeys(playerId)
		msg.SetShardingKey(playerId)
		msg.SetProperty("playerId", playerId)
		msg.SetProperty("event", event)
		msg.SetProperty("level", fmt.Sprintf("%d", eventIndex+1))

		result, err := producer.SendOrderedMessage(msg, roundRobinSelector, playerId)
		if err != nil {
			log.Printf("发送游戏事件消息失败: %v", err)
		} else {
			log.Printf("发送游戏事件消息成功: 玩家=%s, 事件=%s, MsgId=%s, Queue=%d", 
				playerId, event, result.MsgId, result.MessageQueue.QueueId)
		}
		
		time.Sleep(200 * time.Millisecond)
	}

	log.Println("\n顺序消息生产者示例完成")
	log.Println("提示: 同一分片键的消息会被发送到同一队列，确保消费时的顺序性")
}