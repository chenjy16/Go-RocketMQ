//go:build batch_trace_acl

package main

import (
	"log"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
	log.Println("=== 批量发送 + 追踪 + ACL 示例 ===")

	producer := client.NewProducer("advanced_producer_group")
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启用消息追踪（使用默认追踪Topic）
	if err := producer.EnableTrace("127.0.0.1:9876", "RMQ_SYS_TRACE_TOPIC"); err != nil {
		log.Printf("启用追踪失败: %v", err)
	}

	// 启用 ACL（示例使用默认签名算法）
	// 请确保 Broker 开启 AclEnable 并且 config/plain_acl.yml 配置了对应的 accessKey/secretKey
	if err := producer.EnableACL("rocketmq_access", "rocketmq_secret"); err != nil {
		log.Printf("启用ACL失败: %v", err)
	}

	// 启动生产者
	if err := producer.Start(); err != nil {
		log.Fatalf("启动生产者失败: %v", err)
	}
	defer producer.Shutdown()

	// 组装批量消息
	msgs := []*client.Message{
		client.NewMessage("BatchTopic", []byte("Batch Message 1")).SetTags("TagA").SetKeys("BID_001"),
		client.NewMessage("BatchTopic", []byte("Batch Message 2")).SetTags("TagB").SetKeys("BID_002"),
		client.NewMessage("BatchTopic", []byte("Batch Message 3")).SetTags("TagA").SetKeys("BID_003"),
	}

	// 批量发送
	result, err := producer.SendBatchMessages(msgs)
	if err != nil {
		log.Printf("批量发送失败: %v", err)
	} else {
		log.Printf("批量发送成功: MsgId=%s, Offset=%d", result.MsgId, result.QueueOffset)
	}

	// 再发送一个定时（计划）消息示例
	scheduled := client.NewMessage("BatchTopic", []byte("Scheduled Message"))
	// 设置10秒后投递
	_, err = producer.SendScheduledMessage(scheduled, time.Now().Add(10*time.Second))
	if err != nil {
		log.Printf("计划消息发送失败: %v", err)
	} else {
		log.Printf("计划消息发送成功: 将在10秒后投递")
	}

	log.Println("示例完成")
}