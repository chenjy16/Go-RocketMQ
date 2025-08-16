//go:build pull_consumer

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
	log.Println("=== Pull 消费者示例 ===")

	// 创建Pull消费者
	pullConsumer := client.NewPullConsumer("pull_consumer_group")
	pullConsumer.Consumer.SetNameServerAddr("127.0.0.1:9876")
	pullConsumer.SetPullTimeout(10 * time.Second)

	// 启动消费者
	if err := pullConsumer.Start(); err != nil {
		log.Fatalf("启动Pull消费者失败: %v", err)
	}
	defer pullConsumer.Stop()

	log.Println("Pull消费者已启动，开始手动拉取消息...")

	// 创建消息队列
	messageQueue := &client.MessageQueue{
		Topic:      "TestTopic",
		BrokerName: "broker-a",
		QueueId:    0,
	}

	// 维护消费偏移量
	offset := int64(0)
	maxNums := int32(32)

	// 创建退出信号通道
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sigChan:
			log.Println("收到退出信号，停止拉取消息...")
			return

		case <-ticker.C:
			// 阻塞拉取消息
			log.Printf("从队列 %s:%d 偏移量 %d 开始拉取消息", messageQueue.Topic, messageQueue.QueueId, offset)
			
			pullResult, err := pullConsumer.PullBlockIfNotFound(messageQueue, "*", offset, maxNums)
			if err != nil {
				log.Printf("拉取消息失败: %v", err)
				continue
			}

			// 处理拉取结果
			switch pullResult.PullStatus {
			case client.PullFound:
				log.Printf("成功拉取到 %d 条消息", len(pullResult.MsgFoundList))
				
				// 处理消息
				for _, msg := range pullResult.MsgFoundList {
					log.Printf("处理消息: MsgId=%s, Topic=%s, Tags=%s, Keys=%s, Body=%s",
						msg.MsgId, msg.Topic, msg.Tags, msg.Keys, string(msg.Body))
					
					// 这里可以加入业务逻辑处理
					processMessage(msg)
				}
				
				// 更新偏移量
				offset = pullResult.NextBeginOffset
				log.Printf("更新偏移量为: %d", offset)

			case client.PullNoNewMsg:
				log.Println("没有新消息")

			case client.PullNoMatchedMsg:
				log.Println("没有匹配的消息")
				offset = pullResult.NextBeginOffset

			case client.PullOffsetIllegal:
				log.Printf("偏移量非法，重置为: %d", pullResult.NextBeginOffset)
				offset = pullResult.NextBeginOffset
			}

			// 显示拉取状态
			log.Printf("拉取状态 - MinOffset: %d, MaxOffset: %d, NextOffset: %d",
				pullResult.MinOffset, pullResult.MaxOffset, pullResult.NextBeginOffset)
		}
	}
}

// processMessage 处理单条消息
func processMessage(msg *client.MessageExt) {
	// 模拟业务处理
	time.Sleep(100 * time.Millisecond)
	
	log.Printf("消息处理完成: %s", msg.MsgId)
}