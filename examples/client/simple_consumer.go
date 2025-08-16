//go:build simple_consumer

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
	log.Println("=== SimpleConsumer 示例 ===")

	sc := client.NewSimpleConsumer("simple_consumer_group")
	sc.Consumer.SetNameServerAddr("127.0.0.1:9876")
	// 可选：更新重试策略
	sc.SetRetryPolicy(&client.RetryPolicy{
		MaxRetryTimes:   5,
		RetryDelayLevel: client.DelayLevel5s,
		RetryInterval:   2 * time.Second,
		EnableRetry:     true,
	})

	// 可选：调整接收参数
	sc.SetAwaitDuration(5 * time.Second)
	sc.SetInvisibleDuration(20 * time.Second)
	sc.SetMaxMessageNum(8)

	if err := sc.Start(); err != nil {
		log.Fatalf("启动SimpleConsumer失败: %v", err)
	}
	defer sc.Stop()

	log.Println("SimpleConsumer已启动，开始接收消息...")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sig:
			log.Println("收到退出信号，停止接收...")
			return
		default:
			msgs, err := sc.ReceiveMessage(8, 20*time.Second)
			if err != nil {
				log.Printf("接收消息失败: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if len(msgs) == 0 {
				// 没有消息
				continue
			}

			for _, m := range msgs {
				log.Printf("收到消息: MsgId=%s, Topic=%s, Keys=%s, Body=%s", m.MsgId, m.Topic, m.Keys, string(m.Body))

				// 模拟业务处理耗时
				time.Sleep(500 * time.Millisecond)

				// 示范延长不可见时间（如果处理需要更久）
				if err := sc.ChangeInvisibleDuration(m, 30*time.Second); err != nil {
					log.Printf("延长不可见时间失败: %v", err)
				}

				// 业务处理成功则ACK
				if err := sc.AckMessage(m); err != nil {
					log.Printf("ACK失败，将尝试重试: %v", err)
					// 失败的情况下根据策略触发重试
					if rp := sc.GetRetryPolicy(); rp != nil && rp.EnableRetry {
						if err := sc.RetryMessage(m); err != nil {
							log.Printf("重试失败: %v", err)
						}
					}
				} else {
					log.Printf("ACK成功: %s", m.MsgId)
				}
			}
		}
	}
}