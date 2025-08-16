//go:build tx_producer

package main

import (
	"log"
	"math/rand"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

// 实现事务监听器
type MyTransactionListener struct{}

func (l *MyTransactionListener) ExecuteLocalTransaction(msg *client.Message, arg interface{}) client.LocalTransactionState {
	log.Printf("执行本地事务: topic=%s, keys=%s", msg.Topic, msg.Keys)
	// 模拟本地事务执行
	time.Sleep(200 * time.Millisecond)
	// 随机成功或未知以演示
	if rand.Intn(100)%2 == 0 {
		log.Println("本地事务执行成功，提交消息")
		return client.CommitMessage
	}
	log.Println("本地事务未知状态")
	return client.UnknownMessage
}

func (l *MyTransactionListener) CheckLocalTransaction(msgExt *client.MessageExt) client.LocalTransactionState {
	log.Printf("回查本地事务: msgId=%s, keys=%s", msgExt.MsgId, msgExt.Keys)
	// 简单返回提交
	return client.CommitMessage
}

func main() {
	log.Println("=== 事务消息生产者示例 ===")

	// 创建事务生产者（根据实现需要传入监听器）
	txProducer := client.NewTransactionProducer("tx_producer_group", &MyTransactionListener{})
	// 设置NameServer地址
	txProducer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	if err := txProducer.Start(); err != nil {
		log.Fatalf("启动事务生产者失败: %v", err)
	}
	defer txProducer.Shutdown()

	// 构建事务消息
	msg := client.NewMessage("TransactionTopic", []byte("Hello Transaction"))
	msg.SetTags("TxTag").SetKeys("TxOrder_001")

	// 发送事务消息
	result, err := txProducer.SendMessageInTransaction(msg, nil)
	if err != nil {
		log.Printf("事务消息发送失败: %v", err)
	} else {
		log.Printf("事务消息发送成功: MsgId=%s", result.MsgId)
	}
}