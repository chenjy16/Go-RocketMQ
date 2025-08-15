package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"log"
	"strconv"
	"time"

	client "github.com/chenjy16/go-rocketmq-client"
)

// ACLConfig ACL配置
type ACLConfig struct {
	AccessKey string
	SecretKey string
	Enabled   bool
}

// ACLProducer 支持ACL的生产者
type ACLProducer struct {
	*client.Producer
	aclConfig *ACLConfig
}

// ACLConsumer 支持ACL的消费者
type ACLConsumer struct {
	*client.Consumer
	aclConfig *ACLConfig
}

func main() {
	fmt.Println("=== Go-RocketMQ 客户端ACL权限控制示例 ===")

	// ACL配置
	aclConfig := &ACLConfig{
		AccessKey: "RocketMQ",
		SecretKey: "12345678",
		Enabled:   true,
	}

	// 演示ACL生产者
	demoACLProducer(aclConfig)

	// 演示ACL消费者
	demoACLConsumer(aclConfig)

	fmt.Println("\n客户端ACL权限控制示例完成！")
}

// 演示ACL生产者
func demoACLProducer(aclConfig *ACLConfig) {
	fmt.Println("\n--- ACL生产者演示 ---")

	// 创建支持ACL的生产者
	producer := NewACLProducer("acl_producer_group", aclConfig)
	producer.SetNameServers([]string{"127.0.0.1:9876"})

	// 启动生产者
	if err := producer.Start(); err != nil {
		log.Fatalf("启动ACL生产者失败: %v", err)
	}
	defer producer.Shutdown()

	fmt.Println("ACL生产者启动成功")

	// 发送带ACL认证的消息
	for i := 0; i < 3; i++ {
		msg := client.NewMessage(
			"TestTopic",
			[]byte(fmt.Sprintf("ACL消息内容 #%d - %s", i+1, time.Now().Format("2006-01-02 15:04:05"))),
		)
		msg.SetTags("ACL")
		msg.SetKeys(fmt.Sprintf("ACL_KEY_%d", i+1))

		// 添加ACL认证信息
		if err := producer.AddACLSignature(msg); err != nil {
			log.Printf("添加ACL签名失败: %v", err)
			continue
		}

		// 发送消息
		result, err := producer.SendSync(msg)
		if err != nil {
			log.Printf("发送ACL消息失败: %v", err)
		} else {
			fmt.Printf("   ✓ ACL消息发送成功: %s\n", result.MsgId)
		}
	}
}

// 演示ACL消费者
func demoACLConsumer(aclConfig *ACLConfig) {
	fmt.Println("\n--- ACL消费者演示 ---")

	// 创建支持ACL的消费者
	consumer := NewACLConsumer(&client.ConsumerConfig{
		GroupName:        "acl_consumer_group",
		NameServerAddr:   "127.0.0.1:9876",
		ConsumeFromWhere: client.ConsumeFromLastOffset,
		MessageModel:     client.Clustering,
	}, aclConfig)

	// 订阅Topic
	listener := &ACLMessageListener{aclConfig: aclConfig}
	err := consumer.Subscribe("TestTopic", "ACL", listener)
	if err != nil {
		log.Fatalf("订阅Topic失败: %v", err)
	}

	// 启动消费者
	if err := consumer.Start(); err != nil {
		log.Fatalf("启动ACL消费者失败: %v", err)
	}
	defer consumer.Stop()

	fmt.Println("ACL消费者启动成功")

	// 运行一段时间
	time.Sleep(5 * time.Second)
	fmt.Println("ACL消费者演示完成")
}

// NewACLProducer 创建支持ACL的生产者
func NewACLProducer(groupName string, aclConfig *ACLConfig) *ACLProducer {
	producer := client.NewProducer(groupName)
	return &ACLProducer{
		Producer:  producer,
		aclConfig: aclConfig,
	}
}

// AddACLSignature 为消息添加ACL签名
func (p *ACLProducer) AddACLSignature(msg *client.Message) error {
	if !p.aclConfig.Enabled {
		return nil
	}

	// 生成时间戳
	timestamp := time.Now().Unix()

	// 构造签名字符串
	signatureString := fmt.Sprintf("%s\n%s\n%d",
		p.aclConfig.AccessKey,
		msg.Topic,
		timestamp,
	)

	// 生成HMAC-SHA1签名
	signature, err := generateSignature(signatureString, p.aclConfig.SecretKey)
	if err != nil {
		return fmt.Errorf("生成签名失败: %v", err)
	}

	// 设置ACL属性
	msg.SetProperty("AccessKey", p.aclConfig.AccessKey)
	msg.SetProperty("Signature", signature)
	msg.SetProperty("Timestamp", strconv.FormatInt(timestamp, 10))

	return nil
}

// NewACLConsumer 创建支持ACL的消费者
func NewACLConsumer(config *client.ConsumerConfig, aclConfig *ACLConfig) *ACLConsumer {
	consumer := client.NewConsumer(config)
	return &ACLConsumer{
		Consumer:  consumer,
		aclConfig: aclConfig,
	}
}

// ACLMessageListener ACL消息监听器
type ACLMessageListener struct {
	aclConfig *ACLConfig
}

// ConsumeMessage 消费消息
func (l *ACLMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
	for _, msg := range msgs {
		// 验证ACL信息
		if l.aclConfig.Enabled {
			if err := l.validateACL(msg); err != nil {
				log.Printf("ACL验证失败: %v", err)
				continue
			}
		}

		// 处理消息
		fmt.Printf("   ✓ 接收到ACL消息: %s, 内容: %s\n",
			msg.MsgId, string(msg.Body))
	}
	return client.ConsumeSuccess
}

// validateACL 验证ACL信息
func (l *ACLMessageListener) validateACL(msg *client.MessageExt) error {
	// 获取ACL属性
	accessKey := msg.GetProperty("AccessKey")
	signature := msg.GetProperty("Signature")
	timestampStr := msg.GetProperty("Timestamp")

	if accessKey == "" || signature == "" || timestampStr == "" {
		return fmt.Errorf("缺少ACL认证信息")
	}

	// 验证AccessKey
	if accessKey != l.aclConfig.AccessKey {
		return fmt.Errorf("AccessKey验证失败")
	}

	// 解析时间戳
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return fmt.Errorf("时间戳格式错误: %v", err)
	}

	// 检查时间戳有效性（5分钟内）
	now := time.Now().Unix()
	if now-timestamp > 300 || timestamp-now > 300 {
		return fmt.Errorf("时间戳已过期")
	}

	// 重新计算签名进行验证
	signatureString := fmt.Sprintf("%s\n%s\n%d",
		accessKey,
		msg.Topic,
		timestamp,
	)

	expectedSignature, err := generateSignature(signatureString, l.aclConfig.SecretKey)
	if err != nil {
		return fmt.Errorf("计算签名失败: %v", err)
	}

	if signature != expectedSignature {
		return fmt.Errorf("签名验证失败")
	}

	return nil
}

// generateSignature 生成HMAC-SHA1签名
func generateSignature(data, secretKey string) (string, error) {
	h := hmac.New(sha1.New, []byte(secretKey))
	_, err := h.Write([]byte(data))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

// 演示ACL配置管理
func demoACLConfigManagement() {
	fmt.Println("\n--- ACL配置管理演示 ---")

	// 创建ACL配置
	aclConfig := &ACLConfig{
		AccessKey: "testUser",
		SecretKey: "testSecret",
		Enabled:   true,
	}

	fmt.Printf("ACL配置: AccessKey=%s, Enabled=%v\n",
		aclConfig.AccessKey, aclConfig.Enabled)

	// 测试签名生成
	testData := "testUser\nTestTopic\n" + strconv.FormatInt(time.Now().Unix(), 10)
	signature, err := generateSignature(testData, aclConfig.SecretKey)
	if err != nil {
		log.Printf("签名生成失败: %v", err)
	} else {
		fmt.Printf("生成的签名: %s\n", signature)
	}

	// 演示配置切换
	aclConfig.Enabled = false
	fmt.Printf("ACL已禁用: Enabled=%v\n", aclConfig.Enabled)

	aclConfig.Enabled = true
	fmt.Printf("ACL已启用: Enabled=%v\n", aclConfig.Enabled)
}