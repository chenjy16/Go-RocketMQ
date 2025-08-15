# Go-RocketMQ Client

这是一个独立的Go-RocketMQ客户端库，可以被第三方项目引入使用。

## 特性

- 🚀 **高性能**: 支持同步、异步和单向发送模式
- 🔄 **可靠性**: 内置重试机制和故障转移
- 📊 **监控**: 提供详细的发送和消费统计
- 🛡️ **安全**: 支持消息属性和标签过滤
- 🎯 **易用**: 简洁的API设计，易于集成

## 安装

```bash
go get github.com/chenjy16/go-rocketmq-client
```

## 快速开始

### 发送消息

```go
package main

import (
    "log"
    
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建生产者
    producer := client.NewProducer("my_producer_group")
    
    // 设置NameServer地址
    producer.SetNameServers([]string{"127.0.0.1:9876"})
    
    // 启动生产者
    if err := producer.Start(); err != nil {
        log.Fatalf("Failed to start producer: %v", err)
    }
    defer producer.Shutdown()
    
    // 创建消息
    msg := client.NewMessage("TestTopic", []byte("Hello RocketMQ!"))
    msg.SetTags("TagA")
    msg.SetKeys("OrderID_001")
    
    // 同步发送消息
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Fatalf("Failed to send message: %v", err)
    }
    
    log.Printf("Message sent successfully: %s", result.MsgId)
}
```

### 消费消息

```go
package main

import (
    "log"
    "os"
    "os/signal"
    "syscall"
    
    client "github.com/your-org/go-rocketmq-client"
)

// 实现消息监听器
type MyMessageListener struct{}

func (l *MyMessageListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
    for _, msg := range msgs {
        log.Printf("Received message: %s", string(msg.Body))
        // 处理业务逻辑
    }
    return client.ConsumeSuccess
}

func main() {
    // 创建消费者配置
    config := &client.ConsumerConfig{
        GroupName:        "my_consumer_group",
        NameServerAddr:   "127.0.0.1:9876",
        ConsumeFromWhere: client.ConsumeFromLastOffset,
        MessageModel:     client.Clustering,
    }
    
    // 创建消费者
    consumer := client.NewConsumer(config)
    
    // 订阅Topic
    listener := &MyMessageListener{}
    err := consumer.Subscribe("TestTopic", "*", listener)
    if err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }
    
    // 启动消费者
    if err := consumer.Start(); err != nil {
        log.Fatalf("Failed to start consumer: %v", err)
    }
    defer consumer.Stop()
    
    log.Println("Consumer started, waiting for messages...")
    
    // 等待中断信号
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
    
    log.Println("Shutting down consumer...")
}
```

## API 文档

### Producer API

#### 创建生产者
```go
producer := client.NewProducer("producer_group_name")
```

#### 配置NameServer
```go
producer.SetNameServers([]string{"127.0.0.1:9876", "127.0.0.1:9877"})
```

#### 启动和关闭
```go
// 启动
err := producer.Start()

// 关闭
producer.Shutdown()
```

#### 发送消息
```go
// 同步发送
result, err := producer.SendSync(msg)

// 异步发送
err := producer.SendAsync(msg, func(result *client.SendResult, err error) {
    if err != nil {
        log.Printf("Send failed: %v", err)
    } else {
        log.Printf("Send success: %s", result.MsgId)
    }
})

// 单向发送（不关心结果）
err := producer.SendOneway(msg)
```

### Consumer API

#### 创建消费者
```go
config := &client.ConsumerConfig{
    GroupName:        "consumer_group",
    NameServerAddr:   "127.0.0.1:9876",
    ConsumeFromWhere: client.ConsumeFromLastOffset,
    MessageModel:     client.Clustering,
}
consumer := client.NewConsumer(config)
```

#### 订阅Topic
```go
// 订阅所有消息
err := consumer.Subscribe("TopicName", "*", listener)

// 订阅特定标签
err := consumer.Subscribe("TopicName", "TagA || TagB", listener)
```

#### 启动和停止
```go
// 启动
err := consumer.Start()

// 停止
err := consumer.Stop()
```

### Message API

#### 创建消息
```go
msg := client.NewMessage("TopicName", []byte("message body"))
```

#### 设置消息属性
```go
// 设置标签
msg.SetTags("TagA")

// 设置键
msg.SetKeys("OrderID_001")

// 设置自定义属性
msg.SetProperty("userId", "12345")
msg.SetProperty("source", "web")
```

## 配置选项

### 生产者配置

```go
type ProducerConfig struct {
    GroupName                        string        // 生产者组名
    NameServers                      []string      // NameServer地址列表
    SendMsgTimeout                   time.Duration // 发送超时时间
    CompressMsgBodyOver              int32         // 消息体压缩阈值
    RetryTimesWhenSendFailed         int32         // 同步发送失败重试次数
    RetryTimesWhenSendAsyncFailed    int32         // 异步发送失败重试次数
    RetryAnotherBrokerWhenNotStoreOK bool          // 存储失败时是否重试其他Broker
    MaxMessageSize                   int32         // 最大消息大小
}
```

### 消费者配置

```go
type ConsumerConfig struct {
    GroupName            string                // 消费者组名
    NameServerAddr       string                // NameServer地址
    ConsumeFromWhere     ConsumeFromWhere      // 消费起始位置
    MessageModel         MessageModel          // 消息模式（集群/广播）
    ConsumeThreadMin     int                   // 最小消费线程数
    ConsumeThreadMax     int                   // 最大消费线程数
    PullInterval         time.Duration         // 拉取间隔
    PullBatchSize        int32                 // 批量拉取大小
    ConsumeTimeout       time.Duration         // 消费超时时间
}
```

## 最佳实践

### 1. 生产者最佳实践

- **复用生产者实例**: 一个应用中同一个生产者组只需要一个Producer实例
- **合理设置超时**: 根据网络环境调整发送超时时间
- **使用异步发送**: 对于高吞吐量场景，推荐使用异步发送
- **设置消息键**: 为消息设置唯一键，便于问题排查

```go
// 推荐的生产者配置
producer := client.NewProducer("my_producer_group")
producer.SetNameServers([]string{"127.0.0.1:9876"})

// 设置合理的超时时间
config := producer.GetConfig()
config.SendMsgTimeout = 3 * time.Second
config.RetryTimesWhenSendFailed = 2
```

### 2. 消费者最佳实践

- **幂等消费**: 确保消息处理逻辑是幂等的
- **快速消费**: 避免在消费逻辑中执行耗时操作
- **合理设置线程数**: 根据消费能力调整线程池大小
- **监控消费进度**: 定期检查消费延迟

```go
// 推荐的消费者配置
config := &client.ConsumerConfig{
    GroupName:        "my_consumer_group",
    NameServerAddr:   "127.0.0.1:9876",
    ConsumeFromWhere: client.ConsumeFromLastOffset,
    MessageModel:     client.Clustering,
    ConsumeThreadMin: 5,
    ConsumeThreadMax: 20,
    PullBatchSize:    32,
    ConsumeTimeout:   15 * time.Second,
}
```

### 3. 错误处理

```go
// 生产者错误处理
result, err := producer.SendSync(msg)
if err != nil {
    log.Printf("Send failed: %v", err)
    // 根据错误类型进行相应处理
    return
}

// 消费者错误处理
func (l *MyListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
    for _, msg := range msgs {
        if err := processMessage(msg); err != nil {
            log.Printf("Process message failed: %v", err)
            return client.ReconsumeLater // 重试
        }
    }
    return client.ConsumeSuccess
}
```

## 故障排除

### 常见问题

1. **连接NameServer失败**
   - 检查NameServer地址是否正确
   - 确认NameServer服务是否启动
   - 检查网络连通性

2. **发送消息失败**
   - 检查Topic是否存在
   - 确认Broker服务是否正常
   - 检查消息大小是否超限

3. **消费消息延迟**
   - 检查消费者线程数配置
   - 优化消费逻辑性能
   - 检查网络延迟

### 日志配置

```go
import "log"

// 启用详细日志
log.SetFlags(log.LstdFlags | log.Lshortfile)
```

## 许可证

Apache License 2.0

## 贡献

欢迎提交Issue和Pull Request！

## 支持

如有问题，请提交Issue或联系维护者。