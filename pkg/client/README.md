# Go-RocketMQ Client

这是一个独立的Go-RocketMQ客户端库，可以被第三方项目引入使用。

## 特性

- 🚀 **高性能**: 支持同步、异步和单向发送模式
- 🔄 **可靠性**: 内置重试机制和故障转移
- 📊 **监控**: 提供详细的发送和消费统计，支持消息追踪
- 🛡️ **安全**: 支持消息属性和标签过滤
- 🎯 **易用**: 简洁的API设计，易于集成
- 🔧 **多消费者类型**: 支持PushConsumer、PullConsumer和SimpleConsumer
- ⚖️ **负载均衡**: 支持多种负载均衡策略
- 🔍 **消息过滤**: 支持Tag和SQL92表达式过滤
- 📈 **事务消息**: 支持分布式事务消息
- ⏰ **延时消息**: 支持定时和延时消息发送
- 📦 **批量消息**: 支持批量消息发送和消费
- 🔄 **顺序消息**: 支持全局和分区顺序消息

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

#### 基础消费者

```go
package main

import (
    "log"
    "os"
    "os/signal"
    "syscall"
    
    client "github.com/chenjy16/go-rocketmq-client"
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
    
    // 设置负载均衡策略
    consumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})
    
    // 启用消息追踪
    consumer.EnableTrace("trace_topic", "trace_group")
    
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

#### 生产者增强功能

##### 启用消息追踪
```go
// 启用消息追踪
err := producer.EnableTrace("RMQ_SYS_TRACE_TOPIC", "trace_group")
if err != nil {
    log.Printf("Failed to enable trace: %v", err)
}
```

##### 事务消息
```go
// 创建事务生产者
txProducer := client.NewTransactionProducer(&client.ProducerConfig{
    GroupName: "tx_producer_group",
})

// 设置事务监听器
txProducer.SetTransactionListener(&MyTransactionListener{})

// 发送事务消息
msg := client.NewMessage("TransactionTopic", "Hello Transaction")
result, err := txProducer.SendMessageInTransaction(msg, nil)
if err != nil {
    log.Printf("Failed to send transaction message: %v", err)
}
```

##### 延时消息
```go
// 发送延时消息
msg := client.NewMessage("DelayTopic", "Hello Delay")
msg.SetDelayTimeLevel(3) // 延时级别3 (10秒)

result, err := producer.SendSync(msg)
if err != nil {
    log.Printf("Failed to send delay message: %v", err)
}
```

##### 批量消息
```go
// 创建批量消息
messages := []*client.Message{
    client.NewMessage("BatchTopic", "Message 1"),
    client.NewMessage("BatchTopic", "Message 2"),
    client.NewMessage("BatchTopic", "Message 3"),
}

// 发送批量消息
result, err := producer.SendBatch(messages)
if err != nil {
    log.Printf("Failed to send batch messages: %v", err)
}
```

### Consumer API

#### 创建消费者

##### 基础消费者
```go
config := &client.ConsumerConfig{
    GroupName:        "consumer_group",
    NameServerAddr:   "127.0.0.1:9876",
    ConsumeFromWhere: client.ConsumeFromLastOffset,
    MessageModel:     client.Clustering,
}
consumer := client.NewConsumer(config)
```

##### PushConsumer（推荐）
```go
pushConsumer := client.NewPushConsumer("push_consumer_group")
pushConsumer.SetNameServers([]string{"127.0.0.1:9876"})
pushConsumer.SetConsumeFromWhere(client.ConsumeFromLastOffset)
pushConsumer.SetMessageModel(client.Clustering)
pushConsumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})
```

##### PullConsumer
```go
pullConsumer := client.NewPullConsumer("pull_consumer_group")
pullConsumer.SetNameServers([]string{"127.0.0.1:9876"})
```

##### SimpleConsumer
```go
simpleConsumer := client.NewSimpleConsumer("simple_consumer_group")
simpleConsumer.SetNameServers([]string{"127.0.0.1:9876"})
simpleConsumer.SetAwaitDuration(10 * time.Second)
simpleConsumer.SetMaxMessageNum(16)
```

#### 消费者增强功能

##### 设置负载均衡策略
```go
// 平均分配策略
consumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})

// 一致性哈希策略
consumer.SetLoadBalanceStrategy(&client.ConsistentHashStrategy{})

// 机房就近策略
consumer.SetLoadBalanceStrategy(&client.RoomStrategy{})
```

##### 启用消息追踪
```go
err := consumer.EnableTrace("trace_topic", "trace_group")
if err != nil {
    log.Printf("Enable trace failed: %v", err)
}
```

##### 设置消息过滤器
```go
// Tag过滤器
tagFilter := &client.TagFilter{Tags: []string{"TagA", "TagB"}}
consumer.SetMessageFilter(tagFilter)

// SQL过滤器
sqlFilter := &client.SQLFilter{Expression: "age > 18 AND region = 'beijing'"}
consumer.SetMessageFilter(sqlFilter)
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
- **启用消息追踪**: 有助于问题排查和监控
- **事务消息**: 适用于需要保证本地事务与消息发送一致性的场景
- **批量发送**: 可以提高吞吐量，但要注意批量大小限制

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

#### 生产者错误处理

```go
// 发送消息错误处理
result, err := producer.SendSync(msg)
if err != nil {
    switch {
    case strings.Contains(err.Error(), "timeout"):
        log.Printf("Send timeout: %v", err)
        // 重试逻辑
    case strings.Contains(err.Error(), "broker not available"):
        log.Printf("Broker not available: %v", err)
        // 等待重连
    default:
        log.Printf("Send failed: %v", err)
    }
}

// 事务消息错误处理
result, err := txProducer.SendMessageInTransaction(msg, nil)
if err != nil {
    log.Printf("Transaction message send failed: %v", err)
    // 处理事务失败逻辑
}

// 批量消息错误处理
result, err := producer.SendBatch(messages)
if err != nil {
    log.Printf("Batch send failed: %v", err)
    // 可能需要拆分批量重试
}
```

#### 消费者错误处理

```go
// Push消费者错误处理
func (l *MyListener) ConsumeMessage(msgs []*client.MessageExt) client.ConsumeResult {
    for _, msg := range msgs {
        if err := processMessage(msg); err != nil {
            log.Printf("Process message failed: %v", err)
            return client.ReconsumeLater // 重试
        }
    }
    return client.ConsumeSuccess
}

// Simple消费者错误处理
messages, err := simpleConsumer.ReceiveMessage(10, 30*time.Second)
if err != nil {
    log.Printf("Receive message failed: %v", err)
    return
}

for _, msg := range messages {
    err := processMessage(msg)
    if err != nil {
        // 处理失败，可以选择重试或者改变不可见时间
        err = simpleConsumer.ChangeInvisibleDuration(msg, 60*time.Second)
        if err != nil {
            log.Printf("Change invisible duration failed: %v", err)
        }
    } else {
        // 处理成功，确认消息
        err = simpleConsumer.AckMessage(msg)
        if err != nil {
            log.Printf("Ack message failed: %v", err)
        }
    }
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

4. **事务消息问题**
   - 检查事务监听器实现
   - 确认本地事务执行状态
   - 检查事务回查逻辑
   - 查看事务消息日志

5. **负载均衡问题**
   - 检查负载均衡策略配置
   - 确认消费者实例数量
   - 检查队列数量设置
   - 查看Rebalance日志

6. **消息过滤问题**
   - 检查过滤器表达式语法
   - 确认消息Tag设置
   - 检查SQL过滤器编译
   - 验证消息属性设置

7. **消息追踪问题**
   - 检查追踪功能是否启用
   - 确认追踪Topic配置
   - 检查追踪消费者组设置
   - 查看追踪相关日志

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