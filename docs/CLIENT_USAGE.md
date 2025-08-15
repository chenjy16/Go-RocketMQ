# 客户端库使用指南

## 概述

本项目提供了两种使用 RocketMQ 客户端的方式：

1. **独立客户端库** - 推荐用于第三方项目集成
2. **本地开发模式** - 用于本项目内部开发和示例

## 独立客户端库使用 (推荐)

### 安装

```bash
go get github.com/chenjy16/go-rocketmq-client
```

### 导入

```go
import "github.com/chenjy16/go-rocketmq-client"
```

### 基本使用示例

#### 生产者

##### 基础生产者

```go
package main

import (
    "fmt"
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建生产者
    producer, err := client.NewProducer("test_producer_group")
    if err != nil {
        log.Fatal(err)
    }
    
    // 设置 NameServer 地址
    producer.SetNameServers([]string{"localhost:9876"})
    
    // 启动生产者
    if err := producer.Start(); err != nil {
        log.Fatal(err)
    }
    defer producer.Shutdown()
    
    // 创建消息
    msg := &client.Message{
        Topic: "test_topic",
        Body:  []byte("Hello RocketMQ"),
    }
    
    // 发送消息
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("消息发送成功: %s\n", result.MsgID)
}
```

##### 启用消息追踪

```go
package main

import (
    "fmt"
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建生产者
    producer, err := client.NewProducer("trace_producer_group")
    if err != nil {
        log.Fatal(err)
    }
    
    producer.SetNameServers([]string{"localhost:9876"})
    
    // 启用消息追踪
    producer.EnableTrace("trace_topic", "producer_instance")
    
    if err := producer.Start(); err != nil {
        log.Fatal(err)
    }
    defer producer.Shutdown()
    
    // 发送带追踪的消息
    msg := &client.Message{
        Topic: "test_topic",
        Body:  []byte("Hello RocketMQ with Trace"),
    }
    
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("带追踪的消息发送成功: %s\n", result.MsgID)
}
```

##### 批量发送消息

```go
package main

import (
    "fmt"
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建生产者
    producer, err := client.NewProducer("batch_producer_group")
    if err != nil {
        log.Fatal(err)
    }
    
    producer.SetNameServers([]string{"localhost:9876"})
    
    if err := producer.Start(); err != nil {
        log.Fatal(err)
    }
    defer producer.Shutdown()
    
    // 创建批量消息
    var messages []*client.Message
    for i := 0; i < 10; i++ {
        msg := &client.Message{
            Topic: "test_topic",
            Body:  []byte(fmt.Sprintf("Batch message %d", i)),
        }
        messages = append(messages, msg)
    }
    
    // 批量发送消息
    result, err := producer.SendBatchMessages(messages)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("批量消息发送成功: %s\n", result.MsgID)
}
```

##### 事务消息

```go
package main

import (
    "fmt"
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

// 实现事务监听器
type MyTransactionListener struct{}

func (l *MyTransactionListener) ExecuteLocalTransaction(msg *client.Message, arg interface{}) client.LocalTransactionState {
    // 执行本地事务逻辑
    fmt.Printf("执行本地事务: %s\n", string(msg.Body))
    
    // 根据业务逻辑返回事务状态
    return client.CommitMessage // 或 client.RollbackMessage
}

func (l *MyTransactionListener) CheckLocalTransaction(msgExt *client.MessageExt) client.LocalTransactionState {
    // 检查本地事务状态
    fmt.Printf("检查本地事务: %s\n", msgExt.MsgId)
    
    // 根据业务逻辑返回事务状态
    return client.CommitMessage
}

func main() {
    // 创建事务监听器
    listener := &MyTransactionListener{}
    
    // 创建事务生产者
    txProducer, err := client.NewTransactionProducer("tx_producer_group", listener)
    if err != nil {
        log.Fatal(err)
    }
    
    txProducer.SetNameServers([]string{"localhost:9876"})
    
    if err := txProducer.Start(); err != nil {
        log.Fatal(err)
    }
    defer txProducer.Shutdown()
    
    // 创建事务消息
    msg := &client.Message{
        Topic: "test_topic",
        Body:  []byte("Transaction Message"),
    }
    
    // 发送事务消息
    result, err := txProducer.SendMessageInTransaction(msg, nil)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("事务消息发送成功: %s\n", result.MsgID)
}
```

#### 消费者

##### 基础消费者（推荐用于简单场景）

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建基础消费者
    consumer, err := client.NewConsumer("test_consumer_group")
    if err != nil {
        log.Fatal(err)
    }
    
    // 设置 NameServer 地址
    consumer.SetNameServers([]string{"localhost:9876"})
    
    // 订阅主题
    if err := consumer.Subscribe("test_topic", "*"); err != nil {
        log.Fatal(err)
    }
    
    // 注册消息监听器
    consumer.RegisterMessageListener(func(msgs []*client.MessageExt) client.ConsumeResult {
        for _, msg := range msgs {
            fmt.Printf("收到消息: %s\n", string(msg.Body))
        }
        return client.ConsumeSuccess
    })
    
    // 启动消费者
    if err := consumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer consumer.Shutdown()
    
    // 保持程序运行
    time.Sleep(time.Hour)
}
```

##### Push消费者（推荐用于高吞吐量场景）

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建Push消费者
    pushConsumer := client.NewPushConsumer("push_consumer_group")
    pushConsumer.SetNameServers([]string{"localhost:9876"})
    
    // 设置负载均衡策略
    pushConsumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})
    
    // 订阅主题
    if err := pushConsumer.Subscribe("test_topic", "*"); err != nil {
        log.Fatal(err)
    }
    
    // 注册消息监听器
    pushConsumer.RegisterMessageListener(func(msgs []*client.MessageExt) client.ConsumeResult {
        for _, msg := range msgs {
            fmt.Printf("Push消费者收到消息: %s\n", string(msg.Body))
        }
        return client.ConsumeSuccess
    })
    
    // 启动消费者
    if err := pushConsumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer pushConsumer.Stop()
    
    time.Sleep(time.Hour)
}
```

##### Pull消费者（适用于需要精确控制消费进度的场景）

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建Pull消费者
    pullConsumer := client.NewPullConsumer("pull_consumer_group")
    pullConsumer.SetNameServers([]string{"localhost:9876"})
    
    // 启动消费者
    if err := pullConsumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer pullConsumer.Stop()
    
    // 手动拉取消息
    for {
        // 获取消息队列
        queues, err := pullConsumer.FetchSubscribeMessageQueues("test_topic")
        if err != nil {
            log.Printf("获取消息队列失败: %v", err)
            time.Sleep(5 * time.Second)
            continue
        }
        
        for _, queue := range queues {
            // 拉取消息
            result, err := pullConsumer.PullBlockIfNotFound(queue, "", 0, 32)
            if err != nil {
                log.Printf("拉取消息失败: %v", err)
                continue
            }
            
            for _, msg := range result.Messages {
                fmt.Printf("Pull消费者收到消息: %s\n", string(msg.Body))
            }
        }
        
        time.Sleep(1 * time.Second)
    }
}
```

##### Simple消费者（轻量级消费者）

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建Simple消费者
    simpleConsumer := client.NewSimpleConsumer("simple_consumer_group")
    simpleConsumer.SetNameServers([]string{"localhost:9876"})
    
    // 订阅主题
    if err := simpleConsumer.Subscribe("test_topic", "*"); err != nil {
        log.Fatal(err)
    }
    
    // 注册消息监听器
    simpleConsumer.RegisterMessageListener(func(msgs []*client.MessageExt) client.ConsumeResult {
        for _, msg := range msgs {
            fmt.Printf("Simple消费者收到消息: %s\n", string(msg.Body))
        }
        return client.ConsumeSuccess
    })
    
    // 启动消费者
    if err := simpleConsumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer simpleConsumer.Stop()
    
    time.Sleep(time.Hour)
}
```

## 高级配置

### 负载均衡策略

客户端支持多种负载均衡策略，可以根据业务需求选择合适的策略：

```go
package main

import (
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建Push消费者
    pushConsumer := client.NewPushConsumer("consumer_group")
    pushConsumer.SetNameServers([]string{"localhost:9876"})
    
    // 设置不同的负载均衡策略
    
    // 1. 平均分配策略（默认）
    pushConsumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})
    
    // 2. 轮询分配策略
    pushConsumer.SetLoadBalanceStrategy(&client.RoundRobinAllocateStrategy{})
    
    // 3. 一致性哈希分配策略
    pushConsumer.SetLoadBalanceStrategy(&client.ConsistentHashAllocateStrategy{})
    
    // 4. 配置分配策略
    pushConsumer.SetLoadBalanceStrategy(&client.ConfigAllocateStrategy{})
    
    // 5. 机房就近分配策略
    pushConsumer.SetLoadBalanceStrategy(&client.MachineRoomNearbyAllocateStrategy{})
    
    if err := pushConsumer.Subscribe("test_topic", "*"); err != nil {
        log.Fatal(err)
    }
    
    pushConsumer.RegisterMessageListener(func(msgs []*client.MessageExt) client.ConsumeResult {
        // 处理消息
        return client.ConsumeSuccess
    })
    
    if err := pushConsumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer pushConsumer.Stop()
}
```

### 消费者配置选项

```go
package main

import (
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建消费者并设置高级配置
    pushConsumer := client.NewPushConsumer("advanced_consumer_group")
    pushConsumer.SetNameServers([]string{"localhost:9876"})
    
    // 设置消费模式
    pushConsumer.SetConsumeFromWhere(client.ConsumeFromFirstOffset) // 从最早位置开始消费
    // pushConsumer.SetConsumeFromWhere(client.ConsumeFromLastOffset) // 从最新位置开始消费
    
    // 设置消费线程数
    pushConsumer.SetConsumeThreadMin(5)
    pushConsumer.SetConsumeThreadMax(20)
    
    // 设置批量消费大小
    pushConsumer.SetPullBatchSize(32)
    pushConsumer.SetConsumeMessageBatchMaxSize(16)
    
    // 设置消费超时时间
    pushConsumer.SetConsumeTimeout(15 * time.Minute)
    
    // 设置消息重试次数
    pushConsumer.SetMaxReconsumeTimes(16)
    
    if err := pushConsumer.Subscribe("test_topic", "*"); err != nil {
        log.Fatal(err)
    }
    
    pushConsumer.RegisterMessageListener(func(msgs []*client.MessageExt) client.ConsumeResult {
        // 处理消息逻辑
        for _, msg := range msgs {
            // 业务处理
            if processMessage(msg) {
                continue
            } else {
                // 处理失败，返回稍后重试
                return client.ConsumeRetryLater
            }
        }
        return client.ConsumeSuccess
    })
    
    if err := pushConsumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer pushConsumer.Stop()
}

func processMessage(msg *client.MessageExt) bool {
    // 模拟消息处理逻辑
    return true
}
```

### 生产者配置选项

```go
package main

import (
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建生产者并设置高级配置
    producer, err := client.NewProducer("advanced_producer_group")
    if err != nil {
        log.Fatal(err)
    }
    
    producer.SetNameServers([]string{"localhost:9876"})
    
    // 设置发送超时时间
    producer.SetSendMsgTimeout(10 * time.Second)
    
    // 设置重试次数
    producer.SetRetryTimesWhenSendFailed(3)
    producer.SetRetryTimesWhenSendAsyncFailed(3)
    
    // 设置压缩阈值
    producer.SetCompressMsgBodyOverHowmuch(4096)
    
    // 设置最大消息大小
    producer.SetMaxMessageSize(4 * 1024 * 1024) // 4MB
    
    if err := producer.Start(); err != nil {
        log.Fatal(err)
    }
    defer producer.Shutdown()
    
    // 发送消息示例
    msg := &client.Message{
        Topic: "test_topic",
        Body:  []byte("Advanced producer message"),
        // 设置消息属性
        Properties: map[string]string{
            "key1": "value1",
            "key2": "value2",
        },
    }
    
    // 同步发送
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Printf("发送失败: %v", err)
    } else {
        log.Printf("发送成功: %s", result.MsgID)
    }
}
```

## 本地开发模式

### 适用场景

- 本项目内部开发
- 运行项目示例
- 贡献代码开发

### 导入方式

```go
import "go-rocketmq/pkg/client"
```

### 示例位置

本项目的 `examples/` 目录中的所有示例都使用本地开发模式，包括：

- `examples/basic/` - 基础使用示例
- `examples/advanced/` - 高级特性示例
- `examples/performance/` - 性能优化示例
- `examples/integration/` - 集成示例

## 性能优化功能

### 内存池管理

客户端库提供了内存池功能来减少 GC 压力和提高性能：

```go
import (
    client "github.com/chenjy16/go-rocketmq-client"
    "github.com/chenjy16/go-rocketmq-client/performance"
)

func main() {
    // 初始化全局内存池
    performance.InitGlobalPools()
    
    // 使用内存池创建消息
    msg := performance.GetMessage()
    msg.Topic = "test_topic"
    msg.Body = []byte("Hello RocketMQ")
    
    // 发送消息
    producer.SendSync(msg)
    
    // 归还消息到池中
    performance.PutMessage(msg)
}
```

### 批量处理

批量处理可以显著提高吞吐量：

```go
// 批量发送消息
func batchSendExample() {
    // 初始化批量管理器
    performance.InitGlobalBatchManager()
    
    // 创建批量处理器
    batchProcessor := performance.NewBatchProcessor(
        performance.DefaultBatchConfig,
        performance.BatchHandlerFunc(func(items []interface{}) error {
            messages := make([]*client.Message, len(items))
            for i, item := range items {
                messages[i] = item.(*client.Message)
            }
            return producer.SendBatch(messages)
        }),
    )
    
    batchProcessor.Start()
    defer batchProcessor.Stop()
    
    // 提交消息到批量处理器
    for i := 0; i < 1000; i++ {
        msg := &client.Message{
            Topic: "test_topic",
            Body:  []byte(fmt.Sprintf("Message %d", i)),
        }
        batchProcessor.Submit(msg)
    }
}
```

### 性能监控

启用性能监控来跟踪系统性能：

```go
func enableMonitoring() {
    // 初始化性能监控
    config := performance.DefaultMonitorConfig
    config.HTTPPort = 8080
    config.MetricsPath = "/metrics"
    
    performance.InitGlobalPerformanceMonitor(config)
    
    // 启动监控
    monitor := performance.GetGlobalPerformanceMonitor()
    monitor.Start()
    defer monitor.Stop()
    
    // 监控指标将在 http://localhost:8080/metrics 可用
}
```

### 网络优化

客户端库自动应用网络优化：

- **连接池**: 自动复用连接减少建立开销
- **多路复用**: 单连接处理多个请求
- **数据压缩**: 自动压缩大消息减少网络传输
- **异步 I/O**: 非阻塞网络操作

```go
// 配置网络优化参数
producer.SetNetworkConfig(&client.NetworkConfig{
    MaxConnections:    100,
    ConnectionTimeout: 30 * time.Second,
    ReadTimeout:      10 * time.Second,
    WriteTimeout:     10 * time.Second,
    EnableCompression: true,
    CompressionLevel:  6,
})
```

### 性能配置建议

#### 高吞吐量场景

```go
// 配置大批量处理
batchConfig := &performance.BatchConfig{
    BatchSize:      1000,
    FlushInterval:  100 * time.Millisecond,
    MaxRetries:     3,
    RetryDelay:     50 * time.Millisecond,
    BufferSize:     10000,
}

// 配置大内存池
poolConfig := &performance.PoolConfig{
    MessagePoolSize: 10000,
    BufferPoolSizes: []int{1024, 4096, 16384, 65536},
    ObjectPoolSize:  5000,
}
```

#### 低延迟场景

```go
// 配置小批量快速处理
batchConfig := &performance.BatchConfig{
    BatchSize:      10,
    FlushInterval:  1 * time.Millisecond,
    MaxRetries:     1,
    RetryDelay:     1 * time.Millisecond,
    BufferSize:     1000,
}

// 启用零拷贝缓冲区
zeroCopyBuffer := performance.NewZeroCopyBuffer(4096)
defer zeroCopyBuffer.Release()
```

### 性能基准测试

运行性能基准测试来验证优化效果：

```bash
# 运行所有性能基准测试
go test -bench=. -benchmem ./pkg/performance/

# 运行特定基准测试
go test -bench=BenchmarkMemoryPool -benchmem ./pkg/performance/
go test -bench=BenchmarkBatchProcessor -benchmem ./pkg/performance/
```

### 性能监控指标

通过 HTTP 端点获取性能指标：

```bash
# 获取所有指标
curl http://localhost:8080/metrics

# 获取系统指标
curl http://localhost:8080/metrics/system

# 获取健康检查
curl http://localhost:8080/health
```

关键指标包括：
- 消息发送/接收速率
- 内存使用情况
- GC 频率和耗时
- 网络连接数
- 批量处理统计
- 错误率和延迟

## 选择建议

### 使用独立客户端库的情况：

- ✅ 在自己的项目中集成 RocketMQ 功能
- ✅ 构建微服务应用
- ✅ 开发生产环境应用
- ✅ 需要稳定的 API 版本

### 使用本地开发模式的情况：

- ✅ 学习和了解 RocketMQ 实现
- ✅ 运行项目示例代码
- ✅ 为本项目贡献代码
- ✅ 自定义修改客户端功能

## 版本兼容性

独立客户端库遵循语义化版本控制，主要版本变更可能包含破坏性更改。建议在生产环境中固定使用特定版本：

```bash
go get github.com/chenjy16/go-rocketmq-client@v1.0.0
```

## 获取帮助

- 查看 [API 文档](https://pkg.go.dev/github.com/chenjy16/go-rocketmq-client)
- 参考 [示例代码](../examples/)
- 提交 [Issue](https://github.com/chenjy16/go-rocketmq-client/issues)