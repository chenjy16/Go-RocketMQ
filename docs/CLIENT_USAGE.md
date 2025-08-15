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
    
    // 发送消息
    msg := &client.Message{
        Topic: "test_topic",
        Body:  []byte("Hello RocketMQ"),
    }
    
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("消息发送成功: %s\n", result.MsgId)
}
```

#### 消费者

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // 创建消费者
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
- `examples/performance/` - 性能测试示例
- `examples/integration/` - 集成示例

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