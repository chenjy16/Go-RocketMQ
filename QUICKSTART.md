# Go-RocketMQ 快速开始指南

## 环境要求

- Go 1.19 或更高版本
- Git
- Make (可选，用于构建脚本)

## 安装和构建

### 1. 克隆项目
```bash
git clone https://github.com/your-org/go-rocketmq.git
cd go-rocketmq
```

### 2. 安装依赖
```bash
go mod tidy
```

### 3. 构建项目
```bash
make build
```

或者手动构建：
```bash
# 构建 NameServer
go build -o build/bin/nameserver ./cmd/nameserver

# 构建 Broker
go build -o build/bin/broker ./cmd/broker

# 构建示例程序
go build -o build/bin/producer-example ./examples/producer
go build -o build/bin/consumer-example ./examples/consumer
```

## 快速启动

### 1. 启动 NameServer
```bash
# 使用 Makefile
make run-nameserver

# 或直接运行
./build/bin/nameserver
```

NameServer 将在端口 9876 上启动。

### 2. 启动 Broker
在新的终端窗口中：
```bash
# 使用 Makefile
make run-broker

# 或直接运行
./build/bin/broker
```

Broker 将在端口 10911 上启动，并自动注册到 NameServer。

### 3. 运行生产者示例
在新的终端窗口中：
```bash
# 使用 Makefile
make run-producer

# 或直接运行
./build/bin/producer-example
```

### 4. 运行消费者示例
在新的终端窗口中：
```bash
# 使用 Makefile
make run-consumer

# 或直接运行
./build/bin/consumer-example
```

## 基本使用示例

### 发送消息

```go
package main

import (
    "fmt"
    "log"
    
    "go-rocketmq/pkg/client"
    "go-rocketmq/pkg/common"
)

func main() {
    // 创建生产者
    producer := client.NewProducer(nil)
    producer.SetNameServerAddr("127.0.0.1:9876")
    
    // 启动生产者
    err := producer.Start()
    if err != nil {
        log.Fatalf("Failed to start producer: %v", err)
    }
    defer producer.Stop()
    
    // 创建消息
    msg := common.NewMessage("TestTopic", []byte("Hello RocketMQ!"))
    msg.SetTags("test").SetKeys("key1")
    
    // 发送消息
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Fatalf("Failed to send message: %v", err)
    }
    
    fmt.Printf("Message sent successfully: %s\n", result.MsgId)
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
    
    "go-rocketmq/pkg/client"
    "go-rocketmq/pkg/common"
)

// 消息监听器
type MyMessageListener struct{}

func (l *MyMessageListener) ConsumeMessage(msgs []*common.MessageExt) common.ConsumeResult {
    for _, msg := range msgs {
        log.Printf("Received: %s", string(msg.Body))
    }
    return common.ConsumeSuccess
}

func main() {
    // 创建消费者
    consumer := client.NewConsumer(nil)
    consumer.SetNameServerAddr("127.0.0.1:9876")
    
    // 订阅 Topic
    listener := &MyMessageListener{}
    err := consumer.Subscribe("TestTopic", "*", listener)
    if err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }
    
    // 启动消费者
    err = consumer.Start()
    if err != nil {
        log.Fatalf("Failed to start consumer: %v", err)
    }
    defer consumer.Stop()
    
    // 等待中断信号
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
}
```

## 配置选项

### NameServer 配置
```bash
./build/bin/nameserver -port 9876
```

### Broker 配置
```bash
./build/bin/broker \
  -port 10911 \
  -name "broker-1" \
  -cluster "DefaultCluster" \
  -nameserver "127.0.0.1:9876" \
  -store "/tmp/rocketmq-store"
```

### 生产者配置
```go
config := &client.ProducerConfig{
    GroupName:      "ProducerGroup",
    NameServerAddr: "127.0.0.1:9876",
    SendMsgTimeout: 3 * time.Second,
    RetryTimes:     2,
}
producer := client.NewProducer(config)
```

### 消费者配置
```go
config := &client.ConsumerConfig{
    GroupName:        "ConsumerGroup",
    NameServerAddr:   "127.0.0.1:9876",
    ConsumeFromWhere: common.ConsumeFromLastOffset,
    MessageModel:     common.Clustering,
    ConsumeThreadMax: 4,
    PullBatchSize:    32,
}
consumer := client.NewConsumer(config)
```

## 常用命令

### 构建相关
```bash
make build          # 构建所有组件
make clean          # 清理构建文件
make test           # 运行测试
```

### 运行相关
```bash
make run-nameserver # 运行 NameServer
make run-broker     # 运行 Broker
make run-producer   # 运行生产者示例
make run-consumer   # 运行消费者示例
```

### 开发相关
```bash
make fmt            # 格式化代码
make vet            # 代码检查
make lint           # 运行 linter
```

## 故障排除

### 1. 端口冲突
如果默认端口被占用，可以通过参数指定其他端口：
```bash
./build/bin/nameserver -port 9877
./build/bin/broker -port 10912 -nameserver "127.0.0.1:9877"
```

### 2. 连接失败
确保 NameServer 已启动并且网络连接正常：
```bash
# 检查 NameServer 是否运行
netstat -an | grep 9876

# 检查 Broker 是否运行
netstat -an | grep 10911
```

### 3. 消息发送失败
检查 Topic 是否存在，Broker 是否正常运行。

### 4. 消息消费异常
确保消费者组名唯一，订阅表达式正确。

## 性能调优

### 1. Broker 调优
- 增加发送线程池大小
- 调整刷盘策略
- 优化存储路径

### 2. 客户端调优
- 调整批量大小
- 设置合适的超时时间
- 优化线程池配置

## 监控和日志

### 查看日志
```bash
# NameServer 日志
tail -f /tmp/nameserver.log

# Broker 日志
tail -f /tmp/broker.log
```

### 监控指标
- 消息发送 TPS
- 消息消费延迟
- 队列深度
- 系统资源使用率

## 下一步

1. 阅读 [架构设计文档](ARCHITECTURE.md)
2. 查看 [API 文档](docs/API.md)
3. 了解 [最佳实践](docs/BEST_PRACTICES.md)
4. 参与 [社区讨论](https://github.com/your-org/go-rocketmq/discussions)

## 获取帮助

- 查看 [FAQ](docs/FAQ.md)
- 提交 [Issue](https://github.com/your-org/go-rocketmq/issues)
- 加入 [讨论组](https://github.com/your-org/go-rocketmq/discussions)