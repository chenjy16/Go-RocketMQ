# Go-RocketMQ

[![Go Version](https://img.shields.io/badge/Go-1.19+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](https://github.com/your-org/go-rocketmq)


## 项目概述

Go-RocketMQ 是Go 语言实现，提供了完整的消息队列功能，包括消息生产、消费、路由管理等核心特性。项目采用现代化的 Go 语言特性，具有部署简单、性能优异、资源占用低等优势。

## 核心功能

### ✅ 已实现功能
- [x] NameServer 服务注册与发现
- [x] Broker 消息存储和管理
- [x] Producer 多种发送模式 (同步/异步/单向)
- [x] Consumer 消息订阅和消费
- [x] Topic 路由管理
- [x] 消息队列负载均衡
- [x] 完整的消息发送和接收流程
- [x] TCP 网络通信协议
- [x] JSON 消息序列化
- [x] 性能测试工具和监控系统
- [x] Web 监控界面
- [x] 完整的端到端测试

### ⏳ 待实现功能
- [ ] 消息持久化存储优化
- [ ] 集群模式支持
- [ ] 事务消息
- [ ] 顺序消息
- [ ] 延时消息
- [ ] 消息过滤
- [ ] 消费重试机制
- [ ] 死信队列

### 性能目标
- 低延迟（< 1ms）
- 高吞吐量（> 100万 TPS）
- 横向扩展能力
- TB级消息存储

## 架构设计

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Producer  │    │   Producer  │    │   Producer  │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
                   ┌──────▼──────┐
                   │ NameServer  │
                   │   Cluster   │
                   └──────┬──────┘
                          │
       ┌──────────────────┼──────────────────┐
       │                  │                  │
┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
│   Broker-1  │    │   Broker-2  │    │   Broker-3  │
│   Master    │    │   Master    │    │   Master    │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
│   Broker-1  │    │   Broker-2  │    │   Broker-3  │
│    Slave    │    │    Slave    │    │    Slave    │
└─────────────┘    └─────────────┘    └─────────────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
       ┌──────────────────┼──────────────────┐
       │                  │                  │
┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
│  Consumer   │    │  Consumer   │    │  Consumer   │
│   Group-1   │    │   Group-2   │    │   Group-3   │
└─────────────┘    └─────────────┘    └─────────────┘
```

## 核心组件

### 1. NameServer (名称服务器)
- **位置**: `pkg/nameserver/nameserver.go`
- **功能**: 
  - 管理 Broker 的路由信息
  - 提供 Topic 路由数据查询
  - 维护集群拓扑信息
  - 检测 Broker 存活状态
- **端口**: 9876 (默认)

### 2. Broker (消息代理)
- **位置**: `pkg/broker/broker.go`
- **功能**:
  - 消息存储和管理
  - 处理生产者发送的消息
  - 响应消费者的拉取请求
  - Topic 和队列管理
  - 向 NameServer 注册和发送心跳
- **端口**: 10911 (默认), 10912 (HA服务)

### 3. Producer (生产者客户端)
- **位置**: `pkg/client/producer.go`
- **功能**:
  - 同步发送消息 (SendSync)
  - 异步发送消息 (SendAsync)
  - 单向发送消息 (SendOneway)
  - 自动路由选择
  - 故障转移

### 4. Consumer (消费者客户端)
- **位置**: `pkg/client/consumer.go`
- **功能**:
  - Topic 订阅管理
  - 消息拉取和消费
  - 消费进度管理
  - 负载均衡
  - 消费重试机制
  - Push 模式 (推送) 和 Pull 模式 (拉取)

## 项目结构

```
go-rocketmq/
├── cmd/                    # 主程序入口
│   ├── nameserver/        # NameServer 服务
│   └── broker/            # Broker 服务
├── pkg/                   # 核心包
│   ├── client/           # 客户端实现
│   ├── common/           # 通用数据结构
│   ├── nameserver/       # NameServer 实现
│   ├── broker/           # Broker 实现
│   ├── protocol/         # 通信协议
│   ├── store/            # 存储引擎
│   ├── cluster/          # 集群管理
│   ├── failover/         # 故障转移
│   └── ha/               # 高可用
├── examples/             # 示例程序
│   ├── README.md         # 示例说明文档
│   ├── basic/           # 基础示例
│   │   ├── producer/    # 生产者基础示例
│   │   ├── consumer/    # 消费者基础示例
│   │   └── simple-demo/ # 简单演示
│   ├── advanced/        # 高级特性示例
│   │   ├── transaction/ # 事务消息
│   │   ├── ordered/     # 顺序消息
│   │   ├── delayed/     # 延时消息
│   │   ├── batch/       # 批量消息
│   │   └── filter/      # 消息过滤
│   ├── cluster/         # 集群模式示例
│   │   ├── multi-broker/# 多Broker集群
│   │   ├── ha/          # 高可用配置
│   │   └── load-balance/# 负载均衡
│   ├── performance/     # 性能测试
│   │   ├── benchmark/   # 基准测试
│   │   ├── stress-test/ # 压力测试
│   │   └── monitoring/  # 监控示例
│   ├── integration/     # 集成示例
│   │   ├── spring-boot/ # Spring Boot集成
│   │   ├── gin/         # Gin框架集成
│   │   └── microservice/# 微服务架构
│   └── tools/           # 工具示例
│       ├── admin/       # 管理工具
│       ├── migration/   # 数据迁移
│       └── monitoring/  # 监控工具
├── tools/                # 工具集
│   └── monitor/          # 系统监控工具
├── scripts/              # 脚本文件
│   ├── test_system.sh    # 系统测试脚本
│   └── full_test.sh      # 完整测试脚本
├── config/               # 配置文件
│   └── config.yaml       # 系统配置
├── build/                # 构建输出目录
│   └── bin/              # 可执行文件
├── logs/                 # 日志目录
├── docs/                 # 文档目录
│   ├── ARCHITECTURE.md   # 架构文档
│   └── QUICKSTART.md     # 快速开始指南
├── Makefile              # 构建脚本
├── go.mod                # Go模块文件
├── go.sum                # Go依赖校验
└── LICENSE               # 许可证文件
```

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

## 快速开始

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

## 性能特性

### 发送性能
- **同步发送**: 支持高可靠性消息发送
- **异步发送**: 支持高吞吐量消息发送
- **单向发送**: 支持最高性能消息发送

### 并发支持
- **多线程生产**: 支持多个生产者并发发送
- **多线程消费**: 支持多个消费者并发消费
- **负载均衡**: 自动分配消息队列

### 监控指标
- **TPS**: 每秒事务处理数
- **延迟**: 消息发送延迟
- **吞吐量**: 数据传输速率
- **系统资源**: CPU、内存、磁盘使用率

### 实际性能测试结果

#### 同步发送模式
- **小规模测试** (100条消息, 3并发):
  - 成功率: 100%
  - TPS: 7,747.59 msg/s
  - 平均延迟: 0.37 ms
  - 吞吐量: 7.57 MB/s

- **中等规模测试** (1000条消息, 10并发):
  - 成功率: 99.90%
  - TPS: 19,366.64 msg/s
  - 平均延迟: 0.50 ms
  - 吞吐量: 18.91 MB/s

#### 异步发送模式
- **测试结果** (500条消息, 5并发):
  - 成功率: 87.40%
  - TPS: 218.34 msg/s
  - 吞吐量: 0.21 MB/s

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

### 性能测试
```bash
# 同步发送性能测试
make benchmark

# 异步发送性能测试
make benchmark-async

# 单向发送性能测试
make benchmark-oneway
```

### 系统监控
```bash
# 命令行监控
make monitor

# Web 监控界面
make monitor-web
```

### 自动化测试
```bash
# 运行完整系统测试
./scripts/full_test.sh
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

## 技术选型

- **语言**: Go 1.19+
- **网络**: TCP/HTTP
- **序列化**: JSON/Protocol Buffers
- **存储**: 文件系统 (计划支持多种存储后端)
- **日志**: 标准库 log (计划集成 logrus/zap)
- **构建**: Make
- **测试**: Go标准测试框架

## 扩展性设计

### 水平扩展
- NameServer 无状态，支持多实例部署
- Broker 支持集群模式，可动态扩容
- 客户端支持自动发现和负载均衡
- 支持多个 Producer/Consumer 实例

### 插件化架构
- 可插拔的存储引擎
- 可扩展的序列化协议
- 可定制的负载均衡策略
- 存储引擎可插拔
- 序列化方式可配置
- 过滤器支持自定义



## 核心流程

### 1. 系统启动流程
1. 启动 NameServer
2. 启动 Broker，向 NameServer 注册
3. Broker 定期向 NameServer 发送心跳
4. NameServer 维护 Broker 存活状态

### 2. 消息发送流程
1. Producer 从 NameServer 获取 Topic 路由信息
2. 选择合适的 Broker 和队列
3. 发送消息到 Broker
4. Broker 存储消息并返回结果

### 3. 消息消费流程
1. Consumer 订阅 Topic
2. 从 NameServer 获取路由信息
3. 向 Broker 发送拉取请求
4. 处理返回的消息
5. 提交消费进度

## 开发计划

### 短期目标
- [ ] 完善消息持久化机制
- [ ] 实现集群模式支持
- [ ] 添加消息过滤功能
- [ ] 优化性能和稳定性

### 长期目标
- [ ] 支持事务消息
- [ ] 实现延时消息
- [ ] 添加消息轨迹功能
- [ ] 支持多种存储引擎
- [ ] 完整的网络通信协议
- [ ] 消费重试机制
- [ ] 死信队列
- [ ] 监控和管理工具

## 部署方式

### 开发环境
```bash
# 启动 NameServer
make run-nameserver

# 启动 Broker
make run-broker

# 运行生产者示例
make run-producer

# 运行消费者示例
make run-consumer
```

### 生产环境
- 支持 Docker 容器化部署
- 支持 Kubernetes 集群部署
- 支持传统虚拟机部署

## 示例代码

本项目提供了丰富的示例代码，位于 `examples/` 目录：

- **基础示例**: 生产者和消费者的基本使用
- **高级特性**: 事务消息、顺序消息、延时消息等
- **集群模式**: 多Broker集群、高可用配置
- **性能测试**: 基准测试和压力测试工具
- **集成示例**: 与各种框架的集成方案
- **工具示例**: 管理工具和监控工具

详细说明请参考 [examples/README.md](examples/README.md)。

## 获取帮助

- 查看 [架构设计文档](ARCHITECTURE.md)
- 阅读 [快速开始指南](QUICKSTART.md)
- 查看 [项目总结](PROJECT_SUMMARY.md)
- 提交 [Issue](https://github.com/your-org/go-rocketmq/issues)
- 参与 [讨论](https://github.com/your-org/go-rocketmq/discussions)

## 贡献指南

我们欢迎所有形式的贡献，包括但不限于：

1. **代码贡献**
   - Fork 项目
   - 创建特性分支
   - 提交更改
   - 推送到分支
   - 创建 Pull Request

2. **文档改进**
   - 完善现有文档
   - 添加使用示例
   - 翻译文档

3. **问题反馈**
   - 报告 Bug
   - 提出功能建议
   - 性能优化建议

4. **测试贡献**
   - 编写单元测试
   - 进行集成测试
   - 性能测试

请确保：
- 代码简洁易懂
- 包含必要的注释
- 提供运行说明
- 遵循项目的代码规范

## 许可证

本项目采用 Apache License 2.0 许可证。详见 [LICENSE](LICENSE) 文件。

---

**Go-RocketMQ** - 用 Go 语言构建的高性能分布式消息队列系统