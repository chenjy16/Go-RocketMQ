# Go-RocketMQ

[![Go Version](https://img.shields.io/badge/Go-1.19+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](https://github.com/your-org/go-rocketmq)

一个用Go语言实现的Apache RocketMQ，提供高性能的消息队列功能。

## 项目目标

实现与Apache RocketMQ功能对等的消息队列系统，具备以下特性：

### 核心功能
- ✅ 分布式消息队列
- ✅ 发布/订阅模式
- ✅ 高可用架构
- ✅ 消息持久化
- ✅ 事务消息
- ✅ 顺序消息
- ✅ 延时消息
- ✅ 消息过滤

### 性能目标
- 低延迟（< 1ms）
- 高吞吐量（> 100万 TPS）
- 横向扩展能力
- 万亿级消息存储

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

## 项目结构

```
go-rocketmq/
├── cmd/                    # 命令行工具
│   ├── nameserver/        # NameServer启动程序
│   ├── broker/            # Broker启动程序
│   └── admin/             # 管理工具
├── pkg/                   # 核心包
│   ├── nameserver/        # NameServer实现
│   ├── broker/            # Broker实现
│   ├── client/            # 客户端SDK
│   ├── common/            # 公共组件
│   ├── protocol/          # 通信协议
│   ├── store/             # 存储引擎
│   └── admin/             # 管理接口
├── internal/              # 内部包
│   ├── config/            # 配置管理
│   ├── log/               # 日志组件
│   └── utils/             # 工具函数
├── api/                   # API定义
│   └── proto/             # Protocol Buffers定义
├── examples/              # 示例代码
├── docs/                  # 文档
└── scripts/               # 脚本文件
```

## 快速开始

### 1. 启动NameServer
```bash
go run cmd/nameserver/main.go
```

### 2. 启动Broker
```bash
go run cmd/broker/main.go -nameserver localhost:9876
```

### 3. 发送消息
```go
producer := client.NewProducer("test-group")
producer.Start()
defer producer.Shutdown()

msg := &common.Message{
    Topic: "test-topic",
    Body:  []byte("Hello Go-RocketMQ"),
}
result := producer.SendSync(msg)
```

### 4. 消费消息
```go
consumer := client.NewPushConsumer("test-group")
consumer.Subscribe("test-topic", "*", func(msgs []*common.MessageExt) error {
    for _, msg := range msgs {
        fmt.Printf("Received: %s\n", string(msg.Body))
    }
    return nil
})
consumer.Start()
```

## 开发计划

### Phase 1: 基础架构 (4周)
- [x] 项目结构搭建
- [ ] 基础通信协议
- [ ] NameServer基础功能
- [ ] Broker基础功能

### Phase 2: 核心功能 (6周)
- [ ] 消息存储引擎
- [ ] Producer/Consumer客户端
- [ ] 路由管理
- [ ] 负载均衡

### Phase 3: 高级特性 (8周)
- [ ] 事务消息
- [ ] 顺序消息
- [ ] 延时消息
- [ ] 消息过滤

### Phase 4: 性能优化 (4周)
- [ ] 性能调优
- [ ] 压力测试
- [ ] 监控指标

## 技术选型

- **语言**: Go 1.21+
- **通信**: gRPC + TCP
- **存储**: 文件系统 + 内存映射
- **序列化**: Protocol Buffers
- **配置**: Viper
- **日志**: Logrus
- **测试**: Go标准测试框架

## 贡献指南

1. Fork 项目
2. 创建特性分支
3. 提交更改
4. 推送到分支
5. 创建 Pull Request

## 许可证

Apache License 2.0