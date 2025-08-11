# Go-RocketMQ 架构设计文档

## 项目概述

Go-RocketMQ 是 Apache RocketMQ 的 Go 语言实现，旨在提供高性能、高可靠性的分布式消息队列服务。本项目完全使用 Go 语言重新实现了 RocketMQ 的核心功能和架构。

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
- **端口**: 10911 (默认)

### 3. Producer (生产者客户端)
- **位置**: `pkg/client/producer.go`
- **功能**:
  - 同步发送消息
  - 异步发送消息
  - 单向发送消息
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

## 数据结构

### 消息相关
- **Message**: 基础消息结构
- **MessageExt**: 扩展消息结构，包含系统属性
- **MessageQueue**: 消息队列标识
- **SendResult**: 发送结果

### 协议相关
- **RemotingCommand**: 远程调用命令
- **TopicRouteData**: Topic 路由数据
- **ClusterInfo**: 集群信息
- **SubscriptionData**: 订阅数据

## 通信协议

项目定义了完整的通信协议，包括：

### 请求码 (RequestCode)
- `RegisterBroker`: Broker 注册
- `GetRouteInfoByTopic`: 获取 Topic 路由信息
- `SendMessage`: 发送消息
- `PullMessage`: 拉取消息

### 响应码 (ResponseCode)
- `Success`: 成功
- `SystemError`: 系统错误
- `SystemBusy`: 系统繁忙
- `TopicNotExist`: Topic 不存在

## 项目结构

```
go-rocketmq/
├── cmd/                    # 可执行程序入口
│   ├── nameserver/        # NameServer 启动程序
│   └── broker/            # Broker 启动程序
├── pkg/                   # 核心包
│   ├── nameserver/        # NameServer 实现
│   ├── broker/            # Broker 实现
│   ├── client/            # 客户端实现
│   ├── common/            # 公共数据结构
│   └── protocol/          # 通信协议
├── examples/              # 示例程序
│   ├── producer/          # 生产者示例
│   └── consumer/          # 消费者示例
├── build/                 # 构建输出
└── docs/                  # 文档
```

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

## 特性支持

### 已实现特性
- ✅ NameServer 路由管理
- ✅ Broker 消息存储
- ✅ Producer 消息发送
- ✅ Consumer 消息消费
- ✅ Topic 管理
- ✅ 队列管理
- ✅ 基础通信协议

### 待实现特性
- ⏳ 完整的网络通信协议
- ⏳ 消息持久化存储
- ⏳ 集群模式支持
- ⏳ 事务消息
- ⏳ 顺序消息
- ⏳ 延迟消息
- ⏳ 消息过滤
- ⏳ 消费重试机制
- ⏳ 死信队列
- ⏳ 监控和管理工具

## 性能目标

- **吞吐量**: 单机 10万+ TPS
- **延迟**: P99 < 10ms
- **可用性**: 99.9%+
- **存储**: 支持 TB 级消息存储

## 技术栈

- **语言**: Go 1.19+
- **网络**: TCP/HTTP
- **序列化**: JSON/Protocol Buffers
- **存储**: 文件系统 (计划支持多种存储后端)
- **日志**: 标准库 log (计划集成 logrus/zap)

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

## 监控指标

### NameServer 指标
- Broker 注册数量
- 路由查询 QPS
- 响应时间

### Broker 指标
- 消息发送 TPS
- 消息存储量
- 队列深度
- 磁盘使用率

### 客户端指标
- 发送成功率
- 消费延迟
- 重试次数

## 扩展性设计

### 水平扩展
- NameServer 无状态，支持多实例部署
- Broker 支持集群模式，可动态扩容
- 客户端支持自动发现和负载均衡

### 插件化
- 存储引擎可插拔
- 序列化方式可配置
- 过滤器支持自定义

## 与原版 RocketMQ 的对比

| 特性 | 原版 RocketMQ | Go-RocketMQ |
|------|---------------|-------------|
| 语言 | Java | Go |
| 内存占用 | 较高 | 较低 |
| 启动速度 | 较慢 | 快速 |
| 部署复杂度 | 需要 JVM | 单一二进制 |
| 性能 | 高 | 目标相当 |
| 生态系统 | 成熟 | 发展中 |

## 贡献指南

1. Fork 项目
2. 创建特性分支
3. 提交代码
4. 创建 Pull Request

## 许可证

本项目采用 Apache 2.0 许可证。