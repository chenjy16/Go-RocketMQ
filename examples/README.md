# Go-RocketMQ 示例代码

本目录包含了 Go-RocketMQ 的各种使用示例，帮助开发者快速上手和理解项目的功能特性。

## 目录结构

```
examples/
├── README.md                    # 本文件
├── basic/                       # 基础示例
│   ├── producer/               # 生产者基础示例
│   ├── consumer/               # 消费者基础示例
│   └── simple-demo/            # 简单演示
├── advanced/                   # 高级特性示例
│   ├── transaction/            # 事务消息
│   ├── ordered/                # 顺序消息
│   ├── delayed/                # 延时消息
│   ├── batch/                  # 批量消息
│   └── filter/                 # 消息过滤
├── cluster/                    # 集群模式示例
│   ├── multi-broker/           # 多Broker集群
│   ├── ha/                     # 高可用配置
│   └── load-balance/           # 负载均衡
├── performance/                # 性能测试
│   ├── benchmark/              # 基准测试
│   ├── stress-test/            # 压力测试
│   └── monitoring/             # 监控示例
├── integration/                # 集成示例
│   ├── spring-boot/            # Spring Boot集成
│   ├── gin/                    # Gin框架集成
│   └── microservice/           # 微服务架构
└── tools/                      # 工具示例
    ├── admin/                  # 管理工具
    ├── migration/              # 数据迁移
    └── monitoring/             # 监控工具
```

## 快速开始

### 1. 启动服务

首先启动 NameServer 和 Broker：

```bash
# 启动 NameServer
go run cmd/nameserver/main.go

# 启动 Broker
go run cmd/broker/main.go
```

### 2. 运行基础示例

```bash
# 运行生产者示例
go run examples/basic/producer/main.go

# 运行消费者示例
go run examples/basic/consumer/main.go
```

### 3. 运行完整演示

```bash
# 运行简单演示
go run examples/basic/simple-demo/main.go
```

## 示例说明

### 基础示例 (basic/)
- **producer/**: 展示如何创建生产者并发送消息
- **consumer/**: 展示如何创建消费者并接收消息
- **simple-demo/**: 完整的生产者-消费者演示

### 高级特性 (advanced/)
- **transaction/**: 事务消息的发送和处理
- **ordered/**: 顺序消息的发送和消费
- **delayed/**: 延时消息的使用
- **batch/**: 批量消息处理
- **filter/**: 消息过滤功能

### 集群模式 (cluster/)
- **multi-broker/**: 多Broker集群部署
- **ha/**: 高可用配置和故障转移
- **load-balance/**: 负载均衡策略

### 性能测试 (performance/)
- **benchmark/**: 性能基准测试
- **stress-test/**: 压力测试工具
- **monitoring/**: 性能监控示例

### 集成示例 (integration/)
- **spring-boot/**: 与Spring Boot的集成
- **gin/**: 与Gin框架的集成
- **microservice/**: 微服务架构中的使用

### 工具示例 (tools/)
- **admin/**: 管理工具的使用
- **migration/**: 数据迁移工具
- **monitoring/**: 监控工具配置

## 环境要求

- Go 1.19+
- 已启动的 NameServer (默认端口: 9876)
- 已启动的 Broker (默认端口: 10911)

## 配置说明

大部分示例使用默认配置，如需自定义配置，请参考 `config/config.yaml` 文件。

## 常见问题

1. **连接失败**: 确保 NameServer 和 Broker 已正确启动
2. **消息发送失败**: 检查 Topic 是否已创建
3. **消费者无法接收消息**: 确认订阅的 Topic 和 Tags 是否正确

## 贡献

欢迎提交新的示例代码或改进现有示例。请确保：
- 代码简洁易懂
- 包含必要的注释
- 提供运行说明
- 遵循项目的代码规范

## 许可证

本项目采用 Apache 2.0 许可证。