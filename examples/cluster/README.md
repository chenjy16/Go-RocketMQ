# RocketMQ 集群示例

本目录包含了 RocketMQ 集群功能的完整示例代码，展示了如何构建和管理高可用的 RocketMQ 集群。

## 目录结构

```
cluster/
├── README.md                    # 本文档
├── cluster-management/          # 集群管理示例
│   └── cluster-example.go
├── failover/                    # 故障转移示例
│   └── failover-example.go
├── load-balance/                # 负载均衡示例
│   └── loadbalance-example.go
├── high-availability/           # 高可用配置示例
│   └── ha-example.go
├── broker-cluster/              # Broker集群示例
│   └── broker-cluster-example.go
└── nameserver-cluster/          # NameServer集群示例
    └── nameserver-cluster-example.go
```

## 功能特性

### 1. 集群管理 (cluster-management)
- 集群节点发现和注册
- 集群拓扑管理
- 节点状态监控
- 集群配置同步
- 动态扩缩容

### 2. 故障转移 (failover)
- 自动故障检测
- 主备切换
- 数据同步
- 服务恢复
- 故障隔离

### 3. 负载均衡 (load-balance)
- 多种负载均衡策略
  - 轮询 (Round Robin)
  - 加权轮询 (Weighted Round Robin)
  - 最少连接 (Least Connections)
  - 一致性哈希 (Consistent Hash)
- 动态权重调整
- 健康检查集成

### 4. 高可用配置 (high-availability)
- 主从复制
- 数据备份
- 自动恢复
- 分区容错
- 一致性保证

### 5. Broker集群 (broker-cluster)
- 多Broker节点管理
- 消息分片和路由
- 负载均衡
- 故障转移
- 性能监控

### 6. NameServer集群 (nameserver-cluster)
- NameServer节点管理
- 服务发现
- 路由信息同步
- 选举机制
- 健康检查

## 快速开始

### 前置条件

1. Go 1.19 或更高版本
2. RocketMQ NameServer 和 Broker 服务

### 安装依赖

```bash
cd examples/cluster
go mod tidy
```

### 运行示例

#### 1. 集群管理示例

```bash
cd cluster-management
go run cluster-example.go
```

#### 2. 故障转移示例

```bash
cd failover
go run failover-example.go
```

#### 3. 负载均衡示例

```bash
cd load-balance
go run loadbalance-example.go
```

#### 4. 高可用配置示例

```bash
cd high-availability
go run ha-example.go
```

#### 5. Broker集群示例

```bash
cd broker-cluster
go run broker-cluster-example.go
```

#### 6. NameServer集群示例

```bash
cd nameserver-cluster
go run nameserver-cluster-example.go
```

## 配置说明

### 集群配置

每个示例都包含详细的配置选项：

- **节点配置**: 地址、端口、权重等
- **健康检查**: 检查间隔、超时时间、失败阈值
- **负载均衡**: 策略选择、权重分配
- **故障转移**: 检测机制、切换策略
- **监控告警**: 指标收集、阈值设置

### 环境变量

可以通过环境变量覆盖默认配置：

```bash
export ROCKETMQ_NAMESERVER_ADDR="127.0.0.1:9876;127.0.0.1:9877"
export ROCKETMQ_CLUSTER_NAME="DefaultCluster"
export ROCKETMQ_BROKER_GROUP="broker-group-1"
```

## 最佳实践

### 1. 集群规划

- **NameServer**: 建议部署奇数个节点（3个或5个）
- **Broker**: 根据业务量和可用性要求部署
- **网络**: 确保节点间网络连通性良好
- **存储**: 使用高性能存储设备

### 2. 监控和告警

- 监控关键指标：CPU、内存、磁盘、网络
- 设置合理的告警阈值
- 建立故障响应流程

### 3. 容量规划

- 根据消息量评估集群规模
- 预留足够的扩展空间
- 定期进行性能测试

### 4. 安全配置

- 启用访问控制（ACL）
- 配置网络安全策略
- 定期更新和备份

## 故障排查

### 常见问题

1. **节点无法加入集群**
   - 检查网络连通性
   - 验证配置文件
   - 查看日志文件

2. **消息发送失败**
   - 检查Broker状态
   - 验证Topic配置
   - 查看路由信息

3. **性能问题**
   - 监控系统资源
   - 调整JVM参数
   - 优化存储配置

### 日志分析

重要日志文件位置：
- NameServer: `~/logs/rocketmqlogs/namesrv.log`
- Broker: `~/logs/rocketmqlogs/broker.log`
- Client: 应用程序日志

## 性能调优

### 1. JVM 参数

```bash
-Xms4g -Xmx4g -Xmn2g
-XX:+UseG1GC
-XX:G1HeapRegionSize=16m
-XX:G1ReservePercent=25
-XX:InitiatingHeapOccupancyPercent=30
```

### 2. 系统参数

```bash
# 增加文件描述符限制
ulimit -n 655350

# 调整网络参数
echo 'net.core.rmem_default = 262144' >> /etc/sysctl.conf
echo 'net.core.rmem_max = 16777216' >> /etc/sysctl.conf
echo 'net.core.wmem_default = 262144' >> /etc/sysctl.conf
echo 'net.core.wmem_max = 16777216' >> /etc/sysctl.conf
```

### 3. 存储优化

- 使用SSD存储
- 配置RAID 10
- 调整文件系统参数

## 扩展开发

### 自定义负载均衡策略

```go
type CustomLoadBalancer struct {
    // 自定义字段
}

func (lb *CustomLoadBalancer) SelectBroker(brokers []BrokerInfo) *BrokerInfo {
    // 自定义选择逻辑
    return &brokers[0]
}
```

### 自定义监控指标

```go
type CustomMetrics struct {
    CustomCounter   int64   `json:"custom_counter"`
    CustomGauge     float64 `json:"custom_gauge"`
    CustomHistogram []float64 `json:"custom_histogram"`
}
```

## 参考资料

- [RocketMQ 官方文档](https://rocketmq.apache.org/docs/)
- [Go RocketMQ Client 文档](../client/README.md)
- [性能优化指南](../../docs/PERFORMANCE_OPTIMIZATION.md)
- [故障排查指南](../../docs/TROUBLESHOOTING.md)

## 贡献

欢迎提交 Issue 和 Pull Request 来改进这些示例代码。

## 许可证

本项目采用 Apache 2.0 许可证。详见 [LICENSE](../../LICENSE) 文件。