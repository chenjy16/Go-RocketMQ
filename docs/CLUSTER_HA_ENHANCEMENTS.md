# Go-RocketMQ 集群和高可用增强

本文档详细介绍了Go-RocketMQ的集群和高可用性增强功能，包括NameServer集群、Broker集群、数据复制和故障检测与恢复等核心组件。

## 1. NameServer集群实现

### 1.1 架构概述

NameServer集群通过多个NameServer节点协同工作，提供高可用的服务发现和路由管理功能。

### 1.2 核心特性

- **多节点部署**: 支持多个NameServer节点组成集群
- **数据同步**: 节点间自动同步路由信息
- **心跳检测**: 定期检测节点健康状态
- **故障转移**: 单节点故障不影响整体服务

### 1.3 组件说明

#### NameServerCluster
NameServer集群管理器，负责协调多个NameServer节点。

**主要功能**:
- 节点注册与发现
- 数据同步管理
- 心跳检测
- HTTP管理接口

**配置参数**:
```go
type ClusterConfig struct {
    LocalConfig       *Config       // 本地NameServer配置
    ClusterName       string        // 集群名称
    PeerAddresses     []string      // 其他节点地址
    SyncInterval      time.Duration // 数据同步间隔
    HeartbeatInterval time.Duration // 心跳间隔
}
```

## 2. Broker集群实现

### 2.1 架构概述

Broker集群支持多Broker节点部署，提供水平扩展和负载均衡能力。

### 2.2 核心特性

- **多Broker部署**: 支持主从架构和多主架构
- **负载均衡**: 自动分配消息处理负载
- **集群管理**: 统一的集群状态管理
- **高可用**: 故障自动检测和恢复

### 2.3 组件说明

#### BrokerCluster
Broker集群管理器，负责管理Broker节点集群。

**主要功能**:
- Broker节点管理
- 集群状态监控
- 与NameServer通信
- HA服务集成

**配置参数**:
```go
type ClusterConfig struct {
    LocalConfig       *Config       // 本地Broker配置
    ClusterName       string        // 集群名称
    NameServerAddress string        // NameServer地址
    PeerAddresses     []string      // 其他Broker节点地址
    SyncInterval      time.Duration // 同步间隔
    HeartbeatInterval time.Duration // 心跳间隔
    HAConfig          *ha.HAConfig  // HA配置
}
```

## 3. 数据复制实现

### 3.1 架构概述

数据复制服务确保集群中各节点数据的一致性，支持同步和异步两种复制模式。

### 3.2 核心特性

- **多种复制模式**: 支持同步和异步复制
- **批量复制**: 提高复制效率
- **故障重试**: 自动重试失败的复制任务
- **延迟监控**: 实时监控复制延迟

### 3.3 组件说明

#### DataReplicator
数据复制器，负责跨节点数据同步。

**主要功能**:
- 数据复制任务管理
- 复制模式选择
- 节点健康检测
- 复制状态监控

**配置参数**:
```go
type ReplicationConfig struct {
    Mode              ReplicationMode // 复制模式
    ReplicationFactor int             // 复制因子
    SyncTimeout       time.Duration   // 同步超时时间
    BatchSize         int             // 批量复制大小
    MaxRetries        int             // 最大重试次数
    RetryInterval     time.Duration   // 重试间隔
}
```

**复制模式**:
- `ASYNC_REPLICATION`: 异步复制，高性能但可能丢失数据
- `SYNC_REPLICATION`: 同步复制，强一致性但性能较低

## 4. 故障检测与恢复

### 4.1 架构概述

故障检测与恢复服务提供自动化的故障检测和恢复能力，确保系统的高可用性。

### 4.2 核心特性

- **多维度检测**: 网络、磁盘、内存、进程等多方面检测
- **自动恢复**: 支持自动和手动恢复策略
- **告警通知**: 故障事件通知机制
- **历史记录**: 故障事件记录和分析

### 4.3 组件说明

#### FaultDetector
故障检测器，负责检测和处理系统故障。

**主要功能**:
- 故障检测
- 事件记录
- 自动恢复
- 通知告警

**配置参数**:
```go
type FaultDetectorConfig struct {
    CheckInterval        time.Duration // 检查间隔
    NetworkTimeout       time.Duration // 网络超时时间
    DiskUsageThreshold   int64         // 磁盘使用阈值
    MemoryUsageThreshold int64         // 内存使用阈值
    HeartbeatTimeout     time.Duration // 心跳超时时间
    AutoRecovery         bool          // 是否自动恢复
    NotificationHooks    []NotificationHook // 通知钩子
}
```

**故障类型**:
- `NETWORK_FAILURE`: 网络故障
- `DISK_FAILURE`: 磁盘故障
- `MEMORY_FAILURE`: 内存故障
- `PROCESS_FAILURE`: 进程故障
- `UNKNOWN_FAILURE`: 未知故障

**恢复策略**:
- `NO_RECOVERY`: 不恢复
- `AUTO_RECOVERY`: 自动恢复
- `MANUAL_RECOVERY`: 手动恢复

## 5. 集群管理服务

### 5.1 架构概述

集群管理服务作为统一入口，整合所有集群相关功能。

### 5.2 核心特性

- **统一管理**: 集中管理所有集群组件
- **状态监控**: 实时监控集群状态
- **服务协调**: 协调各子服务工作
- **扩展支持**: 易于扩展新功能

### 5.3 组件说明

#### ClusterManagerService
集群管理服务，提供统一的集群管理接口。

**主要功能**:
- 组件初始化
- 服务启动/停止
- 状态监控
- 统一接口

**配置参数**:
```go
type ClusterManagerConfig struct {
    ClusterName           string
    EnableDataReplication bool
    EnableFaultDetection  bool
    EnableHA              bool
    ReplicationConfig     *ReplicationConfig
    FaultDetectorConfig   *FaultDetectorConfig
    HAConfig              *ha.HAConfig
}
```

## 6. 部署指南

### 6.1 NameServer集群部署

1. **配置文件设置**:
```yaml
# nameserver-cluster.yaml
cluster:
  name: "rocketmq-cluster"
  peers:
    - "192.168.1.10:9876"
    - "192.168.1.11:9876"
    - "192.168.1.12:9876"
  syncInterval: "30s"
  heartbeatInterval: "5s"
```

2. **启动命令**:
```bash
# 启动第一个节点
./nameserver --config=nameserver-cluster.yaml --node-id=0

# 启动第二个节点
./nameserver --config=nameserver-cluster.yaml --node-id=1

# 启动第三个节点
./nameserver --config=nameserver-cluster.yaml --node-id=2
```

### 6.2 Broker集群部署

1. **主从架构配置**:
```yaml
# broker-master.yaml
broker:
  name: "broker-a"
  id: 0
  cluster: "rocketmq-cluster"
  role: "MASTER"
  ha:
    listenPort: 10912
    mode: "SYNC_REPLICATION"

# broker-slave.yaml
broker:
  name: "broker-a"
  id: 1
  cluster: "rocketmq-cluster"
  role: "SLAVE"
  ha:
    masterAddress: "192.168.1.10:10912"
    mode: "SYNC_REPLICATION"
```

2. **多主架构配置**:
```yaml
# broker-a.yaml
broker:
  name: "broker-a"
  id: 0
  cluster: "rocketmq-cluster"
  role: "MASTER"

# broker-b.yaml
broker:
  name: "broker-b"
  id: 0
  cluster: "rocketmq-cluster"
  role: "MASTER"

# broker-c.yaml
broker:
  name: "broker-c"
  id: 0
  cluster: "rocketmq-cluster"
  role: "MASTER"
```

## 7. 监控与运维

### 7.1 状态监控

通过HTTP接口获取集群状态:
```bash
# 获取集群状态
curl http://localhost:10976/cluster/status

# 获取Broker详细信息
curl http://localhost:10976/cluster/brokers

# 获取复制状态
curl http://localhost:10976/cluster/replication
```

### 7.2 故障处理

1. **查看故障事件**:
```bash
curl http://localhost:10976/cluster/faults
```

2. **手动触发恢复**:
```bash
curl -X POST http://localhost:10976/cluster/recovery \
  -H "Content-Type: application/json" \
  -d '{"node":"192.168.1.10:10911","action":"restart"}'
```

## 8. 性能优化建议

### 8.1 网络优化
- 使用高速网络连接节点
- 配置合适的超时时间
- 启用网络压缩

### 8.2 存储优化
- 使用SSD存储CommitLog
- 合理配置刷盘策略
- 定期清理过期数据

### 8.3 复制优化
- 根据业务需求选择复制模式
- 调整批量复制大小
- 监控复制延迟

## 9. 安全考虑

### 9.1 网络安全
- 使用TLS加密节点间通信
- 配置防火墙规则
- 启用ACL访问控制

### 9.2 数据安全
- 定期备份重要数据
- 启用数据校验
- 配置合适的复制因子

## 10. 故障排除

### 10.1 常见问题

1. **节点无法加入集群**
   - 检查网络连接
   - 验证配置文件
   - 查看日志信息

2. **数据复制延迟**
   - 检查网络带宽
   - 调整批量复制参数
   - 监控磁盘IO

3. **故障检测误报**
   - 调整检测阈值
   - 检查系统资源
   - 优化检测间隔

### 10.2 日志分析

关键日志信息:
- 节点注册/注销日志
- 数据复制日志
- 故障检测日志
- 恢复操作日志

通过以上增强功能，Go-RocketMQ现在具备了完整的集群和高可用能力，能够满足企业级消息中间件的高可用性要求。