# Go-RocketMQ Kubernetes部署指南

本目录包含了在Kubernetes环境中部署Go-RocketMQ集群的完整配置文件和脚本。

## 目录结构

```
k8s/
├── README.md                 # 本文档
├── namespace.yaml           # 命名空间配置
├── rbac.yaml               # RBAC权限配置
├── nameserver.yaml         # NameServer部署配置
├── broker.yaml             # Broker部署配置
├── monitoring.yaml         # 监控配置
├── network-policy.yaml     # 网络策略配置
└── deploy.sh              # 自动化部署脚本
```

## 快速开始

### 前置条件

1. **Kubernetes集群**: 版本 >= 1.20
2. **kubectl**: 已配置并能访问集群
3. **存储类**: 集群中需要有可用的存储类（默认使用`fast-ssd`）
4. **资源要求**: 
   - CPU: 至少4核心
   - 内存: 至少8GB
   - 存储: 至少200GB

### 一键部署

```bash
# 克隆项目并进入部署目录
cd examples/deployment/k8s

# 执行部署脚本
./deploy.sh deploy

# 查看部署状态
./deploy.sh status
```

### 手动部署

如果你想逐步部署，可以按以下顺序执行：

```bash
# 1. 创建命名空间
kubectl apply -f namespace.yaml

# 2. 部署RBAC配置
kubectl apply -f rbac.yaml

# 3. 部署NameServer
kubectl apply -f nameserver.yaml

# 4. 等待NameServer就绪
kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=nameserver -n rocketmq --timeout=300s

# 5. 部署Broker
kubectl apply -f broker.yaml

# 6. 等待Broker就绪
kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=broker -n rocketmq --timeout=300s

# 7. （可选）部署监控
kubectl apply -f monitoring.yaml

# 8. （可选）部署网络策略
kubectl apply -f network-policy.yaml
```

## 配置说明

### 命名空间配置 (namespace.yaml)

创建专用的`rocketmq`命名空间，用于隔离RocketMQ相关资源。

### RBAC配置 (rbac.yaml)

包含以下组件：
- **ServiceAccount**: `rocketmq-serviceaccount`
- **ClusterRole**: 集群级别权限
- **Role**: 命名空间级别权限
- **RoleBinding**: 权限绑定
- **SecurityContextConstraints**: OpenShift安全上下文约束

### NameServer配置 (nameserver.yaml)

- **副本数**: 2个（高可用）
- **资源配置**: 1Gi内存，500m CPU
- **存储**: 20Gi日志存储，10Gi数据存储
- **端口**: 9876（服务端口），8080（监控端口）
- **健康检查**: 就绪性、存活性和启动探针

### Broker配置 (broker.yaml)

包含Master和Slave两种角色：

#### Broker Master
- **副本数**: 2个
- **资源配置**: 4Gi内存，2000m CPU
- **存储**: 100Gi数据存储，20Gi日志存储
- **端口**: 10911（服务端口），10912（HA端口），8080（监控端口）

#### Broker Slave
- **副本数**: 2个
- **资源配置**: 4Gi内存，2000m CPU
- **存储**: 100Gi数据存储，20Gi日志存储
- **端口**: 10921（服务端口），10922（HA端口），8080（监控端口）

### 监控配置 (monitoring.yaml)

包含以下监控组件：
- **ServiceMonitor**: Prometheus服务发现
- **PrometheusRule**: 告警规则
- **Grafana Dashboard**: 监控面板
- **Prometheus配置**: 指标收集配置
- **Alertmanager配置**: 告警管理
- **Jaeger配置**: 分布式追踪
- **Fluent Bit配置**: 日志收集

### 网络策略配置 (network-policy.yaml)

实现网络安全隔离：
- **默认拒绝策略**: 拒绝所有入站流量
- **组件间通信**: 允许必要的组件间通信
- **监控访问**: 允许监控系统访问metrics端口
- **跨命名空间访问**: 控制跨命名空间访问

## 部署脚本使用

`deploy.sh`脚本提供了完整的部署管理功能：

### 基本命令

```bash
# 部署集群
./deploy.sh deploy

# 查看状态
./deploy.sh status

# 查看日志
./deploy.sh logs nameserver
./deploy.sh logs broker-master
./deploy.sh logs broker-slave
./deploy.sh logs all

# 扩缩容
./deploy.sh scale nameserver 3
./deploy.sh scale broker-master 4

# 升级
./deploy.sh upgrade 1.1.0

# 备份配置
./deploy.sh backup

# 卸载集群
./deploy.sh undeploy
./deploy.sh undeploy --force  # 强制删除包括数据
```

### 高级选项

```bash
# 指定命名空间
./deploy.sh -n my-rocketmq deploy

# 指定镜像标签
./deploy.sh -t 1.2.0 deploy

# 指定存储类
./deploy.sh -s standard deploy

# 详细输出
./deploy.sh -v deploy

# 干运行（仅显示操作）
./deploy.sh --dry-run deploy
```

## 访问集群

### 获取服务端点

```bash
# 获取NameServer地址
kubectl get service nameserver-service -n rocketmq

# 获取Broker地址
kubectl get service broker-master-service -n rocketmq
kubectl get service broker-slave-service -n rocketmq
```

### 端口转发（用于本地测试）

```bash
# 转发NameServer端口
kubectl port-forward service/nameserver-service 9876:9876 -n rocketmq

# 转发Broker Master端口
kubectl port-forward service/broker-master-service 10911:10911 -n rocketmq

# 转发监控端口
kubectl port-forward service/nameserver-service 8080:8080 -n rocketmq
```

### 客户端连接

在应用程序中使用以下地址连接：

```go
// NameServer地址
nameServerAddr := "nameserver-service.rocketmq.svc.cluster.local:9876"

// 或者使用Service IP
nameServerAddr := "<NAMESERVER_SERVICE_IP>:9876"
```

## 监控和运维

### 查看Pod状态

```bash
# 查看所有Pod
kubectl get pods -n rocketmq

# 查看Pod详细信息
kubectl describe pod <pod-name> -n rocketmq

# 查看Pod日志
kubectl logs <pod-name> -n rocketmq -f
```

### 查看资源使用

```bash
# 查看资源使用情况
kubectl top pods -n rocketmq
kubectl top nodes

# 查看存储使用
kubectl get pvc -n rocketmq
```

### 故障排查

```bash
# 查看事件
kubectl get events -n rocketmq --sort-by='.lastTimestamp'

# 查看服务状态
kubectl get svc -n rocketmq

# 查看端点
kubectl get endpoints -n rocketmq

# 进入Pod调试
kubectl exec -it <pod-name> -n rocketmq -- /bin/bash
```

## 性能调优

### 资源配置

根据实际负载调整资源配置：

```yaml
# 在broker.yaml中调整
resources:
  requests:
    memory: "4Gi"    # 根据消息量调整
    cpu: "1000m"     # 根据并发量调整
  limits:
    memory: "8Gi"
    cpu: "4000m"
```

### 存储优化

```yaml
# 使用高性能存储类
storageClassName: "fast-ssd"

# 调整存储大小
resources:
  requests:
    storage: 500Gi  # 根据消息保留时间调整
```

### JVM参数调优

在ConfigMap中添加JVM参数：

```yaml
data:
  broker.conf: |
    # JVM参数
    jvmFlags=-Xms4g -Xmx4g -XX:+UseG1GC
```

## 安全配置

### 网络安全

1. **启用网络策略**:
   ```bash
   kubectl apply -f network-policy.yaml
   ```

2. **配置TLS**:
   ```yaml
   # 在broker配置中启用TLS
   tlsEnable=true
   tlsKeyFile=/etc/ssl/private/server.key
   tlsCertFile=/etc/ssl/certs/server.crt
   ```

### 访问控制

1. **启用ACL**:
   ```yaml
   # 在broker配置中启用ACL
   aclEnable=true
   ```

2. **配置用户权限**:
   ```bash
   # 创建用户和权限配置
   kubectl create secret generic rocketmq-acl \
     --from-file=plain_acl.yml \
     -n rocketmq
   ```

## 备份和恢复

### 数据备份

```bash
# 备份配置
./deploy.sh backup

# 备份持久化数据
kubectl exec -n rocketmq <broker-pod> -- tar czf /tmp/backup.tar.gz /data/rocketmq
kubectl cp rocketmq/<broker-pod>:/tmp/backup.tar.gz ./backup.tar.gz
```

### 数据恢复

```bash
# 恢复数据到新Pod
kubectl cp ./backup.tar.gz rocketmq/<new-broker-pod>:/tmp/
kubectl exec -n rocketmq <new-broker-pod> -- tar xzf /tmp/backup.tar.gz -C /
```

## 升级策略

### 滚动升级

```bash
# 使用脚本升级
./deploy.sh upgrade 1.1.0

# 或手动升级
kubectl set image statefulset/nameserver nameserver=go-rocketmq:1.1.0 -n rocketmq
kubectl set image statefulset/broker-master broker=go-rocketmq:1.1.0 -n rocketmq
kubectl set image statefulset/broker-slave broker=go-rocketmq:1.1.0 -n rocketmq
```

### 回滚

```bash
# 查看历史版本
kubectl rollout history statefulset/broker-master -n rocketmq

# 回滚到上一版本
kubectl rollout undo statefulset/broker-master -n rocketmq
```

## 常见问题

### Q: Pod启动失败

**A**: 检查以下几点：
1. 资源是否充足
2. 存储类是否存在
3. 镜像是否可用
4. 配置是否正确

```bash
kubectl describe pod <pod-name> -n rocketmq
kubectl logs <pod-name> -n rocketmq
```

### Q: 服务无法访问

**A**: 检查网络配置：
1. Service是否正常
2. 端点是否就绪
3. 网络策略是否阻止访问

```bash
kubectl get svc -n rocketmq
kubectl get endpoints -n rocketmq
kubectl describe networkpolicy -n rocketmq
```

### Q: 存储空间不足

**A**: 扩容存储：
```bash
# 编辑PVC
kubectl edit pvc data-broker-master-0 -n rocketmq

# 修改storage大小
spec:
  resources:
    requests:
      storage: 200Gi  # 增加存储大小
```

### Q: 性能问题

**A**: 性能调优：
1. 增加资源配置
2. 调整JVM参数
3. 优化存储配置
4. 增加副本数

## 联系支持

如果遇到问题，请：
1. 查看日志和事件
2. 检查配置文件
3. 参考官方文档
4. 提交Issue到项目仓库

---

更多详细信息请参考：
- [Go-RocketMQ官方文档](../../../README.md)
- [Kubernetes官方文档](https://kubernetes.io/docs/)
- [RocketMQ官方文档](https://rocketmq.apache.org/docs/)