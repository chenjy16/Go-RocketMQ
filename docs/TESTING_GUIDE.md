# Go-RocketMQ 测试指南

本文档提供了 Go-RocketMQ 项目的完整测试指南，包括测试策略、最佳实践和工具使用。

## 目录

- [测试概述](#测试概述)
- [测试类型](#测试类型)
- [运行测试](#运行测试)
- [测试覆盖率](#测试覆盖率)
- [测试工具](#测试工具)
- [最佳实践](#最佳实践)
- [CI/CD 集成](#cicd-集成)

## 测试概述

Go-RocketMQ 采用多层次的测试策略，确保代码质量和系统稳定性：

- **单元测试**: 测试单个函数和方法
- **集成测试**: 测试模块间的交互
- **性能测试**: 验证系统性能指标
- **基准测试**: 监控性能回归

## 测试类型

### 1. 单元测试

单元测试覆盖各个模块的核心功能：

```bash
# 运行所有单元测试
make test

# 运行特定模块测试
go test ./pkg/nameserver/...
go test ./pkg/broker/...
go test ./pkg/store/...
```

### 2. 集成测试

集成测试验证各模块间的协作：

```bash
# 运行集成测试
make test-integration

# 或直接运行
go test ./pkg/integration_test.go -v
```

### 3. 性能测试

性能测试包括基准测试和性能回归测试：

```bash
# 运行性能基准测试
make test-benchmark

# 运行特定性能测试
go test -bench=. ./pkg/performance/...
```

### 4. 竞态检测

检测并发访问中的竞态条件：

```bash
# 运行竞态检测测试
make test-race

# 或直接运行
go test -race ./...
```

## 运行测试

### 基本命令

```bash
# 运行所有测试
make test

# 运行详细测试
make test-verbose

# 运行覆盖率测试
make test-coverage

# 生成HTML覆盖率报告
make test-coverage-html
```

### 高级选项

```bash
# 运行特定测试
go test -run TestSpecificFunction ./pkg/module/

# 运行测试并显示详细输出
go test -v ./...

# 运行测试超时设置
go test -timeout 30s ./...

# 并行运行测试
go test -parallel 4 ./...
```

## 测试覆盖率

### 生成覆盖率报告

```bash
# 生成覆盖率文件
go test -coverprofile=coverage/coverage.out ./...

# 查看覆盖率统计
go tool cover -func=coverage/coverage.out

# 生成HTML报告
go tool cover -html=coverage/coverage.out -o coverage/coverage.html
```

### 覆盖率目标

- **总体覆盖率**: 目标 > 70%
- **核心模块**: 目标 > 80%
  - `pkg/nameserver`: > 90%
  - `pkg/broker`: > 80%
  - `pkg/store`: > 75%
  - `pkg/protocol`: 100%

### 当前覆盖率状态

| 模块 | 覆盖率 | 状态 | 最近更新 |
|------|--------|------|----------|
| nameserver | 91.2% | ✅ 优秀 | - |
| protocol | 100% | ✅ 完美 | - |
| common | 84.6% | ✅ 良好 | - |
| cluster | 55.2% | ⚠️ 需改进 | - |
| store | 54.3% | ⚠️ 需改进 | ⬆️ 提升 |
| performance | 41.1% | ⚠️ 需改进 | - |
| testutil | 43.1% | ⚠️ 需改进 | - |
| **failover** | **19.0%** | **🆕 新增** | **✅ 新增测试** |
| **ha** | **31.4%** | **🆕 新增** | **✅ 新增测试** |
| broker | 53.6% | ⚠️ 需改进 | - |

## 测试工具

### 内置测试工具

项目提供了丰富的测试工具在 `pkg/testutil` 包中：

```go
// 测试环境管理
env := &testutil.TestEnvironment{}
env.StartNameServer()
env.StartBroker()
defer env.Cleanup()

// 断言函数
testutil.AssertEqual(t, expected, actual, "Values should be equal")
testutil.AssertNoError(t, err)
testutil.AssertTrue(t, condition, "Condition should be true")

// 消息创建
msg := testutil.CreateTestMessage("TestTopic", "Hello World")
msgs := testutil.CreateTestMessages("TestTopic", 10)

// 条件等待
testutil.WaitForCondition(t, func() bool {
    return someCondition
}, 5*time.Second, "Condition should be met")
```

### 模拟对象

```go
// 模拟处理器
handler := &testutil.MockHandler{}
handler.SetResponse("success")

// 检查调用
testutil.AssertEqual(t, 1, handler.GetCallCount(), "Handler should be called once")
```

### 性能测试助手

```go
// 性能监控
helper := testutil.NewPerformanceTestHelper()
helper.StartMonitoring()
defer helper.StopMonitoring()

// 获取性能指标
metrics := helper.GetMetrics()
```

## 最佳实践

### 1. 测试命名

```go
// 好的测试命名
func TestBrokerStartStop(t *testing.T) { ... }
func TestMessageStoreWithValidData(t *testing.T) { ... }
func TestConsumerGroupRegistration(t *testing.T) { ... }

// 避免的命名
func TestBroker(t *testing.T) { ... }
func Test1(t *testing.T) { ... }
```

### 2. 测试结构

使用 AAA 模式（Arrange, Act, Assert）：

```go
func TestMessageStore(t *testing.T) {
    // Arrange - 准备测试数据
    store := NewMessageStore(config)
    msg := CreateTestMessage("topic", "body")
    
    // Act - 执行操作
    result, err := store.PutMessage(msg)
    
    // Assert - 验证结果
    testutil.AssertNoError(t, err)
    testutil.AssertNotNil(t, result)
    testutil.AssertEqual(t, "topic", result.Topic)
}
```

### 3. 资源清理

```go
func TestWithResources(t *testing.T) {
    // 使用 defer 确保资源清理
    broker := NewBroker(config)
    defer broker.Stop()
    
    err := broker.Start()
    testutil.AssertNoError(t, err)
    
    // 测试逻辑...
}
```

### 4. 并发测试

```go
func TestConcurrentAccess(t *testing.T) {
    const numGoroutines = 10
    var wg sync.WaitGroup
    
    for i := 0; i < numGoroutines; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            // 并发测试逻辑
        }(i)
    }
    
    wg.Wait()
}
```

### 5. 表驱动测试

```go
func TestMessageValidation(t *testing.T) {
    tests := []struct {
        name    string
        input   *Message
        wantErr bool
    }{
        {"valid message", &Message{Topic: "test", Body: []byte("body")}, false},
        {"empty topic", &Message{Topic: "", Body: []byte("body")}, true},
        {"nil body", &Message{Topic: "test", Body: nil}, true},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := ValidateMessage(tt.input)
            if tt.wantErr {
                testutil.AssertError(t, err)
            } else {
                testutil.AssertNoError(t, err)
            }
        })
    }
}
```

## CI/CD 集成

### GitHub Actions

项目使用 GitHub Actions 进行持续集成：

```yaml
# .github/workflows/test.yml
name: Test
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v3
        with:
          go-version: '1.21'
      - run: make ci-test
      - uses: codecov/codecov-action@v3
        with:
          file: ./coverage/coverage.out
```

### 本地 CI 模拟

```bash
# 运行完整的 CI 测试流程
make ci-test

# 包括：依赖检查、代码检查、测试、覆盖率
```

## 故障排除

### 常见问题

1. **端口冲突**
   ```bash
   # 错误：bind: address already in use
   # 解决：使用 testutil.GetFreePort() 获取空闲端口
   ```

2. **竞态条件**
   ```bash
   # 错误：WARNING: DATA RACE
   # 解决：使用适当的同步机制（mutex, channel）
   ```

3. **测试超时**
   ```bash
   # 增加超时时间
   go test -timeout 30s ./...
   ```

4. **内存泄漏**
   ```bash
   # 使用内存分析
   go test -memprofile=mem.prof ./...
   go tool pprof mem.prof
   ```

### 调试技巧

```go
// 使用 t.Log 输出调试信息
t.Logf("Debug info: %v", value)

// 使用 testing.Short() 跳过长时间运行的测试
if testing.Short() {
    t.Skip("Skipping long-running test in short mode")
}

// 使用 t.Helper() 标记辅助函数
func assertValid(t *testing.T, obj interface{}) {
    t.Helper()
    if !isValid(obj) {
        t.Errorf("Object is not valid: %v", obj)
    }
}
```

## 性能测试指南

### 基准测试编写

```go
func BenchmarkMessageStore(b *testing.B) {
    store := NewMessageStore(config)
    msg := CreateTestMessage("topic", "body")
    
    b.ResetTimer() // 重置计时器
    
    for i := 0; i < b.N; i++ {
        store.PutMessage(msg)
    }
}
```

### 性能回归检测

```go
func TestPerformanceRegression(t *testing.T) {
    start := time.Now()
    
    // 执行性能测试
    for i := 0; i < 10000; i++ {
        // 操作
    }
    
    duration := time.Since(start)
    threshold := 500 * time.Millisecond
    
    if duration > threshold {
        t.Errorf("Performance regression: took %v, expected < %v", duration, threshold)
    }
}
```

## 模块特定测试指南

### Failover 模块测试

故障转移模块的测试重点关注服务的可靠性和故障恢复能力：

```go
func TestFailoverServiceCreation(t *testing.T) {
    config := &FailoverConfig{
        CheckInterval:    5 * time.Second,
        FailoverTimeout:  30 * time.Second,
        RecoveryTimeout:  60 * time.Second,
        MaxRetryCount:    3,
    }
    
    service := NewFailoverService(config)
    testutil.AssertNotNil(t, service)
}

func TestFailoverServiceStartStop(t *testing.T) {
    service := NewFailoverService(defaultConfig)
    
    err := service.Start()
    testutil.AssertNoError(t, err)
    
    defer func() {
        err := service.Stop()
        testutil.AssertNoError(t, err)
    }()
}
```

### HA 模块测试

高可用模块的测试重点关注主从复制和数据一致性：

```go
func TestHAServiceReplication(t *testing.T) {
    // 创建模拟的 CommitLog
    mockLog := &MockCommitLog{}
    
    config := &HAConfig{
        Role:             Master,
        ReplicationMode:  Sync,
        ListenPort:       testutil.GetFreePort(),
        MaxTransferSize:  65536,
    }
    
    service := NewHAService(config, mockLog)
    testutil.AssertNotNil(t, service)
    
    err := service.Start()
    testutil.AssertNoError(t, err)
    defer service.Shutdown()
    
    // 验证复制状态
    status := service.GetReplicationStatus()
    testutil.AssertEqual(t, "master", status["role"])
    testutil.AssertEqual(t, "sync", status["mode"])
}
```

### 并发测试最佳实践

对于涉及并发的模块，需要特别注意资源管理和竞态条件：

```go
func TestHAServiceConcurrentStartStop(t *testing.T) {
    service := NewHAService(defaultConfig, &MockCommitLog{})
    
    // 顺序启动和停止，避免 channel 重复关闭
    for i := 0; i < 3; i++ {
        err := service.Start()
        testutil.AssertNoError(t, err)
        
        time.Sleep(100 * time.Millisecond) // 确保服务完全启动
        
        service.Shutdown()
        time.Sleep(100 * time.Millisecond) // 确保服务完全停止
    }
}
```

## 总结

良好的测试实践是确保 Go-RocketMQ 质量的关键。通过遵循本指南中的最佳实践，可以：

- 提高代码质量和可靠性
- 及早发现和修复问题
- 确保性能不会回归
- 简化代码维护和重构
- **增强关键模块（如 failover 和 ha）的稳定性**

### 最新改进

- ✅ **Failover 模块**: 新增测试覆盖率 19.0%，包含服务创建、启动停止、策略注册等核心功能测试
- ✅ **HA 模块**: 新增测试覆盖率 31.4%，包含主从复制、状态管理、并发安全等测试
- ✅ **并发安全**: 修复了 HA 服务中 channel 重复关闭的问题，提升了并发测试的稳定性
- ✅ **测试稳定性**: 通过合理的资源管理和状态检查，确保测试的可重复性

记住：**测试不仅是验证代码正确性的工具，更是设计良好 API 的指南。**