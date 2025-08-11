#!/bin/bash

# Go-RocketMQ 完整系统测试脚本
# 包括功能测试和性能测试

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 清理函数
cleanup() {
    log_info "清理进程..."
    
    # 停止所有相关进程
    pkill -f "nameserver" 2>/dev/null || true
    pkill -f "broker" 2>/dev/null || true
    pkill -f "producer-example" 2>/dev/null || true
    pkill -f "consumer-example" 2>/dev/null || true
    pkill -f "benchmark" 2>/dev/null || true
    
    sleep 2
    log_success "清理完成"
}

# 设置清理陷阱
trap cleanup EXIT

# 检查构建文件
check_binaries() {
    log_info "检查构建文件..."
    
    local binaries=("build/bin/nameserver" "build/bin/broker" "build/bin/producer-example" "build/bin/consumer-example" "build/bin/benchmark")
    
    for binary in "${binaries[@]}"; do
        if [ ! -f "$binary" ]; then
            log_error "构建文件不存在: $binary"
            log_info "正在构建..."
            make build
            break
        fi
    done
    
    log_success "所有构建文件检查完成"
}

# 启动NameServer
start_nameserver() {
    log_info "启动NameServer..."
    ./build/bin/nameserver > logs/nameserver.log 2>&1 &
    NAMESERVER_PID=$!
    sleep 3
    
    if kill -0 $NAMESERVER_PID 2>/dev/null; then
        log_success "NameServer启动成功 (PID: $NAMESERVER_PID)"
    else
        log_error "NameServer启动失败"
        exit 1
    fi
}

# 启动Broker
start_broker() {
    log_info "启动Broker..."
    ./build/bin/broker > logs/broker.log 2>&1 &
    BROKER_PID=$!
    sleep 5
    
    if kill -0 $BROKER_PID 2>/dev/null; then
        log_success "Broker启动成功 (PID: $BROKER_PID)"
    else
        log_error "Broker启动失败"
        exit 1
    fi
}

# 功能测试
functional_test() {
    log_info "开始功能测试..."
    
    # 测试生产者
    log_info "测试生产者..."
    if ./build/bin/producer-example > logs/producer_test.log 2>&1; then
        log_success "生产者测试通过"
    else
        log_error "生产者测试失败"
        cat logs/producer_test.log
        return 1
    fi
    
    # 启动消费者（后台运行）
    log_info "启动消费者..."
    ./build/bin/consumer-example > logs/consumer_test.log 2>&1 &
    CONSUMER_PID=$!
    sleep 3
    
    if kill -0 $CONSUMER_PID 2>/dev/null; then
        log_success "消费者启动成功"
        # 停止消费者
        kill $CONSUMER_PID 2>/dev/null || true
        wait $CONSUMER_PID 2>/dev/null || true
    else
        log_error "消费者启动失败"
        return 1
    fi
    
    log_success "功能测试完成"
}

# 性能测试
performance_test() {
    log_info "开始性能测试..."
    
    # 同步发送性能测试
    log_info "同步发送性能测试..."
    ./build/bin/benchmark -count=1000 -concurrency=5 -mode=sync -topic=BenchmarkTopic > logs/benchmark_sync.log 2>&1
    if [ $? -eq 0 ]; then
        log_success "同步发送性能测试完成"
        grep -E "(TPS|平均延迟|吞吐量)" logs/benchmark_sync.log
    else
        log_error "同步发送性能测试失败"
        cat logs/benchmark_sync.log
    fi
    
    sleep 2
    
    # 异步发送性能测试
    log_info "异步发送性能测试..."
    ./build/bin/benchmark -count=1000 -concurrency=5 -mode=async -topic=BenchmarkTopic > logs/benchmark_async.log 2>&1
    if [ $? -eq 0 ]; then
        log_success "异步发送性能测试完成"
        grep -E "(TPS|吞吐量)" logs/benchmark_async.log
    else
        log_error "异步发送性能测试失败"
        cat logs/benchmark_async.log
    fi
    
    sleep 2
    
    # 单向发送性能测试
    log_info "单向发送性能测试..."
    ./build/bin/benchmark -count=1000 -concurrency=5 -mode=oneway -topic=BenchmarkTopic > logs/benchmark_oneway.log 2>&1
    if [ $? -eq 0 ]; then
        log_success "单向发送性能测试完成"
        grep -E "(TPS|吞吐量)" logs/benchmark_oneway.log
    else
        log_error "单向发送性能测试失败"
        cat logs/benchmark_oneway.log
    fi
    
    log_success "性能测试完成"
}

# 生成测试报告
generate_report() {
    log_info "生成测试报告..."
    
    local report_file="logs/test_report_$(date +%Y%m%d_%H%M%S).txt"
    
    cat > "$report_file" << EOF
Go-RocketMQ 系统测试报告
========================
测试时间: $(date)
测试环境: $(uname -a)

功能测试结果:
- NameServer: 启动成功
- Broker: 启动成功  
- Producer: 消息发送成功
- Consumer: 启动成功

性能测试结果:
EOF
    
    if [ -f "logs/benchmark_sync.log" ]; then
        echo "" >> "$report_file"
        echo "同步发送性能:" >> "$report_file"
        grep -E "(TPS|平均延迟|吞吐量)" logs/benchmark_sync.log >> "$report_file"
    fi
    
    if [ -f "logs/benchmark_async.log" ]; then
        echo "" >> "$report_file"
        echo "异步发送性能:" >> "$report_file"
        grep -E "(TPS|吞吐量)" logs/benchmark_async.log >> "$report_file"
    fi
    
    if [ -f "logs/benchmark_oneway.log" ]; then
        echo "" >> "$report_file"
        echo "单向发送性能:" >> "$report_file"
        grep -E "(TPS|吞吐量)" logs/benchmark_oneway.log >> "$report_file"
    fi
    
    log_success "测试报告已生成: $report_file"
}

# 主函数
main() {
    echo "========================================"
    echo "    Go-RocketMQ 完整系统测试"
    echo "========================================"
    
    # 创建日志目录
    mkdir -p logs
    
    # 检查构建文件
    check_binaries
    
    # 启动系统组件
    start_nameserver
    start_broker
    
    # 等待系统稳定
    log_info "等待系统稳定..."
    sleep 5
    
    # 运行功能测试
    if functional_test; then
        log_success "功能测试通过"
    else
        log_error "功能测试失败，跳过性能测试"
        exit 1
    fi
    
    # 运行性能测试
    performance_test
    
    # 生成测试报告
    generate_report
    
    echo "========================================"
    log_success "所有测试完成！"
    echo "========================================"
}

# 运行主函数
main "$@"