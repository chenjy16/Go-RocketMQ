#!/bin/sh

# Go-RocketMQ Docker容器健康检查脚本
# 检查NameServer和Broker的健康状态

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[HEALTH]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[HEALTH]${NC} $1"
}

log_error() {
    echo -e "${RED}[HEALTH]${NC} $1"
}

# 检查端口是否可用
check_port() {
    local host=${1:-localhost}
    local port=$2
    local service_name=$3
    
    if nc -z "$host" "$port" 2>/dev/null; then
        log_info "$service_name 端口检查通过: $host:$port"
        return 0
    else
        log_error "$service_name 端口检查失败: $host:$port"
        return 1
    fi
}

# 检查进程是否运行
check_process() {
    local process_name=$1
    local service_name=$2
    
    if pgrep -f "$process_name" > /dev/null 2>&1; then
        log_info "$service_name 进程检查通过: $process_name"
        return 0
    else
        log_error "$service_name 进程检查失败: $process_name"
        return 1
    fi
}

# 检查文件是否存在
check_file() {
    local file_path=$1
    local description=$2
    
    if [ -f "$file_path" ]; then
        log_info "文件检查通过: $description ($file_path)"
        return 0
    else
        log_warn "文件检查失败: $description ($file_path)"
        return 1
    fi
}

# 检查目录是否存在且可写
check_directory() {
    local dir_path=$1
    local description=$2
    
    if [ -d "$dir_path" ] && [ -w "$dir_path" ]; then
        log_info "目录检查通过: $description ($dir_path)"
        return 0
    else
        log_error "目录检查失败: $description ($dir_path)"
        return 1
    fi
}

# 检查磁盘空间
check_disk_space() {
    local path=${1:-/data/rocketmq}
    local threshold=${2:-90}  # 默认90%阈值
    
    if [ ! -d "$path" ]; then
        log_warn "磁盘空间检查跳过: 目录不存在 ($path)"
        return 0
    fi
    
    local usage=$(df "$path" | awk 'NR==2 {print $5}' | sed 's/%//')
    
    if [ "$usage" -lt "$threshold" ]; then
        log_info "磁盘空间检查通过: $path 使用率 ${usage}%"
        return 0
    else
        log_error "磁盘空间检查失败: $path 使用率 ${usage}% (阈值: ${threshold}%)"
        return 1
    fi
}

# 检查内存使用
check_memory() {
    local threshold=${1:-90}  # 默认90%阈值
    
    local total_mem=$(free | awk 'NR==2{print $2}')
    local used_mem=$(free | awk 'NR==2{print $3}')
    local usage=$((used_mem * 100 / total_mem))
    
    if [ "$usage" -lt "$threshold" ]; then
        log_info "内存使用检查通过: 使用率 ${usage}%"
        return 0
    else
        log_error "内存使用检查失败: 使用率 ${usage}% (阈值: ${threshold}%)"
        return 1
    fi
}

# NameServer健康检查
check_nameserver_health() {
    log_info "开始NameServer健康检查..."
    
    local port=${ROCKETMQ_NAMESERVER_PORT:-9876}
    local errors=0
    
    # 检查端口
    if ! check_port "localhost" "$port" "NameServer"; then
        errors=$((errors + 1))
    fi
    
    # 检查进程
    if ! check_process "nameserver" "NameServer"; then
        errors=$((errors + 1))
    fi
    
    # 检查配置文件
    check_file "$ROCKETMQ_CONFIG_DIR/nameserver.conf" "NameServer配置文件"
    
    # 检查日志目录
    if ! check_directory "$ROCKETMQ_LOG_DIR" "日志目录"; then
        errors=$((errors + 1))
    fi
    
    # 检查磁盘空间
    if ! check_disk_space "$ROCKETMQ_LOG_DIR" 95; then
        errors=$((errors + 1))
    fi
    
    # 检查内存使用
    check_memory 95
    
    if [ $errors -eq 0 ]; then
        log_info "NameServer健康检查通过"
        return 0
    else
        log_error "NameServer健康检查失败 (错误数: $errors)"
        return 1
    fi
}

# Broker健康检查
check_broker_health() {
    log_info "开始Broker健康检查..."
    
    local port=${ROCKETMQ_BROKER_PORT:-10911}
    local ha_port=${ROCKETMQ_BROKER_HA_PORT:-10912}
    local errors=0
    
    # 检查主端口
    if ! check_port "localhost" "$port" "Broker"; then
        errors=$((errors + 1))
    fi
    
    # 检查HA端口
    if ! check_port "localhost" "$ha_port" "Broker HA"; then
        errors=$((errors + 1))
    fi
    
    # 检查进程
    if ! check_process "broker" "Broker"; then
        errors=$((errors + 1))
    fi
    
    # 检查配置文件
    check_file "$ROCKETMQ_CONFIG_DIR/broker.conf" "Broker配置文件"
    
    # 检查数据目录
    if ! check_directory "$ROCKETMQ_DATA_DIR" "数据目录"; then
        errors=$((errors + 1))
    fi
    
    # 检查日志目录
    if ! check_directory "$ROCKETMQ_LOG_DIR" "日志目录"; then
        errors=$((errors + 1))
    fi
    
    # 检查CommitLog目录
    if ! check_directory "$ROCKETMQ_DATA_DIR/commitlog" "CommitLog目录"; then
        errors=$((errors + 1))
    fi
    
    # 检查ConsumeQueue目录
    if ! check_directory "$ROCKETMQ_DATA_DIR/consumequeue" "ConsumeQueue目录"; then
        errors=$((errors + 1))
    fi
    
    # 检查磁盘空间
    if ! check_disk_space "$ROCKETMQ_DATA_DIR" 90; then
        errors=$((errors + 1))
    fi
    
    # 检查内存使用
    check_memory 90
    
    # 检查NameServer连接
    if [ -n "$ROCKETMQ_NAMESERVER_ADDR" ]; then
        nameserver_host=$(echo "$ROCKETMQ_NAMESERVER_ADDR" | cut -d':' -f1)
        nameserver_port=$(echo "$ROCKETMQ_NAMESERVER_ADDR" | cut -d':' -f2)
        if ! check_port "$nameserver_host" "$nameserver_port" "NameServer连接"; then
            log_warn "NameServer连接检查失败，但不影响Broker健康状态"
        fi
    fi
    
    if [ $errors -eq 0 ]; then
        log_info "Broker健康检查通过"
        return 0
    else
        log_error "Broker健康检查失败 (错误数: $errors)"
        return 1
    fi
}

# 通用健康检查
check_general_health() {
    log_info "开始通用健康检查..."
    
    local errors=0
    
    # 检查基本目录
    if ! check_directory "$ROCKETMQ_HOME" "RocketMQ主目录"; then
        errors=$((errors + 1))
    fi
    
    # 检查可执行文件
    if [ -f "$ROCKETMQ_HOME/nameserver" ]; then
        log_info "NameServer可执行文件存在"
    fi
    
    if [ -f "$ROCKETMQ_HOME/broker" ]; then
        log_info "Broker可执行文件存在"
    fi
    
    # 检查系统负载
    local load_avg=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | sed 's/,//')
    local cpu_count=$(nproc)
    local load_threshold=$((cpu_count * 2))
    
    if [ "$(echo "$load_avg < $load_threshold" | bc 2>/dev/null || echo "1")" = "1" ]; then
        log_info "系统负载检查通过: $load_avg (CPU数: $cpu_count)"
    else
        log_warn "系统负载较高: $load_avg (CPU数: $cpu_count)"
    fi
    
    if [ $errors -eq 0 ]; then
        log_info "通用健康检查通过"
        return 0
    else
        log_error "通用健康检查失败 (错误数: $errors)"
        return 1
    fi
}

# 自动检测服务类型
detect_service_type() {
    if pgrep -f "nameserver" > /dev/null 2>&1; then
        echo "nameserver"
    elif pgrep -f "broker" > /dev/null 2>&1; then
        echo "broker"
    else
        echo "unknown"
    fi
}

# 主函数
main() {
    local service_type=${1:-$(detect_service_type)}
    
    log_info "开始健康检查 (服务类型: $service_type)"
    log_info "检查时间: $(date)"
    
    # 通用检查
    if ! check_general_health; then
        exit 1
    fi
    
    # 服务特定检查
    case $service_type in
        "nameserver")
            if ! check_nameserver_health; then
                exit 1
            fi
            ;;
        "broker")
            if ! check_broker_health; then
                exit 1
            fi
            ;;
        "unknown")
            log_warn "无法检测服务类型，仅执行通用检查"
            ;;
        *)
            log_warn "未知服务类型: $service_type，仅执行通用检查"
            ;;
    esac
    
    log_info "所有健康检查通过 ✓"
    exit 0
}

# 执行主函数
main "$@"