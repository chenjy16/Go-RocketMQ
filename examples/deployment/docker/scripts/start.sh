#!/bin/sh

# Go-RocketMQ Docker容器启动脚本
# 支持启动NameServer、Broker和示例程序

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    if [ "$DEBUG" = "true" ]; then
        echo -e "${BLUE}[DEBUG]${NC} $1"
    fi
}

# 检查必要的目录
check_directories() {
    log_info "检查必要的目录..."
    
    for dir in "$ROCKETMQ_DATA_DIR" "$ROCKETMQ_LOG_DIR" "$ROCKETMQ_CONFIG_DIR"; do
        if [ ! -d "$dir" ]; then
            log_info "创建目录: $dir"
            mkdir -p "$dir"
        fi
    done
}

# 生成配置文件
generate_config() {
    local service_type=$1
    local config_file="$ROCKETMQ_CONFIG_DIR/${service_type}.conf"
    
    log_info "生成 $service_type 配置文件: $config_file"
    
    case $service_type in
        "nameserver")
            cat > "$config_file" << EOF
# NameServer配置文件
listenPort=${ROCKETMQ_NAMESERVER_PORT:-9876}
logLevel=${ROCKETMQ_LOG_LEVEL:-INFO}
logFile=${ROCKETMQ_LOG_DIR}/nameserver.log
routeInfoCleanInterval=${ROCKETMQ_ROUTE_INFO_CLEAN_INTERVAL:-10}
brokerHeartbeatTimeout=${ROCKETMQ_BROKER_HEARTBEAT_TIMEOUT:-120}
EOF
            ;;
        "broker")
            cat > "$config_file" << EOF
# Broker配置文件
brokerName=${ROCKETMQ_BROKER_NAME:-DefaultBroker}
brokerId=${ROCKETMQ_BROKER_ID:-0}
listenPort=${ROCKETMQ_BROKER_PORT:-10911}
haListenPort=${ROCKETMQ_BROKER_HA_PORT:-10912}
nameServerAddr=${ROCKETMQ_NAMESERVER_ADDR:-127.0.0.1:9876}
clusterName=${ROCKETMQ_CLUSTER_NAME:-DefaultCluster}
brokerRole=${ROCKETMQ_BROKER_ROLE:-SYNC_MASTER}
flushDiskType=${ROCKETMQ_FLUSH_DISK_TYPE:-ASYNC_FLUSH}
storePathRoot=${ROCKETMQ_DATA_DIR}
storePathCommitLog=${ROCKETMQ_DATA_DIR}/commitlog
storePathConsumeQueue=${ROCKETMQ_DATA_DIR}/consumequeue
storePathIndex=${ROCKETMQ_DATA_DIR}/index
logLevel=${ROCKETMQ_LOG_LEVEL:-INFO}
logFile=${ROCKETMQ_LOG_DIR}/broker.log
EOF
            # 如果是从节点，添加主节点地址
            if [ "$ROCKETMQ_BROKER_ROLE" = "SLAVE" ] && [ -n "$ROCKETMQ_HA_MASTER_ADDRESS" ]; then
                echo "haMasterAddress=$ROCKETMQ_HA_MASTER_ADDRESS" >> "$config_file"
            fi
            ;;
    esac
}

# 等待服务就绪
wait_for_service() {
    local host=$1
    local port=$2
    local timeout=${3:-60}
    local count=0
    
    log_info "等待服务就绪: $host:$port (超时: ${timeout}s)"
    
    while [ $count -lt $timeout ]; do
        if nc -z "$host" "$port" 2>/dev/null; then
            log_info "服务已就绪: $host:$port"
            return 0
        fi
        sleep 1
        count=$((count + 1))
        if [ $((count % 10)) -eq 0 ]; then
            log_info "等待中... (${count}s/${timeout}s)"
        fi
    done
    
    log_error "等待服务超时: $host:$port"
    return 1
}

# 启动NameServer
start_nameserver() {
    log_info "启动NameServer..."
    
    check_directories
    generate_config "nameserver"
    
    # 设置JVM参数（如果需要）
    export JAVA_OPT="${JAVA_OPT} -server -Xms512m -Xmx512m -Xmn256m"
    
    log_info "NameServer配置:"
    log_info "  监听端口: ${ROCKETMQ_NAMESERVER_PORT:-9876}"
    log_info "  日志级别: ${ROCKETMQ_LOG_LEVEL:-INFO}"
    log_info "  数据目录: $ROCKETMQ_DATA_DIR"
    log_info "  日志目录: $ROCKETMQ_LOG_DIR"
    
    exec "$ROCKETMQ_HOME/nameserver" -c "$ROCKETMQ_CONFIG_DIR/nameserver.conf"
}

# 启动Broker
start_broker() {
    log_info "启动Broker..."
    
    check_directories
    generate_config "broker"
    
    # 等待NameServer就绪
    if [ -n "$ROCKETMQ_NAMESERVER_ADDR" ]; then
        nameserver_host=$(echo "$ROCKETMQ_NAMESERVER_ADDR" | cut -d':' -f1)
        nameserver_port=$(echo "$ROCKETMQ_NAMESERVER_ADDR" | cut -d':' -f2)
        wait_for_service "$nameserver_host" "$nameserver_port"
    fi
    
    # 如果是从节点，等待主节点就绪
    if [ "$ROCKETMQ_BROKER_ROLE" = "SLAVE" ] && [ -n "$ROCKETMQ_HA_MASTER_ADDRESS" ]; then
        master_host=$(echo "$ROCKETMQ_HA_MASTER_ADDRESS" | cut -d':' -f1)
        master_port=$(echo "$ROCKETMQ_HA_MASTER_ADDRESS" | cut -d':' -f2)
        wait_for_service "$master_host" "$master_port"
    fi
    
    # 设置JVM参数（如果需要）
    export JAVA_OPT="${JAVA_OPT} -server -Xms1g -Xmx1g -Xmn512m"
    
    log_info "Broker配置:"
    log_info "  Broker名称: ${ROCKETMQ_BROKER_NAME:-DefaultBroker}"
    log_info "  Broker ID: ${ROCKETMQ_BROKER_ID:-0}"
    log_info "  监听端口: ${ROCKETMQ_BROKER_PORT:-10911}"
    log_info "  HA端口: ${ROCKETMQ_BROKER_HA_PORT:-10912}"
    log_info "  NameServer: ${ROCKETMQ_NAMESERVER_ADDR:-127.0.0.1:9876}"
    log_info "  集群名称: ${ROCKETMQ_CLUSTER_NAME:-DefaultCluster}"
    log_info "  Broker角色: ${ROCKETMQ_BROKER_ROLE:-SYNC_MASTER}"
    log_info "  刷盘类型: ${ROCKETMQ_FLUSH_DISK_TYPE:-ASYNC_FLUSH}"
    log_info "  数据目录: $ROCKETMQ_DATA_DIR"
    log_info "  日志目录: $ROCKETMQ_LOG_DIR"
    
    exec "$ROCKETMQ_HOME/broker" -c "$ROCKETMQ_CONFIG_DIR/broker.conf"
}

# 启动生产者示例
start_producer_example() {
    log_info "启动生产者示例..."
    
    # 等待Broker就绪
    if [ -n "$ROCKETMQ_NAMESERVER_ADDR" ]; then
        nameserver_host=$(echo "$ROCKETMQ_NAMESERVER_ADDR" | cut -d':' -f1)
        nameserver_port=$(echo "$ROCKETMQ_NAMESERVER_ADDR" | cut -d':' -f2)
        wait_for_service "$nameserver_host" "$nameserver_port"
        
        # 额外等待一段时间确保Broker注册完成
        log_info "等待Broker注册完成..."
        sleep 10
    fi
    
    log_info "生产者配置:"
    log_info "  NameServer: ${ROCKETMQ_NAMESERVER_ADDR:-127.0.0.1:9876}"
    log_info "  生产者组: ${PRODUCER_GROUP:-demo_producer_group}"
    log_info "  Topic: ${TOPIC_NAME:-DemoTopic}"
    log_info "  消息数量: ${MESSAGE_COUNT:-100}"
    
    exec "$ROCKETMQ_HOME/examples/producer-example"
}

# 启动消费者示例
start_consumer_example() {
    log_info "启动消费者示例..."
    
    # 等待Broker就绪
    if [ -n "$ROCKETMQ_NAMESERVER_ADDR" ]; then
        nameserver_host=$(echo "$ROCKETMQ_NAMESERVER_ADDR" | cut -d':' -f1)
        nameserver_port=$(echo "$ROCKETMQ_NAMESERVER_ADDR" | cut -d':' -f2)
        wait_for_service "$nameserver_host" "$nameserver_port"
        
        # 额外等待一段时间确保Broker注册完成
        log_info "等待Broker注册完成..."
        sleep 10
    fi
    
    log_info "消费者配置:"
    log_info "  NameServer: ${ROCKETMQ_NAMESERVER_ADDR:-127.0.0.1:9876}"
    log_info "  消费者组: ${CONSUMER_GROUP:-demo_consumer_group}"
    log_info "  Topic: ${TOPIC_NAME:-DemoTopic}"
    
    exec "$ROCKETMQ_HOME/examples/consumer-example"
}

# 启动管理工具
start_admin_tool() {
    log_info "启动管理工具..."
    
    log_info "管理工具配置:"
    log_info "  NameServer: ${ROCKETMQ_NAMESERVER_ADDR:-127.0.0.1:9876}"
    
    exec "$ROCKETMQ_HOME/examples/admin-tool" "$@"
}

# 显示帮助信息
show_help() {
    echo "Go-RocketMQ Docker容器启动脚本"
    echo ""
    echo "用法: $0 <service> [options]"
    echo ""
    echo "服务类型:"
    echo "  nameserver          启动NameServer"
    echo "  broker              启动Broker"
    echo "  producer-example    启动生产者示例"
    echo "  consumer-example    启动消费者示例"
    echo "  admin-tool          启动管理工具"
    echo "  help                显示此帮助信息"
    echo ""
    echo "环境变量:"
    echo "  ROCKETMQ_NAMESERVER_ADDR     NameServer地址 (默认: 127.0.0.1:9876)"
    echo "  ROCKETMQ_NAMESERVER_PORT     NameServer端口 (默认: 9876)"
    echo "  ROCKETMQ_BROKER_NAME         Broker名称 (默认: DefaultBroker)"
    echo "  ROCKETMQ_BROKER_ID           Broker ID (默认: 0)"
    echo "  ROCKETMQ_BROKER_PORT         Broker端口 (默认: 10911)"
    echo "  ROCKETMQ_BROKER_HA_PORT      Broker HA端口 (默认: 10912)"
    echo "  ROCKETMQ_BROKER_ROLE         Broker角色 (默认: SYNC_MASTER)"
    echo "  ROCKETMQ_CLUSTER_NAME        集群名称 (默认: DefaultCluster)"
    echo "  ROCKETMQ_FLUSH_DISK_TYPE     刷盘类型 (默认: ASYNC_FLUSH)"
    echo "  ROCKETMQ_LOG_LEVEL           日志级别 (默认: INFO)"
    echo "  DEBUG                        调试模式 (默认: false)"
    echo ""
    echo "示例:"
    echo "  $0 nameserver"
    echo "  $0 broker"
    echo "  $0 producer-example"
    echo "  $0 consumer-example"
    echo "  $0 admin-tool cluster list"
}

# 信号处理
trap 'log_info "接收到停止信号，正在优雅关闭..."; exit 0' TERM INT

# 主函数
main() {
    local service_type=${1:-nameserver}
    
    log_info "Go-RocketMQ Docker容器启动"
    log_info "服务类型: $service_type"
    log_info "容器ID: $(hostname)"
    log_info "启动时间: $(date)"
    
    case $service_type in
        "nameserver")
            start_nameserver
            ;;
        "broker")
            start_broker
            ;;
        "producer-example")
            start_producer_example
            ;;
        "consumer-example")
            start_consumer_example
            ;;
        "admin-tool")
            shift
            start_admin_tool "$@"
            ;;
        "help" | "-h" | "--help")
            show_help
            ;;
        *)
            log_error "未知的服务类型: $service_type"
            show_help
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@"