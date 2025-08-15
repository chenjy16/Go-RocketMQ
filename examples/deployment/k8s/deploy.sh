#!/bin/bash

# Go-RocketMQ Kubernetes部署脚本
# 用于自动化部署Go-RocketMQ集群到Kubernetes环境

set -euo pipefail

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

# 配置变量
NAMESPACE="rocketmq"
IMAGE_TAG="1.0.0"
STORAGE_CLASS="fast-ssd"
DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 帮助信息
show_help() {
    cat << EOF
Go-RocketMQ Kubernetes部署脚本

用法: $0 [选项] [命令]

命令:
  deploy      部署完整的RocketMQ集群
  undeploy    卸载RocketMQ集群
  status      查看部署状态
  logs        查看日志
  scale       扩缩容
  upgrade     升级
  backup      备份配置
  restore     恢复配置

选项:
  -n, --namespace NAMESPACE    指定命名空间 (默认: rocketmq)
  -t, --tag TAG               指定镜像标签 (默认: 1.0.0)
  -s, --storage-class CLASS   指定存储类 (默认: fast-ssd)
  -h, --help                  显示帮助信息
  -v, --verbose               详细输出
  --dry-run                   仅显示将要执行的操作
  --force                     强制执行操作

示例:
  $0 deploy                           # 部署RocketMQ集群
  $0 -n mq-prod deploy               # 部署到指定命名空间
  $0 status                          # 查看部署状态
  $0 logs nameserver                 # 查看NameServer日志
  $0 scale broker-master 3           # 扩容Broker Master到3个副本
  $0 undeploy --force                # 强制卸载集群

EOF
}

# 检查依赖
check_dependencies() {
    log_info "检查依赖..."
    
    # 检查kubectl
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl未安装，请先安装kubectl"
        exit 1
    fi
    
    # 检查集群连接
    if ! kubectl cluster-info &> /dev/null; then
        log_error "无法连接到Kubernetes集群"
        exit 1
    fi
    
    # 检查存储类
    if ! kubectl get storageclass "$STORAGE_CLASS" &> /dev/null; then
        log_warning "存储类 '$STORAGE_CLASS' 不存在，将使用默认存储类"
        STORAGE_CLASS=""
    fi
    
    log_success "依赖检查完成"
}

# 创建命名空间
create_namespace() {
    log_info "创建命名空间 $NAMESPACE..."
    
    if kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log_warning "命名空间 $NAMESPACE 已存在"
    else
        kubectl apply -f "$DEPLOY_DIR/namespace.yaml"
        log_success "命名空间 $NAMESPACE 创建成功"
    fi
}

# 部署RBAC
deploy_rbac() {
    log_info "部署RBAC配置..."
    kubectl apply -f "$DEPLOY_DIR/rbac.yaml"
    log_success "RBAC配置部署成功"
}

# 部署NameServer
deploy_nameserver() {
    log_info "部署NameServer..."
    kubectl apply -f "$DEPLOY_DIR/nameserver.yaml"
    
    # 等待NameServer就绪
    log_info "等待NameServer就绪..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=nameserver -n "$NAMESPACE" --timeout=300s
    log_success "NameServer部署成功"
}

# 部署Broker
deploy_broker() {
    log_info "部署Broker..."
    
    # 更新存储类配置
    if [[ -n "$STORAGE_CLASS" ]]; then
        sed -i.bak "s/storageClassName: \"fast-ssd\"/storageClassName: \"$STORAGE_CLASS\"/g" "$DEPLOY_DIR/broker.yaml"
    fi
    
    kubectl apply -f "$DEPLOY_DIR/broker.yaml"
    
    # 等待Broker Master就绪
    log_info "等待Broker Master就绪..."
    kubectl wait --for=condition=ready pod -l broker-role=master -n "$NAMESPACE" --timeout=300s
    
    # 等待Broker Slave就绪
    log_info "等待Broker Slave就绪..."
    kubectl wait --for=condition=ready pod -l broker-role=slave -n "$NAMESPACE" --timeout=300s
    
    log_success "Broker部署成功"
}

# 部署完整集群
deploy_cluster() {
    log_info "开始部署Go-RocketMQ集群..."
    
    check_dependencies
    create_namespace
    deploy_rbac
    deploy_nameserver
    deploy_broker
    
    log_success "Go-RocketMQ集群部署完成！"
    show_cluster_info
}

# 卸载集群
undeploy_cluster() {
    log_info "开始卸载Go-RocketMQ集群..."
    
    if [[ "${FORCE:-false}" == "true" ]] || read -p "确定要卸载集群吗？这将删除所有数据 (y/N): " -n 1 -r; then
        echo
        
        # 删除Broker
        log_info "删除Broker..."
        kubectl delete -f "$DEPLOY_DIR/broker.yaml" --ignore-not-found=true
        
        # 删除NameServer
        log_info "删除NameServer..."
        kubectl delete -f "$DEPLOY_DIR/nameserver.yaml" --ignore-not-found=true
        
        # 删除RBAC
        log_info "删除RBAC配置..."
        kubectl delete -f "$DEPLOY_DIR/rbac.yaml" --ignore-not-found=true
        
        # 删除PVC（如果强制删除）
        if [[ "${FORCE:-false}" == "true" ]]; then
            log_info "删除持久化存储..."
            kubectl delete pvc -l app.kubernetes.io/name=go-rocketmq -n "$NAMESPACE" --ignore-not-found=true
        fi
        
        # 删除命名空间
        if [[ "${FORCE:-false}" == "true" ]]; then
            log_info "删除命名空间..."
            kubectl delete namespace "$NAMESPACE" --ignore-not-found=true
        fi
        
        log_success "Go-RocketMQ集群卸载完成"
    else
        log_info "取消卸载操作"
    fi
}

# 查看集群状态
show_status() {
    log_info "查看Go-RocketMQ集群状态..."
    
    echo
    echo "=== 命名空间 ==="
    kubectl get namespace "$NAMESPACE" 2>/dev/null || echo "命名空间不存在"
    
    echo
    echo "=== Pod状态 ==="
    kubectl get pods -n "$NAMESPACE" -o wide
    
    echo
    echo "=== Service状态 ==="
    kubectl get services -n "$NAMESPACE"
    
    echo
    echo "=== StatefulSet状态 ==="
    kubectl get statefulsets -n "$NAMESPACE"
    
    echo
    echo "=== PVC状态 ==="
    kubectl get pvc -n "$NAMESPACE"
    
    echo
    echo "=== 事件 ==="
    kubectl get events -n "$NAMESPACE" --sort-by='.lastTimestamp' | tail -10
}

# 查看日志
show_logs() {
    local component="$1"
    local lines="${2:-100}"
    
    case "$component" in
        "nameserver")
            log_info "查看NameServer日志..."
            kubectl logs -l app.kubernetes.io/component=nameserver -n "$NAMESPACE" --tail="$lines" -f
            ;;
        "broker-master")
            log_info "查看Broker Master日志..."
            kubectl logs -l broker-role=master -n "$NAMESPACE" --tail="$lines" -f
            ;;
        "broker-slave")
            log_info "查看Broker Slave日志..."
            kubectl logs -l broker-role=slave -n "$NAMESPACE" --tail="$lines" -f
            ;;
        "all")
            log_info "查看所有组件日志..."
            kubectl logs -l app.kubernetes.io/name=go-rocketmq -n "$NAMESPACE" --tail="$lines"
            ;;
        *)
            log_error "未知组件: $component"
            log_info "可用组件: nameserver, broker-master, broker-slave, all"
            exit 1
            ;;
    esac
}

# 扩缩容
scale_component() {
    local component="$1"
    local replicas="$2"
    
    case "$component" in
        "nameserver")
            log_info "扩缩容NameServer到 $replicas 个副本..."
            kubectl scale statefulset nameserver -n "$NAMESPACE" --replicas="$replicas"
            ;;
        "broker-master")
            log_info "扩缩容Broker Master到 $replicas 个副本..."
            kubectl scale statefulset broker-master -n "$NAMESPACE" --replicas="$replicas"
            ;;
        "broker-slave")
            log_info "扩缩容Broker Slave到 $replicas 个副本..."
            kubectl scale statefulset broker-slave -n "$NAMESPACE" --replicas="$replicas"
            ;;
        *)
            log_error "未知组件: $component"
            log_info "可用组件: nameserver, broker-master, broker-slave"
            exit 1
            ;;
    esac
    
    log_success "扩缩容操作已提交"
}

# 升级集群
upgrade_cluster() {
    local new_tag="$1"
    
    log_info "升级Go-RocketMQ集群到版本 $new_tag..."
    
    # 更新镜像标签
    kubectl set image statefulset/nameserver nameserver="go-rocketmq:$new_tag" -n "$NAMESPACE"
    kubectl set image statefulset/broker-master broker="go-rocketmq:$new_tag" -n "$NAMESPACE"
    kubectl set image statefulset/broker-slave broker="go-rocketmq:$new_tag" -n "$NAMESPACE"
    
    # 等待滚动更新完成
    log_info "等待滚动更新完成..."
    kubectl rollout status statefulset/nameserver -n "$NAMESPACE"
    kubectl rollout status statefulset/broker-master -n "$NAMESPACE"
    kubectl rollout status statefulset/broker-slave -n "$NAMESPACE"
    
    log_success "集群升级完成"
}

# 备份配置
backup_config() {
    local backup_dir="backup-$(date +%Y%m%d-%H%M%S)"
    
    log_info "备份配置到 $backup_dir..."
    
    mkdir -p "$backup_dir"
    
    # 备份ConfigMap
    kubectl get configmap -n "$NAMESPACE" -o yaml > "$backup_dir/configmaps.yaml"
    
    # 备份Secret
    kubectl get secret -n "$NAMESPACE" -o yaml > "$backup_dir/secrets.yaml"
    
    # 备份StatefulSet
    kubectl get statefulset -n "$NAMESPACE" -o yaml > "$backup_dir/statefulsets.yaml"
    
    # 备份Service
    kubectl get service -n "$NAMESPACE" -o yaml > "$backup_dir/services.yaml"
    
    # 备份PVC
    kubectl get pvc -n "$NAMESPACE" -o yaml > "$backup_dir/pvcs.yaml"
    
    log_success "配置备份完成: $backup_dir"
}

# 显示集群信息
show_cluster_info() {
    echo
    echo "=== Go-RocketMQ集群信息 ==="
    echo "命名空间: $NAMESPACE"
    echo "镜像标签: $IMAGE_TAG"
    echo "存储类: ${STORAGE_CLASS:-默认}"
    echo
    
    # 获取服务端点
    local nameserver_ip=$(kubectl get service nameserver-service -n "$NAMESPACE" -o jsonpath='{.spec.clusterIP}' 2>/dev/null || echo "N/A")
    local broker_master_ip=$(kubectl get service broker-master-service -n "$NAMESPACE" -o jsonpath='{.spec.clusterIP}' 2>/dev/null || echo "N/A")
    
    echo "NameServer地址: $nameserver_ip:9876"
    echo "Broker Master地址: $broker_master_ip:10911"
    echo
    
    echo "连接示例:"
    echo "  export ROCKETMQ_NAMESERVER_ADDR=$nameserver_ip:9876"
    echo
    
    echo "管理命令:"
    echo "  查看状态: $0 status"
    echo "  查看日志: $0 logs nameserver"
    echo "  扩缩容: $0 scale nameserver 3"
    echo
}

# 解析命令行参数
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -n|--namespace)
                NAMESPACE="$2"
                shift 2
                ;;
            -t|--tag)
                IMAGE_TAG="$2"
                shift 2
                ;;
            -s|--storage-class)
                STORAGE_CLASS="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            -v|--verbose)
                set -x
                shift
                ;;
            --dry-run)
                DRY_RUN="true"
                shift
                ;;
            --force)
                FORCE="true"
                shift
                ;;
            deploy)
                COMMAND="deploy"
                shift
                ;;
            undeploy)
                COMMAND="undeploy"
                shift
                ;;
            status)
                COMMAND="status"
                shift
                ;;
            logs)
                COMMAND="logs"
                COMPONENT="${2:-all}"
                LINES="${3:-100}"
                shift
                [[ $# -gt 0 ]] && shift
                [[ $# -gt 0 ]] && shift
                ;;
            scale)
                COMMAND="scale"
                SCALE_COMPONENT="$2"
                SCALE_REPLICAS="$3"
                shift 3
                ;;
            upgrade)
                COMMAND="upgrade"
                UPGRADE_TAG="$2"
                shift 2
                ;;
            backup)
                COMMAND="backup"
                shift
                ;;
            restore)
                COMMAND="restore"
                RESTORE_DIR="$2"
                shift 2
                ;;
            *)
                log_error "未知参数: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# 主函数
main() {
    # 默认值
    COMMAND=""
    DRY_RUN="false"
    FORCE="false"
    VERBOSE="false"
    
    # 解析参数
    parse_args "$@"
    
    # 如果没有指定命令，显示帮助
    if [[ -z "$COMMAND" ]]; then
        show_help
        exit 1
    fi
    
    # 执行命令
    case "$COMMAND" in
        "deploy")
            deploy_cluster
            ;;
        "undeploy")
            undeploy_cluster
            ;;
        "status")
            show_status
            ;;
        "logs")
            show_logs "$COMPONENT" "$LINES"
            ;;
        "scale")
            if [[ -z "$SCALE_COMPONENT" ]] || [[ -z "$SCALE_REPLICAS" ]]; then
                log_error "扩缩容需要指定组件和副本数"
                exit 1
            fi
            scale_component "$SCALE_COMPONENT" "$SCALE_REPLICAS"
            ;;
        "upgrade")
            if [[ -z "$UPGRADE_TAG" ]]; then
                log_error "升级需要指定新的镜像标签"
                exit 1
            fi
            upgrade_cluster "$UPGRADE_TAG"
            ;;
        "backup")
            backup_config
            ;;
        "restore")
            if [[ -z "$RESTORE_DIR" ]]; then
                log_error "恢复需要指定备份目录"
                exit 1
            fi
            log_error "恢复功能尚未实现"
            exit 1
            ;;
        *)
            log_error "未知命令: $COMMAND"
            show_help
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@"