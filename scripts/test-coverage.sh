#!/bin/bash

# Go-RocketMQ 测试覆盖率脚本
# 用于生成测试覆盖率报告

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
COVERAGE_DIR="coverage"
COVERAGE_FILE="${COVERAGE_DIR}/coverage.out"
COVERAGE_HTML="${COVERAGE_DIR}/coverage.html"
COVERAGE_JSON="${COVERAGE_DIR}/coverage.json"
MIN_COVERAGE=80

# 函数：打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 函数：显示帮助信息
show_help() {
    echo "Go-RocketMQ 测试覆盖率工具"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -h, --help          显示此帮助信息"
    echo "  -v, --verbose       详细输出"
    echo "  -o, --output DIR    指定输出目录 (默认: coverage)"
    echo "  -m, --min-coverage  最小覆盖率阈值 (默认: 80)"
    echo "  --html              生成HTML报告"
    echo "  --json              生成JSON报告"
    echo "  --upload            上传覆盖率到codecov (需要CODECOV_TOKEN)"
    echo "  --ci                CI模式，失败时退出"
    echo ""
    echo "示例:"
    echo "  $0                  # 运行基本覆盖率测试"
    echo "  $0 --html --json   # 生成HTML和JSON报告"
    echo "  $0 --ci --min-coverage 85  # CI模式，要求85%覆盖率"
}

# 解析命令行参数
VERBOSE=false
GENERATE_HTML=false
GENERATE_JSON=false
UPLOAD_CODECOV=false
CI_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -o|--output)
            COVERAGE_DIR="$2"
            COVERAGE_FILE="${COVERAGE_DIR}/coverage.out"
            COVERAGE_HTML="${COVERAGE_DIR}/coverage.html"
            COVERAGE_JSON="${COVERAGE_DIR}/coverage.json"
            shift 2
            ;;
        -m|--min-coverage)
            MIN_COVERAGE="$2"
            shift 2
            ;;
        --html)
            GENERATE_HTML=true
            shift
            ;;
        --json)
            GENERATE_JSON=true
            shift
            ;;
        --upload)
            UPLOAD_CODECOV=true
            shift
            ;;
        --ci)
            CI_MODE=true
            shift
            ;;
        *)
            print_error "未知选项: $1"
            show_help
            exit 1
            ;;
    esac
done

# 函数：检查依赖
check_dependencies() {
    print_info "检查依赖..."
    
    if ! command -v go &> /dev/null; then
        print_error "Go 未安装或不在PATH中"
        exit 1
    fi
    
    if $UPLOAD_CODECOV && ! command -v curl &> /dev/null; then
        print_error "curl 未安装，无法上传到codecov"
        exit 1
    fi
    
    print_success "依赖检查通过"
}

# 函数：创建输出目录
setup_directories() {
    print_info "设置输出目录: $COVERAGE_DIR"
    mkdir -p "$COVERAGE_DIR"
}

# 函数：运行测试并生成覆盖率
run_tests_with_coverage() {
    print_info "运行测试并生成覆盖率报告..."
    
    # 清理之前的覆盖率文件
    rm -f "$COVERAGE_FILE"
    
    # 运行测试
    if $VERBOSE; then
        go test -v -race -coverprofile="$COVERAGE_FILE" -covermode=atomic ./...
    else
        go test -race -coverprofile="$COVERAGE_FILE" -covermode=atomic ./...
    fi
    
    if [ $? -ne 0 ]; then
        print_error "测试失败"
        exit 1
    fi
    
    print_success "测试完成"
}

# 函数：生成覆盖率报告
generate_coverage_report() {
    print_info "生成覆盖率报告..."
    
    # 显示覆盖率统计
    echo ""
    echo "=== 覆盖率统计 ==="
    go tool cover -func="$COVERAGE_FILE" | tail -1
    
    # 获取总覆盖率
    TOTAL_COVERAGE=$(go tool cover -func="$COVERAGE_FILE" | tail -1 | awk '{print $3}' | sed 's/%//')
    
    echo ""
    echo "=== 详细覆盖率 ==="
    go tool cover -func="$COVERAGE_FILE"
    
    # 检查覆盖率阈值
    if (( $(echo "$TOTAL_COVERAGE < $MIN_COVERAGE" | bc -l) )); then
        print_warning "覆盖率 ${TOTAL_COVERAGE}% 低于最小要求 ${MIN_COVERAGE}%"
        if $CI_MODE; then
            print_error "CI模式下覆盖率不达标，退出"
            exit 1
        fi
    else
        print_success "覆盖率 ${TOTAL_COVERAGE}% 达到要求 (>= ${MIN_COVERAGE}%)"
    fi
}

# 函数：生成HTML报告
generate_html_report() {
    if $GENERATE_HTML; then
        print_info "生成HTML覆盖率报告..."
        go tool cover -html="$COVERAGE_FILE" -o "$COVERAGE_HTML"
        print_success "HTML报告已生成: $COVERAGE_HTML"
        
        # 在macOS上自动打开HTML报告
        if [[ "$OSTYPE" == "darwin"* ]] && ! $CI_MODE; then
            print_info "在浏览器中打开HTML报告..."
            open "$COVERAGE_HTML"
        fi
    fi
}

# 函数：生成JSON报告
generate_json_report() {
    if $GENERATE_JSON; then
        print_info "生成JSON覆盖率报告..."
        
        # 使用go tool cover生成JSON格式的覆盖率数据
        go tool cover -func="$COVERAGE_FILE" | awk '
        BEGIN { print "{\"files\": [" }
        /\.go:/ { 
            gsub(/\.go:.*/, ".go", $1)
            printf "{\"name\": \"%s\", \"coverage\": %s}%s\n", $1, $3, (NR==1?"":",")
        }
        END { print "]}" }
        ' > "$COVERAGE_JSON"
        
        print_success "JSON报告已生成: $COVERAGE_JSON"
    fi
}

# 函数：上传到codecov
upload_to_codecov() {
    if $UPLOAD_CODECOV; then
        print_info "上传覆盖率到codecov..."
        
        if [ -z "$CODECOV_TOKEN" ]; then
            print_warning "CODECOV_TOKEN 环境变量未设置，跳过上传"
            return
        fi
        
        # 下载并运行codecov上传脚本
        curl -s https://codecov.io/bash | bash -s -- -f "$COVERAGE_FILE" -t "$CODECOV_TOKEN"
        
        if [ $? -eq 0 ]; then
            print_success "覆盖率已上传到codecov"
        else
            print_error "上传到codecov失败"
            if $CI_MODE; then
                exit 1
            fi
        fi
    fi
}

# 函数：清理临时文件
cleanup() {
    if ! $VERBOSE && ! $GENERATE_HTML && ! $GENERATE_JSON; then
        print_info "清理临时文件..."
        # 保留coverage.out文件，但可以选择清理其他临时文件
    fi
}

# 函数：显示总结
show_summary() {
    echo ""
    echo "=== 测试覆盖率总结 ==="
    echo "覆盖率文件: $COVERAGE_FILE"
    
    if $GENERATE_HTML; then
        echo "HTML报告: $COVERAGE_HTML"
    fi
    
    if $GENERATE_JSON; then
        echo "JSON报告: $COVERAGE_JSON"
    fi
    
    echo "最小覆盖率要求: ${MIN_COVERAGE}%"
    
    # 再次显示总覆盖率
    TOTAL_COVERAGE=$(go tool cover -func="$COVERAGE_FILE" | tail -1 | awk '{print $3}' | sed 's/%//')
    echo "当前覆盖率: ${TOTAL_COVERAGE}%"
    
    print_success "测试覆盖率分析完成"
}

# 主函数
main() {
    print_info "开始Go-RocketMQ测试覆盖率分析..."
    
    check_dependencies
    setup_directories
    run_tests_with_coverage
    generate_coverage_report
    generate_html_report
    generate_json_report
    upload_to_codecov
    cleanup
    show_summary
}

# 运行主函数
main "$@"