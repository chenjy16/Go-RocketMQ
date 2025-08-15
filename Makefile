# Go-RocketMQ Makefile

.PHONY: build clean test run-nameserver run-producer run-consumer fmt vet
.PHONY: test-verbose test-race test-coverage test-coverage-html test-integration test-benchmark
.PHONY: lint lint-fix security coverage-report ci-test help

# 构建目录
BUILD_DIR := build
BIN_DIR := $(BUILD_DIR)/bin

# Go 参数
GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOGET := $(GOCMD) get
GOFMT := $(GOCMD) fmt
GOVET := $(GOCMD) vet

# 覆盖率目录
COVERAGE_DIR := coverage

# 颜色定义
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

# 二进制文件名
NAMESERVER_BINARY := $(BIN_DIR)/nameserver
BROKER_BINARY := $(BIN_DIR)/broker
PRODUCER_EXAMPLE := $(BIN_DIR)/producer-example
CONSUMER_EXAMPLE := $(BIN_DIR)/consumer-example
BENCHMARK := $(BIN_DIR)/benchmark
MONITOR := $(BIN_DIR)/monitor

# 默认目标
all: build

# 创建构建目录
$(BIN_DIR):
	mkdir -p $(BIN_DIR)

# 构建所有组件
build: build-nameserver build-broker build-examples build-benchmark build-tools build-client
	@echo "所有组件构建完成"

# 构建 NameServer
build-nameserver: $(BIN_DIR)
	$(GOBUILD) -o $(NAMESERVER_BINARY) ./cmd/nameserver

# 构建 Broker
build-broker: $(BIN_DIR)
	$(GOBUILD) -o $(BROKER_BINARY) ./cmd/broker

# 构建示例程序
build-examples:
	$(GOBUILD) -o $(PRODUCER_EXAMPLE) ./examples/basic/producer
	$(GOBUILD) -o $(CONSUMER_EXAMPLE) ./examples/basic/consumer

# 构建客户端库
build-client:
	@echo "构建客户端库..."
	cd pkg/client && go build ./...
	@echo "客户端库构建完成"

# 测试客户端库
test-client:
	@echo "测试客户端库..."
	cd pkg/client && go test -v ./...

# 构建性能测试工具
build-benchmark:
	$(GOBUILD) -o $(BENCHMARK) ./examples/benchmark

# 构建工具
build-tools:
	$(GOBUILD) -o $(MONITOR) ./tools/monitor

# 清理构建文件
clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

# 运行测试
test:
	$(GOTEST) -v ./...

# 运行 NameServer
run-nameserver: build-nameserver
	$(NAMESERVER_BINARY)

# 运行 Broker
run-broker: build-broker
	$(BROKER_BINARY)

# 运行生产者示例
run-producer: build-examples
	$(PRODUCER_EXAMPLE)

# 运行消费者示例
run-consumer: build-examples
	$(CONSUMER_EXAMPLE)

# 运行性能测试
benchmark: build-benchmark
	$(BENCHMARK) -count=1000 -concurrency=5 -mode=sync

# 运行异步性能测试
benchmark-async: build-benchmark
	$(BENCHMARK) -count=1000 -concurrency=5 -mode=async

# 运行单向性能测试
benchmark-oneway: build-benchmark
	$(BENCHMARK) -count=1000 -concurrency=5 -mode=oneway

# 运行系统监控
monitor: build-tools
	$(MONITOR)

# 运行Web监控
monitor-web: build-tools
	$(MONITOR) -web -port=8080

# 格式化代码
fmt:
	$(GOFMT) ./...

# 代码检查
vet:
	$(GOVET) ./...

# 下载依赖
deps:
	$(GOGET) -d ./...

# 安装工具
install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# 代码检查（使用 golangci-lint）
lint:
	golangci-lint run

# 开发环境设置
dev-setup: deps install-tools

# 启动开发环境
dev: run-nameserver

# 构建 Docker 镜像
docker-build:
	docker build -t go-rocketmq:latest .

# 运行 Docker 容器
docker-run:
	docker run -p 9876:9876 go-rocketmq:latest

## test-verbose: 运行详细测试
test-verbose:
	@echo "$(BLUE)运行详细测试...$(NC)"
	$(GOTEST) -v ./...
	@echo "$(GREEN)详细测试完成$(NC)"

## test-race: 运行竞态检测测试
test-race:
	@echo "$(BLUE)运行竞态检测测试...$(NC)"
	$(GOTEST) -race ./...
	@echo "$(GREEN)竞态检测测试完成$(NC)"

## test-coverage: 运行覆盖率测试
test-coverage:
	@echo "$(BLUE)运行覆盖率测试...$(NC)"
	@mkdir -p $(COVERAGE_DIR)
	$(GOTEST) -race -coverprofile=$(COVERAGE_DIR)/coverage.out -covermode=atomic ./...
	go tool cover -func=$(COVERAGE_DIR)/coverage.out | tail -1
	@echo "$(GREEN)覆盖率测试完成$(NC)"

## test-coverage-html: 生成HTML覆盖率报告
test-coverage-html: test-coverage
	@echo "$(BLUE)生成HTML覆盖率报告...$(NC)"
	go tool cover -html=$(COVERAGE_DIR)/coverage.out -o $(COVERAGE_DIR)/coverage.html
	@echo "$(GREEN)HTML覆盖率报告已生成: $(COVERAGE_DIR)/coverage.html$(NC)"
	@if command -v open >/dev/null 2>&1; then \
		echo "$(BLUE)在浏览器中打开报告...$(NC)"; \
		open $(COVERAGE_DIR)/coverage.html; \
	fi

## test-integration: 运行集成测试
test-integration:
	@echo "$(BLUE)运行集成测试...$(NC)"
	$(GOTEST) -tags=integration -timeout 10m ./pkg/integration_test.go -v
	@echo "$(GREEN)集成测试完成$(NC)"

## test-benchmark: 运行性能测试
test-benchmark:
	@echo "$(BLUE)运行性能测试...$(NC)"
	$(GOTEST) -bench=. -benchmem ./...
	@echo "$(GREEN)性能测试完成$(NC)"

## lint-fix: 运行代码检查并自动修复
lint-fix:
	@echo "$(BLUE)运行代码检查并自动修复...$(NC)"
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run --fix; \
	else \
		echo "$(YELLOW)golangci-lint 未安装，使用基本修复...$(NC)"; \
		$(GOFMT) ./...; \
	fi
	@echo "$(GREEN)代码检查和修复完成$(NC)"

## security: 运行安全检查
security:
	@echo "$(BLUE)运行安全检查...$(NC)"
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
	else \
		echo "$(YELLOW)gosec 未安装，跳过安全检查$(NC)"; \
	fi
	@echo "$(GREEN)安全检查完成$(NC)"

## coverage-report: 生成覆盖率报告
coverage-report:
	@echo "$(BLUE)生成覆盖率报告...$(NC)"
	./scripts/test-coverage.sh --html --json --min-coverage 70
	@echo "$(GREEN)覆盖率报告生成完成$(NC)"

## ci-test: CI测试流程
ci-test: deps vet lint test-race test-coverage
	@echo "$(GREEN)CI测试流程完成$(NC)"

# 帮助信息
help:
	@echo "$(BLUE)Go-RocketMQ 开发工具$(NC)"
	@echo ""
	@echo "$(BLUE)构建相关:$(NC)"
	@echo "  build           - Build all components"
	@echo "  build-nameserver - Build NameServer"
	@echo "  build-broker    - Build Broker"
	@echo "  build-examples  - Build example programs"
	@echo "  build-client    - Build client library"
	@echo "  clean           - Clean build files"
	@echo ""
	@echo "$(BLUE)测试相关:$(NC)"
	@echo "  test            - Run tests"
	@echo "  test-verbose    - Run verbose tests"
	@echo "  test-race       - Run race detection tests"
	@echo "  test-coverage   - Run coverage tests"
	@echo "  test-coverage-html - Generate HTML coverage report"
	@echo "  test-integration - Run integration tests"
	@echo "  test-benchmark  - Run benchmark tests"
	@echo "  test-client     - Test client library"
	@echo ""
	@echo "$(BLUE)代码质量:$(NC)"
	@echo "  fmt             - Format code"
	@echo "  vet             - Run go vet"
	@echo "  lint            - Run golangci-lint"
	@echo "  lint-fix        - Run lint with auto-fix"
	@echo "  security        - Run security checks"
	@echo ""
	@echo "$(BLUE)运行相关:$(NC)"
	@echo "  run-nameserver  - Run NameServer"
	@echo "  run-broker      - Run Broker"
	@echo "  run-producer    - Run producer example"
	@echo "  run-consumer    - Run consumer example"
	@echo "  benchmark       - Run sync performance test"
	@echo "  benchmark-async - Run async performance test"
	@echo "  benchmark-oneway - Run oneway performance test"
	@echo "  monitor         - 运行系统监控"
	@echo "  monitor-web     - 运行Web监控界面"
	@echo ""
	@echo "$(BLUE)CI/CD:$(NC)"
	@echo "  ci-test         - CI test pipeline"
	@echo "  coverage-report - Generate coverage report"
	@echo "  deps            - Download dependencies"
	@echo "  dev-setup       - Setup development environment"
	@echo "  dev             - Start development environment"
	@echo ""
	@echo "$(BLUE)Docker:$(NC)"
	@echo "  docker-build    - Build Docker image"
	@echo "  docker-run      - Run Docker container"
	@echo ""
	@echo "$(BLUE)其他:$(NC)"
	@echo "  help            - Show this help message"