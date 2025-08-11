# Go-RocketMQ Makefile

.PHONY: build clean test run-nameserver run-producer run-consumer fmt vet

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
build: build-nameserver build-broker build-examples build-benchmark build-tools
	@echo "所有组件构建完成"

# 构建 NameServer
build-nameserver: $(BIN_DIR)
	$(GOBUILD) -o $(NAMESERVER_BINARY) ./cmd/nameserver

# 构建 Broker
build-broker: $(BIN_DIR)
	$(GOBUILD) -o $(BROKER_BINARY) ./cmd/broker

# 构建示例程序
build-examples:
	$(GOBUILD) -o $(PRODUCER_EXAMPLE) ./examples/producer
	$(GOBUILD) -o $(CONSUMER_EXAMPLE) ./examples/consumer

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

# 帮助信息
help:
	@echo "Available targets:"
	@echo "  build           - Build all components"
	@echo "  build-nameserver - Build NameServer"
	@echo "  build-broker    - Build Broker"
	@echo "  build-examples  - Build example programs"
	@echo "  clean           - Clean build files"
	@echo "  test            - Run tests"
	@echo "  run-nameserver  - Run NameServer"
	@echo "  run-broker      - Run Broker"
	@echo "  run-producer    - Run producer example"
	@echo "  run-consumer    - Run consumer example"
	@echo "  benchmark        - Run sync performance test"
	@echo "  benchmark-async  - Run async performance test"
	@echo "  benchmark-oneway - Run oneway performance test"
	@echo "  monitor          - 运行系统监控"
	@echo "  monitor-web      - 运行Web监控界面"
	@echo "  fmt             - Format code"
	@echo "  vet             - Run go vet"
	@echo "  lint            - Run golangci-lint"
	@echo "  deps            - Download dependencies"
	@echo "  dev-setup       - Setup development environment"
	@echo "  dev             - Start development environment"
	@echo "  docker-build    - Build Docker image"
	@echo "  docker-run      - Run Docker container"
	@echo "  help            - Show this help message"