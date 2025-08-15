# Go-RocketMQ Client Makefile

.PHONY: all build test clean fmt vet lint deps example

# 默认目标
all: fmt vet test build

# 构建
build:
	@echo "Building Go-RocketMQ Client..."
	go build -v ./...

# 运行测试
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.out ./...
	@echo "Test coverage:"
	go tool cover -func=coverage.out

# 运行基准测试
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# 格式化代码
fmt:
	@echo "Formatting code..."
	go fmt ./...

# 静态分析
vet:
	@echo "Running go vet..."
	go vet ./...

# 代码检查（需要安装golangci-lint）
lint:
	@echo "Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed. Install it with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

# 安装依赖
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod tidy

# 更新依赖
update-deps:
	@echo "Updating dependencies..."
	go get -u ./...
	go mod tidy

# 生成覆盖率报告
coverage:
	@echo "Generating coverage report..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# 运行示例
example-producer:
	@echo "Running producer example..."
	go run example_test.go -example=producer

example-consumer:
	@echo "Running consumer example..."
	go run example_test.go -example=consumer

# 清理
clean:
	@echo "Cleaning..."
	go clean
	rm -f coverage.out coverage.html

# 检查模块
mod-verify:
	@echo "Verifying module..."
	go mod verify

# 显示模块信息
mod-info:
	@echo "Module information:"
	go list -m all

# 安装开发工具
install-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/fzipp/gocyclo/cmd/gocyclo@latest

# 代码复杂度检查
cyclo:
	@echo "Checking code complexity..."
	@if command -v gocyclo >/dev/null 2>&1; then \
		gocyclo -over 10 .; \
	else \
		echo "gocyclo not installed. Install it with: make install-tools"; \
	fi

# 安全检查（需要安装gosec）
sec:
	@echo "Running security check..."
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
	else \
		echo "gosec not installed. Install it with: go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest"; \
	fi

# 性能分析
profile:
	@echo "Running performance profile..."
	go test -cpuprofile=cpu.prof -memprofile=mem.prof -bench=. ./...
	@echo "Profile files generated: cpu.prof, mem.prof"
	@echo "View with: go tool pprof cpu.prof or go tool pprof mem.prof"

# 生成文档
doc:
	@echo "Generating documentation..."
	godoc -http=:6060 &
	@echo "Documentation server started at http://localhost:6060"
	@echo "Press Ctrl+C to stop"

# 发布检查
release-check: fmt vet lint test
	@echo "Release check completed successfully"

# 帮助信息
help:
	@echo "Available targets:"
	@echo "  all           - Run fmt, vet, test, and build"
	@echo "  build         - Build the project"
	@echo "  test          - Run tests with coverage"
	@echo "  bench         - Run benchmarks"
	@echo "  fmt           - Format code"
	@echo "  vet           - Run go vet"
	@echo "  lint          - Run linter (requires golangci-lint)"
	@echo "  deps          - Install dependencies"
	@echo "  update-deps   - Update dependencies"
	@echo "  coverage      - Generate HTML coverage report"
	@echo "  example-*     - Run examples"
	@echo "  clean         - Clean build artifacts"
	@echo "  mod-verify    - Verify module"
	@echo "  mod-info      - Show module information"
	@echo "  install-tools - Install development tools"
	@echo "  cyclo         - Check code complexity"
	@echo "  sec           - Run security check"
	@echo "  profile       - Run performance profiling"
	@echo "  doc           - Start documentation server"
	@echo "  release-check - Run all checks for release"
	@echo "  help          - Show this help"