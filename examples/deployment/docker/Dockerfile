# Go-RocketMQ Docker镜像构建文件
# 多阶段构建，优化镜像大小和安全性

# 构建阶段
FROM golang:1.21-alpine AS builder

# 设置工作目录
WORKDIR /build

# 安装构建依赖
RUN apk add --no-cache git ca-certificates tzdata

# 复制go mod文件
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 构建应用程序
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o nameserver ./cmd/nameserver/main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o broker ./cmd/broker/main.go

# 构建示例程序
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o producer-example ./examples/basic/producer/main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o consumer-example ./examples/basic/consumer/main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o admin-tool ./examples/tools/admin/main.go

# 运行阶段
FROM alpine:3.18

# 安装运行时依赖
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    netcat-openbsd \
    curl \
    && rm -rf /var/cache/apk/*

# 创建非root用户
RUN addgroup -g 1000 rocketmq && \
    adduser -D -s /bin/sh -u 1000 -G rocketmq rocketmq

# 创建必要的目录
RUN mkdir -p /app /data/rocketmq /var/log/rocketmq /etc/rocketmq && \
    chown -R rocketmq:rocketmq /app /data/rocketmq /var/log/rocketmq /etc/rocketmq

# 复制构建的二进制文件
COPY --from=builder /build/nameserver /app/
COPY --from=builder /build/broker /app/
COPY --from=builder /build/producer-example /app/examples/
COPY --from=builder /build/consumer-example /app/examples/
COPY --from=builder /build/admin-tool /app/examples/

# 复制配置文件模板
COPY --from=builder /build/examples/config/ /etc/rocketmq/templates/

# 复制启动脚本
COPY examples/deployment/docker/scripts/ /app/scripts/
RUN chmod +x /app/scripts/*.sh

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 设置环境变量
ENV ROCKETMQ_HOME=/app
ENV ROCKETMQ_DATA_DIR=/data/rocketmq
ENV ROCKETMQ_LOG_DIR=/var/log/rocketmq
ENV ROCKETMQ_CONFIG_DIR=/etc/rocketmq

# 暴露端口
# NameServer端口
EXPOSE 9876
# Broker端口
EXPOSE 10911 10912
# 监控端口
EXPOSE 8080

# 切换到非root用户
USER rocketmq

# 设置工作目录
WORKDIR /app

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD /app/scripts/healthcheck.sh

# 默认启动NameServer
CMD ["/app/scripts/start.sh", "nameserver"]

# 添加标签
LABEL maintainer="Go-RocketMQ Team" \
      version="1.0.0" \
      description="Go-RocketMQ Docker Image" \
      org.opencontainers.image.title="Go-RocketMQ" \
      org.opencontainers.image.description="A high-performance message queue implementation in Go" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.vendor="Go-RocketMQ" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/example/go-rocketmq"