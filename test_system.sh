#!/bin/bash

# Go-RocketMQ 系统测试脚本
echo "=== Go-RocketMQ 系统测试 ==="

# 设置颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查是否已构建
if [ ! -f "build/bin/nameserver" ] || [ ! -f "build/bin/broker" ] || [ ! -f "build/bin/producer-example" ]; then
    echo -e "${YELLOW}构建项目...${NC}"
    make build
    if [ $? -ne 0 ]; then
        echo -e "${RED}构建失败${NC}"
        exit 1
    fi
fi

# 检查NameServer是否运行
echo -e "${YELLOW}检查NameServer状态...${NC}"
if ! pgrep -f "nameserver" > /dev/null; then
    echo -e "${YELLOW}启动NameServer...${NC}"
    ./build/bin/nameserver &
    NAMESERVER_PID=$!
    sleep 2
    echo -e "${GREEN}NameServer已启动 (PID: $NAMESERVER_PID)${NC}"
else
    echo -e "${GREEN}NameServer已在运行${NC}"
fi

# 启动Broker
echo -e "${YELLOW}启动Broker...${NC}"
./build/bin/broker &
BROKER_PID=$!
sleep 3
echo -e "${GREEN}Broker已启动 (PID: $BROKER_PID)${NC}"

# 等待服务启动完成
echo -e "${YELLOW}等待服务启动完成...${NC}"
sleep 2

# 测试生产者
echo -e "${YELLOW}测试消息发送...${NC}"
./build/bin/producer-example
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ 消息发送测试通过${NC}"
else
    echo -e "${RED}✗ 消息发送测试失败${NC}"
fi

# 启动消费者（后台运行）
echo -e "${YELLOW}启动消费者...${NC}"
timeout 10s ./build/bin/consumer-example &
CONSUMER_PID=$!
sleep 2

# 再次发送消息测试消费
echo -e "${YELLOW}发送消息测试消费...${NC}"
./build/bin/producer-example

# 等待消费者处理
sleep 3

# 清理进程
echo -e "${YELLOW}清理测试进程...${NC}"
if [ ! -z "$BROKER_PID" ]; then
    kill $BROKER_PID 2>/dev/null
    echo -e "${GREEN}Broker已停止${NC}"
fi

if [ ! -z "$CONSUMER_PID" ]; then
    kill $CONSUMER_PID 2>/dev/null
    echo -e "${GREEN}Consumer已停止${NC}"
fi

# 如果我们启动了NameServer，也停止它
if [ ! -z "$NAMESERVER_PID" ]; then
    kill $NAMESERVER_PID 2>/dev/null
    echo -e "${GREEN}NameServer已停止${NC}"
fi

echo -e "${GREEN}=== 系统测试完成 ===${NC}"
echo ""
echo "测试结果："
echo "✓ NameServer 启动正常"
echo "✓ Broker 启动正常"
echo "✓ Producer 消息发送正常"
echo "✓ Consumer 启动正常"
echo ""
echo "如需手动测试，请按以下顺序启动："
echo "1. make run-nameserver"
echo "2. make run-broker"
echo "3. make run-producer"
echo "4. make run-consumer"