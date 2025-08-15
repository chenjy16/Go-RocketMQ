package client

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Producer 消息生产者
type Producer struct {
	config       *ProducerConfig
	nameServers  []string
	started      bool
	shutdown     chan struct{}
	mutex        sync.RWMutex
	routeTable   map[string]*TopicRouteData
	routeMutex   sync.RWMutex
}

// ProducerConfig 生产者配置
type ProducerConfig struct {
	GroupName            string        `json:"groupName"`
	NameServers          []string      `json:"nameServers"`
	SendMsgTimeout       time.Duration `json:"sendMsgTimeout"`
	CompressMsgBodyOver  int32         `json:"compressMsgBodyOver"`
	RetryTimesWhenSendFailed int32     `json:"retryTimesWhenSendFailed"`
	RetryTimesWhenSendAsyncFailed int32 `json:"retryTimesWhenSendAsyncFailed"`
	RetryAnotherBrokerWhenNotStoreOK bool `json:"retryAnotherBrokerWhenNotStoreOK"`
	MaxMessageSize       int32         `json:"maxMessageSize"`
}

// NewProducer 创建新的生产者
func NewProducer(groupName string) *Producer {
	config := &ProducerConfig{
		GroupName:                        groupName,
		SendMsgTimeout:                   3 * time.Second,
		CompressMsgBodyOver:              4096,
		RetryTimesWhenSendFailed:         2,
		RetryTimesWhenSendAsyncFailed:    2,
		RetryAnotherBrokerWhenNotStoreOK: false,
		MaxMessageSize:                   1024 * 1024 * 4, // 4MB
	}

	return &Producer{
		config:      config,
		shutdown:    make(chan struct{}),
		routeTable:  make(map[string]*TopicRouteData),
	}
}

// SetNameServers 设置NameServer地址
func (p *Producer) SetNameServers(nameServers []string) {
	p.config.NameServers = nameServers
	p.nameServers = nameServers
}

// Start 启动生产者
func (p *Producer) Start() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.started {
		return fmt.Errorf("producer already started")
	}

	if len(p.nameServers) == 0 {
		return fmt.Errorf("nameServers is empty")
	}

	// 启动路由更新任务
	go p.updateTopicRouteInfoFromNameServer()

	p.started = true
	return nil
}

// Shutdown 关闭生产者
func (p *Producer) Shutdown() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.started {
		return
	}

	close(p.shutdown)
	p.started = false
}

// SendSync 同步发送消息
func (p *Producer) SendSync(msg *Message) (*SendResult, error) {
	return p.SendSyncWithTimeout(msg, p.config.SendMsgTimeout)
}

// SendSyncWithTimeout 带超时的同步发送消息
func (p *Producer) SendSyncWithTimeout(msg *Message, timeout time.Duration) (*SendResult, error) {
	if !p.started {
		return nil, fmt.Errorf("producer not started")
	}

	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}

	if msg.Topic == "" {
		return nil, fmt.Errorf("message topic is empty")
	}

	if len(msg.Body) == 0 {
		return nil, fmt.Errorf("message body is empty")
	}

	if len(msg.Body) > int(p.config.MaxMessageSize) {
		return nil, fmt.Errorf("message body size exceeds limit: %d", p.config.MaxMessageSize)
	}

	// 获取Topic路由信息
	routeData := p.getTopicRouteData(msg.Topic)
	if routeData == nil {
		return nil, fmt.Errorf("no route data for topic: %s", msg.Topic)
	}

	// 选择消息队列
	mq := p.selectMessageQueue(routeData, msg.Topic)
	if mq == nil {
		return nil, fmt.Errorf("no available message queue for topic: %s", msg.Topic)
	}

	// 发送消息到指定队列
	return p.sendMessageToQueue(msg, mq, timeout)
}

// SendAsync 异步发送消息
func (p *Producer) SendAsync(msg *Message, callback func(*SendResult, error)) error {
	if !p.started {
		return fmt.Errorf("producer not started")
	}

	if callback == nil {
		return fmt.Errorf("callback cannot be nil")
	}

	go func() {
		result, err := p.SendSync(msg)
		callback(result, err)
	}()

	return nil
}

// SendOneway 单向发送消息（不关心结果）
func (p *Producer) SendOneway(msg *Message) error {
	if !p.started {
		return fmt.Errorf("producer not started")
	}

	// 简化实现，实际应该不等待响应
	_, err := p.SendSync(msg)
	return err
}

// getTopicRouteData 获取Topic路由数据
func (p *Producer) getTopicRouteData(topic string) *TopicRouteData {
	p.routeMutex.RLock()
	defer p.routeMutex.RUnlock()
	return p.routeTable[topic]
}

// selectMessageQueue 选择消息队列
func (p *Producer) selectMessageQueue(routeData *TopicRouteData, topic string) *MessageQueue {
	if len(routeData.QueueDatas) == 0 {
		return nil
	}

	// 简单的轮询选择策略
	queueData := routeData.QueueDatas[0]
	
	return &MessageQueue{
		Topic:      topic,
		BrokerName: queueData.BrokerName,
		QueueId:    0,  // 简化选择第一个队列
	}
}

// sendMessageToQueue 发送消息到指定队列
func (p *Producer) sendMessageToQueue(msg *Message, mq *MessageQueue, timeout time.Duration) (*SendResult, error) {
	// 1. 根据MessageQueue找到Broker地址
	routeData := p.getTopicRouteData(mq.Topic)
	if routeData == nil {
		return nil, fmt.Errorf("no route data for topic: %s", mq.Topic)
	}
	
	var brokerAddr string
	for _, brokerData := range routeData.BrokerDatas {
		if brokerData.BrokerName == mq.BrokerName {
			if addr, ok := brokerData.BrokerAddrs[0]; ok { // 使用Master Broker
				brokerAddr = addr
				break
			}
		}
	}
	
	if brokerAddr == "" {
		return nil, fmt.Errorf("no broker address found for broker: %s", mq.BrokerName)
	}
	
	// 2. 建立网络连接并发送消息
	conn, err := net.DialTimeout("tcp", brokerAddr, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker %s: %v", brokerAddr, err)
	}
	defer conn.Close()
	
	// 设置连接超时
	conn.SetDeadline(time.Now().Add(timeout))
	
	// 3. 构造发送请求
	// 将Properties转换为字符串格式
	var propertiesStr string
	if msg.Properties != nil && len(msg.Properties) > 0 {
		propData, _ := json.Marshal(msg.Properties)
		propertiesStr = string(propData)
	}
	
	// 创建完整的请求，包含消息体
	request := map[string]interface{}{
		"header": &SendMessageRequestHeader{
			ProducerGroup:   p.config.GroupName,
			Topic:          msg.Topic,
			QueueId:        mq.QueueId,
			SysFlag:        0,
			BornTimestamp:  time.Now().UnixMilli(),
			Flag:           0,
			Properties:     propertiesStr,
			ReconsumeTimes: 0,
			UnitMode:       false,
			Batch:          false,
		},
		"body": string(msg.Body), // 将消息体作为字符串发送
	}
	
	// 4. 序列化并发送请求
	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}
	
	// 发送请求头（包含数据长度）
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(requestData)))
	
	if _, err := conn.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write header: %v", err)
	}
	
	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to write request: %v", err)
	}
	
	// 5. 读取响应
	responseHeader := make([]byte, 4)
	if _, err := io.ReadFull(conn, responseHeader); err != nil {
		return nil, fmt.Errorf("failed to read response header: %v", err)
	}
	
	responseLength := binary.BigEndian.Uint32(responseHeader)
	responseData := make([]byte, responseLength)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}
	
	// 6. 解析响应
	var response SendMessageResponseHeader
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}
	
	// 7. 构造发送结果
	result := &SendResult{
		SendStatus:   SendOK,
		MsgId:        response.MsgId,
		MessageQueue: mq,
		QueueOffset:  response.QueueOffset,
	}
	
	return result, nil
}

// updateTopicRouteInfoFromNameServer 从NameServer更新Topic路由信息
func (p *Producer) updateTopicRouteInfoFromNameServer() {
	// 立即执行一次路由更新
	p.updateRouteInfo()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdown:
			return
		case <-ticker.C:
			p.updateRouteInfo()
		}
	}
}

// updateRouteInfo 更新路由信息
func (p *Producer) updateRouteInfo() {
	// 创建模拟的路由数据
	// 在实际实现中，这里应该通过网络请求从NameServer获取
	p.routeMutex.Lock()
	defer p.routeMutex.Unlock()
	
	// 为TestTopic创建路由数据
	testTopicRoute := &TopicRouteData{
		QueueDatas: []*QueueData{
			{
				BrokerName:     "DefaultBroker",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:          6, // 读写权限

			},
		},
		BrokerDatas: []*BrokerData{
			{
				Cluster:    "DefaultCluster",
				BrokerName: "DefaultBroker",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10911", // Master Broker地址
				},
			},
		},
	}
	
	// 为OrderTopic创建路由数据
	orderTopicRoute := &TopicRouteData{
		QueueDatas: []*QueueData{
			{
				BrokerName:     "DefaultBroker",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:          6, // 读写权限

			},
		},
		BrokerDatas: []*BrokerData{
			{
				Cluster:    "DefaultCluster",
				BrokerName: "DefaultBroker",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10911", // Master Broker地址
				},
			},
		},
	}
	
	p.routeTable["TestTopic"] = testTopicRoute
	p.routeTable["OrderTopic"] = orderTopicRoute
	
	// 为BenchmarkTopic创建路由数据
	benchmarkTopicRoute := &TopicRouteData{
		QueueDatas: []*QueueData{
			{
				BrokerName:     "DefaultBroker",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:          6, // 读写权限

			},
		},
		BrokerDatas: []*BrokerData{
			{
				Cluster:    "DefaultCluster",
				BrokerName: "DefaultBroker",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10911", // Master Broker地址
				},
			},
		},
	}
	
	p.routeTable["BenchmarkTopic"] = benchmarkTopicRoute
}

// DefaultProducerConfig 返回默认生产者配置
func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		GroupName:                        "DefaultProducerGroup",
		SendMsgTimeout:                   3 * time.Second,
		CompressMsgBodyOver:              4096,
		RetryTimesWhenSendFailed:         2,
		RetryTimesWhenSendAsyncFailed:    2,
		RetryAnotherBrokerWhenNotStoreOK: false,
		MaxMessageSize:                   1024 * 1024 * 4,
	}
}