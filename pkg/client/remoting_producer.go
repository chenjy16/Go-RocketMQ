package client

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// RemotingClient 简化的远程通信客户端
type RemotingClient struct {
	connections map[string]net.Conn
	mutex       sync.RWMutex
}

// RouteManager 简化的路由管理器
type RouteManager struct {
	nameServerAddrs []string
	topicRoutes     map[string]*TopicRouteData
	mutex           sync.RWMutex
}

// RemotingProducer 基于remoting组件的生产者实现
type RemotingProducer struct {
	config         *ProducerConfig
	nameServers    []string
	started        bool
	remotingClient *RemotingClient
	routeManager   *RouteManager
	aclMiddleware  *ACLMiddleware
}

// NewRemotingProducer 创建基于remoting的生产者
func NewRemotingProducer(groupName string) *RemotingProducer {
	config := DefaultProducerConfig()
	config.GroupName = groupName
	
	return &RemotingProducer{
		config:      config,
		nameServers: []string{"127.0.0.1:9876"},
		started:     false,
	}
}

// Start 启动生产者
func (rp *RemotingProducer) Start() error {
	if rp.started {
		return fmt.Errorf("producer already started")
	}
	
	// 1. 创建RemotingClient
	rp.remotingClient = &RemotingClient{
		connections: make(map[string]net.Conn),
	}
	
	// 2. 创建RouteManager
	rp.routeManager = &RouteManager{
		nameServerAddrs: rp.nameServers,
		topicRoutes:     make(map[string]*TopicRouteData),
	}
	
	// 3. 启动路由更新
	go rp.updateRouteInfo()
	
	rp.started = true
	return nil
}

// updateRouteInfo 更新路由信息
func (rp *RemotingProducer) updateRouteInfo() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for rp.started {
		select {
		case <-ticker.C:
			// 定期更新路由信息
			rp.routeManager.mutex.Lock()
			for topic := range rp.routeManager.topicRoutes {
				rp.updateTopicRoute(topic)
			}
			rp.routeManager.mutex.Unlock()
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// updateTopicRoute 更新指定Topic的路由信息
func (rp *RemotingProducer) updateTopicRoute(topic string) {
	// 模拟从NameServer获取路由信息
	routeData := &TopicRouteData{
		OrderTopicConf: "",
		QueueDatas: []*QueueData{
			{
				BrokerName:     "DefaultBroker",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
				TopicSynFlag:   0,
			},
		},
		BrokerDatas: []*BrokerData{
			{
				Cluster:    "DefaultCluster",
				BrokerName: "DefaultBroker",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10911",
				},
			},
		},
	}
	
	rp.routeManager.topicRoutes[topic] = routeData
}

// Shutdown 关闭生产者
func (rp *RemotingProducer) Shutdown() {
	if !rp.started {
		return
	}
	
	// 1. 停止路由更新
	rp.started = false
	
	// 2. 关闭连接
	if rp.remotingClient != nil {
		rp.remotingClient.mutex.Lock()
		for _, conn := range rp.remotingClient.connections {
			if conn != nil {
				conn.Close()
			}
		}
		rp.remotingClient.connections = make(map[string]net.Conn)
		rp.remotingClient.mutex.Unlock()
	}
}

// SendSync 同步发送消息
func (rp *RemotingProducer) SendSync(msg *Message) (*SendResult, error) {
	return rp.SendSyncWithTimeout(msg, 3*time.Second)
}

// SendSyncWithTimeout 带超时的同步发送
func (rp *RemotingProducer) SendSyncWithTimeout(msg *Message, timeout time.Duration) (*SendResult, error) {
	if !rp.started {
		return nil, fmt.Errorf("producer not started")
	}
	
	// 1. 获取Topic路由信息
	routeData, err := rp.getTopicRoute(msg.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic route: %v", err)
	}
	
	// 2. 选择MessageQueue
	mq, err := rp.selectMessageQueue(routeData)
	if err != nil {
		return nil, fmt.Errorf("failed to select message queue: %v", err)
	}
	
	// 3. 获取Broker地址
	brokerAddr, err := rp.getBrokerAddr(routeData, mq.BrokerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get broker address: %v", err)
	}
	
	// 4. 发送消息到Broker
	return rp.sendMessageToBroker(brokerAddr, mq, msg, timeout)
}

// SendAsync 异步发送消息
func (rp *RemotingProducer) SendAsync(msg *Message, callback func(*SendResult, error)) error {
	if !rp.started {
		return fmt.Errorf("producer not started")
	}
	
	// TODO: 使用remoting组件实现异步发送
	go func() {
		result, err := rp.SendSync(msg)
		callback(result, err)
	}()
	
	return nil
}

// SendOneway 单向发送消息
func (rp *RemotingProducer) SendOneway(msg *Message) error {
	if !rp.started {
		return fmt.Errorf("producer not started")
	}
	
	// TODO: 使用remoting组件实现单向发送
	// 单向发送不需要等待响应
	
	return nil
}

// SetNameServers 设置NameServer地址
func (rp *RemotingProducer) SetNameServers(nameServers []string) {
	rp.nameServers = nameServers
}

// IsStarted 检查是否已启动
func (rp *RemotingProducer) IsStarted() bool {
	return rp.started
}

// SetACLMiddleware 设置ACL中间件
func (rp *RemotingProducer) SetACLMiddleware(aclMiddleware *ACLMiddleware) {
	rp.aclMiddleware = aclMiddleware
}

// GetACLMiddleware 获取ACL中间件
func (rp *RemotingProducer) GetACLMiddleware() *ACLMiddleware {
	return rp.aclMiddleware
}

// getTopicRoute 获取Topic路由信息
func (rp *RemotingProducer) getTopicRoute(topic string) (*TopicRouteData, error) {
	rp.routeManager.mutex.RLock()
	routeData, exists := rp.routeManager.topicRoutes[topic]
	rp.routeManager.mutex.RUnlock()
	
	if !exists {
		// 如果路由不存在，先更新路由信息
		rp.updateTopicRoute(topic)
		rp.routeManager.mutex.RLock()
		routeData, exists = rp.routeManager.topicRoutes[topic]
		rp.routeManager.mutex.RUnlock()
		
		if !exists {
			return nil, fmt.Errorf("topic route not found: %s", topic)
		}
	}
	
	return routeData, nil
}

// selectMessageQueue 选择消息队列
func (rp *RemotingProducer) selectMessageQueue(routeData *TopicRouteData) (*MessageQueue, error) {
	if len(routeData.QueueDatas) == 0 {
		return nil, fmt.Errorf("no queue data available")
	}
	
	// 简单选择第一个可写队列
	queueData := routeData.QueueDatas[0]
	return &MessageQueue{
		Topic:      "", // 将在调用处设置
		BrokerName: queueData.BrokerName,
		QueueId:    0, // 选择第一个队列
	}, nil
}

// getBrokerAddr 获取Broker地址
func (rp *RemotingProducer) getBrokerAddr(routeData *TopicRouteData, brokerName string) (string, error) {
	for _, brokerData := range routeData.BrokerDatas {
		if brokerData.BrokerName == brokerName {
			// 选择主Broker (ID=0)
			if addr, exists := brokerData.BrokerAddrs[0]; exists {
				return addr, nil
			}
		}
	}
	return "", fmt.Errorf("broker address not found: %s", brokerName)
}

// sendMessageToBroker 发送消息到Broker
func (rp *RemotingProducer) sendMessageToBroker(brokerAddr string, mq *MessageQueue, msg *Message, timeout time.Duration) (*SendResult, error) {
	// 建立连接
	conn, err := rp.getConnection(brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %v", err)
	}
	
	// 构建发送请求
	request := rp.buildSendRequest(mq, msg)
	
	// 发送请求并接收响应
	response, err := rp.sendRequest(conn, request, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	
	// 解析响应
	return rp.parseSendResponse(response, mq)
}

// getConnection 获取到Broker的连接
func (rp *RemotingProducer) getConnection(brokerAddr string) (net.Conn, error) {
	rp.remotingClient.mutex.RLock()
	conn, exists := rp.remotingClient.connections[brokerAddr]
	rp.remotingClient.mutex.RUnlock()
	
	if exists && conn != nil {
		return conn, nil
	}
	
	// 创建新连接
	newConn, err := net.DialTimeout("tcp", brokerAddr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	
	rp.remotingClient.mutex.Lock()
	rp.remotingClient.connections[brokerAddr] = newConn
	rp.remotingClient.mutex.Unlock()
	
	return newConn, nil
}

// buildSendRequest 构建发送请求
func (rp *RemotingProducer) buildSendRequest(mq *MessageQueue, msg *Message) *RemotingCommand {
	header := &SendMessageRequestHeader{
		ProducerGroup:         rp.config.GroupName,
		Topic:                 msg.Topic,
		DefaultTopic:          "TBW102",
		DefaultTopicQueueNums: 4,
		QueueId:               int32(mq.QueueId),
		SysFlag:               0,
		BornTimestamp:         time.Now().UnixNano() / 1000000,
		Flag:                  0,
		Properties:            rp.encodeProperties(msg),
		ReconsumeTimes:        0,
		UnitMode:              false,
		Batch:                 false,
	}
	
	cmd := CreateRequestCommand(SEND_MESSAGE, header)
	
	// 添加ACL认证头
	if rp.aclMiddleware != nil && rp.aclMiddleware.IsEnabled() {
		authHeaders, err := rp.aclMiddleware.GenerateAuthHeaders(msg.Topic, rp.config.GroupName, "")
		if err == nil {
			if cmd.ExtFields == nil {
				cmd.ExtFields = make(map[string]string)
			}
			for k, v := range authHeaders {
				cmd.ExtFields[k] = v
			}
		}
	}
	
	return cmd
}

// encodeProperties 编码消息属性
func (rp *RemotingProducer) encodeProperties(msg *Message) string {
	properties := ""
	if msg.Tags != "" {
		properties += "TAGS" + "\u0001" + msg.Tags + "\u0002"
	}
	if msg.Keys != "" {
		properties += "KEYS" + "\u0001" + msg.Keys + "\u0002"
	}
	for k, v := range msg.Properties {
		properties += k + "\u0001" + v + "\u0002"
	}
	return properties
}

// sendRequest 发送请求
func (rp *RemotingProducer) sendRequest(conn net.Conn, request *RemotingCommand, timeout time.Duration) (*RemotingCommand, error) {
	// 设置超时
	conn.SetWriteDeadline(time.Now().Add(timeout))
	conn.SetReadDeadline(time.Now().Add(timeout))
	
	// 模拟发送和接收
	// 这里应该实现真实的协议编码和解码
	// 为了简化，直接返回模拟响应
	return &RemotingCommand{
		Code:   int32(SUCCESS),
		Opaque: request.Opaque,
		ExtFields: map[string]string{
			"msgId":       fmt.Sprintf("msg_%d", time.Now().UnixNano()),
			"queueId":     "0",
			"queueOffset": fmt.Sprintf("%d", time.Now().UnixNano()),
		},
	}, nil
}

// parseSendResponse 解析发送响应
func (rp *RemotingProducer) parseSendResponse(response *RemotingCommand, mq *MessageQueue) (*SendResult, error) {
	if response.Code != int32(SUCCESS) {
		return nil, fmt.Errorf("send failed with code: %d, remark: %s", response.Code, response.Remark)
	}
	
	msgId := response.ExtFields["msgId"]
	queueOffset := response.ExtFields["queueOffset"]
	
	var offset int64
	if queueOffset != "" {
		if parsed, err := fmt.Sscanf(queueOffset, "%d", &offset); err != nil || parsed != 1 {
			offset = 0
		}
	}
	
	return &SendResult{
		SendStatus:   SendOK,
		MsgId:        msgId,
		MessageQueue: mq,
		QueueOffset:  offset,
	}, nil
}