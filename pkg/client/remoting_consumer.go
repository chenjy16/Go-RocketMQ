package client

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// RemotingConsumer 基于remoting组件的消费者实现
type RemotingConsumer struct {
	config          *ConsumerConfig
	nameServerAddrs []string
	subscriptions   map[string]*Subscription
	mutex           sync.RWMutex
	started         bool
	shutdown        chan struct{}
	wg              sync.WaitGroup
	// remoting组件字段
	remotingClient  *RemotingClient
	routeManager    *RouteManager
	connectionPool  map[string]net.Conn
	connMutex       sync.RWMutex
	aclMiddleware   *ACLMiddleware
}

// NewRemotingConsumer 创建基于remoting的消费者
func NewRemotingConsumer(config *ConsumerConfig) *RemotingConsumer {
	if config == nil {
		config = DefaultConsumerConfig()
	}
	
	return &RemotingConsumer{
		config:          config,
		nameServerAddrs: []string{config.NameServerAddr},
		subscriptions:   make(map[string]*Subscription),
		shutdown:        make(chan struct{}),
		started:         false,
	}
}

// Start 启动消费者
func (rc *RemotingConsumer) Start() error {
	if rc.started {
		return fmt.Errorf("consumer already started")
	}
	
	// 初始化remoting组件
	rc.remotingClient = &RemotingClient{
		connections: make(map[string]net.Conn),
		mutex:       sync.RWMutex{},
	}
	
	rc.routeManager = &RouteManager{
		nameServerAddrs: rc.nameServerAddrs,
		topicRoutes:     make(map[string]*TopicRouteData),
		mutex:           sync.RWMutex{},
	}
	
	rc.connectionPool = make(map[string]net.Conn)
	
	// 启动路由更新
	rc.wg.Add(1)
	go rc.updateRouteInfo()
	
	rc.started = true
	
	// 启动消费循环
	rc.wg.Add(1)
	go rc.consumeLoop()
	
	return nil
}

// Stop 停止消费者
func (rc *RemotingConsumer) Stop() error {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	
	if !rc.started {
		return fmt.Errorf("consumer not started")
	}
	
	// 发送停止信号（防止重复关闭）
	select {
	case <-rc.shutdown:
		// 已经关闭
	default:
		close(rc.shutdown)
	}
	
	// 等待所有goroutine结束
	rc.wg.Wait()
	
	// TODO: 关闭remoting组件
	// 1. 停止心跳
	// 2. 关闭连接池
	// 3. 清理资源
	
	rc.started = false
	return nil
}

// Subscribe 订阅Topic
func (rc *RemotingConsumer) Subscribe(topic, subExpression string, listener MessageListener) error {
	return rc.SubscribeWithFilter(topic, subExpression, listener, nil)
}

// SubscribeWithFilter 订阅主题并指定过滤器
func (rc *RemotingConsumer) SubscribeWithFilter(topic, subExpression string, listener MessageListener, filter MessageFilter) error {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	
	if rc.started {
		return fmt.Errorf("consumer already started, cannot subscribe to new topics")
	}
	
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	
	if listener == nil {
		return fmt.Errorf("message listener cannot be nil")
	}
	
	rc.subscriptions[topic] = &Subscription{
		Topic:         topic,
		SubExpression: subExpression,
		Listener:      listener,
		Filter:        filter,
	}
	
	return nil
}

// consumeLoop 消费循环
func (rc *RemotingConsumer) consumeLoop() {
	defer rc.wg.Done()
	
	ticker := time.NewTicker(rc.config.PullInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-rc.shutdown:
			return
		case <-ticker.C:
			rc.pullAndConsumeMessages()
		}
	}
}

// pullAndConsumeMessages 拉取并消费消息
func (rc *RemotingConsumer) pullAndConsumeMessages() {
	rc.mutex.RLock()
	subscriptions := make(map[string]*Subscription)
	for k, v := range rc.subscriptions {
		subscriptions[k] = v
	}
	rc.mutex.RUnlock()
	
	for topic, subscription := range subscriptions {
		rc.pullMessagesForTopic(topic, subscription)
	}
}

// pullMessagesForTopic 为指定Topic拉取消息
func (rc *RemotingConsumer) pullMessagesForTopic(topic string, subscription *Subscription) {
	// 1. 通过RouteManager获取Topic路由信息
	routeData, err := rc.getTopicRoute(topic)
	if err != nil {
		fmt.Printf("获取Topic路由失败: %v\n", err)
		return
	}
	
	// 2. 选择MessageQueue
	mq, err := rc.selectMessageQueue(routeData, topic)
	if err != nil {
		fmt.Printf("选择MessageQueue失败: %v\n", err)
		return
	}
	
	// 3. 获取Broker地址
	brokerAddr, err := rc.getBrokerAddr(routeData, mq.BrokerName)
	if err != nil {
		fmt.Printf("获取Broker地址失败: %v\n", err)
		return
	}
	
	// 4. 从Broker拉取消息
	messages, err := rc.pullMessagesFromBroker(brokerAddr, mq, subscription)
	if err != nil {
		fmt.Printf("从Broker拉取消息失败: %v\n", err)
		return
	}
	
	// 5. 处理拉取到的消息
	if len(messages) > 0 {
		rc.consumeMessages(messages, subscription.Listener)
	}
}

// mockPullMessages 模拟拉取消息
func (rc *RemotingConsumer) mockPullMessages(topic string) []*MessageExt {
	// 模拟返回空消息列表
	return []*MessageExt{}
}

// consumeMessages 消费消息
func (rc *RemotingConsumer) consumeMessages(messages []*MessageExt, listener MessageListener) {
	for _, msg := range messages {
		go func(message *MessageExt) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("消费消息时发生panic: %v\n", r)
				}
			}()
			
			// 调用消息监听器处理消息
			result := listener.ConsumeMessage([]*MessageExt{message})
			
			// TODO: 根据消费结果处理ACK/NACK
			if result == ConsumeSuccess {
				// 消费成功，提交offset
				rc.commitConsumeProgress([]*MessageExt{message})
			} else {
				// 消费失败，重试消息
				rc.retryMessages([]*MessageExt{message})
			}
		}(msg)
	}
}

// commitConsumeProgress 提交消费进度
func (rc *RemotingConsumer) commitConsumeProgress(messages []*MessageExt) error {
	for _, msg := range messages {
		// 获取Topic路由信息
		routeData, err := rc.getTopicRoute(msg.Topic)
		if err != nil {
			fmt.Printf("获取Topic路由失败: %v\n", err)
			continue
		}
		
		// 从路由信息中获取BrokerName
		var brokerName string
		for _, qd := range routeData.QueueDatas {
			if qd.ReadQueueNums > int32(msg.QueueId) {
				brokerName = qd.BrokerName
				break
			}
		}
		if brokerName == "" {
			fmt.Printf("未找到队列对应的Broker: queueId=%d\n", msg.QueueId)
			continue
		}
		
		// 构造消息队列信息
		mq := &MessageQueue{
			Topic:      msg.Topic,
			BrokerName: brokerName,
			QueueId:    msg.QueueId,
		}
		
		// 获取Broker地址
		brokerAddr, err := rc.getBrokerAddr(routeData, mq.BrokerName)
		if err != nil {
			fmt.Printf("获取Broker地址失败: %v\n", err)
			continue
		}
		
		// 提交消费进度
		err = rc.commitOffsetToBroker(brokerAddr, mq, msg.QueueOffset+1)
		if err != nil {
			fmt.Printf("提交消费进度失败: %v\n", err)
		}
	}
	return nil
}

// retryMessages 重试消息
func (rc *RemotingConsumer) retryMessages(messages []*MessageExt) error {
	for _, msg := range messages {
		// 增加重试次数
		msg.ReconsumeTimes++
		
		// 如果重试次数超过最大值，发送到DLQ
		if msg.ReconsumeTimes >= 16 {
			err := rc.sendToDLQ(msg)
			if err != nil {
				fmt.Printf("发送消息到DLQ失败: %v\n", err)
			}
			continue
		}
		
		// 计算延迟时间（指数退避）
		delayLevel := rc.calculateDelayLevel(msg.ReconsumeTimes)
		
		// 发送重试消息
		err := rc.sendRetryMessage(msg, delayLevel)
		if err != nil {
			fmt.Printf("发送重试消息失败: %v\n", err)
		}
	}
	return nil
}

// SetNameServerAddr 设置NameServer地址
func (rc *RemotingConsumer) SetNameServerAddr(addr string) {
	rc.config.NameServerAddr = addr
	rc.nameServerAddrs = []string{addr}
}

// IsStarted 检查是否已启动
func (rc *RemotingConsumer) IsStarted() bool {
	return rc.started
}

// SetACLMiddleware 设置ACL中间件
func (rc *RemotingConsumer) SetACLMiddleware(aclMiddleware *ACLMiddleware) {
	rc.aclMiddleware = aclMiddleware
}

// GetACLMiddleware 获取ACL中间件
func (rc *RemotingConsumer) GetACLMiddleware() *ACLMiddleware {
	return rc.aclMiddleware
}

// GetSubscriptions 获取订阅信息
func (rc *RemotingConsumer) GetSubscriptions() map[string]*Subscription {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()
	
	subscriptions := make(map[string]*Subscription)
	for k, v := range rc.subscriptions {
		subscriptions[k] = v
	}
	return subscriptions
}

// updateRouteInfo 定期更新路由信息
func (rc *RemotingConsumer) updateRouteInfo() {
	defer rc.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-rc.shutdown:
			return
		case <-ticker.C:
			// 更新所有订阅Topic的路由信息
			rc.mutex.RLock()
			for topic := range rc.subscriptions {
				rc.updateTopicRoute(topic)
			}
			rc.mutex.RUnlock()
		}
	}
}

// updateTopicRoute 更新指定Topic的路由信息
func (rc *RemotingConsumer) updateTopicRoute(topic string) {
	// 模拟从NameServer获取路由信息
	routeData := &TopicRouteData{
		QueueDatas: []*QueueData{
			{
				BrokerName:     "broker-a",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
				TopicSynFlag:   0,
			},
		},
		BrokerDatas: []*BrokerData{
			{
				Cluster:     "DefaultCluster",
				BrokerName:  "broker-a",
				BrokerAddrs: map[int64]string{0: "127.0.0.1:10911"},
			},
		},
	}
	
	rc.routeManager.mutex.Lock()
	rc.routeManager.topicRoutes[topic] = routeData
	rc.routeManager.mutex.Unlock()
}

// getTopicRoute 获取Topic路由信息
func (rc *RemotingConsumer) getTopicRoute(topic string) (*TopicRouteData, error) {
	rc.routeManager.mutex.RLock()
	routeData, exists := rc.routeManager.topicRoutes[topic]
	rc.routeManager.mutex.RUnlock()
	
	if !exists {
		// 如果路由不存在，先更新路由信息
		rc.updateTopicRoute(topic)
		rc.routeManager.mutex.RLock()
		routeData, exists = rc.routeManager.topicRoutes[topic]
		rc.routeManager.mutex.RUnlock()
		
		if !exists {
			return nil, fmt.Errorf("topic route not found: %s", topic)
		}
	}
	
	return routeData, nil
}

// selectMessageQueue 选择消息队列
func (rc *RemotingConsumer) selectMessageQueue(routeData *TopicRouteData, topic string) (*MessageQueue, error) {
	if len(routeData.QueueDatas) == 0 {
		return nil, fmt.Errorf("no queue data available")
	}
	
	// 简单选择第一个可读队列
	queueData := routeData.QueueDatas[0]
	return &MessageQueue{
		Topic:      topic,
		BrokerName: queueData.BrokerName,
		QueueId:    0, // 选择第一个队列
	}, nil
}

// getBrokerAddr 获取Broker地址
func (rc *RemotingConsumer) getBrokerAddr(routeData *TopicRouteData, brokerName string) (string, error) {
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

// pullMessagesFromBroker 从Broker拉取消息
func (rc *RemotingConsumer) pullMessagesFromBroker(brokerAddr string, mq *MessageQueue, subscription *Subscription) ([]*MessageExt, error) {
	// 建立连接
	conn, err := rc.getConnection(brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %v", err)
	}
	
	// 构建拉取请求
	request := rc.buildPullRequest(mq, subscription)
	
	// 发送请求并接收响应
	response, err := rc.sendPullRequest(conn, request, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to send pull request: %v", err)
	}
	
	// 解析响应
	return rc.parsePullResponse(response)
}

// getConnection 获取到Broker的连接
func (rc *RemotingConsumer) getConnection(brokerAddr string) (net.Conn, error) {
	rc.connMutex.RLock()
	conn, exists := rc.connectionPool[brokerAddr]
	rc.connMutex.RUnlock()
	
	if exists && conn != nil {
		return conn, nil
	}
	
	// 创建新连接
	newConn, err := net.DialTimeout("tcp", brokerAddr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	
	rc.connMutex.Lock()
	rc.connectionPool[brokerAddr] = newConn
	rc.connMutex.Unlock()
	
	return newConn, nil
}

// buildPullRequest 构建拉取请求
func (rc *RemotingConsumer) buildPullRequest(mq *MessageQueue, subscription *Subscription) *RemotingCommand {
	header := &PullMessageRequestHeader{
		ConsumerGroup:        rc.config.GroupName,
		Topic:                mq.Topic,
		QueueId:              int32(mq.QueueId),
		QueueOffset:          0, // 从头开始拉取
		MaxMsgNums:           32,
		SysFlag:              0,
		CommitOffset:         0,
		SuspendTimeoutMillis: 20000,
		Subscription:         subscription.SubExpression,
		SubVersion:           time.Now().Unix(),
	}
	
	cmd := CreateRequestCommand(PULL_MESSAGE, header)
	
	// 添加ACL认证头
	if rc.aclMiddleware != nil && rc.aclMiddleware.IsEnabled() {
		authHeaders, err := rc.aclMiddleware.GenerateAuthHeaders(mq.Topic, rc.config.GroupName, "")
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

// sendPullRequest 发送拉取请求
func (rc *RemotingConsumer) sendPullRequest(conn net.Conn, request *RemotingCommand, timeout time.Duration) (*RemotingCommand, error) {
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
			"nextBeginOffset": "100",
			"minOffset":       "0",
			"maxOffset":       "1000",
		},
		Body: []byte{}, // 模拟空消息体
	}, nil
}

// parsePullResponse 解析拉取响应
func (rc *RemotingConsumer) parsePullResponse(response *RemotingCommand) ([]*MessageExt, error) {
	if response.Code != int32(SUCCESS) {
		return nil, fmt.Errorf("pull failed with code: %d, remark: %s", response.Code, response.Remark)
	}
	
	// 模拟解析消息
	// 实际实现中需要解析response.Body中的消息数据
	messages := []*MessageExt{}
	
	// 为了演示，创建一个模拟消息
	if len(response.Body) == 0 {
		// 没有消息
		return messages, nil
	}
	
	return messages, nil
}

// commitOffsetToBroker 向Broker提交消费进度
func (rc *RemotingConsumer) commitOffsetToBroker(brokerAddr string, mq *MessageQueue, offset int64) error {
	// 建立连接
	conn, err := rc.getConnection(brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %v", err)
	}
	
	// 构建提交offset请求
	request := rc.buildCommitOffsetRequest(mq, offset)
	
	// 发送请求
	_, err = rc.sendPullRequest(conn, request, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to commit offset: %v", err)
	}
	
	return nil
}

// buildCommitOffsetRequest 构建提交offset请求
func (rc *RemotingConsumer) buildCommitOffsetRequest(mq *MessageQueue, offset int64) *RemotingCommand {
	header := map[string]string{
		"consumerGroup": rc.config.GroupName,
		"topic":         mq.Topic,
		"queueId":       fmt.Sprintf("%d", mq.QueueId),
		"commitOffset":  fmt.Sprintf("%d", offset),
	}
	
	cmd := &RemotingCommand{
		Code:      int32(UPDATE_CONSUMER_OFFSET),
		Opaque:    int32(time.Now().UnixNano()),
		ExtFields: header,
	}
	
	// 添加ACL认证头
	if rc.aclMiddleware != nil && rc.aclMiddleware.IsEnabled() {
		authHeaders, err := rc.aclMiddleware.GenerateAuthHeaders(mq.Topic, rc.config.GroupName, "")
		if err == nil {
			for k, v := range authHeaders {
				cmd.ExtFields[k] = v
			}
		}
	}
	
	return cmd
}

// sendToDLQ 发送消息到死信队列
func (rc *RemotingConsumer) sendToDLQ(msg *MessageExt) error {
	// 构造DLQ Topic名称
	dlqTopic := fmt.Sprintf("%%DLQ%%_%s", rc.config.GroupName)
	
	// 创建DLQ消息
	dlqMsg := &Message{
		Topic: dlqTopic,
		Body:  msg.Body,
		Tags:  msg.Tags,
		Keys:  msg.Keys,
		Properties: map[string]string{
			"ORIGIN_TOPIC":      msg.Topic,
			"ORIGIN_QUEUE_ID":   fmt.Sprintf("%d", msg.QueueId),
			"ORIGIN_MSG_ID":     msg.MsgId,
			"RETRY_TIMES":       fmt.Sprintf("%d", msg.ReconsumeTimes),
			"DLQ_ORIGIN_BROKER": msg.StoreHost,
		},
	}
	
	fmt.Printf("Sending message %s to DLQ topic %s after %d retries\n", msg.MsgId, dlqTopic, msg.ReconsumeTimes)
	
	// 实现真实的DLQ发送逻辑
	return rc.sendMessageToBroker(dlqMsg, dlqTopic)
}

// calculateDelayLevel 计算延迟级别
func (rc *RemotingConsumer) calculateDelayLevel(retryTimes int32) int {
	// RocketMQ延迟级别: 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
	delayLevels := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}
	
	if int(retryTimes) < len(delayLevels) {
		return delayLevels[retryTimes-1]
	}
	return delayLevels[len(delayLevels)-1] // 最大延迟级别
}

// sendRetryMessage 发送重试消息
func (rc *RemotingConsumer) sendRetryMessage(msg *MessageExt, delayLevel int) error {
	// 构造重试Topic名称
	retryTopic := fmt.Sprintf("%%RETRY%%%s", rc.config.GroupName)
	
	// 创建重试消息
	retryMsg := &Message{
		Topic: retryTopic,
		Body:  msg.Body,
		Tags:  msg.Tags,
		Keys:  msg.Keys,
		Properties: map[string]string{
			"ORIGIN_TOPIC":      msg.Topic,
			"ORIGIN_QUEUE_ID":   fmt.Sprintf("%d", msg.QueueId),
			"ORIGIN_MSG_ID":     msg.MsgId,
			"RETRY_TIMES":       fmt.Sprintf("%d", msg.ReconsumeTimes),
			"DELAY_LEVEL":       fmt.Sprintf("%d", delayLevel),
		},
	}
	
	fmt.Printf("Sending retry message: topic=%s, msgId=%s, delayLevel=%d\n", retryTopic, msg.MsgId, delayLevel)
	
	// 实现真实的重试消息发送逻辑
	return rc.sendMessageToBroker(retryMsg, retryTopic)
}

// sendMessageToBroker 发送消息到Broker
func (rc *RemotingConsumer) sendMessageToBroker(msg *Message, topic string) error {
	// 获取Topic路由信息
	routeData, err := rc.getTopicRoute(topic)
	if err != nil {
		return fmt.Errorf("failed to get route for topic %s: %v", topic, err)
	}
	
	// 选择消息队列
	mq, err := rc.selectMessageQueue(routeData, topic)
	if err != nil {
		return fmt.Errorf("failed to select message queue for topic %s: %v", topic, err)
	}
	
	// 获取Broker地址
	brokerAddr, err := rc.getBrokerAddr(routeData, mq.BrokerName)
	if err != nil {
		return fmt.Errorf("failed to get broker address for %s: %v", mq.BrokerName, err)
	}
	
	// 构建发送请求
	request := &RemotingCommand{
		Code:      10, // SEND_MESSAGE
		Language:  "JAVA",
		Version:   317,
		Opaque:    int32(time.Now().UnixNano() % 1000000),
		Flag:      0,
		ExtFields: make(map[string]string),
		Body:      msg.Body,
	}
	
	// 设置扩展字段
	request.ExtFields["topic"] = msg.Topic
	request.ExtFields["queueId"] = fmt.Sprintf("%d", mq.QueueId)
	request.ExtFields["sysFlag"] = "0"
	request.ExtFields["bornTimestamp"] = fmt.Sprintf("%d", time.Now().UnixMilli())
	request.ExtFields["flag"] = "0"
	request.ExtFields["properties"] = rc.encodeProperties(msg.Properties)
	
	// 添加ACL认证头
	if rc.aclMiddleware != nil {
		authHeaders, err := rc.aclMiddleware.GenerateAuthHeaders(msg.Topic, rc.config.GroupName, "")
		if err != nil {
			return fmt.Errorf("failed to generate ACL auth headers: %v", err)
		}
		for k, v := range authHeaders {
			request.ExtFields[k] = v
		}
	}
	
	// 获取连接
	conn, err := rc.getConnection(brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to get connection to broker %s: %v", brokerAddr, err)
	}
	
	// 发送请求
	response, err := rc.sendPullRequest(conn, request, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to send request to broker: %v", err)
	}
	
	// 检查响应状态
	if response.Code != 0 {
		return fmt.Errorf("broker returned error code %d: %s", response.Code, response.Remark)
	}
	
	fmt.Printf("Successfully sent message to topic %s\n", topic)
	return nil
}

// encodeProperties 编码消息属性
func (rc *RemotingConsumer) encodeProperties(properties map[string]string) string {
	if len(properties) == 0 {
		return ""
	}
	
	var parts []string
	for k, v := range properties {
		parts = append(parts, fmt.Sprintf("%s%c%s", k, 1, v))
	}
	// 使用字符2作为分隔符连接所有属性
	result := ""
	for i, part := range parts {
		if i > 0 {
			result += string(rune(2))
		}
		result += part
	}
	return result
}