package client

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
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
	// 延时消息调度器
	delayScheduler *DelayMessageScheduler
	// 批量消息处理器
	batchProcessor *BatchMessageProcessor
	// 消息追踪管理器
	traceManager *TraceManager
	// ACL中间件
	aclMiddleware *ACLMiddleware
	// TODO: 添加remoting组件
	// remotingClient *remoting.RemotingClient
	// routeManager   *remoting.RouteManager
	// heartbeatManager *remoting.HeartbeatManager
}

// DelayMessageScheduler 延时消息调度器
type DelayMessageScheduler struct {
	producer     *Producer
	delayQueue   chan *DelayMessageTask
	running      bool
	mutex        sync.RWMutex
	stopChan     chan struct{}
}

// DelayMessageTask 延时消息任务
type DelayMessageTask struct {
	Message     *Message
	DeliverTime time.Time
	Callback    func(*SendResult, error)
}

// NewDelayMessageScheduler 创建延时消息调度器
func NewDelayMessageScheduler(producer *Producer) *DelayMessageScheduler {
	return &DelayMessageScheduler{
		producer:   producer,
		delayQueue: make(chan *DelayMessageTask, 1000),
		running:    false,
		stopChan:   make(chan struct{}),
	}
}

// Start 启动延时消息调度器
func (dms *DelayMessageScheduler) Start() {
	dms.mutex.Lock()
	defer dms.mutex.Unlock()
	
	if dms.running {
		return
	}
	
	dms.running = true
	go dms.scheduleLoop()
}

// Stop 停止延时消息调度器
func (dms *DelayMessageScheduler) Stop() {
	dms.mutex.Lock()
	defer dms.mutex.Unlock()
	
	if !dms.running {
		return
	}
	
	dms.running = false
	close(dms.stopChan)
}

// ScheduleDelayMessage 调度延时消息
func (dms *DelayMessageScheduler) ScheduleDelayMessage(msg *Message, deliverTime time.Time, callback func(*SendResult, error)) error {
	task := &DelayMessageTask{
		Message:     msg,
		DeliverTime: deliverTime,
		Callback:    callback,
	}
	
	select {
	case dms.delayQueue <- task:
		return nil
	default:
		return fmt.Errorf("delay queue is full")
	}
}

// scheduleLoop 调度循环
func (dms *DelayMessageScheduler) scheduleLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	var pendingTasks []*DelayMessageTask
	
	for {
		select {
		case <-dms.stopChan:
			return
		case task := <-dms.delayQueue:
			pendingTasks = append(pendingTasks, task)
		case <-ticker.C:
			now := time.Now()
			var remainingTasks []*DelayMessageTask
			
			for _, task := range pendingTasks {
				if now.After(task.DeliverTime) || now.Equal(task.DeliverTime) {
					// 时间到了，发送消息
					go dms.sendDelayedMessage(task)
				} else {
					// 时间未到，继续等待
					remainingTasks = append(remainingTasks, task)
				}
			}
			
			pendingTasks = remainingTasks
		}
	}
}

// sendDelayedMessage 发送延时消息
func (dms *DelayMessageScheduler) sendDelayedMessage(task *DelayMessageTask) {
	result, err := dms.producer.SendSync(task.Message)
	if task.Callback != nil {
		task.Callback(result, err)
	}
}

// BatchMessageProcessor 批量消息处理器
type BatchMessageProcessor struct {
	producer     *Producer
	batchQueue   chan *BatchMessageTask
	running      bool
	mutex        sync.RWMutex
	stopChan     chan struct{}
	batchSize    int
	batchTimeout time.Duration
}

// BatchMessageTask 批量消息任务
type BatchMessageTask struct {
	Messages []*Message
	Callback func(*SendResult, error)
}

// NewBatchMessageProcessor 创建批量消息处理器
func NewBatchMessageProcessor(producer *Producer) *BatchMessageProcessor {
	return &BatchMessageProcessor{
		producer:     producer,
		batchQueue:   make(chan *BatchMessageTask, 1000),
		running:      false,
		stopChan:     make(chan struct{}),
		batchSize:    32,  // 默认批量大小
		batchTimeout: 10 * time.Millisecond, // 默认批量超时
	}
}

// Start 启动批量消息处理器
func (bmp *BatchMessageProcessor) Start() {
	bmp.mutex.Lock()
	defer bmp.mutex.Unlock()
	
	if bmp.running {
		return
	}
	
	bmp.running = true
	go bmp.batchProcessLoop()
}

// Stop 停止批量消息处理器
func (bmp *BatchMessageProcessor) Stop() {
	bmp.mutex.Lock()
	defer bmp.mutex.Unlock()
	
	if !bmp.running {
		return
	}
	
	bmp.running = false
	close(bmp.stopChan)
}

// AddBatchTask 添加批量消息任务
func (bmp *BatchMessageProcessor) AddBatchTask(messages []*Message, callback func(*SendResult, error)) error {
	task := &BatchMessageTask{
		Messages: messages,
		Callback: callback,
	}
	
	select {
	case bmp.batchQueue <- task:
		return nil
	default:
		return fmt.Errorf("batch queue is full")
	}
}

// SetBatchSize 设置批量大小
func (bmp *BatchMessageProcessor) SetBatchSize(size int) {
	bmp.mutex.Lock()
	defer bmp.mutex.Unlock()
	bmp.batchSize = size
}

// SetBatchTimeout 设置批量超时
func (bmp *BatchMessageProcessor) SetBatchTimeout(timeout time.Duration) {
	bmp.mutex.Lock()
	defer bmp.mutex.Unlock()
	bmp.batchTimeout = timeout
}

// batchProcessLoop 批量处理循环
func (bmp *BatchMessageProcessor) batchProcessLoop() {
	ticker := time.NewTicker(bmp.batchTimeout)
	defer ticker.Stop()
	
	var pendingTasks []*BatchMessageTask
	
	for {
		select {
		case <-bmp.stopChan:
			// 处理剩余任务
			if len(pendingTasks) > 0 {
				bmp.processBatchTasks(pendingTasks)
			}
			return
		case task := <-bmp.batchQueue:
			pendingTasks = append(pendingTasks, task)
			
			// 检查是否达到批量大小
			if len(pendingTasks) >= bmp.batchSize {
				bmp.processBatchTasks(pendingTasks)
				pendingTasks = nil
				ticker.Reset(bmp.batchTimeout)
			}
		case <-ticker.C:
			// 超时处理
			if len(pendingTasks) > 0 {
				bmp.processBatchTasks(pendingTasks)
				pendingTasks = nil
			}
			ticker.Reset(bmp.batchTimeout)
		}
	}
}

// processBatchTasks 处理批量任务
func (bmp *BatchMessageProcessor) processBatchTasks(tasks []*BatchMessageTask) {
	for _, task := range tasks {
		go bmp.sendBatchMessages(task)
	}
}

// sendBatchMessages 发送批量消息
func (bmp *BatchMessageProcessor) sendBatchMessages(task *BatchMessageTask) {
	result, err := bmp.producer.SendBatchMessages(task.Messages)
	if task.Callback != nil {
		task.Callback(result, err)
	}
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
	// ACL 配置字段
	AccessKey            string        `json:"accessKey"`            // ACL访问密钥
	SecretKey            string        `json:"secretKey"`            // ACL秘密密钥
	SecurityToken        string        `json:"securityToken"`        // ACL安全令牌
	SignatureMethod      string        `json:"signatureMethod"`      // 签名算法，默认HmacSHA1
	EnableACL            bool          `json:"enableACL"`            // 是否启用ACL认证
	ACLConfigPath        string        `json:"aclConfigPath"`        // ACL配置文件路径
	ACLHotReload         bool          `json:"aclHotReload"`         // 是否启用ACL配置热重载
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

	p := &Producer{
		config:      config,
		shutdown:    make(chan struct{}),
		routeTable:  make(map[string]*TopicRouteData),
	}
	
	// 初始化延时消息调度器
	p.delayScheduler = NewDelayMessageScheduler(p)
	
	// 初始化批量消息处理器
	p.batchProcessor = NewBatchMessageProcessor(p)
	
	// 初始化消息追踪管理器（默认不启用）
	p.traceManager = NewTraceManager(nil)
	p.traceManager.SetEnabled(false)
	
	// 初始化ACL中间件（默认不启用）
	p.aclMiddleware = NewACLMiddleware("", "", HmacSHA1, false)
	
	return p
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
	
	// 启动延时消息调度器
	p.delayScheduler.Start()
	
	// 启动批量消息处理器
	p.batchProcessor.Start()
	
	// 启动消息追踪管理器
	if p.traceManager != nil {
		p.traceManager.Start()
	}

	// 初始化和启动ACL中间件
	if err := p.initACLMiddleware(); err != nil {
		return fmt.Errorf("failed to initialize ACL middleware: %v", err)
	}

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

	// 停止延时消息调度器
	p.delayScheduler.Stop()
	
	// 停止批量消息处理器
	p.batchProcessor.Stop()
	
	// 停止消息追踪管理器
	if p.traceManager != nil {
		p.traceManager.Stop()
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

	// 创建生产者追踪上下文
	var traceCtx *TraceContext
	if p.traceManager != nil && p.traceManager.IsEnabled() {
		traceCtx = CreateProduceTraceContext(p.config.GroupName, msg)
		traceCtx.TimeStamp = time.Now().UnixMilli()
	}

	start := time.Now()

	// 获取Topic路由信息
	routeData := p.getTopicRouteData(msg.Topic)
	if routeData == nil {
		if traceCtx != nil {
			traceCtx.Success = false
			traceCtx.CostTime = time.Since(start).Milliseconds()
			p.traceManager.TraceMessage(traceCtx)
		}
		return nil, fmt.Errorf("no route data for topic: %s", msg.Topic)
	}

	// 选择消息队列
	mq := p.selectMessageQueue(routeData, msg.Topic)
	if mq == nil {
		if traceCtx != nil {
			traceCtx.Success = false
			traceCtx.CostTime = time.Since(start).Milliseconds()
			p.traceManager.TraceMessage(traceCtx)
		}
		return nil, fmt.Errorf("no available message queue for topic: %s", msg.Topic)
	}

	// 发送消息到指定队列
	result, err := p.sendMessageToQueue(msg, mq, timeout)

	// 更新追踪信息
	if traceCtx != nil {
		traceCtx.Success = (err == nil)
		traceCtx.CostTime = time.Since(start).Milliseconds()
		if result != nil {
			traceCtx.TraceBeans[0].MsgId = result.MsgId
			traceCtx.RequestId = result.MsgId
		}
		p.traceManager.TraceMessage(traceCtx)
	}

	return result, err
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

// SendDelayMessageAsync 异步发送延时消息
func (p *Producer) SendDelayMessageAsync(msg *Message, delayLevel int32, callback func(*SendResult, error)) error {
	if !p.started {
		return fmt.Errorf("producer not started")
	}
	
	if delayLevel < 1 || delayLevel > 18 {
		return fmt.Errorf("invalid delay level: %d, should be 1-18", delayLevel)
	}
	
	// 计算延时时间
	delayDuration := p.getDelayDuration(delayLevel)
	deliverTime := time.Now().Add(delayDuration)
	
	// 设置延时级别
	msg.SetDelayTimeLevel(delayLevel)
	msg.SetProperty("DELAY_MESSAGE", "true")
	
	// 调度延时消息
	return p.delayScheduler.ScheduleDelayMessage(msg, deliverTime, callback)
}

// SendScheduledMessageAsync 异步发送定时消息
func (p *Producer) SendScheduledMessageAsync(msg *Message, deliverTime time.Time, callback func(*SendResult, error)) error {
	if !p.started {
		return fmt.Errorf("producer not started")
	}
	
	if deliverTime.Before(time.Now()) {
		return fmt.Errorf("deliver time should be in the future")
	}
	
	// 设置开始投递时间
	msg.SetStartDeliverTime(deliverTime.UnixMilli())
	msg.SetProperty("SCHEDULED_MESSAGE", "true")
	
	// 调度延时消息
	return p.delayScheduler.ScheduleDelayMessage(msg, deliverTime, callback)
}

// getDelayDuration 根据延时级别获取延时时间
func (p *Producer) getDelayDuration(delayLevel int32) time.Duration {
	switch delayLevel {
	case 1:
		return 1 * time.Second
	case 2:
		return 5 * time.Second
	case 3:
		return 10 * time.Second
	case 4:
		return 30 * time.Second
	case 5:
		return 1 * time.Minute
	case 6:
		return 2 * time.Minute
	case 7:
		return 3 * time.Minute
	case 8:
		return 4 * time.Minute
	case 9:
		return 5 * time.Minute
	case 10:
		return 6 * time.Minute
	case 11:
		return 7 * time.Minute
	case 12:
		return 8 * time.Minute
	case 13:
		return 9 * time.Minute
	case 14:
		return 10 * time.Minute
	case 15:
		return 20 * time.Minute
	case 16:
		return 30 * time.Minute
	case 17:
		return 1 * time.Hour
	case 18:
		return 2 * time.Hour
	default:
		return 1 * time.Second
	}
}

// SendBatchMessagesAsync 异步发送批量消息
func (p *Producer) SendBatchMessagesAsync(messages []*Message, callback func(*SendResult, error)) error {
	if !p.started {
		return fmt.Errorf("producer not started")
	}
	
	if len(messages) == 0 {
		return fmt.Errorf("messages list is empty")
	}
	
	// 验证所有消息都属于同一个Topic
	topic := messages[0].Topic
	for _, msg := range messages {
		if msg.Topic != topic {
			return fmt.Errorf("all messages in batch must have the same topic")
		}
	}
	
	// 添加到批量处理器
	return p.batchProcessor.AddBatchTask(messages, callback)
}

// SetBatchSize 设置批量大小
func (p *Producer) SetBatchSize(size int) {
	p.batchProcessor.SetBatchSize(size)
}

// SetBatchTimeout 设置批量超时
func (p *Producer) SetBatchTimeout(timeout time.Duration) {
	p.batchProcessor.SetBatchTimeout(timeout)
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
	// TODO: 使用remoting组件实现真实Broker交互
	// 当前保持原有实现，待remoting组件集成完成后替换
	
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
	
	// 创建请求头
	header := &SendMessageRequestHeader{
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
	}

	// 添加ACL认证信息
	if p.IsACLEnabled() {
		authHeaders, err := p.aclMiddleware.GenerateAuthHeaders(msg.Topic, p.config.GroupName, "")
		if err != nil {
			return nil, fmt.Errorf("failed to generate ACL auth headers: %v", err)
		}
		
		if accessKey, ok := authHeaders["AccessKey"]; ok {
			header.AccessKey = accessKey
		}
		if signature, ok := authHeaders["Signature"]; ok {
			header.Signature = signature
		}
		if timestamp, ok := authHeaders["Timestamp"]; ok {
			if ts, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
				header.Timestamp = ts
			}
		}
		if signatureMethod, ok := authHeaders["SignatureMethod"]; ok {
			header.SignatureMethod = signatureMethod
		}
		if securityToken, ok := authHeaders["SecurityToken"]; ok {
			header.SecurityToken = securityToken
		}
	}

	// 创建完整的请求，包含消息体
	request := map[string]interface{}{
		"header": header,
		"body": string(msg.Body), // 将消息体作为字符串发送
	}
	
	// 4. 序列化并发送请求
	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}
	
	// 发送请求头（包含数据长度）
	headerBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(headerBytes, uint32(len(requestData)))
	
	if _, err := conn.Write(headerBytes); err != nil {
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

// EnableTrace 启用消息追踪
func (p *Producer) EnableTrace(nameServerAddr string, traceTopic string) error {
	if p.traceManager == nil {
		return fmt.Errorf("trace manager not initialized")
	}

	// 创建追踪分发器
	dispatcher := NewDefaultTraceDispatcher(nameServerAddr, traceTopic)
	p.traceManager = NewTraceManager(dispatcher)
	p.traceManager.SetEnabled(true)

	// 如果生产者已启动，启动追踪管理器
	if p.started {
		return p.traceManager.Start()
	}

	return nil
}

// DisableTrace 禁用消息追踪
func (p *Producer) DisableTrace() {
	if p.traceManager != nil {
		p.traceManager.SetEnabled(false)
	}
}

// AddTraceHook 添加追踪钩子
func (p *Producer) AddTraceHook(hook TraceHook) {
	if p.traceManager != nil {
		p.traceManager.AddHook(hook)
	}
}

// RemoveTraceHook 移除追踪钩子
func (p *Producer) RemoveTraceHook(hookName string) {
	if p.traceManager != nil {
		p.traceManager.RemoveHook(hookName)
	}
}

// initACLMiddleware 初始化ACL中间件
func (p *Producer) initACLMiddleware() error {
	if !p.config.EnableACL {
		return nil
	}

	// 如果配置了ACL配置文件路径，从文件加载
	if p.config.ACLConfigPath != "" {
		middleware, err := NewACLMiddlewareFromConfig(p.config.ACLConfigPath, p.config.ACLHotReload)
		if err != nil {
			return err
		}
		p.aclMiddleware = middleware
	} else {
		// 从配置字段创建ACL中间件
		if err := ValidateACLConfig(p.config.AccessKey, p.config.SecretKey); err != nil {
			return err
		}
		
		signatureMethod := ACLSignatureMethod(p.config.SignatureMethod)
		if signatureMethod == "" {
			signatureMethod = HmacSHA1
		}
		
		p.aclMiddleware = NewACLMiddleware(p.config.AccessKey, p.config.SecretKey, signatureMethod, true)
	}

	return nil
}

// SetACLConfig 设置ACL配置
func (p *Producer) SetACLConfig(accessKey, secretKey string, signatureMethod ACLSignatureMethod) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if err := ValidateACLConfig(accessKey, secretKey); err != nil {
		return err
	}

	p.config.AccessKey = accessKey
	p.config.SecretKey = secretKey
	p.config.SignatureMethod = string(signatureMethod)
	p.config.EnableACL = true

	// 重新初始化ACL中间件
	if p.started {
		p.aclMiddleware = NewACLMiddleware(accessKey, secretKey, signatureMethod, true)
	}

	return nil
}

// EnableACL 启用ACL认证
func (p *Producer) EnableACL(accessKey, secretKey string) error {
	return p.SetACLConfig(accessKey, secretKey, HmacSHA1)
}

// DisableACL 禁用ACL认证
func (p *Producer) DisableACL() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.config.EnableACL = false
	if p.aclMiddleware != nil {
		p.aclMiddleware.SetEnabled(false)
	}
}

// IsACLEnabled 检查ACL是否启用
func (p *Producer) IsACLEnabled() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.config.EnableACL && p.aclMiddleware != nil && p.aclMiddleware.IsEnabled()
}