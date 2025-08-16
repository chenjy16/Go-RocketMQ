package remoting

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"go-rocketmq/pkg/protocol"
)

// ProcessorContext 处理器上下文
type ProcessorContext struct {
	Ctx        context.Context
	Connection *Connection
	Request    *protocol.RemotingCommand
	StartTime  time.Time
}

// ProcessorResult 处理器结果
type ProcessorResult struct {
	Response *protocol.RemotingCommand
	Error    error
}

// ProcessorInterface 处理器接口（避免与remoting_server.go中的RequestProcessor冲突）
type ProcessorInterface interface {
	ProcessRequest(ctx *ProcessorContext) *ProcessorResult
	GetRequestCode() protocol.RequestCode
}

// ProtocolProcessor 协议处理器
type ProtocolProcessor struct {
	processors map[protocol.RequestCode]ProcessorInterface
	mutex      sync.RWMutex
	metrics    *ProcessorMetrics
}

// ProcessorMetrics 处理器指标
type ProcessorMetrics struct {
	TotalRequests    int64
	SuccessRequests  int64
	FailedRequests   int64
	AverageLatency   time.Duration
	MaxLatency       time.Duration
	MinLatency       time.Duration
	mutex            sync.RWMutex
}

// NewProtocolProcessor 创建协议处理器
func NewProtocolProcessor() *ProtocolProcessor {
	return &ProtocolProcessor{
		processors: make(map[protocol.RequestCode]ProcessorInterface),
		metrics:    &ProcessorMetrics{
			MinLatency: time.Hour, // 初始化为一个大值
		},
	}
}

// RegisterProcessor 注册请求处理器
func (pp *ProtocolProcessor) RegisterProcessor(processor ProcessorInterface) {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()
	
	requestCode := processor.GetRequestCode()
	pp.processors[requestCode] = processor
	log.Printf("Registered processor for request code: %d", requestCode)
}

// UnregisterProcessor 注销请求处理器
func (pp *ProtocolProcessor) UnregisterProcessor(requestCode protocol.RequestCode) {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()
	
	delete(pp.processors, requestCode)
	log.Printf("Unregistered processor for request code: %d", requestCode)
}

// ProcessRequest 处理请求
func (pp *ProtocolProcessor) ProcessRequest(ctx context.Context, conn *Connection, request *protocol.RemotingCommand) *protocol.RemotingCommand {
	startTime := time.Now()
	
	// 更新指标
	defer func() {
		latency := time.Since(startTime)
		pp.updateMetrics(latency)
	}()
	
	// 查找处理器
	pp.mutex.RLock()
	processor, exists := pp.processors[request.Code]
	pp.mutex.RUnlock()
	
	if !exists {
		log.Printf("No processor found for request code: %d", request.Code)
		return pp.createErrorResponse(request, protocol.SystemError, "No processor found")
	}
	
	// 创建处理器上下文
	processorCtx := &ProcessorContext{
		Ctx:        ctx,
		Connection: conn,
		Request:    request,
		StartTime:  startTime,
	}
	
	// 处理请求
	result := processor.ProcessRequest(processorCtx)
	if result == nil {
		return pp.createErrorResponse(request, protocol.SystemError, "Processor returned nil result")
	}
	
	if result.Error != nil {
		log.Printf("Processor error for request code %d: %v", request.Code, result.Error)
		return pp.createErrorResponse(request, protocol.SystemError, result.Error.Error())
	}
	
	if result.Response == nil {
		return pp.createErrorResponse(request, protocol.SystemError, "Processor returned nil response")
	}
	
	// 设置响应的Opaque
	result.Response.Opaque = request.Opaque
	
	return result.Response
}

// createErrorResponse 创建错误响应
func (pp *ProtocolProcessor) createErrorResponse(request *protocol.RemotingCommand, code protocol.ResponseCode, message string) *protocol.RemotingCommand {
	return protocol.CreateResponseCommand(code, message)
}

// updateMetrics 更新指标
func (pp *ProtocolProcessor) updateMetrics(latency time.Duration) {
	pp.metrics.mutex.Lock()
	defer pp.metrics.mutex.Unlock()
	
	pp.metrics.TotalRequests++
	pp.metrics.SuccessRequests++
	
	// 更新延迟统计
	if latency > pp.metrics.MaxLatency {
		pp.metrics.MaxLatency = latency
	}
	if latency < pp.metrics.MinLatency {
		pp.metrics.MinLatency = latency
	}
	
	// 计算平均延迟（简化实现）
	pp.metrics.AverageLatency = (pp.metrics.AverageLatency*time.Duration(pp.metrics.TotalRequests-1) + latency) / time.Duration(pp.metrics.TotalRequests)
}

// GetMetrics 获取处理器指标
func (pp *ProtocolProcessor) GetMetrics() ProcessorMetrics {
	pp.metrics.mutex.RLock()
	defer pp.metrics.mutex.RUnlock()
	
	return *pp.metrics
}

// GetProcessors 获取所有处理器
func (pp *ProtocolProcessor) GetProcessors() map[protocol.RequestCode]ProcessorInterface {
	pp.mutex.RLock()
	defer pp.mutex.RUnlock()
	
	processors := make(map[protocol.RequestCode]ProcessorInterface)
	for code, processor := range pp.processors {
		processors[code] = processor
	}
	return processors
}

// DefaultSendMessageProcessor 默认发送消息处理器
type DefaultSendMessageProcessor struct {
	messageStore MessageStore
}

// MessageStore 消息存储接口
type MessageStore interface {
	PutMessage(msg *Message) error
	GetMessage(topic string, queueId int32, offset int64) (*MessageExt, error)
}

// NewDefaultSendMessageProcessor 创建默认发送消息处理器
func NewDefaultSendMessageProcessor(messageStore MessageStore) *DefaultSendMessageProcessor {
	return &DefaultSendMessageProcessor{
		messageStore: messageStore,
	}
}

// ProcessRequest 处理发送消息请求
func (p *DefaultSendMessageProcessor) ProcessRequest(ctx *ProcessorContext) *ProcessorResult {
	request := ctx.Request
	
	// 解析请求头
	var header protocol.SendMessageRequestHeader
	if request.ExtFields != nil {
		headerData, _ := json.Marshal(request.ExtFields)
		if err := json.Unmarshal(headerData, &header); err != nil {
			return &ProcessorResult{
				Error: fmt.Errorf("failed to parse send message header: %v", err),
			}
		}
	}
	
	// 解析Properties字符串为map
	properties := make(map[string]string)
	if header.Properties != "" {
		// 简化实现：假设Properties是JSON格式
		json.Unmarshal([]byte(header.Properties), &properties)
	}
	
	// 创建消息
	msg := &Message{
		Topic:      header.Topic,
		Flag:       header.Flag,
		Properties: properties,
		Body:       request.Body,
	}
	
	// 存储消息
	if err := p.messageStore.PutMessage(msg); err != nil {
		return &ProcessorResult{
			Error: fmt.Errorf("failed to store message: %v", err),
		}
	}
	
	// 创建响应
	responseHeader := &protocol.SendMessageResponseHeader{
		MsgId:         fmt.Sprintf("%d_%d", time.Now().UnixNano(), header.QueueId),
		QueueId:       header.QueueId,
		QueueOffset:   0, // 简化实现
		TransactionId: "", // 简化实现
	}
	
	extFields := make(map[string]string)
	headerData, _ := json.Marshal(responseHeader)
	json.Unmarshal(headerData, &extFields)
	
	response := protocol.CreateResponseCommand(protocol.Success, "")
	response.Opaque = request.Opaque
	response.ExtFields = extFields
	
	return &ProcessorResult{
		Response: response,
	}
}

// GetRequestCode 获取请求代码
func (p *DefaultSendMessageProcessor) GetRequestCode() protocol.RequestCode {
	return protocol.SendMessage
}

// DefaultPullMessageProcessor 默认拉取消息处理器
type DefaultPullMessageProcessor struct {
	messageStore MessageStore
}

// NewDefaultPullMessageProcessor 创建默认拉取消息处理器
func NewDefaultPullMessageProcessor(messageStore MessageStore) *DefaultPullMessageProcessor {
	return &DefaultPullMessageProcessor{
		messageStore: messageStore,
	}
}

// ProcessRequest 处理拉取消息请求
func (p *DefaultPullMessageProcessor) ProcessRequest(ctx *ProcessorContext) *ProcessorResult {
	request := ctx.Request
	
	// 解析请求头
	var header protocol.PullMessageRequestHeader
	if request.ExtFields != nil {
		headerData, _ := json.Marshal(request.ExtFields)
		if err := json.Unmarshal(headerData, &header); err != nil {
			return &ProcessorResult{
				Error: fmt.Errorf("failed to parse pull message header: %v", err),
			}
		}
	}
	
	// 拉取消息（简化实现）
	msg, err := p.messageStore.GetMessage(header.Topic, header.QueueId, header.QueueOffset)
	if err != nil {
		return &ProcessorResult{
			Error: fmt.Errorf("failed to get message: %v", err),
		}
	}
	
	// 创建响应
	responseHeader := &protocol.PullMessageResponseHeader{
		SuggestWhichBrokerId: 0,
		NextBeginOffset:      header.QueueOffset + 1,
		MinOffset:            0,
		MaxOffset:            1000, // 简化实现
	}
	
	extFields := make(map[string]string)
	headerData, _ := json.Marshal(responseHeader)
	json.Unmarshal(headerData, &extFields)
	
	var responseBody []byte
	if msg != nil {
		// 编码消息
		codec := NewMessageCodec()
		responseBody, _ = codec.EncodeMessage(msg.Message)
	}
	
	response := protocol.CreateResponseCommand(protocol.Success, "")
	response.Opaque = request.Opaque
	response.ExtFields = extFields
	response.Body = responseBody
	
	return &ProcessorResult{
		Response: response,
	}
}

// GetRequestCode 获取请求代码
func (p *DefaultPullMessageProcessor) GetRequestCode() protocol.RequestCode {
	return protocol.PullMessage
}

// DefaultHeartbeatProcessor 默认心跳处理器
type DefaultHeartbeatProcessor struct {
	heartbeatManager *HeartbeatManager
}

// NewDefaultHeartbeatProcessor 创建默认心跳处理器
func NewDefaultHeartbeatProcessor(heartbeatManager *HeartbeatManager) *DefaultHeartbeatProcessor {
	return &DefaultHeartbeatProcessor{
		heartbeatManager: heartbeatManager,
	}
}

// ProcessRequest 处理心跳请求
func (p *DefaultHeartbeatProcessor) ProcessRequest(ctx *ProcessorContext) *ProcessorResult {
	request := ctx.Request
	
	// 解析心跳数据
	var heartbeatData HeartbeatData
	if request.Body != nil {
		if err := json.Unmarshal(request.Body, &heartbeatData); err != nil {
			return &ProcessorResult{
				Error: fmt.Errorf("failed to parse heartbeat data: %v", err),
			}
		}
	}
	
	// 处理心跳
	if p.heartbeatManager != nil {
		// 更新客户端信息（简化实现）
		log.Printf("Received heartbeat from client: %s", heartbeatData.ClientID)
	}
	
	// 创建响应
	response := protocol.CreateResponseCommand(protocol.Success, "")
	response.Opaque = request.Opaque
	
	return &ProcessorResult{
		Response: response,
	}
}

// GetRequestCode 获取请求代码
func (p *DefaultHeartbeatProcessor) GetRequestCode() protocol.RequestCode {
	return protocol.SendMessage // 暂时使用已存在的RequestCode，实际应该定义HeartBeat
}

// DefaultQueryRouteProcessor 默认查询路由处理器
type DefaultQueryRouteProcessor struct {
	routeManager *RouteManager
}

// NewDefaultQueryRouteProcessor 创建默认查询路由处理器
func NewDefaultQueryRouteProcessor(routeManager *RouteManager) *DefaultQueryRouteProcessor {
	return &DefaultQueryRouteProcessor{
		routeManager: routeManager,
	}
}

// ProcessRequest 处理查询路由请求
func (p *DefaultQueryRouteProcessor) ProcessRequest(ctx *ProcessorContext) *ProcessorResult {
	request := ctx.Request
	
	// 从ExtFields中获取Topic
	topic := ""
	if request.ExtFields != nil {
		topic = request.ExtFields["topic"]
	}
	
	if topic == "" {
		return &ProcessorResult{
			Error: fmt.Errorf("topic is required for route query"),
		}
	}
	
	// 查询路由信息（简化实现）
	routeData := &protocol.TopicRouteData{
		OrderTopicConf: "",
		QueueDatas: []*protocol.QueueData{
			{
				BrokerName:     "broker-a",
				ReadQueueNums:  4,
				WriteQueueNums: 4,
				Perm:           6,
				TopicSysFlag:   0,
			},
		},
		BrokerDatas: []*protocol.BrokerData{
			{
				Cluster:    "DefaultCluster",
				BrokerName: "broker-a",
				BrokerAddrs: map[int64]string{
					0: "127.0.0.1:10911",
				},
			},
		},
		FilterServerTable: make(map[string][]string),
	}
	
	// 序列化路由数据
	routeBody, err := json.Marshal(routeData)
	if err != nil {
		return &ProcessorResult{
			Error: fmt.Errorf("failed to marshal route data: %v", err),
		}
	}
	
	// 创建响应
	response := protocol.CreateResponseCommand(protocol.Success, "")
	response.Opaque = request.Opaque
	response.Body = routeBody
	
	return &ProcessorResult{
		Response: response,
	}
}

// GetRequestCode 获取请求代码
func (p *DefaultQueryRouteProcessor) GetRequestCode() protocol.RequestCode {
	return protocol.GetRouteInfoByTopic
}