package client

import (
	"fmt"
	"time"
)

// Message 消息结构体
type Message struct {
	Topic      string            `json:"topic"`       // 主题
	Tags       string            `json:"tags"`        // 标签
	Keys       string            `json:"keys"`        // 消息键
	Body       []byte            `json:"body"`        // 消息体
	Properties map[string]string `json:"properties"`  // 消息属性
	
	// 延时消息相关
	DelayTimeLevel    int32     `json:"delayTimeLevel,omitempty"`    // 延时级别 (1-18)
	StartDeliverTime  int64     `json:"startDeliverTime,omitempty"`  // 开始投递时间戳(毫秒)
	
	// 事务消息相关
	TransactionId     string    `json:"transactionId,omitempty"`     // 事务ID
	
	// 顺序消息相关
	ShardingKey       string    `json:"shardingKey,omitempty"`       // 分片键，用于顺序消息
}

// MessageExt 扩展消息结构体，包含系统属性
type MessageExt struct {
	*Message
	MsgId                string    `json:"msgId"`                // 消息ID
	QueueId              int32     `json:"queueId"`              // 队列ID
	StoreSize            int32     `json:"storeSize"`            // 存储大小
	QueueOffset          int64     `json:"queueOffset"`          // 队列偏移量
	SysFlag              int32     `json:"sysFlag"`              // 系统标志
	BornTimestamp        time.Time `json:"bornTimestamp"`        // 产生时间
	BornHost             string    `json:"bornHost"`             // 产生主机
	StoreTimestamp       time.Time `json:"storeTimestamp"`       // 存储时间
	StoreHost            string    `json:"storeHost"`            // 存储主机
	ReconsumeTimes       int32     `json:"reconsumeTimes"`       // 重试次数
	PreparedTransaction  bool      `json:"preparedTransaction"`  // 是否为事务消息
	CommitLogOffset      int64     `json:"commitLogOffset"`      // CommitLog偏移量
}

// SendResult 发送结果
type SendResult struct {
	SendStatus    SendStatus    `json:"sendStatus"`    // 发送状态
	MsgId         string        `json:"msgId"`         // 消息ID
	MessageQueue  *MessageQueue `json:"messageQueue"`  // 消息队列
	QueueOffset   int64         `json:"queueOffset"`   // 队列偏移量
	TransactionId string        `json:"transactionId"` // 事务ID
	OffsetMsgId   string        `json:"offsetMsgId"`   // 偏移消息ID
	RegionId      string        `json:"regionId"`      // 区域ID
	TraceOn       bool          `json:"traceOn"`       // 是否开启追踪
}

// SendStatus 发送状态枚举
type SendStatus int32

const (
	SendOK                SendStatus = iota // 发送成功
	SendFlushDiskTimeout                    // 刷盘超时
	SendFlushSlaveTimeout                   // 同步到Slave超时
	SendSlaveNotAvailable                   // Slave不可用
)

// MessageQueue 消息队列
type MessageQueue struct {
	Topic      string `json:"topic"`      // 主题
	BrokerName string `json:"brokerName"` // Broker名称
	QueueId    int32  `json:"queueId"`    // 队列ID
}

// ConsumeResult 消费结果
type ConsumeResult int32

const (
	ConsumeSuccess ConsumeResult = iota // 消费成功
	ReconsumeLater                      // 稍后重试
)

// MessageListener 消息监听器接口
type MessageListener interface {
	ConsumeMessage(msgs []*MessageExt) ConsumeResult
}

// MessageListenerConcurrently 并发消息监听器
type MessageListenerConcurrently func(msgs []*MessageExt) ConsumeResult

// ConsumeMessage 实现MessageListener接口
func (listener MessageListenerConcurrently) ConsumeMessage(msgs []*MessageExt) ConsumeResult {
	return listener(msgs)
}

// MessageListenerOrderly 顺序消息监听器
type MessageListenerOrderly func(msgs []*MessageExt, context *ConsumeOrderlyContext) ConsumeResult

// ConsumeMessage 实现MessageListener接口
func (listener MessageListenerOrderly) ConsumeMessage(msgs []*MessageExt) ConsumeResult {
	// 为顺序消费创建默认上下文
	context := &ConsumeOrderlyContext{
		AutoCommit: true,
	}
	return listener(msgs, context)
}

// ConsumeOrderlyContext 顺序消费上下文
type ConsumeOrderlyContext struct {
	MessageQueue *MessageQueue
	AutoCommit   bool
	SuspendTime  time.Duration
}

// ConsumeFromWhere 消费起始位置
type ConsumeFromWhere int32

const (
	ConsumeFromLastOffset  ConsumeFromWhere = iota // 从最后偏移量开始
	ConsumeFromFirstOffset                         // 从第一个偏移量开始
	ConsumeFromTimestamp                           // 从指定时间戳开始
)

// MessageModel 消息模式
type MessageModel int32

const (
	Clustering  MessageModel = iota // 集群模式
	Broadcasting                    // 广播模式
)

// MessageType 消息类型
type MessageType int32

const (
	NormalMessage      MessageType = iota // 普通消息
	FIFOMessage                           // 顺序消息
	DelayMessage                          // 延时消息
	TransactionMessage                    // 事务消息
	BatchMessage                          // 批量消息
)

// BatchMessage 批量消息结构体
type BatchMessages struct {
	Topic    string     `json:"topic"`    // 主题
	Messages []*Message `json:"messages"` // 消息列表
}

// MessageBatch 消息批次接口
type MessageBatch interface {
	GetTopic() string
	GetMessages() []*Message
	Size() int
}

// TransactionStatus 事务状态
type TransactionStatus int32

const (
	CommitTransaction   TransactionStatus = iota // 提交事务
	RollbackTransaction                          // 回滚事务
	UnknownTransaction                           // 未知状态
)

// LocalTransactionState 本地事务状态
type LocalTransactionState int32

const (
	CommitMessage   LocalTransactionState = iota // 提交消息
	RollbackMessage                              // 回滚消息
	UnknownMessage                               // 未知状态
)

// 延时消息级别常量
const (
	DelayLevel1s   = 1  // 1秒
	DelayLevel5s   = 2  // 5秒
	DelayLevel10s  = 3  // 10秒
	DelayLevel30s  = 4  // 30秒
	DelayLevel1m   = 5  // 1分钟
	DelayLevel2m   = 6  // 2分钟
	DelayLevel3m   = 7  // 3分钟
	DelayLevel4m   = 8  // 4分钟
	DelayLevel5m   = 9  // 5分钟
	DelayLevel6m   = 10 // 6分钟
	DelayLevel7m   = 11 // 7分钟
	DelayLevel8m   = 12 // 8分钟
	DelayLevel9m   = 13 // 9分钟
	DelayLevel10m  = 14 // 10分钟
	DelayLevel20m  = 15 // 20分钟
	DelayLevel30m  = 16 // 30分钟
	DelayLevel1h   = 17 // 1小时
	DelayLevel2h   = 18 // 2小时
)

// TransactionListener 事务监听器接口
type TransactionListener interface {
	// ExecuteLocalTransaction 执行本地事务
	ExecuteLocalTransaction(msg *Message, arg interface{}) LocalTransactionState
	// CheckLocalTransaction 检查本地事务状态
	CheckLocalTransaction(msgExt *MessageExt) LocalTransactionState
}

// MessageQueueSelector 消息队列选择器接口
type MessageQueueSelector interface {
	// Select 选择消息队列
	Select(mqs []*MessageQueue, msg *Message, arg interface{}) *MessageQueue
}

// SendCallback 异步发送回调接口
type SendCallback interface {
	// OnSuccess 发送成功回调
	OnSuccess(result *SendResult)
	// OnException 发送异常回调
	OnException(err error)
}

// PullResult 拉取消息结果
type PullResult struct {
	PullStatus      PullStatus     `json:"pullStatus"`      // 拉取状态
	NextBeginOffset int64          `json:"nextBeginOffset"` // 下次开始偏移量
	MinOffset       int64          `json:"minOffset"`       // 最小偏移量
	MaxOffset       int64          `json:"maxOffset"`       // 最大偏移量
	MsgFoundList    []*MessageExt  `json:"msgFoundList"`    // 拉取到的消息列表
}

// PullStatus 拉取状态
type PullStatus int32

const (
	PullFound             PullStatus = iota // 找到消息
	PullNoNewMsg                            // 没有新消息
	PullNoMatchedMsg                        // 没有匹配的消息
	PullOffsetIllegal                       // 偏移量非法
	PullBrokerTimeout                       // Broker超时
)

// SubscriptionData 订阅数据
type SubscriptionData struct {
	Topic           string   `json:"topic"`           // 主题
	SubString       string   `json:"subString"`       // 订阅表达式
	TagsSet         []string `json:"tagsSet"`         // 标签集合
	CodeSet         []int32  `json:"codeSet"`         // 代码集合
	SubVersion      int64    `json:"subVersion"`      // 订阅版本
	ClassFilterMode bool     `json:"classFilterMode"` // 类过滤模式
	FilterClassSource string `json:"filterClassSource"` // 过滤类源码
	ExpressionType  string   `json:"expressionType"`  // 表达式类型 (TAG/SQL92)
}

// FilterType 过滤器类型
type FilterType string

const (
	FilterByTag  FilterType = "TAG"   // 标签过滤
	FilterBySQL  FilterType = "SQL92" // SQL92表达式过滤
)

// TraceType 追踪类型
type TraceType int

const (
	TraceTypeProduce TraceType = iota // 生产者追踪
	TraceTypeConsume                  // 消费者追踪
	TraceTypeSubBefore               // 订阅前追踪
	TraceTypeSubAfter                // 订阅后追踪
)

// TraceBean 追踪数据结构
type TraceBean struct {
	Topic         string            `json:"topic"`
	MsgId         string            `json:"msgId"`
	OffsetMsgId   string            `json:"offsetMsgId"`
	Tags          string            `json:"tags"`
	Keys          string            `json:"keys"`
	StoreHost     string            `json:"storeHost"`
	ClientHost    string            `json:"clientHost"`
	StoreTime     int64             `json:"storeTime"`
	RetryTimes    int               `json:"retryTimes"`
	BodyLength    int               `json:"bodyLength"`
	MsgType       MessageType       `json:"msgType"`
	TraceType     TraceType         `json:"traceType"`
	GroupName     string            `json:"groupName"`
	CostTime      int64             `json:"costTime"`
	Success       bool              `json:"success"`
	RequestId     string            `json:"requestId"`
	ContextCode   int               `json:"contextCode"`
	TimeStamp     int64             `json:"timeStamp"`
	InstanceName  string            `json:"instanceName"`
	RegionId      string            `json:"regionId"`
	Properties    map[string]string `json:"properties"`
}

// TraceContext 追踪上下文
type TraceContext struct {
	TraceType    TraceType    `json:"traceType"`
	TimeStamp    int64        `json:"timeStamp"`
	RegionId     string       `json:"regionId"`
	RegionName   string       `json:"regionName"`
	GroupName    string       `json:"groupName"`
	CostTime     int64        `json:"costTime"`
	Success      bool         `json:"success"`
	RequestId    string       `json:"requestId"`
	ContextCode  int          `json:"contextCode"`
	TraceBeans   []*TraceBean `json:"traceBeans"`
}

// TraceHook 追踪钩子接口
type TraceHook interface {
	// SendTraceData 发送追踪数据
	SendTraceData(ctx *TraceContext) error
	// GetHookName 获取钩子名称
	GetHookName() string
}

// TraceDispatcher 追踪分发器接口
type TraceDispatcher interface {
	// Start 启动分发器
	Start() error
	// Stop 停止分发器
	Stop() error
	// Append 添加追踪数据
	Append(ctx *TraceContext) error
	// Flush 刷新追踪数据
	Flush() error
}

// ConsumeType 消费类型
type ConsumeType int32

const (
	ConsumeConcurrently ConsumeType = iota // 并发消费
	ConsumeOrderly                         // 顺序消费
)

// AllocateMessageQueueStrategy 消息队列分配策略接口
type AllocateMessageQueueStrategy interface {
	// Allocate 分配消息队列
	Allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) []*MessageQueue
	// GetName 获取策略名称
	GetName() string
}

// NewMessage 创建新消息
func NewMessage(topic string, body []byte) *Message {
	return &Message{
		Topic:      topic,
		Body:       body,
		Properties: make(map[string]string),
	}
}

// SetTags 设置消息标签
func (m *Message) SetTags(tags string) *Message {
	m.Tags = tags
	return m
}

// SetKeys 设置消息键
func (m *Message) SetKeys(keys string) *Message {
	m.Keys = keys
	return m
}

// SetProperty 设置消息属性
func (m *Message) SetProperty(key, value string) *Message {
	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	m.Properties[key] = value
	return m
}

// GetProperty 获取消息属性
func (m *Message) GetProperty(key string) string {
	if m.Properties == nil {
		return ""
	}
	return m.Properties[key]
}

// SetDelayTimeLevel 设置延时级别
func (m *Message) SetDelayTimeLevel(level int32) *Message {
	m.DelayTimeLevel = level
	return m
}

// SetStartDeliverTime 设置开始投递时间
func (m *Message) SetStartDeliverTime(timestamp int64) *Message {
	m.StartDeliverTime = timestamp
	return m
}

// SetTransactionId 设置事务ID
func (m *Message) SetTransactionId(transactionId string) *Message {
	m.TransactionId = transactionId
	return m
}

// SetShardingKey 设置分片键（用于顺序消息）
func (m *Message) SetShardingKey(shardingKey string) *Message {
	m.ShardingKey = shardingKey
	return m
}

// IsDelayMessage 判断是否为延时消息
func (m *Message) IsDelayMessage() bool {
	return m.DelayTimeLevel > 0 || m.StartDeliverTime > 0
}

// IsTransactionMessage 判断是否为事务消息
func (m *Message) IsTransactionMessage() bool {
	return m.TransactionId != ""
}

// IsOrderedMessage 判断是否为顺序消息
func (m *Message) IsOrderedMessage() bool {
	return m.ShardingKey != ""
}

// String 返回消息队列的字符串表示
func (mq *MessageQueue) String() string {
	return fmt.Sprintf("MessageQueue{topic='%s', brokerName='%s', queueId=%d}", mq.Topic, mq.BrokerName, mq.QueueId)
}

// NewBatchMessages 创建批量消息
func NewBatchMessages(topic string, messages []*Message) *BatchMessages {
	return &BatchMessages{
		Topic:    topic,
		Messages: messages,
	}
}

// GetTopic 获取主题
func (bm *BatchMessages) GetTopic() string {
	return bm.Topic
}

// GetMessages 获取消息列表
func (bm *BatchMessages) GetMessages() []*Message {
	return bm.Messages
}

// Size 获取批量消息数量
func (bm *BatchMessages) Size() int {
	return len(bm.Messages)
}

// AddMessage 添加消息到批次
func (bm *BatchMessages) AddMessage(msg *Message) {
	bm.Messages = append(bm.Messages, msg)
}

// GetMessageType 获取消息类型
func (m *Message) GetMessageType() MessageType {
	if m.IsTransactionMessage() {
		return TransactionMessage
	}
	if m.IsDelayMessage() {
		return DelayMessage
	}
	if m.IsOrderedMessage() {
		return FIFOMessage
	}
	return NormalMessage
}