package common

import (
	"fmt"
	"time"
)

// Message 消息结构体
type Message struct {
	Topic      string            `json:"topic"`      // 主题
	Tags       string            `json:"tags"`       // 标签
	Keys       string            `json:"keys"`       // 消息键
	Body       []byte            `json:"body"`       // 消息体
	Properties map[string]string `json:"properties"` // 消息属性
}

// MessageExt 扩展消息结构体，包含系统属性
type MessageExt struct {
	*Message
	MsgId               string    `json:"msgId"`               // 消息ID
	QueueId             int32     `json:"queueId"`             // 队列ID
	StoreSize           int32     `json:"storeSize"`           // 存储大小
	QueueOffset         int64     `json:"queueOffset"`         // 队列偏移量
	SysFlag             int32     `json:"sysFlag"`             // 系统标志
	BornTimestamp       time.Time `json:"bornTimestamp"`       // 产生时间
	BornHost            string    `json:"bornHost"`            // 产生主机
	StoreTimestamp      time.Time `json:"storeTimestamp"`      // 存储时间
	StoreHost           string    `json:"storeHost"`           // 存储主机
	ReconsumeTimes      int32     `json:"reconsumeTimes"`      // 重试次数
	PreparedTransaction bool      `json:"preparedTransaction"` // 是否为事务消息
	CommitLogOffset     int64     `json:"commitLogOffset"`     // CommitLog偏移量
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

// MessageQueue 消息队列
type MessageQueue struct {
	Topic      string `json:"topic"`      // 主题
	BrokerName string `json:"brokerName"` // Broker名称
	QueueId    int32  `json:"queueId"`    // 队列ID
}

// ConsumeResult 消费结果
type ConsumeResult int32

// MessageListener 消息监听器接口
type MessageListener interface {
	ConsumeMessage(msgs []*MessageExt) ConsumeResult
}

// MessageListenerConcurrently 并发消息监听器
type MessageListenerConcurrently func(msgs []*MessageExt) ConsumeResult

// MessageListenerOrderly 顺序消息监听器
type MessageListenerOrderly func(msgs []*MessageExt, context *ConsumeOrderlyContext) ConsumeResult

// ConsumeOrderlyContext 顺序消费上下文
type ConsumeOrderlyContext struct {
	MessageQueue *MessageQueue
	AutoCommit   bool
	SuspendTime  time.Duration
}

// ConsumeFromWhere 消费起始位置
type ConsumeFromWhere int32

// MessageModel 消息模式
type MessageModel int32

// NewMessage 创建新消息
func NewMessage(topic string, body []byte) *Message {
	return &Message{
		Topic:      topic,
		Body:       body,
		Properties: make(map[string]string),
	}
}

// SetTags 设置标签
func (m *Message) SetTags(tags string) *Message {
	m.Tags = tags
	return m
}

// SetKeys 设置消息键
func (m *Message) SetKeys(keys string) *Message {
	m.Keys = keys
	return m
}

// SetProperty 设置属性
func (m *Message) SetProperty(key, value string) *Message {
	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	m.Properties[key] = value
	return m
}

// GetProperty 获取属性
func (m *Message) GetProperty(key string) string {
	if m.Properties == nil {
		return ""
	}
	return m.Properties[key]
}

// SetDelayTimeLevel 设置延时级别
func (m *Message) SetDelayTimeLevel(level int32) *Message {
	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	m.Properties["DELAY_TIME_LEVEL"] = fmt.Sprintf("%d", level)
	return m
}

// SetStartDeliverTime 设置开始投递时间
func (m *Message) SetStartDeliverTime(timestamp int64) *Message {
	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	m.Properties["START_DELIVER_TIME"] = fmt.Sprintf("%d", timestamp)
	return m
}

// String 返回消息队列的字符串表示
func (mq *MessageQueue) String() string {
	return fmt.Sprintf("MessageQueue{topic=%s, brokerName=%s, queueId=%d}", mq.Topic, mq.BrokerName, mq.QueueId)
}

// TraceManager 消息追踪管理器（简化版本）
type TraceManager struct {
	enabled bool
}

// NewTraceManager 创建新的追踪管理器
func NewTraceManager(dispatcher interface{}) *TraceManager {
	return &TraceManager{}
}

// SetEnabled 设置是否启用追踪
func (tm *TraceManager) SetEnabled(enabled bool) {
	tm.enabled = enabled
}

// IsEnabled 检查是否启用追踪
func (tm *TraceManager) IsEnabled() bool {
	return tm.enabled
}

// Start 启动追踪管理器
func (tm *TraceManager) Start() error {
	return nil
}

// Stop 停止追踪管理器
func (tm *TraceManager) Stop() {
	// 简化实现
}

// TraceMessage 追踪消息
func (tm *TraceManager) TraceMessage(context interface{}) {
	// 简化实现
}

// ACLMiddleware ACL中间件（简化版本）
type ACLMiddleware struct {
	enabled bool
}

// NewACLMiddleware 创建新的ACL中间件
func NewACLMiddleware(accessKey, secretKey string, signatureMethod string, enabled bool) *ACLMiddleware {
	return &ACLMiddleware{enabled: enabled}
}

// SetEnabled 设置是否启用ACL
func (acl *ACLMiddleware) SetEnabled(enabled bool) {
	acl.enabled = enabled
}

// IsEnabled 检查是否启用ACL
func (acl *ACLMiddleware) IsEnabled() bool {
	return acl.enabled
}

// HmacSHA1 签名方法
const HmacSHA1 = "HmacSHA1"

// SendStatus 枚举保持不变
const (
	SendOK                SendStatus = iota // 发送成功
	SendFlushDiskTimeout                    // 刷盘超时
	SendFlushSlaveTimeout                   // 同步到Slave超时
	SendSlaveNotAvailable                   // Slave不可用
)

// ConsumeResult 枚举保持不变
const (
	ConsumeSuccess ConsumeResult = iota // 消费成功
	ReconsumeLater                      // 稍后重试
)

// MessageModel 枚举保持不变
const (
	Clustering   MessageModel = iota // 集群模式
	Broadcasting                     // 广播模式
)

// ConsumeFromWhere 枚举保持不变
const (
	ConsumeFromLastOffset  ConsumeFromWhere = iota // 从最后偏移量开始
	ConsumeFromFirstOffset                         // 从第一个偏移量开始
	ConsumeFromTimestamp                           // 从指定时间戳开始
)
