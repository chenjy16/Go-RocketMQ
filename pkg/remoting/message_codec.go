package remoting

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Message RocketMQ消息结构
type Message struct {
	Topic          string            `json:"topic"`
	Flag           int32             `json:"flag"`
	Properties     map[string]string `json:"properties"`
	Body           []byte            `json:"body"`
	TransactionId  string            `json:"transactionId,omitempty"`
	Batch          bool              `json:"batch,omitempty"`
	Compressed     bool              `json:"compressed,omitempty"`
}

// MessageExt 扩展消息结构（包含系统属性）
type MessageExt struct {
	*Message
	MsgId                 string `json:"msgId"`
	QueueId               int32  `json:"queueId"`
	StoreSize             int32  `json:"storeSize"`
	QueueOffset           int64  `json:"queueOffset"`
	SysFlag               int32  `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	BornHost              string `json:"bornHost"`
	StoreTimestamp        int64  `json:"storeTimestamp"`
	StoreHost             string `json:"storeHost"`
	ReconsumeTimes        int32  `json:"reconsumeTimes"`
	PreparedTransactionOffset int64 `json:"preparedTransactionOffset"`
}

// MessageCodec 消息编解码器
type MessageCodec struct{}

// NewMessageCodec 创建消息编解码器
func NewMessageCodec() *MessageCodec {
	return &MessageCodec{}
}

// EncodeMessage 编码消息
func (mc *MessageCodec) EncodeMessage(msg *Message) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}
	
	// 计算消息总长度
	topicLen := len(msg.Topic)
	bodyLen := len(msg.Body)
	propertiesData := mc.encodeProperties(msg.Properties)
	propertiesLen := len(propertiesData)
	
	// 消息格式：
	// 4字节总长度 + 4字节魔数 + 4字节CRC + 4字节Flag + 4字节Body长度 + 4字节Body CRC
	// + 1字节Topic长度 + Topic + 2字节Properties长度 + Properties + Body
	totalLen := 4 + 4 + 4 + 4 + 4 + 4 + 1 + topicLen + 2 + propertiesLen + bodyLen
	
	buf := bytes.NewBuffer(make([]byte, 0, totalLen))
	
	// 写入总长度（不包括这4个字节）
	binary.Write(buf, binary.BigEndian, int32(totalLen-4))
	
	// 写入魔数
	binary.Write(buf, binary.BigEndian, int32(0x7ABBCCDD))
	
	// 写入CRC（暂时写0，实际应该计算）
	binary.Write(buf, binary.BigEndian, int32(0))
	
	// 写入Flag
	binary.Write(buf, binary.BigEndian, msg.Flag)
	
	// 写入Body长度
	binary.Write(buf, binary.BigEndian, int32(bodyLen))
	
	// 写入Body CRC（暂时写0）
	binary.Write(buf, binary.BigEndian, int32(0))
	
	// 写入Topic长度和Topic
	if topicLen > 255 {
		return nil, fmt.Errorf("topic length exceeds 255 bytes")
	}
	buf.WriteByte(byte(topicLen))
	buf.WriteString(msg.Topic)
	
	// 写入Properties长度和Properties
	if propertiesLen > 65535 {
		return nil, fmt.Errorf("properties length exceeds 65535 bytes")
	}
	binary.Write(buf, binary.BigEndian, int16(propertiesLen))
	buf.Write(propertiesData)
	
	// 写入Body
	buf.Write(msg.Body)
	
	return buf.Bytes(), nil
}

// DecodeMessage 解码消息
func (mc *MessageCodec) DecodeMessage(data []byte) (*Message, error) {
	if len(data) < 24 { // 最小消息长度
		return nil, fmt.Errorf("message data too short")
	}
	
	reader := bytes.NewReader(data)
	
	// 读取总长度
	var totalLen int32
	if err := binary.Read(reader, binary.BigEndian, &totalLen); err != nil {
		return nil, fmt.Errorf("failed to read total length: %v", err)
	}
	
	// 读取魔数
	var magicCode int32
	if err := binary.Read(reader, binary.BigEndian, &magicCode); err != nil {
		return nil, fmt.Errorf("failed to read magic code: %v", err)
	}
	if magicCode != 0x7ABBCCDD {
		return nil, fmt.Errorf("invalid magic code: %x", magicCode)
	}
	
	// 读取CRC
	var crc int32
	if err := binary.Read(reader, binary.BigEndian, &crc); err != nil {
		return nil, fmt.Errorf("failed to read crc: %v", err)
	}
	
	// 读取Flag
	var flag int32
	if err := binary.Read(reader, binary.BigEndian, &flag); err != nil {
		return nil, fmt.Errorf("failed to read flag: %v", err)
	}
	
	// 读取Body长度
	var bodyLen int32
	if err := binary.Read(reader, binary.BigEndian, &bodyLen); err != nil {
		return nil, fmt.Errorf("failed to read body length: %v", err)
	}
	
	// 读取Body CRC
	var bodyCrc int32
	if err := binary.Read(reader, binary.BigEndian, &bodyCrc); err != nil {
		return nil, fmt.Errorf("failed to read body crc: %v", err)
	}
	
	// 读取Topic长度
	topicLenByte, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read topic length: %v", err)
	}
	topicLen := int(topicLenByte)
	
	// 读取Topic
	topicData := make([]byte, topicLen)
	if _, err := reader.Read(topicData); err != nil {
		return nil, fmt.Errorf("failed to read topic: %v", err)
	}
	topic := string(topicData)
	
	// 读取Properties长度
	var propertiesLen int16
	if err := binary.Read(reader, binary.BigEndian, &propertiesLen); err != nil {
		return nil, fmt.Errorf("failed to read properties length: %v", err)
	}
	
	// 读取Properties
	var properties map[string]string
	if propertiesLen > 0 {
		propertiesData := make([]byte, propertiesLen)
		if _, err := reader.Read(propertiesData); err != nil {
			return nil, fmt.Errorf("failed to read properties: %v", err)
		}
		properties = mc.decodeProperties(propertiesData)
	} else {
		properties = make(map[string]string)
	}
	
	// 读取Body
	var body []byte
	if bodyLen > 0 {
		body = make([]byte, bodyLen)
		if _, err := reader.Read(body); err != nil {
			return nil, fmt.Errorf("failed to read body: %v", err)
		}
	}
	
	return &Message{
		Topic:      topic,
		Flag:       flag,
		Properties: properties,
		Body:       body,
	}, nil
}

// EncodeMessages 编码批量消息
func (mc *MessageCodec) EncodeMessages(messages []*Message) ([]byte, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to encode")
	}
	
	var buf bytes.Buffer
	
	for _, msg := range messages {
		msgData, err := mc.EncodeMessage(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to encode message: %v", err)
		}
		
		// 写入单个消息长度
		binary.Write(&buf, binary.BigEndian, int32(len(msgData)))
		// 写入消息数据
		buf.Write(msgData)
	}
	
	return buf.Bytes(), nil
}

// DecodeMessages 解码批量消息
func (mc *MessageCodec) DecodeMessages(data []byte) ([]*Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("no data to decode")
	}
	
	reader := bytes.NewReader(data)
	var messages []*Message
	
	for reader.Len() > 0 {
		// 读取消息长度
		var msgLen int32
		if err := binary.Read(reader, binary.BigEndian, &msgLen); err != nil {
			break // 可能已经读完了
		}
		
		if msgLen <= 0 || msgLen > int32(reader.Len()) {
			return nil, fmt.Errorf("invalid message length: %d", msgLen)
		}
		
		// 读取消息数据
		msgData := make([]byte, msgLen)
		if _, err := reader.Read(msgData); err != nil {
			return nil, fmt.Errorf("failed to read message data: %v", err)
		}
		
		// 解码消息
		msg, err := mc.DecodeMessage(msgData)
		if err != nil {
			return nil, fmt.Errorf("failed to decode message: %v", err)
		}
		
		messages = append(messages, msg)
	}
	
	return messages, nil
}

// encodeProperties 编码属性
func (mc *MessageCodec) encodeProperties(properties map[string]string) []byte {
	if len(properties) == 0 {
		return nil
	}
	
	var parts []string
	for key, value := range properties {
		parts = append(parts, key+"\x01"+value)
	}
	
	return []byte(strings.Join(parts, "\x02"))
}

// decodeProperties 解码属性
func (mc *MessageCodec) decodeProperties(data []byte) map[string]string {
	properties := make(map[string]string)
	
	if len(data) == 0 {
		return properties
	}
	
	propertyStr := string(data)
	pairs := strings.Split(propertyStr, "\x02")
	
	for _, pair := range pairs {
		if pair == "" {
			continue
		}
		
		parts := strings.Split(pair, "\x01")
		if len(parts) == 2 {
			properties[parts[0]] = parts[1]
		}
	}
	
	return properties
}

// SetMessageProperty 设置消息属性
func (mc *MessageCodec) SetMessageProperty(msg *Message, key, value string) {
	if msg.Properties == nil {
		msg.Properties = make(map[string]string)
	}
	msg.Properties[key] = value
}

// GetMessageProperty 获取消息属性
func (mc *MessageCodec) GetMessageProperty(msg *Message, key string) (string, bool) {
	if msg.Properties == nil {
		return "", false
	}
	value, exists := msg.Properties[key]
	return value, exists
}

// SetMessageKeys 设置消息Keys
func (mc *MessageCodec) SetMessageKeys(msg *Message, keys []string) {
	if len(keys) > 0 {
		mc.SetMessageProperty(msg, "KEYS", strings.Join(keys, " "))
	}
}

// GetMessageKeys 获取消息Keys
func (mc *MessageCodec) GetMessageKeys(msg *Message) []string {
	keysStr, exists := mc.GetMessageProperty(msg, "KEYS")
	if !exists || keysStr == "" {
		return nil
	}
	return strings.Split(keysStr, " ")
}

// SetMessageTag 设置消息Tag
func (mc *MessageCodec) SetMessageTag(msg *Message, tag string) {
	if tag != "" {
		mc.SetMessageProperty(msg, "TAGS", tag)
	}
}

// GetMessageTag 获取消息Tag
func (mc *MessageCodec) GetMessageTag(msg *Message) string {
	tag, _ := mc.GetMessageProperty(msg, "TAGS")
	return tag
}

// SetMessageDelayLevel 设置消息延迟级别
func (mc *MessageCodec) SetMessageDelayLevel(msg *Message, delayLevel int) {
	if delayLevel > 0 {
		mc.SetMessageProperty(msg, "DELAY", strconv.Itoa(delayLevel))
	}
}

// GetMessageDelayLevel 获取消息延迟级别
func (mc *MessageCodec) GetMessageDelayLevel(msg *Message) int {
	delayStr, exists := mc.GetMessageProperty(msg, "DELAY")
	if !exists {
		return 0
	}
	delay, err := strconv.Atoi(delayStr)
	if err != nil {
		return 0
	}
	return delay
}

// SetMessageBornTimestamp 设置消息出生时间戳
func (mc *MessageCodec) SetMessageBornTimestamp(msg *Message, timestamp int64) {
	mc.SetMessageProperty(msg, "BORN_TIMESTAMP", strconv.FormatInt(timestamp, 10))
}

// GetMessageBornTimestamp 获取消息出生时间戳
func (mc *MessageCodec) GetMessageBornTimestamp(msg *Message) int64 {
	timestampStr, exists := mc.GetMessageProperty(msg, "BORN_TIMESTAMP")
	if !exists {
		return time.Now().UnixMilli()
	}
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return time.Now().UnixMilli()
	}
	return timestamp
}

// SetMessageBornHost 设置消息出生主机
func (mc *MessageCodec) SetMessageBornHost(msg *Message, host string) {
	if host != "" {
		mc.SetMessageProperty(msg, "BORN_HOST", host)
	}
}

// GetMessageBornHost 获取消息出生主机
func (mc *MessageCodec) GetMessageBornHost(msg *Message) string {
	host, _ := mc.GetMessageProperty(msg, "BORN_HOST")
	return host
}

// SetMessageTraceContext 设置消息追踪上下文
func (mc *MessageCodec) SetMessageTraceContext(msg *Message, traceContext string) {
	if traceContext != "" {
		mc.SetMessageProperty(msg, "TRACE_CONTEXT", traceContext)
	}
}

// GetMessageTraceContext 获取消息追踪上下文
func (mc *MessageCodec) GetMessageTraceContext(msg *Message) string {
	traceContext, _ := mc.GetMessageProperty(msg, "TRACE_CONTEXT")
	return traceContext
}

// ValidateMessage 验证消息
func (mc *MessageCodec) ValidateMessage(msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	
	if msg.Topic == "" {
		return fmt.Errorf("message topic is empty")
	}
	
	if len(msg.Topic) > 255 {
		return fmt.Errorf("message topic length exceeds 255 bytes")
	}
	
	if len(msg.Body) > 4*1024*1024 { // 4MB限制
		return fmt.Errorf("message body size exceeds 4MB")
	}
	
	// 验证属性
	propertiesData := mc.encodeProperties(msg.Properties)
	if len(propertiesData) > 65535 {
		return fmt.Errorf("message properties size exceeds 65535 bytes")
	}
	
	return nil
}

// CalculateMessageSize 计算消息大小
func (mc *MessageCodec) CalculateMessageSize(msg *Message) int {
	if msg == nil {
		return 0
	}
	
	topicLen := len(msg.Topic)
	bodyLen := len(msg.Body)
	propertiesLen := len(mc.encodeProperties(msg.Properties))
	
	// 消息头部固定大小 + Topic长度 + Properties长度 + Body长度
	return 4 + 4 + 4 + 4 + 4 + 4 + 1 + topicLen + 2 + propertiesLen + bodyLen
}