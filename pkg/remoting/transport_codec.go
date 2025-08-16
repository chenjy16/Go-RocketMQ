package remoting

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"go-rocketmq/pkg/protocol"
)

// TransportCodec 传输层编解码器
type TransportCodec struct {
	messageCodec *MessageCodec
}

// NewTransportCodec 创建传输层编解码器
func NewTransportCodec() *TransportCodec {
	return &TransportCodec{
		messageCodec: NewMessageCodec(),
	}
}

// EncodeRemotingCommand 编码RemotingCommand
func (tc *TransportCodec) EncodeRemotingCommand(cmd *protocol.RemotingCommand) ([]byte, error) {
	if cmd == nil {
		return nil, fmt.Errorf("remoting command is nil")
	}

	// 序列化头部信息
	headerData, err := tc.encodeHeader(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to encode header: %v", err)
	}

	// 计算总长度
	headerLen := len(headerData)
	bodyLen := len(cmd.Body)
	totalLen := 4 + headerLen + bodyLen // 4字节头部长度 + 头部 + 消息体

	// 创建缓冲区
	buf := bytes.NewBuffer(make([]byte, 0, totalLen))

	// 写入总长度（不包括这4个字节）
	if err := binary.Write(buf, binary.BigEndian, int32(totalLen-4)); err != nil {
		return nil, fmt.Errorf("failed to write total length: %v", err)
	}

	// 写入头部长度
	if err := binary.Write(buf, binary.BigEndian, int32(headerLen)); err != nil {
		return nil, fmt.Errorf("failed to write header length: %v", err)
	}

	// 写入头部数据
	if _, err := buf.Write(headerData); err != nil {
		return nil, fmt.Errorf("failed to write header data: %v", err)
	}

	// 写入消息体
	if bodyLen > 0 {
		if _, err := buf.Write(cmd.Body); err != nil {
			return nil, fmt.Errorf("failed to write body: %v", err)
		}
	}

	return buf.Bytes(), nil
}

// DecodeRemotingCommand 解码RemotingCommand
func (tc *TransportCodec) DecodeRemotingCommand(reader io.Reader) (*protocol.RemotingCommand, error) {
	// 读取总长度
	var totalLen int32
	if err := binary.Read(reader, binary.BigEndian, &totalLen); err != nil {
		return nil, fmt.Errorf("failed to read total length: %v", err)
	}

	if totalLen <= 0 || totalLen > 16*1024*1024 { // 16MB限制
		return nil, fmt.Errorf("invalid total length: %d", totalLen)
	}

	// 读取头部长度
	var headerLen int32
	if err := binary.Read(reader, binary.BigEndian, &headerLen); err != nil {
		return nil, fmt.Errorf("failed to read header length: %v", err)
	}

	if headerLen <= 0 || headerLen > totalLen {
		return nil, fmt.Errorf("invalid header length: %d", headerLen)
	}

	// 读取头部数据
	headerData := make([]byte, headerLen)
	if _, err := io.ReadFull(reader, headerData); err != nil {
		return nil, fmt.Errorf("failed to read header data: %v", err)
	}

	// 解码头部
	cmd, err := tc.decodeHeader(headerData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode header: %v", err)
	}

	// 读取消息体
	bodyLen := totalLen - 4 - headerLen // 总长度 - 头部长度字段 - 头部数据
	if bodyLen > 0 {
		cmd.Body = make([]byte, bodyLen)
		if _, err := io.ReadFull(reader, cmd.Body); err != nil {
			return nil, fmt.Errorf("failed to read body: %v", err)
		}
	}

	return cmd, nil
}

// encodeHeader 编码头部信息
func (tc *TransportCodec) encodeHeader(cmd *protocol.RemotingCommand) ([]byte, error) {
	// 创建头部结构
	header := map[string]interface{}{
		"code":     int32(cmd.Code),
		"language": cmd.Language,
		"version":  cmd.Version,
		"opaque":   cmd.Opaque,
		"flag":     cmd.Flag,
	}

	// 添加备注
	if cmd.Remark != "" {
		header["remark"] = cmd.Remark
	}

	// 添加扩展字段
	if cmd.ExtFields != nil && len(cmd.ExtFields) > 0 {
		header["extFields"] = cmd.ExtFields
	}

	// 序列化为JSON
	headerData, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal header: %v", err)
	}

	return headerData, nil
}

// decodeHeader 解码头部信息
func (tc *TransportCodec) decodeHeader(headerData []byte) (*protocol.RemotingCommand, error) {
	// 解析JSON
	var header map[string]interface{}
	if err := json.Unmarshal(headerData, &header); err != nil {
		return nil, fmt.Errorf("failed to unmarshal header: %v", err)
	}

	// 创建RemotingCommand
	cmd := &protocol.RemotingCommand{
		ExtFields: make(map[string]string),
	}

	// 解析基本字段
	if code, ok := header["code"].(float64); ok {
		cmd.Code = protocol.RequestCode(int32(code))
	}

	if language, ok := header["language"].(string); ok {
		cmd.Language = language
	}

	if version, ok := header["version"].(float64); ok {
		cmd.Version = int32(version)
	}

	if opaque, ok := header["opaque"].(float64); ok {
		cmd.Opaque = int32(opaque)
	}

	if flag, ok := header["flag"].(float64); ok {
		cmd.Flag = int32(flag)
	}

	if remark, ok := header["remark"].(string); ok {
		cmd.Remark = remark
	}

	// 解析扩展字段
	if extFields, ok := header["extFields"].(map[string]interface{}); ok {
		for key, value := range extFields {
			if strValue, ok := value.(string); ok {
				cmd.ExtFields[key] = strValue
			}
		}
	}

	return cmd, nil
}

// EncodeMessage 编码消息（委托给MessageCodec）
func (tc *TransportCodec) EncodeMessage(msg *Message) ([]byte, error) {
	return tc.messageCodec.EncodeMessage(msg)
}

// DecodeMessage 解码消息（委托给MessageCodec）
func (tc *TransportCodec) DecodeMessage(data []byte) (*Message, error) {
	return tc.messageCodec.DecodeMessage(data)
}

// EncodeMessages 编码批量消息（委托给MessageCodec）
func (tc *TransportCodec) EncodeMessages(messages []*Message) ([]byte, error) {
	return tc.messageCodec.EncodeMessages(messages)
}

// DecodeMessages 解码批量消息（委托给MessageCodec）
func (tc *TransportCodec) DecodeMessages(data []byte) ([]*Message, error) {
	return tc.messageCodec.DecodeMessages(data)
}

// ValidateRemotingCommand 验证RemotingCommand
func (tc *TransportCodec) ValidateRemotingCommand(cmd *protocol.RemotingCommand) error {
	if cmd == nil {
		return fmt.Errorf("remoting command is nil")
	}

	if cmd.Language == "" {
		cmd.Language = "GO" // 设置默认语言
	}

	if cmd.Version <= 0 {
		cmd.Version = 1 // 设置默认版本
	}

	// 验证消息体大小
	if len(cmd.Body) > 4*1024*1024 { // 4MB限制
		return fmt.Errorf("message body size exceeds 4MB")
	}

	// 验证扩展字段
	if cmd.ExtFields != nil {
		extFieldsData, _ := json.Marshal(cmd.ExtFields)
		if len(extFieldsData) > 64*1024 { // 64KB限制
			return fmt.Errorf("ext fields size exceeds 64KB")
		}
	}

	return nil
}

// CalculateRemotingCommandSize 计算RemotingCommand大小
func (tc *TransportCodec) CalculateRemotingCommandSize(cmd *protocol.RemotingCommand) (int, error) {
	if cmd == nil {
		return 0, fmt.Errorf("remoting command is nil")
	}

	headerData, err := tc.encodeHeader(cmd)
	if err != nil {
		return 0, fmt.Errorf("failed to encode header: %v", err)
	}

	// 4字节总长度 + 4字节头部长度 + 头部数据 + 消息体
	return 4 + 4 + len(headerData) + len(cmd.Body), nil
}

// CreateRequestCommand 创建请求命令
func (tc *TransportCodec) CreateRequestCommand(code protocol.RequestCode, extFields map[string]string, body []byte) *protocol.RemotingCommand {
	cmd := protocol.CreateRemotingCommand(code)
	if extFields != nil {
		cmd.ExtFields = extFields
	}
	cmd.Body = body
	return cmd
}

// CreateResponseCommand 创建响应命令
func (tc *TransportCodec) CreateResponseCommand(code protocol.ResponseCode, remark string, extFields map[string]string, body []byte) *protocol.RemotingCommand {
	cmd := protocol.CreateResponseCommand(code, remark)
	if extFields != nil {
		cmd.ExtFields = extFields
	}
	cmd.Body = body
	return cmd
}

// IsRequestCommand 判断是否为请求命令
func (tc *TransportCodec) IsRequestCommand(cmd *protocol.RemotingCommand) bool {
	return cmd != nil && (cmd.Flag&0x01) == 0
}

// IsResponseCommand 判断是否为响应命令
func (tc *TransportCodec) IsResponseCommand(cmd *protocol.RemotingCommand) bool {
	return cmd != nil && (cmd.Flag&0x01) == 1
}

// IsOnewayCommand 判断是否为单向命令
func (tc *TransportCodec) IsOnewayCommand(cmd *protocol.RemotingCommand) bool {
	return cmd != nil && (cmd.Flag&0x02) == 2
}

// SetRequestFlag 设置请求标志
func (tc *TransportCodec) SetRequestFlag(cmd *protocol.RemotingCommand) {
	if cmd != nil {
		cmd.Flag = cmd.Flag &^ 0x01 // 清除响应标志
	}
}

// SetResponseFlag 设置响应标志
func (tc *TransportCodec) SetResponseFlag(cmd *protocol.RemotingCommand) {
	if cmd != nil {
		cmd.Flag = cmd.Flag | 0x01 // 设置响应标志
	}
}

// SetOnewayFlag 设置单向标志
func (tc *TransportCodec) SetOnewayFlag(cmd *protocol.RemotingCommand) {
	if cmd != nil {
		cmd.Flag = cmd.Flag | 0x02 // 设置单向标志
	}
}

// ClearOnewayFlag 清除单向标志
func (tc *TransportCodec) ClearOnewayFlag(cmd *protocol.RemotingCommand) {
	if cmd != nil {
		cmd.Flag = cmd.Flag &^ 0x02 // 清除单向标志
	}
}

// GetCommandType 获取命令类型
func (tc *TransportCodec) GetCommandType(cmd *protocol.RemotingCommand) string {
	if cmd == nil {
		return "unknown"
	}

	if tc.IsOnewayCommand(cmd) {
		return "oneway"
	} else if tc.IsResponseCommand(cmd) {
		return "response"
	} else {
		return "request"
	}
}

// CloneRemotingCommand 克隆RemotingCommand
func (tc *TransportCodec) CloneRemotingCommand(cmd *protocol.RemotingCommand) *protocol.RemotingCommand {
	if cmd == nil {
		return nil
	}

	clone := &protocol.RemotingCommand{
		Code:     cmd.Code,
		Language: cmd.Language,
		Version:  cmd.Version,
		Opaque:   cmd.Opaque,
		Flag:     cmd.Flag,
		Remark:   cmd.Remark,
	}

	// 克隆ExtFields
	if cmd.ExtFields != nil {
		clone.ExtFields = make(map[string]string)
		for k, v := range cmd.ExtFields {
			clone.ExtFields[k] = v
		}
	}

	// 克隆Body
	if cmd.Body != nil {
		clone.Body = make([]byte, len(cmd.Body))
		copy(clone.Body, cmd.Body)
	}

	return clone
}