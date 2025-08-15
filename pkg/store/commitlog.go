package store

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go-rocketmq/pkg/common"
)

// CommitLog 提交日志，所有消息顺序写入
type CommitLog struct {
	storeConfig *StoreConfig
	
	// 映射文件管理
	mapedFileQueue *MapedFileQueue
	
	// 写入位置
	writePosition int64
	
	// 刷盘位置
	flushedPosition int64
	
	// 提交位置（用于异步刷盘）
	committedPosition int64
	
	// 锁
	putMessageLock sync.Mutex
	
	// 运行状态
	running bool
	mutex   sync.RWMutex
}

// PutMessageResult 存储消息结果
type PutMessageResult struct {
	PutMessageStatus PutMessageStatus
	AppendMessageResult *AppendMessageResult
}

// PutMessageStatus 存储消息状态
type PutMessageStatus int

const (
	PUT_OK PutMessageStatus = iota
	CREATE_MAPEDFILE_FAILED
	MESSAGE_ILLEGAL
	PROPERTIES_SIZE_EXCEEDED
	UNKNOWN_ERROR
)

// AppendMessageResult 追加消息结果
type AppendMessageResult struct {
	Status          AppendMessageStatus
	WroteOffset     int64  // 写入偏移量
	WroteBytes      int32  // 写入字节数
	MsgId           string // 消息ID
	StoreTimestamp  time.Time // 存储时间戳
	LogicsOffset    int64  // 逻辑偏移量
	PageCacheRT     int64  // 页缓存往返时间
}

// AppendMessageStatus 追加消息状态
type AppendMessageStatus int

const (
	PUT_MESSAGE_OK AppendMessageStatus = iota
	END_OF_FILE
	MESSAGE_SIZE_EXCEEDED
	PROPERTIES_SIZE_EXCEEDED_APPEND
	UNKNOWN_ERROR_APPEND
)

// 消息存储格式常量
const (
	// 消息头部固定长度
	MESSAGE_MAGIC_CODE_POSTION = 4
	MESSAGE_FLAG_POSTION       = 16
	MESSAGE_PHYSIC_OFFSET_POSTION = 28
	MESSAGE_STORE_TIMESTAMP_POSTION = 56
	
	// 消息魔数
	MESSAGE_MAGIC_CODE = 0xAABBCCDD
	
	// 空消息长度
	BLANK_MAGIC_CODE = 0xBBCCDDEE
)

// NewCommitLog 创建CommitLog
func NewCommitLog(storeConfig *StoreConfig) (*CommitLog, error) {
	mapedFileQueue, err := NewMapedFileQueue(
		storeConfig.StorePathCommitLog,
		storeConfig.MapedFileSizeCommitLog,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create maped file queue: %v", err)
	}
	
	commitLog := &CommitLog{
		storeConfig:    storeConfig,
		mapedFileQueue: mapedFileQueue,
	}
	
	// 恢复写入位置
	commitLog.recoverWritePosition()
	
	return commitLog, nil
}

// Start 启动CommitLog
func (cl *CommitLog) Start() error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()
	
	if cl.running {
		return fmt.Errorf("commit log is already running")
	}
	
	cl.running = true
	return nil
}

// Shutdown 关闭CommitLog
func (cl *CommitLog) Shutdown() {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()
	
	if !cl.running {
		return
	}
	
	// 强制刷盘
	cl.flush()
	
	// 关闭映射文件队列
	cl.mapedFileQueue.Shutdown()
	
	cl.running = false
}

// PutMessage 存储消息
func (cl *CommitLog) PutMessage(msgExt *common.MessageExt) (*common.SendResult, error) {
	cl.putMessageLock.Lock()
	defer cl.putMessageLock.Unlock()
	
	if !cl.running {
		return nil, fmt.Errorf("commit log is not running")
	}
	
	// 序列化消息
	msgBytes, err := cl.serializeMessage(msgExt)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize message: %v", err)
	}
	
	// 获取当前映射文件
	mapedFile := cl.mapedFileQueue.GetLastMapedFile()
	if mapedFile == nil {
		// 创建新的映射文件
		mapedFile, err = cl.mapedFileQueue.GetLastMapedFileOrCreate(cl.writePosition)
		if err != nil {
			return nil, fmt.Errorf("failed to get maped file: %v", err)
		}
	}
	
	// 追加消息到映射文件
	result, err := cl.appendMessage(mapedFile, msgExt, msgBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to append message: %v", err)
	}
	
	// 处理追加结果
	switch result.Status {
	case PUT_MESSAGE_OK:
		// 更新写入位置
		atomic.StoreInt64(&cl.writePosition, result.WroteOffset+int64(result.WroteBytes))
		
		// 更新消息扩展信息
		msgExt.CommitLogOffset = result.WroteOffset
		msgExt.StoreSize = result.WroteBytes
		msgExt.MsgId = result.MsgId
		msgExt.StoreTimestamp = result.StoreTimestamp
		
		// 构造发送结果
		sendResult := &common.SendResult{
			SendStatus: common.SendOK,
			MsgId:      result.MsgId,
			MessageQueue: &common.MessageQueue{
				Topic:      msgExt.Topic,
				BrokerName: "broker-a", // 简化版本
				QueueId:    msgExt.QueueId,
			},
			QueueOffset: result.LogicsOffset,
		}
		
		return sendResult, nil
		
	case END_OF_FILE:
		// 文件已满，创建新文件
		newMapedFile, err := cl.mapedFileQueue.GetLastMapedFileOrCreate(cl.writePosition)
		if err != nil {
			return nil, fmt.Errorf("failed to create new maped file: %v", err)
		}
		
		// 在新文件中重试
		return cl.retryAppendMessage(newMapedFile, msgExt, msgBytes)
		
	default:
		return nil, fmt.Errorf("failed to append message, status: %v", result.Status)
	}
}

// appendMessage 追加消息到映射文件
func (cl *CommitLog) appendMessage(mapedFile *MapedFile, msgExt *common.MessageExt, msgBytes []byte) (*AppendMessageResult, error) {
	currentPos := mapedFile.GetWrotePosition()
	
	// 检查剩余空间
	if currentPos+int64(len(msgBytes)) > mapedFile.GetFileSize() {
		return &AppendMessageResult{
			Status: END_OF_FILE,
		}, nil
	}
	
	// 写入消息
	n, err := mapedFile.AppendMessage(msgBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to write message to maped file: %v", err)
	}
	
	// 生成消息ID
	msgId := cl.generateMessageId(mapedFile.GetFileName(), currentPos)
	
	return &AppendMessageResult{
		Status:         PUT_MESSAGE_OK,
		WroteOffset:    mapedFile.GetFileFromOffset() + currentPos,
		WroteBytes:     int32(n),
		MsgId:          msgId,
		StoreTimestamp: time.Now(),
		LogicsOffset:   0, // 将在ConsumeQueue中设置
	}, nil
}

// retryAppendMessage 在新文件中重试追加消息
func (cl *CommitLog) retryAppendMessage(mapedFile *MapedFile, msgExt *common.MessageExt, msgBytes []byte) (*common.SendResult, error) {
	result, err := cl.appendMessage(mapedFile, msgExt, msgBytes)
	if err != nil {
		return nil, err
	}
	
	if result.Status != PUT_MESSAGE_OK {
		return nil, fmt.Errorf("failed to append message to new file, status: %v", result.Status)
	}
	
	// 更新写入位置
	atomic.StoreInt64(&cl.writePosition, result.WroteOffset+int64(result.WroteBytes))
	
	// 更新消息扩展信息
	msgExt.CommitLogOffset = result.WroteOffset
	msgExt.StoreSize = result.WroteBytes
	msgExt.MsgId = result.MsgId
	msgExt.StoreTimestamp = result.StoreTimestamp
	
	// 构造发送结果
	sendResult := &common.SendResult{
		SendStatus: common.SendOK,
		MsgId:      result.MsgId,
		MessageQueue: &common.MessageQueue{
			Topic:      msgExt.Topic,
			BrokerName: "broker-a", // 简化版本
			QueueId:    msgExt.QueueId,
		},
		QueueOffset: result.LogicsOffset,
	}
	
	return sendResult, nil
}

// serializeMessage 序列化消息
func (cl *CommitLog) serializeMessage(msgExt *common.MessageExt) ([]byte, error) {
	// 计算消息总长度
	topicLen := len(msgExt.Topic)
	bodyLen := len(msgExt.Body)
	propertiesLen := cl.calculatePropertiesLength(msgExt)
	
	// 消息总长度 = 固定头部 + topic长度 + body长度 + properties长度
	totalLen := 4 + // 总长度
		4 + // 魔数
		4 + // CRC
		4 + // 队列ID
		4 + // Flag
		8 + // 队列偏移量
		8 + // 物理偏移量
		4 + // 系统Flag
		8 + // 出生时间戳
		8 + // 出生主机
		8 + // 存储时间戳
		8 + // 存储主机
		4 + // 重试次数
		4 + // 事务偏移量
		1 + topicLen + // Topic
		2 + bodyLen + // Body
		2 + propertiesLen // Properties
	
	buf := make([]byte, totalLen)
	offset := 0
	
	// 写入总长度
	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += 4
	
	// 写入魔数
	binary.BigEndian.PutUint32(buf[offset:], MESSAGE_MAGIC_CODE)
	offset += 4
	
	// 写入CRC（暂时设为0）
	binary.BigEndian.PutUint32(buf[offset:], 0)
	offset += 4
	
	// 写入队列ID
	binary.BigEndian.PutUint32(buf[offset:], uint32(msgExt.QueueId))
	offset += 4
	
	// 写入Flag
	binary.BigEndian.PutUint32(buf[offset:], uint32(msgExt.SysFlag))
	offset += 4
	
	// 写入队列偏移量
	binary.BigEndian.PutUint64(buf[offset:], uint64(msgExt.QueueOffset))
	offset += 8
	
	// 写入物理偏移量（暂时设为0，将在写入时更新）
	binary.BigEndian.PutUint64(buf[offset:], 0)
	offset += 8
	
	// 写入系统Flag
	binary.BigEndian.PutUint32(buf[offset:], uint32(msgExt.SysFlag))
	offset += 4
	
	// 写入出生时间戳
	binary.BigEndian.PutUint64(buf[offset:], uint64(msgExt.BornTimestamp.UnixNano()/1000000))
	offset += 8
	
	// 写入出生主机（简化为8字节）
	binary.BigEndian.PutUint64(buf[offset:], 0)
	offset += 8
	
	// 写入存储时间戳
	binary.BigEndian.PutUint64(buf[offset:], uint64(time.Now().UnixNano()/1000000))
	offset += 8
	
	// 写入存储主机（简化为8字节）
	binary.BigEndian.PutUint64(buf[offset:], 0)
	offset += 8
	
	// 写入重试次数
	binary.BigEndian.PutUint32(buf[offset:], uint32(msgExt.ReconsumeTimes))
	offset += 4
	
	// 写入事务偏移量
	binary.BigEndian.PutUint32(buf[offset:], 0)
	offset += 4
	
	// 写入Topic
	buf[offset] = byte(topicLen)
	offset++
	copy(buf[offset:], msgExt.Topic)
	offset += topicLen
	
	// 写入Body
	binary.BigEndian.PutUint16(buf[offset:], uint16(bodyLen))
	offset += 2
	copy(buf[offset:], msgExt.Body)
	offset += bodyLen
	
	// 写入Properties
	propertiesBytes := cl.serializeProperties(msgExt)
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(propertiesBytes)))
	offset += 2
	copy(buf[offset:], propertiesBytes)
	
	return buf, nil
}

// calculatePropertiesLength 计算Properties长度
func (cl *CommitLog) calculatePropertiesLength(msgExt *common.MessageExt) int {
	propertiesBytes := cl.serializeProperties(msgExt)
	return len(propertiesBytes)
}

// serializeProperties 序列化Properties
func (cl *CommitLog) serializeProperties(msgExt *common.MessageExt) []byte {
	var result []byte
	
	// 添加Tags
	if msgExt.Tags != "" {
		result = append(result, []byte("TAGS")...)
		result = append(result, 1) // 分隔符
		result = append(result, []byte(msgExt.Tags)...)
		result = append(result, 2) // 分隔符
	}
	
	// 添加Keys
	if msgExt.Keys != "" {
		result = append(result, []byte("KEYS")...)
		result = append(result, 1) // 分隔符
		result = append(result, []byte(msgExt.Keys)...)
		result = append(result, 2) // 分隔符
	}
	
	// 添加其他Properties
	for key, value := range msgExt.Properties {
		result = append(result, []byte(key)...)
		result = append(result, 1) // 分隔符
		result = append(result, []byte(value)...)
		result = append(result, 2) // 分隔符
	}
	
	return result
}

// generateMessageId 生成消息ID
func (cl *CommitLog) generateMessageId(fileName string, offset int64) string {
	return fmt.Sprintf("%s_%d_%d", filepath.Base(fileName), offset, time.Now().UnixNano())
}

// GetMessage 根据偏移量获取消息
func (cl *CommitLog) GetMessage(offset int64, size int32) (*common.MessageExt, error) {
	if !cl.running {
		return nil, fmt.Errorf("commit log is not running")
	}
	
	// 根据偏移量找到对应的映射文件
	mapedFile := cl.mapedFileQueue.FindMapedFileByOffset(offset)
	if mapedFile == nil {
		return nil, fmt.Errorf("maped file not found for offset %d", offset)
	}
	
	// 计算文件内偏移量
	fileOffset := offset - mapedFile.GetFileFromOffset()
	
	// 读取消息数据
	msgBytes, err := mapedFile.SelectMappedBuffer(fileOffset, int64(size))
	if err != nil {
		return nil, fmt.Errorf("failed to read message data: %v", err)
	}
	
	// 反序列化消息
	msgExt, err := cl.deserializeMessage(msgBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %v", err)
	}
	
	return msgExt, nil
}

// deserializeMessage 反序列化消息
func (cl *CommitLog) deserializeMessage(data []byte) (*common.MessageExt, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("message data too short")
	}
	
	offset := 0
	
	// 读取总长度
	totalLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	
	if int(totalLen) != len(data) {
		return nil, fmt.Errorf("message length mismatch: expected %d, got %d", totalLen, len(data))
	}
	
	// 读取魔数
	magicCode := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if magicCode != MESSAGE_MAGIC_CODE {
		return nil, fmt.Errorf("invalid magic code: %x", magicCode)
	}
	
	// 跳过CRC
	offset += 4
	
	// 读取队列ID
	queueId := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	
	// 跳过Flag
	offset += 4
	
	// 读取队列偏移量
	queueOffset := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	
	// 读取物理偏移量
	physicOffset := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	
	// 读取系统Flag
	sysFlag := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	
	// 读取出生时间戳
	bornTimestamp := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	
	// 跳过出生主机
	offset += 8
	
	// 读取存储时间戳
	storeTimestamp := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	
	// 跳过存储主机
	offset += 8
	
	// 读取重试次数
	reconsumeTimes := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	
	// 跳过事务偏移量
	offset += 4
	
	// 读取Topic
	topicLen := int(data[offset])
	offset++
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	
	// 读取Body
	bodyLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	body := make([]byte, bodyLen)
	copy(body, data[offset:offset+int(bodyLen)])
	offset += int(bodyLen)
	
	// 读取Properties
	propertiesLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	properties := make(map[string]string)
	tags := ""
	keys := ""
	if propertiesLen > 0 {
		properties, tags, keys = cl.deserializeProperties(data[offset : offset+int(propertiesLen)])
	}
	
	// 构造消息扩展对象
	msgExt := &common.MessageExt{
		Message: &common.Message{
			Topic:      topic,
			Tags:       tags,
			Keys:       keys,
			Body:       body,
			Properties: properties,
		},
		QueueId:           int32(queueId),
		StoreSize:         int32(totalLen),
		QueueOffset:       int64(queueOffset),
		SysFlag:           int32(sysFlag),
		BornTimestamp:     time.Unix(0, int64(bornTimestamp)*1000000),
		StoreTimestamp:    time.Unix(0, int64(storeTimestamp)*1000000),
		ReconsumeTimes:    int32(reconsumeTimes),
		CommitLogOffset:   int64(physicOffset),
	}
	
	return msgExt, nil
}

// deserializeProperties 反序列化Properties
func (cl *CommitLog) deserializeProperties(data []byte) (map[string]string, string, string) {
	properties := make(map[string]string)
	tags := ""
	keys := ""
	
	offset := 0
	for offset < len(data) {
		// 查找key结束位置
		keyEnd := offset
		for keyEnd < len(data) && data[keyEnd] != 1 {
			keyEnd++
		}
		if keyEnd >= len(data) {
			break
		}
		
		key := string(data[offset:keyEnd])
		offset = keyEnd + 1
		
		// 查找value结束位置
		valueEnd := offset
		for valueEnd < len(data) && data[valueEnd] != 2 {
			valueEnd++
		}
		if valueEnd >= len(data) {
			break
		}
		
		value := string(data[offset:valueEnd])
		offset = valueEnd + 1
		
		// 处理特殊属性
		switch key {
		case "TAGS":
			tags = value
		case "KEYS":
			keys = value
		default:
			properties[key] = value
		}
	}
	
	return properties, tags, keys
}

// recoverWritePosition 恢复写入位置
func (cl *CommitLog) recoverWritePosition() {
	// 获取最后一个映射文件
	lastMapedFile := cl.mapedFileQueue.GetLastMapedFile()
	if lastMapedFile == nil {
		cl.writePosition = 0
		cl.flushedPosition = 0
		cl.committedPosition = 0
		return
	}
	
	// 恢复写入位置
	cl.writePosition = lastMapedFile.GetFileFromOffset() + lastMapedFile.GetWrotePosition()
	cl.flushedPosition = cl.writePosition
	cl.committedPosition = cl.writePosition
}

// flush 刷盘
func (cl *CommitLog) flush() bool {
	// 获取当前写入位置
	currentWritePosition := atomic.LoadInt64(&cl.writePosition)
	
	// 如果没有新数据，直接返回
	if currentWritePosition == cl.flushedPosition {
		return true
	}
	
	// 刷盘
	result := cl.mapedFileQueue.Flush(0)
	if result {
		cl.flushedPosition = currentWritePosition
	}
	
	return result
}

// GetMaxOffset 获取最大偏移量
func (cl *CommitLog) GetMaxOffset() int64 {
	return atomic.LoadInt64(&cl.writePosition)
}

// GetMinOffset 获取最小偏移量
func (cl *CommitLog) GetMinOffset() int64 {
	firstMapedFile := cl.mapedFileQueue.GetFirstMapedFile()
	if firstMapedFile == nil {
		return 0
	}
	return firstMapedFile.GetFileFromOffset()
}

// GetData 获取指定偏移量和大小的数据（用于HA复制）
func (cl *CommitLog) GetData(offset int64, size int32) ([]byte, error) {
	cl.mutex.RLock()
	defer cl.mutex.RUnlock()

	// 检查偏移量是否有效
	if offset < cl.GetMinOffset() || offset >= cl.GetMaxOffset() {
		return nil, fmt.Errorf("invalid offset: %d", offset)
	}

	// 查找对应的映射文件
	mapedFile := cl.mapedFileQueue.FindMapedFileByOffset(offset)
	if mapedFile == nil {
		return nil, fmt.Errorf("mapped file not found for offset: %d", offset)
	}

	// 计算文件内偏移量
	fileOffset := offset - mapedFile.GetFileFromOffset()

	// 确保不超过文件边界
	remainSize := mapedFile.GetFileSize() - fileOffset
	if int64(size) > remainSize {
		size = int32(remainSize)
	}

	// 读取数据
	data, err := mapedFile.SelectMappedBuffer(fileOffset, int64(size))
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %v", err)
	}

	return data, nil
}

// AppendData 追加数据（用于从节点接收主节点数据）
func (cl *CommitLog) AppendData(data []byte) (int64, error) {
	cl.putMessageLock.Lock()
	defer cl.putMessageLock.Unlock()

	if len(data) == 0 {
		return cl.writePosition, nil
	}

	// 获取当前写入文件
	mapedFile := cl.mapedFileQueue.GetLastMapedFile()
	if mapedFile == nil {
		// 创建新的映射文件
		newMapedFile, err := cl.mapedFileQueue.GetLastMapedFileOrCreate(cl.writePosition)
		if err != nil {
			return 0, fmt.Errorf("failed to create mapped file: %v", err)
		}
		mapedFile = newMapedFile
	}

	// 检查当前文件是否有足够空间
	if mapedFile.GetWrotePosition()+int64(len(data)) <= mapedFile.GetFileSize() {
		// 写入数据
		n, err := mapedFile.AppendMessage(data)
		if err != nil {
			return 0, fmt.Errorf("failed to write data: %v", err)
		}

		// 更新写入位置
		startOffset := mapedFile.GetFileFromOffset() + mapedFile.GetWrotePosition() - int64(n)
		atomic.StoreInt64(&cl.writePosition, mapedFile.GetFileFromOffset()+mapedFile.GetWrotePosition())

		return startOffset, nil
	} else {
		// 当前文件空间不足，创建新文件
		newMapedFile, err := cl.mapedFileQueue.GetLastMapedFileOrCreate(cl.writePosition)
		if err != nil {
			return 0, fmt.Errorf("failed to create new mapped file: %v", err)
		}

		// 写入数据到新文件
		_, err = newMapedFile.AppendMessage(data)
		if err != nil {
			return 0, fmt.Errorf("failed to write data to new file: %v", err)
		}

		// 更新写入位置
		startOffset := newMapedFile.GetFileFromOffset()
		atomic.StoreInt64(&cl.writePosition, newMapedFile.GetFileFromOffset()+newMapedFile.GetWrotePosition())

		return startOffset, nil
	}
}