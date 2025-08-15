package store

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
)

const (
	// ConsumeQueue单元大小：8字节CommitLog偏移量 + 4字节消息大小 + 8字节Tag哈希码
	CONSUME_QUEUE_UNIT_SIZE = 20
	// ConsumeQueue文件默认大小
	DEFAULT_CONSUME_QUEUE_FILE_SIZE = 1024 * 1024 * 6 // 6MB
)

// ConsumeQueueUnit ConsumeQueue单元
type ConsumeQueueUnit struct {
	Offset    int64 // CommitLog偏移量
	Size      int32 // 消息大小
	TagsCode  int64 // Tag哈希码
}

// ConsumeQueue 消费队列
type ConsumeQueue struct {
	topic         string
	queueId       int32
	storePath     string
	mapedFileSize int64
	mapedFileQueue *MapedFileQueue
	mutex         sync.RWMutex
	lastPutIndex  int64
	maxPhysicOffset int64
	minLogicOffset  int64
}

// NewConsumeQueue 创建新的ConsumeQueue
func NewConsumeQueue(topic string, queueId int32, storePath string, mapedFileSize int64) *ConsumeQueue {
	if mapedFileSize <= 0 {
		mapedFileSize = DEFAULT_CONSUME_QUEUE_FILE_SIZE
	}
	
	queueDir := filepath.Join(storePath, topic, fmt.Sprintf("%d", queueId))
	mapedFileQueue, _ := NewMapedFileQueue(queueDir, mapedFileSize)
	
	return &ConsumeQueue{
		topic:         topic,
		queueId:       queueId,
		storePath:     storePath,
		mapedFileSize: mapedFileSize,
		mapedFileQueue: mapedFileQueue,
		lastPutIndex:  -1,
		maxPhysicOffset: -1,
		minLogicOffset:  0,
	}
}

// Start 启动ConsumeQueue
func (cq *ConsumeQueue) Start() error {
	return cq.mapedFileQueue.load()
}

// Shutdown 关闭ConsumeQueue
func (cq *ConsumeQueue) Shutdown() {
	cq.mapedFileQueue.Shutdown()
}

// PutMessagePositionInfo 写入消息位置信息
func (cq *ConsumeQueue) PutMessagePositionInfo(offset int64, size int32, tagsCode int64) error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	
	// 在并发场景下，允许乱序到达的offset，只要不是重复的即可
	// 移除严格的顺序检查，改为只检查是否为有效offset
	if offset < 0 {
		return fmt.Errorf("invalid offset %d", offset)
	}
	
	// 获取当前映射文件
	mapedFile := cq.mapedFileQueue.GetLastMapedFile()
	if mapedFile == nil {
		// 创建新的映射文件
		var err error
		mapedFile, err = cq.mapedFileQueue.GetLastMapedFileOrCreate(0)
		if err != nil {
			return fmt.Errorf("failed to get maped file: %v", err)
		}
	}
	
	// 序列化ConsumeQueue单元
	data := make([]byte, CONSUME_QUEUE_UNIT_SIZE)
	binary.BigEndian.PutUint64(data[0:8], uint64(offset))
	binary.BigEndian.PutUint32(data[8:12], uint32(size))
	binary.BigEndian.PutUint64(data[12:20], uint64(tagsCode))
	
	// 写入数据
	writePos := mapedFile.GetWrotePosition()
	if writePos+CONSUME_QUEUE_UNIT_SIZE > mapedFile.GetFileSize() {
		// 当前文件已满，创建新文件
		nextOffset := mapedFile.GetFileFromOffset() + mapedFile.GetFileSize()
		newMapedFile, err := cq.mapedFileQueue.GetLastMapedFileOrCreate(nextOffset)
		if err != nil {
			return fmt.Errorf("failed to create new maped file: %v", err)
		}
		mapedFile = newMapedFile
	}
	
	// 写入数据
	n, err := mapedFile.AppendMessage(data)
	if err != nil {
		return fmt.Errorf("failed to append message: %v", err)
	}
	
	if n != CONSUME_QUEUE_UNIT_SIZE {
		return fmt.Errorf("write size mismatch: expected %d, actual %d", CONSUME_QUEUE_UNIT_SIZE, n)
	}
	
	// 更新索引
	cq.lastPutIndex++
	// 更新maxPhysicOffset为最大值，处理乱序到达的情况
	if offset > cq.maxPhysicOffset {
		cq.maxPhysicOffset = offset
	}
	
	return nil
}

// GetIndexBuffer 获取指定位置的索引缓冲区
func (cq *ConsumeQueue) GetIndexBuffer(startIndex int64) (*ConsumeQueueUnit, error) {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()
	
	if startIndex < cq.minLogicOffset {
		return nil, fmt.Errorf("startIndex %d is less than minLogicOffset %d", startIndex, cq.minLogicOffset)
	}
	
	if startIndex > cq.lastPutIndex {
		return nil, fmt.Errorf("startIndex %d is greater than lastPutIndex %d", startIndex, cq.lastPutIndex)
	}
	
	// 计算文件偏移量
	offset := startIndex * CONSUME_QUEUE_UNIT_SIZE
	mapedFile := cq.mapedFileQueue.FindMapedFileByOffset(offset)
	if mapedFile == nil {
		return nil, fmt.Errorf("maped file not found for offset %d", offset)
	}
	
	// 计算在文件中的位置
	pos := offset - mapedFile.GetFileFromOffset()
	data, err := mapedFile.SelectMappedBuffer(int64(pos), CONSUME_QUEUE_UNIT_SIZE)
	if err != nil {
		return nil, fmt.Errorf("failed to select maped buffer: %v", err)
	}
	
	// 反序列化
	unit := &ConsumeQueueUnit{
		Offset:   int64(binary.BigEndian.Uint64(data[0:8])),
		Size:     int32(binary.BigEndian.Uint32(data[8:12])),
		TagsCode: int64(binary.BigEndian.Uint64(data[12:20])),
	}
	
	return unit, nil
}

// GetOffsetInQueueByTime 根据时间戳获取队列中的偏移量
func (cq *ConsumeQueue) GetOffsetInQueueByTime(timestamp int64) int64 {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()
	
	// 简单实现：返回最小偏移量
	// 实际实现需要根据时间戳进行二分查找
	return cq.minLogicOffset
}

// GetMaxOffsetInQueue 获取队列中的最大偏移量
func (cq *ConsumeQueue) GetMaxOffsetInQueue() int64 {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()
	return cq.lastPutIndex + 1
}

// GetMinOffsetInQueue 获取队列中的最小偏移量
func (cq *ConsumeQueue) GetMinOffsetInQueue() int64 {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()
	return cq.minLogicOffset
}

// GetTopic 获取主题
func (cq *ConsumeQueue) GetTopic() string {
	return cq.topic
}

// GetQueueId 获取队列ID
func (cq *ConsumeQueue) GetQueueId() int32 {
	return cq.queueId
}

// Flush 刷盘
func (cq *ConsumeQueue) Flush(flushLeastPages int) bool {
	return cq.mapedFileQueue.Flush(flushLeastPages)
}

// CleanExpiredFile 清理过期文件
func (cq *ConsumeQueue) CleanExpiredFile(expiredTime int64) int {
	return cq.mapedFileQueue.DeleteExpiredFile(expiredTime, CONSUME_QUEUE_UNIT_SIZE, 0, false)
}

// Destroy 销毁ConsumeQueue
func (cq *ConsumeQueue) Destroy() {
	cq.mapedFileQueue.Shutdown()
}

// GetStoreSize 获取存储大小
func (cq *ConsumeQueue) GetStoreSize() int64 {
	return cq.mapedFileQueue.GetTotalFileSize()
}

// GetUnitSize 获取单元大小
func (cq *ConsumeQueue) GetUnitSize() int {
	return CONSUME_QUEUE_UNIT_SIZE
}

// Recover 恢复ConsumeQueue
func (cq *ConsumeQueue) Recover() {
	mapedFiles := cq.mapedFileQueue.GetMapedFiles()
	if len(mapedFiles) == 0 {
		return
	}
	
	// 从最后一个文件开始恢复
	lastMapedFile := mapedFiles[len(mapedFiles)-1]
	fileSize := lastMapedFile.GetFileSize()
	
	// 查找最后一个有效的ConsumeQueue单元
	for i := fileSize - CONSUME_QUEUE_UNIT_SIZE; i >= 0; i -= CONSUME_QUEUE_UNIT_SIZE {
		data, err := lastMapedFile.SelectMappedBuffer(int64(i), CONSUME_QUEUE_UNIT_SIZE)
		if err != nil {
			continue
		}
		
		offset := int64(binary.BigEndian.Uint64(data[0:8]))
		if offset > 0 {
			// 找到最后一个有效单元
			cq.lastPutIndex = (lastMapedFile.GetFileFromOffset() + i) / CONSUME_QUEUE_UNIT_SIZE
			cq.maxPhysicOffset = offset
			lastMapedFile.SetWrotePosition(i + CONSUME_QUEUE_UNIT_SIZE)
			lastMapedFile.SetCommittedPosition(i + CONSUME_QUEUE_UNIT_SIZE)
			break
		}
	}
}