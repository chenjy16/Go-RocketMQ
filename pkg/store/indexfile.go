package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// IndexFile头部大小
	INDEX_HEADER_SIZE = 40
	// 哈希槽数量
	HASH_SLOT_SIZE = 4
	// 索引单元大小
	INDEX_SIZE = 20
	// 无效索引
	INVALID_INDEX = 0
	// 默认哈希槽数量
	DEFAULT_HASH_SLOT_NUM = 5000000
	// 默认索引数量
	DEFAULT_INDEX_NUM = 20000000
)

// IndexHeader IndexFile头部
type IndexHeader struct {
	BeginTimestamp int64 // 开始时间戳
	EndTimestamp   int64 // 结束时间戳
	BeginPhyOffset int64 // 开始物理偏移量
	EndPhyOffset   int64 // 结束物理偏移量
	HashSlotCount  int32 // 哈希槽数量
	IndexCount     int32 // 索引数量
}

// IndexFile 索引文件
type IndexFile struct {
	fileName      string
	mapedFile     *MapedFile
	header        *IndexHeader
	hashSlotNum   int32
	indexNum      int32
	mutex         sync.RWMutex
	endPhyOffset  int64
	endTimestamp  int64
}

// NewIndexFile 创建新的IndexFile
func NewIndexFile(fileName string, hashSlotNum, indexNum int32) (*IndexFile, error) {
	if hashSlotNum <= 0 {
		hashSlotNum = DEFAULT_HASH_SLOT_NUM
	}
	if indexNum <= 0 {
		indexNum = DEFAULT_INDEX_NUM
	}
	
	// 计算文件大小
	fileSize := INDEX_HEADER_SIZE + int64(hashSlotNum)*HASH_SLOT_SIZE + int64(indexNum)*INDEX_SIZE
	
	// 创建映射文件
	mapedFile, err := NewMapedFile(fileName, fileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create maped file: %v", err)
	}
	
	indexFile := &IndexFile{
		fileName:    fileName,
		mapedFile:   mapedFile,
		header:      &IndexHeader{},
		hashSlotNum: hashSlotNum,
		indexNum:    indexNum,
	}
	
	// 加载或初始化头部
	if err := indexFile.loadHeader(); err != nil {
		return nil, fmt.Errorf("failed to load header: %v", err)
	}
	
	return indexFile, nil
}

// loadHeader 加载头部信息
func (idx *IndexFile) loadHeader() error {
	if idx.mapedFile.GetWrotePosition() == 0 {
		// 新文件，初始化头部
		idx.header = &IndexHeader{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			BeginPhyOffset: 0,
			EndPhyOffset:   0,
			HashSlotCount:  0,
			IndexCount:     0,
		}
		return idx.updateHeader()
	}
	
	// 读取现有头部
	data, err := idx.mapedFile.SelectMappedBuffer(0, INDEX_HEADER_SIZE)
	if err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}
	
	idx.header = &IndexHeader{
		BeginTimestamp: int64(binary.BigEndian.Uint64(data[0:8])),
		EndTimestamp:   int64(binary.BigEndian.Uint64(data[8:16])),
		BeginPhyOffset: int64(binary.BigEndian.Uint64(data[16:24])),
		EndPhyOffset:   int64(binary.BigEndian.Uint64(data[24:32])),
		HashSlotCount:  int32(binary.BigEndian.Uint32(data[32:36])),
		IndexCount:     int32(binary.BigEndian.Uint32(data[36:40])),
	}
	
	idx.endPhyOffset = idx.header.EndPhyOffset
	idx.endTimestamp = idx.header.EndTimestamp
	
	return nil
}

// updateHeader 更新头部信息
func (idx *IndexFile) updateHeader() error {
	data := make([]byte, INDEX_HEADER_SIZE)
	binary.BigEndian.PutUint64(data[0:8], uint64(idx.header.BeginTimestamp))
	binary.BigEndian.PutUint64(data[8:16], uint64(idx.header.EndTimestamp))
	binary.BigEndian.PutUint64(data[16:24], uint64(idx.header.BeginPhyOffset))
	binary.BigEndian.PutUint64(data[24:32], uint64(idx.header.EndPhyOffset))
	binary.BigEndian.PutUint32(data[32:36], uint32(idx.header.HashSlotCount))
	binary.BigEndian.PutUint32(data[36:40], uint32(idx.header.IndexCount))
	
	// 写入头部
	copy(idx.mapedFile.GetMappedByteBuffer()[0:INDEX_HEADER_SIZE], data)
	return nil
}

// PutKey 添加索引
func (idx *IndexFile) PutKey(key string, phyOffset int64, storeTimestamp int64) bool {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
	
	// 检查是否已满
	if idx.header.IndexCount >= idx.indexNum {
		return false
	}
	
	// 计算哈希值
	keyHash := idx.indexKeyHashMethod(key)
	slotPos := keyHash % uint32(idx.hashSlotNum)
	
	// 获取哈希槽位置
	slotOffset := INDEX_HEADER_SIZE + int64(slotPos)*HASH_SLOT_SIZE
	slotData, err := idx.mapedFile.SelectMappedBuffer(slotOffset, HASH_SLOT_SIZE)
	if err != nil {
		return false
	}
	
	// 读取槽中的索引位置
	slotValue := int32(binary.BigEndian.Uint32(slotData))
	
	// 计算新索引位置
	newIndexPos := idx.header.IndexCount
	indexOffset := INDEX_HEADER_SIZE + int64(idx.hashSlotNum)*HASH_SLOT_SIZE + int64(newIndexPos)*INDEX_SIZE
	
	// 写入索引数据
	indexData := make([]byte, INDEX_SIZE)
	binary.BigEndian.PutUint32(indexData[0:4], keyHash)                    // key hash
	binary.BigEndian.PutUint64(indexData[4:12], uint64(phyOffset))         // phyOffset
	binary.BigEndian.PutUint32(indexData[12:16], uint32(storeTimestamp))   // timeDiff
	binary.BigEndian.PutUint32(indexData[16:20], uint32(slotValue))        // slotValue
	
	// 写入索引
	copy(idx.mapedFile.GetMappedByteBuffer()[indexOffset:indexOffset+INDEX_SIZE], indexData)
	
	// 更新哈希槽
	slotData = make([]byte, HASH_SLOT_SIZE)
	binary.BigEndian.PutUint32(slotData, uint32(newIndexPos))
	copy(idx.mapedFile.GetMappedByteBuffer()[slotOffset:slotOffset+HASH_SLOT_SIZE], slotData)
	
	// 更新头部
	if idx.header.IndexCount == 0 {
		idx.header.BeginTimestamp = storeTimestamp
		idx.header.BeginPhyOffset = phyOffset
	}
	
	idx.header.EndTimestamp = storeTimestamp
	idx.header.EndPhyOffset = phyOffset
	idx.header.IndexCount++
	idx.header.HashSlotCount++
	
	idx.endPhyOffset = phyOffset
	idx.endTimestamp = storeTimestamp
	
	// 更新头部到文件
	idx.updateHeader()
	
	return true
}

// SelectPhyOffset 根据key查找物理偏移量
func (idx *IndexFile) SelectPhyOffset(keys []string, maxNum int32, begin int64, end int64) ([]int64, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	
	if idx.header.IndexCount <= 0 {
		return nil, nil
	}
	
	// 时间范围检查
	if end < idx.header.BeginTimestamp || begin > idx.header.EndTimestamp {
		return nil, nil
	}
	
	var phyOffsets []int64
	
	for _, key := range keys {
		if int32(len(phyOffsets)) >= maxNum {
			break
		}
		
		// 计算哈希值
		keyHash := idx.indexKeyHashMethod(key)
		slotPos := keyHash % uint32(idx.hashSlotNum)
		
		// 获取哈希槽位置
		slotOffset := INDEX_HEADER_SIZE + int64(slotPos)*HASH_SLOT_SIZE
		slotData, err := idx.mapedFile.SelectMappedBuffer(slotOffset, HASH_SLOT_SIZE)
		if err != nil {
			continue
		}
		
		// 读取槽中的索引位置
		slotValue := int32(binary.BigEndian.Uint32(slotData))
		
		// 遍历链表
		for slotValue > 0 && slotValue <= idx.header.IndexCount && int32(len(phyOffsets)) < maxNum {
			// 读取索引数据
			indexOffset := INDEX_HEADER_SIZE + int64(idx.hashSlotNum)*HASH_SLOT_SIZE + int64(slotValue-1)*INDEX_SIZE
			indexData, err := idx.mapedFile.SelectMappedBuffer(indexOffset, INDEX_SIZE)
			if err != nil {
				break
			}
			
			// 解析索引数据
			indexKeyHash := binary.BigEndian.Uint32(indexData[0:4])
			phyOffset := int64(binary.BigEndian.Uint64(indexData[4:12]))
			timeDiff := int64(binary.BigEndian.Uint32(indexData[12:16]))
			prevIndex := int32(binary.BigEndian.Uint32(indexData[16:20]))
			
			// 检查key是否匹配
			if indexKeyHash == keyHash {
				// 检查时间范围
				if timeDiff >= begin && timeDiff <= end {
					phyOffsets = append(phyOffsets, phyOffset)
				}
			}
			
			// 移动到下一个索引
			slotValue = prevIndex
		}
	}
	
	return phyOffsets, nil
}

// indexKeyHashMethod 计算key的哈希值
func (idx *IndexFile) indexKeyHashMethod(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// IsWriteFull 检查是否写满
func (idx *IndexFile) IsWriteFull() bool {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	return idx.header.IndexCount >= idx.indexNum
}

// IsTimeMatched 检查时间是否匹配
func (idx *IndexFile) IsTimeMatched(begin, end int64) bool {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	
	result := begin < idx.header.EndTimestamp && end > idx.header.BeginTimestamp
	return result
}

// GetBeginTimestamp 获取开始时间戳
func (idx *IndexFile) GetBeginTimestamp() int64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	return idx.header.BeginTimestamp
}

// GetEndTimestamp 获取结束时间戳
func (idx *IndexFile) GetEndTimestamp() int64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	return idx.header.EndTimestamp
}

// GetEndPhyOffset 获取结束物理偏移量
func (idx *IndexFile) GetEndPhyOffset() int64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	return idx.header.EndPhyOffset
}

// GetBeginPhyOffset 获取开始物理偏移量
func (idx *IndexFile) GetBeginPhyOffset() int64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	return idx.header.BeginPhyOffset
}

// GetIndexCount 获取索引数量
func (idx *IndexFile) GetIndexCount() int32 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	return idx.header.IndexCount
}

// GetFileName 获取文件名
func (idx *IndexFile) GetFileName() string {
	return idx.fileName
}

// Flush 刷盘
func (idx *IndexFile) Flush() bool {
	return idx.mapedFile.Flush(0)
}

// Destroy 销毁IndexFile
func (idx *IndexFile) Destroy(intervalForcibly int64) bool {
	return idx.mapedFile.Destroy(intervalForcibly)
}

// IndexService 索引服务
type IndexService struct {
	storePath     string
	hashSlotNum   int32
	indexNum      int32
	indexFileList []*IndexFile
	mutex         sync.RWMutex
	lastIndexFile *IndexFile
}

// NewIndexService 创建索引服务
func NewIndexService(storePath string) *IndexService {
	return &IndexService{
		storePath:     storePath,
		hashSlotNum:   DEFAULT_HASH_SLOT_NUM,
		indexNum:      DEFAULT_INDEX_NUM,
		indexFileList: make([]*IndexFile, 0),
	}
}

// Start 启动索引服务
func (is *IndexService) Start() error {
	// 创建索引目录
	if err := os.MkdirAll(is.storePath, 0755); err != nil {
		return fmt.Errorf("failed to create index path: %v", err)
	}
	
	// 加载现有索引文件
	return is.load()
}

// load 加载索引文件
func (is *IndexService) load() error {
	files, err := os.ReadDir(is.storePath)
	if err != nil {
		return fmt.Errorf("failed to read index directory: %v", err)
	}
	
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		
		fileName := filepath.Join(is.storePath, file.Name())
		indexFile, err := NewIndexFile(fileName, is.hashSlotNum, is.indexNum)
		if err != nil {
			continue
		}
		
		is.indexFileList = append(is.indexFileList, indexFile)
	}
	
	// 设置最后一个索引文件
	if len(is.indexFileList) > 0 {
		is.lastIndexFile = is.indexFileList[len(is.indexFileList)-1]
	}
	
	return nil
}

// BuildIndex 构建索引
func (is *IndexService) BuildIndex(key string, phyOffset int64, storeTimestamp int64) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	
	// 获取或创建索引文件
	indexFile := is.getAndCreateLastIndexFile()
	if indexFile == nil {
		return
	}
	
	// 添加索引
	if !indexFile.PutKey(key, phyOffset, storeTimestamp) {
		// 当前文件已满，创建新文件
		newIndexFile := is.createIndexFile()
		if newIndexFile != nil {
			newIndexFile.PutKey(key, phyOffset, storeTimestamp)
		}
	}
}

// getAndCreateLastIndexFile 获取或创建最后一个索引文件
func (is *IndexService) getAndCreateLastIndexFile() *IndexFile {
	if is.lastIndexFile == nil || is.lastIndexFile.IsWriteFull() {
		is.lastIndexFile = is.createIndexFile()
	}
	return is.lastIndexFile
}

// createIndexFile 创建索引文件
func (is *IndexService) createIndexFile() *IndexFile {
	fileName := filepath.Join(is.storePath, fmt.Sprintf("%020d", time.Now().UnixNano()))
	indexFile, err := NewIndexFile(fileName, is.hashSlotNum, is.indexNum)
	if err != nil {
		return nil
	}
	
	is.indexFileList = append(is.indexFileList, indexFile)
	return indexFile
}

// QueryOffset 查询偏移量
func (is *IndexService) QueryOffset(topic, key string, maxNum int32, begin, end int64) ([]int64, error) {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	
	var allOffsets []int64
	
	for _, indexFile := range is.indexFileList {
		if !indexFile.IsTimeMatched(begin, end) {
			continue
		}
		
		offsets, err := indexFile.SelectPhyOffset([]string{key}, maxNum, begin, end)
		if err != nil {
			continue
		}
		
		allOffsets = append(allOffsets, offsets...)
		if int32(len(allOffsets)) >= maxNum {
			break
		}
	}
	
	return allOffsets, nil
}

// Shutdown 关闭索引服务
func (is *IndexService) Shutdown() {
	for _, indexFile := range is.indexFileList {
		indexFile.Flush()
	}
}