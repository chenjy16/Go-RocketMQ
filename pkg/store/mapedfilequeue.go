package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// MapedFileQueue 映射文件队列
type MapedFileQueue struct {
	// 存储目录
	storePath string
	
	// 映射文件大小
	mapedFileSize int64
	
	// 映射文件列表
	mapedFiles []*MapedFile
	
	// 读写锁
	mutex sync.RWMutex
	
	// 刷盘检查点
	flushedWhere int64
	committedWhere int64
	
	// 存储检查点
	storeTimestamp int64
}

// NewMapedFileQueue 创建映射文件队列
func NewMapedFileQueue(storePath string, mapedFileSize int64) (*MapedFileQueue, error) {
	// 创建存储目录
	if err := os.MkdirAll(storePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create store path %s: %v", storePath, err)
	}
	
	mfq := &MapedFileQueue{
		storePath:     storePath,
		mapedFileSize: mapedFileSize,
		mapedFiles:    make([]*MapedFile, 0),
	}
	
	// 加载现有文件
	if err := mfq.load(); err != nil {
		return nil, fmt.Errorf("failed to load maped files: %v", err)
	}
	
	return mfq, nil
}

// load 加载现有的映射文件
func (mfq *MapedFileQueue) load() error {
	// 读取目录中的文件
	files, err := os.ReadDir(mfq.storePath)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %v", mfq.storePath, err)
	}
	
	// 过滤并排序文件
	var fileNames []string
	for _, file := range files {
		if !file.IsDir() && mfq.isMapedFile(file.Name()) {
			fileNames = append(fileNames, file.Name())
		}
	}
	
	// 按文件名排序（文件名包含偏移量）
	sort.Strings(fileNames)
	
	// 加载映射文件
	for _, fileName := range fileNames {
		filePath := filepath.Join(mfq.storePath, fileName)
		offset, err := mfq.parseOffsetFromFileName(fileName)
		if err != nil {
			fmt.Printf("Warning: failed to parse offset from file name %s: %v\n", fileName, err)
			continue
		}
		
		mapedFile, err := NewMapedFileWithOffset(filePath, mfq.mapedFileSize, offset)
		if err != nil {
			fmt.Printf("Warning: failed to load maped file %s: %v\n", filePath, err)
			continue
		}
		
		mfq.mapedFiles = append(mfq.mapedFiles, mapedFile)
	}
	
	return nil
}

// isMapedFile 检查是否是映射文件
func (mfq *MapedFileQueue) isMapedFile(fileName string) bool {
	// 简单检查：文件名应该是数字（偏移量）
	_, err := strconv.ParseInt(fileName, 10, 64)
	return err == nil
}

// parseOffsetFromFileName 从文件名解析偏移量
func (mfq *MapedFileQueue) parseOffsetFromFileName(fileName string) (int64, error) {
	return strconv.ParseInt(fileName, 10, 64)
}

// GetLastMapedFile 获取最后一个映射文件
func (mfq *MapedFileQueue) GetLastMapedFile() *MapedFile {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()
	
	if len(mfq.mapedFiles) == 0 {
		return nil
	}
	
	return mfq.mapedFiles[len(mfq.mapedFiles)-1]
}

// GetLastMapedFileOrCreate 获取最后一个映射文件，如果不存在或已满则创建新文件
func (mfq *MapedFileQueue) GetLastMapedFileOrCreate(startOffset int64) (*MapedFile, error) {
	mfq.mutex.Lock()
	defer mfq.mutex.Unlock()
	
	// 检查最后一个文件
	if len(mfq.mapedFiles) > 0 {
		lastFile := mfq.mapedFiles[len(mfq.mapedFiles)-1]
		if !lastFile.IsFull() {
			return lastFile, nil
		}
	}
	
	// 创建新文件
	return mfq.createMapedFile(startOffset)
}

// GetFirstMapedFile 获取第一个映射文件
func (mfq *MapedFileQueue) GetFirstMapedFile() *MapedFile {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()
	
	if len(mfq.mapedFiles) == 0 {
		return nil
	}
	
	return mfq.mapedFiles[0]
}

// FindMapedFileByOffset 根据偏移量查找映射文件
func (mfq *MapedFileQueue) FindMapedFileByOffset(offset int64) *MapedFile {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()
	
	for _, mapedFile := range mfq.mapedFiles {
		fileStartOffset := mapedFile.GetFileFromOffset()
		fileEndOffset := fileStartOffset + mfq.mapedFileSize
		
		if offset >= fileStartOffset && offset < fileEndOffset {
			return mapedFile
		}
	}
	
	return nil
}

// FindMapedFileByOffset 根据偏移量查找映射文件（返回索引）
func (mfq *MapedFileQueue) FindMapedFileByOffsetWithIndex(offset int64, returnFirstOnNotFound bool) (*MapedFile, int) {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()
	
	for i, mapedFile := range mfq.mapedFiles {
		fileStartOffset := mapedFile.GetFileFromOffset()
		fileEndOffset := fileStartOffset + mfq.mapedFileSize
		
		if offset >= fileStartOffset && offset < fileEndOffset {
			return mapedFile, i
		}
	}
	
	if returnFirstOnNotFound && len(mfq.mapedFiles) > 0 {
		return mfq.mapedFiles[0], 0
	}
	
	return nil, -1
}

// createMapedFile 创建新的映射文件
func (mfq *MapedFileQueue) createMapedFile(startOffset int64) (*MapedFile, error) {
	// 计算文件偏移量（对齐到文件大小）
	fileOffset := startOffset - (startOffset % mfq.mapedFileSize)
	
	// 生成文件名
	fileName := fmt.Sprintf("%020d", fileOffset)
	filePath := filepath.Join(mfq.storePath, fileName)
	
	// 创建映射文件
	mapedFile, err := NewMapedFileWithOffset(filePath, mfq.mapedFileSize, fileOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to create maped file %s: %v", filePath, err)
	}
	
	// 添加到列表
	mfq.mapedFiles = append(mfq.mapedFiles, mapedFile)
	
	return mapedFile, nil
}

// Flush 刷盘
func (mfq *MapedFileQueue) Flush(flushLeastPages int) bool {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()
	
	result := true
	for _, mapedFile := range mfq.mapedFiles {
		if !mapedFile.Flush(flushLeastPages) {
			result = false
		}
	}
	
	return result
}

// Commit 提交（用于异步刷盘）
func (mfq *MapedFileQueue) Commit(commitLeastPages int) bool {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()
	
	result := true
	for _, mapedFile := range mfq.mapedFiles {
		if !mapedFile.Commit(commitLeastPages) {
			result = false
		}
	}
	
	return result
}

// GetMapedFiles 获取所有映射文件
func (mfq *MapedFileQueue) GetMapedFiles() []*MapedFile {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()
	
	// 返回副本
	result := make([]*MapedFile, len(mfq.mapedFiles))
	copy(result, mfq.mapedFiles)
	return result
}

// GetMapedFileSize 获取映射文件大小
func (mfq *MapedFileQueue) GetMapedFileSize() int64 {
	return mfq.mapedFileSize
}

// GetTotalFileSize 获取总文件大小
func (mfq *MapedFileQueue) GetTotalFileSize() int64 {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()
	
	return int64(len(mfq.mapedFiles)) * mfq.mapedFileSize
}

// GetMinOffset 获取最小偏移量
func (mfq *MapedFileQueue) GetMinOffset() int64 {
	firstFile := mfq.GetFirstMapedFile()
	if firstFile == nil {
		return 0
	}
	return firstFile.GetFileFromOffset()
}

// GetMaxOffset 获取最大偏移量
func (mfq *MapedFileQueue) GetMaxOffset() int64 {
	lastFile := mfq.GetLastMapedFile()
	if lastFile == nil {
		return 0
	}
	return lastFile.GetFileFromOffset() + lastFile.GetWrotePosition()
}

// GetMaxWrotePosition 获取最大写入位置
func (mfq *MapedFileQueue) GetMaxWrotePosition() int64 {
	lastFile := mfq.GetLastMapedFile()
	if lastFile == nil {
		return 0
	}
	return lastFile.GetFileFromOffset() + lastFile.GetWrotePosition()
}

// GetRemainingBytes 获取剩余字节数
func (mfq *MapedFileQueue) GetRemainingBytes() int64 {
	lastFile := mfq.GetLastMapedFile()
	if lastFile == nil {
		return 0
	}
	return mfq.mapedFileSize - lastFile.GetWrotePosition()
}

// DeleteLastMapedFile 删除最后一个映射文件
func (mfq *MapedFileQueue) DeleteLastMapedFile() {
	mfq.mutex.Lock()
	defer mfq.mutex.Unlock()
	
	if len(mfq.mapedFiles) == 0 {
		return
	}
	
	// 获取最后一个文件
	lastFile := mfq.mapedFiles[len(mfq.mapedFiles)-1]
	
	// 销毁文件
	lastFile.Destroy(1000)
	
	// 从列表中移除
	mfq.mapedFiles = mfq.mapedFiles[:len(mfq.mapedFiles)-1]
}

// DeleteExpiredFile 删除过期文件
func (mfq *MapedFileQueue) DeleteExpiredFile(expiredTime int64, deleteFilesInterval int, destroyMapedFileIntervalForcibly int64, cleanImmediately bool) int {
	mfq.mutex.Lock()
	defer mfq.mutex.Unlock()
	
	deleteCount := 0
	filesToDelete := make([]*MapedFile, 0)
	
	// 查找需要删除的文件
	for _, mapedFile := range mfq.mapedFiles {
		// 检查文件是否过期
		if mfq.isFileExpired(mapedFile, expiredTime) {
			filesToDelete = append(filesToDelete, mapedFile)
			if len(filesToDelete) >= deleteFilesInterval {
				break
			}
		} else {
			break // 文件是按时间顺序的，后面的文件不会过期
		}
	}
	
	// 删除文件
	for _, mapedFile := range filesToDelete {
		if mapedFile.Destroy(destroyMapedFileIntervalForcibly) {
			// 从列表中移除
			mfq.removeMapedFile(mapedFile)
			deleteCount++
			
			// 删除物理文件
			if err := os.Remove(mapedFile.GetFileName()); err != nil {
				fmt.Printf("Warning: failed to delete file %s: %v\n", mapedFile.GetFileName(), err)
			}
		}
	}
	
	return deleteCount
}

// isFileExpired 检查文件是否过期
func (mfq *MapedFileQueue) isFileExpired(mapedFile *MapedFile, expiredTime int64) bool {
	// 获取文件修改时间
	fileInfo, err := os.Stat(mapedFile.GetFileName())
	if err != nil {
		return true // 如果无法获取文件信息，认为已过期
	}
	
	return time.Now().Unix()-fileInfo.ModTime().Unix() > expiredTime
}

// removeMapedFile 从列表中移除映射文件
func (mfq *MapedFileQueue) removeMapedFile(mapedFile *MapedFile) {
	for i, mf := range mfq.mapedFiles {
		if mf == mapedFile {
			// 移除元素
			mfq.mapedFiles = append(mfq.mapedFiles[:i], mfq.mapedFiles[i+1:]...)
			break
		}
	}
}

// Shutdown 关闭映射文件队列
func (mfq *MapedFileQueue) Shutdown() {
	mfq.mutex.Lock()
	defer mfq.mutex.Unlock()
	
	// 关闭所有映射文件
	for _, mapedFile := range mfq.mapedFiles {
		mapedFile.Destroy(1000)
	}
	
	// 清空列表
	mfq.mapedFiles = nil
}

// RetryDeleteFirstFile 重试删除第一个文件
func (mfq *MapedFileQueue) RetryDeleteFirstFile(intervalForcibly int64) bool {
	mfq.mutex.Lock()
	defer mfq.mutex.Unlock()
	
	if len(mfq.mapedFiles) == 0 {
		return true
	}
	
	firstFile := mfq.mapedFiles[0]
	if firstFile.Destroy(intervalForcibly) {
		// 从列表中移除
		mfq.mapedFiles = mfq.mapedFiles[1:]
		
		// 删除物理文件
		if err := os.Remove(firstFile.GetFileName()); err != nil {
			fmt.Printf("Warning: failed to delete file %s: %v\n", firstFile.GetFileName(), err)
		}
		
		return true
	}
	
	return false
}

// String 返回字符串表示
func (mfq *MapedFileQueue) String() string {
	return fmt.Sprintf("MapedFileQueue{storePath='%s', mapedFileSize=%d, fileCount=%d}",
		mfq.storePath, mfq.mapedFileSize, len(mfq.mapedFiles))
}