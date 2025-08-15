package store

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// MapedFile 内存映射文件
type MapedFile struct {
	// 文件信息
	fileName     string
	fileSize     int64
	file         *os.File
	mappedBuffer []byte
	
	// 位置信息
	fileFromOffset int64 // 文件起始偏移量
	wrotePosition  int64 // 写入位置
	committedPosition int64 // 提交位置
	flushedPosition   int64 // 刷盘位置
	
	// 引用计数
	referenceCount int64
	
	// 锁
	mutex sync.RWMutex
	
	// 是否可用
	available bool
	
	// 第一次创建时间
	firstCreateInQueue bool
}

// NewMapedFile 创建内存映射文件
func NewMapedFile(fileName string, fileSize int64) (*MapedFile, error) {
	// 创建文件
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", fileName, err)
	}
	
	// 设置文件大小
	if err := file.Truncate(fileSize); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to truncate file %s: %v", fileName, err)
	}
	
	// 创建内存映射
	mappedBuffer, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), 
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file %s: %v", fileName, err)
	}
	
	mapedFile := &MapedFile{
		fileName:           fileName,
		fileSize:           fileSize,
		file:               file,
		mappedBuffer:       mappedBuffer,
		fileFromOffset:     0,
		wrotePosition:      0,
		committedPosition:  0,
		flushedPosition:    0,
		referenceCount:     1,
		available:          true,
		firstCreateInQueue: true,
	}
	
	return mapedFile, nil
}

// NewMapedFileWithOffset 创建带偏移量的内存映射文件
func NewMapedFileWithOffset(fileName string, fileSize int64, fileFromOffset int64) (*MapedFile, error) {
	mapedFile, err := NewMapedFile(fileName, fileSize)
	if err != nil {
		return nil, err
	}
	
	mapedFile.fileFromOffset = fileFromOffset
	return mapedFile, nil
}

// GetFileName 获取文件名
func (mf *MapedFile) GetFileName() string {
	return mf.fileName
}

// GetFileSize 获取文件大小
func (mf *MapedFile) GetFileSize() int64 {
	return mf.fileSize
}

// GetFileFromOffset 获取文件起始偏移量
func (mf *MapedFile) GetFileFromOffset() int64 {
	return mf.fileFromOffset
}

// GetWrotePosition 获取写入位置
func (mf *MapedFile) GetWrotePosition() int64 {
	return atomic.LoadInt64(&mf.wrotePosition)
}

// SetWrotePosition 设置写入位置
func (mf *MapedFile) SetWrotePosition(pos int64) {
	atomic.StoreInt64(&mf.wrotePosition, pos)
}

// GetCommittedPosition 获取提交位置
func (mf *MapedFile) GetCommittedPosition() int64 {
	return atomic.LoadInt64(&mf.committedPosition)
}

// SetCommittedPosition 设置提交位置
func (mf *MapedFile) SetCommittedPosition(pos int64) {
	atomic.StoreInt64(&mf.committedPosition, pos)
}

// GetFlushedPosition 获取刷盘位置
func (mf *MapedFile) GetFlushedPosition() int64 {
	return atomic.LoadInt64(&mf.flushedPosition)
}

// SetFlushedPosition 设置刷盘位置
func (mf *MapedFile) SetFlushedPosition(pos int64) {
	atomic.StoreInt64(&mf.flushedPosition, pos)
}

// IsFull 检查文件是否已满
func (mf *MapedFile) IsFull() bool {
	return mf.GetWrotePosition() >= mf.fileSize
}

// IsAvailable 检查文件是否可用
func (mf *MapedFile) IsAvailable() bool {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	return mf.available
}

// Hold 增加引用计数
func (mf *MapedFile) Hold() bool {
	if !mf.IsAvailable() {
		return false
	}
	atomic.AddInt64(&mf.referenceCount, 1)
	return true
}

// Release 减少引用计数
func (mf *MapedFile) Release() bool {
	refCount := atomic.AddInt64(&mf.referenceCount, -1)
	if refCount <= 0 && !mf.IsAvailable() {
		// 可以清理资源
		return true
	}
	return false
}

// AppendMessage 追加消息
func (mf *MapedFile) AppendMessage(data []byte) (int, error) {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()
	
	if !mf.available {
		return 0, fmt.Errorf("maped file is not available")
	}
	
	currentPos := mf.GetWrotePosition()
	if currentPos+int64(len(data)) > mf.fileSize {
		return 0, fmt.Errorf("not enough space in maped file")
	}
	
	// 写入数据到内存映射区域
	copy(mf.mappedBuffer[currentPos:], data)
	
	// 更新写入位置
	mf.SetWrotePosition(currentPos + int64(len(data)))
	
	return len(data), nil
}

// SelectMappedBuffer 选择映射缓冲区
func (mf *MapedFile) SelectMappedBuffer(pos int64, size int64) ([]byte, error) {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	
	if !mf.available {
		return nil, fmt.Errorf("maped file is not available")
	}
	
	if pos < 0 || pos >= mf.fileSize {
		return nil, fmt.Errorf("invalid position: %d", pos)
	}
	
	if pos+size > mf.fileSize {
		size = mf.fileSize - pos
	}
	
	// 返回数据副本
	result := make([]byte, size)
	copy(result, mf.mappedBuffer[pos:pos+size])
	
	return result, nil
}

// Flush 刷盘
func (mf *MapedFile) Flush(flushLeastPages int) bool {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()
	
	if !mf.available {
		return false
	}
	
	currentWritePos := mf.GetWrotePosition()
	currentFlushPos := mf.GetFlushedPosition()
	
	// 检查是否需要刷盘
	if flushLeastPages > 0 {
		pageSize := int64(4096) // 4KB页面大小
		dirtyPages := (currentWritePos - currentFlushPos) / pageSize
		if dirtyPages < int64(flushLeastPages) {
			return false
		}
	}
	
	// 如果没有新数据需要刷盘
	if currentWritePos == currentFlushPos {
		return true
	}
	
	// 执行刷盘操作
	if err := unix.Msync(mf.mappedBuffer, unix.MS_SYNC); err != nil {
		fmt.Printf("Warning: failed to msync file %s: %v\n", mf.fileName, err)
		return false
	}
	
	// 更新刷盘位置
	mf.SetFlushedPosition(currentWritePos)
	
	return true
}

// Commit 提交（用于异步刷盘）
func (mf *MapedFile) Commit(commitLeastPages int) bool {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()
	
	if !mf.available {
		return false
	}
	
	currentWritePos := mf.GetWrotePosition()
	currentCommitPos := mf.GetCommittedPosition()
	
	// 检查是否需要提交
	if commitLeastPages > 0 {
		pageSize := int64(4096) // 4KB页面大小
		dirtyPages := (currentWritePos - currentCommitPos) / pageSize
		if dirtyPages < int64(commitLeastPages) {
			return false
		}
	}
	
	// 如果没有新数据需要提交
	if currentWritePos == currentCommitPos {
		return true
	}
	
	// 更新提交位置
	mf.SetCommittedPosition(currentWritePos)
	
	return true
}

// Destroy 销毁映射文件
func (mf *MapedFile) Destroy(intervalForcibly int64) bool {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()
	
	// 标记为不可用
	mf.available = false
	
	// 检查引用计数
	if atomic.LoadInt64(&mf.referenceCount) > 0 {
		return false
	}
	
	// 取消内存映射
	if mf.mappedBuffer != nil {
		if err := syscall.Munmap(mf.mappedBuffer); err != nil {
			fmt.Printf("Warning: failed to munmap file %s: %v\n", mf.fileName, err)
		}
		mf.mappedBuffer = nil
	}
	
	// 关闭文件
	if mf.file != nil {
		if err := mf.file.Close(); err != nil {
			fmt.Printf("Warning: failed to close file %s: %v\n", mf.fileName, err)
		}
		mf.file = nil
	}
	
	return true
}

// Cleanup 清理文件
func (mf *MapedFile) Cleanup(currentRef int64) bool {
	if mf.IsAvailable() {
		return false
	}
	
	if atomic.LoadInt64(&mf.referenceCount) == currentRef {
		return mf.Destroy(1000)
	}
	
	return false
}

// WarmMappedFile 预热映射文件
func (mf *MapedFile) WarmMappedFile(flushDiskType FlushDiskType, pages int) {
	if !mf.IsAvailable() {
		return
	}
	
	// 简化的预热实现：访问每个页面
	pageSize := int64(4096)
	for i := int64(0); i < mf.fileSize; i += pageSize {
		if i < int64(len(mf.mappedBuffer)) {
			// 读取一个字节来触发页面加载
			_ = mf.mappedBuffer[i]
		}
	}
	
	// 如果是同步刷盘，立即刷盘
	if flushDiskType == SYNC_FLUSH {
		mf.Flush(0)
	}
}

// GetMappedByteBuffer 获取映射字节缓冲区（用于零拷贝）
func (mf *MapedFile) GetMappedByteBuffer() []byte {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	
	if !mf.available {
		return nil
	}
	
	return mf.mappedBuffer
}

// SliceByteBuffer 切片字节缓冲区
func (mf *MapedFile) SliceByteBuffer() []byte {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	
	if !mf.available {
		return nil
	}
	
	currentPos := mf.GetWrotePosition()
	return mf.mappedBuffer[:currentPos]
}

// GetFileChannel 获取文件通道（Go中返回文件描述符）
func (mf *MapedFile) GetFileChannel() *os.File {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	
	if !mf.available {
		return nil
	}
	
	return mf.file
}

// String 返回字符串表示
func (mf *MapedFile) String() string {
	return fmt.Sprintf("MapedFile{fileName='%s', fileSize=%d, wrotePosition=%d, flushedPosition=%d, available=%t}",
		mf.fileName, mf.fileSize, mf.GetWrotePosition(), mf.GetFlushedPosition(), mf.available)
}

// 辅助函数：将指针转换为uintptr（用于内存映射）
func bytesToUintptr(b []byte) uintptr {
	return uintptr(unsafe.Pointer(&b[0]))
}