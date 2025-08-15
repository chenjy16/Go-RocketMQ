package performance

import (
	"sync"
	"time"
	"unsafe"
)

// MemoryPool 内存池管理器
type MemoryPool struct {
	bufferPools map[int]*sync.Pool // 不同大小的缓冲区池
	objectPools map[string]*sync.Pool // 对象池
	metrics     *PoolMetrics
	mutex       sync.RWMutex
}

// PoolMetrics 内存池指标
type PoolMetrics struct {
	BufferAllocations   int64 // 缓冲区分配次数
	BufferDeallocations int64 // 缓冲区释放次数
	ObjectAllocations   int64 // 对象分配次数
	ObjectDeallocations int64 // 对象释放次数
	PoolHits           int64 // 池命中次数
	PoolMisses         int64 // 池未命中次数
	TotalMemoryUsed    int64 // 总内存使用量
	mutex              sync.RWMutex
}

// BufferPool 缓冲区池
type BufferPool struct {
	pool     *sync.Pool
	size     int
	maxItems int
	current  int
	mutex    sync.Mutex
}

// ObjectPool 对象池
type ObjectPool struct {
	pool     *sync.Pool
	typeName string
	factory  func() interface{}
	reset    func(interface{})
	maxItems int
	current  int
	mutex    sync.Mutex
}

// 预定义的缓冲区大小
var (
	DefaultBufferSizes = []int{
		64,    // 小消息
		256,   // 中等消息
		1024,  // 大消息
		4096,  // 超大消息
		16384, // 批量消息
		65536, // 大批量消息
	}
)

// NewMemoryPool 创建内存池管理器
func NewMemoryPool() *MemoryPool {
	mp := &MemoryPool{
		bufferPools: make(map[int]*sync.Pool),
		objectPools: make(map[string]*sync.Pool),
		metrics:     &PoolMetrics{},
	}
	
	// 初始化缓冲区池
	for _, size := range DefaultBufferSizes {
		mp.initBufferPool(size)
	}
	
	return mp
}

// initBufferPool 初始化指定大小的缓冲区池
func (mp *MemoryPool) initBufferPool(size int) {
	mp.bufferPools[size] = &sync.Pool{
		New: func() interface{} {
			mp.metrics.incrementPoolMisses()
			return make([]byte, size)
		},
	}
}

// GetBuffer 获取指定大小的缓冲区
func (mp *MemoryPool) GetBuffer(size int) []byte {
	// 找到最合适的缓冲区大小
	poolSize := mp.findBestPoolSize(size)
	
	mp.mutex.RLock()
	pool, exists := mp.bufferPools[poolSize]
	mp.mutex.RUnlock()
	
	if !exists {
		// 如果没有合适的池，创建新的
		mp.mutex.Lock()
		mp.initBufferPool(poolSize)
		pool = mp.bufferPools[poolSize]
		mp.mutex.Unlock()
	}
	
	buf := pool.Get().([]byte)
	mp.metrics.incrementBufferAllocations()
	mp.metrics.incrementPoolHits()
	
	// 如果缓冲区太大，截取到需要的大小
	if len(buf) > size {
		buf = buf[:size]
	}
	
	return buf
}

// PutBuffer 归还缓冲区到池中
func (mp *MemoryPool) PutBuffer(buf []byte) {
	if buf == nil || len(buf) == 0 {
		return
	}
	
	// 重置缓冲区内容
	for i := range buf {
		buf[i] = 0
	}
	
	// 找到对应的池
	poolSize := cap(buf)
	mp.mutex.RLock()
	pool, exists := mp.bufferPools[poolSize]
	mp.mutex.RUnlock()
	
	if exists {
		pool.Put(buf)
		mp.metrics.incrementBufferDeallocations()
	}
}

// findBestPoolSize 找到最合适的池大小
func (mp *MemoryPool) findBestPoolSize(size int) int {
	for _, poolSize := range DefaultBufferSizes {
		if poolSize >= size {
			return poolSize
		}
	}
	
	// 如果没有合适的，创建一个稍大的
	return ((size + 1023) / 1024) * 1024 // 向上取整到1KB的倍数
}

// RegisterObjectPool 注册对象池
func (mp *MemoryPool) RegisterObjectPool(typeName string, factory func() interface{}, reset func(interface{})) {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()
	
	mp.objectPools[typeName] = &sync.Pool{
		New: func() interface{} {
			mp.metrics.incrementPoolMisses()
			return factory()
		},
	}
}

// GetObject 获取对象
func (mp *MemoryPool) GetObject(typeName string) interface{} {
	mp.mutex.RLock()
	pool, exists := mp.objectPools[typeName]
	mp.mutex.RUnlock()
	
	if !exists {
		return nil
	}
	
	obj := pool.Get()
	mp.metrics.incrementObjectAllocations()
	mp.metrics.incrementPoolHits()
	
	return obj
}

// PutObject 归还对象到池中
func (mp *MemoryPool) PutObject(typeName string, obj interface{}, reset func(interface{})) {
	if obj == nil {
		return
	}
	
	// 重置对象状态
	if reset != nil {
		reset(obj)
	}
	
	mp.mutex.RLock()
	pool, exists := mp.objectPools[typeName]
	mp.mutex.RUnlock()
	
	if exists {
		pool.Put(obj)
		mp.metrics.incrementObjectDeallocations()
	}
}

// GetMetrics 获取内存池指标
func (mp *MemoryPool) GetMetrics() *PoolMetrics {
	return mp.metrics
}

// 指标操作方法
func (pm *PoolMetrics) incrementBufferAllocations() {
	pm.mutex.Lock()
	pm.BufferAllocations++
	pm.mutex.Unlock()
}

func (pm *PoolMetrics) incrementBufferDeallocations() {
	pm.mutex.Lock()
	pm.BufferDeallocations++
	pm.mutex.Unlock()
}

func (pm *PoolMetrics) incrementObjectAllocations() {
	pm.mutex.Lock()
	pm.ObjectAllocations++
	pm.mutex.Unlock()
}

func (pm *PoolMetrics) incrementObjectDeallocations() {
	pm.mutex.Lock()
	pm.ObjectDeallocations++
	pm.mutex.Unlock()
}

func (pm *PoolMetrics) incrementPoolHits() {
	pm.mutex.Lock()
	pm.PoolHits++
	pm.mutex.Unlock()
}

func (pm *PoolMetrics) incrementPoolMisses() {
	pm.mutex.Lock()
	pm.PoolMisses++
	pm.mutex.Unlock()
}

// GetStats 获取统计信息
func (pm *PoolMetrics) GetStats() map[string]int64 {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()
	
	return map[string]int64{
		"buffer_allocations":   pm.BufferAllocations,
		"buffer_deallocations": pm.BufferDeallocations,
		"object_allocations":   pm.ObjectAllocations,
		"object_deallocations": pm.ObjectDeallocations,
		"pool_hits":           pm.PoolHits,
		"pool_misses":         pm.PoolMisses,
		"total_memory_used":   pm.TotalMemoryUsed,
		"hit_rate":            pm.getHitRate(),
	}
}

// getHitRate 计算命中率
func (pm *PoolMetrics) getHitRate() int64 {
	total := pm.PoolHits + pm.PoolMisses
	if total == 0 {
		return 0
	}
	return (pm.PoolHits * 100) / total
}

// ZeroCopyBuffer 零拷贝缓冲区
type ZeroCopyBuffer struct {
	data []byte
	ref  int32
}

// NewZeroCopyBuffer 创建零拷贝缓冲区
func NewZeroCopyBuffer(data []byte) *ZeroCopyBuffer {
	return &ZeroCopyBuffer{
		data: data,
		ref:  1,
	}
}

// Data 获取数据
func (zcb *ZeroCopyBuffer) Data() []byte {
	return zcb.data
}

// Size 获取大小
func (zcb *ZeroCopyBuffer) Size() int {
	return len(zcb.data)
}

// Slice 切片操作（零拷贝）
func (zcb *ZeroCopyBuffer) Slice(start, end int) []byte {
	if start < 0 || end > len(zcb.data) || start > end {
		return nil
	}
	return zcb.data[start:end]
}

// UnsafeString 零拷贝转换为字符串
func (zcb *ZeroCopyBuffer) UnsafeString() string {
	return *(*string)(unsafe.Pointer(&zcb.data))
}

// MessagePool 消息对象池
type MessagePool struct {
	pool *sync.Pool
}

// NewMessagePool 创建消息池
func NewMessagePool() *MessagePool {
	return &MessagePool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &Message{}
			},
		},
	}
}

// Message 消息结构（简化版）
type Message struct {
	Topic      string
	Body       []byte
	Properties map[string]string
	Tags       string
	Keys       string
}

// Get 获取消息对象
func (mp *MessagePool) Get() *Message {
	msg := mp.pool.Get().(*Message)
	// 重置消息状态
	msg.reset()
	return msg
}

// Put 归还消息对象
func (mp *MessagePool) Put(msg *Message) {
	if msg != nil {
		mp.pool.Put(msg)
	}
}

// reset 重置消息状态
func (m *Message) reset() {
	m.Topic = ""
	m.Body = m.Body[:0]
	m.Tags = ""
	m.Keys = ""
	if m.Properties != nil {
		for k := range m.Properties {
			delete(m.Properties, k)
		}
	} else {
		m.Properties = make(map[string]string)
	}
}

// 全局内存池实例
var (
	GlobalMemoryPool *MemoryPool
	GlobalMessagePool *MessagePool
	once             sync.Once
)

// InitGlobalPools 初始化全局内存池
func InitGlobalPools() {
	once.Do(func() {
		GlobalMemoryPool = NewMemoryPool()
		GlobalMessagePool = NewMessagePool()
		
		// 注册常用对象池
		GlobalMemoryPool.RegisterObjectPool("Message", 
			func() interface{} { return &Message{Properties: make(map[string]string)} },
			func(obj interface{}) { obj.(*Message).reset() })
		
		GlobalMemoryPool.RegisterObjectPool("MessageExt",
			func() interface{} { return &MessageExt{} },
			func(obj interface{}) { obj.(*MessageExt).reset() })
	})
}

// MessageExt 扩展消息结构（简化版）
type MessageExt struct {
	Message
	MsgId       string
	QueueId     int32
	QueueOffset int64
	BornTime    time.Time
	StoreTime   time.Time
}

// reset 重置扩展消息状态
func (me *MessageExt) reset() {
	me.Message.reset()
	me.MsgId = ""
	me.QueueId = 0
	me.QueueOffset = 0
	me.BornTime = time.Time{}
	me.StoreTime = time.Time{}
}

// GetBuffer 全局获取缓冲区
func GetBuffer(size int) []byte {
	InitGlobalPools()
	return GlobalMemoryPool.GetBuffer(size)
}

// PutBuffer 全局归还缓冲区
func PutBuffer(buf []byte) {
	InitGlobalPools()
	GlobalMemoryPool.PutBuffer(buf)
}

// GetMessage 全局获取消息对象
func GetMessage() *Message {
	InitGlobalPools()
	return GlobalMessagePool.Get()
}

// PutMessage 全局归还消息对象
func PutMessage(msg *Message) {
	InitGlobalPools()
	GlobalMessagePool.Put(msg)
}