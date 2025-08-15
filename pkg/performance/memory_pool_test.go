package performance

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestNewMemoryPool 测试内存池创建
func TestNewMemoryPool(t *testing.T) {
	pool := NewMemoryPool()
	if pool == nil {
		t.Fatal("NewMemoryPool should not return nil")
	}
	if pool.bufferPools == nil {
		t.Error("bufferPools should be initialized")
	}
	if pool.objectPools == nil {
		t.Error("objectPools should be initialized")
	}
	if pool.metrics == nil {
		t.Error("metrics should be initialized")
	}
}

// TestMemoryPoolGetPutBuffer 测试缓冲区获取和归还
func TestMemoryPoolGetPutBuffer(t *testing.T) {
	pool := NewMemoryPool()
	
	tests := []struct {
		name string
		size int
	}{
		{"small buffer", 64},
		{"medium buffer", 1024},
		{"large buffer", 16384},
		{"extra large buffer", 65536},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := pool.GetBuffer(tt.size)
			if buf == nil {
				t.Errorf("GetBuffer(%d) should not return nil", tt.size)
			}
			if cap(buf) < tt.size {
				t.Errorf("Buffer capacity %d should be at least %d", cap(buf), tt.size)
			}
			
			// 测试归还缓冲区
			pool.PutBuffer(buf)
			
			// 再次获取，应该复用
			buf2 := pool.GetBuffer(tt.size)
			if buf2 == nil {
				t.Errorf("Second GetBuffer(%d) should not return nil", tt.size)
			}
			pool.PutBuffer(buf2)
		})
	}
}

// TestMessagePool 测试消息池
func TestMessagePool(t *testing.T) {
	msgPool := NewMessagePool()
	
	msg := msgPool.Get()
	if msg == nil {
		t.Fatal("Get should not return nil")
	}
	
	// 设置消息属性
	msg.Topic = "TestTopic"
	msg.Body = []byte("test message")
	msg.Tags = "tag1"
	
	// 归还消息
	msgPool.Put(msg)
	
	// 再次获取，应该复用且已重置
	msg2 := msgPool.Get()
	if msg2 == nil {
		t.Fatal("Second Get should not return nil")
	}
	if msg2.Topic != "" {
		t.Error("Reused message should have empty topic")
	}
	if len(msg2.Body) != 0 {
		t.Error("Reused message should have empty body")
	}
	if msg2.Tags != "" {
		t.Error("Reused message should have empty tags")
	}
	
	msgPool.Put(msg2)
}

// TestMemoryPoolConcurrentAccess 测试并发访问
func TestMemoryPoolConcurrentAccess(t *testing.T) {
	pool := NewMemoryPool()
	const numGoroutines = 100
	const numOperations = 1000
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	// 并发测试缓冲区操作
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				buf := pool.GetBuffer(1024)
				if buf == nil {
					t.Errorf("GetBuffer should not return nil")
					return
				}
				pool.PutBuffer(buf)
			}
		}()
	}
	
	wg.Wait()
	
	// 并发测试消息操作
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				msg := GetMessage()
				if msg == nil {
					t.Errorf("GetMessage should not return nil")
					return
				}
				PutMessage(msg)
			}
		}()
	}
	
	wg.Wait()
}

// TestMemoryPoolMetrics 测试内存池指标
func TestMemoryPoolMetrics(t *testing.T) {
	pool := NewMemoryPool()
	
	// 初始指标应该为0
	metrics := pool.GetMetrics()
	if metrics.BufferAllocations != 0 {
		t.Error("Initial BufferAllocations should be 0")
	}
	if metrics.BufferDeallocations != 0 {
		t.Error("Initial BufferDeallocations should be 0")
	}
	if metrics.ObjectAllocations != 0 {
		t.Error("Initial ObjectAllocations should be 0")
	}
	
	// 执行一些操作
	buf := pool.GetBuffer(1024)
	msg := GetMessage()
	
	// 检查指标更新
	metrics = pool.GetMetrics()
	if metrics.BufferAllocations == 0 {
		t.Error("BufferAllocations should be greater than 0")
	}
	
	// 归还对象
	pool.PutBuffer(buf)
	PutMessage(msg)
	
	// 检查释放指标
	metrics = pool.GetMetrics()
	if metrics.BufferDeallocations == 0 {
		t.Error("BufferDeallocations should be greater than 0")
	}
}

// TestMemoryPoolStats 测试内存池统计
func TestMemoryPoolStats(t *testing.T) {
	pool := NewMemoryPool()
	
	// 执行一些操作
	pool.GetBuffer(1024)
	GetMessage()
	
	// 检查统计信息
	metrics := pool.GetMetrics()
	stats := metrics.GetStats()
	if stats == nil {
		t.Error("GetStats should not return nil")
	}
	
	// 检查基本统计信息存在
	if len(stats) == 0 {
		t.Error("Stats should not be empty")
	}
	
	// 验证统计信息的基本结构
	for key, value := range stats {
		if key == "" {
			t.Error("Stats key should not be empty")
		}
		if value < 0 {
			t.Errorf("Stats value for key %s should not be negative, got %d", key, value)
		}
	}
}

// TestMemoryPoolGCPressure 测试GC压力
func TestMemoryPoolGCPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping GC pressure test in short mode")
	}
	
	pool := NewMemoryPool()
	
	// 记录初始GC统计
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	
	// 使用内存池进行大量操作
	for i := 0; i < 10000; i++ {
		buf := pool.GetBuffer(1024)
		msg := GetMessage()
		pool.PutBuffer(buf)
		PutMessage(msg)
	}
	
	// 记录结束GC统计
	runtime.GC()
	runtime.ReadMemStats(&m2)
	
	// GC次数应该相对较少
	gcCount := m2.NumGC - m1.NumGC
	t.Logf("GC count during memory pool operations: %d", gcCount)
	
	// 与直接分配进行对比
	runtime.GC()
	runtime.ReadMemStats(&m1)
	
	// 直接分配相同数量的对象
	for i := 0; i < 10000; i++ {
		_ = make([]byte, 1024)
		_ = &Message{}
	}
	
	runtime.GC()
	runtime.ReadMemStats(&m2)
	
	directGCCount := m2.NumGC - m1.NumGC
	t.Logf("GC count during direct allocation: %d", directGCCount)
	
	// 内存池应该产生更少的GC压力
	if gcCount > directGCCount {
		t.Logf("Warning: Memory pool GC count (%d) is higher than direct allocation (%d)", gcCount, directGCCount)
	}
}

// TestMemoryPoolEdgeCases 测试边界情况
func TestMemoryPoolEdgeCases(t *testing.T) {
	pool := NewMemoryPool()
	
	// 测试零大小缓冲区
	buf := pool.GetBuffer(0)
	if buf == nil {
		t.Error("GetBuffer(0) should not return nil")
	}
	pool.PutBuffer(buf)
	
	// 测试非常大的缓冲区
	buf = pool.GetBuffer(1024 * 1024) // 1MB
	if buf == nil {
		t.Error("GetBuffer(1MB) should not return nil")
	}
	pool.PutBuffer(buf)
	
	// 测试归还nil缓冲区（应该不会panic）
	pool.PutBuffer(nil)
	
	// 测试归还nil消息（应该不会panic）
	PutMessage(nil)
	
	// 测试正常大小范围
	for _, size := range []int{1, 10, 100, 1000, 10000} {
		buf := pool.GetBuffer(size)
		if buf == nil {
			t.Errorf("GetBuffer(%d) should not return nil", size)
		}
		if cap(buf) < size {
			t.Errorf("Buffer capacity %d should be at least %d", cap(buf), size)
		}
		pool.PutBuffer(buf)
	}
}

// TestMemoryPoolPerformance 测试内存池性能
func TestMemoryPoolPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}
	
	pool := NewMemoryPool()
	const iterations = 100000
	
	// 测试内存池性能
	start := time.Now()
	for i := 0; i < iterations; i++ {
		buf := pool.GetBuffer(1024)
		pool.PutBuffer(buf)
	}
	poolDuration := time.Since(start)
	
	// 测试直接分配性能
	start = time.Now()
	for i := 0; i < iterations; i++ {
		_ = make([]byte, 1024)
	}
	directDuration := time.Since(start)
	
	t.Logf("Memory pool duration: %v", poolDuration)
	t.Logf("Direct allocation duration: %v", directDuration)
	t.Logf("Performance ratio: %.2fx", float64(directDuration)/float64(poolDuration))
	
	// 内存池应该更快（至少不会慢太多）
	if poolDuration > directDuration*2 {
		t.Logf("Warning: Memory pool is significantly slower than direct allocation")
	}
}

// TestGlobalMemoryPool 测试全局内存池
func TestGlobalMemoryPool(t *testing.T) {
	// 初始化全局内存池
	InitGlobalPools()
	
	// 测试全局函数
	buf := GetBuffer(1024)
	if buf == nil {
		t.Error("GetBuffer should not return nil")
	}
	PutBuffer(buf)
	
	msg := GetMessage()
	if msg == nil {
		t.Error("GetMessage should not return nil")
	}
	PutMessage(msg)
	
	// 测试全局内存池访问
	if GlobalMemoryPool == nil {
		t.Error("GlobalMemoryPool should be initialized")
	}
	if GlobalMessagePool == nil {
		t.Error("GlobalMessagePool should be initialized")
	}
}