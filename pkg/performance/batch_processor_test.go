package performance

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// BatchError 批量处理错误
type BatchError struct {
	Message string
	Items   []interface{}
}

func (e *BatchError) Error() string {
	return e.Message
}

func (e *BatchError) GetFailedItems() []interface{} {
	return e.Items
}

// MockBatchHandler 模拟批量处理器
type MockBatchHandler struct {
	processedBatches [][]interface{}
	processDelay     time.Duration
	errorOnBatch     int // 在第几个批次返回错误
	currentBatch     int
	mutex            sync.Mutex
}

func (m *MockBatchHandler) Process(items []interface{}) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.currentBatch++
	if m.errorOnBatch > 0 && m.currentBatch == m.errorOnBatch {
		return &BatchError{Message: "mock error", Items: items}
	}
	
	if m.processDelay > 0 {
		time.Sleep(m.processDelay)
	}
	
	m.processedBatches = append(m.processedBatches, items)
	return nil
}

func (m *MockBatchHandler) GetProcessedBatches() [][]interface{} {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.processedBatches
}

func (m *MockBatchHandler) Reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.processedBatches = nil
	m.currentBatch = 0
}

// TestNewBatchProcessor 测试批量处理器创建
func TestNewBatchProcessor(t *testing.T) {
	handler := &MockBatchHandler{}
	config := BatchConfig{
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MaxRetries:    3,
		RetryDelay:    50 * time.Millisecond,
		BufferSize:    1000,
	}
	
	processor := NewBatchProcessor(config, handler)
	if processor == nil {
		t.Fatal("NewBatchProcessor should not return nil")
	}
	
	if processor.batchSize != config.BatchSize {
		t.Errorf("Expected batch size %d, got %d", config.BatchSize, processor.batchSize)
	}
	if processor.flushInterval != config.FlushInterval {
		t.Errorf("Expected flush interval %v, got %v", config.FlushInterval, processor.flushInterval)
	}
	if processor.processor != handler {
		t.Error("Handler should be set correctly")
	}
	if processor.metrics == nil {
		t.Error("Metrics should be initialized")
	}
}

// TestBatchProcessorAdd 测试添加项目
func TestBatchProcessorAdd(t *testing.T) {
	handler := &MockBatchHandler{}
	config := BatchConfig{
		BatchSize:     3,
		FlushInterval: 1 * time.Second,
		BufferSize:    10,
	}
	
	processor := NewBatchProcessor(config, handler)
	
	err := processor.Start()
	if err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Stop()
	
	// 添加项目，但不足以触发批量处理
	err = processor.Add("item1")
	if err != nil {
		t.Errorf("Add should not return error: %v", err)
	}
	
	err = processor.Add("item2")
	if err != nil {
		t.Errorf("Add should not return error: %v", err)
	}
	
	// 等待一小段时间，确保没有处理
	time.Sleep(50 * time.Millisecond)
	batches := handler.GetProcessedBatches()
	if len(batches) != 0 {
		t.Errorf("Expected 0 processed batches, got %d", len(batches))
	}
	
	// 添加第三个项目，应该触发批量处理
	err = processor.Add("item3")
	if err != nil {
		t.Errorf("Add should not return error: %v", err)
	}
	
	// 等待处理完成
	time.Sleep(100 * time.Millisecond)
	batches = handler.GetProcessedBatches()
	if len(batches) != 1 {
		t.Errorf("Expected 1 processed batch, got %d", len(batches))
	}
	if len(batches[0]) != 3 {
		t.Errorf("Expected batch size 3, got %d", len(batches[0]))
	}
}

// TestBatchProcessorFlushInterval 测试刷新间隔
func TestBatchProcessorFlushInterval(t *testing.T) {
	handler := &MockBatchHandler{}
	config := BatchConfig{
		BatchSize:     10, // 大批量大小，不会被触发
		FlushInterval: 100 * time.Millisecond,
		BufferSize:    10,
	}
	
	processor := NewBatchProcessor(config, handler)
	
	err := processor.Start()
	if err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Stop()
	
	// 添加少量项目
	processor.Add("item1")
	processor.Add("item2")
	
	// 等待刷新间隔
	time.Sleep(150 * time.Millisecond)
	
	// 应该已经处理了
	batches := handler.GetProcessedBatches()
	if len(batches) != 1 {
		t.Errorf("Expected 1 processed batch, got %d", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Errorf("Expected batch size 2, got %d", len(batches[0]))
	}
}

// TestBatchProcessorConcurrentAdd 测试并发添加
func TestBatchProcessorConcurrentAdd(t *testing.T) {
	handler := &MockBatchHandler{}
	config := BatchConfig{
		BatchSize:     5,
		FlushInterval: 100 * time.Millisecond,
		BufferSize:    1000,
	}
	
	processor := NewBatchProcessor(config, handler)
	
	err := processor.Start()
	if err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Stop()
	
	const numGoroutines = 10
	const itemsPerGoroutine = 20
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				item := fmt.Sprintf("item-%d-%d", goroutineID, j)
				err := processor.Add(item)
				if err != nil {
					t.Errorf("Add failed: %v", err)
					return
				}
			}
		}(i)
	}
	
	wg.Wait()
	
	// 等待所有项目被处理
	time.Sleep(200 * time.Millisecond)
	
	// 检查所有项目都被处理了
	batches := handler.GetProcessedBatches()
	totalProcessed := 0
	for _, batch := range batches {
		totalProcessed += len(batch)
	}
	
	expectedTotal := numGoroutines * itemsPerGoroutine
	if totalProcessed != expectedTotal {
		t.Errorf("Expected %d total processed items, got %d", expectedTotal, totalProcessed)
	}
}

// TestBatchProcessorMetrics 测试指标收集
func TestBatchProcessorMetrics(t *testing.T) {
	handler := &MockBatchHandler{}
	config := BatchConfig{
		BatchSize:     2,
		FlushInterval: 50 * time.Millisecond,
		BufferSize:    10,
	}
	
	processor := NewBatchProcessor(config, handler)
	
	err := processor.Start()
	if err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Stop()
	
	// 初始指标
	metrics := processor.GetMetrics()
	if metrics.TotalItems != 0 {
		t.Error("Initial TotalItems should be 0")
	}
	if metrics.TotalBatches != 0 {
		t.Error("Initial TotalBatches should be 0")
	}
	
	// 添加项目
	processor.Add("item1")
	processor.Add("item2")
	
	// 等待处理
	time.Sleep(100 * time.Millisecond)
	
	// 检查指标
	metrics = processor.GetMetrics()
	if metrics.TotalItems != 2 {
		t.Errorf("Expected TotalItems 2, got %d", metrics.TotalItems)
	}
	if metrics.TotalBatches != 1 {
		t.Errorf("Expected TotalBatches 1, got %d", metrics.TotalBatches)
	}
	if metrics.SuccessfulItems != 2 {
		t.Errorf("Expected SuccessfulItems 2, got %d", metrics.SuccessfulItems)
	}
}

// TestBatchProcessorErrorHandling 测试错误处理
func TestBatchProcessorErrorHandling(t *testing.T) {
	handler := &MockBatchHandler{
		errorOnBatch: 1, // 第一个批次返回错误
	}
	config := BatchConfig{
		BatchSize:     2,
		FlushInterval: 50 * time.Millisecond,
		MaxRetries:    2,
		RetryDelay:    10 * time.Millisecond,
		BufferSize:    10,
	}
	
	processor := NewBatchProcessor(config, handler)
	
	err := processor.Start()
	if err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Stop()
	
	// 添加项目
	processor.Add("item1")
	processor.Add("item2")
	
	// 等待处理和重试
	time.Sleep(200 * time.Millisecond)
	
	// 检查指标
	metrics := processor.GetMetrics()
	if metrics.FailedItems == 0 {
		t.Error("Expected some failed items")
	}
	if metrics.TotalBatches == 0 {
		t.Error("Expected some processed batches")
	}
}

// TestBatchProcessorStop 测试停止处理器
func TestBatchProcessorStop(t *testing.T) {
	handler := &MockBatchHandler{}
	config := BatchConfig{
		BatchSize:     10,
		FlushInterval: 1 * time.Second,
		BufferSize:    10,
	}
	
	processor := NewBatchProcessor(config, handler)
	
	err := processor.Start()
	if err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	
	// 添加一些项目
	processor.Add("item1")
	processor.Add("item2")
	
	// 停止处理器
	processor.Stop()
	
	// 应该处理剩余的项目
	time.Sleep(50 * time.Millisecond)
	batches := handler.GetProcessedBatches()
	if len(batches) != 1 {
		t.Errorf("Expected 1 batch after stop, got %d", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Errorf("Expected 2 items in final batch, got %d", len(batches[0]))
	}
	
	// 停止后添加项目应该失败
	err = processor.Add("item3")
	if err == nil {
		t.Error("Add should fail after processor is stopped")
	}
}

// TestBatchProcessorBufferFull 测试缓冲区满
func TestBatchProcessorBufferFull(t *testing.T) {
	handler := &MockBatchHandler{
		processDelay: 100 * time.Millisecond, // 慢处理
	}
	config := BatchConfig{
		BatchSize:     10,
		FlushInterval: 1 * time.Second,
		BufferSize:    5, // 小缓冲区
	}
	
	processor := NewBatchProcessor(config, handler)
	
	err := processor.Start()
	if err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Stop()
	
	// 快速添加超过缓冲区大小的项目
	for i := 0; i < 10; i++ {
		err := processor.Add(fmt.Sprintf("item%d", i))
		if i >= 5 && err == nil {
			t.Errorf("Expected error when buffer is full at item %d", i)
		}
	}
}

// TestBatchError 测试批量错误
func TestBatchError(t *testing.T) {
	items := []interface{}{"item1", "item2"}
	err := &BatchError{
		Message: "test error",
		Items:   items,
	}
	
	if err.Error() != "test error" {
		t.Errorf("Expected error message 'test error', got '%s'", err.Error())
	}
	
	if len(err.GetFailedItems()) != 2 {
		t.Errorf("Expected 2 failed items, got %d", len(err.GetFailedItems()))
	}
}

// TestBatchHandlerFunc 测试函数式批量处理器
func TestBatchHandlerFunc(t *testing.T) {
	processedItems := make([]interface{}, 0)
	var mutex sync.Mutex
	
	handlerFunc := BatchHandlerFunc(func(items []interface{}) error {
		mutex.Lock()
		defer mutex.Unlock()
		processedItems = append(processedItems, items...)
		return nil
	})
	
	config := BatchConfig{
		BatchSize:     3,
		FlushInterval: 50 * time.Millisecond,
		BufferSize:    10,
	}
	
	processor := NewBatchProcessor(config, handlerFunc)
	
	err := processor.Start()
	if err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Stop()
	
	// 添加项目
	processor.Add("item1")
	processor.Add("item2")
	processor.Add("item3")
	
	// 等待处理
	time.Sleep(100 * time.Millisecond)
	
	// 检查结果
	mutex.Lock()
	defer mutex.Unlock()
	if len(processedItems) != 3 {
		t.Errorf("Expected 3 processed items, got %d", len(processedItems))
	}
}