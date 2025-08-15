package testutil

import (
	"sync"
	"testing"
	"time"
)

// TestTestEnvironment 测试TestEnvironment的基本功能
func TestTestEnvironment(t *testing.T) {
	env := NewTestEnvironment(t)
	
	// 验证临时目录创建
	AssertTrue(t, env.TempDir != "", "TempDir should not be empty")
	
	// 验证清理函数注册
	env.AddCleanup(func() {
		// 清理函数
	})
	
	AssertEqual(t, 1, len(env.CleanupFns), "Should have one cleanup function")
}

// TestAssertFunctions 测试断言函数
func TestAssertFunctions(t *testing.T) {
	// 这些测试不会失败，只是验证函数存在且可调用
	
	// 测试AssertEqual
	AssertEqual(t, 1, 1, "Numbers should be equal")
	
	// 测试AssertNotEqual
	AssertNotEqual(t, 1, 2, "Numbers should not be equal")
	
	// 测试AssertTrue
	AssertTrue(t, true, "Should be true")
	
	// 测试AssertFalse
	AssertFalse(t, false, "Should be false")
}

// TestCreateTestMessage 测试消息创建函数
func TestCreateTestMessage(t *testing.T) {
	msg := CreateTestMessage("test-topic", "test-body")
	
	AssertNotEqual(t, nil, msg, "Message should not be nil")
	AssertEqual(t, "test-topic", msg.Topic, "Topic should match")
	AssertEqual(t, "test-body", string(msg.Body), "Body should match")
}

// TestCreateTestMessages 测试批量消息创建
func TestCreateTestMessages(t *testing.T) {
	messages := CreateTestMessages("test-topic", 5)
	
	AssertEqual(t, 5, len(messages), "Should create 5 messages")
	
	for i, msg := range messages {
		AssertEqual(t, "test-topic", msg.Topic, "Topic should match")
		expectedBody := "Test message " + string(rune('0'+i))
		AssertEqual(t, expectedBody, string(msg.Body), "Body should match")
	}
}

// TestGetFreePort 测试端口分配
func TestGetFreePort(t *testing.T) {
	port1 := GetFreePort()
	port2 := GetFreePort()
	
	AssertTrue(t, port1 >= 10000 && port1 <= 60000, "Port should be in valid range")
	AssertTrue(t, port2 >= 10000 && port2 <= 60000, "Port should be in valid range")
	
	// 注意：由于是随机分配，不能保证两次调用返回不同的端口
}

// TestMockHandler 测试模拟处理器
func TestMockHandler(t *testing.T) {
	processed := false
	handler := NewMockHandler(func(items []interface{}) error {
		processed = true
		return nil
	})
	
	AssertEqual(t, 0, handler.GetCallCount(), "Initial call count should be 0")
	
	err := handler.Process([]interface{}{"test"})
	AssertNoError(t, err, "Process should not return error")
	AssertEqual(t, 1, handler.GetCallCount(), "Call count should be 1")
	AssertTrue(t, processed, "Process function should be called")
	
	handler.ResetCallCount()
	AssertEqual(t, 0, handler.GetCallCount(), "Call count should be reset to 0")
}

// TestWaitForCondition 测试条件等待
func TestWaitForCondition(t *testing.T) {
	var mu sync.Mutex
	conditionMet := false
	
	// 启动一个goroutine在100ms后设置条件
	go func() {
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		conditionMet = true
		mu.Unlock()
	}()
	
	// 等待条件满足
	WaitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return conditionMet
	}, 1*time.Second, "Condition should be met")
	
	mu.Lock()
	result := conditionMet
	mu.Unlock()
	AssertTrue(t, result, "Condition should be met after wait")
}