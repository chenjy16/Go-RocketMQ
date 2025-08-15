package client

import (
	"fmt"
	"sync"
	"testing"
)

// TestACLIntegrationBasic 基本ACL集成测试
func TestACLIntegrationBasic(t *testing.T) {
	// 创建ACL中间件
	aclMiddleware := NewACLMiddleware("test_access_key", "test_secret_key_12345", HmacSHA256, true)

	// 验证ACL配置
	config := aclMiddleware.GetConfig()
	if config.AccessKey != "test_access_key" {
		t.Errorf("Expected AccessKey test_access_key, got %s", config.AccessKey)
	}

	// 测试认证头生成
	headers, err := aclMiddleware.GenerateAuthHeaders("test_topic", "test_consumer_group", "")
	if err != nil {
		t.Fatalf("Failed to generate auth headers: %v", err)
	}

	// 验证必要的认证头
	requiredHeaders := []string{"AccessKey", "Signature", "Timestamp", "SignatureMethod"}
	for _, header := range requiredHeaders {
		if _, exists := headers[header]; !exists {
			t.Errorf("Missing required header: %s", header)
		}
	}

	// 测试认证头验证
	err = aclMiddleware.VerifyAuthHeaders(headers, "test_topic", "test_consumer_group", "")
	if err != nil {
		t.Errorf("Auth headers verification failed: %v", err)
	}

	t.Log("Basic ACL integration test passed successfully")
}

// BenchmarkACLSignatureGeneration ACL签名生成性能测试
func BenchmarkACLSignatureGeneration(b *testing.B) {
	aclUtils := NewACLUtils("test_key", "test_secret_key_12345", HmacSHA256)
	requestData := map[string]string{
		"topic":         "test_topic",
		"consumerGroup": "test_group",
		"timestamp":     fmt.Sprintf("%d", GenerateTimestamp()),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := aclUtils.GenerateSignature(requestData)
		if err != nil {
			b.Fatalf("Failed to generate signature: %v", err)
		}
	}
}

// BenchmarkACLSignatureVerification ACL签名验证性能测试
func BenchmarkACLSignatureVerification(b *testing.B) {
	aclUtils := NewACLUtils("test_key", "test_secret_key_12345", HmacSHA256)
	requestData := map[string]string{
		"topic":         "test_topic",
		"consumerGroup": "test_group",
		"timestamp":     fmt.Sprintf("%d", GenerateTimestamp()),
	}

	// 预先生成签名
	signature, err := aclUtils.GenerateSignature(requestData)
	if err != nil {
		b.Fatalf("Failed to generate signature: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !aclUtils.VerifySignature(requestData, signature) {
			b.Error("Signature verification failed")
		}
	}
}

// BenchmarkACLMiddlewareAuthHeaders ACL中间件认证头生成性能测试
func BenchmarkACLMiddlewareAuthHeaders(b *testing.B) {
	aclMiddleware := NewACLMiddleware("test_access_key", "test_secret_key_12345", HmacSHA256, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := aclMiddleware.GenerateAuthHeaders("test_topic", "test_group", "")
		if err != nil {
			b.Fatalf("Failed to generate auth headers: %v", err)
		}
	}
}

// TestACLConcurrentAccess 测试ACL并发访问
func TestACLConcurrentAccess(t *testing.T) {
	aclMiddleware := NewACLMiddleware("test_access_key", "test_secret_key_12345", HmacSHA256, true)

	const numGoroutines = 50
	const numOperations = 5

	var wg sync.WaitGroup
	errorChan := make(chan error, numGoroutines*numOperations)

	// 启动多个goroutine并发测试
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				topic := fmt.Sprintf("test_topic_%d_%d", id, j)
				group := fmt.Sprintf("test_group_%d_%d", id, j)

				// 生成认证头
				headers, err := aclMiddleware.GenerateAuthHeaders(topic, group, "")
				if err != nil {
					errorChan <- fmt.Errorf("goroutine %d operation %d: failed to generate headers: %v", id, j, err)
					return
				}

				// 验证认证头
				err = aclMiddleware.VerifyAuthHeaders(headers, topic, group, "")
				if err != nil {
					errorChan <- fmt.Errorf("goroutine %d operation %d: header verification failed: %v", id, j, err)
					return
				}
			}
		}(i)
	}

	// 等待所有goroutine完成
	wg.Wait()
	close(errorChan)

	// 检查是否有错误
	for err := range errorChan {
		t.Error(err)
	}

	t.Logf("Concurrent ACL test completed: %d goroutines × %d operations", numGoroutines, numOperations)
}

// TestACLErrorHandling 测试ACL错误处理
func TestACLErrorHandling(t *testing.T) {
	// 测试禁用状态下的ACL中间件
	aclMiddleware := NewACLMiddleware("test_access_key", "test_secret_key_12345", HmacSHA256, false)

	// 禁用状态下应该跳过验证
	headers := map[string]string{}
	err := aclMiddleware.VerifyAuthHeaders(headers, "test_topic", "test_group", "")
	if err != nil {
		t.Errorf("Disabled ACL middleware should pass verification, but got error: %v", err)
	}

	// 测试启用状态下的空认证头
	aclMiddleware.SetEnabled(true)
	err = aclMiddleware.VerifyAuthHeaders(headers, "test_topic", "test_group", "")
	if err == nil {
		t.Error("Empty headers should fail verification when ACL is enabled")
	}

	t.Log("ACL error handling test completed")
}

// TestACLTimestampValidation 测试ACL时间戳验证
func TestACLTimestampValidation(t *testing.T) {
	aclMiddleware := NewACLMiddleware("test_access_key", "test_secret_key_12345", HmacSHA256, true)

	// 生成有效的认证头
	headers, err := aclMiddleware.GenerateAuthHeaders("test_topic", "test_group", "")
	if err != nil {
		t.Fatalf("Failed to generate auth headers: %v", err)
	}

	// 修改时间戳为过期时间
	expiredTimestamp := GenerateTimestamp() - 600000 // 10分钟前
	headers["Timestamp"] = fmt.Sprintf("%d", expiredTimestamp)

	// 验证过期时间戳应该失败
	err = aclMiddleware.VerifyAuthHeaders(headers, "test_topic", "test_group", "")
	if err == nil {
		t.Error("Expired timestamp should fail verification")
	}

	t.Log("ACL timestamp validation test completed")
}