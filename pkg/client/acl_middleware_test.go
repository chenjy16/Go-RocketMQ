package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

// TestNewACLMiddleware 测试ACL中间件创建
func TestNewACLMiddleware(t *testing.T) {
	accessKey := "test_access_key"
	secretKey := "test_secret_key"
	signatureMethod := HmacSHA1
	enabled := true

	middleware := NewACLMiddleware(accessKey, secretKey, signatureMethod, enabled)

	if middleware == nil {
		t.Fatal("ACL middleware should not be nil")
	}

	if !middleware.IsEnabled() {
		t.Error("ACL middleware should be enabled")
	}

	config := middleware.GetConfig()
	if config.AccessKey != accessKey {
		t.Errorf("Expected accessKey %s, got %s", accessKey, config.AccessKey)
	}

	// SecretKey应该被隐藏
	if config.SecretKey != "***" {
		t.Errorf("Expected secretKey to be masked, got %s", config.SecretKey)
	}

	if config.SignatureMethod != string(signatureMethod) {
		t.Errorf("Expected signatureMethod %s, got %s", signatureMethod, config.SignatureMethod)
	}
}

// TestNewACLMiddlewareFromConfig 测试从配置文件创建ACL中间件
func TestNewACLMiddlewareFromConfig(t *testing.T) {
	// 创建临时配置文件
	config := ACLConfig{
		AccessKey:       "test_key",
		SecretKey:       "test_secret",
		SignatureMethod: "HmacSHA1",
		Enabled:         true,
		TimestampTolerance: 300000,
	}

	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}

	tmpFile, err := ioutil.TempFile("", "acl_config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(configData); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}
	tmpFile.Close()

	// 测试从配置文件创建中间件
	middleware, err := NewACLMiddlewareFromConfig(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("Failed to create middleware from config: %v", err)
	}

	if !middleware.IsEnabled() {
		t.Error("Middleware should be enabled")
	}

	middlewareConfig := middleware.GetConfig()
	if middlewareConfig.AccessKey != config.AccessKey {
		t.Errorf("Expected accessKey %s, got %s", config.AccessKey, middlewareConfig.AccessKey)
	}
}

// TestACLMiddlewareSetEnabled 测试启用/禁用ACL
func TestACLMiddlewareSetEnabled(t *testing.T) {
	middleware := NewACLMiddleware("key", "secret", HmacSHA1, true)

	if !middleware.IsEnabled() {
		t.Error("Middleware should be enabled initially")
	}

	middleware.SetEnabled(false)
	if middleware.IsEnabled() {
		t.Error("Middleware should be disabled after SetEnabled(false)")
	}

	middleware.SetEnabled(true)
	if !middleware.IsEnabled() {
		t.Error("Middleware should be enabled after SetEnabled(true)")
	}
}

// TestGenerateAuthHeaders 测试认证头生成
func TestGenerateAuthHeaders(t *testing.T) {
	middleware := NewACLMiddleware("test_key", "test_secret", HmacSHA1, true)

	topic := "test_topic"
	consumerGroup := "test_group"
	msgId := "test_msg_id"

	headers, err := middleware.GenerateAuthHeaders(topic, consumerGroup, msgId)
	if err != nil {
		t.Fatalf("Failed to generate auth headers: %v", err)
	}

	// 检查必要的头信息
	requiredHeaders := []string{"AccessKey", "Signature", "Timestamp", "SignatureMethod"}
	for _, header := range requiredHeaders {
		if _, exists := headers[header]; !exists {
			t.Errorf("Missing required header: %s", header)
		}
	}

	if headers["AccessKey"] != "test_key" {
		t.Errorf("Expected AccessKey 'test_key', got %s", headers["AccessKey"])
	}

	if headers["SignatureMethod"] != string(HmacSHA1) {
		t.Errorf("Expected SignatureMethod %s, got %s", HmacSHA1, headers["SignatureMethod"])
	}
}

// TestVerifyAuthHeaders 测试认证头验证
func TestVerifyAuthHeaders(t *testing.T) {
	middleware := NewACLMiddleware("test_key", "test_secret", HmacSHA1, true)

	topic := "test_topic"
	consumerGroup := "test_group"
	msgId := "test_msg_id"

	// 生成正确的认证头
	headers, err := middleware.GenerateAuthHeaders(topic, consumerGroup, msgId)
	if err != nil {
		t.Fatalf("Failed to generate auth headers: %v", err)
	}

	// 验证正确的认证头
	err = middleware.VerifyAuthHeaders(headers, topic, consumerGroup, msgId)
	if err != nil {
		t.Errorf("Valid auth headers should pass verification: %v", err)
	}

	// 测试错误的签名
	headers["Signature"] = "wrong_signature"
	err = middleware.VerifyAuthHeaders(headers, topic, consumerGroup, msgId)
	if err == nil {
		t.Error("Wrong signature should fail verification")
	}
}

// TestVerifyAuthHeadersDisabled 测试禁用状态下的验证
func TestVerifyAuthHeadersDisabled(t *testing.T) {
	middleware := NewACLMiddleware("test_key", "test_secret", HmacSHA1, false)

	headers := map[string]string{
		"AccessKey": "test_key",
		"Signature": "wrong_signature",
	}

	// 禁用状态下应该跳过验证
	err := middleware.VerifyAuthHeaders(headers, "topic", "group", "msgId")
	if err != nil {
		t.Errorf("Disabled middleware should skip verification: %v", err)
	}
}

// TestVerifyAuthHeadersMissingHeaders 测试缺少认证头的情况
func TestVerifyAuthHeadersMissingHeaders(t *testing.T) {
	middleware := NewACLMiddleware("test_key", "test_secret", HmacSHA1, true)

	// 测试缺少AccessKey
	headers := map[string]string{
		"Signature": "some_signature",
		"Timestamp": "1640995200000",
	}

	err := middleware.VerifyAuthHeaders(headers, "topic", "group", "msgId")
	if err == nil {
		t.Error("Missing AccessKey should fail verification")
	}

	// 测试缺少Signature
	headers = map[string]string{
		"AccessKey": "test_key",
		"Timestamp": "1640995200000",
	}

	err = middleware.VerifyAuthHeaders(headers, "topic", "group", "msgId")
	if err == nil {
		t.Error("Missing Signature should fail verification")
	}

	// 测试缺少Timestamp
	headers = map[string]string{
		"AccessKey": "test_key",
		"Signature": "some_signature",
	}

	err = middleware.VerifyAuthHeaders(headers, "topic", "group", "msgId")
	if err == nil {
		t.Error("Missing Timestamp should fail verification")
	}
}

// TestVerifyAuthHeadersExpiredTimestamp 测试过期时间戳
func TestVerifyAuthHeadersExpiredTimestamp(t *testing.T) {
	middleware := NewACLMiddleware("test_key", "test_secret", HmacSHA1, true)

	// 使用过期的时间戳（10分钟前）
	expiredTimestamp := time.Now().UnixMilli() - 600000

	headers := map[string]string{
		"AccessKey":       "test_key",
		"Signature":       "some_signature",
		"Timestamp":       fmt.Sprintf("%d", expiredTimestamp),
		"SignatureMethod": string(HmacSHA1),
	}

	err := middleware.VerifyAuthHeaders(headers, "topic", "group", "msgId")
	if err == nil {
		t.Error("Expired timestamp should fail verification")
	}
}

// TestSetReloadInterval 测试设置重载间隔
func TestSetReloadInterval(t *testing.T) {
	middleware := NewACLMiddleware("key", "secret", HmacSHA1, true)

	newInterval := 60 * time.Second
	middleware.SetReloadInterval(newInterval)

	if middleware.reloadInterval != newInterval {
		t.Errorf("Expected reload interval %v, got %v", newInterval, middleware.reloadInterval)
	}
}

// TestStopHotReload 测试停止热重载
func TestStopHotReload(t *testing.T) {
	middleware := NewACLMiddleware("key", "secret", HmacSHA1, true)

	// 这个测试主要确保StopHotReload不会panic
	middleware.StopHotReload()

	// 注意：多次调用StopHotReload会导致panic，因为会重复关闭channel
	// 这是预期行为，因为StopHotReload应该只调用一次
}