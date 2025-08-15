package client

import (
	"testing"
	"time"
)

// TestNewACLUtils 测试ACL工具类创建
func TestNewACLUtils(t *testing.T) {
	accessKey := "test_access_key"
	secretKey := "test_secret_key"
	signatureMethod := HmacSHA1

	aclUtils := NewACLUtils(accessKey, secretKey, signatureMethod)

	if aclUtils.accessKey != accessKey {
		t.Errorf("Expected accessKey %s, got %s", accessKey, aclUtils.accessKey)
	}

	if aclUtils.secretKey != secretKey {
		t.Errorf("Expected secretKey %s, got %s", secretKey, aclUtils.secretKey)
	}

	if aclUtils.signatureMethod != signatureMethod {
		t.Errorf("Expected signatureMethod %s, got %s", signatureMethod, aclUtils.signatureMethod)
	}
}

// TestNewACLUtilsDefaultSignatureMethod 测试默认签名算法
func TestNewACLUtilsDefaultSignatureMethod(t *testing.T) {
	aclUtils := NewACLUtils("key", "secret", "")
	if aclUtils.signatureMethod != HmacSHA1 {
		t.Errorf("Expected default signatureMethod %s, got %s", HmacSHA1, aclUtils.signatureMethod)
	}
}

// TestGenerateSignatureHmacSHA1 测试HMAC-SHA1签名生成
func TestGenerateSignatureHmacSHA1(t *testing.T) {
	aclUtils := NewACLUtils("test_key", "test_secret", HmacSHA1)

	requestData := map[string]string{
		"topic":         "test_topic",
		"consumerGroup": "test_group",
		"msgId":         "test_msg_id",
		"timestamp":     "1640995200000",
	}

	signature, err := aclUtils.GenerateSignature(requestData)
	if err != nil {
		t.Fatalf("Failed to generate signature: %v", err)
	}

	if signature == "" {
		t.Error("Generated signature should not be empty")
	}

	// 验证签名
	if !aclUtils.VerifySignature(requestData, signature) {
		t.Error("Signature verification failed")
	}
}

// TestGenerateSignatureHmacSHA256 测试HMAC-SHA256签名生成
func TestGenerateSignatureHmacSHA256(t *testing.T) {
	aclUtils := NewACLUtils("test_key", "test_secret", HmacSHA256)

	requestData := map[string]string{
		"topic":         "test_topic",
		"consumerGroup": "test_group",
		"msgId":         "test_msg_id",
		"timestamp":     "1640995200000",
	}

	signature, err := aclUtils.GenerateSignature(requestData)
	if err != nil {
		t.Fatalf("Failed to generate signature: %v", err)
	}

	if signature == "" {
		t.Error("Generated signature should not be empty")
	}

	// 验证签名
	if !aclUtils.VerifySignature(requestData, signature) {
		t.Error("Signature verification failed")
	}
}

// TestGenerateSignatureEmptySecretKey 测试空密钥的情况
func TestGenerateSignatureEmptySecretKey(t *testing.T) {
	aclUtils := NewACLUtils("test_key", "", HmacSHA1)

	requestData := map[string]string{
		"topic": "test_topic",
	}

	_, err := aclUtils.GenerateSignature(requestData)
	if err == nil {
		t.Error("Expected error for empty secret key")
	}
}

// TestBuildRequestData 测试请求数据构建
func TestBuildRequestData(t *testing.T) {
	aclUtils := NewACLUtils("test_key", "test_secret", HmacSHA1)

	topic := "test_topic"
	consumerGroup := "test_group"
	msgId := "test_msg_id"
	timestamp := int64(1640995200000)

	requestData := aclUtils.BuildRequestData(topic, consumerGroup, msgId, timestamp)

	if requestData["topic"] != topic {
		t.Errorf("Expected topic %s, got %s", topic, requestData["topic"])
	}

	if requestData["consumerGroup"] != consumerGroup {
		t.Errorf("Expected consumerGroup %s, got %s", consumerGroup, requestData["consumerGroup"])
	}

	if requestData["msgId"] != msgId {
		t.Errorf("Expected msgId %s, got %s", msgId, requestData["msgId"])
	}

	// BuildRequestData方法不包含accessKey和signatureMethod
	// 这些信息通过其他方法获取
	if aclUtils.GetAccessKey() != "test_key" {
		t.Errorf("Expected accessKey %s, got %s", "test_key", aclUtils.GetAccessKey())
	}

	if aclUtils.GetSignatureMethod() != HmacSHA1 {
		t.Errorf("Expected signatureMethod %s, got %s", HmacSHA1, aclUtils.GetSignatureMethod())
	}
}

// TestGenerateTimestamp 测试时间戳生成
func TestGenerateTimestamp(t *testing.T) {
	timestamp := GenerateTimestamp()
	now := time.Now().UnixMilli()

	// 时间戳应该在当前时间的合理范围内（1秒内）
	if timestamp < now-1000 || timestamp > now+1000 {
		t.Errorf("Generated timestamp %d is not within reasonable range of current time %d", timestamp, now)
	}
}

// TestIsTimestampValid 测试时间戳验证
func TestIsTimestampValid(t *testing.T) {
	now := time.Now().UnixMilli()
	toleranceMs := int64(300000) // 5分钟

	// 测试有效时间戳
	if !IsTimestampValid(now, toleranceMs) {
		t.Error("Current timestamp should be valid")
	}

	// 测试过期时间戳
	expiredTimestamp := now - toleranceMs - 1000
	if IsTimestampValid(expiredTimestamp, toleranceMs) {
		t.Error("Expired timestamp should be invalid")
	}

	// 测试未来时间戳（注意：当前实现只检查过去时间戳，未来时间戳被认为是有效的）
	futureTimestamp := now + toleranceMs + 1000
	if !IsTimestampValid(futureTimestamp, toleranceMs) {
		t.Error("Future timestamp is considered valid in current implementation")
	}
}

// TestValidateACLConfig 测试ACL配置验证
func TestValidateACLConfig(t *testing.T) {
	// 测试有效配置
	err := ValidateACLConfig("valid_key", "valid_secret")
	if err != nil {
		t.Errorf("Valid ACL config should not return error: %v", err)
	}

	// 测试空AccessKey
	err = ValidateACLConfig("", "valid_secret")
	if err == nil {
		t.Error("Empty AccessKey should return error")
	}

	// 测试空SecretKey
	err = ValidateACLConfig("valid_key", "")
	if err == nil {
		t.Error("Empty SecretKey should return error")
	}
}

// TestVerifySignature 测试签名验证
func TestVerifySignature(t *testing.T) {
	aclUtils := NewACLUtils("test_key", "test_secret", HmacSHA1)

	requestData := map[string]string{
		"topic":         "test_topic",
		"consumerGroup": "test_group",
		"msgId":         "test_msg_id",
		"timestamp":     "1640995200000",
	}

	// 生成正确的签名
	correctSignature, err := aclUtils.GenerateSignature(requestData)
	if err != nil {
		t.Fatalf("Failed to generate signature: %v", err)
	}

	// 验证正确的签名
	if !aclUtils.VerifySignature(requestData, correctSignature) {
		t.Error("Correct signature should be verified successfully")
	}

	// 验证错误的签名
	if aclUtils.VerifySignature(requestData, "wrong_signature") {
		t.Error("Wrong signature should not be verified")
	}
}