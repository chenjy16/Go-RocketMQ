package acl

import (
	"strings"
	"testing"
)

func TestHmacSHA1SignatureGenerator_GenerateSignature(t *testing.T) {
	generator := NewHmacSHA1SignatureGenerator()
	secretKey := "test_secret_key"
	data := map[string]string{
		"AccessKey": "test_access_key",
		"Topic":     "test_topic",
		"Operation": "PUB",
		"Timestamp": "1234567890",
	}

	signature, err := generator.GenerateSignature(secretKey, data)
	if err != nil {
		t.Fatalf("Failed to generate signature: %v", err)
	}
	if signature == "" {
		t.Error("Expected non-empty signature")
	}

	// 测试相同输入产生相同签名
	signature2, err := generator.GenerateSignature(secretKey, data)
	if err != nil {
		t.Fatalf("Failed to generate signature: %v", err)
	}
	if signature != signature2 {
		t.Errorf("Expected same signature for same input, got %s and %s", signature, signature2)
	}

	// 测试不同密钥产生不同签名
	differentSignature, err := generator.GenerateSignature("different_key", data)
	if err != nil {
		t.Fatalf("Failed to generate signature: %v", err)
	}
	if signature == differentSignature {
		t.Error("Expected different signatures for different keys")
	}
}

func TestHmacSHA1SignatureGenerator_VerifySignature(t *testing.T) {
	generator := NewHmacSHA1SignatureGenerator()
	secretKey := "test_secret_key"
	data := map[string]string{
		"AccessKey": "test_access_key",
		"Topic":     "test_topic",
		"Operation": "PUB",
		"Timestamp": "1234567890",
	}

	// 生成签名
	signature, err := generator.GenerateSignature(secretKey, data)
	if err != nil {
		t.Fatalf("Failed to generate signature: %v", err)
	}

	// 验证正确的签名
	if !generator.VerifySignature(secretKey, data, signature) {
		t.Error("Expected signature verification to pass")
	}

	// 验证错误的签名
	if generator.VerifySignature(secretKey, data, "wrong_signature") {
		t.Error("Expected signature verification to fail for wrong signature")
	}

	// 验证错误的密钥
	if generator.VerifySignature("wrong_key", data, signature) {
		t.Error("Expected signature verification to fail for wrong key")
	}

	// 验证错误的数据
	wrongData := map[string]string{
		"AccessKey": "wrong_access_key",
		"Topic":     "test_topic",
		"Operation": "PUB",
		"Timestamp": "1234567890",
	}
	if generator.VerifySignature(secretKey, wrongData, signature) {
		t.Error("Expected signature verification to fail for wrong data")
	}
}

func TestHmacSHA1SignatureGenerator_SignRequest(t *testing.T) {
	generator := NewHmacSHA1SignatureGenerator()
	credentials := &SessionCredentials{
		AccessKey: "test_access_key",
		SecretKey: "test_secret_key",
	}
	requestData := map[string]string{
		"Topic":     "test_topic",
		"Operation": "PUB",
		"Timestamp": "1234567890",
	}

	signedData, err := generator.SignRequest(credentials, requestData)
	if err != nil {
		t.Fatalf("Failed to sign request: %v", err)
	}

	if signedData["Signature"] == "" {
		t.Error("Expected non-empty signature in signed data")
	}

	if signedData["AccessKey"] != "test_access_key" {
		t.Error("Expected access key to be added to signed data")
	}
}

func TestHmacSHA1SignatureGenerator_ExtractCredentialsFromRequest(t *testing.T) {
	generator := NewHmacSHA1SignatureGenerator()

	// 测试有效的请求数据
	requestData := map[string]string{
		"AccessKey": "test_key",
		"Signature": "test_sig",
		"Timestamp": "1234567890",
		"Topic":     "test_topic",
	}

	creds, signature, err := generator.ExtractCredentialsFromRequest(requestData)
	if err != nil {
		t.Fatalf("Failed to extract credentials: %v", err)
	}

	if creds.AccessKey != "test_key" {
		t.Errorf("Expected access key 'test_key', got '%s'", creds.AccessKey)
	}

	if signature != "test_sig" {
		t.Errorf("Expected signature 'test_sig', got '%s'", signature)
	}

	// 测试缺少AccessKey的请求数据
	incompleteData := map[string]string{
		"Signature": "test_sig",
		"Timestamp": "1234567890",
	}

	_, _, err = generator.ExtractCredentialsFromRequest(incompleteData)
	if err == nil {
		t.Error("Expected error for missing AccessKey")
	}
}

func TestBuildSignString(t *testing.T) {
	generator := NewHmacSHA1SignatureGenerator()
	data := map[string]string{
		"AccessKey": "test_access_key",
		"Topic":     "test_topic",
		"Operation": "PUB",
		"Timestamp": "1234567890",
	}

	signString := generator.buildSignString(data)
	// 验证签名字符串包含所有必要的字段
	if !strings.Contains(signString, "AccessKey=test_access_key") {
		t.Error("Expected sign string to contain AccessKey")
	}
	if !strings.Contains(signString, "Topic=test_topic") {
		t.Error("Expected sign string to contain Topic")
	}
	if !strings.Contains(signString, "Operation=PUB") {
		t.Error("Expected sign string to contain Operation")
	}
	if !strings.Contains(signString, "Timestamp=1234567890") {
		t.Error("Expected sign string to contain Timestamp")
	}
}

func TestBuildSignStringWithSignature(t *testing.T) {
	generator := NewHmacSHA1SignatureGenerator()
	data := map[string]string{
		"AccessKey": "test_access_key",
		"Topic":     "test_topic",
		"Signature": "should_be_ignored",
		"Timestamp": "1234567890",
	}

	signString := generator.buildSignString(data)
	// 验证签名字段被忽略
	if strings.Contains(signString, "Signature") {
		t.Error("Expected sign string to ignore Signature field")
	}
}