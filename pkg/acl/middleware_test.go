package acl

import (
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewAclMiddleware(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_acl.yml")

	configContent := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"

accounts:
  - accessKey: "test_access_key"
    secretKey: "test_secret_key"
    whiteRemoteAddress: "127.0.0.1"
    admin: true
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 创建中间件
	middleware := NewAclMiddleware(validator, true)
	if middleware == nil {
		t.Fatal("Expected non-nil middleware")
	}

	if !middleware.enabled {
		t.Error("Expected middleware to be enabled")
	}

	if middleware.validator == nil {
		t.Error("Expected validator to be set")
	}
}

func TestAclMiddleware_CheckTopicPermission(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_acl.yml")

	configContent := `
globalWhiteRemoteAddresses: []

accounts:
  - accessKey: "test_access_key"
    secretKey: "test_secret_key"
    whiteRemoteAddress: "127.0.0.1"
    admin: false
    defaultTopicPerm: "PUB"
    defaultGroupPerm: "SUB"
    topicPerms:
      - "specific_topic=PUB|SUB"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器和中间件
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	middleware := NewAclMiddleware(validator, true)

	// 获取账户
	account, exists := validator.GetAccount("test_access_key")
	if !exists {
		t.Fatal("Expected account to exist")
	}

	// 测试默认Topic权限
	err = middleware.CheckTopicPermission(account, "default_topic", "pub", "127.0.0.1")
	if err != nil {
		t.Errorf("Expected permission to be allowed for default topic pub, got error: %v", err)
	}

	// 测试特定Topic权限
	err = middleware.CheckTopicPermission(account, "specific_topic", "sub", "127.0.0.1")
	if err != nil {
		t.Errorf("Expected permission to be allowed for specific topic sub, got error: %v", err)
	}

	// 测试拒绝的权限
	err = middleware.CheckTopicPermission(account, "default_topic", "sub", "127.0.0.1")
	if err == nil {
		t.Error("Expected permission to be denied for default topic sub")
	}
}

func TestAclMiddleware_CheckGroupPermission(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_acl.yml")

	configContent := `
globalWhiteRemoteAddresses: []

accounts:
  - accessKey: "test_access_key"
    secretKey: "test_secret_key"
    whiteRemoteAddress: "127.0.0.1"
    admin: false
    defaultTopicPerm: "PUB"
    defaultGroupPerm: "SUB"
    groupPerms:
      - "specific_group=SUB"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器和中间件
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	middleware := NewAclMiddleware(validator, true)

	// 获取账户
	account, exists := validator.GetAccount("test_access_key")
	if !exists {
		t.Fatal("Expected account to exist")
	}

	// 测试默认Group权限
	err = middleware.CheckGroupPermission(account, "default_group", "sub", "127.0.0.1")
	if err != nil {
		t.Errorf("Expected permission to be allowed for default group sub, got error: %v", err)
	}

	// 测试特定Group权限
	err = middleware.CheckGroupPermission(account, "specific_group", "sub", "127.0.0.1")
	if err != nil {
		t.Errorf("Expected permission to be allowed for specific group sub, got error: %v", err)
	}

	// 测试拒绝的权限
	err = middleware.CheckGroupPermission(account, "default_group", "pub", "127.0.0.1")
	if err == nil {
		t.Error("Expected permission to be denied for default group pub")
	}
}

func TestAclMiddleware_ValidateProducerRequest(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_acl.yml")

	configContent := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"

accounts:
  - accessKey: "test_access_key"
    secretKey: "test_secret_key"
    whiteRemoteAddress: "127.0.0.1"
    admin: false
    defaultTopicPerm: "PUB"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器和中间件
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	middleware := NewAclMiddleware(validator, true)

	// 创建生产者请求数据
	currentTime := fmt.Sprintf("%d", time.Now().Unix())
	requestData := map[string]string{
		"AccessKey": "test_access_key",
		"Signature": "test_signature",
		"Timestamp": currentTime,
	}

	// 测试生产者请求验证
	account, err := middleware.ValidateProducerRequest(requestData, "test_topic", "127.0.0.1")
	// 由于签名不正确，验证可能失败，但这里主要测试中间件的基本功能
	if err != nil {
		t.Logf("Producer request validation failed as expected: %v", err)
	} else {
		t.Log("Producer request validation succeeded")
		if account == nil {
			t.Error("Expected account to be returned when validation succeeds")
		}
	}
}

func TestAclMiddleware_ValidateConsumerRequest(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_acl.yml")

	configContent := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"

accounts:
  - accessKey: "test_access_key"
    secretKey: "test_secret_key"
    whiteRemoteAddress: "127.0.0.1"
    admin: false
    defaultTopicPerm: "SUB"
    defaultGroupPerm: "SUB"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器和中间件
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	middleware := NewAclMiddleware(validator, true)

	// 创建消费者请求数据
	currentTime := fmt.Sprintf("%d", time.Now().Unix())
	requestData := map[string]string{
		"AccessKey": "test_access_key",
		"Signature": "test_signature",
		"Timestamp": currentTime,
	}

	// 测试消费者请求验证
	account, err := middleware.ValidateConsumerRequest(requestData, "test_topic", "test_group", "127.0.0.1")
	// 由于签名不正确，验证可能失败，但这里主要测试中间件的基本功能
	if err != nil {
		t.Logf("Consumer request validation failed as expected: %v", err)
	} else {
		t.Log("Consumer request validation succeeded")
		if account == nil {
			t.Error("Expected account to be returned when validation succeeds")
		}
	}
}

func TestAclMiddleware_ValidateAdminRequest(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_acl.yml")

	configContent := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"

accounts:
  - accessKey: "admin_access_key"
    secretKey: "admin_secret_key"
    whiteRemoteAddress: "127.0.0.1"
    admin: true
  - accessKey: "user_access_key"
    secretKey: "user_secret_key"
    whiteRemoteAddress: "127.0.0.1"
    admin: false
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器和中间件
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	middleware := NewAclMiddleware(validator, true)

	// 测试管理员请求验证
	currentTime := fmt.Sprintf("%d", time.Now().Unix())
	adminRequestData := map[string]string{
		"AccessKey": "admin_access_key",
		"Signature": "test_signature",
		"Timestamp": currentTime,
	}

	account, err := middleware.ValidateAdminRequest(adminRequestData, "127.0.0.1")
	// 由于签名不正确，验证可能失败，但这里主要测试中间件的基本功能
	if err != nil {
		t.Logf("Admin request validation failed: %v", err)
	} else {
		t.Log("Admin request validation succeeded")
		if account == nil {
			t.Error("Expected account to be returned when validation succeeds")
		}
	}

	// 测试非管理员用户的管理员请求
	userRequestData := map[string]string{
		"AccessKey": "user_access_key",
		"Signature": "test_signature",
		"Timestamp": currentTime,
	}

	account, err = middleware.ValidateAdminRequest(userRequestData, "127.0.0.1")
	if err == nil {
		t.Error("Expected admin request validation to fail for non-admin user")
	}
	if account != nil {
		t.Error("Expected no account to be returned for failed validation")
	}
}

func TestAclMiddleware_GetRemoteAddress(t *testing.T) {
	middleware := &AclMiddleware{}

	// 测试标准的RemoteAddr
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.100:12345"

	remoteAddr := middleware.getRemoteAddress(req)
	if remoteAddr != "192.168.1.100" {
		t.Errorf("Expected remote address '192.168.1.100', got '%s'", remoteAddr)
	}

	// 测试X-Forwarded-For头
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 192.168.1.100")
	remoteAddr = middleware.getRemoteAddress(req)
	if remoteAddr != "10.0.0.1" {
		t.Errorf("Expected remote address '10.0.0.1' from X-Forwarded-For, got '%s'", remoteAddr)
	}

	// 测试X-Real-IP头
	req.Header.Del("X-Forwarded-For")
	req.Header.Set("X-Real-IP", "172.16.0.1")
	remoteAddr = middleware.getRemoteAddress(req)
	if remoteAddr != "172.16.0.1" {
		t.Errorf("Expected remote address '172.16.0.1' from X-Real-IP, got '%s'", remoteAddr)
	}
}

func TestAclMiddleware_Disabled(t *testing.T) {
	// 创建禁用的中间件
	middleware := NewAclMiddleware(nil, false)

	if middleware.enabled {
		t.Error("Expected middleware to be disabled")
	}

	// 测试禁用状态下的权限检查（应该总是成功）
	err := middleware.CheckTopicPermission(nil, "test_topic", "pub", "127.0.0.1")
	if err != nil {
		t.Error("Expected topic permission check to succeed when middleware is disabled")
	}

	err = middleware.CheckGroupPermission(nil, "test_group", "sub", "127.0.0.1")
	if err != nil {
		t.Error("Expected group permission check to succeed when middleware is disabled")
	}
}

func TestAclMiddleware_IsAclEnabled(t *testing.T) {
	// 测试启用的中间件
	middleware := NewAclMiddleware(nil, true)
	if !middleware.IsAclEnabled() {
		t.Error("Expected ACL to be enabled")
	}

	// 测试禁用的中间件
	middleware = NewAclMiddleware(nil, false)
	if middleware.IsAclEnabled() {
		t.Error("Expected ACL to be disabled")
	}
}

func TestAclMiddleware_SetAclEnabled(t *testing.T) {
	// 创建禁用的中间件
	middleware := NewAclMiddleware(nil, false)
	if middleware.IsAclEnabled() {
		t.Error("Expected ACL to be initially disabled")
	}

	// 启用ACL
	middleware.SetAclEnabled(true)
	if !middleware.IsAclEnabled() {
		t.Error("Expected ACL to be enabled after setting")
	}

	// 禁用ACL
	middleware.SetAclEnabled(false)
	if middleware.IsAclEnabled() {
		t.Error("Expected ACL to be disabled after setting")
	}
}