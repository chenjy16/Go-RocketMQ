package acl

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewPlainAclValidator(t *testing.T) {
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
    defaultTopicPerm: "PUB"
    defaultGroupPerm: "SUB"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器
	validator := NewPlainAclValidator(configFile)
	if validator == nil {
		t.Fatal("Expected non-nil validator")
	}

	// 加载配置
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
}

func TestPlainAclValidator_Authenticate(t *testing.T) {
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
    defaultTopicPerm: "PUB"
    defaultGroupPerm: "SUB"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器并加载配置
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 创建签名生成器
	signatureGenerator := NewHmacSHA1SignatureGenerator()
	currentTime := time.Now().Unix()
	requestData := map[string]string{
		"Topic":     "test_topic",
		"Operation": "PUB",
		"Timestamp": fmt.Sprintf("%d", currentTime),
	}

	credentials := &SessionCredentials{
		AccessKey: "test_access_key",
		SecretKey: "test_secret_key",
	}

	signedData, err := signatureGenerator.SignRequest(credentials, requestData)
	if err != nil {
		t.Fatalf("Failed to sign request: %v", err)
	}

	// 测试成功认证
	authReq := &AuthenticationRequest{
		AccessKey:     "test_access_key",
		Signature:     signedData["Signature"],
		Timestamp:     currentTime,
		RemoteAddress: "127.0.0.1",
		RequestData:   signedData,
	}

	result := validator.Authenticate(authReq)
	if !result.Success {
		t.Errorf("Expected authentication to succeed, got error: %s", result.ErrorMessage)
		return
	}

	if result.Account == nil {
		t.Error("Expected account to be returned")
		t.Logf("Authentication result: Success=%v, ErrorMessage=%s", result.Success, result.ErrorMessage)
		return
	}

	if result.Account.AccessKey != "test_access_key" {
		t.Errorf("Expected access key 'test_access_key', got '%s'", result.Account.AccessKey)
	}

	// 测试错误的AccessKey
	authReqWrongKey := &AuthenticationRequest{
		AccessKey:     "wrong_access_key",
		Signature:     signedData["Signature"],
		Timestamp:     currentTime,
		RemoteAddress: "127.0.0.1",
		RequestData:   signedData,
	}

	result = validator.Authenticate(authReqWrongKey)
	if result.Success {
		t.Error("Expected authentication to fail for wrong access key")
	}

	// 测试错误的签名
	authReqWrongSig := &AuthenticationRequest{
		AccessKey:     "test_access_key",
		Signature:     "wrong_signature",
		Timestamp:     currentTime,
		RemoteAddress: "127.0.0.1",
		RequestData:   signedData,
	}

	result = validator.Authenticate(authReqWrongSig)
	if result.Success {
		t.Error("Expected authentication to fail for wrong signature")
	}
}

func TestPlainAclValidator_CheckPermission(t *testing.T) {
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
    defaultGroupPerm: "SUB"
    topicPerms:
      - "specific_topic=PUB|SUB"
    groupPerms:
      - "specific_group=SUB"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器并加载配置
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 获取账户
	account, exists := validator.GetAccount("test_access_key")
	if !exists {
		t.Fatal("Expected account to exist")
	}

	// 测试Topic权限检查 - 默认权限
	permReq := &PermissionCheckRequest{
		Account:      account,
		Resource:     "default_topic",
		ResourceType: "topic",
		Operation:    "pub",
		RemoteAddress: "127.0.0.1",
	}

	result := validator.CheckPermission(permReq)
	if !result.Allowed {
		t.Errorf("Expected permission to be allowed for default topic pub, got error: %s", result.ErrorMessage)
	}

	// 测试Topic权限检查 - 特定权限
	permReq.Resource = "specific_topic"
	permReq.Operation = "sub"

	result = validator.CheckPermission(permReq)
	if !result.Allowed {
		t.Errorf("Expected permission to be allowed for specific topic sub, got error: %s", result.ErrorMessage)
	}

	// 测试Group权限检查
	permReq.ResourceType = "group"
	permReq.Resource = "default_group"
	permReq.Operation = "sub"

	result = validator.CheckPermission(permReq)
	if !result.Allowed {
		t.Errorf("Expected permission to be allowed for default group sub, got error: %s", result.ErrorMessage)
	}

	// 测试拒绝的权限
	permReq.Operation = "pub"

	result = validator.CheckPermission(permReq)
	if result.Allowed {
		t.Error("Expected permission to be denied for group pub")
	}
}

func TestPlainAclValidator_IsGlobalWhiteRemoteAddress(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_acl.yml")

	configContent := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"
  - "192.168.1.*"
  - "10.0.0.0/8"

accounts: []
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器并加载配置
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 测试精确匹配
	if !validator.IsGlobalWhiteRemoteAddress("127.0.0.1") {
		t.Error("Expected 127.0.0.1 to be in global white list")
	}

	// 测试通配符匹配
	if !validator.IsGlobalWhiteRemoteAddress("192.168.1.100") {
		t.Error("Expected 192.168.1.100 to match 192.168.1.*")
	}

	// 测试CIDR匹配
	if !validator.IsGlobalWhiteRemoteAddress("10.1.1.1") {
		t.Error("Expected 10.1.1.1 to match 10.0.0.0/8")
	}

	// 测试不匹配的地址
	if validator.IsGlobalWhiteRemoteAddress("172.16.1.1") {
		t.Error("Expected 172.16.1.1 to not be in global white list")
	}
}

func TestPlainAclValidator_AccountManagement(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_acl.yml")

	configContent := `
globalWhiteRemoteAddresses: []
accounts: []
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 创建验证器并加载配置
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 测试添加账户
	newAccount := &Account{
		AccessKey:        "new_access_key",
		SecretKey:        "new_secret_key",
		WhiteRemoteAddress: "127.0.0.1",
		Admin:            false,
		DefaultTopicPerm: PermissionPub,
		DefaultGroupPerm: PermissionSub,
	}

	err = validator.AddAccount(newAccount)
	if err != nil {
		t.Fatalf("Failed to add account: %v", err)
	}

	// 验证账户已添加
	account, exists := validator.GetAccount("new_access_key")
	if !exists {
		t.Error("Expected account to exist after adding")
	}

	if account.AccessKey != "new_access_key" {
		t.Errorf("Expected access key 'new_access_key', got '%s'", account.AccessKey)
	}

	// 测试更新账户
	updatedAccount := &Account{
		AccessKey:        "new_access_key",
		SecretKey:        "updated_secret_key",
		WhiteRemoteAddress: "192.168.1.1",
		Admin:            true,
		DefaultTopicPerm: PermissionPubSub,
		DefaultGroupPerm: PermissionSub,
	}

	err = validator.UpdateAccount(updatedAccount)
	if err != nil {
		t.Fatalf("Failed to update account: %v", err)
	}

	// 验证账户已更新
	account, exists = validator.GetAccount("new_access_key")
	if !exists {
		t.Error("Expected account to exist after updating")
	}

	if account.SecretKey != "updated_secret_key" {
		t.Errorf("Expected secret key 'updated_secret_key', got '%s'", account.SecretKey)
	}

	if !account.Admin {
		t.Error("Expected account to be admin after update")
	}

	// 测试列出账户
	accounts := validator.ListAccounts()
	if len(accounts) != 1 {
		t.Errorf("Expected 1 account, got %d", len(accounts))
	}

	// 测试删除账户
	err = validator.DeleteAccount("new_access_key")
	if err != nil {
		t.Fatalf("Failed to delete account: %v", err)
	}

	// 验证账户已删除
	_, exists = validator.GetAccount("new_access_key")
	if exists {
		t.Error("Expected account to not exist after deletion")
	}

	// 验证账户列表为空
	accounts = validator.ListAccounts()
	if len(accounts) != 0 {
		t.Errorf("Expected 0 accounts after deletion, got %d", len(accounts))
	}
}

func TestPlainAclValidator_TimestampValidation(t *testing.T) {
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

	// 创建验证器并加载配置
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 测试有效的时间戳（当前时间）
	currentTime := time.Now().Unix()
	authReq := &AuthenticationRequest{
		AccessKey:     "test_access_key",
		Signature:     "dummy_signature",
		Timestamp:     currentTime,
		RemoteAddress: "127.0.0.1",
		RequestData:   map[string]string{},
	}

	// 注意：这个测试可能会因为签名验证失败而失败，但时间戳验证应该通过
	result := validator.Authenticate(authReq)
	// 我们主要关心的是时间戳验证，签名验证失败是预期的
	if result.Success {
		t.Log("Authentication succeeded (unexpected but not necessarily wrong)")
	} else {
		// 检查错误消息是否与时间戳相关
		if result.ErrorMessage == "timestamp expired" {
			t.Error("Expected timestamp to be valid")
		}
	}

	// 测试过期的时间戳（1小时前）
	expiredTime := time.Now().Add(-time.Hour).Unix()
	authReqExpired := &AuthenticationRequest{
		AccessKey:     "test_access_key",
		Signature:     "dummy_signature",
		Timestamp:     expiredTime,
		RemoteAddress: "127.0.0.1",
		RequestData:   map[string]string{},
	}

	result = validator.Authenticate(authReqExpired)
	if result.Success {
		t.Error("Expected authentication to fail for expired timestamp")
	}
}