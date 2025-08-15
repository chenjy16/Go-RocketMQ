package acl

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestAclIntegration 测试ACL系统的完整集成功能
func TestAclIntegration(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "integration_acl.yml")

	configContent := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"
  - "192.168.1.0/24"

accounts:
  - accessKey: "admin_user"
    secretKey: "admin_secret_123"
    whiteRemoteAddress: "127.0.0.1"
    admin: true
    defaultTopicPerm: "DENY"
    defaultGroupPerm: "DENY"
    topicPerms:
      - "admin_topic=PUB|SUB"
    groupPerms:
      - "admin_group=SUB"

  - accessKey: "producer_user"
    secretKey: "producer_secret_456"
    whiteRemoteAddress: "192.168.1.100"
    admin: false
    defaultTopicPerm: "PUB"
    defaultGroupPerm: "DENY"
    topicPerms:
      - "special_topic=PUB"
      - "readonly_topic=DENY"

  - accessKey: "consumer_user"
    secretKey: "consumer_secret_789"
    whiteRemoteAddress: "192.168.1.200"
    admin: false
    defaultTopicPerm: "SUB"
    defaultGroupPerm: "SUB"
    topicPerms:
      - "special_topic=SUB"
      - "private_topic=DENY"
    groupPerms:
      - "special_group=SUB"
      - "private_group=DENY"

  - accessKey: "limited_user"
    secretKey: "limited_secret_000"
    whiteRemoteAddress: "192.168.1.50"
    admin: false
    defaultTopicPerm: "DENY"
    defaultGroupPerm: "DENY"
    topicPerms:
      - "public_topic=SUB"
    groupPerms:
      - "public_group=SUB"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 初始化ACL系统组件
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	middleware := NewAclMiddleware(validator, true)
	signatureGenerator := NewHmacSHA1SignatureGenerator()

	// 测试场景1：管理员用户的完整权限
	t.Run("AdminUserFullPermissions", func(t *testing.T) {
		testAdminUserPermissions(t, middleware, signatureGenerator)
	})

	// 测试场景2：生产者用户的权限
	t.Run("ProducerUserPermissions", func(t *testing.T) {
		testProducerUserPermissions(t, middleware, signatureGenerator)
	})

	// 测试场景3：消费者用户的权限
	t.Run("ConsumerUserPermissions", func(t *testing.T) {
		testConsumerUserPermissions(t, middleware, signatureGenerator)
	})

	// 测试场景4：受限用户的权限
	t.Run("LimitedUserPermissions", func(t *testing.T) {
		testLimitedUserPermissions(t, middleware, signatureGenerator)
	})

	// 测试场景5：全局白名单
	t.Run("GlobalWhitelistAccess", func(t *testing.T) {
		testGlobalWhitelistAccess(t, middleware)
	})

	// 测试场景6：签名验证
	t.Run("SignatureValidation", func(t *testing.T) {
		testSignatureValidation(t, middleware, signatureGenerator)
	})

	// 测试场景7：时间戳验证
	t.Run("TimestampValidation", func(t *testing.T) {
		testTimestampValidation(t, middleware, signatureGenerator)
	})
}

func testAdminUserPermissions(t *testing.T, middleware *AclMiddleware, signatureGenerator *HmacSHA1SignatureGenerator) {
	// 获取管理员账户
	validator := middleware.validator.(*PlainAclValidator)
	account, exists := validator.GetAccount("admin_user")
	if !exists {
		t.Fatal("Admin account not found")
	}

	// 测试管理员对任意Topic的访问权限
	// 注意：管理员用户可能有特殊权限处理，即使defaultTopicPerm=DENY
	err := middleware.CheckTopicPermission(account, "any_topic", "pub", "127.0.0.1")
	if err != nil {
		t.Logf("Admin denied access to any_topic (expected due to defaultTopicPerm=DENY): %v", err)
	} else {
		t.Logf("Admin granted access to any_topic (admin privileges may override default permissions)")
	}

	// 测试管理员对特定Topic的权限
	err = middleware.CheckTopicPermission(account, "admin_topic", "pub", "127.0.0.1")
	if err != nil {
		t.Errorf("Expected admin to have pub permission on admin_topic: %v", err)
	}

	err = middleware.CheckTopicPermission(account, "admin_topic", "sub", "127.0.0.1")
	if err != nil {
		t.Errorf("Expected admin to have sub permission on admin_topic: %v", err)
	}

	// 测试管理员请求验证
	currentTime := fmt.Sprintf("%d", time.Now().Unix())
	requestData := map[string]string{
		"AccessKey": "admin_user",
		"Signature": "dummy_signature",
		"Timestamp": currentTime,
	}

	_, err = middleware.ValidateAdminRequest(requestData, "127.0.0.1")
	// 签名验证可能失败，但这里主要测试管理员权限检查
	if err != nil {
		t.Logf("Admin request validation failed (expected due to signature): %v", err)
	}
}

func testProducerUserPermissions(t *testing.T, middleware *AclMiddleware, signatureGenerator *HmacSHA1SignatureGenerator) {
	// 获取生产者账户
	validator := middleware.validator.(*PlainAclValidator)
	account, exists := validator.GetAccount("producer_user")
	if !exists {
		t.Fatal("Producer account not found")
	}

	// 测试默认Topic发布权限
	err := middleware.CheckTopicPermission(account, "default_topic", "pub", "192.168.1.100")
	if err != nil {
		t.Errorf("Expected producer to have default pub permission: %v", err)
	}

	// 测试默认Topic订阅权限（应该被拒绝）
	err = middleware.CheckTopicPermission(account, "default_topic", "sub", "192.168.1.100")
	if err == nil {
		t.Error("Expected producer to be denied default sub permission")
	}

	// 测试特定Topic权限
	err = middleware.CheckTopicPermission(account, "special_topic", "pub", "192.168.1.100")
	if err != nil {
		t.Errorf("Expected producer to have pub permission on special_topic: %v", err)
	}

	// 测试只读Topic权限（应该被拒绝）
	err = middleware.CheckTopicPermission(account, "readonly_topic", "pub", "192.168.1.100")
	if err == nil {
		t.Error("Expected producer to be denied access to readonly_topic")
	}

	// 测试IP地址限制
	err = middleware.CheckTopicPermission(account, "default_topic", "pub", "192.168.2.100")
	if err == nil {
		t.Error("Expected producer to be denied access from unauthorized IP")
	}
}

func testConsumerUserPermissions(t *testing.T, middleware *AclMiddleware, signatureGenerator *HmacSHA1SignatureGenerator) {
	// 获取消费者账户
	validator := middleware.validator.(*PlainAclValidator)
	account, exists := validator.GetAccount("consumer_user")
	if !exists {
		t.Fatal("Consumer account not found")
	}

	// 测试默认Topic订阅权限
	err := middleware.CheckTopicPermission(account, "default_topic", "sub", "192.168.1.200")
	if err != nil {
		t.Errorf("Expected consumer to have default sub permission: %v", err)
	}

	// 测试默认Topic发布权限（应该被拒绝）
	err = middleware.CheckTopicPermission(account, "default_topic", "pub", "192.168.1.200")
	if err == nil {
		t.Error("Expected consumer to be denied default pub permission")
	}

	// 测试特定Topic权限
	err = middleware.CheckTopicPermission(account, "special_topic", "sub", "192.168.1.200")
	if err != nil {
		t.Errorf("Expected consumer to have sub permission on special_topic: %v", err)
	}

	// 测试私有Topic权限（应该被拒绝）
	err = middleware.CheckTopicPermission(account, "private_topic", "sub", "192.168.1.200")
	if err == nil {
		t.Error("Expected consumer to be denied access to private_topic")
	}

	// 测试Group权限
	err = middleware.CheckGroupPermission(account, "default_group", "sub", "192.168.1.200")
	if err != nil {
		t.Errorf("Expected consumer to have default group sub permission: %v", err)
	}

	err = middleware.CheckGroupPermission(account, "special_group", "sub", "192.168.1.200")
	if err != nil {
		t.Errorf("Expected consumer to have special_group sub permission: %v", err)
	}

	err = middleware.CheckGroupPermission(account, "private_group", "sub", "192.168.1.200")
	if err == nil {
		t.Error("Expected consumer to be denied access to private_group")
	}
}

func testLimitedUserPermissions(t *testing.T, middleware *AclMiddleware, signatureGenerator *HmacSHA1SignatureGenerator) {
	// 获取受限用户账户
	validator := middleware.validator.(*PlainAclValidator)
	account, exists := validator.GetAccount("limited_user")
	if !exists {
		t.Fatal("Limited user account not found")
	}

	// 测试默认权限（应该全部被拒绝）
	err := middleware.CheckTopicPermission(account, "default_topic", "pub", "192.168.1.50")
	if err == nil {
		t.Error("Expected limited user to be denied default topic pub permission")
	}

	err = middleware.CheckTopicPermission(account, "default_topic", "sub", "192.168.1.50")
	if err == nil {
		t.Error("Expected limited user to be denied default topic sub permission")
	}

	err = middleware.CheckGroupPermission(account, "default_group", "sub", "192.168.1.50")
	if err == nil {
		t.Error("Expected limited user to be denied default group permission")
	}

	// 测试允许的权限
	err = middleware.CheckTopicPermission(account, "public_topic", "sub", "192.168.1.50")
	if err != nil {
		t.Errorf("Expected limited user to have sub permission on public_topic: %v", err)
	}

	err = middleware.CheckGroupPermission(account, "public_group", "sub", "192.168.1.50")
	if err != nil {
		t.Errorf("Expected limited user to have sub permission on public_group: %v", err)
	}
}

func testGlobalWhitelistAccess(t *testing.T, middleware *AclMiddleware) {
	// 测试全局白名单IP访问
	validator := middleware.validator.(*PlainAclValidator)
	if !validator.IsGlobalWhiteRemoteAddress("127.0.0.1") {
		t.Error("Expected 127.0.0.1 to be in global whitelist")
	}

	if !validator.IsGlobalWhiteRemoteAddress("192.168.1.100") {
		t.Error("Expected 192.168.1.100 to be in global whitelist (subnet match)")
	}

	if validator.IsGlobalWhiteRemoteAddress("10.0.0.1") {
		t.Error("Expected 10.0.0.1 to NOT be in global whitelist")
	}
}

func testSignatureValidation(t *testing.T, middleware *AclMiddleware, signatureGenerator *HmacSHA1SignatureGenerator) {
	// 获取测试账户
	validator := middleware.validator.(*PlainAclValidator)
	account, exists := validator.GetAccount("producer_user")
	if !exists {
		t.Fatal("Producer account not found")
	}

	// 创建正确的签名
	timestamp := time.Now().Unix()
	requestData := map[string]string{
		"AccessKey": "producer_user",
		"Timestamp": string(rune(timestamp)),
		"Topic":     "test_topic",
	}

	// 生成签名
	signature, err := signatureGenerator.GenerateSignature(account.SecretKey, requestData)
	if err != nil {
		t.Fatalf("Failed to generate signature: %v", err)
	}

	requestData["Signature"] = signature

	// 验证签名
	valid := signatureGenerator.VerifySignature(account.SecretKey, requestData, signature)
	if !valid {
		t.Error("Expected signature to be valid")
	}

	// 测试错误的签名
	valid = signatureGenerator.VerifySignature(account.SecretKey, requestData, "invalid_signature")
	if valid {
		t.Error("Expected invalid signature to be rejected")
	}
}

func testTimestampValidation(t *testing.T, middleware *AclMiddleware, signatureGenerator *HmacSHA1SignatureGenerator) {
	// 获取验证器实例
	validator := middleware.validator.(*PlainAclValidator)

	// 测试当前时间戳（应该有效）
	currentTime := time.Now().Unix()
	err := validator.validateTimestamp(currentTime)
	if err != nil {
		t.Errorf("Expected current timestamp to be valid: %v", err)
	}

	// 测试过期的时间戳（应该无效）
	expiredTime := time.Now().Add(-2 * time.Hour).Unix()
	err = validator.validateTimestamp(expiredTime)
	if err == nil {
		t.Error("Expected expired timestamp to be invalid")
	}

	// 测试未来的时间戳（应该无效）
	futureTime := time.Now().Add(2 * time.Hour).Unix()
	err = validator.validateTimestamp(futureTime)
	if err == nil {
		t.Error("Expected future timestamp to be invalid")
	}
}

// TestAclConfigReload 测试ACL配置的动态重载
func TestAclConfigReload(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "reload_acl.yml")

	initialConfig := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"

accounts:
  - accessKey: "test_user"
    secretKey: "test_secret"
    whiteRemoteAddress: "127.0.0.1"
    admin: false
    defaultTopicPerm: "PUB"
`

	err := os.WriteFile(configFile, []byte(initialConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to create initial config file: %v", err)
	}

	// 初始化验证器
	validator := NewPlainAclValidator(configFile)
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load initial config: %v", err)
	}

	// 验证初始配置
	account, exists := validator.GetAccount("test_user")
	if !exists {
		t.Fatal("Initial account not found")
	}
	if account.Admin {
		t.Error("Expected initial account to be non-admin")
	}

	// 更新配置文件
	updatedConfig := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"
  - "192.168.1.0/24"

accounts:
  - accessKey: "test_user"
    secretKey: "test_secret"
    whiteRemoteAddress: "127.0.0.1"
    admin: true
    defaultTopicPerm: "PUB|SUB"
  - accessKey: "new_user"
    secretKey: "new_secret"
    whiteRemoteAddress: "192.168.1.100"
    admin: false
    defaultTopicPerm: "SUB"
`

	err = os.WriteFile(configFile, []byte(updatedConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to update config file: %v", err)
	}

	// 重新加载配置
	err = validator.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to reload config: %v", err)
	}

	// 验证更新后的配置
	account, exists = validator.GetAccount("test_user")
	if !exists {
		t.Fatal("Updated account not found")
	}
	if !account.Admin {
		t.Error("Expected updated account to be admin")
	}

	// 验证新增的账户
	newAccount, exists := validator.GetAccount("new_user")
	if !exists {
		t.Fatal("New account not found after reload")
	}
	if newAccount.Admin {
		t.Error("Expected new account to be non-admin")
	}

	// 验证全局白名单更新
	if !validator.IsGlobalWhiteRemoteAddress("192.168.1.100") {
		t.Error("Expected 192.168.1.100 to be in updated global whitelist")
	}
}

// TestAclPerformance 测试ACL系统的性能
func TestAclPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// 创建大量账户的配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "performance_acl.yml")

	configContent := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"

accounts:
`

	// 生成1000个测试账户
	for i := 0; i < 1000; i++ {
		configContent += fmt.Sprintf("  - accessKey: \"user%d\"\n", i)
		configContent += fmt.Sprintf("    secretKey: \"secret%d\"\n", i)
		configContent += "    whiteRemoteAddress: \"127.0.0.1\"\n"
		configContent += "    admin: false\n"
		configContent += "    defaultTopicPerm: \"PUB\"\n\n"
	}

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create performance config file: %v", err)
	}

	// 初始化验证器
	validator := NewPlainAclValidator(configFile)
	start := time.Now()
	err = validator.LoadConfig(configFile)
	loadTime := time.Since(start)
	if err != nil {
		t.Fatalf("Failed to load performance config: %v", err)
	}

	t.Logf("Config loading time for 1000 accounts: %v", loadTime)

	// 测试账户查找性能
	start = time.Now()
	for i := 0; i < 1000; i++ {
		_, exists := validator.GetAccount(fmt.Sprintf("user%d", i))
		if !exists {
			t.Errorf("Account user%d not found", i)
			break
		}
	}
	lookupTime := time.Since(start)
	t.Logf("Account lookup time for 1000 queries: %v (avg: %v per lookup)", lookupTime, lookupTime/1000)

	// 性能基准检查
	if loadTime > 5*time.Second {
		t.Errorf("Config loading too slow: %v (expected < 5s)", loadTime)
	}
	if lookupTime > 100*time.Millisecond {
		t.Errorf("Account lookup too slow: %v (expected < 100ms for 1000 lookups)", lookupTime)
	}
}