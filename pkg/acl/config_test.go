package acl

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_acl.yml")

	configContent := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"
  - "192.168.1.*"

accounts:
  - accessKey: "test_access_key"
    secretKey: "test_secret_key"
    whiteRemoteAddress: "127.0.0.1"
    admin: true
    defaultTopicPerm: "DENY"
    defaultGroupPerm: "SUB"
    topicPerms:
      - "test_topic=PUB"
    groupPerms:
      - "test_group=SUB"
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	// 测试加载配置
	cm := NewConfigManager(configFile)
	err = cm.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	config := cm.GetConfig()

	// 验证全局白名单
	if len(config.GlobalWhiteRemoteAddresses) != 2 {
		t.Errorf("Expected 2 global white addresses, got %d", len(config.GlobalWhiteRemoteAddresses))
	}

	if config.GlobalWhiteRemoteAddresses[0] != "127.0.0.1" {
		t.Errorf("Expected first global white address to be '127.0.0.1', got '%s'", config.GlobalWhiteRemoteAddresses[0])
	}

	// 验证账户配置
	if len(config.Accounts) != 1 {
		t.Errorf("Expected 1 account, got %d", len(config.Accounts))
	}

	account := config.Accounts[0]
	if account.AccessKey != "test_access_key" {
		t.Errorf("Expected access key 'test_access_key', got '%s'", account.AccessKey)
	}

	if account.SecretKey != "test_secret_key" {
		t.Errorf("Expected secret key 'test_secret_key', got '%s'", account.SecretKey)
	}

	if !account.Admin {
		t.Error("Expected admin to be true")
	}

	if account.DefaultTopicPerm != "DENY" {
		t.Errorf("Expected default topic perm 'DENY', got '%s'", account.DefaultTopicPerm)
	}

	if len(account.TopicPerms) != 1 {
		t.Errorf("Expected 1 topic perm, got %d", len(account.TopicPerms))
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	cm := NewConfigManager("/nonexistent/path/config.yml")
	err := cm.LoadConfig()
	if err == nil {
		t.Error("Expected error for non-existent config file")
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "invalid.yml")

	// 创建无效的YAML文件
	invalidContent := `
globalWhiteRemoteAddresses:
  - "127.0.0.1"
accounts:
  - accessKey: "test
    secretKey: "invalid yaml
`

	err := os.WriteFile(configFile, []byte(invalidContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create invalid config file: %v", err)
	}

	cm := NewConfigManager(configFile)
	err = cm.LoadConfig()
	if err == nil {
		t.Error("Expected error for invalid YAML")
	}
}