package acl

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ConfigManager ACL配置管理器
type ConfigManager struct {
	mu         sync.RWMutex
	configPath string
	config     *AclConfig
	accountMap map[string]*Account
	lastModTime time.Time
}

// NewConfigManager 创建配置管理器
func NewConfigManager(configPath string) *ConfigManager {
	return &ConfigManager{
		configPath:  configPath,
		config:     &AclConfig{},
		accountMap: make(map[string]*Account),
	}
}

// LoadConfig 加载配置文件
func (cm *ConfigManager) LoadConfig() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 检查文件是否存在
	if _, err := os.Stat(cm.configPath); os.IsNotExist(err) {
		return NewAclException(ErrCodeConfigLoadFailed, fmt.Sprintf("config file not found: %s", cm.configPath), err)
	}

	// 读取文件内容
	data, err := ioutil.ReadFile(cm.configPath)
	if err != nil {
		return NewAclException(ErrCodeConfigLoadFailed, fmt.Sprintf("failed to read config file: %s", err.Error()), err)
	}

	// 解析YAML
	var config AclConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return NewAclException(ErrCodeConfigLoadFailed, fmt.Sprintf("failed to parse config file: %s", err.Error()), err)
	}

	// 验证配置
	if err := cm.validateConfig(&config); err != nil {
		return err
	}

	// 更新配置
	cm.config = &config
	cm.buildAccountMap()

	// 更新文件修改时间
	if stat, err := os.Stat(cm.configPath); err == nil {
		cm.lastModTime = stat.ModTime()
	}

	return nil
}

// ReloadConfig 重新加载配置（如果文件已修改）
func (cm *ConfigManager) ReloadConfig() error {
	// 检查文件修改时间
	stat, err := os.Stat(cm.configPath)
	if err != nil {
		return err
	}

	cm.mu.RLock()
	lastModTime := cm.lastModTime
	cm.mu.RUnlock()

	if stat.ModTime().After(lastModTime) {
		return cm.LoadConfig()
	}

	return nil
}

// GetConfig 获取配置
func (cm *ConfigManager) GetConfig() *AclConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.config
}

// GetAccount 获取账户
func (cm *ConfigManager) GetAccount(accessKey string) (*Account, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	account, exists := cm.accountMap[accessKey]
	return account, exists
}

// AddAccount 添加账户
func (cm *ConfigManager) AddAccount(account *Account) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 验证账户
	if err := cm.validateAccount(account); err != nil {
		return err
	}

	// 检查是否已存在
	if _, exists := cm.accountMap[account.AccessKey]; exists {
		return NewAclException(ErrCodeInvalidAccessKey, fmt.Sprintf("account already exists: %s", account.AccessKey), nil)
	}

	// 添加到配置
	cm.config.Accounts = append(cm.config.Accounts, *account)
	cm.accountMap[account.AccessKey] = account

	// 保存配置
	return cm.saveConfig()
}

// UpdateAccount 更新账户
func (cm *ConfigManager) UpdateAccount(account *Account) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 验证账户
	if err := cm.validateAccount(account); err != nil {
		return err
	}

	// 查找并更新
	found := false
	for i, acc := range cm.config.Accounts {
		if acc.AccessKey == account.AccessKey {
			cm.config.Accounts[i] = *account
			found = true
			break
		}
	}

	if !found {
		return NewAclException(ErrCodeInvalidAccessKey, fmt.Sprintf("account not found: %s", account.AccessKey), nil)
	}

	// 更新映射
	cm.accountMap[account.AccessKey] = account

	// 保存配置
	return cm.saveConfig()
}

// DeleteAccount 删除账户
func (cm *ConfigManager) DeleteAccount(accessKey string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 查找并删除
	found := false
	for i, acc := range cm.config.Accounts {
		if acc.AccessKey == accessKey {
			// 从切片中删除
			cm.config.Accounts = append(cm.config.Accounts[:i], cm.config.Accounts[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return NewAclException(ErrCodeInvalidAccessKey, fmt.Sprintf("account not found: %s", accessKey), nil)
	}

	// 从映射中删除
	delete(cm.accountMap, accessKey)

	// 保存配置
	return cm.saveConfig()
}

// ListAccounts 列出所有账户
func (cm *ConfigManager) ListAccounts() []*Account {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	accounts := make([]*Account, 0, len(cm.accountMap))
	for _, account := range cm.accountMap {
		accounts = append(accounts, account)
	}
	return accounts
}

// IsGlobalWhiteRemoteAddress 检查是否为全局白名单地址
func (cm *ConfigManager) IsGlobalWhiteRemoteAddress(remoteAddress string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, whiteAddr := range cm.config.GlobalWhiteRemoteAddresses {
		if cm.matchAddress(remoteAddress, whiteAddr) {
			return true
		}
	}
	return false
}

// IsAccountWhiteRemoteAddress 检查是否为账户白名单地址
func (cm *ConfigManager) IsAccountWhiteRemoteAddress(account *Account, remoteAddress string) bool {
	if account.WhiteRemoteAddress == "" {
		return true // 如果没有设置白名单，则允许所有地址
	}
	return cm.matchAddress(remoteAddress, account.WhiteRemoteAddress)
}

// validateConfig 验证配置
func (cm *ConfigManager) validateConfig(config *AclConfig) error {
	// 验证账户
	accessKeySet := make(map[string]bool)
	for _, account := range config.Accounts {
		// 检查重复的AccessKey
		if accessKeySet[account.AccessKey] {
			return NewAclException(ErrCodeInvalidAccessKey, fmt.Sprintf("duplicate access key: %s", account.AccessKey), nil)
		}
		accessKeySet[account.AccessKey] = true

		// 验证账户
		if err := cm.validateAccount(&account); err != nil {
			return err
		}
	}

	return nil
}

// validateAccount 验证账户
func (cm *ConfigManager) validateAccount(account *Account) error {
	if account.AccessKey == "" {
		return NewAclException(ErrCodeInvalidAccessKey, "access key cannot be empty", nil)
	}
	if account.SecretKey == "" {
		return NewAclException(ErrCodeInvalidAccessKey, "secret key cannot be empty", nil)
	}
	return nil
}

// buildAccountMap 构建账户映射
func (cm *ConfigManager) buildAccountMap() {
	cm.accountMap = make(map[string]*Account)
	for i := range cm.config.Accounts {
		account := &cm.config.Accounts[i]
		cm.accountMap[account.AccessKey] = account
	}
}

// saveConfig 保存配置到文件
func (cm *ConfigManager) saveConfig() error {
	// 创建目录（如果不存在）
	dir := filepath.Dir(cm.configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return NewAclException(ErrCodeConfigLoadFailed, fmt.Sprintf("failed to create config directory: %s", err.Error()), err)
	}

	// 序列化为YAML
	data, err := yaml.Marshal(cm.config)
	if err != nil {
		return NewAclException(ErrCodeConfigLoadFailed, fmt.Sprintf("failed to marshal config: %s", err.Error()), err)
	}

	// 写入文件
	if err := ioutil.WriteFile(cm.configPath, data, 0644); err != nil {
		return NewAclException(ErrCodeConfigLoadFailed, fmt.Sprintf("failed to write config file: %s", err.Error()), err)
	}

	// 更新修改时间
	if stat, err := os.Stat(cm.configPath); err == nil {
		cm.lastModTime = stat.ModTime()
	}

	return nil
}

// matchAddress 匹配地址（支持通配符）
func (cm *ConfigManager) matchAddress(remoteAddress, pattern string) bool {
	// 如果模式为空或为*，则匹配所有
	if pattern == "" || pattern == "*" {
		return true
	}

	// 提取IP地址（去除端口）
	remoteIP := remoteAddress
	if host, _, err := net.SplitHostPort(remoteAddress); err == nil {
		remoteIP = host
	}

	// 精确匹配
	if remoteIP == pattern {
		return true
	}

	// 通配符匹配（如192.168.1.*）
	if strings.Contains(pattern, "*") {
		// 将*替换为正则表达式
		patternRegex := strings.ReplaceAll(pattern, "*", ".*")
		patternRegex = "^" + patternRegex + "$"
		
		// 简单的通配符匹配
		if strings.HasSuffix(pattern, "*") {
			prefix := strings.TrimSuffix(pattern, "*")
			return strings.HasPrefix(remoteIP, prefix)
		}
	}

	// CIDR匹配
	if strings.Contains(pattern, "/") {
		_, cidr, err := net.ParseCIDR(pattern)
		if err != nil {
			return false
		}
		ip := net.ParseIP(remoteIP)
		if ip == nil {
			return false
		}
		return cidr.Contains(ip)
	}

	return false
}