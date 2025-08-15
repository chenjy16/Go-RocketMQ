package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

// ACLMiddleware ACL中间件
type ACLMiddleware struct {
	aclUtils        *ACLUtils
	enabled         bool
	configPath      string
	hotReload       bool
	config          *ACLConfig
	mutex           sync.RWMutex
	stopChan        chan struct{}
	reloadInterval  time.Duration
}

// ACLConfig ACL配置结构
type ACLConfig struct {
	AccessKey       string `json:"accessKey"`
	SecretKey       string `json:"secretKey"`
	SecurityToken   string `json:"securityToken"`
	SignatureMethod string `json:"signatureMethod"`
	Enabled         bool   `json:"enabled"`
	TimestampTolerance int64 `json:"timestampTolerance"` // 时间戳容忍度（毫秒）
}

// NewACLMiddleware 创建ACL中间件
func NewACLMiddleware(accessKey, secretKey string, signatureMethod ACLSignatureMethod, enabled bool) *ACLMiddleware {
	aclUtils := NewACLUtils(accessKey, secretKey, signatureMethod)
	return &ACLMiddleware{
		aclUtils:       aclUtils,
		enabled:        enabled,
		stopChan:       make(chan struct{}),
		reloadInterval: 30 * time.Second, // 默认30秒检查一次配置文件
		config: &ACLConfig{
			AccessKey:       accessKey,
			SecretKey:       secretKey,
			SignatureMethod: string(signatureMethod),
			Enabled:         enabled,
			TimestampTolerance: 300000, // 默认5分钟
		},
	}
}

// NewACLMiddlewareFromConfig 从配置文件创建ACL中间件
func NewACLMiddlewareFromConfig(configPath string, hotReload bool) (*ACLMiddleware, error) {
	middleware := &ACLMiddleware{
		configPath:     configPath,
		hotReload:      hotReload,
		stopChan:       make(chan struct{}),
		reloadInterval: 30 * time.Second,
	}

	// 加载配置
	if err := middleware.loadConfig(); err != nil {
		return nil, fmt.Errorf("failed to load ACL config: %v", err)
	}

	// 启动热重载
	if hotReload {
		go middleware.startHotReload()
	}

	return middleware, nil
}

// loadConfig 加载ACL配置
func (acl *ACLMiddleware) loadConfig() error {
	if acl.configPath == "" {
		return fmt.Errorf("config path is empty")
	}

	data, err := ioutil.ReadFile(acl.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %v", err)
	}

	var config ACLConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse config file: %v", err)
	}

	// 验证配置
	if err := ValidateACLConfig(config.AccessKey, config.SecretKey); err != nil {
		return fmt.Errorf("invalid ACL config: %v", err)
	}

	acl.mutex.Lock()
	defer acl.mutex.Unlock()

	acl.config = &config
	acl.enabled = config.Enabled

	// 更新ACL工具
	signatureMethod := ACLSignatureMethod(config.SignatureMethod)
	if signatureMethod == "" {
		signatureMethod = HmacSHA1
	}
	acl.aclUtils = NewACLUtils(config.AccessKey, config.SecretKey, signatureMethod)

	log.Printf("ACL config loaded successfully from %s", acl.configPath)
	return nil
}

// startHotReload 启动配置热重载
func (acl *ACLMiddleware) startHotReload() {
	ticker := time.NewTicker(acl.reloadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := acl.reloadConfigIfChanged(); err != nil {
				log.Printf("Failed to reload ACL config: %v", err)
			}
		case <-acl.stopChan:
			return
		}
	}
}

// reloadConfigIfChanged 检查并重载配置文件
func (acl *ACLMiddleware) reloadConfigIfChanged() error {
	if acl.configPath == "" {
		return nil
	}

	// 检查文件是否存在
	if _, err := os.Stat(acl.configPath); os.IsNotExist(err) {
		return nil
	}

	// 重新加载配置
	return acl.loadConfig()
}

// StopHotReload 停止配置热重载
func (acl *ACLMiddleware) StopHotReload() {
	close(acl.stopChan)
}

// IsEnabled 检查ACL是否启用
func (acl *ACLMiddleware) IsEnabled() bool {
	acl.mutex.RLock()
	defer acl.mutex.RUnlock()
	return acl.enabled
}

// SetEnabled 设置ACL启用状态
func (acl *ACLMiddleware) SetEnabled(enabled bool) {
	acl.mutex.Lock()
	defer acl.mutex.Unlock()
	acl.enabled = enabled
}

// GenerateAuthHeaders 生成认证头
func (acl *ACLMiddleware) GenerateAuthHeaders(topic, consumerGroup, msgId string) (map[string]string, error) {
	if !acl.IsEnabled() {
		return nil, nil
	}

	acl.mutex.RLock()
	defer acl.mutex.RUnlock()

	if acl.aclUtils == nil {
		return nil, fmt.Errorf("ACL utils not initialized")
	}

	// 生成时间戳
	timestamp := GenerateTimestamp()

	// 构建请求数据
	requestData := acl.aclUtils.BuildRequestData(topic, consumerGroup, msgId, timestamp)

	// 生成签名
	signature, err := acl.aclUtils.GenerateSignature(requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to generate signature: %v", err)
	}

	// 构建认证头
	headers := map[string]string{
		"AccessKey":       acl.aclUtils.GetAccessKey(),
		"Signature":       signature,
		"SignatureMethod": string(acl.aclUtils.GetSignatureMethod()),
		"Timestamp":       fmt.Sprintf("%d", timestamp),
	}

	// 添加安全令牌（如果有）
	if acl.config.SecurityToken != "" {
		headers["SecurityToken"] = acl.config.SecurityToken
	}

	return headers, nil
}

// VerifyAuthHeaders 验证认证头
func (acl *ACLMiddleware) VerifyAuthHeaders(headers map[string]string, topic, consumerGroup, msgId string) error {
	if !acl.IsEnabled() {
		return nil
	}

	acl.mutex.RLock()
	defer acl.mutex.RUnlock()

	if acl.aclUtils == nil {
		return fmt.Errorf("ACL utils not initialized")
	}

	// 检查必需的头部字段
	accessKey, ok := headers["AccessKey"]
	if !ok || accessKey == "" {
		return fmt.Errorf("missing AccessKey header")
	}

	signature, ok := headers["Signature"]
	if !ok || signature == "" {
		return fmt.Errorf("missing Signature header")
	}

	timestampStr, ok := headers["Timestamp"]
	if !ok || timestampStr == "" {
		return fmt.Errorf("missing Timestamp header")
	}

	// 验证访问密钥
	if accessKey != acl.aclUtils.GetAccessKey() {
		return fmt.Errorf("invalid AccessKey")
	}

	// 验证时间戳
	timestamp, err := parseTimestamp(timestampStr)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %v", err)
	}

	if !IsTimestampValid(timestamp, acl.config.TimestampTolerance) {
		return fmt.Errorf("timestamp expired or invalid")
	}

	// 构建请求数据进行签名验证
	requestData := acl.aclUtils.BuildRequestData(topic, consumerGroup, msgId, timestamp)

	// 验证签名
	if !acl.aclUtils.VerifySignature(requestData, signature) {
		return fmt.Errorf("signature verification failed")
	}

	return nil
}

// GetConfig 获取当前配置
func (acl *ACLMiddleware) GetConfig() *ACLConfig {
	acl.mutex.RLock()
	defer acl.mutex.RUnlock()
	if acl.config == nil {
		return nil
	}
	// 返回配置副本，隐藏敏感信息
	return &ACLConfig{
		AccessKey:       acl.config.AccessKey,
		SecretKey:       "***", // 隐藏密钥
		SecurityToken:   "***", // 隐藏令牌
		SignatureMethod: acl.config.SignatureMethod,
		Enabled:         acl.config.Enabled,
		TimestampTolerance: acl.config.TimestampTolerance,
	}
}

// parseTimestamp 解析时间戳字符串
func parseTimestamp(timestampStr string) (int64, error) {
	var timestamp int64
	if _, err := fmt.Sscanf(timestampStr, "%d", &timestamp); err != nil {
		return 0, err
	}
	return timestamp, nil
}

// SetReloadInterval 设置配置重载间隔
func (acl *ACLMiddleware) SetReloadInterval(interval time.Duration) {
	acl.reloadInterval = interval
}