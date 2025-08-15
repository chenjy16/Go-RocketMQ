package acl

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PlainAclValidator 基于配置文件的ACL验证器
type PlainAclValidator struct {
	configManager *ConfigManager
	signatureGen  SignatureGenerator
}

// NewPlainAclValidator 创建ACL验证器
func NewPlainAclValidator(configPath string) *PlainAclValidator {
	return &PlainAclValidator{
		configManager: NewConfigManager(configPath),
		signatureGen:  NewHmacSHA1SignatureGenerator(),
	}
}

// LoadConfig 加载配置
func (v *PlainAclValidator) LoadConfig(configPath string) error {
	v.configManager = NewConfigManager(configPath)
	return v.configManager.LoadConfig()
}

// ReloadConfig 重新加载配置
func (v *PlainAclValidator) ReloadConfig() error {
	return v.configManager.ReloadConfig()
}

// Authenticate 认证
func (v *PlainAclValidator) Authenticate(req *AuthenticationRequest) *AuthenticationResult {
	// 检查AccessKey是否存在
	if req.AccessKey == "" {
		return &AuthenticationResult{
			Success:      false,
			ErrorMessage: "no access key provided",
		}
	}

	// 获取账户信息
	account, exists := v.configManager.GetAccount(req.AccessKey)
	if !exists {
		return &AuthenticationResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("invalid access key: %s", req.AccessKey),
		}
	}

	// 检查是否为全局白名单地址或账户白名单地址
	if !v.IsGlobalWhiteRemoteAddress(req.RemoteAddress) && !v.configManager.IsAccountWhiteRemoteAddress(account, req.RemoteAddress) {
		return &AuthenticationResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("remote address not allowed: %s", req.RemoteAddress),
		}
	}

	// 验证时间戳（防重放攻击）
	if err := v.validateTimestamp(req.Timestamp); err != nil {
		return &AuthenticationResult{
			Success:      false,
			ErrorMessage: err.Error(),
		}
	}

	// 验证签名
	if !v.signatureGen.VerifySignature(account.SecretKey, req.RequestData, req.Signature) {
		return &AuthenticationResult{
			Success:      false,
			ErrorMessage: "signature verification failed",
		}
	}

	return &AuthenticationResult{
		Success: true,
		Account: account,
	}
}

// CheckPermission 检查权限
func (v *PlainAclValidator) CheckPermission(req *PermissionCheckRequest) *PermissionCheckResult {
	// 如果没有账户信息，检查是否为全局白名单地址
	if req.Account == nil {
		if v.IsGlobalWhiteRemoteAddress(req.RemoteAddress) {
			return &PermissionCheckResult{Allowed: true}
		}
		return &PermissionCheckResult{
			Allowed:      false,
			ErrorMessage: "no account information provided",
		}
	}

	// 检查是否为全局白名单地址或账户白名单地址
	if !v.IsGlobalWhiteRemoteAddress(req.RemoteAddress) && !v.configManager.IsAccountWhiteRemoteAddress(req.Account, req.RemoteAddress) {
		return &PermissionCheckResult{
			Allowed:      false,
			ErrorMessage: fmt.Sprintf("remote address not allowed: %s", req.RemoteAddress),
		}
	}

	// 如果有账户信息，进行权限检查

	// 管理员拥有所有权限
	if req.Account.Admin {
		return &PermissionCheckResult{Allowed: true}
	}

	// 检查资源权限
	var permission Permission
	var found bool

	if req.ResourceType == "topic" {
		permission, found = v.getTopicPermission(req.Account, req.Resource)
		if !found {
			permission = req.Account.DefaultTopicPerm
		}
	} else if req.ResourceType == "group" {
		permission, found = v.getGroupPermission(req.Account, req.Resource)
		if !found {
			permission = req.Account.DefaultGroupPerm
		}
	} else {
		return &PermissionCheckResult{
			Allowed:      false,
			ErrorMessage: fmt.Sprintf("unknown resource type: %s", req.ResourceType),
		}
	}

	// 检查权限
	allowed := v.checkOperationPermission(permission, req.Operation)
	if !allowed {
		return &PermissionCheckResult{
			Allowed:      false,
			ErrorMessage: fmt.Sprintf("operation %s not allowed for %s %s", req.Operation, req.ResourceType, req.Resource),
		}
	}

	return &PermissionCheckResult{Allowed: true}
}

// IsGlobalWhiteRemoteAddress 检查是否为全局白名单地址
func (v *PlainAclValidator) IsGlobalWhiteRemoteAddress(remoteAddress string) bool {
	return v.configManager.IsGlobalWhiteRemoteAddress(remoteAddress)
}

// GetAccount 获取账户（实现AclManager接口）
func (v *PlainAclValidator) GetAccount(accessKey string) (*Account, bool) {
	return v.configManager.GetAccount(accessKey)
}

// AddAccount 添加账户（实现AclManager接口）
func (v *PlainAclValidator) AddAccount(account *Account) error {
	return v.configManager.AddAccount(account)
}

// UpdateAccount 更新账户（实现AclManager接口）
func (v *PlainAclValidator) UpdateAccount(account *Account) error {
	return v.configManager.UpdateAccount(account)
}

// DeleteAccount 删除账户（实现AclManager接口）
func (v *PlainAclValidator) DeleteAccount(accessKey string) error {
	return v.configManager.DeleteAccount(accessKey)
}

// ListAccounts 列出所有账户（实现AclManager接口）
func (v *PlainAclValidator) ListAccounts() []*Account {
	return v.configManager.ListAccounts()
}

// validateTimestamp 验证时间戳
func (v *PlainAclValidator) validateTimestamp(timestamp int64) error {
	if timestamp == 0 {
		return nil // 如果没有提供时间戳，跳过验证
	}

	now := time.Now().Unix()
	diff := now - timestamp

	// 允许5分钟的时间偏差
	if diff > 300 || diff < -300 {
		return NewAclException(ErrCodeSignatureFailed, "request timestamp is too old or too new", nil)
	}

	return nil
}

// getTopicPermission 获取Topic权限
func (v *PlainAclValidator) getTopicPermission(account *Account, topicName string) (Permission, bool) {
	for _, perm := range account.TopicPerms {
		parts := strings.Split(perm, "=")
		if len(parts) == 2 && parts[0] == topicName {
			return Permission(parts[1]), true
		}
	}
	return "", false
}

// getGroupPermission 获取Group权限
func (v *PlainAclValidator) getGroupPermission(account *Account, groupName string) (Permission, bool) {
	for _, perm := range account.GroupPerms {
		parts := strings.Split(perm, "=")
		if len(parts) == 2 && parts[0] == groupName {
			return Permission(parts[1]), true
		}
	}
	return "", false
}

// checkOperationPermission 检查操作权限
func (v *PlainAclValidator) checkOperationPermission(permission Permission, operation string) bool {
	switch permission {
	case PermissionDeny:
		return false
	case PermissionPub:
		return operation == "pub" || operation == "publish"
	case PermissionSub:
		return operation == "sub" || operation == "subscribe"
	case PermissionPubSub:
		return operation == "pub" || operation == "publish" || operation == "sub" || operation == "subscribe"
	default:
		return false
	}
}

// CreateAuthenticationRequest 创建认证请求
func CreateAuthenticationRequest(accessKey, signature string, timestamp int64, remoteAddress string, requestData map[string]string) *AuthenticationRequest {
	return &AuthenticationRequest{
		AccessKey:     accessKey,
		Signature:     signature,
		Timestamp:     timestamp,
		RemoteAddress: remoteAddress,
		RequestData:   requestData,
	}
}

// CreatePermissionCheckRequest 创建权限检查请求
func CreatePermissionCheckRequest(account *Account, resource, resourceType, operation, remoteAddress string) *PermissionCheckRequest {
	return &PermissionCheckRequest{
		Account:       account,
		Resource:      resource,
		ResourceType:  resourceType,
		Operation:     operation,
		RemoteAddress: remoteAddress,
	}
}

// ParseTimestampFromString 从字符串解析时间戳
func ParseTimestampFromString(timestampStr string) (int64, error) {
	if timestampStr == "" {
		return 0, nil
	}
	return strconv.ParseInt(timestampStr, 10, 64)
}