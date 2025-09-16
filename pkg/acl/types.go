package acl

import (
	"time"
)

// Permission 权限类型
type Permission string

const (
	// PermissionDeny 拒绝访问
	PermissionDeny Permission = "DENY"
	// PermissionPub 发布权限
	PermissionPub Permission = "PUB"
	// PermissionSub 订阅权限
	PermissionSub Permission = "SUB"
	// PermissionPubSub 发布和订阅权限
	PermissionPubSub Permission = "PUB|SUB"
)

// RateLimit 速率限制配置
type RateLimit struct {
	// RequestsPerSecond 每秒请求数
	RequestsPerSecond int `yaml:"requestsPerSecond" json:"requestsPerSecond"`
	// Burst 突发请求数
	Burst int `yaml:"burst" json:"burst"`
}

// ResourceQuota 资源配额
type ResourceQuota struct {
	// MaxTopics 最大Topic数
	MaxTopics int `yaml:"maxTopics" json:"maxTopics"`
	// MaxGroups 最大Group数
	MaxGroups int `yaml:"maxGroups" json:"maxGroups"`
	// MaxConnections 最大连接数
	MaxConnections int `yaml:"maxConnections" json:"maxConnections"`
	// MaxMessageSize 最大消息大小
	MaxMessageSize int64 `yaml:"maxMessageSize" json:"maxMessageSize"`
}

// Account ACL账户信息
type Account struct {
	// AccessKey 访问密钥
	AccessKey string `yaml:"accessKey" json:"accessKey"`
	// SecretKey 秘密密钥
	SecretKey string `yaml:"secretKey" json:"secretKey"`
	// WhiteRemoteAddress 白名单IP地址
	WhiteRemoteAddress string `yaml:"whiteRemoteAddress" json:"whiteRemoteAddress"`
	// BlackRemoteAddress 黑名单IP地址
	BlackRemoteAddress string `yaml:"blackRemoteAddress" json:"blackRemoteAddress"`
	// Admin 是否为管理员
	Admin bool `yaml:"admin" json:"admin"`
	// ReadOnly 是否为只读用户
	ReadOnly bool `yaml:"readOnly" json:"readOnly"`
	// ExpiredAt 过期时间
	ExpiredAt *time.Time `yaml:"expiredAt" json:"expiredAt"`
	// DefaultTopicPerm 默认Topic权限
	DefaultTopicPerm Permission `yaml:"defaultTopicPerm" json:"defaultTopicPerm"`
	// DefaultGroupPerm 默认Group权限
	DefaultGroupPerm Permission `yaml:"defaultGroupPerm" json:"defaultGroupPerm"`
	// TopicPerms Topic权限列表
	TopicPerms []string `yaml:"topicPerms" json:"topicPerms"`
	// GroupPerms Group权限列表
	GroupPerms []string `yaml:"groupPerms" json:"groupPerms"`
	// RateLimit 速率限制
	RateLimit *RateLimit `yaml:"rateLimit" json:"rateLimit"`
	// ResourceQuota 资源配额
	ResourceQuota *ResourceQuota `yaml:"resourceQuota" json:"resourceQuota"`
	// Enabled 是否启用
	Enabled bool `yaml:"enabled" json:"enabled"`
}

// AclConfig ACL配置
type AclConfig struct {
	// GlobalWhiteRemoteAddresses 全局白名单IP地址
	GlobalWhiteRemoteAddresses []string `yaml:"globalWhiteRemoteAddresses" json:"globalWhiteRemoteAddresses"`
	// GlobalBlackRemoteAddresses 全局黑名单IP地址
	GlobalBlackRemoteAddresses []string `yaml:"globalBlackRemoteAddresses" json:"globalBlackRemoteAddresses"`
	// Accounts 账户列表
	Accounts []Account `yaml:"accounts" json:"accounts"`
	// DefaultRateLimit 默认速率限制
	DefaultRateLimit *RateLimit `yaml:"defaultRateLimit" json:"defaultRateLimit"`
	// AuditEnabled 是否启用审计
	AuditEnabled bool `yaml:"auditEnabled" json:"auditEnabled"`
	// AuditLogPath 审计日志路径
	AuditLogPath string `yaml:"auditLogPath" json:"auditLogPath"`
}

// SessionCredentials 会话凭证
type SessionCredentials struct {
	// AccessKey 访问密钥
	AccessKey string
	// SecretKey 秘密密钥
	SecretKey string
	// SecurityToken 安全令牌（可选）
	SecurityToken string
}

// AuthenticationRequest 认证请求
type AuthenticationRequest struct {
	// AccessKey 访问密钥
	AccessKey string
	// Signature 签名
	Signature string
	// Timestamp 时间戳
	Timestamp int64
	// RemoteAddress 远程地址
	RemoteAddress string
	// RequestData 请求数据
	RequestData map[string]string
	// UserAgent 用户代理
	UserAgent string
	// Protocol 协议
	Protocol string
}

// AuthenticationResult 认证结果
type AuthenticationResult struct {
	// Success 是否成功
	Success bool
	// Account 账户信息
	Account *Account
	// ErrorMessage 错误信息
	ErrorMessage string
	// ExpireAt 过期时间
	ExpireAt *time.Time
}

// PermissionCheckRequest 权限检查请求
type PermissionCheckRequest struct {
	// Account 账户信息
	Account *Account
	// Resource 资源名称（Topic或Group）
	Resource string
	// ResourceType 资源类型（topic或group）
	ResourceType string
	// Operation 操作类型（pub或sub）
	Operation string
	// RemoteAddress 远程地址
	RemoteAddress string
	// MessageSize 消息大小（用于配额检查）
	MessageSize int64
	// UserAgent 用户代理
	UserAgent string
}

// PermissionCheckResult 权限检查结果
type PermissionCheckResult struct {
	// Allowed 是否允许
	Allowed bool
	// ErrorMessage 错误信息
	ErrorMessage string
	// QuotaInfo 配额信息
	QuotaInfo *ResourceQuota
	// RateLimitInfo 速率限制信息
	RateLimitInfo *RateLimit
}

// AuditEvent 审计事件
type AuditEvent struct {
	// Timestamp 时间戳
	Timestamp time.Time `json:"timestamp"`
	// AccessKey 访问密钥
	AccessKey string `json:"accessKey"`
	// Operation 操作
	Operation string `json:"operation"`
	// Resource 资源
	Resource string `json:"resource"`
	// Success 是否成功
	Success bool `json:"success"`
	// RemoteAddress 远程地址
	RemoteAddress string `json:"remoteAddress"`
	// UserAgent 用户代理
	UserAgent string `json:"userAgent"`
	// ErrorMessage 错误信息
	ErrorMessage string `json:"errorMessage,omitempty"`
	// Details 详细信息
	Details map[string]interface{} `json:"details,omitempty"`
}

// AclValidator ACL验证器接口
type AclValidator interface {
	// Authenticate 认证
	Authenticate(req *AuthenticationRequest) *AuthenticationResult
	// CheckPermission 检查权限
	CheckPermission(req *PermissionCheckRequest) *PermissionCheckResult
	// IsGlobalWhiteRemoteAddress 检查是否为全局白名单地址
	IsGlobalWhiteRemoteAddress(remoteAddress string) bool
	// IsGlobalBlackRemoteAddress 检查是否为全局黑名单地址
	IsGlobalBlackRemoteAddress(remoteAddress string) bool
	// LoadConfig 加载配置
	LoadConfig(configPath string) error
	// ReloadConfig 重新加载配置
	ReloadConfig() error
	// GetAccount 获取账户
	GetAccount(accessKey string) (*Account, bool)
	// AddAccount 添加账户
	AddAccount(account *Account) error
	// UpdateAccount 更新账户
	UpdateAccount(account *Account) error
	// DeleteAccount 删除账户
	DeleteAccount(accessKey string) error
	// ListAccounts 列出所有账户
	ListAccounts() []*Account
	// LogAuditEvent 记录审计事件
	LogAuditEvent(event *AuditEvent)
	// GetAuditEvents 获取审计事件
	GetAuditEvents(limit int) []*AuditEvent
}

// AclManager ACL管理器接口
type AclManager interface {
	// GetAccount 获取账户
	GetAccount(accessKey string) (*Account, bool)
	// AddAccount 添加账户
	AddAccount(account *Account) error
	// UpdateAccount 更新账户
	UpdateAccount(account *Account) error
	// DeleteAccount 删除账户
	DeleteAccount(accessKey string) error
	// ListAccounts 列出所有账户
	ListAccounts() []*Account
}

// SignatureGenerator 签名生成器接口
type SignatureGenerator interface {
	// GenerateSignature 生成签名
	GenerateSignature(secretKey string, data map[string]string) (string, error)
	// VerifySignature 验证签名
	VerifySignature(secretKey string, data map[string]string, signature string) bool
	// SignRequest 签名请求
	SignRequest(credentials *SessionCredentials, requestData map[string]string) (map[string]string, error)
	// ExtractCredentialsFromRequest 从请求中提取凭证
	ExtractCredentialsFromRequest(requestData map[string]string) (*SessionCredentials, string, error)
}

// RateLimiter 速率限制器接口
type RateLimiter interface {
	// Allow 检查是否允许请求
	Allow(key string) bool
	// GetRate 获取当前速率
	GetRate(key string) float64
	// Reset 重置速率限制
	Reset(key string)
}

// AclException ACL异常
type AclException struct {
	Code    int
	Message string
	Cause   error
}

func (e *AclException) Error() string {
	return e.Message
}

// NewAclException 创建ACL异常
func NewAclException(code int, message string, cause error) *AclException {
	return &AclException{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// ACL错误代码
const (
	// ErrCodeNoAccessKey 没有访问密钥
	ErrCodeNoAccessKey = 1001
	// ErrCodeInvalidAccessKey 无效的访问密钥
	ErrCodeInvalidAccessKey = 1002
	// ErrCodeSignatureFailed 签名失败
	ErrCodeSignatureFailed = 1003
	// ErrCodePermissionDenied 权限被拒绝
	ErrCodePermissionDenied = 1004
	// ErrCodeIPNotAllowed IP地址不被允许
	ErrCodeIPNotAllowed = 1005
	// ErrCodeConfigLoadFailed 配置加载失败
	ErrCodeConfigLoadFailed = 1006
	// ErrCodeAccountExpired 账户已过期
	ErrCodeAccountExpired = 1007
	// ErrCodeAccountDisabled 账户已禁用
	ErrCodeAccountDisabled = 1008
	// ErrCodeRateLimitExceeded 速率限制超出
	ErrCodeRateLimitExceeded = 1009
	// ErrCodeQuotaExceeded 配额超出
	ErrCodeQuotaExceeded = 1010
	// ErrCodeIPBlocked IP地址被阻止
	ErrCodeIPBlocked = 1011
	// ErrCodeReadOnlyOperation 只读账户尝试写操作
	ErrCodeReadOnlyOperation = 1012
)
