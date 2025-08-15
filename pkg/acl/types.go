package acl

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

// Account ACL账户信息
type Account struct {
	// AccessKey 访问密钥
	AccessKey string `yaml:"accessKey" json:"accessKey"`
	// SecretKey 秘密密钥
	SecretKey string `yaml:"secretKey" json:"secretKey"`
	// WhiteRemoteAddress 白名单IP地址
	WhiteRemoteAddress string `yaml:"whiteRemoteAddress" json:"whiteRemoteAddress"`
	// Admin 是否为管理员
	Admin bool `yaml:"admin" json:"admin"`
	// DefaultTopicPerm 默认Topic权限
	DefaultTopicPerm Permission `yaml:"defaultTopicPerm" json:"defaultTopicPerm"`
	// DefaultGroupPerm 默认Group权限
	DefaultGroupPerm Permission `yaml:"defaultGroupPerm" json:"defaultGroupPerm"`
	// TopicPerms Topic权限列表
	TopicPerms []string `yaml:"topicPerms" json:"topicPerms"`
	// GroupPerms Group权限列表
	GroupPerms []string `yaml:"groupPerms" json:"groupPerms"`
}

// AclConfig ACL配置
type AclConfig struct {
	// GlobalWhiteRemoteAddresses 全局白名单IP地址
	GlobalWhiteRemoteAddresses []string `yaml:"globalWhiteRemoteAddresses" json:"globalWhiteRemoteAddresses"`
	// Accounts 账户列表
	Accounts []Account `yaml:"accounts" json:"accounts"`
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
}

// AuthenticationResult 认证结果
type AuthenticationResult struct {
	// Success 是否成功
	Success bool
	// Account 账户信息
	Account *Account
	// ErrorMessage 错误信息
	ErrorMessage string
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
}

// PermissionCheckResult 权限检查结果
type PermissionCheckResult struct {
	// Allowed 是否允许
	Allowed bool
	// ErrorMessage 错误信息
	ErrorMessage string
}

// AclValidator ACL验证器接口
type AclValidator interface {
	// Authenticate 认证
	Authenticate(req *AuthenticationRequest) *AuthenticationResult
	// CheckPermission 检查权限
	CheckPermission(req *PermissionCheckRequest) *PermissionCheckResult
	// IsGlobalWhiteRemoteAddress 检查是否为全局白名单地址
	IsGlobalWhiteRemoteAddress(remoteAddress string) bool
	// LoadConfig 加载配置
	LoadConfig(configPath string) error
	// ReloadConfig 重新加载配置
	ReloadConfig() error
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
)