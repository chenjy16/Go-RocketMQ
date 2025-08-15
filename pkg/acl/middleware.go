package acl

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// AclMiddleware ACL中间件
type AclMiddleware struct {
	validator AclValidator
	enabled   bool
}

// NewAclMiddleware 创建ACL中间件
func NewAclMiddleware(validator AclValidator, enabled bool) *AclMiddleware {
	return &AclMiddleware{
		validator: validator,
		enabled:   enabled,
	}
}

// HTTPMiddleware HTTP中间件
func (m *AclMiddleware) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !m.enabled {
			next.ServeHTTP(w, r)
			return
		}

		// 提取认证信息
		authReq, err := m.extractAuthenticationFromHTTP(r)
		if err != nil {
			m.writeErrorResponse(w, http.StatusUnauthorized, err.Error())
			return
		}

		// 认证
		authResult := m.validator.Authenticate(authReq)
		if !authResult.Success {
			m.writeErrorResponse(w, http.StatusUnauthorized, authResult.ErrorMessage)
			return
		}

		// 将账户信息添加到上下文
		ctx := context.WithValue(r.Context(), "acl_account", authResult.Account)
		r = r.WithContext(ctx)

		next.ServeHTTP(w, r)
	})
}

// CheckTopicPermission 检查Topic权限
func (m *AclMiddleware) CheckTopicPermission(account *Account, topicName, operation, remoteAddress string) error {
	if !m.enabled {
		return nil
	}

	// 检查权限
	permReq := CreatePermissionCheckRequest(account, topicName, "topic", operation, remoteAddress)
	permResult := m.validator.CheckPermission(permReq)

	if !permResult.Allowed {
		return NewAclException(ErrCodePermissionDenied, permResult.ErrorMessage, nil)
	}

	return nil
}

// CheckGroupPermission 检查Group权限
func (m *AclMiddleware) CheckGroupPermission(account *Account, groupName, operation, remoteAddress string) error {
	if !m.enabled {
		return nil
	}

	// 检查权限
	permReq := CreatePermissionCheckRequest(account, groupName, "group", operation, remoteAddress)
	permResult := m.validator.CheckPermission(permReq)

	if !permResult.Allowed {
		return NewAclException(ErrCodePermissionDenied, permResult.ErrorMessage, nil)
	}

	return nil
}

// AuthenticateRequest 认证请求
func (m *AclMiddleware) AuthenticateRequest(requestData map[string]string, remoteAddress string) (*Account, error) {
	if !m.enabled {
		return nil, nil
	}

	// 提取认证信息
	accessKey := requestData["AccessKey"]
	signature := requestData["Signature"]
	timestampStr := requestData["Timestamp"]

	// 解析时间戳
	timestamp, err := ParseTimestampFromString(timestampStr)
	if err != nil {
		return nil, NewAclException(ErrCodeSignatureFailed, fmt.Sprintf("invalid timestamp: %s", err.Error()), err)
	}

	// 创建认证请求
	authReq := CreateAuthenticationRequest(accessKey, signature, timestamp, remoteAddress, requestData)

	// 认证
	authResult := m.validator.Authenticate(authReq)
	if !authResult.Success {
		return nil, NewAclException(ErrCodeInvalidAccessKey, authResult.ErrorMessage, nil)
	}

	return authResult.Account, nil
}

// ValidateProducerRequest 验证生产者请求
func (m *AclMiddleware) ValidateProducerRequest(requestData map[string]string, topicName, remoteAddress string) (*Account, error) {
	// 认证
	account, err := m.AuthenticateRequest(requestData, remoteAddress)
	if err != nil {
		return nil, err
	}

	// 检查Topic发布权限
	if account != nil {
		if err := m.CheckTopicPermission(account, topicName, "pub", remoteAddress); err != nil {
			return nil, err
		}
	}

	return account, nil
}

// ValidateConsumerRequest 验证消费者请求
func (m *AclMiddleware) ValidateConsumerRequest(requestData map[string]string, topicName, groupName, remoteAddress string) (*Account, error) {
	// 认证
	account, err := m.AuthenticateRequest(requestData, remoteAddress)
	if err != nil {
		return nil, err
	}

	// 检查权限
	if account != nil {
		// 检查Topic订阅权限
		if err := m.CheckTopicPermission(account, topicName, "sub", remoteAddress); err != nil {
			return nil, err
		}

		// 检查Group权限
		if err := m.CheckGroupPermission(account, groupName, "sub", remoteAddress); err != nil {
			return nil, err
		}
	}

	return account, nil
}

// ValidateAdminRequest 验证管理员请求
func (m *AclMiddleware) ValidateAdminRequest(requestData map[string]string, remoteAddress string) (*Account, error) {
	// 认证
	account, err := m.AuthenticateRequest(requestData, remoteAddress)
	if err != nil {
		return nil, err
	}

	// 检查是否为管理员
	if account != nil && !account.Admin {
		return nil, NewAclException(ErrCodePermissionDenied, "admin permission required", nil)
	}

	return account, nil
}

// extractAuthenticationFromHTTP 从HTTP请求中提取认证信息
func (m *AclMiddleware) extractAuthenticationFromHTTP(r *http.Request) (*AuthenticationRequest, error) {
	// 从Header中提取
	accessKey := r.Header.Get("AccessKey")
	signature := r.Header.Get("Signature")
	timestampStr := r.Header.Get("Timestamp")

	// 如果Header中没有，尝试从Query参数中提取
	if accessKey == "" {
		accessKey = r.URL.Query().Get("AccessKey")
	}
	if signature == "" {
		signature = r.URL.Query().Get("Signature")
	}
	if timestampStr == "" {
		timestampStr = r.URL.Query().Get("Timestamp")
	}

	// 解析时间戳
	timestamp, err := ParseTimestampFromString(timestampStr)
	if err != nil {
		return nil, NewAclException(ErrCodeSignatureFailed, fmt.Sprintf("invalid timestamp: %s", err.Error()), err)
	}

	// 获取远程地址
	remoteAddress := m.getRemoteAddress(r)

	// 构建请求数据
	requestData := make(map[string]string)
	requestData["AccessKey"] = accessKey
	requestData["Signature"] = signature
	if timestampStr != "" {
		requestData["Timestamp"] = timestampStr
	}

	// 添加其他Header和Query参数
	for key, values := range r.Header {
		if len(values) > 0 && !strings.HasPrefix(strings.ToLower(key), "x-") {
			requestData[key] = values[0]
		}
	}
	for key, values := range r.URL.Query() {
		if len(values) > 0 {
			requestData[key] = values[0]
		}
	}

	return CreateAuthenticationRequest(accessKey, signature, timestamp, remoteAddress, requestData), nil
}

// getRemoteAddress 获取远程地址
func (m *AclMiddleware) getRemoteAddress(r *http.Request) string {
	// 尝试从X-Forwarded-For获取真实IP
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}

	// 尝试从X-Real-IP获取
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// 使用RemoteAddr，去掉端口号
	remoteAddr := r.RemoteAddr
	if idx := strings.LastIndex(remoteAddr, ":"); idx != -1 {
		return remoteAddr[:idx]
	}
	return remoteAddr
}

// writeErrorResponse 写入错误响应
func (m *AclMiddleware) writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, message)))
}

// GetAccountFromContext 从上下文中获取账户信息
func GetAccountFromContext(ctx context.Context) (*Account, bool) {
	account, ok := ctx.Value("acl_account").(*Account)
	return account, ok
}

// IsAclEnabled 检查ACL是否启用
func (m *AclMiddleware) IsAclEnabled() bool {
	return m.enabled
}

// SetAclEnabled 设置ACL启用状态
func (m *AclMiddleware) SetAclEnabled(enabled bool) {
	m.enabled = enabled
}

// ReloadConfig 重新加载配置
func (m *AclMiddleware) ReloadConfig() error {
	return m.validator.ReloadConfig()
}

// CreateSignedRequest 创建签名请求
func CreateSignedRequest(credentials *SessionCredentials, requestData map[string]string) (map[string]string, error) {
	signatureGen := NewHmacSHA1SignatureGenerator()
	return signatureGen.SignRequest(credentials, requestData)
}

// ExtractCredentialsFromRequest 从请求中提取凭证
func ExtractCredentialsFromRequest(requestData map[string]string) (*SessionCredentials, string, error) {
	signatureGen := NewHmacSHA1SignatureGenerator()
	return signatureGen.ExtractCredentialsFromRequest(requestData)
}