package client

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ACLSignatureMethod ACL签名算法类型
type ACLSignatureMethod string

const (
	// HmacSHA1 HMAC-SHA1签名算法
	HmacSHA1 ACLSignatureMethod = "HmacSHA1"
	// HmacSHA256 HMAC-SHA256签名算法
	HmacSHA256 ACLSignatureMethod = "HmacSHA256"
)

// ACLUtils ACL工具类
type ACLUtils struct {
	accessKey       string
	secretKey       string
	signatureMethod ACLSignatureMethod
}

// NewACLUtils 创建ACL工具实例
func NewACLUtils(accessKey, secretKey string, signatureMethod ACLSignatureMethod) *ACLUtils {
	if signatureMethod == "" {
		signatureMethod = HmacSHA1
	}
	return &ACLUtils{
		accessKey:       accessKey,
		secretKey:       secretKey,
		signatureMethod: signatureMethod,
	}
}

// GenerateSignature 生成ACL签名
func (acl *ACLUtils) GenerateSignature(requestData map[string]string) (string, error) {
	if acl.secretKey == "" {
		return "", fmt.Errorf("secretKey cannot be empty")
	}

	// 构建签名字符串
	signString := acl.buildSignString(requestData)

	// 根据签名算法生成签名
	switch acl.signatureMethod {
	case HmacSHA1:
		return acl.hmacSHA1Sign(signString)
	case HmacSHA256:
		return acl.hmacSHA256Sign(signString)
	default:
		return "", fmt.Errorf("unsupported signature method: %s", acl.signatureMethod)
	}
}

// VerifySignature 验证ACL签名
func (acl *ACLUtils) VerifySignature(requestData map[string]string, expectedSignature string) bool {
	generatedSignature, err := acl.GenerateSignature(requestData)
	if err != nil {
		return false
	}
	return generatedSignature == expectedSignature
}

// buildSignString 构建签名字符串
func (acl *ACLUtils) buildSignString(requestData map[string]string) string {
	// 获取所有键并排序
	keys := make([]string, 0, len(requestData))
	for key := range requestData {
		// 跳过签名字段本身
		if key != "Signature" && key != "AccessKey" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	// 构建签名字符串
	var signParts []string
	for _, key := range keys {
		value := requestData[key]
		if value != "" {
			signParts = append(signParts, key+"="+value)
		}
	}

	return strings.Join(signParts, "&")
}

// hmacSHA1Sign HMAC-SHA1签名
func (acl *ACLUtils) hmacSHA1Sign(data string) (string, error) {
	h := hmac.New(sha1.New, []byte(acl.secretKey))
	_, err := h.Write([]byte(data))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

// hmacSHA256Sign HMAC-SHA256签名
func (acl *ACLUtils) hmacSHA256Sign(data string) (string, error) {
	h := hmac.New(sha256.New, []byte(acl.secretKey))
	_, err := h.Write([]byte(data))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

// BuildRequestData 构建请求数据用于签名
func (acl *ACLUtils) BuildRequestData(topic, consumerGroup, msgId string, timestamp int64) map[string]string {
	requestData := make(map[string]string)
	
	if topic != "" {
		requestData["topic"] = topic
	}
	if consumerGroup != "" {
		requestData["consumerGroup"] = consumerGroup
	}
	if msgId != "" {
		requestData["msgId"] = msgId
	}
	if timestamp > 0 {
		requestData["timestamp"] = strconv.FormatInt(timestamp, 10)
	}
	
	return requestData
}

// GetAccessKey 获取访问密钥
func (acl *ACLUtils) GetAccessKey() string {
	return acl.accessKey
}

// GetSignatureMethod 获取签名算法
func (acl *ACLUtils) GetSignatureMethod() ACLSignatureMethod {
	return acl.signatureMethod
}

// GenerateTimestamp 生成当前时间戳
func GenerateTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// IsTimestampValid 验证时间戳是否有效（防重放攻击）
func IsTimestampValid(timestamp int64, toleranceMs int64) bool {
	if toleranceMs <= 0 {
		toleranceMs = 300000 // 默认5分钟容忍度
	}
	
	currentTime := GenerateTimestamp()
	return timestamp > 0 && (currentTime-timestamp) <= toleranceMs
}

// ValidateACLConfig 验证ACL配置
func ValidateACLConfig(accessKey, secretKey string) error {
	if accessKey == "" {
		return fmt.Errorf("accessKey cannot be empty")
	}
	if secretKey == "" {
		return fmt.Errorf("secretKey cannot be empty")
	}
	if len(secretKey) < 8 {
		return fmt.Errorf("secretKey length must be at least 8 characters")
	}
	return nil
}