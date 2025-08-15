package acl

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
)

// HmacSHA1SignatureGenerator HmacSHA1签名生成器
type HmacSHA1SignatureGenerator struct{}

// NewHmacSHA1SignatureGenerator 创建HmacSHA1签名生成器
func NewHmacSHA1SignatureGenerator() *HmacSHA1SignatureGenerator {
	return &HmacSHA1SignatureGenerator{}
}

// GenerateSignature 生成签名
func (sg *HmacSHA1SignatureGenerator) GenerateSignature(secretKey string, data map[string]string) (string, error) {
	// 构建待签名字符串
	signString := sg.buildSignString(data)
	
	// 使用HmacSHA1算法生成签名
	h := hmac.New(sha1.New, []byte(secretKey))
	_, err := h.Write([]byte(signString))
	if err != nil {
		return "", NewAclException(ErrCodeSignatureFailed, fmt.Sprintf("failed to generate signature: %s", err.Error()), err)
	}
	
	// Base64编码
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return signature, nil
}

// VerifySignature 验证签名
func (sg *HmacSHA1SignatureGenerator) VerifySignature(secretKey string, data map[string]string, signature string) bool {
	// 生成期望的签名
	expectedSignature, err := sg.GenerateSignature(secretKey, data)
	if err != nil {
		return false
	}
	
	// 比较签名
	return hmac.Equal([]byte(expectedSignature), []byte(signature))
}

// buildSignString 构建待签名字符串
func (sg *HmacSHA1SignatureGenerator) buildSignString(data map[string]string) string {
	// 获取所有键并排序
	keys := make([]string, 0, len(data))
	for key := range data {
		// 跳过签名字段
		if key == "Signature" || key == "signature" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	
	// 构建签名字符串
	var parts []string
	for _, key := range keys {
		value := data[key]
		if value != "" {
			parts = append(parts, fmt.Sprintf("%s=%s", key, value))
		}
	}
	
	return strings.Join(parts, "&")
}

// SignRequest 为请求签名
func (sg *HmacSHA1SignatureGenerator) SignRequest(credentials *SessionCredentials, requestData map[string]string) (map[string]string, error) {
	// 复制请求数据
	signedData := make(map[string]string)
	for k, v := range requestData {
		signedData[k] = v
	}
	
	// 添加访问密钥
	signedData["AccessKey"] = credentials.AccessKey
	
	// 添加安全令牌（如果有）
	if credentials.SecurityToken != "" {
		signedData["SecurityToken"] = credentials.SecurityToken
	}
	
	// 生成签名
	signature, err := sg.GenerateSignature(credentials.SecretKey, signedData)
	if err != nil {
		return nil, err
	}
	
	// 添加签名
	signedData["Signature"] = signature
	
	return signedData, nil
}

// ExtractCredentialsFromRequest 从请求中提取凭证
func (sg *HmacSHA1SignatureGenerator) ExtractCredentialsFromRequest(requestData map[string]string) (*SessionCredentials, string, error) {
	accessKey, ok := requestData["AccessKey"]
	if !ok || accessKey == "" {
		return nil, "", NewAclException(ErrCodeNoAccessKey, "no access key found in request", nil)
	}
	
	signature, ok := requestData["Signature"]
	if !ok || signature == "" {
		return nil, "", NewAclException(ErrCodeSignatureFailed, "no signature found in request", nil)
	}
	
	credentials := &SessionCredentials{
		AccessKey: accessKey,
	}
	
	// 提取安全令牌（如果有）
	if securityToken, ok := requestData["SecurityToken"]; ok {
		credentials.SecurityToken = securityToken
	}
	
	return credentials, signature, nil
}