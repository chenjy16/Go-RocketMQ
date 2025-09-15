package config

import (
	"fmt"
	"regexp"
	"strings"
)

// NotEmpty 非空验证器
func NotEmpty(key string, value interface{}) error {
	if value == nil {
		return fmt.Errorf("value cannot be nil")
	}
	if str, ok := value.(string); ok && strings.TrimSpace(str) == "" {
		return fmt.Errorf("value cannot be empty")
	}
	return nil
}

// Port 端口验证器 (1-65535)
func Port(key string, value interface{}) error {
	port := 0
	switch v := value.(type) {
	case int:
		port = v
	case int64:
		port = int(v)
	case float64:
		port = int(v)
	default:
		return fmt.Errorf("invalid port type: %T", value)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got %d", port)
	}
	return nil
}

// PositiveInteger 正整数验证器
func PositiveInteger(key string, value interface{}) error {
	num := 0
	switch v := value.(type) {
	case int:
		num = v
	case int64:
		num = int(v)
	case float64:
		num = int(v)
	default:
		return fmt.Errorf("invalid integer type: %T", value)
	}
	if num <= 0 {
		return fmt.Errorf("value must be positive, got %d", num)
	}
	return nil
}

// NonNegativeInteger 非负整数验证器
func NonNegativeInteger(key string, value interface{}) error {
	num := 0
	switch v := value.(type) {
	case int:
		num = v
	case int64:
		num = int(v)
	case float64:
		num = int(v)
	default:
		return fmt.Errorf("invalid integer type: %T", value)
	}
	if num < 0 {
		return fmt.Errorf("value must be non-negative, got %d", num)
	}
	return nil
}

// FilePath 文件路径验证器
func FilePath(key string, value interface{}) error {
	if str, ok := value.(string); ok {
		if str == "" {
			return fmt.Errorf("file path cannot be empty")
		}
		// 简单的路径格式验证
		matched, _ := regexp.MatchString(`^[^<>:"|?*\x00-\x1F]+$`, str)
		if !matched {
			return fmt.Errorf("invalid file path format")
		}
	} else {
		return fmt.Errorf("invalid file path type: %T", value)
	}
	return nil
}

// DirectoryPath 目录路径验证器
func DirectoryPath(key string, value interface{}) error {
	// 目录路径验证与文件路径类似
	return FilePath(key, value)
}

// LogLevel 日志级别验证器
func LogLevel(key string, value interface{}) error {
	if str, ok := value.(string); ok {
		validLevels := map[string]bool{
			"debug": true,
			"info":  true,
			"warn":  true,
			"error": true,
		}
		if !validLevels[strings.ToLower(str)] {
			return fmt.Errorf("invalid log level: %s", str)
		}
	} else {
		return fmt.Errorf("invalid log level type: %T", value)
	}
	return nil
}

// FlushDiskType 刷盘类型验证器
func FlushDiskType(key string, value interface{}) error {
	if str, ok := value.(string); ok {
		validTypes := map[string]bool{
			"SYNC_FLUSH":  true,
			"ASYNC_FLUSH": true,
		}
		if !validTypes[strings.ToUpper(str)] {
			return fmt.Errorf("invalid flush disk type: %s", str)
		}
	} else {
		return fmt.Errorf("invalid flush disk type: %T", value)
	}
	return nil
}
