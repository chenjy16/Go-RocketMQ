package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ConfigChangeEvent 配置变更事件
type ConfigChangeEvent struct {
	Key       string      `json:"key"`
	OldValue  interface{} `json:"oldValue"`
	NewValue  interface{} `json:"newValue"`
	Timestamp time.Time   `json:"timestamp"`
}

// ConfigChangeListener 配置变更监听器
type ConfigChangeListener func(event *ConfigChangeEvent)

// ConfigValidator 配置验证器
type ConfigValidator func(key string, value interface{}) error

// ConfigManager 配置管理器
type ConfigManager struct {
	configFile string
	configData map[string]interface{}
	mutex      sync.RWMutex
	listeners  map[string][]ConfigChangeListener
	validators map[string]ConfigValidator
	watcher    *ConfigWatcher
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewConfigManager 创建配置管理器
func NewConfigManager(configFile string) (*ConfigManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	cm := &ConfigManager{
		configFile: configFile,
		configData: make(map[string]interface{}),
		listeners:  make(map[string][]ConfigChangeListener),
		validators: make(map[string]ConfigValidator),
		ctx:        ctx,
		cancel:     cancel,
	}

	// 加载配置
	if err := cm.loadConfig(); err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	// 启动配置监听器
	cm.watcher = NewConfigWatcher(configFile, cm.onConfigFileChanged)
	go cm.watcher.Start(cm.ctx)

	return cm, nil
}

// loadConfig 加载配置文件
func (cm *ConfigManager) loadConfig() error {
	data, err := os.ReadFile(cm.configFile)
	if err != nil {
		return fmt.Errorf("failed to read config file: %v", err)
	}

	var config map[string]interface{}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse config file: %v", err)
	}

	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.configData = config

	return nil
}

// Get 获取配置值
func (cm *ConfigManager) Get(key string) interface{} {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	return cm.getNestedValue(cm.configData, key)
}

// GetString 获取字符串配置值
func (cm *ConfigManager) GetString(key string, defaultValue string) string {
	value := cm.Get(key)
	if value == nil {
		return defaultValue
	}

	if str, ok := value.(string); ok {
		return str
	}

	return defaultValue
}

// GetInt 获取整数配置值
func (cm *ConfigManager) GetInt(key string, defaultValue int) int {
	value := cm.Get(key)
	if value == nil {
		return defaultValue
	}

	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}

	return defaultValue
}

// GetBool 获取布尔配置值
func (cm *ConfigManager) GetBool(key string, defaultValue bool) bool {
	value := cm.Get(key)
	if value == nil {
		return defaultValue
	}

	if b, ok := value.(bool); ok {
		return b
	}

	return defaultValue
}

// Set 设置配置值
func (cm *ConfigManager) Set(key string, value interface{}) error {
	// 验证配置值
	if validator, exists := cm.validators[key]; exists {
		if err := validator(key, value); err != nil {
			return fmt.Errorf("validation failed for key %s: %v", key, err)
		}
	}

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	oldValue := cm.getNestedValue(cm.configData, key)
	cm.setNestedValue(cm.configData, key, value)

	// 触发配置变更事件
	if oldValue != value {
		event := &ConfigChangeEvent{
			Key:       key,
			OldValue:  oldValue,
			NewValue:  value,
			Timestamp: time.Now(),
		}
		cm.notifyListeners(event)
	}

	return nil
}

// RegisterListener 注册配置变更监听器
func (cm *ConfigManager) RegisterListener(key string, listener ConfigChangeListener) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if _, exists := cm.listeners[key]; !exists {
		cm.listeners[key] = make([]ConfigChangeListener, 0)
	}
	cm.listeners[key] = append(cm.listeners[key], listener)
}

// UnregisterListener 注销配置变更监听器
func (cm *ConfigManager) UnregisterListener(key string, listener ConfigChangeListener) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if listeners, exists := cm.listeners[key]; exists {
		for i, l := range listeners {
			if &l == &listener {
				cm.listeners[key] = append(listeners[:i], listeners[i+1:]...)
				break
			}
		}
	}
}

// RegisterValidator 注册配置验证器
func (cm *ConfigManager) RegisterValidator(key string, validator ConfigValidator) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.validators[key] = validator
}

// Save 保存配置到文件
func (cm *ConfigManager) Save() error {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	data, err := yaml.Marshal(cm.configData)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %v", err)
	}

	// 确保目录存在
	dir := filepath.Dir(cm.configFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %v", err)
	}

	if err := os.WriteFile(cm.configFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %v", err)
	}

	return nil
}

// Reload 重新加载配置
func (cm *ConfigManager) Reload() error {
	if err := cm.loadConfig(); err != nil {
		return fmt.Errorf("failed to reload config: %v", err)
	}

	// 触发所有监听器的通知（简化处理）
	event := &ConfigChangeEvent{
		Key:       "*",
		OldValue:  nil,
		NewValue:  nil,
		Timestamp: time.Now(),
	}
	cm.notifyListeners(event)

	return nil
}

// Close 关闭配置管理器
func (cm *ConfigManager) Close() {
	cm.cancel()
}

// getNestedValue 获取嵌套配置值
func (cm *ConfigManager) getNestedValue(data map[string]interface{}, key string) interface{} {
	keys := splitKey(key)
	current := data

	for i, k := range keys {
		if i == len(keys)-1 {
			return current[k]
		}

		if next, ok := current[k].(map[string]interface{}); ok {
			current = next
		} else {
			return nil
		}
	}

	return nil
}

// setNestedValue 设置嵌套配置值
func (cm *ConfigManager) setNestedValue(data map[string]interface{}, key string, value interface{}) {
	keys := splitKey(key)
	current := data

	for i, k := range keys {
		if i == len(keys)-1 {
			current[k] = value
			return
		}

		if _, exists := current[k]; !exists {
			current[k] = make(map[string]interface{})
		}

		if next, ok := current[k].(map[string]interface{}); ok {
			current = next
		} else {
			newMap := make(map[string]interface{})
			current[k] = newMap
			current = newMap
		}
	}
}

// splitKey 分割配置键
func splitKey(key string) []string {
	// 简单实现，实际应该处理转义字符等
	return []string{key}
}

// notifyListeners 通知监听器
func (cm *ConfigManager) notifyListeners(event *ConfigChangeEvent) {
	// 通知特定键的监听器
	if listeners, exists := cm.listeners[event.Key]; exists {
		for _, listener := range listeners {
			go listener(event)
		}
	}

	// 通知全局监听器
	if listeners, exists := cm.listeners["*"]; exists {
		for _, listener := range listeners {
			go listener(event)
		}
	}
}

// onConfigFileChanged 配置文件变更回调
func (cm *ConfigManager) onConfigFileChanged() {
	// 重新加载配置
	if err := cm.Reload(); err != nil {
		fmt.Printf("Warning: failed to reload config: %v\n", err)
	}
}
