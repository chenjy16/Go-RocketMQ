package config

import (
	"context"
	"fmt"
	"os"
	"time"
)

// ConfigFileWatcher 配置文件监听器回调函数
type ConfigFileWatcher func()

// ConfigWatcher 配置文件监视器
type ConfigWatcher struct {
	filePath    string
	callback    ConfigFileWatcher
	lastModTime time.Time
}

// NewConfigWatcher 创建配置文件监视器
func NewConfigWatcher(filePath string, callback ConfigFileWatcher) *ConfigWatcher {
	return &ConfigWatcher{
		filePath: filePath,
		callback: callback,
	}
}

// Start 启动监视器
func (cw *ConfigWatcher) Start(ctx context.Context) {
	// 获取初始修改时间
	if info, err := os.Stat(cw.filePath); err == nil {
		cw.lastModTime = info.ModTime()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cw.checkFileChange()
		}
	}
}

// checkFileChange 检查文件是否发生变化
func (cw *ConfigWatcher) checkFileChange() {
	info, err := os.Stat(cw.filePath)
	if err != nil {
		// 文件不存在或无法访问
		fmt.Printf("Warning: failed to stat config file %s: %v\n", cw.filePath, err)
		return
	}

	if info.ModTime().After(cw.lastModTime) {
		// 文件已修改
		cw.lastModTime = info.ModTime()
		if cw.callback != nil {
			cw.callback()
		}
	}
}
