package cluster

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// HealthChecker 健康检查器
type HealthChecker struct {
	clusterManager *ClusterManager
	running        bool
	mutex          sync.RWMutex
	shutdown       chan struct{}
	checkInterval  time.Duration
	timeout        time.Duration
	httpClient     *http.Client
}

// HealthCheckResult 健康检查结果
type HealthCheckResult struct {
	BrokerName string        `json:"brokerName"`
	Healthy    bool          `json:"healthy"`
	Latency    time.Duration `json:"latency"`
	Error      string        `json:"error,omitempty"`
	Timestamp  int64         `json:"timestamp"`
}

// NewHealthChecker 创建健康检查器
func NewHealthChecker(cm *ClusterManager) *HealthChecker {
	return &HealthChecker{
		clusterManager: cm,
		shutdown:       make(chan struct{}),
		checkInterval:  30 * time.Second,
		timeout:        5 * time.Second,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Start 启动健康检查器
func (hc *HealthChecker) Start() error {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	if hc.running {
		return fmt.Errorf("health checker already running")
	}

	hc.running = true
	log.Printf("Starting health checker")

	// 启动健康检查goroutine
	go hc.runHealthCheck()

	return nil
}

// Stop 停止健康检查器
func (hc *HealthChecker) Stop() {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	if !hc.running {
		return
	}

	log.Printf("Stopping health checker")
	hc.running = false
	close(hc.shutdown)
}

// runHealthCheck 运行健康检查
func (hc *HealthChecker) runHealthCheck() {
	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.shutdown:
			return
		case <-ticker.C:
			hc.performHealthCheck()
		}
	}
}

// performHealthCheck 执行健康检查
func (hc *HealthChecker) performHealthCheck() {
	brokers := hc.clusterManager.GetAllBrokers()
	if len(brokers) == 0 {
		return
	}

	log.Printf("Performing health check for %d brokers", len(brokers))

	// 并发检查所有Broker
	var wg sync.WaitGroup
	results := make(chan *HealthCheckResult, len(brokers))

	for _, broker := range brokers {
		wg.Add(1)
		go func(b *BrokerInfo) {
			defer wg.Done()
			result := hc.checkBrokerHealth(b)
			results <- result
		}(broker)
	}

	// 等待所有检查完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 处理检查结果
	for result := range results {
		hc.handleHealthCheckResult(result)
	}
}

// checkBrokerHealth 检查单个Broker健康状态
func (hc *HealthChecker) checkBrokerHealth(broker *BrokerInfo) *HealthCheckResult {
	start := time.Now()
	result := &HealthCheckResult{
		BrokerName: broker.BrokerName,
		Timestamp:  time.Now().UnixMilli(),
	}

	// 构造健康检查URL
	healthURL := fmt.Sprintf("http://%s/health", broker.BrokerAddr)

	// 发送HTTP请求
	resp, err := hc.httpClient.Get(healthURL)
	if err != nil {
		result.Healthy = false
		result.Error = fmt.Sprintf("HTTP request failed: %v", err)
		result.Latency = time.Since(start)
		return result
	}
	defer resp.Body.Close()

	result.Latency = time.Since(start)

	// 检查HTTP状态码
	if resp.StatusCode == http.StatusOK {
		result.Healthy = true
	} else {
		result.Healthy = false
		result.Error = fmt.Sprintf("HTTP status: %d", resp.StatusCode)
	}

	return result
}

// handleHealthCheckResult 处理健康检查结果
func (hc *HealthChecker) handleHealthCheckResult(result *HealthCheckResult) {
	broker, exists := hc.clusterManager.GetBroker(result.BrokerName)
	if !exists {
		return
	}

	hc.clusterManager.mutex.Lock()
	defer hc.clusterManager.mutex.Unlock()

	if result.Healthy {
		// Broker健康，更新状态
		if broker.Status == SUSPECT || broker.Status == OFFLINE {
			log.Printf("Broker %s recovered, marking as online", result.BrokerName)
			broker.Status = ONLINE
		}
		broker.LastUpdateTime = time.Now().UnixMilli()
	} else {
		// Broker不健康
		log.Printf("Broker %s health check failed: %s", result.BrokerName, result.Error)
		
		switch broker.Status {
		case ONLINE:
			// 从在线变为可疑
			broker.Status = SUSPECT
			log.Printf("Broker %s marked as suspect", result.BrokerName)
		case SUSPECT:
			// 从可疑变为离线
			broker.Status = OFFLINE
			log.Printf("Broker %s marked as offline", result.BrokerName)
		}
	}
}

// GetHealthStatus 获取健康状态
func (hc *HealthChecker) GetHealthStatus() map[string]interface{} {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	status := make(map[string]interface{})
	status["running"] = hc.running
	status["check_interval"] = hc.checkInterval.String()
	status["timeout"] = hc.timeout.String()

	return status
}

// SetCheckInterval 设置检查间隔
func (hc *HealthChecker) SetCheckInterval(interval time.Duration) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	hc.checkInterval = interval
}

// SetTimeout 设置超时时间
func (hc *HealthChecker) SetTimeout(timeout time.Duration) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	hc.timeout = timeout
	hc.httpClient.Timeout = timeout
}