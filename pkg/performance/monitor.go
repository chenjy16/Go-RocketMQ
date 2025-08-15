package performance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// PerformanceMonitor 性能监控器
type PerformanceMonitor struct {
	memoryPool    *MemoryPool
	batchManager  *BatchManager
	networkOpt    *NetworkOptimizer
	metrics       *SystemMetrics
	collectors    []MetricsCollector
	server        *http.Server
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	mutex         sync.RWMutex
	collectInterval time.Duration
}

// SystemMetrics 系统指标
type SystemMetrics struct {
	// CPU指标
	CPUUsage     float64 `json:"cpu_usage"`
	Goroutines   int     `json:"goroutines"`
	
	// 内存指标
	MemoryUsed   uint64  `json:"memory_used"`
	MemoryTotal  uint64  `json:"memory_total"`
	GCCount      uint32  `json:"gc_count"`
	GCPauseTotal uint64  `json:"gc_pause_total"`
	
	// 性能指标
	Throughput   float64 `json:"throughput"`
	Latency      float64 `json:"latency"`
	ErrorRate    float64 `json:"error_rate"`
	
	// 时间戳
	Timestamp    time.Time `json:"timestamp"`
	mutex        sync.RWMutex
}

// MetricsCollector 指标收集器接口
type MetricsCollector interface {
	Collect() map[string]interface{}
	Name() string
}

// MemoryMetricsCollector 内存指标收集器
type MemoryMetricsCollector struct {
	memoryPool *MemoryPool
}

// BatchMetricsCollector 批量处理指标收集器
type BatchMetricsCollector struct {
	batchManager *BatchManager
}

// NetworkMetricsCollector 网络指标收集器
type NetworkMetricsCollector struct {
	networkOpt *NetworkOptimizer
}

// SystemMetricsCollector 系统指标收集器
type SystemMetricsCollector struct {
	lastGCCount uint32
	lastGCTime  time.Time
}

// MonitorConfig 监控配置
type MonitorConfig struct {
	CollectInterval time.Duration // 收集间隔
	HTTPPort        int           // HTTP服务端口
	EnableHTTP      bool          // 启用HTTP服务
	MetricsPath     string        // 指标路径
}

// DefaultMonitorConfig 默认监控配置
var DefaultMonitorConfig = MonitorConfig{
	CollectInterval: 10 * time.Second,
	HTTPPort:        8080,
	EnableHTTP:      true,
	MetricsPath:     "/metrics",
}

// NewPerformanceMonitor 创建性能监控器
func NewPerformanceMonitor(config MonitorConfig) *PerformanceMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	
	pm := &PerformanceMonitor{
		metrics:         &SystemMetrics{},
		collectors:      make([]MetricsCollector, 0),
		ctx:             ctx,
		cancel:          cancel,
		collectInterval: config.CollectInterval,
	}
	
	// 注册默认收集器
	pm.RegisterCollector(&SystemMetricsCollector{})
	
	// 启用HTTP服务
	if config.EnableHTTP {
		pm.setupHTTPServer(config)
	}
	
	return pm
}

// RegisterMemoryPool 注册内存池
func (pm *PerformanceMonitor) RegisterMemoryPool(memoryPool *MemoryPool) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	
	pm.memoryPool = memoryPool
	pm.RegisterCollector(&MemoryMetricsCollector{memoryPool: memoryPool})
}

// RegisterBatchManager 注册批量管理器
func (pm *PerformanceMonitor) RegisterBatchManager(batchManager *BatchManager) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	
	pm.batchManager = batchManager
	pm.RegisterCollector(&BatchMetricsCollector{batchManager: batchManager})
}

// RegisterNetworkOptimizer 注册网络优化器
func (pm *PerformanceMonitor) RegisterNetworkOptimizer(networkOpt *NetworkOptimizer) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	
	pm.networkOpt = networkOpt
	pm.RegisterCollector(&NetworkMetricsCollector{networkOpt: networkOpt})
}

// RegisterCollector 注册指标收集器
func (pm *PerformanceMonitor) RegisterCollector(collector MetricsCollector) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	
	pm.collectors = append(pm.collectors, collector)
}

// Start 启动性能监控
func (pm *PerformanceMonitor) Start() error {
	pm.wg.Add(1)
	go pm.collectMetrics()
	
	if pm.server != nil {
		pm.wg.Add(1)
		go pm.startHTTPServer()
	}
	
	return nil
}

// Stop 停止性能监控
func (pm *PerformanceMonitor) Stop() error {
	pm.cancel()
	
	if pm.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pm.server.Shutdown(ctx)
	}
	
	pm.wg.Wait()
	return nil
}

// collectMetrics 收集指标
func (pm *PerformanceMonitor) collectMetrics() {
	defer pm.wg.Done()
	
	ticker := time.NewTicker(pm.collectInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.UpdateMetrics()
		}
	}
}

// UpdateMetrics 更新指标
func (pm *PerformanceMonitor) UpdateMetrics() {
	pm.mutex.RLock()
	collectors := make([]MetricsCollector, len(pm.collectors))
	copy(collectors, pm.collectors)
	pm.mutex.RUnlock()
	
	// 收集所有指标
	allMetrics := make(map[string]interface{})
	for _, collector := range collectors {
		metrics := collector.Collect()
		for k, v := range metrics {
			allMetrics[collector.Name()+"_"+k] = v
		}
	}
	
	// 更新系统指标
	pm.metrics.mutex.Lock()
	pm.metrics.Timestamp = time.Now()
	
	// 从收集的指标中更新SystemMetrics字段
	if goroutines, ok := allMetrics["system_goroutines"]; ok {
		if val, ok := goroutines.(int); ok {
			pm.metrics.Goroutines = val
		}
	}
	
	if memAlloc, ok := allMetrics["system_memory_alloc"]; ok {
		if val, ok := memAlloc.(uint64); ok {
			pm.metrics.MemoryUsed = val
		}
	}
	
	if memSys, ok := allMetrics["system_memory_sys"]; ok {
		if val, ok := memSys.(uint64); ok {
			pm.metrics.MemoryTotal = val
		}
	}
	
	if gcCount, ok := allMetrics["system_gc_count"]; ok {
		if val, ok := gcCount.(uint32); ok {
			pm.metrics.GCCount = val
		}
	}
	
	if gcPause, ok := allMetrics["system_gc_pause_total"]; ok {
		if val, ok := gcPause.(uint64); ok {
			pm.metrics.GCPauseTotal = val
		}
	}
	
	pm.metrics.mutex.Unlock()
}

// setupHTTPServer 设置HTTP服务器
func (pm *PerformanceMonitor) setupHTTPServer(config MonitorConfig) {
	mux := http.NewServeMux()
	mux.HandleFunc(config.MetricsPath, pm.handleMetrics)
	mux.HandleFunc("/health", pm.handleHealth)
	mux.HandleFunc("/debug", pm.handleDebug)
	
	pm.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", config.HTTPPort),
		Handler: mux,
	}
}

// startHTTPServer 启动HTTP服务器
func (pm *PerformanceMonitor) startHTTPServer() {
	defer pm.wg.Done()
	
	if err := pm.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Printf("HTTP server error: %v\n", err)
	}
}

// handleMetrics 处理指标请求
func (pm *PerformanceMonitor) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	metrics := pm.GetAllMetrics()
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleHealth 处理健康检查请求
func (pm *PerformanceMonitor) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"uptime":    time.Since(pm.metrics.Timestamp),
	}
	
	if err := json.NewEncoder(w).Encode(health); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleDebug 处理调试信息请求
func (pm *PerformanceMonitor) handleDebug(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	fmt.Fprintf(w, "Goroutines: %d\n", runtime.NumGoroutine())
	fmt.Fprintf(w, "Memory Allocated: %d KB\n", m.Alloc/1024)
	fmt.Fprintf(w, "Memory Total Allocated: %d KB\n", m.TotalAlloc/1024)
	fmt.Fprintf(w, "Memory System: %d KB\n", m.Sys/1024)
	fmt.Fprintf(w, "GC Count: %d\n", m.NumGC)
	fmt.Fprintf(w, "GC Pause Total: %d ns\n", m.PauseTotalNs)
}

// GetAllMetrics 获取所有指标
func (pm *PerformanceMonitor) GetAllMetrics() map[string]interface{} {
	pm.mutex.RLock()
	collectors := make([]MetricsCollector, len(pm.collectors))
	copy(collectors, pm.collectors)
	pm.mutex.RUnlock()
	
	allMetrics := make(map[string]interface{})
	
	// 收集所有指标
	for _, collector := range collectors {
		metrics := collector.Collect()
		allMetrics[collector.Name()] = metrics
	}
	
	// 添加系统指标
	pm.metrics.mutex.RLock()
	allMetrics["system"] = *pm.metrics
	pm.metrics.mutex.RUnlock()
	
	return allMetrics
}

// GetSystemMetrics 获取系统指标
func (pm *PerformanceMonitor) GetSystemMetrics() SystemMetrics {
	pm.metrics.mutex.RLock()
	defer pm.metrics.mutex.RUnlock()
	return *pm.metrics
}

// 实现各种指标收集器

// Name 内存指标收集器名称
func (mmc *MemoryMetricsCollector) Name() string {
	return "memory"
}

// Collect 收集内存指标
func (mmc *MemoryMetricsCollector) Collect() map[string]interface{} {
	if mmc.memoryPool == nil {
		return make(map[string]interface{})
	}
	
	metrics := mmc.memoryPool.GetMetrics()
	return map[string]interface{}{
		"pool_stats": metrics.GetStats(),
	}
}

// Name 批量处理指标收集器名称
func (bmc *BatchMetricsCollector) Name() string {
	return "batch"
}

// Collect 收集批量处理指标
func (bmc *BatchMetricsCollector) Collect() map[string]interface{} {
	if bmc.batchManager == nil {
		return make(map[string]interface{})
	}
	
	return map[string]interface{}{
		"all_metrics": bmc.batchManager.GetAllMetrics(),
	}
}

// Name 网络指标收集器名称
func (nmc *NetworkMetricsCollector) Name() string {
	return "network"
}

// Collect 收集网络指标
func (nmc *NetworkMetricsCollector) Collect() map[string]interface{} {
	if nmc.networkOpt == nil {
		return make(map[string]interface{})
	}
	
	return map[string]interface{}{
		"optimizer_stats": nmc.networkOpt.metrics.GetNetworkStats(),
	}
}

// Name 系统指标收集器名称
func (smc *SystemMetricsCollector) Name() string {
	return "system"
}

// Collect 收集系统指标
func (smc *SystemMetricsCollector) Collect() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	// 计算GC频率
	gcRate := float64(0)
	if smc.lastGCCount > 0 {
		gcDiff := m.NumGC - smc.lastGCCount
		timeDiff := time.Since(smc.lastGCTime).Seconds()
		if timeDiff > 0 {
			gcRate = float64(gcDiff) / timeDiff
		}
	}
	
	smc.lastGCCount = m.NumGC
	smc.lastGCTime = time.Now()
	
	return map[string]interface{}{
		"goroutines":        runtime.NumGoroutine(),
		"memory_alloc":      m.Alloc,
		"memory_total_alloc": m.TotalAlloc,
		"memory_sys":        m.Sys,
		"memory_heap_alloc": m.HeapAlloc,
		"memory_heap_sys":   m.HeapSys,
		"memory_heap_idle":  m.HeapIdle,
		"memory_heap_inuse": m.HeapInuse,
		"gc_count":          m.NumGC,
		"gc_pause_total":    m.PauseTotalNs,
		"gc_rate":           gcRate,
		"last_gc":           time.Unix(0, int64(m.LastGC)),
	}
}

// AlertManager 告警管理器
type AlertManager struct {
	monitor   *PerformanceMonitor
	rules     []AlertRule
	handlers  []AlertHandler
	mutex     sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// AlertRule 告警规则
type AlertRule struct {
	Name        string
	MetricName  string
	Threshold   float64
	Operator    string // ">", "<", ">=", "<=", "=="
	Duration    time.Duration
	Description string
	Severity    string // "critical", "warning", "info"
}

// AlertHandler 告警处理器接口
type AlertHandler interface {
	Handle(alert Alert) error
}

// Alert 告警
type Alert struct {
	Rule        AlertRule
	Value       float64
	Timestamp   time.Time
	Description string
}

// NewAlertManager 创建告警管理器
func NewAlertManager(monitor *PerformanceMonitor) *AlertManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &AlertManager{
		monitor:  monitor,
		rules:    make([]AlertRule, 0),
		handlers: make([]AlertHandler, 0),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// AddRule 添加告警规则
func (am *AlertManager) AddRule(rule AlertRule) {
	am.mutex.Lock()
	defer am.mutex.Unlock()
	am.rules = append(am.rules, rule)
}

// AddHandler 添加告警处理器
func (am *AlertManager) AddHandler(handler AlertHandler) {
	am.mutex.Lock()
	defer am.mutex.Unlock()
	am.handlers = append(am.handlers, handler)
}

// Start 启动告警管理器
func (am *AlertManager) Start() {
	am.wg.Add(1)
	go am.checkAlerts()
}

// Stop 停止告警管理器
func (am *AlertManager) Stop() {
	am.cancel()
	am.wg.Wait()
}

// checkAlerts 检查告警
func (am *AlertManager) checkAlerts() {
	defer am.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-am.ctx.Done():
			return
		case <-ticker.C:
			am.evaluateRules()
		}
	}
}

// evaluateRules 评估告警规则
func (am *AlertManager) evaluateRules() {
	metrics := am.monitor.GetAllMetrics()
	
	am.mutex.RLock()
	rules := make([]AlertRule, len(am.rules))
	copy(rules, am.rules)
	handlers := make([]AlertHandler, len(am.handlers))
	copy(handlers, am.handlers)
	am.mutex.RUnlock()
	
	for _, rule := range rules {
		if am.evaluateRule(rule, metrics) {
			alert := Alert{
				Rule:        rule,
				Timestamp:   time.Now(),
				Description: fmt.Sprintf("Alert: %s", rule.Description),
			}
			
			for _, handler := range handlers {
				handler.Handle(alert)
			}
		}
	}
}

// evaluateRule 评估单个规则
func (am *AlertManager) evaluateRule(rule AlertRule, metrics map[string]interface{}) bool {
	// 简化的规则评估逻辑
	// 实际实现中需要更复杂的指标路径解析
	return false
}

// LogAlertHandler 日志告警处理器
type LogAlertHandler struct{}

// Handle 处理告警
func (lah *LogAlertHandler) Handle(alert Alert) error {
	fmt.Printf("[ALERT] %s: %s at %s\n", 
		alert.Rule.Severity, 
		alert.Description, 
		alert.Timestamp.Format(time.RFC3339))
	return nil
}

// 全局性能监控器
var (
	GlobalPerformanceMonitor *PerformanceMonitor
	monitorOnce              sync.Once
)

// InitGlobalPerformanceMonitor 初始化全局性能监控器
func InitGlobalPerformanceMonitor(config MonitorConfig) {
	monitorOnce.Do(func() {
		GlobalPerformanceMonitor = NewPerformanceMonitor(config)
		
		// 注册全局组件
		if GlobalMemoryPool != nil {
			GlobalPerformanceMonitor.RegisterMemoryPool(GlobalMemoryPool)
		}
		if GlobalBatchManager != nil {
			GlobalPerformanceMonitor.RegisterBatchManager(GlobalBatchManager)
		}
		if GlobalNetworkOptimizer != nil {
			GlobalPerformanceMonitor.RegisterNetworkOptimizer(GlobalNetworkOptimizer)
		}
		
		GlobalPerformanceMonitor.Start()
	})
}

// GetGlobalPerformanceMonitor 获取全局性能监控器
func GetGlobalPerformanceMonitor() *PerformanceMonitor {
	if GlobalPerformanceMonitor == nil {
		InitGlobalPerformanceMonitor(DefaultMonitorConfig)
	}
	return GlobalPerformanceMonitor
}