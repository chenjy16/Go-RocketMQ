package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"go-rocketmq/pkg/cluster"
	"go-rocketmq/pkg/ha"
)

// BrokerCluster Broker集群管理器
type BrokerCluster struct {
	config         *ClusterConfig
	localBroker    *Broker
	clusterManager *cluster.ClusterManager
	haService      *ha.HAService
	mutex          sync.RWMutex
	running        bool
	shutdown       chan struct{}
}

// ClusterConfig Broker集群配置
type ClusterConfig struct {
	// 本地Broker配置
	LocalConfig *Config

	// 集群配置
	ClusterName       string        `json:"clusterName"`
	NameServerAddress string        `json:"nameServerAddress"` // NameServer地址
	PeerAddresses     []string      `json:"peerAddresses"`     // 其他Broker节点地址
	SyncInterval      time.Duration `json:"syncInterval"`      // 同步间隔
	HeartbeatInterval time.Duration `json:"heartbeatInterval"` // 心跳间隔

	// HA配置
	HAConfig *ha.HAConfig
}

// BrokerInfo Broker信息扩展
type BrokerInfo struct {
	*cluster.BrokerInfo
	NameserverAddr string `json:"nameserverAddr"`
}

// NewBrokerCluster 创建Broker集群实例
func NewBrokerCluster(config *ClusterConfig, localBroker *Broker) *BrokerCluster {
	// 创建集群管理器
	clusterManager := cluster.NewClusterManager(config.ClusterName)

	// 如果配置了HA服务，创建HA服务
	var haService *ha.HAService
	if config.HAConfig != nil {
		// Note: We need to get the commit log from the broker's message store
		// This is a simplified implementation - in practice, you'd need to access the commit log properly
		haService = ha.NewHAService(config.HAConfig, nil)
	}

	return &BrokerCluster{
		config:         config,
		localBroker:    localBroker,
		clusterManager: clusterManager,
		haService:      haService,
		shutdown:       make(chan struct{}),
	}
}

// Start 启动Broker集群
func (bc *BrokerCluster) Start() error {
	if bc.running {
		return fmt.Errorf("Broker cluster already running")
	}

	bc.running = true
	log.Printf("Starting Broker cluster: %s", bc.config.ClusterName)

	// 启动本地集群管理器
	if err := bc.clusterManager.Start(); err != nil {
		return fmt.Errorf("failed to start cluster manager: %v", err)
	}

	// 启动HA服务（如果配置了）
	if bc.haService != nil {
		if err := bc.haService.Start(); err != nil {
			return fmt.Errorf("failed to start HA service: %v", err)
		}
		log.Printf("HA service started with role: %v", bc.config.HAConfig.BrokerRole)
	}

	// 注册到NameServer
	if err := bc.registerToNameServer(); err != nil {
		return fmt.Errorf("failed to register to NameServer: %v", err)
	}

	// 启动集群同步
	go bc.syncWithPeers()

	// 启动心跳检测
	go bc.heartbeatToPeers()

	// 启动HTTP管理接口
	go bc.startHTTPServer()

	// 定期向NameServer注册
	go bc.periodicRegisterToNameServer()

	log.Printf("Broker cluster started successfully")
	return nil
}

// Stop 停止Broker集群
func (bc *BrokerCluster) Stop() {
	if !bc.running {
		return
	}

	bc.running = false
	close(bc.shutdown)
	bc.clusterManager.Stop()

	if bc.haService != nil {
		bc.haService.Shutdown()
	}

	log.Printf("Broker cluster stopped")
}

// registerToNameServer 注册到NameServer
func (bc *BrokerCluster) registerToNameServer() error {
	// 构造Broker信息
	brokerInfo := &cluster.BrokerInfo{
		BrokerName:     bc.localBroker.config.BrokerName,
		BrokerId:       bc.localBroker.config.BrokerId,
		ClusterName:    bc.config.ClusterName,
		BrokerAddr:     fmt.Sprintf("localhost:%d", bc.localBroker.config.ListenPort),
		Version:        "V1",
		DataVersion:    time.Now().UnixMilli(),
		LastUpdateTime: time.Now().UnixMilli(),
		Role:           bc.getBrokerRole(),
		Status:         cluster.ONLINE,
		Metrics: &cluster.BrokerMetrics{
			CpuUsage:        0.0,
			MemoryUsage:     0.0,
			DiskUsage:       0.0,
			NetworkIn:       0,
			NetworkOut:      0,
			MessageCount:    0,
			Tps:             0,
			QueueDepth:      0,
			ConnectionCount: 0,
		},
	}

	// 注册到本地集群管理器
	if err := bc.clusterManager.RegisterBroker(brokerInfo); err != nil {
		return fmt.Errorf("failed to register broker to cluster manager: %v", err)
	}

	// 注册到NameServer
	if err := bc.doRegisterToNameServer(); err != nil {
		return fmt.Errorf("failed to register to NameServer: %v", err)
	}

	log.Printf("Broker registered to NameServer: %s", bc.config.NameServerAddress)
	return nil
}

// doRegisterToNameServer 执行向NameServer注册
func (bc *BrokerCluster) doRegisterToNameServer() error {
	// 这里应该通过Remoting协议向NameServer注册
	// 简化实现，使用HTTP方式注册

	// 构造注册数据
	registerData := map[string]interface{}{
		"clusterName":  bc.config.ClusterName,
		"brokerName":   bc.localBroker.config.BrokerName,
		"brokerAddr":   fmt.Sprintf("localhost:%d", bc.localBroker.config.ListenPort),
		"brokerId":     bc.localBroker.config.BrokerId,
		"haServerAddr": fmt.Sprintf("localhost:%d", bc.config.HAConfig.HaListenPort),
	}

	// 发送注册请求到NameServer
	url := fmt.Sprintf("http://%s/broker/register", bc.config.NameServerAddress)
	data, _ := json.Marshal(registerData)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
	}

	return nil
}

// periodicRegisterToNameServer 定期向NameServer注册
func (bc *BrokerCluster) periodicRegisterToNameServer() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒注册一次
	defer ticker.Stop()

	for {
		select {
		case <-bc.shutdown:
			return
		case <-ticker.C:
			if err := bc.registerToNameServer(); err != nil {
				log.Printf("Failed to register to NameServer: %v", err)
			}
		}
	}
}

// getBrokerRole 获取Broker角色
func (bc *BrokerCluster) getBrokerRole() string {
	if bc.config.HAConfig != nil {
		switch bc.config.HAConfig.BrokerRole {
		case ha.SYNC_MASTER:
			return "MASTER"
		case ha.ASYNC_MASTER:
			return "MASTER"
		case ha.SLAVE:
			return "SLAVE"
		default:
			return "UNKNOWN"
		}
	}
	return "STANDALONE"
}

// syncWithPeers 与Peer节点同步数据
func (bc *BrokerCluster) syncWithPeers() {
	ticker := time.NewTicker(bc.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bc.shutdown:
			return
		case <-ticker.C:
			bc.doSyncWithPeers()
		}
	}
}

// doSyncWithPeers 执行与Peer节点的数据同步
func (bc *BrokerCluster) doSyncWithPeers() {
	// Broker之间的数据同步主要通过HA服务完成
	// 这里可以实现一些额外的集群管理功能
	log.Printf("Syncing with peers...")
}

// heartbeatToPeers 向Peer节点发送心跳
func (bc *BrokerCluster) heartbeatToPeers() {
	ticker := time.NewTicker(bc.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bc.shutdown:
			return
		case <-ticker.C:
			bc.doHeartbeatToPeers()
		}
	}
}

// doHeartbeatToPeers 执行向Peer节点发送心跳
func (bc *BrokerCluster) doHeartbeatToPeers() {
	// Broker之间的心跳主要通过HA服务完成
	// 这里可以实现一些额外的集群管理功能
	log.Printf("Sending heartbeat to peers...")
}

// startHTTPServer 启动HTTP管理接口
func (bc *BrokerCluster) startHTTPServer() {
	mux := http.NewServeMux()

	// 集群信息接口
	mux.HandleFunc("/cluster/info", func(w http.ResponseWriter, r *http.Request) {
		// 返回集群信息
		info := map[string]interface{}{
			"clusterName": bc.config.ClusterName,
			"brokerName":  bc.localBroker.config.BrokerName,
			"brokerId":    bc.localBroker.config.BrokerId,
			"role":        bc.getBrokerRole(),
			"haConfig":    bc.config.HAConfig,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info)
	})

	// 心跳接口
	mux.HandleFunc("/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var heartbeatData map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&heartbeatData); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// 处理心跳数据
		log.Printf("Received heartbeat from peer: %v", heartbeatData)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// 集群状态接口
	mux.HandleFunc("/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		status := bc.clusterManager.GetClusterStatus()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	// HA状态接口
	mux.HandleFunc("/ha/status", func(w http.ResponseWriter, r *http.Request) {
		if bc.haService != nil {
			status := bc.haService.GetReplicationStatus()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(status)
		} else {
			http.Error(w, "HA service not enabled", http.StatusServiceUnavailable)
		}
	})

	// 监听HTTP端口（使用Broker端口+1000）
	httpAddr := fmt.Sprintf(":%d", bc.localBroker.config.ListenPort+1000)
	log.Printf("Starting Broker cluster HTTP server on %s", httpAddr)

	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Printf("Failed to start HTTP server: %v", err)
	}
}

// UpdateBrokerMetrics 更新Broker指标
func (bc *BrokerCluster) UpdateBrokerMetrics(metrics *cluster.BrokerMetrics) error {
	return bc.clusterManager.UpdateBrokerMetrics(bc.localBroker.config.BrokerName, metrics)
}

// GetHAService 获取HA服务
func (bc *BrokerCluster) GetHAService() *ha.HAService {
	return bc.haService
}

// DefaultClusterConfig 默认集群配置
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		LocalConfig:       DefaultBrokerConfig(),
		ClusterName:       "DEFAULT_CLUSTER",
		NameServerAddress: "127.0.0.1:9876",
		PeerAddresses:     []string{},
		SyncInterval:      30 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		HAConfig:          ha.DefaultHAConfig(),
	}
}
