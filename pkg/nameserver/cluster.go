package nameserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"go-rocketmq/pkg/cluster"

	remoting "github.com/chenjy16/go-rocketmq-remoting"
)

// NameServerCluster NameServer集群管理器
type NameServerCluster struct {
	config          *ClusterConfig
	localNameServer *NameServer
	clusterManager  *cluster.ClusterManager
	peerClients     map[string]*NameServerClient
	mutex           sync.RWMutex
	running         bool
	shutdown        chan struct{}
}

// ClusterConfig NameServer集群配置
type ClusterConfig struct {
	// 本地NameServer配置
	LocalConfig *Config

	// 集群配置
	ClusterName       string        `json:"clusterName"`
	PeerAddresses     []string      `json:"peerAddresses"`     // 其他NameServer节点地址
	SyncInterval      time.Duration `json:"syncInterval"`      // 同步间隔
	HeartbeatInterval time.Duration `json:"heartbeatInterval"` // 心跳间隔
}

// NameServerClient NameServer客户端，用于与其他NameServer通信
type NameServerClient struct {
	address       string
	lastHeartbeat time.Time
	healthy       bool
	mutex         sync.RWMutex
}

// NewNameServerCluster 创建NameServer集群实例
func NewNameServerCluster(config *ClusterConfig, localNameServer *NameServer) *NameServerCluster {
	return &NameServerCluster{
		config:          config,
		localNameServer: localNameServer,
		clusterManager:  cluster.NewClusterManager(config.ClusterName),
		peerClients:     make(map[string]*NameServerClient),
		shutdown:        make(chan struct{}),
	}
}

// Start 启动NameServer集群
func (nsc *NameServerCluster) Start() error {
	if nsc.running {
		return fmt.Errorf("NameServer cluster already running")
	}

	nsc.running = true
	log.Printf("Starting NameServer cluster: %s", nsc.config.ClusterName)

	// 启动本地集群管理器
	if err := nsc.clusterManager.Start(); err != nil {
		return fmt.Errorf("failed to start cluster manager: %v", err)
	}

	// 初始化Peer客户端
	nsc.initPeerClients()

	// 启动集群同步
	go nsc.syncWithPeers()

	// 启动心跳检测
	go nsc.heartbeatToPeers()

	// 启动HTTP管理接口
	go nsc.startHTTPServer()

	log.Printf("NameServer cluster started successfully")
	return nil
}

// Stop 停止NameServer集群
func (nsc *NameServerCluster) Stop() {
	if !nsc.running {
		return
	}

	nsc.running = false
	close(nsc.shutdown)
	nsc.clusterManager.Stop()

	log.Printf("NameServer cluster stopped")
}

// initPeerClients 初始化Peer客户端
func (nsc *NameServerCluster) initPeerClients() {
	nsc.mutex.Lock()
	defer nsc.mutex.Unlock()

	for _, addr := range nsc.config.PeerAddresses {
		nsc.peerClients[addr] = &NameServerClient{
			address: addr,
			healthy: false,
		}
		log.Printf("Initialized peer client for: %s", addr)
	}
}

// syncWithPeers 与Peer节点同步数据
func (nsc *NameServerCluster) syncWithPeers() {
	ticker := time.NewTicker(nsc.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-nsc.shutdown:
			return
		case <-ticker.C:
			nsc.doSyncWithPeers()
		}
	}
}

// doSyncWithPeers 执行与Peer节点的数据同步
func (nsc *NameServerCluster) doSyncWithPeers() {
	nsc.mutex.RLock()
	defer nsc.mutex.RUnlock()

	for addr, client := range nsc.peerClients {
		go func(peerAddr string, peerClient *NameServerClient) {
			if err := nsc.syncWithPeer(peerAddr); err != nil {
				log.Printf("Failed to sync with peer %s: %v", peerAddr, err)
				peerClient.setHealthy(false)
			} else {
				peerClient.setHealthy(true)
				log.Printf("Successfully synced with peer: %s", peerAddr)
			}
		}(addr, client)
	}
}

// syncWithPeer 与单个Peer节点同步数据
func (nsc *NameServerCluster) syncWithPeer(peerAddr string) error {
	// 这里实现与Peer节点的数据同步逻辑
	// 由于NameServer主要存储路由信息，同步逻辑相对简单
	// 可以通过HTTP接口获取Peer的集群信息并合并到本地

	// 获取Peer的集群信息
	clusterInfo, err := nsc.getPeerClusterInfo(peerAddr)
	if err != nil {
		return fmt.Errorf("failed to get peer cluster info: %v", err)
	}

	// 合并集群信息到本地
	nsc.mergeClusterInfo(clusterInfo)

	return nil
}

// getPeerClusterInfo 获取Peer节点的集群信息
func (nsc *NameServerCluster) getPeerClusterInfo(peerAddr string) (*remoting.ClusterInfo, error) {
	// 构造HTTP请求URL
	url := fmt.Sprintf("http://%s/cluster/info", peerAddr)

	// 发送HTTP请求
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
	}

	// 解析响应
	var clusterInfo remoting.ClusterInfo
	if err := json.NewDecoder(resp.Body).Decode(&clusterInfo); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return &clusterInfo, nil
}

// mergeClusterInfo 合并集群信息
func (nsc *NameServerCluster) mergeClusterInfo(peerClusterInfo *remoting.ClusterInfo) {
	// 合并Broker地址表
	for brokerName, brokerAddrs := range peerClusterInfo.BrokerAddrTable {
		// 在实际实现中，需要检查数据的新鲜度并决定是否更新
		// 这里简化处理，直接更新
		nsc.localNameServer.routeTable.mutex.Lock()
		nsc.localNameServer.routeTable.brokerAddrTable[brokerName] = brokerAddrs
		nsc.localNameServer.routeTable.mutex.Unlock()
	}

	// 合并集群地址表
	for clusterName, brokerNames := range peerClusterInfo.ClusterAddrTable {
		nsc.localNameServer.routeTable.mutex.Lock()
		nsc.localNameServer.routeTable.clusterAddrTable[clusterName] = brokerNames
		nsc.localNameServer.routeTable.mutex.Unlock()
	}

	log.Printf("Merged cluster info from peer")
}

// heartbeatToPeers 向Peer节点发送心跳
func (nsc *NameServerCluster) heartbeatToPeers() {
	ticker := time.NewTicker(nsc.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-nsc.shutdown:
			return
		case <-ticker.C:
			nsc.doHeartbeatToPeers()
		}
	}
}

// doHeartbeatToPeers 执行向Peer节点发送心跳
func (nsc *NameServerCluster) doHeartbeatToPeers() {
	nsc.mutex.RLock()
	defer nsc.mutex.RUnlock()

	for addr, client := range nsc.peerClients {
		go func(peerAddr string, peerClient *NameServerClient) {
			if err := nsc.sendHeartbeatToPeer(peerAddr); err != nil {
				log.Printf("Failed to send heartbeat to peer %s: %v", peerAddr, err)
				peerClient.setHealthy(false)
			} else {
				peerClient.setHealthy(true)
				peerClient.updateHeartbeat()
				log.Printf("Heartbeat sent to peer: %s", peerAddr)
			}
		}(addr, client)
	}
}

// sendHeartbeatToPeer 向单个Peer节点发送心跳
func (nsc *NameServerCluster) sendHeartbeatToPeer(peerAddr string) error {
	// 构造心跳数据
	heartbeatData := map[string]interface{}{
		"clusterName": nsc.config.ClusterName,
		"timestamp":   time.Now().UnixMilli(),
		"status":      "alive",
	}

	// 发送心跳数据到Peer节点
	url := fmt.Sprintf("http://%s/heartbeat", peerAddr)
	data, _ := json.Marshal(heartbeatData)

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

// startHTTPServer 启动HTTP管理接口
func (nsc *NameServerCluster) startHTTPServer() {
	mux := http.NewServeMux()

	// 集群信息接口
	mux.HandleFunc("/cluster/info", func(w http.ResponseWriter, r *http.Request) {
		clusterInfo := nsc.localNameServer.GetAllClusterInfo()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(clusterInfo)
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
		status := nsc.clusterManager.GetClusterStatus()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	// 监听HTTP端口（使用NameServer端口+1000）
	httpAddr := fmt.Sprintf(":%d", nsc.config.LocalConfig.ListenPort+1000)
	log.Printf("Starting NameServer cluster HTTP server on %s", httpAddr)

	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Printf("Failed to start HTTP server: %v", err)
	}
}

// setHealthy 设置健康状态
func (nsc *NameServerClient) setHealthy(healthy bool) {
	nsc.mutex.Lock()
	defer nsc.mutex.Unlock()
	nsc.healthy = healthy
}

// updateHeartbeat 更新心跳时间
func (nsc *NameServerClient) updateHeartbeat() {
	nsc.mutex.Lock()
	defer nsc.mutex.Unlock()
	nsc.lastHeartbeat = time.Now()
}

// IsHealthy 检查是否健康
func (nsc *NameServerClient) IsHealthy() bool {
	nsc.mutex.RLock()
	defer nsc.mutex.RUnlock()
	return nsc.healthy
}

// GetLastHeartbeat 获取最后心跳时间
func (nsc *NameServerClient) GetLastHeartbeat() time.Time {
	nsc.mutex.RLock()
	defer nsc.mutex.RUnlock()
	return nsc.lastHeartbeat
}

// DefaultClusterConfig 默认集群配置
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		LocalConfig:       DefaultConfig(),
		ClusterName:       "DEFAULT_CLUSTER",
		PeerAddresses:     []string{},
		SyncInterval:      30 * time.Second,
		HeartbeatInterval: 5 * time.Second,
	}
}
