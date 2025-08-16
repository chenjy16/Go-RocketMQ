package nameserver

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"go-rocketmq/pkg/acl"
	"go-rocketmq/pkg/common"
	"go-rocketmq/pkg/protocol"
)

// NameServer NameServer服务器
type NameServer struct {
	config       *Config
	routeTable   *RouteInfoManager
	brokerLiveTable map[string]*BrokerLiveInfo
	filterServerTable map[string][]string
	aclMiddleware *acl.AclMiddleware
	mutex        sync.RWMutex
	listener     net.Listener
	shutdown     chan struct{}
}

// Config NameServer配置
type Config struct {
	ListenPort                int           `json:"listenPort"`
	ClusterTestEnable         bool          `json:"clusterTestEnable"`
	OrderMessageEnable        bool          `json:"orderMessageEnable"`
	ScanNotActiveBrokerInterval time.Duration `json:"scanNotActiveBrokerInterval"`
	AclEnable                 bool          `json:"aclEnable"`
	AclConfigFile             string        `json:"aclConfigFile"`
}

// BrokerLiveInfo Broker存活信息
type BrokerLiveInfo struct {
	LastUpdateTimestamp time.Time `json:"lastUpdateTimestamp"`
	DataVersion         *protocol.DataVersion `json:"dataVersion"`
	Channel             net.Conn  `json:"-"`
	HaServerAddr        string    `json:"haServerAddr"`
}

// RouteInfoManager 路由信息管理器
type RouteInfoManager struct {
	topicQueueTable   map[string][]*common.MessageQueue  // topic -> queues
	brokerAddrTable   map[string]map[int64]string        // brokerName -> {brokerId -> address}
	clusterAddrTable  map[string][]string                // clusterName -> brokerNames
	brokerLiveTable   map[string]*BrokerLiveInfo         // brokerAddr -> BrokerLiveInfo
	filterServerTable map[string][]string                // brokerAddr -> filterServerList
	mutex             sync.RWMutex
}

// NewNameServer 创建NameServer实例
func NewNameServer(config *Config) *NameServer {
	ns := &NameServer{
		config:            config,
		routeTable:        NewRouteInfoManager(),
		brokerLiveTable:   make(map[string]*BrokerLiveInfo),
		filterServerTable: make(map[string][]string),
		shutdown:          make(chan struct{}),
	}
	
	// 初始化ACL中间件
	if config.AclEnable {
		validator := acl.NewPlainAclValidator(config.AclConfigFile)
		ns.aclMiddleware = acl.NewAclMiddleware(validator, true)
	}
	
	return ns
}

// NewRouteInfoManager 创建路由信息管理器
func NewRouteInfoManager() *RouteInfoManager {
	return &RouteInfoManager{
		topicQueueTable:   make(map[string][]*common.MessageQueue),
		brokerAddrTable:   make(map[string]map[int64]string),
		clusterAddrTable:  make(map[string][]string),
		brokerLiveTable:   make(map[string]*BrokerLiveInfo),
		filterServerTable: make(map[string][]string),
	}
}

// Start 启动NameServer
func (ns *NameServer) Start() error {
	addr := fmt.Sprintf(":%d", ns.config.ListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	ns.listener = listener
	log.Printf("NameServer started on %s", addr)

	// 启动清理任务
	go ns.scanNotActiveBroker()

	// 处理连接
	go ns.handleConnections()

	return nil
}

// Stop 停止NameServer
func (ns *NameServer) Stop() {
	close(ns.shutdown)
	if ns.listener != nil {
		ns.listener.Close()
	}
	log.Println("NameServer stopped")
}

// handleConnections 处理客户端连接
func (ns *NameServer) handleConnections() {
	for {
		select {
		case <-ns.shutdown:
			return
		default:
			conn, err := ns.listener.Accept()
			if err != nil {
				select {
				case <-ns.shutdown:
					return
				default:
					log.Printf("Failed to accept connection: %v", err)
					continue
				}
			}
			go ns.handleConnection(conn)
		}
	}
}

// handleConnection 处理单个连接
func (ns *NameServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	
	remoteAddr := conn.RemoteAddr().String()
	log.Printf("New connection from %s", remoteAddr)
	
	// ACL权限验证
	if ns.config.AclEnable && ns.aclMiddleware != nil {
		if err := ns.validateConnection(conn, remoteAddr); err != nil {
			log.Printf("ACL validation failed for %s: %v", remoteAddr, err)
			return
		}
	}
	
	// 处理RocketMQ协议消息
	reader := bufio.NewReader(conn)
	for {
		// 读取并处理请求
		request, err := ns.readRemotingCommand(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("Failed to read request from %s: %v", remoteAddr, err)
			}
			break
		}
		
		// 处理请求并生成响应
		response := ns.processRequest(request, conn)
		
		// 发送响应
		if err := ns.writeRemotingCommand(conn, response); err != nil {
			log.Printf("Failed to write response to %s: %v", remoteAddr, err)
			break
		}
	}
}

// RegisterBroker 注册Broker
func (ns *NameServer) RegisterBroker(
	clusterName string,
	brokerAddr string,
	brokerName string,
	brokerId int64,
	haServerAddr string,
	topicConfigWrapper *protocol.TopicConfigSerializeWrapper,
	filterServerList []string,
	channel net.Conn,
) *protocol.RegisterBrokerResult {
	
	// ACL权限验证
	if ns.config.AclEnable && ns.aclMiddleware != nil {
		remoteAddr := ""
		if channel != nil {
			remoteAddr = channel.RemoteAddr().String()
		}
		
		requestData := map[string]string{
			"clusterName": clusterName,
			"brokerName":  brokerName,
			"brokerAddr":  brokerAddr,
			"operation":   "REGISTER_BROKER",
		}
		
		if err := ns.ValidateTopicAccess(requestData, "BROKER_REGISTER", "ADMIN", remoteAddr); err != nil {
			log.Printf("ACL validation failed for broker registration from %s: %v", remoteAddr, err)
			return &protocol.RegisterBrokerResult{
				HaServerAddr: "",
				MasterAddr:   "",
			}
		}
	}
	
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	result := &protocol.RegisterBrokerResult{
		HaServerAddr: haServerAddr,
		MasterAddr:   brokerAddr,
	}

	// 更新集群信息
	ns.routeTable.mutex.Lock()
	brokerNames := ns.routeTable.clusterAddrTable[clusterName]
	if brokerNames == nil {
		brokerNames = make([]string, 0)
	}
	
	// 检查brokerName是否已存在
	found := false
	for _, name := range brokerNames {
		if name == brokerName {
			found = true
			break
		}
	}
	if !found {
		brokerNames = append(brokerNames, brokerName)
		ns.routeTable.clusterAddrTable[clusterName] = brokerNames
	}

	// 更新Broker地址表
	brokerAddrs := ns.routeTable.brokerAddrTable[brokerName]
	if brokerAddrs == nil {
		brokerAddrs = make(map[int64]string)
		ns.routeTable.brokerAddrTable[brokerName] = brokerAddrs
	}
	
	oldAddr := brokerAddrs[brokerId]
	if oldAddr != brokerAddr {
		brokerAddrs[brokerId] = brokerAddr
	}

	// 更新Topic配置
	if topicConfigWrapper != nil && topicConfigWrapper.TopicConfigTable != nil {
		for topic, topicConfig := range topicConfigWrapper.TopicConfigTable {
			ns.createAndUpdateQueueData(brokerName, topicConfig)
			log.Printf("Registered topic: %s for broker: %s", topic, brokerName)
		}
	}

	// 更新Broker存活信息
	brokerLiveInfo := &BrokerLiveInfo{
		LastUpdateTimestamp: time.Now(),
		DataVersion:         topicConfigWrapper.DataVersion,
		Channel:             channel,
		HaServerAddr:        haServerAddr,
	}
	ns.routeTable.brokerLiveTable[brokerAddr] = brokerLiveInfo

	// 更新过滤服务器列表
	if filterServerList != nil {
		ns.routeTable.filterServerTable[brokerAddr] = filterServerList
	}

	ns.routeTable.mutex.Unlock()

	log.Printf("Broker registered: cluster=%s, brokerName=%s, brokerId=%d, addr=%s", 
		clusterName, brokerName, brokerId, brokerAddr)

	return result
}

// createAndUpdateQueueData 创建和更新队列数据
func (ns *NameServer) createAndUpdateQueueData(brokerName string, topicConfig *protocol.TopicConfig) {
	queues := make([]*common.MessageQueue, 0, topicConfig.WriteQueueNums)
	
	for i := int32(0); i < topicConfig.WriteQueueNums; i++ {
		queue := &common.MessageQueue{
			Topic:      topicConfig.TopicName,
			BrokerName: brokerName,
			QueueId:    i,
		}
		queues = append(queues, queue)
	}
	
	ns.routeTable.topicQueueTable[topicConfig.TopicName] = queues
}

// GetRouteInfoByTopic 根据Topic获取路由信息
func (ns *NameServer) GetRouteInfoByTopic(topic string) *protocol.TopicRouteData {
	ns.routeTable.mutex.RLock()
	defer ns.routeTable.mutex.RUnlock()

	routeData := &protocol.TopicRouteData{
		OrderTopicConf: "",
		QueueDatas:     make([]*protocol.QueueData, 0),
		BrokerDatas:    make([]*protocol.BrokerData, 0),
		FilterServerTable: make(map[string][]string),
	}

	// 获取队列数据
	queues := ns.routeTable.topicQueueTable[topic]
	if queues == nil {
		return nil
	}

	// 按BrokerName分组队列
	brokerQueues := make(map[string][]*common.MessageQueue)
	for _, queue := range queues {
		brokerQueues[queue.BrokerName] = append(brokerQueues[queue.BrokerName], queue)
	}

	// 构建QueueData
	for brokerName, qs := range brokerQueues {
		queueData := &protocol.QueueData{
			BrokerName:     brokerName,
			ReadQueueNums:  int32(len(qs)),
			WriteQueueNums: int32(len(qs)),
			Perm:           6, // 读写权限
			TopicSysFlag:   0,
		}
		routeData.QueueDatas = append(routeData.QueueDatas, queueData)

		// 构建BrokerData
		brokerAddrs := ns.routeTable.brokerAddrTable[brokerName]
		if brokerAddrs != nil {
			// 从集群表中查找对应的集群名称
			clusterName := ""
			for cluster, brokers := range ns.routeTable.clusterAddrTable {
				for _, broker := range brokers {
					if broker == brokerName {
						clusterName = cluster
						break
					}
				}
				if clusterName != "" {
					break
				}
			}
			
			brokerData := &protocol.BrokerData{
				Cluster:     clusterName,
				BrokerName:  brokerName,
				BrokerAddrs: brokerAddrs,
			}
			routeData.BrokerDatas = append(routeData.BrokerDatas, brokerData)
		}
	}

	return routeData
}

// scanNotActiveBroker 扫描不活跃的Broker
func (ns *NameServer) scanNotActiveBroker() {
	ticker := time.NewTicker(ns.config.ScanNotActiveBrokerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ns.shutdown:
			return
		case <-ticker.C:
			ns.scanNotActiveBrokerInternal()
		}
	}
}

// scanNotActiveBrokerInternal 扫描不活跃Broker的内部实现
func (ns *NameServer) scanNotActiveBrokerInternal() {
	ns.mutex.Lock()
	ns.routeTable.mutex.Lock()
	defer ns.routeTable.mutex.Unlock()
	defer ns.mutex.Unlock()

	now := time.Now()
	toRemove := make([]string, 0)

	for brokerAddr, liveInfo := range ns.routeTable.brokerLiveTable {
		if now.Sub(liveInfo.LastUpdateTimestamp) > 2*time.Minute {
			toRemove = append(toRemove, brokerAddr)
			if liveInfo.Channel != nil {
				liveInfo.Channel.Close()
			}
		}
	}

	for _, brokerAddr := range toRemove {
		delete(ns.routeTable.brokerLiveTable, brokerAddr)
		delete(ns.routeTable.filterServerTable, brokerAddr)
		delete(ns.brokerLiveTable, brokerAddr)
		delete(ns.filterServerTable, brokerAddr)
		log.Printf("Removed inactive broker: %s", brokerAddr)
	}
}

// GetAllClusterInfo 获取所有集群信息
func (ns *NameServer) GetAllClusterInfo() *protocol.ClusterInfo {
	ns.routeTable.mutex.RLock()
	defer ns.routeTable.mutex.RUnlock()

	clusterInfo := &protocol.ClusterInfo{
		BrokerAddrTable:  make(map[string]map[int64]string),
		ClusterAddrTable: make(map[string][]string),
	}

	// 复制数据
	for k, v := range ns.routeTable.brokerAddrTable {
		clusterInfo.BrokerAddrTable[k] = make(map[int64]string)
		for brokerId, addr := range v {
			clusterInfo.BrokerAddrTable[k][brokerId] = addr
		}
	}

	for k, v := range ns.routeTable.clusterAddrTable {
		clusterInfo.ClusterAddrTable[k] = make([]string, len(v))
		copy(clusterInfo.ClusterAddrTable[k], v)
	}

	return clusterInfo
}

// validateConnection 验证连接的ACL权限
func (ns *NameServer) validateConnection(conn net.Conn, remoteAddr string) error {
	if ns.aclMiddleware == nil {
		return fmt.Errorf("ACL middleware not initialized")
	}
	
	// 简化的请求数据，实际应该从协议中解析
	requestData := map[string]string{
		"remoteAddress": remoteAddr,
		"operation":     "CONNECT",
	}
	
	// 进行管理员请求验证（连接验证）
	_, err := ns.aclMiddleware.ValidateAdminRequest(requestData, remoteAddr)
	return err
}

// ValidateTopicAccess 验证Topic访问权限
func (ns *NameServer) ValidateTopicAccess(requestData map[string]string, topicName, operation, remoteAddr string) error {
	if !ns.config.AclEnable || ns.aclMiddleware == nil {
		return nil
	}
	
	switch operation {
	case "PUB":
		_, err := ns.aclMiddleware.ValidateProducerRequest(requestData, topicName, remoteAddr)
		return err
	case "SUB":
		// 对于订阅，需要groupName，这里简化处理
		groupName := requestData["groupName"]
		if groupName == "" {
			groupName = "DEFAULT_GROUP"
		}
		_, err := ns.aclMiddleware.ValidateConsumerRequest(requestData, topicName, groupName, remoteAddr)
		return err
	default:
		_, err := ns.aclMiddleware.ValidateAdminRequest(requestData, remoteAddr)
		return err
	}
}

// IsAclEnabled 检查ACL是否启用
func (ns *NameServer) IsAclEnabled() bool {
	return ns.config.AclEnable && ns.aclMiddleware != nil
}

// ReloadAclConfig 重新加载ACL配置
func (ns *NameServer) ReloadAclConfig() error {
	if !ns.config.AclEnable || ns.aclMiddleware == nil {
		return fmt.Errorf("ACL is not enabled")
	}
	return ns.aclMiddleware.ReloadConfig()
}

// readRemotingCommand 读取RocketMQ协议命令
func (ns *NameServer) readRemotingCommand(reader *bufio.Reader) (*protocol.RemotingCommand, error) {
	// 读取消息长度（4字节）
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBytes); err != nil {
		return nil, err
	}
	
	msgLength := binary.BigEndian.Uint32(lengthBytes)
	if msgLength == 0 || msgLength > 16*1024*1024 { // 最大16MB
		return nil, fmt.Errorf("invalid message length: %d", msgLength)
	}
	
	// 读取消息内容
	msgData := make([]byte, msgLength)
	if _, err := io.ReadFull(reader, msgData); err != nil {
		return nil, err
	}
	
	// 解析协议头长度（前4字节）
	headerLength := binary.BigEndian.Uint32(msgData[:4])
	if headerLength > msgLength-4 {
		return nil, fmt.Errorf("invalid header length: %d", headerLength)
	}
	
	// 解析协议头
	headerData := msgData[4 : 4+headerLength]
	var command protocol.RemotingCommand
	if err := json.Unmarshal(headerData, &command); err != nil {
		return nil, fmt.Errorf("failed to unmarshal header: %v", err)
	}
	
	// 设置消息体
	if msgLength > 4+headerLength {
		command.Body = msgData[4+headerLength:]
	}
	
	return &command, nil
}

// writeRemotingCommand 写入RocketMQ协议命令
func (ns *NameServer) writeRemotingCommand(conn net.Conn, command *protocol.RemotingCommand) error {
	// 序列化协议头
	headerData, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal header: %v", err)
	}
	
	// 计算总长度
	headerLength := len(headerData)
	bodyLength := len(command.Body)
	totalLength := 4 + headerLength + bodyLength // 4字节头长度 + 头数据 + 体数据
	
	// 构造完整消息
	buf := bytes.NewBuffer(make([]byte, 0, 4+totalLength))
	
	// 写入总长度
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(totalLength))
	buf.Write(lengthBytes)
	
	// 写入头长度
	headerLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(headerLengthBytes, uint32(headerLength))
	buf.Write(headerLengthBytes)
	
	// 写入头数据
	buf.Write(headerData)
	
	// 写入体数据
	if bodyLength > 0 {
		buf.Write(command.Body)
	}
	
	// 发送数据
	_, err = conn.Write(buf.Bytes())
	return err
}

// processRequest 处理请求
func (ns *NameServer) processRequest(request *protocol.RemotingCommand, conn net.Conn) *protocol.RemotingCommand {
	switch protocol.RequestCode(request.Code) {
	case protocol.RegisterBroker:
		return ns.handleRegisterBroker(request, conn)
	case protocol.UnregisterBroker:
		return ns.handleUnregisterBroker(request)
	case protocol.GetRouteInfoByTopic:
		return ns.handleGetRouteInfoByTopic(request)
	case protocol.GetBrokerClusterInfo:
		return ns.handleGetBrokerClusterInfo(request)
	case protocol.UpdateAndCreateTopic:
		return ns.handleUpdateAndCreateTopic(request)
	default:
		log.Printf("Unsupported request code: %d", request.Code)
		return ns.createErrorResponse(request, protocol.RequestCodeNotSupported, "Unsupported request code")
	}
}

// handleRegisterBroker 处理Broker注册请求
func (ns *NameServer) handleRegisterBroker(request *protocol.RemotingCommand, conn net.Conn) *protocol.RemotingCommand {
	// 解析请求参数
	clusterName := request.ExtFields["clusterName"]
	brokerName := request.ExtFields["brokerName"]
	brokerAddr := request.ExtFields["brokerAddr"]
	haServerAddr := request.ExtFields["haServerAddr"]
	
	// 解析TopicConfig数据
	var topicConfigWrapper *protocol.TopicConfigSerializeWrapper
	if len(request.Body) > 0 {
		topicConfigWrapper = &protocol.TopicConfigSerializeWrapper{}
		if err := json.Unmarshal(request.Body, topicConfigWrapper); err != nil {
			log.Printf("Failed to unmarshal topic config: %v", err)
			return ns.createErrorResponse(request, protocol.SystemError, "Invalid topic config data")
		}
	}
	
	// 注册Broker
	result := ns.RegisterBroker(clusterName, brokerAddr, brokerName, 0, haServerAddr, topicConfigWrapper, nil, conn)
	
	// 创建响应
	response := ns.createSuccessResponse(request)
	responseData, _ := json.Marshal(result)
	response.Body = responseData
	
	return response
}

// handleUnregisterBroker 处理Broker注销请求
func (ns *NameServer) handleUnregisterBroker(request *protocol.RemotingCommand) *protocol.RemotingCommand {
	clusterName := request.ExtFields["clusterName"]
	brokerName := request.ExtFields["brokerName"]
	brokerAddr := request.ExtFields["brokerAddr"]
	
	ns.mutex.Lock()
	defer ns.mutex.Unlock()
	
	// 从路由表中移除Broker信息
	ns.routeTable.mutex.Lock()
	defer ns.routeTable.mutex.Unlock()
	
	// 移除Broker地址
	if brokerAddrs, exists := ns.routeTable.brokerAddrTable[brokerName]; exists {
		for brokerId, addr := range brokerAddrs {
			if addr == brokerAddr {
				delete(brokerAddrs, brokerId)
				break
			}
		}
		if len(brokerAddrs) == 0 {
			delete(ns.routeTable.brokerAddrTable, brokerName)
		}
	}
	
	// 移除集群信息
	if brokerNames, exists := ns.routeTable.clusterAddrTable[clusterName]; exists {
		for i, name := range brokerNames {
			if name == brokerName {
				ns.routeTable.clusterAddrTable[clusterName] = append(brokerNames[:i], brokerNames[i+1:]...)
				break
			}
		}
	}
	
	// 移除存活信息
	delete(ns.routeTable.brokerLiveTable, brokerAddr)
	
	log.Printf("Unregistered broker: %s from cluster: %s", brokerName, clusterName)
	return ns.createSuccessResponse(request)
}

// handleGetRouteInfoByTopic 处理获取Topic路由信息请求
func (ns *NameServer) handleGetRouteInfoByTopic(request *protocol.RemotingCommand) *protocol.RemotingCommand {
	topic := request.ExtFields["topic"]
	if topic == "" {
		return ns.createErrorResponse(request, protocol.TopicNotExist, "Topic name is empty")
	}
	
	routeData := ns.GetRouteInfoByTopic(topic)
	if routeData == nil {
		return ns.createErrorResponse(request, protocol.TopicNotExist, fmt.Sprintf("Topic %s not exist", topic))
	}
	
	response := ns.createSuccessResponse(request)
	responseData, _ := json.Marshal(routeData)
	response.Body = responseData
	
	return response
}

// handleGetBrokerClusterInfo 处理获取集群信息请求
func (ns *NameServer) handleGetBrokerClusterInfo(request *protocol.RemotingCommand) *protocol.RemotingCommand {
	clusterInfo := ns.GetAllClusterInfo()
	
	response := ns.createSuccessResponse(request)
	responseData, _ := json.Marshal(clusterInfo)
	response.Body = responseData
	
	return response
}

// handleUpdateAndCreateTopic 处理更新和创建Topic请求
func (ns *NameServer) handleUpdateAndCreateTopic(request *protocol.RemotingCommand) *protocol.RemotingCommand {
	topic := request.ExtFields["topic"]
	if topic == "" {
		return ns.createErrorResponse(request, protocol.MessageIllegal, "Topic name is empty")
	}
	
	// 解析TopicConfig
	var topicConfig protocol.TopicConfig
	if len(request.Body) > 0 {
		if err := json.Unmarshal(request.Body, &topicConfig); err != nil {
			return ns.createErrorResponse(request, protocol.MessageIllegal, "Invalid topic config")
		}
	} else {
		// 使用默认配置
		topicConfig = protocol.TopicConfig{
			TopicName:      topic,
			ReadQueueNums:  4,
			WriteQueueNums: 4,
			Perm:           6, // 读写权限
		}
	}
	
	// 更新Topic配置（这里简化处理，实际应该通知所有Broker）
	log.Printf("Updated topic config: %s", topic)
	
	return ns.createSuccessResponse(request)
}

// createSuccessResponse 创建成功响应
func (ns *NameServer) createSuccessResponse(request *protocol.RemotingCommand) *protocol.RemotingCommand {
	return &protocol.RemotingCommand{
		Code:     protocol.RequestCode(protocol.Success),
		Language: "JAVA",
		Version:  request.Version,
		Opaque:   request.Opaque,
		Flag:     1, // 响应标志
		Remark:   "",
	}
}

// createErrorResponse 创建错误响应
func (ns *NameServer) createErrorResponse(request *protocol.RemotingCommand, code protocol.ResponseCode, remark string) *protocol.RemotingCommand {
	return &protocol.RemotingCommand{
		Code:     protocol.RequestCode(code),
		Language: "JAVA",
		Version:  request.Version,
		Opaque:   request.Opaque,
		Flag:     1, // 响应标志
		Remark:   remark,
	}
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		ListenPort:                  9876,
		ClusterTestEnable:           false,
		OrderMessageEnable:          false,
		ScanNotActiveBrokerInterval: 5 * time.Second,
		AclEnable:                   false,
		AclConfigFile:               "config/plain_acl.yml",
	}
}