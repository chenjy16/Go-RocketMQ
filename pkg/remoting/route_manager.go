package remoting

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"go-rocketmq/pkg/protocol"
)

// RouteManager 路由管理器
type RouteManager struct {
	nameServerAddrs []string
	connectionPool  *ConnectionPool
	topicRouteTable sync.Map // map[string]*protocol.TopicRouteData
	brokerAddrTable sync.Map // map[string]map[int64]string
	clusterAddrTable sync.Map // map[string][]string
	mutex           sync.RWMutex
	updateInterval  time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// BrokerInfo Broker信息
type BrokerInfo struct {
	Cluster    string
	BrokerName string
	BrokerID   int64
	BrokerAddr string
}

// MessageQueue 消息队列
type MessageQueue struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueId    int32  `json:"queueId"`
}

// NewRouteManager 创建路由管理器
func NewRouteManager(nameServerAddrs []string, connectionPool *ConnectionPool) *RouteManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RouteManager{
		nameServerAddrs: nameServerAddrs,
		connectionPool:  connectionPool,
		updateInterval:  30 * time.Second, // 默认30秒更新一次路由
		ctx:             ctx,
		cancel:          cancel,
	}
}

// Start 启动路由管理器
func (rm *RouteManager) Start() {
	rm.wg.Add(1)
	go rm.updateRouteRoutine()
}

// Stop 停止路由管理器
func (rm *RouteManager) Stop() {
	rm.cancel()
	rm.wg.Wait()
}

// updateRouteRoutine 路由更新例程
func (rm *RouteManager) updateRouteRoutine() {
	defer rm.wg.Done()
	
	ticker := time.NewTicker(rm.updateInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			rm.updateAllTopicRoutes()
		case <-rm.ctx.Done():
			return
		}
	}
}

// updateAllTopicRoutes 更新所有Topic路由
func (rm *RouteManager) updateAllTopicRoutes() {
	var topics []string
	rm.topicRouteTable.Range(func(key, value interface{}) bool {
		topics = append(topics, key.(string))
		return true
	})
	
	for _, topic := range topics {
		rm.UpdateTopicRoute(topic)
	}
}

// UpdateTopicRoute 更新指定Topic的路由信息
func (rm *RouteManager) UpdateTopicRoute(topic string) error {
	// 从NameServer获取路由信息
	routeData, err := rm.getTopicRouteFromNameServer(topic)
	if err != nil {
		return fmt.Errorf("failed to get route for topic %s: %v", topic, err)
	}
	
	if routeData == nil {
		return fmt.Errorf("no route data for topic %s", topic)
	}
	
	// 更新路由表
	rm.topicRouteTable.Store(topic, routeData)
	
	// 更新Broker地址表
	for _, brokerData := range routeData.BrokerDatas {
		rm.brokerAddrTable.Store(brokerData.BrokerName, brokerData.BrokerAddrs)
		
		// 更新集群地址表
		if clusterAddrs, exists := rm.clusterAddrTable.Load(brokerData.Cluster); exists {
			addrs := clusterAddrs.([]string)
			found := false
			for _, addr := range addrs {
				if addr == brokerData.BrokerName {
					found = true
					break
				}
			}
			if !found {
				addrs = append(addrs, brokerData.BrokerName)
				rm.clusterAddrTable.Store(brokerData.Cluster, addrs)
			}
		} else {
			rm.clusterAddrTable.Store(brokerData.Cluster, []string{brokerData.BrokerName})
		}
	}
	
	return nil
}

// getTopicRouteFromNameServer 从NameServer获取Topic路由
func (rm *RouteManager) getTopicRouteFromNameServer(topic string) (*protocol.TopicRouteData, error) {
	// 创建请求
	request := protocol.CreateRemotingCommand(protocol.GetRouteInfoByTopic)
	request.ExtFields = map[string]string{
		"topic": topic,
	}
	
	// 尝试从所有NameServer获取路由
	for _, nameServerAddr := range rm.nameServerAddrs {
		response, err := rm.connectionPool.SendSync(nameServerAddr, request)
		if err != nil {
			continue
		}
		
		if protocol.ResponseCode(response.Code) == protocol.Success {
			var routeData protocol.TopicRouteData
			if err := json.Unmarshal(response.Body, &routeData); err != nil {
				continue
			}
			return &routeData, nil
		}
	}
	
	return nil, fmt.Errorf("failed to get route from all name servers")
}

// GetTopicRoute 获取Topic路由信息
func (rm *RouteManager) GetTopicRoute(topic string) (*protocol.TopicRouteData, error) {
	// 先从缓存获取
	if routeValue, exists := rm.topicRouteTable.Load(topic); exists {
		return routeValue.(*protocol.TopicRouteData), nil
	}
	
	// 缓存中没有，从NameServer获取
	if err := rm.UpdateTopicRoute(topic); err != nil {
		return nil, err
	}
	
	if routeValue, exists := rm.topicRouteTable.Load(topic); exists {
		return routeValue.(*protocol.TopicRouteData), nil
	}
	
	return nil, fmt.Errorf("no route found for topic %s", topic)
}

// GetWriteQueues 获取可写队列列表
func (rm *RouteManager) GetWriteQueues(topic string) ([]*MessageQueue, error) {
	routeData, err := rm.GetTopicRoute(topic)
	if err != nil {
		return nil, err
	}
	
	var queues []*MessageQueue
	for _, queueData := range routeData.QueueDatas {
		for i := int32(0); i < queueData.WriteQueueNums; i++ {
			queues = append(queues, &MessageQueue{
				Topic:      topic,
				BrokerName: queueData.BrokerName,
				QueueId:    i,
			})
		}
	}
	
	return queues, nil
}

// GetReadQueues 获取可读队列列表
func (rm *RouteManager) GetReadQueues(topic string) ([]*MessageQueue, error) {
	routeData, err := rm.GetTopicRoute(topic)
	if err != nil {
		return nil, err
	}
	
	var queues []*MessageQueue
	for _, queueData := range routeData.QueueDatas {
		for i := int32(0); i < queueData.ReadQueueNums; i++ {
			queues = append(queues, &MessageQueue{
				Topic:      topic,
				BrokerName: queueData.BrokerName,
				QueueId:    i,
			})
		}
	}
	
	return queues, nil
}

// SelectQueue 选择队列（负载均衡）
func (rm *RouteManager) SelectQueue(topic string, lastBrokerName string) (*MessageQueue, error) {
	queues, err := rm.GetWriteQueues(topic)
	if err != nil {
		return nil, err
	}
	
	if len(queues) == 0 {
		return nil, fmt.Errorf("no available queues for topic %s", topic)
	}
	
	// 如果指定了上次使用的Broker，尝试避免使用同一个Broker
	if lastBrokerName != "" {
		var availableQueues []*MessageQueue
		for _, queue := range queues {
			if queue.BrokerName != lastBrokerName {
				availableQueues = append(availableQueues, queue)
			}
		}
		if len(availableQueues) > 0 {
			queues = availableQueues
		}
	}
	
	// 随机选择一个队列
	index := rand.Intn(len(queues))
	return queues[index], nil
}

// GetBrokerAddr 获取Broker地址
func (rm *RouteManager) GetBrokerAddr(brokerName string, brokerId int64) (string, error) {
	if brokerAddrs, exists := rm.brokerAddrTable.Load(brokerName); exists {
		addrs := brokerAddrs.(map[int64]string)
		if addr, ok := addrs[brokerId]; ok {
			return addr, nil
		}
	}
	return "", fmt.Errorf("broker address not found for %s:%d", brokerName, brokerId)
}

// GetMasterBrokerAddr 获取主Broker地址
func (rm *RouteManager) GetMasterBrokerAddr(brokerName string) (string, error) {
	return rm.GetBrokerAddr(brokerName, 0) // Master的ID通常是0
}

// GetAllBrokerAddrs 获取指定Broker的所有地址
func (rm *RouteManager) GetAllBrokerAddrs(brokerName string) (map[int64]string, error) {
	if brokerAddrs, exists := rm.brokerAddrTable.Load(brokerName); exists {
		return brokerAddrs.(map[int64]string), nil
	}
	return nil, fmt.Errorf("broker %s not found", brokerName)
}

// GetAllBrokers 获取所有Broker信息
func (rm *RouteManager) GetAllBrokers() []*BrokerInfo {
	var brokers []*BrokerInfo
	
	rm.topicRouteTable.Range(func(key, value interface{}) bool {
		routeData := value.(*protocol.TopicRouteData)
		for _, brokerData := range routeData.BrokerDatas {
			for brokerId, brokerAddr := range brokerData.BrokerAddrs {
				brokers = append(brokers, &BrokerInfo{
					Cluster:    brokerData.Cluster,
					BrokerName: brokerData.BrokerName,
					BrokerID:   brokerId,
					BrokerAddr: brokerAddr,
				})
			}
		}
		return true
	})
	
	return brokers
}

// GetClusterBrokers 获取指定集群的Broker列表
func (rm *RouteManager) GetClusterBrokers(cluster string) ([]string, error) {
	if brokerNames, exists := rm.clusterAddrTable.Load(cluster); exists {
		return brokerNames.([]string), nil
	}
	return nil, fmt.Errorf("cluster %s not found", cluster)
}

// GetAllClusters 获取所有集群名称
func (rm *RouteManager) GetAllClusters() []string {
	var clusters []string
	rm.clusterAddrTable.Range(func(key, value interface{}) bool {
		clusters = append(clusters, key.(string))
		return true
	})
	return clusters
}

// IsTopicExist 检查Topic是否存在
func (rm *RouteManager) IsTopicExist(topic string) bool {
	_, exists := rm.topicRouteTable.Load(topic)
	return exists
}

// GetTopicList 获取所有Topic列表
func (rm *RouteManager) GetTopicList() []string {
	var topics []string
	rm.topicRouteTable.Range(func(key, value interface{}) bool {
		topics = append(topics, key.(string))
		return true
	})
	sort.Strings(topics)
	return topics
}

// ClearTopicRoute 清除指定Topic的路由信息
func (rm *RouteManager) ClearTopicRoute(topic string) {
	rm.topicRouteTable.Delete(topic)
}

// ClearAllRoutes 清除所有路由信息
func (rm *RouteManager) ClearAllRoutes() {
	rm.topicRouteTable.Range(func(key, value interface{}) bool {
		rm.topicRouteTable.Delete(key)
		return true
	})
	
	rm.brokerAddrTable.Range(func(key, value interface{}) bool {
		rm.brokerAddrTable.Delete(key)
		return true
	})
	
	rm.clusterAddrTable.Range(func(key, value interface{}) bool {
		rm.clusterAddrTable.Delete(key)
		return true
	})
}

// SetUpdateInterval 设置路由更新间隔
func (rm *RouteManager) SetUpdateInterval(interval time.Duration) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()
	rm.updateInterval = interval
}

// GetUpdateInterval 获取路由更新间隔
func (rm *RouteManager) GetUpdateInterval() time.Duration {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()
	return rm.updateInterval
}

// GetRouteStats 获取路由统计信息
func (rm *RouteManager) GetRouteStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	// 统计Topic数量
	topicCount := 0
	rm.topicRouteTable.Range(func(key, value interface{}) bool {
		topicCount++
		return true
	})
	stats["topic_count"] = topicCount
	
	// 统计Broker数量
	brokerCount := 0
	rm.brokerAddrTable.Range(func(key, value interface{}) bool {
		brokerCount++
		return true
	})
	stats["broker_count"] = brokerCount
	
	// 统计集群数量
	clusterCount := 0
	rm.clusterAddrTable.Range(func(key, value interface{}) bool {
		clusterCount++
		return true
	})
	stats["cluster_count"] = clusterCount
	
	stats["name_server_addrs"] = rm.nameServerAddrs
	stats["update_interval"] = rm.updateInterval.String()
	
	return stats
}

// ValidateRoute 验证路由信息
func (rm *RouteManager) ValidateRoute(topic string) error {
	routeData, err := rm.GetTopicRoute(topic)
	if err != nil {
		return err
	}
	
	if len(routeData.QueueDatas) == 0 {
		return fmt.Errorf("no queue data for topic %s", topic)
	}
	
	if len(routeData.BrokerDatas) == 0 {
		return fmt.Errorf("no broker data for topic %s", topic)
	}
	
	// 验证每个队列都有对应的Broker
	for _, queueData := range routeData.QueueDatas {
		found := false
		for _, brokerData := range routeData.BrokerDatas {
			if brokerData.BrokerName == queueData.BrokerName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("queue broker %s not found in broker data for topic %s", queueData.BrokerName, topic)
		}
	}
	
	return nil
}