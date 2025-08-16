package remoting

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go-rocketmq/pkg/protocol"
)

// HeartbeatData 心跳数据
type HeartbeatData struct {
	ClientID                string                    `json:"clientID"`
	ProducerDataSet         []*ProducerData          `json:"producerDataSet"`
	ConsumerDataSet         []*ConsumerData          `json:"consumerDataSet"`
}

// ProducerData 生产者数据
type ProducerData struct {
	GroupName string `json:"groupName"`
}

// ConsumerData 消费者数据
type ConsumerData struct {
	GroupName           string                    `json:"groupName"`
	ConsumeType         string                    `json:"consumeType"`
	MessageModel        string                    `json:"messageModel"`
	ConsumeFromWhere    string                    `json:"consumeFromWhere"`
	SubscriptionDataSet []*protocol.SubscriptionData `json:"subscriptionDataSet"`
	UnitMode            bool                      `json:"unitMode"`
}

// HeartbeatManager 心跳管理器
type HeartbeatManager struct {
	client       *RemotingClient
	brokerAddrs  []string
	heartbeatData *HeartbeatData
	mutex        sync.RWMutex
	interval     time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// NewHeartbeatManager 创建心跳管理器
func NewHeartbeatManager(client *RemotingClient, clientID string) *HeartbeatManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &HeartbeatManager{
		client: client,
		heartbeatData: &HeartbeatData{
			ClientID:        clientID,
			ProducerDataSet: make([]*ProducerData, 0),
			ConsumerDataSet: make([]*ConsumerData, 0),
		},
		interval: 30 * time.Second, // 默认30秒心跳间隔
		ctx:      ctx,
		cancel:   cancel,
	}
}

// SetBrokerAddrs 设置Broker地址列表
func (hm *HeartbeatManager) SetBrokerAddrs(addrs []string) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	hm.brokerAddrs = make([]string, len(addrs))
	copy(hm.brokerAddrs, addrs)
}

// AddProducer 添加生产者
func (hm *HeartbeatManager) AddProducer(groupName string) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	
	// 检查是否已存在
	for _, producer := range hm.heartbeatData.ProducerDataSet {
		if producer.GroupName == groupName {
			return
		}
	}
	
	hm.heartbeatData.ProducerDataSet = append(hm.heartbeatData.ProducerDataSet, &ProducerData{
		GroupName: groupName,
	})
}

// RemoveProducer 移除生产者
func (hm *HeartbeatManager) RemoveProducer(groupName string) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	
	for i, producer := range hm.heartbeatData.ProducerDataSet {
		if producer.GroupName == groupName {
			hm.heartbeatData.ProducerDataSet = append(
				hm.heartbeatData.ProducerDataSet[:i],
				hm.heartbeatData.ProducerDataSet[i+1:]...,
			)
			break
		}
	}
}

// AddConsumer 添加消费者
func (hm *HeartbeatManager) AddConsumer(groupName, consumeType, messageModel, consumeFromWhere string, subscriptions []*protocol.SubscriptionData, unitMode bool) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	
	// 检查是否已存在，如果存在则更新
	for _, consumer := range hm.heartbeatData.ConsumerDataSet {
		if consumer.GroupName == groupName {
			consumer.ConsumeType = consumeType
			consumer.MessageModel = messageModel
			consumer.ConsumeFromWhere = consumeFromWhere
			consumer.SubscriptionDataSet = subscriptions
			consumer.UnitMode = unitMode
			return
		}
	}
	
	// 添加新消费者
	hm.heartbeatData.ConsumerDataSet = append(hm.heartbeatData.ConsumerDataSet, &ConsumerData{
		GroupName:           groupName,
		ConsumeType:         consumeType,
		MessageModel:        messageModel,
		ConsumeFromWhere:    consumeFromWhere,
		SubscriptionDataSet: subscriptions,
		UnitMode:            unitMode,
	})
}

// RemoveConsumer 移除消费者
func (hm *HeartbeatManager) RemoveConsumer(groupName string) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	
	for i, consumer := range hm.heartbeatData.ConsumerDataSet {
		if consumer.GroupName == groupName {
			hm.heartbeatData.ConsumerDataSet = append(
				hm.heartbeatData.ConsumerDataSet[:i],
				hm.heartbeatData.ConsumerDataSet[i+1:]...,
			)
			break
		}
	}
}

// Start 启动心跳
func (hm *HeartbeatManager) Start() {
	hm.wg.Add(1)
	go hm.heartbeatRoutine()
}

// Stop 停止心跳
func (hm *HeartbeatManager) Stop() {
	hm.cancel()
	hm.wg.Wait()
}

// heartbeatRoutine 心跳例程
func (hm *HeartbeatManager) heartbeatRoutine() {
	defer hm.wg.Done()
	
	ticker := time.NewTicker(hm.interval)
	defer ticker.Stop()
	
	// 立即发送一次心跳
	hm.sendHeartbeatToAllBrokers()
	
	for {
		select {
		case <-ticker.C:
			hm.sendHeartbeatToAllBrokers()
		case <-hm.ctx.Done():
			return
		}
	}
}

// sendHeartbeatToAllBrokers 向所有Broker发送心跳
func (hm *HeartbeatManager) sendHeartbeatToAllBrokers() {
	hm.mutex.RLock()
	brokerAddrs := make([]string, len(hm.brokerAddrs))
	copy(brokerAddrs, hm.brokerAddrs)
	heartbeatData := hm.cloneHeartbeatData()
	hm.mutex.RUnlock()
	
	for _, brokerAddr := range brokerAddrs {
		go hm.sendHeartbeatToBroker(brokerAddr, heartbeatData)
	}
}

// sendHeartbeatToBroker 向指定Broker发送心跳
func (hm *HeartbeatManager) sendHeartbeatToBroker(brokerAddr string, heartbeatData *HeartbeatData) {
	// 序列化心跳数据
	body, err := json.Marshal(heartbeatData)
	if err != nil {
		return
	}
	
	// 创建心跳请求
	request := protocol.CreateRemotingCommand(protocol.RequestCode(34)) // HEART_BEAT
	request.Body = body
	
	// 发送心跳（单向发送，不等待响应）
	hm.client.SendOneway(brokerAddr, request)
}

// cloneHeartbeatData 克隆心跳数据
func (hm *HeartbeatManager) cloneHeartbeatData() *HeartbeatData {
	data := &HeartbeatData{
		ClientID:        hm.heartbeatData.ClientID,
		ProducerDataSet: make([]*ProducerData, len(hm.heartbeatData.ProducerDataSet)),
		ConsumerDataSet: make([]*ConsumerData, len(hm.heartbeatData.ConsumerDataSet)),
	}
	
	// 复制生产者数据
	for i, producer := range hm.heartbeatData.ProducerDataSet {
		data.ProducerDataSet[i] = &ProducerData{
			GroupName: producer.GroupName,
		}
	}
	
	// 复制消费者数据
	for i, consumer := range hm.heartbeatData.ConsumerDataSet {
		data.ConsumerDataSet[i] = &ConsumerData{
			GroupName:           consumer.GroupName,
			ConsumeType:         consumer.ConsumeType,
			MessageModel:        consumer.MessageModel,
			ConsumeFromWhere:    consumer.ConsumeFromWhere,
			SubscriptionDataSet: make([]*protocol.SubscriptionData, len(consumer.SubscriptionDataSet)),
			UnitMode:            consumer.UnitMode,
		}
		
		// 复制订阅数据
		for j, sub := range consumer.SubscriptionDataSet {
			data.ConsumerDataSet[i].SubscriptionDataSet[j] = &protocol.SubscriptionData{
				Topic:          sub.Topic,
				SubString:      sub.SubString,
				TagsSet:        append([]string(nil), sub.TagsSet...),
				CodeSet:        append([]int32(nil), sub.CodeSet...),
				SubVersion:     sub.SubVersion,
				ExpressionType: sub.ExpressionType,
			}
		}
	}
	
	return data
}

// SetHeartbeatInterval 设置心跳间隔
func (hm *HeartbeatManager) SetHeartbeatInterval(interval time.Duration) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	hm.interval = interval
}

// GetHeartbeatData 获取心跳数据
func (hm *HeartbeatManager) GetHeartbeatData() *HeartbeatData {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()
	return hm.cloneHeartbeatData()
}

// SendHeartbeatNow 立即发送心跳
func (hm *HeartbeatManager) SendHeartbeatNow() {
	hm.sendHeartbeatToAllBrokers()
}

// HeartbeatProcessor 心跳处理器（用于服务端）
type HeartbeatProcessor struct {
	clientTable sync.Map // map[string]*ClientChannelInfo
}

// ClientChannelInfo 客户端通道信息
type ClientChannelInfo struct {
	ClientID    string
	RemoteAddr  string
	LastUpdate  time.Time
	HeartbeatData *HeartbeatData
}

// NewHeartbeatProcessor 创建心跳处理器
func NewHeartbeatProcessor() *HeartbeatProcessor {
	return &HeartbeatProcessor{}
}

// ProcessRequest 处理心跳请求
func (hp *HeartbeatProcessor) ProcessRequest(ctx context.Context, request *protocol.RemotingCommand, conn *ServerConnection) (*protocol.RemotingCommand, error) {
	// 解析心跳数据
	var heartbeatData HeartbeatData
	if err := json.Unmarshal(request.Body, &heartbeatData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal heartbeat data: %v", err)
	}
	
	// 更新客户端信息
	clientInfo := &ClientChannelInfo{
		ClientID:      heartbeatData.ClientID,
		RemoteAddr:    conn.remoteAddr,
		LastUpdate:    time.Now(),
		HeartbeatData: &heartbeatData,
	}
	
	hp.clientTable.Store(heartbeatData.ClientID, clientInfo)
	
	// 返回成功响应
	response := protocol.CreateResponseCommand(protocol.Success, "")
	return response, nil
}

// GetClientInfo 获取客户端信息
func (hp *HeartbeatProcessor) GetClientInfo(clientID string) (*ClientChannelInfo, bool) {
	value, exists := hp.clientTable.Load(clientID)
	if !exists {
		return nil, false
	}
	return value.(*ClientChannelInfo), true
}

// GetAllClients 获取所有客户端信息
func (hp *HeartbeatProcessor) GetAllClients() []*ClientChannelInfo {
	var clients []*ClientChannelInfo
	hp.clientTable.Range(func(key, value interface{}) bool {
		clients = append(clients, value.(*ClientChannelInfo))
		return true
	})
	return clients
}

// CleanupExpiredClients 清理过期客户端
func (hp *HeartbeatProcessor) CleanupExpiredClients(expireTime time.Duration) {
	now := time.Now()
	hp.clientTable.Range(func(key, value interface{}) bool {
		clientInfo := value.(*ClientChannelInfo)
		if now.Sub(clientInfo.LastUpdate) > expireTime {
			hp.clientTable.Delete(key)
		}
		return true
	})
}

// GetProducerGroups 获取所有生产者组
func (hp *HeartbeatProcessor) GetProducerGroups() []string {
	groupSet := make(map[string]bool)
	hp.clientTable.Range(func(key, value interface{}) bool {
		clientInfo := value.(*ClientChannelInfo)
		for _, producer := range clientInfo.HeartbeatData.ProducerDataSet {
			groupSet[producer.GroupName] = true
		}
		return true
	})
	
	var groups []string
	for group := range groupSet {
		groups = append(groups, group)
	}
	return groups
}

// GetConsumerGroups 获取所有消费者组
func (hp *HeartbeatProcessor) GetConsumerGroups() []string {
	groupSet := make(map[string]bool)
	hp.clientTable.Range(func(key, value interface{}) bool {
		clientInfo := value.(*ClientChannelInfo)
		for _, consumer := range clientInfo.HeartbeatData.ConsumerDataSet {
			groupSet[consumer.GroupName] = true
		}
		return true
	})
	
	var groups []string
	for group := range groupSet {
		groups = append(groups, group)
	}
	return groups
}