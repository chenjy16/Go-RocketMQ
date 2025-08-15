package store

import (
	"fmt"
	"log"
	"sync"

	"go-rocketmq/pkg/common"
)

// 顺序消息相关常量
const (
	// 顺序消息属性键
	PROPERTY_ORDERED_MESSAGE = "ORDERED"
	PROPERTY_SHARDING_KEY    = "SHARDING_KEY"
	PROPERTY_ORDER_ID        = "ORDER_ID"
)

// OrderedQueueService 顺序队列服务
type OrderedQueueService struct {
	storeConfig   *StoreConfig
	messageStore  *DefaultMessageStore
	running       bool
	mutex         sync.RWMutex
	shutdown      chan struct{}
	// 队列锁管理，确保同一队列的消息顺序处理
	queueLocks    map[string]*sync.Mutex // topic:queueId -> lock
	lockMutex     sync.RWMutex
	// 顺序消费进度管理
	consumeProgress map[string]int64 // topic:queueId:consumerGroup -> offset
	progressMutex   sync.RWMutex
}

// NewOrderedQueueService 创建顺序队列服务
func NewOrderedQueueService(storeConfig *StoreConfig, messageStore *DefaultMessageStore) *OrderedQueueService {
	return &OrderedQueueService{
		storeConfig:     storeConfig,
		messageStore:    messageStore,
		shutdown:        make(chan struct{}),
		queueLocks:      make(map[string]*sync.Mutex),
		consumeProgress: make(map[string]int64),
	}
}

// Start 启动顺序队列服务
func (oqs *OrderedQueueService) Start() error {
	oqs.mutex.Lock()
	defer oqs.mutex.Unlock()

	if oqs.running {
		return fmt.Errorf("ordered queue service is already running")
	}

	// 加载消费进度
	oqs.loadConsumeProgress()

	oqs.running = true
	log.Printf("OrderedQueueService started")
	return nil
}

// Shutdown 关闭顺序队列服务
func (oqs *OrderedQueueService) Shutdown() {
	oqs.mutex.Lock()
	defer oqs.mutex.Unlock()

	if !oqs.running {
		return
	}

	// 发送停止信号
	close(oqs.shutdown)

	// 保存消费进度
	oqs.saveConsumeProgress()

	oqs.running = false
	log.Printf("OrderedQueueService stopped")
}

// PutOrderedMessage 存储顺序消息
func (oqs *OrderedQueueService) PutOrderedMessage(msg *common.Message, shardingKey string) (*common.SendResult, error) {
	// 根据分片键计算队列ID，确保相同分片键的消息进入同一队列
	queueId := oqs.selectQueueByShardingKey(msg.Topic, shardingKey)

	// 设置顺序消息属性
	msg.SetProperty(PROPERTY_ORDERED_MESSAGE, "true")
	msg.SetProperty(PROPERTY_SHARDING_KEY, shardingKey)
	msg.SetProperty("queueId", fmt.Sprintf("%d", queueId))

	// 获取队列锁，确保同一队列的消息顺序写入
	queueKey := fmt.Sprintf("%s:%d", msg.Topic, queueId)
	queueLock := oqs.getQueueLock(queueKey)
	queueLock.Lock()
	defer queueLock.Unlock()

	// 存储消息到指定队列
	result, err := oqs.messageStore.PutMessageToQueue(msg, queueId)
	if err != nil {
		return nil, fmt.Errorf("failed to put ordered message: %v", err)
	}

	log.Printf("Put ordered message to queue %d with sharding key: %s", queueId, shardingKey)
	return result, nil
}

// PullOrderedMessage 拉取顺序消息
func (oqs *OrderedQueueService) PullOrderedMessage(topic string, queueId int32, consumerGroup string, maxNums int32) ([]*common.MessageExt, error) {
	// 获取队列锁，确保同一队列的消息顺序消费
	queueKey := fmt.Sprintf("%s:%d", topic, queueId)
	queueLock := oqs.getQueueLock(queueKey)
	queueLock.Lock()
	defer queueLock.Unlock()

	// 获取消费进度
	progressKey := fmt.Sprintf("%s:%d:%s", topic, queueId, consumerGroup)
	oqs.progressMutex.RLock()
	offset := oqs.consumeProgress[progressKey]
	oqs.progressMutex.RUnlock()

	// 拉取消息
	messages, err := oqs.messageStore.GetMessage(topic, queueId, offset, maxNums)
	if err != nil {
		return nil, fmt.Errorf("failed to pull ordered messages: %v", err)
	}

	// 过滤出顺序消息
	var orderedMessages []*common.MessageExt
	for _, msg := range messages {
		if IsOrderedMessage(msg) {
			orderedMessages = append(orderedMessages, msg)
		}
	}

	log.Printf("Pulled %d ordered messages from queue %s:%d for consumer group: %s", 
		len(orderedMessages), topic, queueId, consumerGroup)
	return orderedMessages, nil
}

// CommitConsumeOffset 提交消费进度
func (oqs *OrderedQueueService) CommitConsumeOffset(topic string, queueId int32, consumerGroup string, offset int64) error {
	progressKey := fmt.Sprintf("%s:%d:%s", topic, queueId, consumerGroup)
	
	oqs.progressMutex.Lock()
	oqs.consumeProgress[progressKey] = offset
	oqs.progressMutex.Unlock()

	log.Printf("Committed consume offset %d for %s", offset, progressKey)
	return nil
}

// GetConsumeOffset 获取消费进度
func (oqs *OrderedQueueService) GetConsumeOffset(topic string, queueId int32, consumerGroup string) int64 {
	progressKey := fmt.Sprintf("%s:%d:%s", topic, queueId, consumerGroup)
	
	oqs.progressMutex.RLock()
	offset, exists := oqs.consumeProgress[progressKey]
	oqs.progressMutex.RUnlock()

	if !exists {
		return -1 // 返回-1表示没有消费进度
	}
	return offset
}

// selectQueueByShardingKey 根据分片键选择队列
func (oqs *OrderedQueueService) selectQueueByShardingKey(topic string, shardingKey string) int32 {
	// 获取Topic的队列数量
	queueNums := oqs.getTopicQueueNums(topic)
	if queueNums <= 0 {
		queueNums = 4 // 默认4个队列
	}

	// 使用简单的哈希算法选择队列
	hash := oqs.hash(shardingKey)
	return int32(hash % uint32(queueNums))
}

// getTopicQueueNums 获取Topic的队列数量
func (oqs *OrderedQueueService) getTopicQueueNums(topic string) int32 {
	// 简化实现，实际应该从Topic配置中获取
	return 4 // 默认4个队列
}

// hash 简单哈希函数
func (oqs *OrderedQueueService) hash(key string) uint32 {
	var hash uint32 = 0
	for _, c := range key {
		hash = hash*31 + uint32(c)
	}
	return hash
}

// getQueueLock 获取队列锁
func (oqs *OrderedQueueService) getQueueLock(queueKey string) *sync.Mutex {
	oqs.lockMutex.RLock()
	lock, exists := oqs.queueLocks[queueKey]
	oqs.lockMutex.RUnlock()

	if exists {
		return lock
	}

	// 双重检查锁定模式
	oqs.lockMutex.Lock()
	defer oqs.lockMutex.Unlock()

	lock, exists = oqs.queueLocks[queueKey]
	if !exists {
		lock = &sync.Mutex{}
		oqs.queueLocks[queueKey] = lock
	}

	return lock
}

// loadConsumeProgress 加载消费进度
func (oqs *OrderedQueueService) loadConsumeProgress() {
	// 简化实现，实际应该从持久化存储中加载
	log.Printf("Loading ordered queue consume progress")
}

// saveConsumeProgress 保存消费进度
func (oqs *OrderedQueueService) saveConsumeProgress() {
	// 简化实现，实际应该持久化到存储
	log.Printf("Saving ordered queue consume progress: %v", oqs.consumeProgress)
}

// OrderedMessageSelector 顺序消息选择器接口
type OrderedMessageSelector interface {
	// Select 根据消息和队列信息选择队列
	Select(msg *common.Message, queues []*common.MessageQueue, arg interface{}) *common.MessageQueue
}

// HashOrderedMessageSelector 基于哈希的顺序消息选择器
type HashOrderedMessageSelector struct{}

// Select 实现OrderedMessageSelector接口
func (homs *HashOrderedMessageSelector) Select(msg *common.Message, queues []*common.MessageQueue, arg interface{}) *common.MessageQueue {
	if len(queues) == 0 {
		return nil
	}

	// 使用分片键进行哈希选择
	shardingKey := ""
	if arg != nil {
		if key, ok := arg.(string); ok {
			shardingKey = key
		}
	}

	if shardingKey == "" {
		// 如果没有分片键，使用消息的Keys
		shardingKey = msg.Keys
	}

	if shardingKey == "" {
		// 如果还是没有，返回第一个队列
		return queues[0]
	}

	// 计算哈希值选择队列
	hash := hashString(shardingKey)
	index := hash % uint32(len(queues))
	return queues[index]
}

// hashString 字符串哈希函数
func hashString(s string) uint32 {
	var hash uint32 = 0
	for _, c := range s {
		hash = hash*31 + uint32(c)
	}
	return hash
}

// OrderedConsumer 顺序消费者接口
type OrderedConsumer interface {
	// ConsumeOrderly 顺序消费消息
	ConsumeOrderly(messages []*common.MessageExt, context *ConsumeOrderlyContext) ConsumeOrderlyStatus
}

// ConsumeOrderlyContext 顺序消费上下文
type ConsumeOrderlyContext struct {
	MessageQueue  *common.MessageQueue
	AutoCommit    bool
	SuspendTimeMillis int64
}

// ConsumeOrderlyStatus 顺序消费状态
type ConsumeOrderlyStatus int32

const (
	ConsumeOrderlyStatusSuccess ConsumeOrderlyStatus = iota
	ConsumeOrderlyStatusSuspendCurrentQueueAMoment
	ConsumeOrderlyStatusRollback
)

func (cos ConsumeOrderlyStatus) String() string {
	switch cos {
	case ConsumeOrderlyStatusSuccess:
		return "SUCCESS"
	case ConsumeOrderlyStatusSuspendCurrentQueueAMoment:
		return "SUSPEND_CURRENT_QUEUE_A_MOMENT"
	case ConsumeOrderlyStatusRollback:
		return "ROLLBACK"
	default:
		return "UNKNOWN"
	}
}

// OrderedMessageLoadBalancer 顺序消息负载均衡器
type OrderedMessageLoadBalancer struct {
	consumerGroup string
	consumers     []string // 消费者实例列表
	mutex         sync.RWMutex
}

// NewOrderedMessageLoadBalancer 创建顺序消息负载均衡器
func NewOrderedMessageLoadBalancer(consumerGroup string) *OrderedMessageLoadBalancer {
	return &OrderedMessageLoadBalancer{
		consumerGroup: consumerGroup,
		consumers:     make([]string, 0),
	}
}

// AddConsumer 添加消费者
func (omlb *OrderedMessageLoadBalancer) AddConsumer(consumerId string) {
	omlb.mutex.Lock()
	defer omlb.mutex.Unlock()

	// 检查是否已存在
	for _, id := range omlb.consumers {
		if id == consumerId {
			return
		}
	}

	omlb.consumers = append(omlb.consumers, consumerId)
	log.Printf("Added consumer %s to group %s", consumerId, omlb.consumerGroup)
}

// RemoveConsumer 移除消费者
func (omlb *OrderedMessageLoadBalancer) RemoveConsumer(consumerId string) {
	omlb.mutex.Lock()
	defer omlb.mutex.Unlock()

	for i, id := range omlb.consumers {
		if id == consumerId {
			omlb.consumers = append(omlb.consumers[:i], omlb.consumers[i+1:]...)
			log.Printf("Removed consumer %s from group %s", consumerId, omlb.consumerGroup)
			return
		}
	}
}

// AllocateQueues 为消费者分配队列
func (omlb *OrderedMessageLoadBalancer) AllocateQueues(consumerId string, queues []*common.MessageQueue) []*common.MessageQueue {
	omlb.mutex.RLock()
	defer omlb.mutex.RUnlock()

	if len(omlb.consumers) == 0 || len(queues) == 0 {
		return nil
	}

	// 找到消费者在列表中的索引
	consumerIndex := -1
	for i, id := range omlb.consumers {
		if id == consumerId {
			consumerIndex = i
			break
		}
	}

	if consumerIndex == -1 {
		return nil
	}

	// 平均分配队列
	var allocatedQueues []*common.MessageQueue
	for i := consumerIndex; i < len(queues); i += len(omlb.consumers) {
		allocatedQueues = append(allocatedQueues, queues[i])
	}

	log.Printf("Allocated %d queues to consumer %s", len(allocatedQueues), consumerId)
	return allocatedQueues
}

// IsOrderedMessage 判断是否为顺序消息
func IsOrderedMessage(msg interface{}) bool {
	switch m := msg.(type) {
	case *common.Message:
		return m.GetProperty(PROPERTY_ORDERED_MESSAGE) == "true"
	case *common.MessageExt:
		return m.GetProperty(PROPERTY_ORDERED_MESSAGE) == "true"
	default:
		return false
	}
}

// GetShardingKey 获取分片键
func GetShardingKey(msg interface{}) string {
	switch m := msg.(type) {
	case *common.Message:
		return m.GetProperty(PROPERTY_SHARDING_KEY)
	case *common.MessageExt:
		return m.GetProperty(PROPERTY_SHARDING_KEY)
	default:
		return ""
	}
}