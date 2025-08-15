package store

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go-rocketmq/pkg/common"
)

// 延迟级别定义 (18级延迟队列)
var DelayLevels = []time.Duration{
	1 * time.Second,    // 1s
	5 * time.Second,    // 5s
	10 * time.Second,   // 10s
	30 * time.Second,   // 30s
	1 * time.Minute,    // 1m
	2 * time.Minute,    // 2m
	3 * time.Minute,    // 3m
	4 * time.Minute,    // 4m
	5 * time.Minute,    // 5m
	6 * time.Minute,    // 6m
	7 * time.Minute,    // 7m
	8 * time.Minute,    // 8m
	9 * time.Minute,    // 9m
	10 * time.Minute,   // 10m
	20 * time.Minute,   // 20m
	30 * time.Minute,   // 30m
	1 * time.Hour,      // 1h
	2 * time.Hour,      // 2h
}

const (
	// 延迟队列Topic前缀
	SCHEDULE_TOPIC_PREFIX = "SCHEDULE_TOPIC_XXXX"
	// 延迟消息属性键
	PROPERTY_DELAY_TIME_LEVEL = "DELAY"
	PROPERTY_REAL_TOPIC       = "REAL_TOPIC"
	PROPERTY_REAL_QUEUE_ID    = "REAL_QID"
	PROPERTY_TIMER_DELIVER_MS = "TIMER_DELIVER_MS"
)

// DelayQueueService 延迟队列服务
type DelayQueueService struct {
	storeConfig    *StoreConfig
	messageStore   *DefaultMessageStore
	running        bool
	mutex          sync.RWMutex
	shutdown       chan struct{}
	tickers        map[int32]*time.Ticker // 每个延迟级别对应一个定时器
	tickerMutex    sync.RWMutex
	offsetTable    map[int32]int64 // 每个延迟级别的消费进度
	offsetMutex    sync.RWMutex
}

// NewDelayQueueService 创建延迟队列服务
func NewDelayQueueService(storeConfig *StoreConfig, messageStore *DefaultMessageStore) *DelayQueueService {
	return &DelayQueueService{
		storeConfig:  storeConfig,
		messageStore: messageStore,
		shutdown:     make(chan struct{}),
		tickers:      make(map[int32]*time.Ticker),
		offsetTable:  make(map[int32]int64),
	}
}

// Start 启动延迟队列服务
func (dqs *DelayQueueService) Start() error {
	dqs.mutex.Lock()
	defer dqs.mutex.Unlock()

	if dqs.running {
		return fmt.Errorf("delay queue service is already running")
	}

	dqs.running = true
	
	// 启动后台goroutine来初始化
	go func() {
		// 等待一小段时间确保store完全启动
		select {
		case <-time.After(100 * time.Millisecond):
			// 继续初始化
		case <-dqs.shutdown:
			// 服务已关闭，退出
			return
		}
		
		// 检查服务是否仍在运行
		dqs.mutex.RLock()
		running := dqs.running
		dqs.mutex.RUnlock()
		
		if !running {
			return
		}
		
		// 加载延迟消息消费进度
		dqs.loadProgress()

		// 启动所有延迟级别的定时器
		for i := 1; i <= len(DelayLevels); i++ {
			select {
			case <-dqs.shutdown:
				// 服务已关闭，退出
				return
			default:
				go dqs.startDelayLevelTimer(int32(i))
			}
		}
	}()

return nil
}

// Shutdown 关闭延迟队列服务
func (dqs *DelayQueueService) Shutdown() {
	dqs.mutex.Lock()
	defer dqs.mutex.Unlock()

	if !dqs.running {
		return
	}

	// 停止所有定时器
	dqs.tickerMutex.Lock()
	for _, ticker := range dqs.tickers {
		if ticker != nil {
			ticker.Stop()
		}
	}
	dqs.tickerMutex.Unlock()

	// 发送停止信号
	select {
	case <-dqs.shutdown:
		// 已经关闭
	default:
		close(dqs.shutdown)
	}

	// 保存延迟消息进度
	dqs.saveProgress()

	dqs.running = false
	log.Printf("DelayQueueService stopped")
}

// PutDelayMessage 存储延迟消息
func (dqs *DelayQueueService) PutDelayMessage(msg *common.Message, delayLevel int32) (*common.SendResult, error) {
	if delayLevel < 1 || delayLevel > int32(len(DelayLevels)) {
		return nil, fmt.Errorf("invalid delay level: %d", delayLevel)
	}

	// 保存原始Topic和QueueId
	realTopic := msg.Topic
	realQueueId := msg.GetProperty("queueId")
	if realQueueId == "" {
		realQueueId = "0"
	}

	// 计算投递时间
	deliverTime := time.Now().Add(DelayLevels[delayLevel-1])

	// 修改消息属性
	msg.Topic = fmt.Sprintf("%s_%d", SCHEDULE_TOPIC_PREFIX, delayLevel)
	msg.SetProperty(PROPERTY_DELAY_TIME_LEVEL, fmt.Sprintf("%d", delayLevel))
	msg.SetProperty(PROPERTY_REAL_TOPIC, realTopic)
	msg.SetProperty(PROPERTY_REAL_QUEUE_ID, realQueueId)
	msg.SetProperty(PROPERTY_TIMER_DELIVER_MS, fmt.Sprintf("%d", deliverTime.UnixMilli()))

	// 存储到延迟队列
	return dqs.messageStore.PutMessage(msg)
}

// startDelayLevelTimer 启动指定延迟级别的定时器
func (dqs *DelayQueueService) startDelayLevelTimer(delayLevel int32) {
	topic := fmt.Sprintf("%s_%d", SCHEDULE_TOPIC_PREFIX, delayLevel)
	queueId := int32(0)

	// 创建定时器
	ticker := time.NewTicker(1 * time.Second) // 每秒检查一次

	dqs.tickerMutex.Lock()
	dqs.tickers[delayLevel] = ticker
	dqs.tickerMutex.Unlock()

	for {
		select {
		case <-dqs.shutdown:
			ticker.Stop()
			return
		case <-ticker.C:
			dqs.processDelayLevel(topic, queueId, delayLevel)
		}
	}
}

// processDelayLevel 处理指定延迟级别的消息
func (dqs *DelayQueueService) processDelayLevel(topic string, queueId int32, delayLevel int32) {
	// 获取当前消费进度
	dqs.offsetMutex.RLock()
	currentOffset := dqs.offsetTable[delayLevel]
	dqs.offsetMutex.RUnlock()

	// 获取队列最大偏移量
	maxOffset := dqs.messageStore.GetMaxOffsetInQueue(topic, queueId)
	if currentOffset >= maxOffset {
		return
	}

	// 批量拉取消息
	messages, err := dqs.messageStore.GetMessage(topic, queueId, currentOffset, 32)
	if err != nil {
		log.Printf("Failed to get delay messages: %v", err)
		return
	}

	now := time.Now()
	processedCount := int64(0)

	for _, msg := range messages {
		// 检查是否到达投递时间
		deliverTimeStr := msg.GetProperty(PROPERTY_TIMER_DELIVER_MS)
		if deliverTimeStr == "" {
			processedCount++
			continue
		}

		var deliverTime int64
		if _, err := fmt.Sscanf(deliverTimeStr, "%d", &deliverTime); err != nil {
			log.Printf("Invalid deliver time: %s", deliverTimeStr)
			processedCount++
			continue
		}

		if now.UnixMilli() < deliverTime {
			// 还未到投递时间，跳出循环
			break
		}

		// 投递消息到真实Topic
		if err := dqs.deliverMessage(msg); err != nil {
			log.Printf("Failed to deliver delay message: %v", err)
			// 投递失败，暂停处理
			break
		}

		processedCount++
	}

	// 更新消费进度
	if processedCount > 0 {
		dqs.offsetMutex.Lock()
		dqs.offsetTable[delayLevel] = currentOffset + processedCount
		dqs.offsetMutex.Unlock()
	}
}

// deliverMessage 投递延迟消息到真实Topic
func (dqs *DelayQueueService) deliverMessage(msg *common.MessageExt) error {
	// 恢复原始Topic和属性
	realTopic := msg.GetProperty(PROPERTY_REAL_TOPIC)
	realQueueId := msg.GetProperty(PROPERTY_REAL_QUEUE_ID)

	if realTopic == "" {
		return fmt.Errorf("real topic is empty")
	}

	// 创建新消息
	newMsg := &common.Message{
		Topic:      realTopic,
		Tags:       msg.Tags,
		Keys:       msg.Keys,
		Body:       msg.Body,
		Properties: make(map[string]string),
	}

	// 复制属性，但排除延迟相关属性
	for k, v := range msg.Properties {
		if k != PROPERTY_DELAY_TIME_LEVEL && k != PROPERTY_REAL_TOPIC && 
		   k != PROPERTY_REAL_QUEUE_ID && k != PROPERTY_TIMER_DELIVER_MS {
			newMsg.Properties[k] = v
		}
	}

	// 设置队列ID
	if realQueueId != "" {
		newMsg.SetProperty("queueId", realQueueId)
	}

	// 投递到真实Topic
	_, err := dqs.messageStore.PutMessage(newMsg)
	if err != nil {
		return fmt.Errorf("failed to put message to real topic: %v", err)
	}

	log.Printf("Delivered delay message to topic: %s, queueId: %s", realTopic, realQueueId)
	return nil
}

// loadProgress 加载延迟消息消费进度
func (dqs *DelayQueueService) loadProgress() {
	// 简化实现，从每个延迟队列的最小偏移量开始
	dqs.offsetMutex.Lock()
	defer dqs.offsetMutex.Unlock()
	
	// 检查messageStore是否可用
	if dqs.messageStore == nil {
		return
	}
	
	for i := 1; i <= len(DelayLevels); i++ {
		// 使用默认偏移量0，避免在启动阶段调用可能导致死锁的方法
		dqs.offsetTable[int32(i)] = 0
	}
}

// saveProgress 保存延迟消息消费进度
func (dqs *DelayQueueService) saveProgress() {
	// 简化实现，实际应该持久化到文件
	dqs.offsetMutex.RLock()
	progressCopy := make(map[int32]int64)
	for k, v := range dqs.offsetTable {
		progressCopy[k] = v
	}
	dqs.offsetMutex.RUnlock()
	
	log.Printf("Saving delay queue progress: %v", progressCopy)
}

// GetDelayLevel 根据延迟时间获取延迟级别
func GetDelayLevel(delay time.Duration) int32 {
	for i, level := range DelayLevels {
		if delay <= level {
			return int32(i + 1)
		}
	}
	return int32(len(DelayLevels)) // 返回最大延迟级别
}

// IsDelayMessage 判断是否为延迟消息
func IsDelayMessage(msg *common.Message) bool {
	return msg.GetProperty(PROPERTY_DELAY_TIME_LEVEL) != ""
}