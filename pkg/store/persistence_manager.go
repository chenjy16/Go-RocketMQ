package store

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go-rocketmq/pkg/common"
)

// PersistenceManager 持久化管理器
type PersistenceManager struct {
	storeConfig *StoreConfig
	mutex       sync.RWMutex
	
	// 消费进度存储
	consumeProgressFile string
	consumeProgress     map[string]int64 // key: topic:queueId:consumerGroup, value: offset
	progressMutex       sync.RWMutex
	
	// 延迟队列进度存储
	delayProgressFile string
	delayProgress     map[int32]int64 // key: delayLevel, value: offset
	delayMutex        sync.RWMutex
	
	// 事务状态存储
	transactionStateFile string
	transactionStates    map[string]*PersistentTransactionState // key: transactionId
	transactionMutex     sync.RWMutex
	
	// 消息索引存储
	messageIndexFile string
	messageIndex     map[string]*MessageIndex // key: messageKey
	indexMutex       sync.RWMutex
	
	// 自动保存定时器
	autoSaveTicker *time.Ticker
	stopChan       chan struct{}
}

// PersistentTransactionState 持久化事务状态
type PersistentTransactionState struct {
	TransactionId string           `json:"transactionId"`
	ProducerGroup string           `json:"producerGroup"`
	State         TransactionState `json:"state"`
	CreateTime    int64            `json:"createTime"`
	UpdateTime    int64            `json:"updateTime"`
	Message       *common.Message  `json:"message"`
}

// MessageIndex 消息索引
type MessageIndex struct {
	MessageKey string `json:"messageKey"`
	Topic      string `json:"topic"`
	QueueId    int32  `json:"queueId"`
	Offset     int64  `json:"offset"`
	StoreTime  int64  `json:"storeTime"`
	Tags       string `json:"tags"`
}

// ConsumeProgressData 消费进度数据结构
type ConsumeProgressData struct {
	Progress   map[string]int64 `json:"progress"`
	UpdateTime int64            `json:"updateTime"`
}

// DelayProgressData 延迟队列进度数据结构
type DelayProgressData struct {
	Progress   map[int32]int64 `json:"progress"`
	UpdateTime int64           `json:"updateTime"`
}

// TransactionStateData 事务状态数据结构
type TransactionStateData struct {
	States     map[string]*PersistentTransactionState `json:"states"`
	UpdateTime int64                                   `json:"updateTime"`
}

// MessageIndexData 消息索引数据结构
type MessageIndexData struct {
	Index      map[string]*MessageIndex `json:"index"`
	UpdateTime int64                     `json:"updateTime"`
}

// NewPersistenceManager 创建持久化管理器
func NewPersistenceManager(config *StoreConfig) *PersistenceManager {
	if config == nil {
		config = NewDefaultStoreConfig()
	}
	
	persistenceDir := filepath.Join(config.StorePathRootDir, "persistence")
	os.MkdirAll(persistenceDir, 0755)
	
	pm := &PersistenceManager{
		storeConfig:          config,
		consumeProgressFile:  filepath.Join(persistenceDir, "consume_progress.json"),
		consumeProgress:      make(map[string]int64),
		delayProgressFile:    filepath.Join(persistenceDir, "delay_progress.json"),
		delayProgress:        make(map[int32]int64),
		transactionStateFile: filepath.Join(persistenceDir, "transaction_state.json"),
		transactionStates:    make(map[string]*PersistentTransactionState),
		messageIndexFile:     filepath.Join(persistenceDir, "message_index.json"),
		messageIndex:         make(map[string]*MessageIndex),
		stopChan:             make(chan struct{}),
	}
	
	return pm
}

// Start 启动持久化管理器
func (pm *PersistenceManager) Start() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	
	// 加载所有持久化数据
	if err := pm.loadConsumeProgress(); err != nil {
		return fmt.Errorf("failed to load consume progress: %v", err)
	}
	
	if err := pm.loadDelayProgress(); err != nil {
		return fmt.Errorf("failed to load delay progress: %v", err)
	}
	
	if err := pm.loadTransactionStates(); err != nil {
		return fmt.Errorf("failed to load transaction states: %v", err)
	}
	
	if err := pm.loadMessageIndex(); err != nil {
		return fmt.Errorf("failed to load message index: %v", err)
	}
	
	// 启动自动保存定时器（每30秒保存一次）
	pm.autoSaveTicker = time.NewTicker(30 * time.Second)
	go pm.autoSaveLoop()
	
	return nil
}

// Stop 停止持久化管理器
func (pm *PersistenceManager) Stop() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	
	// 停止自动保存
	if pm.autoSaveTicker != nil {
		pm.autoSaveTicker.Stop()
	}
	close(pm.stopChan)
	
	// 最后一次保存所有数据
	return pm.saveAll()
}

// autoSaveLoop 自动保存循环
func (pm *PersistenceManager) autoSaveLoop() {
	for {
		select {
		case <-pm.autoSaveTicker.C:
			if err := pm.saveAll(); err != nil {
				fmt.Printf("Auto save failed: %v\n", err)
			}
		case <-pm.stopChan:
			return
		}
	}
}

// saveAll 保存所有数据
func (pm *PersistenceManager) saveAll() error {
	if err := pm.saveConsumeProgress(); err != nil {
		return err
	}
	if err := pm.saveDelayProgress(); err != nil {
		return err
	}
	if err := pm.saveTransactionStates(); err != nil {
		return err
	}
	if err := pm.saveMessageIndex(); err != nil {
		return err
	}
	return nil
}

// === 消费进度相关方法 ===

// UpdateConsumeProgress 更新消费进度
func (pm *PersistenceManager) UpdateConsumeProgress(topic string, queueId int32, consumerGroup string, offset int64) {
	key := fmt.Sprintf("%s:%d:%s", topic, queueId, consumerGroup)
	pm.progressMutex.Lock()
	pm.consumeProgress[key] = offset
	pm.progressMutex.Unlock()
}

// GetConsumeProgress 获取消费进度
func (pm *PersistenceManager) GetConsumeProgress(topic string, queueId int32, consumerGroup string) int64 {
	key := fmt.Sprintf("%s:%d:%s", topic, queueId, consumerGroup)
	pm.progressMutex.RLock()
	offset, exists := pm.consumeProgress[key]
	pm.progressMutex.RUnlock()
	
	if !exists {
		return -1 // 表示没有消费进度
	}
	return offset
}

// loadConsumeProgress 加载消费进度
func (pm *PersistenceManager) loadConsumeProgress() error {
	data, err := ioutil.ReadFile(pm.consumeProgressFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 文件不存在，使用默认值
		}
		return err
	}
	
	var progressData ConsumeProgressData
	if err := json.Unmarshal(data, &progressData); err != nil {
		return err
	}
	
	pm.progressMutex.Lock()
	pm.consumeProgress = progressData.Progress
	pm.progressMutex.Unlock()
	
	return nil
}

// saveConsumeProgress 保存消费进度
func (pm *PersistenceManager) saveConsumeProgress() error {
	pm.progressMutex.RLock()
	progressCopy := make(map[string]int64)
	for k, v := range pm.consumeProgress {
		progressCopy[k] = v
	}
	pm.progressMutex.RUnlock()
	
	progressData := ConsumeProgressData{
		Progress:   progressCopy,
		UpdateTime: time.Now().Unix(),
	}
	
	data, err := json.MarshalIndent(progressData, "", "  ")
	if err != nil {
		return err
	}
	
	return ioutil.WriteFile(pm.consumeProgressFile, data, 0644)
}

// === 延迟队列进度相关方法 ===

// UpdateDelayProgress 更新延迟队列进度
func (pm *PersistenceManager) UpdateDelayProgress(delayLevel int32, offset int64) {
	pm.delayMutex.Lock()
	pm.delayProgress[delayLevel] = offset
	pm.delayMutex.Unlock()
}

// GetDelayProgress 获取延迟队列进度
func (pm *PersistenceManager) GetDelayProgress(delayLevel int32) int64 {
	pm.delayMutex.RLock()
	offset, exists := pm.delayProgress[delayLevel]
	pm.delayMutex.RUnlock()
	
	if !exists {
		return 0 // 默认从0开始
	}
	return offset
}

// loadDelayProgress 加载延迟队列进度
func (pm *PersistenceManager) loadDelayProgress() error {
	data, err := ioutil.ReadFile(pm.delayProgressFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	
	var delayData DelayProgressData
	if err := json.Unmarshal(data, &delayData); err != nil {
		return err
	}
	
	pm.delayMutex.Lock()
	pm.delayProgress = delayData.Progress
	pm.delayMutex.Unlock()
	
	return nil
}

// saveDelayProgress 保存延迟队列进度
func (pm *PersistenceManager) saveDelayProgress() error {
	pm.delayMutex.RLock()
	progressCopy := make(map[int32]int64)
	for k, v := range pm.delayProgress {
		progressCopy[k] = v
	}
	pm.delayMutex.RUnlock()
	
	delayData := DelayProgressData{
		Progress:   progressCopy,
		UpdateTime: time.Now().Unix(),
	}
	
	data, err := json.MarshalIndent(delayData, "", "  ")
	if err != nil {
		return err
	}
	
	return ioutil.WriteFile(pm.delayProgressFile, data, 0644)
}

// === 事务状态相关方法 ===

// UpdateTransactionState 更新事务状态
func (pm *PersistenceManager) UpdateTransactionState(transactionId string, state *PersistentTransactionState) {
	pm.transactionMutex.Lock()
	state.UpdateTime = time.Now().Unix()
	pm.transactionStates[transactionId] = state
	pm.transactionMutex.Unlock()
}

// GetTransactionState 获取事务状态
func (pm *PersistenceManager) GetTransactionState(transactionId string) *PersistentTransactionState {
	pm.transactionMutex.RLock()
	state, exists := pm.transactionStates[transactionId]
	pm.transactionMutex.RUnlock()
	
	if !exists {
		return nil
	}
	return state
}

// RemoveTransactionState 移除事务状态
func (pm *PersistenceManager) RemoveTransactionState(transactionId string) {
	pm.transactionMutex.Lock()
	delete(pm.transactionStates, transactionId)
	pm.transactionMutex.Unlock()
}

// loadTransactionStates 加载事务状态
func (pm *PersistenceManager) loadTransactionStates() error {
	data, err := ioutil.ReadFile(pm.transactionStateFile)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Transaction state file does not exist: %s\n", pm.transactionStateFile)
			return nil
		}
		return err
	}
	
	var stateData TransactionStateData
	if err := json.Unmarshal(data, &stateData); err != nil {
		return err
	}
	
	pm.transactionMutex.Lock()
	pm.transactionStates = stateData.States
	pm.transactionMutex.Unlock()
	
	fmt.Printf("Loaded %d transaction states from file: %s\n", len(stateData.States), pm.transactionStateFile)
	return nil
}

// saveTransactionStates 保存事务状态
func (pm *PersistenceManager) saveTransactionStates() error {
	pm.transactionMutex.RLock()
	statesCopy := make(map[string]*PersistentTransactionState)
	for k, v := range pm.transactionStates {
		statesCopy[k] = v
	}
	pm.transactionMutex.RUnlock()
	
	stateData := TransactionStateData{
		States:     statesCopy,
		UpdateTime: time.Now().Unix(),
	}
	
	data, err := json.MarshalIndent(stateData, "", "  ")
	if err != nil {
		return err
	}
	
	fmt.Printf("Saving %d transaction states to file: %s\n", len(statesCopy), pm.transactionStateFile)
	err = ioutil.WriteFile(pm.transactionStateFile, data, 0644)
	if err != nil {
		fmt.Printf("Failed to write transaction state file: %v\n", err)
		return err
	}
	fmt.Printf("Successfully saved transaction states to file\n")
	return nil
}

// === 消息索引相关方法 ===

// AddMessageIndex 添加消息索引
func (pm *PersistenceManager) AddMessageIndex(messageKey string, index *MessageIndex) {
	pm.indexMutex.Lock()
	pm.messageIndex[messageKey] = index
	pm.indexMutex.Unlock()
}

// GetMessageIndex 获取消息索引
func (pm *PersistenceManager) GetMessageIndex(messageKey string) *MessageIndex {
	pm.indexMutex.RLock()
	index, exists := pm.messageIndex[messageKey]
	pm.indexMutex.RUnlock()
	
	if !exists {
		return nil
	}
	return index
}

// QueryMessagesByKey 根据Key查询消息
func (pm *PersistenceManager) QueryMessagesByKey(messageKey string) []*MessageIndex {
	pm.indexMutex.RLock()
	defer pm.indexMutex.RUnlock()
	
	var results []*MessageIndex
	for key, index := range pm.messageIndex {
		if key == messageKey {
			results = append(results, index)
		}
	}
	return results
}

// QueryMessagesByTimeRange 根据时间范围查询消息
func (pm *PersistenceManager) QueryMessagesByTimeRange(startTime, endTime int64) []*MessageIndex {
	pm.indexMutex.RLock()
	defer pm.indexMutex.RUnlock()
	
	var results []*MessageIndex
	for _, index := range pm.messageIndex {
		if index.StoreTime >= startTime && index.StoreTime <= endTime {
			results = append(results, index)
		}
	}
	return results
}

// loadMessageIndex 加载消息索引
func (pm *PersistenceManager) loadMessageIndex() error {
	data, err := ioutil.ReadFile(pm.messageIndexFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	
	var indexData MessageIndexData
	if err := json.Unmarshal(data, &indexData); err != nil {
		return err
	}
	
	pm.indexMutex.Lock()
	pm.messageIndex = indexData.Index
	pm.indexMutex.Unlock()
	
	return nil
}

// saveMessageIndex 保存消息索引
func (pm *PersistenceManager) saveMessageIndex() error {
	pm.indexMutex.RLock()
	indexCopy := make(map[string]*MessageIndex)
	for k, v := range pm.messageIndex {
		indexCopy[k] = v
	}
	pm.indexMutex.RUnlock()
	
	indexData := MessageIndexData{
		Index:      indexCopy,
		UpdateTime: time.Now().Unix(),
	}
	
	data, err := json.MarshalIndent(indexData, "", "  ")
	if err != nil {
		return err
	}
	
	return ioutil.WriteFile(pm.messageIndexFile, data, 0644)
}

// GetAllConsumeProgress 获取所有消费进度
func (pm *PersistenceManager) GetAllConsumeProgress() map[string]int64 {
	pm.progressMutex.RLock()
	defer pm.progressMutex.RUnlock()
	
	result := make(map[string]int64)
	for k, v := range pm.consumeProgress {
		result[k] = v
	}
	return result
}

// GetAllDelayProgress 获取所有延迟队列进度
func (pm *PersistenceManager) GetAllDelayProgress() map[int32]int64 {
	pm.delayMutex.RLock()
	defer pm.delayMutex.RUnlock()
	
	result := make(map[int32]int64)
	for k, v := range pm.delayProgress {
		result[k] = v
	}
	return result
}

// GetAllTransactionStates 获取所有事务状态
func (pm *PersistenceManager) GetAllTransactionStates() map[string]*PersistentTransactionState {
	pm.transactionMutex.RLock()
	defer pm.transactionMutex.RUnlock()
	
	result := make(map[string]*PersistentTransactionState)
	for k, v := range pm.transactionStates {
		result[k] = v
	}
	return result
}