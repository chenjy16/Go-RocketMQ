package client

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// DefaultTraceDispatcher 默认追踪分发器
type DefaultTraceDispatcher struct {
	traceProducer *Producer
	traceTopic    string
	buffer        []*TraceContext
	bufferMutex   sync.Mutex
	batchSize     int
	flushInterval time.Duration
	stopChan      chan struct{}
	isStarted     bool
	mutex         sync.RWMutex
}

// NewDefaultTraceDispatcher 创建默认追踪分发器
func NewDefaultTraceDispatcher(nameServerAddr string, traceTopic string) *DefaultTraceDispatcher {
	if traceTopic == "" {
		traceTopic = "RMQ_SYS_TRACE_TOPIC" // 默认追踪主题
	}
	
	// 创建追踪专用的生产者
	traceProducer := NewProducer("TRACE_PRODUCER_GROUP")
	traceProducer.SetNameServers([]string{nameServerAddr})
	
	return &DefaultTraceDispatcher{
		traceProducer: traceProducer,
		traceTopic:    traceTopic,
		buffer:        make([]*TraceContext, 0),
		batchSize:     100,
		flushInterval: 5 * time.Second,
		stopChan:      make(chan struct{}),
	}
}

// Start 启动追踪分发器
func (td *DefaultTraceDispatcher) Start() error {
	td.mutex.Lock()
	defer td.mutex.Unlock()
	
	if td.isStarted {
		return fmt.Errorf("trace dispatcher already started")
	}
	
	// 启动追踪生产者
	if err := td.traceProducer.Start(); err != nil {
		return fmt.Errorf("failed to start trace producer: %v", err)
	}
	
	td.isStarted = true
	
	// 启动定时刷新
	go td.flushLoop()
	
	return nil
}

// Stop 停止追踪分发器
func (td *DefaultTraceDispatcher) Stop() error {
	td.mutex.Lock()
	defer td.mutex.Unlock()
	
	if !td.isStarted {
		return nil
	}
	
	td.isStarted = false
	close(td.stopChan)
	
	// 刷新剩余数据
	td.Flush()
	
	// 停止追踪生产者
	td.traceProducer.Shutdown()
	
	return nil
}

// Append 添加追踪数据
func (td *DefaultTraceDispatcher) Append(ctx *TraceContext) error {
	td.bufferMutex.Lock()
	defer td.bufferMutex.Unlock()
	
	td.buffer = append(td.buffer, ctx)
	
	// 如果缓冲区满了，立即刷新
	if len(td.buffer) >= td.batchSize {
		go td.Flush()
	}
	
	return nil
}

// Flush 刷新追踪数据
func (td *DefaultTraceDispatcher) Flush() error {
	td.bufferMutex.Lock()
	if len(td.buffer) == 0 {
		td.bufferMutex.Unlock()
		return nil
	}
	
	// 复制缓冲区数据
	contexts := make([]*TraceContext, len(td.buffer))
	copy(contexts, td.buffer)
	td.buffer = td.buffer[:0] // 清空缓冲区
	td.bufferMutex.Unlock()
	
	// 发送追踪数据
	return td.sendTraceData(contexts)
}

// flushLoop 定时刷新循环
func (td *DefaultTraceDispatcher) flushLoop() {
	ticker := time.NewTicker(td.flushInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			td.Flush()
		case <-td.stopChan:
			return
		}
	}
}

// sendTraceData 发送追踪数据
func (td *DefaultTraceDispatcher) sendTraceData(contexts []*TraceContext) error {
	if len(contexts) == 0 {
		return nil
	}
	
	// 将追踪数据序列化为JSON
	data, err := json.Marshal(contexts)
	if err != nil {
		return fmt.Errorf("failed to marshal trace data: %v", err)
	}
	
	// 创建追踪消息
	msg := &Message{
		Topic: td.traceTopic,
		Body:  data,
		Properties: map[string]string{
			"TRACE_DATA_TYPE": "TRACE_CONTEXT",
			"TRACE_COUNT":     fmt.Sprintf("%d", len(contexts)),
		},
	}
	
	// 发送追踪消息
	_, err = td.traceProducer.SendSync(msg)
	if err != nil {
		log.Printf("Failed to send trace data: %v", err)
		return err
	}
	
	return nil
}

// ConsoleTraceHook 控制台追踪钩子
type ConsoleTraceHook struct {
	name string
}

// NewConsoleTraceHook 创建控制台追踪钩子
func NewConsoleTraceHook() *ConsoleTraceHook {
	return &ConsoleTraceHook{
		name: "ConsoleTraceHook",
	}
}

// SendTraceData 发送追踪数据到控制台
func (cth *ConsoleTraceHook) SendTraceData(ctx *TraceContext) error {
	data, err := json.MarshalIndent(ctx, "", "  ")
	if err != nil {
		return err
	}
	
	log.Printf("[TRACE] %s\n", string(data))
	return nil
}

// GetHookName 获取钩子名称
func (cth *ConsoleTraceHook) GetHookName() string {
	return cth.name
}

// TraceManager 追踪管理器
type TraceManager struct {
	dispatcher TraceDispatcher
	hooks      []TraceHook
	enabled    bool
	mutex      sync.RWMutex
}

// NewTraceManager 创建追踪管理器
func NewTraceManager(dispatcher TraceDispatcher) *TraceManager {
	return &TraceManager{
		dispatcher: dispatcher,
		hooks:      make([]TraceHook, 0),
		enabled:    true,
	}
}

// AddHook 添加追踪钩子
func (tm *TraceManager) AddHook(hook TraceHook) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.hooks = append(tm.hooks, hook)
}

// RemoveHook 移除追踪钩子
func (tm *TraceManager) RemoveHook(hookName string) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	
	for i, hook := range tm.hooks {
		if hook.GetHookName() == hookName {
			tm.hooks = append(tm.hooks[:i], tm.hooks[i+1:]...)
			break
		}
	}
}

// SetEnabled 设置追踪是否启用
func (tm *TraceManager) SetEnabled(enabled bool) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.enabled = enabled
}

// IsEnabled 检查追踪是否启用
func (tm *TraceManager) IsEnabled() bool {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	return tm.enabled
}

// TraceMessage 追踪消息
func (tm *TraceManager) TraceMessage(ctx *TraceContext) {
	if !tm.IsEnabled() {
		return
	}
	
	// 发送到分发器
	if tm.dispatcher != nil {
		if err := tm.dispatcher.Append(ctx); err != nil {
			log.Printf("Failed to append trace data to dispatcher: %v", err)
		}
	}
	
	// 发送到钩子
	tm.mutex.RLock()
	hooks := make([]TraceHook, len(tm.hooks))
	copy(hooks, tm.hooks)
	tm.mutex.RUnlock()
	
	for _, hook := range hooks {
		go func(h TraceHook) {
			if err := h.SendTraceData(ctx); err != nil {
				log.Printf("Failed to send trace data to hook %s: %v", h.GetHookName(), err)
			}
		}(hook)
	}
}

// Start 启动追踪管理器
func (tm *TraceManager) Start() error {
	if tm.dispatcher != nil {
		return tm.dispatcher.Start()
	}
	return nil
}

// Stop 停止追踪管理器
func (tm *TraceManager) Stop() error {
	if tm.dispatcher != nil {
		return tm.dispatcher.Stop()
	}
	return nil
}

// CreateProduceTraceContext 创建生产者追踪上下文
func CreateProduceTraceContext(groupName string, msg *Message) *TraceContext {
	traceBean := &TraceBean{
		Topic:        msg.Topic,
		MsgId:        "", // 发送后会设置
		Tags:         msg.GetProperty("TAGS"),
		Keys:         msg.GetProperty("KEYS"),
		BodyLength:   len(msg.Body),
		MsgType:      msg.GetMessageType(),
		TraceType:    TraceTypeProduce,
		GroupName:    groupName,
		TimeStamp:    time.Now().UnixMilli(),
		Properties:   msg.Properties,
	}
	
	return &TraceContext{
		TraceType:  TraceTypeProduce,
		TimeStamp:  time.Now().UnixMilli(),
		GroupName:  groupName,
		TraceBeans: []*TraceBean{traceBean},
	}
}

// CreateConsumeTraceContext 创建消费者追踪上下文
func CreateConsumeTraceContext(groupName string, msg *MessageExt) *TraceContext {
	traceBean := &TraceBean{
		Topic:        msg.Topic,
		MsgId:        msg.MsgId,
		OffsetMsgId:  "", // MessageExt中没有OffsetMsgId字段，使用空字符串
		Tags:         msg.GetProperty("TAGS"),
		Keys:         msg.GetProperty("KEYS"),
		StoreHost:    msg.StoreHost,
		StoreTime:    msg.StoreTimestamp.UnixMilli(),
		RetryTimes:   int(msg.ReconsumeTimes),
		BodyLength:   len(msg.Body),
		MsgType:      msg.GetMessageType(),
		TraceType:    TraceTypeConsume,
		GroupName:    groupName,
		TimeStamp:    time.Now().UnixMilli(),
		Properties:   msg.Properties,
	}
	
	return &TraceContext{
		TraceType:  TraceTypeConsume,
		TimeStamp:  time.Now().UnixMilli(),
		GroupName:  groupName,
		TraceBeans: []*TraceBean{traceBean},
	}
}