package broker

import (
	"fmt"
	"testing"

	"go-rocketmq/pkg/common"
)

// BenchmarkBrokerPutMessage 基准测试消息发送性能
func BenchmarkBrokerPutMessage(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 创建主题
	err := broker.CreateTopic("BenchmarkTopic", 4)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	msg := &common.Message{
		Topic: "BenchmarkTopic",
		Body:  []byte("benchmark test message"),
		Properties: map[string]string{
			"KEYS": "benchmarkKey",
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := broker.PutMessage(msg)
			if err != nil {
				b.Errorf("Failed to put message: %v", err)
			}
		}
	})
}

// BenchmarkBrokerPullMessage 基准测试消息拉取性能
func BenchmarkBrokerPullMessage(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 创建主题
	err := broker.CreateTopic("PullBenchmarkTopic", 4)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	// 预先发送一些消息
	for i := 0; i < 100; i++ {
		msg := &common.Message{
			Topic: "PullBenchmarkTopic",
			Body:  []byte(fmt.Sprintf("pre-sent message %d", i)),
			Properties: map[string]string{
				"KEYS": fmt.Sprintf("preKey%d", i),
			},
		}
		_, err := broker.PutMessage(msg)
		if err != nil {
			b.Fatalf("Failed to pre-send message: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		offset := int64(0)
		for pb.Next() {
			_, err := broker.PullMessage("PullBenchmarkTopic", 0, offset, 10)
			if err != nil {
				b.Errorf("Failed to pull message: %v", err)
			}
			offset++
		}
	})
}

// BenchmarkBrokerCreateTopic 基准测试主题创建性能
func BenchmarkBrokerCreateTopic(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topicName := fmt.Sprintf("BenchmarkTopic%d", i)
		err := broker.CreateTopic(topicName, 4)
		if err != nil {
			b.Errorf("Failed to create topic: %v", err)
		}
	}
}

// BenchmarkBrokerGetTopicConfig 基准测试主题配置获取性能
func BenchmarkBrokerGetTopicConfig(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 创建一些主题
	for i := 0; i < 10; i++ {
		topicName := fmt.Sprintf("ConfigTopic%d", i)
		err := broker.CreateTopic(topicName, 4)
		if err != nil {
			b.Fatalf("Failed to create topic: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		topicIndex := 0
		for pb.Next() {
			topicName := fmt.Sprintf("ConfigTopic%d", topicIndex%10)
			config := broker.GetTopicConfig(topicName)
			if config == nil {
				b.Errorf("Expected topic config for %s", topicName)
			}
			topicIndex++
		}
	})
}

// BenchmarkBrokerConsumeOffset 基准测试消费偏移量操作性能
func BenchmarkBrokerConsumeOffset(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 创建主题
	err := broker.CreateTopic("OffsetTopic", 4)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		offset := int64(0)
		for pb.Next() {
			// 提交偏移量
			err := broker.CommitConsumeOffset("OffsetTopic", 0, "benchmarkGroup", offset)
			if err != nil {
				b.Errorf("Failed to commit offset: %v", err)
			}
			
			// 获取偏移量
			retrievedOffset := broker.GetConsumeOffset("OffsetTopic", 0, "benchmarkGroup")
			if retrievedOffset != offset {
				b.Errorf("Expected offset %d, got %d", offset, retrievedOffset)
			}
			offset++
		}
	})
}

// BenchmarkBrokerTransactionOperations 基准测试事务操作性能
func BenchmarkBrokerTransactionOperations(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 创建主题
	err := broker.CreateTopic("TransactionBenchmarkTopic", 4)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	// 注册事务监听器
	listener := &MockTransactionListener{}
	broker.RegisterTransactionListener("benchmarkProducerGroup", listener)

	msg := &common.Message{
		Topic: "TransactionBenchmarkTopic",
		Body:  []byte("transaction benchmark message"),
		Properties: map[string]string{
			"KEYS": "txBenchmarkKey",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txId := fmt.Sprintf("txBenchmark%d", i)
		
		// 准备事务消息
		_, err := broker.PrepareMessage(msg, "benchmarkProducerGroup", txId)
		if err != nil {
			b.Errorf("Failed to prepare transaction message: %v", err)
		}
		
		// 提交事务
		err = broker.CommitTransaction(txId)
		if err != nil {
			b.Errorf("Failed to commit transaction: %v", err)
		}
	}
}

// BenchmarkBrokerConcurrentPutMessage 基准测试并发消息发送性能
func BenchmarkBrokerConcurrentPutMessage(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 创建主题
	err := broker.CreateTopic("ConcurrentBenchmarkTopic", 8)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	b.ResetTimer()
	b.SetParallelism(10) // 设置并发度
	b.RunParallel(func(pb *testing.PB) {
		msgIndex := 0
		for pb.Next() {
			msg := &common.Message{
				Topic: "ConcurrentBenchmarkTopic",
				Body:  []byte(fmt.Sprintf("concurrent benchmark message %d", msgIndex)),
				Properties: map[string]string{
					"KEYS": fmt.Sprintf("concurrentKey%d", msgIndex),
				},
			}
			_, err := broker.PutMessage(msg)
			if err != nil {
				b.Errorf("Failed to put concurrent message: %v", err)
			}
			msgIndex++
		}
	})
}

// BenchmarkBrokerDelayMessage 基准测试延迟消息性能
func BenchmarkBrokerDelayMessage(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 创建主题
	err := broker.CreateTopic("DelayBenchmarkTopic", 4)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	msg := &common.Message{
		Topic: "DelayBenchmarkTopic",
		Body:  []byte("delay benchmark message"),
		Properties: map[string]string{
			"KEYS": "delayBenchmarkKey",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := broker.PutDelayMessage(msg, 1) // 延迟级别1
		if err != nil {
			b.Errorf("Failed to put delay message: %v", err)
		}
	}
}

// BenchmarkBrokerOrderedMessage 基准测试有序消息性能
func BenchmarkBrokerOrderedMessage(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 创建主题
	err := broker.CreateTopic("OrderedBenchmarkTopic", 4)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	msg := &common.Message{
		Topic: "OrderedBenchmarkTopic",
		Body:  []byte("ordered benchmark message"),
		Properties: map[string]string{
			"KEYS": "orderedBenchmarkKey",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		shardingKey := fmt.Sprintf("shard%d", i%4) // 分片到4个队列
		_, err := broker.PutOrderedMessage(msg, shardingKey)
		if err != nil {
			b.Errorf("Failed to put ordered message: %v", err)
		}
	}
}

// BenchmarkBrokerMemoryUsage 基准测试内存使用情况
func BenchmarkBrokerMemoryUsage(b *testing.B) {
	config := NewTestBrokerConfig()
	config.EnableCluster = false
	config.EnableFailover = false
	config.AclEnable = false

	broker := NewBroker(config)
	if err := broker.Start(); err != nil {
		b.Fatalf("Failed to start broker: %v", err)
	}
	defer broker.Stop()

	// 创建主题
	err := broker.CreateTopic("MemoryBenchmarkTopic", 4)
	if err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs() // 报告内存分配

	for i := 0; i < b.N; i++ {
		msg := &common.Message{
			Topic: "MemoryBenchmarkTopic",
			Body:  make([]byte, 1024), // 1KB消息
			Properties: map[string]string{
				"KEYS": fmt.Sprintf("memoryKey%d", i),
			},
		}
		_, err := broker.PutMessage(msg)
		if err != nil {
			b.Errorf("Failed to put message: %v", err)
		}
	}
}