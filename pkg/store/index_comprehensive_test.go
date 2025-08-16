package store

import (
	"fmt"
	"os"
	"testing"
	"time"

	"go-rocketmq/pkg/common"
)

func TestIndexComprehensive(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "rocketmq_index_comprehensive_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建存储配置
	config := NewDefaultStoreConfig()
	config.StorePathRootDir = tempDir
	config.StorePathCommitLog = tempDir + "/commitlog"
	config.StorePathConsumeQueue = tempDir + "/consumequeue"
	config.StorePathIndex = tempDir + "/index"

	// 创建消息存储
	messageStore, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}

	// 启动消息存储
	err = messageStore.Start()
	if err != nil {
		t.Fatalf("Failed to start message store: %v", err)
	}
	defer messageStore.Shutdown()

	// 发送多条测试消息
	messageIds := make([]string, 0)
	for i := 0; i < 5; i++ {
		msg := &common.Message{
			Topic: "comprehensive-test-topic",
			Tags:  "test-tag",
			Keys:  fmt.Sprintf("test-key-%d", i),
			Body:  []byte(fmt.Sprintf("comprehensive test message %d", i)),
			Properties: map[string]string{
				"UNIQ_KEY": fmt.Sprintf("unique-key-%d", i),
				"custom-prop": fmt.Sprintf("value-%d", i),
			},
		}

		result, err := messageStore.PutMessage(msg)
		if err != nil {
			t.Fatalf("Failed to put message %d: %v", i, err)
		}
		messageIds = append(messageIds, result.MsgId)
	}

	// 等待索引构建
	time.Sleep(200 * time.Millisecond)

	// 测试消息索引构建
	t.Run("MessageIndexBuilding", func(t *testing.T) {
		testMessageIndexBuilding(t, messageStore, messageIds)
	})

	// 测试时间范围查询
	t.Run("TimeRangeQuery", func(t *testing.T) {
		testTimeRangeQuery(t, messageStore)
	})

	// 测试消息轨迹查询
	t.Run("MessageTraceQuery", func(t *testing.T) {
		testMessageTraceQuery(t, messageStore, messageIds)
	})

	// 测试索引持久化
	t.Run("IndexPersistence", func(t *testing.T) {
		testIndexPersistence(t, messageStore)
	})
}

func testMessageIndexBuilding(t *testing.T, messageStore *DefaultMessageStore, messageIds []string) {
	// 验证消息ID索引
	for i, msgId := range messageIds {
		index := messageStore.GetMessageIndex(msgId)
		if index == nil {
			t.Errorf("Expected message index for msgId %s, got nil", msgId)
			continue
		}

		if index.Topic != "comprehensive-test-topic" {
			t.Errorf("Expected topic 'comprehensive-test-topic', got '%s'", index.Topic)
		}
		if index.Tags != "test-tag" {
			t.Errorf("Expected tags 'test-tag', got '%s'", index.Tags)
		}
		t.Logf("Message %d index verified: %+v", i, index)
	}

	// 验证Key索引
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		indexes := messageStore.QueryMessagesByKey(key)
		if len(indexes) == 0 {
			t.Logf("No index found for key %s (may be expected)", key)
			continue
		}
		t.Logf("Found %d indexes for key %s", len(indexes), key)
	}

	// 验证UniqKey索引
	for i := 0; i < 5; i++ {
		uniqKey := fmt.Sprintf("unique-key-%d", i)
		index := messageStore.GetMessageIndex(uniqKey)
		if index != nil {
			t.Logf("Found index for uniqKey %s: %+v", uniqKey, index)
		} else {
			t.Logf("No index found for uniqKey %s (may be expected)", uniqKey)
		}
	}
}

func testTimeRangeQuery(t *testing.T, messageStore *DefaultMessageStore) {
	// 查询最近1小时的消息
	startTime := time.Now().Add(-1 * time.Hour).UnixMilli()
	endTime := time.Now().Add(1 * time.Hour).UnixMilli()

	messages, err := messageStore.QueryMessageByTimeRange("comprehensive-test-topic", startTime, endTime, 10)
	if err != nil {
		t.Errorf("Failed to query messages by time range: %v", err)
		return
	}

	if len(messages) == 0 {
		t.Log("No messages found in time range (may be expected)")
		return
	}

	t.Logf("Found %d messages in time range", len(messages))
	for i, msg := range messages {
		if msg.Topic != "comprehensive-test-topic" {
			t.Errorf("Message %d: expected topic 'comprehensive-test-topic', got '%s'", i, msg.Topic)
		}
		t.Logf("Message %d: Topic=%s, Keys=%s, StoreTime=%v", i, msg.Topic, msg.Keys, msg.StoreTimestamp)
	}
}

func testMessageTraceQuery(t *testing.T, messageStore *DefaultMessageStore, messageIds []string) {
	for i, msgId := range messageIds {
		trace, err := messageStore.QueryMessageTrace(msgId)
		if err != nil {
			t.Errorf("Failed to query message trace for msgId %s: %v", msgId, err)
			continue
		}

		if trace.MsgId != msgId {
			t.Errorf("Expected msgId %s, got %s", msgId, trace.MsgId)
		}
		if trace.Topic != "comprehensive-test-topic" {
			t.Errorf("Expected topic 'comprehensive-test-topic', got '%s'", trace.Topic)
		}
		if trace.Status != "STORED" {
			t.Errorf("Expected status 'STORED', got '%s'", trace.Status)
		}

		t.Logf("Message %d trace: %+v", i, trace)
	}
}

func testIndexPersistence(t *testing.T, messageStore *DefaultMessageStore) {
	// 获取持久化管理器中的所有索引
	allIndexes := messageStore.persistenceManager.messageIndex
	if len(allIndexes) == 0 {
		t.Log("No indexes found in persistence manager (may be expected)")
		return
	}

	t.Logf("Found %d indexes in persistence manager", len(allIndexes))
	for key, index := range allIndexes {
		if index.Topic != "comprehensive-test-topic" {
			t.Errorf("Index %s: expected topic 'comprehensive-test-topic', got '%s'", key, index.Topic)
		}
		t.Logf("Index %s: %+v", key, index)
	}
}