package client

import (
	"testing"
	"bytes"
	"encoding/binary"
)

// TestBatchMessageEncoding 测试批量消息编码功能
func TestBatchMessageEncoding(t *testing.T) {
	// 创建事务生产者
	producer := &TransactionProducer{
		Producer: &Producer{
			config: &ProducerConfig{
				GroupName: "test_group",
			},
		},
	}

	// 创建测试消息
	messages := []*Message{
		{
			Topic:      "TestTopic",
			Tags:       "TagA",
			Keys:       "Key1",
			Body:       []byte("Message 1"),
			Properties: map[string]string{"prop1": "value1"},
		},
		{
			Topic:      "TestTopic",
			Tags:       "TagB",
			Keys:       "Key2",
			Body:       []byte("Message 2"),
			Properties: map[string]string{"prop2": "value2"},
		},
	}

	// 测试批量消息编码
	encodedData, err := producer.encodeBatchMessages(messages)
	if err != nil {
		t.Fatalf("批量消息编码失败: %v", err)
	}

	// 验证编码结果不为空
	if len(encodedData) == 0 {
		t.Fatal("编码结果为空")
	}

	// 验证批量消息头部
	buffer := bytes.NewReader(encodedData)
	var batchHeader BatchMessageHeader
	if err := binary.Read(buffer, binary.BigEndian, &batchHeader.Magic); err != nil {
		t.Fatalf("读取魔数失败: %v", err)
	}
	if err := binary.Read(buffer, binary.BigEndian, &batchHeader.Version); err != nil {
		t.Fatalf("读取版本号失败: %v", err)
	}
	if err := binary.Read(buffer, binary.BigEndian, &batchHeader.MessageCount); err != nil {
		t.Fatalf("读取消息数量失败: %v", err)
	}
	if err := binary.Read(buffer, binary.BigEndian, &batchHeader.Timestamp); err != nil {
		t.Fatalf("读取时间戳失败: %v", err)
	}

	// 验证魔数
	if batchHeader.Magic != 0x12345678 {
		t.Errorf("魔数不正确，期望: 0x12345678, 实际: 0x%X", batchHeader.Magic)
	}

	// 验证消息数量
	if batchHeader.MessageCount != 2 {
		t.Errorf("消息数量不正确，期望: 2, 实际: %d", batchHeader.MessageCount)
	}

	t.Logf("批量消息编码成功，消息数量: %d, 版本: %d", batchHeader.MessageCount, batchHeader.Version)
}

// TestSingleMessageEncoding 测试单个消息编码功能
func TestSingleMessageEncoding(t *testing.T) {
	// 创建事务生产者
	producer := &TransactionProducer{
		Producer: &Producer{
			config: &ProducerConfig{
				GroupName: "test_group",
			},
		},
	}

	// 创建测试消息
	msg := &Message{
		Topic:      "TestTopic",
		Tags:       "TagA",
		Keys:       "Key1",
		Body:       []byte("Test Message"),
		Properties: map[string]string{"prop1": "value1", "prop2": "value2"},
	}

	// 测试单个消息编码
	encodedData, err := producer.encodeMessage(msg, 0)
	if err != nil {
		t.Fatalf("单个消息编码失败: %v", err)
	}

	// 验证编码结果不为空
	if len(encodedData) == 0 {
		t.Fatal("编码结果为空")
	}

	// 验证消息头部
	buffer := bytes.NewReader(encodedData)
	var messageHeader MessageHeader
	if err := binary.Read(buffer, binary.BigEndian, &messageHeader.Index); err != nil {
		t.Fatalf("读取索引失败: %v", err)
	}
	if err := binary.Read(buffer, binary.BigEndian, &messageHeader.BodyLength); err != nil {
		t.Fatalf("读取消息体长度失败: %v", err)
	}
	if err := binary.Read(buffer, binary.BigEndian, &messageHeader.Flag); err != nil {
		t.Fatalf("读取标志失败: %v", err)
	}
	if err := binary.Read(buffer, binary.BigEndian, &messageHeader.Timestamp); err != nil {
		t.Fatalf("读取时间戳失败: %v", err)
	}

	// 验证索引
	if messageHeader.Index != 0 {
		t.Errorf("索引不正确，期望: 0, 实际: %d", messageHeader.Index)
	}

	t.Logf("单个消息编码成功，索引: %d, 消息体长度: %d", messageHeader.Index, messageHeader.BodyLength)
}

// TestPropertiesEncoding 测试属性编码功能
func TestPropertiesEncoding(t *testing.T) {
	// 创建事务生产者
	producer := &TransactionProducer{
		Producer: &Producer{
			config: &ProducerConfig{
				GroupName: "test_group",
			},
		},
	}

	// 测试属性编码
	properties := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	encodedProps, err := producer.encodeProperties(properties)
	if err != nil {
		t.Fatalf("属性编码失败: %v", err)
	}

	// 验证编码结果不为空
	if len(encodedProps) == 0 {
		t.Fatal("属性编码结果为空")
	}

	// 验证编码结果是二进制格式，包含属性数量
	buffer := bytes.NewReader(encodedProps)
	var propCount int32
	if err := binary.Read(buffer, binary.BigEndian, &propCount); err != nil {
		t.Fatalf("读取属性数量失败: %v", err)
	}

	// 验证属性数量
	if propCount != int32(len(properties)) {
		t.Errorf("属性数量不正确，期望: %d, 实际: %d", len(properties), propCount)
	}

	t.Logf("属性编码成功，属性数量: %d", propCount)
}