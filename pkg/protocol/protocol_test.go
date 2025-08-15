package protocol

import (
	"encoding/json"
	"testing"
	"time"
)

// TestDataVersion 测试DataVersion结构体
func TestDataVersion(t *testing.T) {
	// 测试NewDataVersion
	dv := NewDataVersion()
	if dv == nil {
		t.Fatal("NewDataVersion should not return nil")
	}
	if dv.Timestamp <= 0 {
		t.Error("Timestamp should be greater than 0")
	}
	if dv.Counter != 0 {
		t.Error("Counter should be 0 initially")
	}

	// 测试NextVersion
	originalTimestamp := dv.Timestamp
	originalCounter := dv.Counter
	nextDv := dv.NextVersion()
	if nextDv == nil {
		t.Fatal("NextVersion should not return nil")
	}
	if nextDv.Counter != originalCounter+1 {
		t.Errorf("Counter should be incremented, expected %d, got %d", originalCounter+1, nextDv.Counter)
	}
	if nextDv.Timestamp < originalTimestamp {
		t.Error("Timestamp should not decrease")
	}
}

// TestTopicConfig 测试TopicConfig结构体
func TestTopicConfig(t *testing.T) {
	tc := &TopicConfig{
		TopicName:       "TestTopic",
		ReadQueueNums:   4,
		WriteQueueNums:  4,
		Perm:            6,
		TopicFilterType: 0,
		TopicSysFlag:    0,
		Order:           false,
	}

	// 测试JSON序列化
	data, err := json.Marshal(tc)
	if err != nil {
		t.Fatalf("Failed to marshal TopicConfig: %v", err)
	}

	// 测试JSON反序列化
	var tc2 TopicConfig
	err = json.Unmarshal(data, &tc2)
	if err != nil {
		t.Fatalf("Failed to unmarshal TopicConfig: %v", err)
	}

	if tc2.TopicName != tc.TopicName {
		t.Errorf("TopicName mismatch, expected %s, got %s", tc.TopicName, tc2.TopicName)
	}
	if tc2.ReadQueueNums != tc.ReadQueueNums {
		t.Errorf("ReadQueueNums mismatch, expected %d, got %d", tc.ReadQueueNums, tc2.ReadQueueNums)
	}
}

// TestTopicConfigSerializeWrapper 测试TopicConfigSerializeWrapper
func TestTopicConfigSerializeWrapper(t *testing.T) {
	tcw := &TopicConfigSerializeWrapper{
		TopicConfigTable: make(map[string]*TopicConfig),
		DataVersion:      NewDataVersion(),
	}

	tc := &TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
	}
	tcw.TopicConfigTable["TestTopic"] = tc

	// 测试JSON序列化
	data, err := json.Marshal(tcw)
	if err != nil {
		t.Fatalf("Failed to marshal TopicConfigSerializeWrapper: %v", err)
	}

	// 测试JSON反序列化
	var tcw2 TopicConfigSerializeWrapper
	err = json.Unmarshal(data, &tcw2)
	if err != nil {
		t.Fatalf("Failed to unmarshal TopicConfigSerializeWrapper: %v", err)
	}

	if len(tcw2.TopicConfigTable) != 1 {
		t.Errorf("TopicConfigTable length mismatch, expected 1, got %d", len(tcw2.TopicConfigTable))
	}
}

// TestRegisterBrokerResult 测试RegisterBrokerResult
func TestRegisterBrokerResult(t *testing.T) {
	rbr := &RegisterBrokerResult{
		HaServerAddr: "127.0.0.1:10912",
		MasterAddr:   "127.0.0.1:10911",
	}

	// 测试JSON序列化
	data, err := json.Marshal(rbr)
	if err != nil {
		t.Fatalf("Failed to marshal RegisterBrokerResult: %v", err)
	}

	// 测试JSON反序列化
	var rbr2 RegisterBrokerResult
	err = json.Unmarshal(data, &rbr2)
	if err != nil {
		t.Fatalf("Failed to unmarshal RegisterBrokerResult: %v", err)
	}

	if rbr2.HaServerAddr != rbr.HaServerAddr {
		t.Errorf("HaServerAddr mismatch, expected %s, got %s", rbr.HaServerAddr, rbr2.HaServerAddr)
	}
	if rbr2.MasterAddr != rbr.MasterAddr {
		t.Errorf("MasterAddr mismatch, expected %s, got %s", rbr.MasterAddr, rbr2.MasterAddr)
	}
}

// TestTopicRouteData 测试TopicRouteData
func TestTopicRouteData(t *testing.T) {
	trd := &TopicRouteData{
		OrderTopicConf:    "",
		QueueDatas:        []*QueueData{},
		BrokerDatas:       []*BrokerData{},
		FilterServerTable: make(map[string][]string),
	}

	// 添加QueueData
	qd := &QueueData{
		BrokerName:     "broker-a",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
		TopicSysFlag:   0,
	}
	trd.QueueDatas = append(trd.QueueDatas, qd)

	// 添加BrokerData
	bd := &BrokerData{
		Cluster:     "DefaultCluster",
		BrokerName:  "broker-a",
		BrokerAddrs: map[int64]string{0: "127.0.0.1:10911"},
	}
	trd.BrokerDatas = append(trd.BrokerDatas, bd)

	// 测试JSON序列化
	data, err := json.Marshal(trd)
	if err != nil {
		t.Fatalf("Failed to marshal TopicRouteData: %v", err)
	}

	// 测试JSON反序列化
	var trd2 TopicRouteData
	err = json.Unmarshal(data, &trd2)
	if err != nil {
		t.Fatalf("Failed to unmarshal TopicRouteData: %v", err)
	}

	if len(trd2.QueueDatas) != 1 {
		t.Errorf("QueueDatas length mismatch, expected 1, got %d", len(trd2.QueueDatas))
	}
	if len(trd2.BrokerDatas) != 1 {
		t.Errorf("BrokerDatas length mismatch, expected 1, got %d", len(trd2.BrokerDatas))
	}
}

// TestQueueData 测试QueueData
func TestQueueData(t *testing.T) {
	qd := &QueueData{
		BrokerName:     "broker-a",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
		TopicSysFlag:   0,
	}

	// 测试JSON序列化
	data, err := json.Marshal(qd)
	if err != nil {
		t.Fatalf("Failed to marshal QueueData: %v", err)
	}

	// 测试JSON反序列化
	var qd2 QueueData
	err = json.Unmarshal(data, &qd2)
	if err != nil {
		t.Fatalf("Failed to unmarshal QueueData: %v", err)
	}

	if qd2.BrokerName != qd.BrokerName {
		t.Errorf("BrokerName mismatch, expected %s, got %s", qd.BrokerName, qd2.BrokerName)
	}
	if qd2.ReadQueueNums != qd.ReadQueueNums {
		t.Errorf("ReadQueueNums mismatch, expected %d, got %d", qd.ReadQueueNums, qd2.ReadQueueNums)
	}
}

// TestBrokerData 测试BrokerData
func TestBrokerData(t *testing.T) {
	bd := &BrokerData{
		Cluster:     "DefaultCluster",
		BrokerName:  "broker-a",
		BrokerAddrs: map[int64]string{0: "127.0.0.1:10911", 1: "127.0.0.1:10921"},
	}

	// 测试JSON序列化
	data, err := json.Marshal(bd)
	if err != nil {
		t.Fatalf("Failed to marshal BrokerData: %v", err)
	}

	// 测试JSON反序列化
	var bd2 BrokerData
	err = json.Unmarshal(data, &bd2)
	if err != nil {
		t.Fatalf("Failed to unmarshal BrokerData: %v", err)
	}

	if bd2.Cluster != bd.Cluster {
		t.Errorf("Cluster mismatch, expected %s, got %s", bd.Cluster, bd2.Cluster)
	}
	if bd2.BrokerName != bd.BrokerName {
		t.Errorf("BrokerName mismatch, expected %s, got %s", bd.BrokerName, bd2.BrokerName)
	}
	if len(bd2.BrokerAddrs) != 2 {
		t.Errorf("BrokerAddrs length mismatch, expected 2, got %d", len(bd2.BrokerAddrs))
	}
}

// TestClusterInfo 测试ClusterInfo
func TestClusterInfo(t *testing.T) {
	ci := &ClusterInfo{
		BrokerAddrTable:  make(map[string]map[int64]string),
		ClusterAddrTable: make(map[string][]string),
	}

	// 添加Broker地址
	ci.BrokerAddrTable["broker-a"] = map[int64]string{0: "127.0.0.1:10911"}
	ci.ClusterAddrTable["DefaultCluster"] = []string{"broker-a"}

	// 测试JSON序列化
	data, err := json.Marshal(ci)
	if err != nil {
		t.Fatalf("Failed to marshal ClusterInfo: %v", err)
	}

	// 测试JSON反序列化
	var ci2 ClusterInfo
	err = json.Unmarshal(data, &ci2)
	if err != nil {
		t.Fatalf("Failed to unmarshal ClusterInfo: %v", err)
	}

	if len(ci2.BrokerAddrTable) != 1 {
		t.Errorf("BrokerAddrTable length mismatch, expected 1, got %d", len(ci2.BrokerAddrTable))
	}
	if len(ci2.ClusterAddrTable) != 1 {
		t.Errorf("ClusterAddrTable length mismatch, expected 1, got %d", len(ci2.ClusterAddrTable))
	}
}

// TestRequestCode 测试RequestCode常量
func TestRequestCode(t *testing.T) {
	tests := []struct {
		name string
		code RequestCode
		value int32
	}{
		{"UpdateAndCreateTopic", UpdateAndCreateTopic, 17},
		{"GetRouteInfoByTopic", GetRouteInfoByTopic, 105},
		{"GetBrokerClusterInfo", GetBrokerClusterInfo, 106},
		{"RegisterBroker", RegisterBroker, 103},
		{"UnregisterBroker", UnregisterBroker, 104},
		{"SendMessage", SendMessage, 10},
		{"SendMessageV2", SendMessageV2, 310},
		{"SendBatchMessage", SendBatchMessage, 320},
		{"PullMessage", PullMessage, 11},
		{"QueryMessage", QueryMessage, 12},
		{"QueryMessageByKey", QueryMessageByKey, 33},
		{"QueryMessageById", QueryMessageById, 34},
		{"EndTransaction", EndTransaction, 37},
		{"CheckTransactionState", CheckTransactionState, 39},
		{"UpdateBrokerConfig", UpdateBrokerConfig, 25},
		{"GetBrokerConfig", GetBrokerConfig, 26},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int32(tt.code) != tt.value {
				t.Errorf("%s value mismatch, expected %d, got %d", tt.name, tt.value, int32(tt.code))
			}
		})
	}
}

// TestResponseCode 测试ResponseCode常量
func TestResponseCode(t *testing.T) {
	tests := []struct {
		name string
		code ResponseCode
		value int32
	}{
		{"Success", Success, 0},
		{"SystemError", SystemError, 1},
		{"SystemBusy", SystemBusy, 2},
		{"RequestCodeNotSupported", RequestCodeNotSupported, 3},
		{"TransactionFailed", TransactionFailed, 4},
		{"FlushDiskTimeout", FlushDiskTimeout, 10},
		{"SlaveNotAvailable", SlaveNotAvailable, 11},
		{"FlushSlaveTimeout", FlushSlaveTimeout, 12},
		{"MessageIllegal", MessageIllegal, 13},
		{"ServiceNotAvailable", ServiceNotAvailable, 14},
		{"VersionNotSupported", VersionNotSupported, 15},
		{"NoPermission", NoPermission, 16},
		{"TopicNotExist", TopicNotExist, 17},
		{"TopicExistAlready", TopicExistAlready, 18},
		{"PullNotFound", PullNotFound, 19},
		{"PullRetryImmediately", PullRetryImmediately, 20},
		{"PullOffsetMoved", PullOffsetMoved, 21},
		{"QueryNotFound", QueryNotFound, 22},
		{"SubscriptionParseFailed", SubscriptionParseFailed, 23},
		{"SubscriptionNotExist", SubscriptionNotExist, 24},
		{"SubscriptionNotLatest", SubscriptionNotLatest, 25},
		{"SubscriptionGroupNotExist", SubscriptionGroupNotExist, 26},
		{"TransactionShouldCommit", TransactionShouldCommit, 200},
		{"TransactionShouldRollback", TransactionShouldRollback, 201},
		{"TransactionStateUnknown", TransactionStateUnknown, 202},
		{"TransactionStateGroupWrong", TransactionStateGroupWrong, 203},
		{"NoBuyerId", NoBuyerId, 204},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int32(tt.code) != tt.value {
				t.Errorf("%s value mismatch, expected %d, got %d", tt.name, tt.value, int32(tt.code))
			}
		})
	}
}

// TestSubscriptionData 测试SubscriptionData
func TestSubscriptionData(t *testing.T) {
	sd := &SubscriptionData{
		Topic:          "TestTopic",
		SubString:      "*",
		TagsSet:        []string{"TagA", "TagB"},
		CodeSet:        []int32{1, 2, 3},
		SubVersion:     1,
		ExpressionType: "TAG",
	}

	// 测试JSON序列化
	data, err := json.Marshal(sd)
	if err != nil {
		t.Fatalf("Failed to marshal SubscriptionData: %v", err)
	}

	// 测试JSON反序列化
	var sd2 SubscriptionData
	err = json.Unmarshal(data, &sd2)
	if err != nil {
		t.Fatalf("Failed to unmarshal SubscriptionData: %v", err)
	}

	if sd2.Topic != sd.Topic {
		t.Errorf("Topic mismatch, expected %s, got %s", sd.Topic, sd2.Topic)
	}
	if len(sd2.TagsSet) != len(sd.TagsSet) {
		t.Errorf("TagsSet length mismatch, expected %d, got %d", len(sd.TagsSet), len(sd2.TagsSet))
	}
	if len(sd2.CodeSet) != len(sd.CodeSet) {
		t.Errorf("CodeSet length mismatch, expected %d, got %d", len(sd.CodeSet), len(sd2.CodeSet))
	}
}

// TestRemotingCommand 测试RemotingCommand
func TestRemotingCommand(t *testing.T) {
	// 测试CreateRemotingCommand
	cmd := CreateRemotingCommand(SendMessage)
	if cmd == nil {
		t.Fatal("CreateRemotingCommand should not return nil")
	}
	if cmd.Code != SendMessage {
		t.Errorf("Code mismatch, expected %d, got %d", SendMessage, cmd.Code)
	}
	if cmd.Language != "GO" {
		t.Errorf("Language mismatch, expected GO, got %s", cmd.Language)
	}
	if cmd.Version != 1 {
		t.Errorf("Version mismatch, expected 1, got %d", cmd.Version)
	}

	// 测试CreateResponseCommand
	respCmd := CreateResponseCommand(Success, "OK")
	if respCmd == nil {
		t.Fatal("CreateResponseCommand should not return nil")
	}
	if respCmd.Code != RequestCode(Success) {
		t.Errorf("Code mismatch, expected %d, got %d", Success, respCmd.Code)
	}
	if respCmd.Remark != "OK" {
		t.Errorf("Remark mismatch, expected OK, got %s", respCmd.Remark)
	}
}

// TestSendMessageRequestHeader 测试SendMessageRequestHeader
func TestSendMessageRequestHeader(t *testing.T) {
	header := &SendMessageRequestHeader{
		ProducerGroup:         "DefaultProducerGroup",
		Topic:                 "TestTopic",
		DefaultTopic:          "TBW102",
		DefaultTopicQueueNums: 4,
		QueueId:               0,
		SysFlag:               0,
		BornTimestamp:         time.Now().UnixMilli(),
		Flag:                  0,
		Properties:            "TAGS=TagA",
		ReconsumeTimes:        0,
		UnitMode:              false,
		Batch:                 false,
	}

	// 测试JSON序列化
	data, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("Failed to marshal SendMessageRequestHeader: %v", err)
	}

	// 测试JSON反序列化
	var header2 SendMessageRequestHeader
	err = json.Unmarshal(data, &header2)
	if err != nil {
		t.Fatalf("Failed to unmarshal SendMessageRequestHeader: %v", err)
	}

	if header2.ProducerGroup != header.ProducerGroup {
		t.Errorf("ProducerGroup mismatch, expected %s, got %s", header.ProducerGroup, header2.ProducerGroup)
	}
	if header2.Topic != header.Topic {
		t.Errorf("Topic mismatch, expected %s, got %s", header.Topic, header2.Topic)
	}
}

// TestSendMessageResponseHeader 测试SendMessageResponseHeader
func TestSendMessageResponseHeader(t *testing.T) {
	header := &SendMessageResponseHeader{
		MsgId:         "C0A8010100002A9F0000000000000000",
		QueueId:       0,
		QueueOffset:   100,
		TransactionId: "",
		BatchUniqId:   "",
	}

	// 测试JSON序列化
	data, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("Failed to marshal SendMessageResponseHeader: %v", err)
	}

	// 测试JSON反序列化
	var header2 SendMessageResponseHeader
	err = json.Unmarshal(data, &header2)
	if err != nil {
		t.Fatalf("Failed to unmarshal SendMessageResponseHeader: %v", err)
	}

	if header2.MsgId != header.MsgId {
		t.Errorf("MsgId mismatch, expected %s, got %s", header.MsgId, header2.MsgId)
	}
	if header2.QueueOffset != header.QueueOffset {
		t.Errorf("QueueOffset mismatch, expected %d, got %d", header.QueueOffset, header2.QueueOffset)
	}
}

// TestPullMessageRequestHeader 测试PullMessageRequestHeader
func TestPullMessageRequestHeader(t *testing.T) {
	header := &PullMessageRequestHeader{
		ConsumerGroup:        "DefaultConsumerGroup",
		Topic:                "TestTopic",
		QueueId:              0,
		QueueOffset:          0,
		MaxMsgNums:           32,
		SysFlag:              0,
		CommitOffset:         0,
		SuspendTimeoutMillis: 15000,
		Subscription:         "*",
		SubVersion:           1,
		ExpressionType:       "TAG",
	}

	// 测试JSON序列化
	data, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("Failed to marshal PullMessageRequestHeader: %v", err)
	}

	// 测试JSON反序列化
	var header2 PullMessageRequestHeader
	err = json.Unmarshal(data, &header2)
	if err != nil {
		t.Fatalf("Failed to unmarshal PullMessageRequestHeader: %v", err)
	}

	if header2.ConsumerGroup != header.ConsumerGroup {
		t.Errorf("ConsumerGroup mismatch, expected %s, got %s", header.ConsumerGroup, header2.ConsumerGroup)
	}
	if header2.MaxMsgNums != header.MaxMsgNums {
		t.Errorf("MaxMsgNums mismatch, expected %d, got %d", header.MaxMsgNums, header2.MaxMsgNums)
	}
}

// TestPullMessageResponseHeader 测试PullMessageResponseHeader
func TestPullMessageResponseHeader(t *testing.T) {
	header := &PullMessageResponseHeader{
		SuggestWhichBrokerId: 0,
		NextBeginOffset:      100,
		MinOffset:            0,
		MaxOffset:            1000,
	}

	// 测试JSON序列化
	data, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("Failed to marshal PullMessageResponseHeader: %v", err)
	}

	// 测试JSON反序列化
	var header2 PullMessageResponseHeader
	err = json.Unmarshal(data, &header2)
	if err != nil {
		t.Fatalf("Failed to unmarshal PullMessageResponseHeader: %v", err)
	}

	if header2.NextBeginOffset != header.NextBeginOffset {
		t.Errorf("NextBeginOffset mismatch, expected %d, got %d", header.NextBeginOffset, header2.NextBeginOffset)
	}
	if header2.MaxOffset != header.MaxOffset {
		t.Errorf("MaxOffset mismatch, expected %d, got %d", header.MaxOffset, header2.MaxOffset)
	}
}

// BenchmarkDataVersionCreation 基准测试DataVersion创建
func BenchmarkDataVersionCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewDataVersion()
	}
}

// BenchmarkDataVersionNextVersion 基准测试DataVersion版本递增
func BenchmarkDataVersionNextVersion(b *testing.B) {
	dv := NewDataVersion()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dv = dv.NextVersion()
	}
}

// BenchmarkRemotingCommandCreation 基准测试RemotingCommand创建
func BenchmarkRemotingCommandCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = CreateRemotingCommand(SendMessage)
	}
}

// BenchmarkTopicConfigSerialization 基准测试TopicConfig序列化
func BenchmarkTopicConfigSerialization(b *testing.B) {
	tc := &TopicConfig{
		TopicName:       "TestTopic",
		ReadQueueNums:   4,
		WriteQueueNums:  4,
		Perm:            6,
		TopicFilterType: 0,
		TopicSysFlag:    0,
		Order:           false,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(tc)
	}
}