package protocol

import (
	"time"
)

// DataVersion 数据版本
type DataVersion struct {
	Timestamp int64  `json:"timestamp"`
	Counter   int64  `json:"counter"`
}

// TopicConfig Topic配置
type TopicConfig struct {
	TopicName       string `json:"topicName"`
	ReadQueueNums   int32  `json:"readQueueNums"`
	WriteQueueNums  int32  `json:"writeQueueNums"`
	Perm            int32  `json:"perm"`
	TopicFilterType int32  `json:"topicFilterType"`
	TopicSysFlag    int32  `json:"topicSysFlag"`
	Order           bool   `json:"order"`
}

// TopicConfigSerializeWrapper Topic配置序列化包装器
type TopicConfigSerializeWrapper struct {
	TopicConfigTable map[string]*TopicConfig `json:"topicConfigTable"`
	DataVersion      *DataVersion            `json:"dataVersion"`
}

// RegisterBrokerResult Broker注册结果
type RegisterBrokerResult struct {
	HaServerAddr   string `json:"haServerAddr"`
	MasterAddr     string `json:"masterAddr"`
}

// TopicRouteData Topic路由数据
type TopicRouteData struct {
	OrderTopicConf    string                   `json:"orderTopicConf"`
	QueueDatas        []*QueueData             `json:"queueDatas"`
	BrokerDatas       []*BrokerData            `json:"brokerDatas"`
	FilterServerTable map[string][]string      `json:"filterServerTable"`
}

// QueueData 队列数据
type QueueData struct {
	BrokerName     string `json:"brokerName"`
	ReadQueueNums  int32  `json:"readQueueNums"`
	WriteQueueNums int32  `json:"writeQueueNums"`
	Perm           int32  `json:"perm"`
	TopicSysFlag   int32  `json:"topicSysFlag"`
}

// BrokerData Broker数据
type BrokerData struct {
	Cluster     string            `json:"cluster"`
	BrokerName  string            `json:"brokerName"`
	BrokerAddrs map[int64]string  `json:"brokerAddrs"`
}

// ClusterInfo 集群信息
type ClusterInfo struct {
	BrokerAddrTable  map[string]map[int64]string `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string         `json:"clusterAddrTable"`
}

// RequestCode 请求码
type RequestCode int32

const (
	// NameServer相关
	UpdateAndCreateTopic RequestCode = 17
	GetRouteInfoByTopic  RequestCode = 105
	GetBrokerClusterInfo RequestCode = 106
	RegisterBroker       RequestCode = 103
	UnregisterBroker     RequestCode = 104

	// Producer相关
	SendMessage         RequestCode = 10
	SendMessageV2       RequestCode = 310
	SendBatchMessage    RequestCode = 320

	// Consumer相关
	PullMessage         RequestCode = 11
	QueryMessage        RequestCode = 12
	QueryMessageByKey   RequestCode = 33
	QueryMessageById    RequestCode = 34

	// Transaction相关
	EndTransaction      RequestCode = 37
	CheckTransactionState RequestCode = 39

	// Admin相关
	UpdateBrokerConfig  RequestCode = 25
	GetBrokerConfig     RequestCode = 26
)

// ResponseCode 响应码
type ResponseCode int32

const (
	Success                ResponseCode = 0
	SystemError           ResponseCode = 1
	SystemBusy            ResponseCode = 2
	RequestCodeNotSupported ResponseCode = 3
	TransactionFailed     ResponseCode = 4
	FlushDiskTimeout          ResponseCode = 10
	SlaveNotAvailable         ResponseCode = 11
	FlushSlaveTimeout         ResponseCode = 12
	MessageIllegal            ResponseCode = 13
	ServiceNotAvailable       ResponseCode = 14
	VersionNotSupported       ResponseCode = 15
	NoPermission              ResponseCode = 16
	TopicNotExist             ResponseCode = 17
	TopicExistAlready         ResponseCode = 18
	PullNotFound              ResponseCode = 19
	PullRetryImmediately      ResponseCode = 20
	PullOffsetMoved           ResponseCode = 21
	QueryNotFound             ResponseCode = 22
	SubscriptionParseFailed   ResponseCode = 23
	SubscriptionNotExist      ResponseCode = 24
	SubscriptionNotLatest     ResponseCode = 25
	SubscriptionGroupNotExist ResponseCode = 26
	TransactionShouldCommit   ResponseCode = 200
	TransactionShouldRollback ResponseCode = 201
	TransactionStateUnknown   ResponseCode = 202
	TransactionStateGroupWrong ResponseCode = 203
	NoBuyerId                 ResponseCode = 204
)

// SubscriptionData 订阅数据
type SubscriptionData struct {
	Topic           string   `json:"topic"`
	SubString       string   `json:"subString"`
	TagsSet         []string `json:"tagsSet"`
	CodeSet         []int32  `json:"codeSet"`
	SubVersion      int64    `json:"subVersion"`
	ExpressionType  string   `json:"expressionType"`
}

// RemotingCommand 远程调用命令
type RemotingCommand struct {
	Code      RequestCode           `json:"code"`
	Language  string                `json:"language"`
	Version   int32                 `json:"version"`
	Opaque    int32                 `json:"opaque"`
	Flag      int32                 `json:"flag"`
	Remark    string                `json:"remark"`
	ExtFields map[string]string     `json:"extFields"`
	Body      []byte                `json:"body"`
}

// SendMessageRequestHeader 发送消息请求头
type SendMessageRequestHeader struct {
	ProducerGroup         string `json:"producerGroup"`
	Topic                 string `json:"topic"`
	DefaultTopic          string `json:"defaultTopic"`
	DefaultTopicQueueNums int32  `json:"defaultTopicQueueNums"`
	QueueId               int32  `json:"queueId"`
	SysFlag               int32  `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	Flag                  int32  `json:"flag"`
	Properties            string `json:"properties"`
	ReconsumeTimes        int32  `json:"reconsumeTimes"`
	UnitMode              bool   `json:"unitMode"`
	Batch                 bool   `json:"batch"`
}

// SendMessageResponseHeader 发送消息响应头
type SendMessageResponseHeader struct {
	MsgId         string `json:"msgId"`
	QueueId       int32  `json:"queueId"`
	QueueOffset   int64  `json:"queueOffset"`
	TransactionId string `json:"transactionId"`
	BatchUniqId   string `json:"batchUniqId"`
}

// PullMessageRequestHeader 拉取消息请求头
type PullMessageRequestHeader struct {
	ConsumerGroup        string `json:"consumerGroup"`
	Topic                string `json:"topic"`
	QueueId              int32  `json:"queueId"`
	QueueOffset          int64  `json:"queueOffset"`
	MaxMsgNums           int32  `json:"maxMsgNums"`
	SysFlag              int32  `json:"sysFlag"`
	CommitOffset         int64  `json:"commitOffset"`
	SuspendTimeoutMillis int64  `json:"suspendTimeoutMillis"`
	Subscription         string `json:"subscription"`
	SubVersion           int64  `json:"subVersion"`
	ExpressionType       string `json:"expressionType"`
}

// PullMessageResponseHeader 拉取消息响应头
type PullMessageResponseHeader struct {
	SuggestWhichBrokerId int64 `json:"suggestWhichBrokerId"`
	NextBeginOffset      int64 `json:"nextBeginOffset"`
	MinOffset            int64 `json:"minOffset"`
	MaxOffset            int64 `json:"maxOffset"`
}

// NewDataVersion 创建新的数据版本
func NewDataVersion() *DataVersion {
	return &DataVersion{
		Timestamp: time.Now().UnixMilli(),
		Counter:   0,
	}
}

// NextVersion 获取下一个版本
func (dv *DataVersion) NextVersion() *DataVersion {
	return &DataVersion{
		Timestamp: time.Now().UnixMilli(),
		Counter:   dv.Counter + 1,
	}
}

// CreateRemotingCommand 创建远程调用命令
func CreateRemotingCommand(code RequestCode) *RemotingCommand {
	return &RemotingCommand{
		Code:      code,
		Language:  "GO",
		Version:   1,
		Flag:      0,
		ExtFields: make(map[string]string),
	}
}

// CreateResponseCommand 创建响应命令
func CreateResponseCommand(code ResponseCode, remark string) *RemotingCommand {
	return &RemotingCommand{
		Code:      RequestCode(code),
		Language:  "GO",
		Version:   1,
		Flag:      1, // 响应标志
		Remark:    remark,
		ExtFields: make(map[string]string),
	}
}