package client

import "time"

// TopicRouteData Topic路由数据
type TopicRouteData struct {
	OrderTopicConf string       `json:"orderTopicConf"`
	QueueDatas     []*QueueData `json:"queueDatas"`
	BrokerDatas    []*BrokerData `json:"brokerDatas"`
	FilterServerTable map[string][]string `json:"filterServerTable"`
}

// QueueData 队列数据
type QueueData struct {
	BrokerName     string `json:"brokerName"`
	ReadQueueNums  int32  `json:"readQueueNums"`
	WriteQueueNums int32  `json:"writeQueueNums"`
	Perm           int32  `json:"perm"`
	TopicSynFlag   int32  `json:"topicSynFlag"`
}

// BrokerData Broker数据
type BrokerData struct {
	Cluster     string            `json:"cluster"`
	BrokerName  string            `json:"brokerName"`
	BrokerAddrs map[int64]string  `json:"brokerAddrs"`
}

// TopicConfig Topic配置
type TopicConfig struct {
	TopicName       string `json:"topicName"`
	ReadQueueNums   int32  `json:"readQueueNums"`
	WriteQueueNums  int32  `json:"writeQueueNums"`
	Perm            int32  `json:"perm"`
	TopicFilterType string `json:"topicFilterType"`
	TopicSysFlag    int32  `json:"topicSysFlag"`
	Order           bool   `json:"order"`
}

// RemotingCommand 远程命令
type RemotingCommand struct {
	Code      int32             `json:"code"`
	Language  string            `json:"language"`
	Version   int32             `json:"version"`
	Opaque    int32             `json:"opaque"`
	Flag      int32             `json:"flag"`
	Remark    string            `json:"remark"`
	ExtFields map[string]string `json:"extFields"`
	Body      []byte            `json:"body"`
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
}

// PullMessageResponseHeader 拉取消息响应头
type PullMessageResponseHeader struct {
	SuggestWhichBrokerId int64 `json:"suggestWhichBrokerId"`
	NextBeginOffset      int64 `json:"nextBeginOffset"`
	MinOffset            int64 `json:"minOffset"`
	MaxOffset            int64 `json:"maxOffset"`
}

// 请求码常量
const (
	// Producer相关
	SEND_MESSAGE         = 10
	SEND_MESSAGE_V2      = 310
	SEND_BATCH_MESSAGE   = 320
	
	// Consumer相关
	PULL_MESSAGE         = 11
	QUERY_CONSUMER_OFFSET = 14
	UPDATE_CONSUMER_OFFSET = 15
	
	// NameServer相关
	GET_ROUTEINTO_BY_TOPIC = 105
	REGISTER_BROKER        = 103
	UNREGISTER_BROKER      = 104
	
	// 心跳相关
	HEART_BEAT = 34
)

// 响应码常量
const (
	SUCCESS                    = 0
	SYSTEM_ERROR              = 1
	SYSTEM_BUSY               = 2
	REQUEST_CODE_NOT_SUPPORTED = 3
	TRANSACTION_FAILED        = 4
)

// CreateRemotingCommand 创建远程命令
func CreateRemotingCommand(code int32) *RemotingCommand {
	return &RemotingCommand{
		Code:      code,
		Language:  "GO",
		Version:   1,
		Opaque:    int32(time.Now().UnixNano()),
		Flag:      0,
		ExtFields: make(map[string]string),
	}
}

// CreateRequestCommand 创建请求命令
func CreateRequestCommand(code int32, header interface{}) *RemotingCommand {
	cmd := CreateRemotingCommand(code)
	cmd.Flag = 0 // 请求标志
	return cmd
}

// CreateResponseCommand 创建响应命令
func CreateResponseCommand(code int32, remark string) *RemotingCommand {
	cmd := CreateRemotingCommand(code)
	cmd.Flag = 1 // 响应标志
	cmd.Remark = remark
	return cmd
}