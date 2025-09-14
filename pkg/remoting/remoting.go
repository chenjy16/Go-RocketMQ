package remoting

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/chenjy16/go-rocketmq-remoting/client"
	"github.com/chenjy16/go-rocketmq-remoting/server"
)

// ========== PROTOCOL DEFINITIONS (merged from protocol package) ==========

// DataVersion 数据版本
type DataVersion struct {
	Timestamp int64 `json:"timestamp"`
	Counter   int64 `json:"counter"`
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
	HaServerAddr string `json:"haServerAddr"`
	MasterAddr   string `json:"masterAddr"`
}

// TopicRouteData Topic路由数据
type TopicRouteData struct {
	OrderTopicConf    string              `json:"orderTopicConf"`
	QueueDatas        []*QueueData        `json:"queueDatas"`
	BrokerDatas       []*BrokerData       `json:"brokerDatas"`
	FilterServerTable map[string][]string `json:"filterServerTable"`
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
	Cluster     string           `json:"cluster"`
	BrokerName  string           `json:"brokerName"`
	BrokerAddrs map[int64]string `json:"brokerAddrs"`
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
	UpdateAndCreateTopic        RequestCode = 17
	GetRouteInfoByTopic         RequestCode = 105
	GetBrokerClusterInfo        RequestCode = 106
	RegisterBroker              RequestCode = 103
	UnregisterBroker            RequestCode = 104
	GetAllTopicConfig           RequestCode = 21
	GetTopicConfigList          RequestCode = 22
	GetTopicNameList            RequestCode = 23
	UpdateAndCreateTopicRequest RequestCode = 24

	// Producer相关
	SendMessage         RequestCode = 10
	SendMessageV2       RequestCode = 310
	SendBatchMessage    RequestCode = 320
	ConsumerSendMsgBack RequestCode = 36

	// Consumer相关
	PullMessage                  RequestCode = 11
	QueryMessage                 RequestCode = 12
	QueryMessageByKey            RequestCode = 33
	QueryMessageById             RequestCode = 34
	UpdateConsumerOffset         RequestCode = 34 // Same as QueryMessageById
	UpdateConsumerOffsetInBroker RequestCode = 35
	ConsumeMessageDirectly       RequestCode = 38

	// Transaction相关
	EndTransaction           RequestCode = 37
	CheckTransactionState    RequestCode = 39
	NotifyConsumerIdsChanged RequestCode = 40
	CheckClientConfig        RequestCode = 41

	// Admin相关
	UpdateBrokerConfig                RequestCode = 25
	GetBrokerConfig                   RequestCode = 26
	SearchOffsetByTimestamp           RequestCode = 29
	GetMaxOffset                      RequestCode = 30
	GetMinOffset                      RequestCode = 31
	GetEarliestMsgStoretime           RequestCode = 32
	ViewMessageById                   RequestCode = 43
	HeartBeat                         RequestCode = 31 // Same as GetMinOffset
	UNREGISTER_CLIENT                 RequestCode = 35 // Same as UpdateConsumerOffsetInBroker
	GET_CONSUMER_LIST_BY_GROUP        RequestCode = 44
	CHECK_TRANSACTION_STATE           RequestCode = 39 // Same as CheckTransactionState
	NOTIFY_CONSUMER_IDS_CHANGED       RequestCode = 40 // Same as NotifyConsumerIdsChanged
	LOCK_BATCH_MQ                     RequestCode = 45
	UNLOCK_BATCH_MQ                   RequestCode = 46
	GET_ALL_TOPIC_CONFIG              RequestCode = 21 // Same as GetAllTopicConfig
	GET_ALL_SUBSCRIPTION_GROUP_CONFIG RequestCode = 47
	UPDATE_SUBSCRIPTION_GROUP_CONFIG  RequestCode = 48
	DELETE_SUBSCRIPTION_GROUP_CONFIG  RequestCode = 49
	GET_TOPIC_STATS_INFO              RequestCode = 50
	GET_CONSUMER_CONNECTION_LIST      RequestCode = 51
	GET_PRODUCER_CONNECTION_LIST      RequestCode = 52
	WIPE_WRITE_PERM_OF_BROKER         RequestCode = 53
	GET_ALL_CONSUMER_OFFSET           RequestCode = 54
	GET_ALL_DELAY_OFFSET              RequestCode = 55
	CHECK_CLIENT_CONFIG               RequestCode = 41 // Same as CheckClientConfig
	GET_CLIENT_CONFIG                 RequestCode = 56
	UPDATE_AND_CREATE_ACL_CONFIG      RequestCode = 57
	DELETE_ACL_CONFIG                 RequestCode = 58
	GET_BROKER_CLUSTER_ACL_INFO       RequestCode = 59
	UPDATE_GLOBAL_WHITE_ADDRS_CONFIG  RequestCode = 60
	RESUME_CHECK_HALF_MESSAGE         RequestCode = 61
	SEND_REPLY_MESSAGE                RequestCode = 62
	SEND_REPLY_MESSAGE_V2             RequestCode = 63
	PUSH_REPLY_MESSAGE_TO_CLIENT      RequestCode = 64
	ADD_WRITE_PERM_OF_BROKER          RequestCode = 65
	INVOKE_BROKER_TO_CLIENT           RequestCode = 66
)

// ResponseCode 响应码
type ResponseCode int32

const (
	Success                   ResponseCode = 0
	SystemError               ResponseCode = 1
	SystemBusy                ResponseCode = 2
	RequestCodeNotSupported   ResponseCode = 3
	TransactionFailed         ResponseCode = 4
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

	// Additional response codes
	ConsumerNotOnline            ResponseCode = 27
	ConsumeMsgConcurrentlyFailed ResponseCode = 28
	NoMessageFound               ResponseCode = 29
	OffsetReset                  ResponseCode = 30
	TopicAuthorizationFailed     ResponseCode = 31
	GroupAuthorizationFailed     ResponseCode = 32
	ClientAuthorizationFailed    ResponseCode = 33

	TransactionShouldCommit    ResponseCode = 200
	TransactionShouldRollback  ResponseCode = 201
	TransactionStateUnknown    ResponseCode = 202
	TransactionStateGroupWrong ResponseCode = 203
	NoBuyerId                  ResponseCode = 204

	// ACL related response codes
	AclVerificationFailed    ResponseCode = 401
	AclAuthorizationFailed   ResponseCode = 402
	AclResourceNotFound      ResponseCode = 403
	AclResourceAlreadyExists ResponseCode = 404
)

// SubscriptionData 订阅数据
type SubscriptionData struct {
	Topic          string   `json:"topic"`
	SubString      string   `json:"subString"`
	TagsSet        []string `json:"tagsSet"`
	CodeSet        []int32  `json:"codeSet"`
	SubVersion     int64    `json:"subVersion"`
	ExpressionType string   `json:"expressionType"`
}

// RemotingCommand 远程调用命令
type RemotingCommand struct {
	Code      RequestCode       `json:"code"`
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

	// ACL认证字段
	AccessKey string `json:"accessKey,omitempty"`
	Signature string `json:"signature,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
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

// UpdateConsumerOffsetRequestHeader 更新消费者偏移量请求头
type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
	CommitOffset  int64  `json:"commitOffset"`
}

// GetConsumerListByGroupRequestHeader 根据消费者组获取消费者列表请求头
type GetConsumerListByGroupRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

// GetConsumerListByGroupResponseBody 根据消费者组获取消费者列表响应体
type GetConsumerListByGroupResponseBody struct {
	ConsumerIdList []string `json:"consumerIdList"`
}

// LockBatchMQRequestHeader 批量锁定消息队列请求头
type LockBatchMQRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	ClientId      string `json:"clientId"`
}

// LockBatchMQRequestBody 批量锁定消息队列请求体
type LockBatchMQRequestBody struct {
	MqSet []*MessageQueue `json:"mqSet"`
}

// MessageQueue 消息队列信息
type MessageQueue struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueId    int32  `json:"queueId"`
}

// UnlockBatchMQRequestHeader 批量解锁消息队列请求头
type UnlockBatchMQRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	ClientId      string `json:"clientId"`
}

// UnlockBatchMQRequestBody 批量解锁消息队列请求体
type UnlockBatchMQRequestBody struct {
	MqSet []*MessageQueue `json:"mqSet"`
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

// ========== REMOTING DEFINITIONS ==========

// RemotingClient RocketMQ远程通信客户端接口
type RemotingClient struct {
	connections  sync.Map // map[string]*Connection
	requestTable sync.Map // map[int32]*ResponseFuture
	opaque       int32    // 请求序列号
	closed       int32    // 关闭标志
	ctx          context.Context
	cancel       context.CancelFunc
}

// ResponseCallback 响应回调
type ResponseCallback func(*RemotingCommand, error)

// Connection TCP连接封装
type Connection struct {
	addr     string
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	mutex    sync.RWMutex
	lastUsed time.Time
	closed   bool
}

// RemotingError 远程调用错误
type RemotingError struct {
	Code    int
	Message string
	Err     error
}

// Error 实现error接口
func (e *RemotingError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("RemotingError[code=%d, message=%s]: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("RemotingError[code=%d, message=%s]", e.Code, e.Message)
}

// Unwrap 返回底层错误
func (e *RemotingError) Unwrap() error {
	return e.Err
}

// NewRemotingError 创建远程调用错误
func NewRemotingError(code int, message string, err error) *RemotingError {
	return &RemotingError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// ServerConnection 服务端连接类型别名
type ServerConnection = server.ServerConnection

// RequestProcessor 请求处理器接口类型别名
type RequestProcessor = server.RequestProcessor

// RequestProcessorFunc 请求处理器函数类型别名
type RequestProcessorFunc = server.RequestProcessorFunc

// RemotingClientWrapper 包装器，用于暴露client.RemotingClient的方法
type RemotingClientWrapper struct {
	client *client.RemotingClient
}

// NewRemotingClient 创建远程通信客户端
func NewRemotingClient() *RemotingClientWrapper {
	return &RemotingClientWrapper{
		client: client.NewRemotingClient(),
	}
}

// convertToClientCommand converts remoting.RemotingCommand to client.RemotingCommand
func convertToClientCommand(cmd *RemotingCommand) *client.RemotingCommand {
	if cmd == nil {
		return nil
	}
	return &client.RemotingCommand{
		Code:      int32(cmd.Code),
		Language:  cmd.Language,
		Version:   cmd.Version,
		Opaque:    cmd.Opaque,
		Flag:      cmd.Flag,
		Remark:    cmd.Remark,
		ExtFields: cmd.ExtFields,
		Body:      cmd.Body,
	}
}

// convertFromClientCommand converts client.RemotingCommand to remoting.RemotingCommand
func convertFromClientCommand(cmd *client.RemotingCommand) *RemotingCommand {
	if cmd == nil {
		return nil
	}
	return &RemotingCommand{
		Code:      RequestCode(cmd.Code),
		Language:  cmd.Language,
		Version:   cmd.Version,
		Opaque:    cmd.Opaque,
		Flag:      cmd.Flag,
		Remark:    cmd.Remark,
		ExtFields: cmd.ExtFields,
		Body:      cmd.Body,
	}
}

// SendSync 同步发送请求
func (rcw *RemotingClientWrapper) SendSync(addr string, request *RemotingCommand, timeoutMs int64) (*RemotingCommand, error) {
	// Convert remoting command to client command
	clientRequest := convertToClientCommand(request)

	// Send request
	clientResponse, err := rcw.client.SendSync(addr, clientRequest, timeoutMs)
	if err != nil {
		return nil, err
	}

	// Convert response back to remoting command
	return convertFromClientCommand(clientResponse), nil
}

// Close 关闭客户端
func (rcw *RemotingClientWrapper) Close() {
	rcw.client.Close()
}

// RemotingServer 服务端类型别名
type RemotingServer = server.RemotingServer

// NewRemotingServer 创建远程通信服务端
func NewRemotingServer(listenPort int) *RemotingServer {
	return server.NewRemotingServer(listenPort)
}

// ProtocolProcessor 协议处理器接口
type ProtocolProcessor interface {
	RegisterProcessor(code RequestCode, processor interface{})
	UnregisterProcessor(requestCode RequestCode)
	ProcessRequest(ctx context.Context, conn *ServerConnection, request *RemotingCommand) (*RemotingCommand, error)
}

// NewProtocolProcessor 创建协议处理器
func NewProtocolProcessor() ProtocolProcessor {
	// 由于循环依赖，这里返回nil，实际使用时应该从processor包创建
	return nil
}
