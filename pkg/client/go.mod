module github.com/chenjy16/go-rocketmq-client

go 1.21

require (
	go-rocketmq/pkg/common v0.0.0-00010101000000-000000000000
	go-rocketmq/pkg/remoting v0.0.0-00010101000000-000000000000
)

replace (
	go-rocketmq/pkg/common => ../common
	go-rocketmq/pkg/remoting => ../remoting
)
