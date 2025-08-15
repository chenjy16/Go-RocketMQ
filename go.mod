module go-rocketmq

go 1.21

require (
	github.com/chenjy16/go-rocketmq-client v0.0.0
	golang.org/x/sys v0.15.0
)

replace github.com/chenjy16/go-rocketmq-client => ./pkg/client
