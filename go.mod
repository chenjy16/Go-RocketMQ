module go-rocketmq

go 1.21

require (
	github.com/chenjy16/go-rocketmq-common v0.0.0-20250914051914-f4b24256bf66
	github.com/chenjy16/go-rocketmq-remoting v0.0.0-20250914051907-23566d90315d
	golang.org/x/sys v0.15.0
	gopkg.in/yaml.v3 v3.0.1
)


replace github.com/chenjy16/go-rocketmq-common => ./pkg/common

replace github.com/chenjy16/go-rocketmq-remoting => ./pkg/remoting
