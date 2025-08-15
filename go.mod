module go-rocketmq

go 1.21

require (
	github.com/chenjy16/go-rocketmq-client v0.0.0
	golang.org/x/sys v0.15.0
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/chenjy16/go-rocketmq-client => ./pkg/client
