module go-rocketmq

go 1.21

require (
	golang.org/x/sys v0.15.0
	gopkg.in/yaml.v3 v3.0.1
)

require github.com/chenjy16/go-rocketmq-client v0.0.0-00010101000000-000000000000 // indirect

replace github.com/chenjy16/go-rocketmq-client => ./pkg/client
