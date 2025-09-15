module go-rocketmq

go 1.21

require (
	github.com/chenjy16/go-rocketmq-common v0.0.0-20250914051914-f4b24256bf66
	github.com/chenjy16/go-rocketmq-remoting v0.0.0-20250914051907-23566d90315d
	golang.org/x/sys v0.15.0
	gopkg.in/yaml.v3 v3.0.1
)

// These dependencies are managed as Git submodules.
// The github.com/chenjy16/go-rocketmq-common and github.com/chenjy16/go-rocketmq-remoting
// modules are integrated as submodules in the pkg/common and pkg/remoting directories.

replace github.com/chenjy16/go-rocketmq-remoting => ./pkg/remoting

replace github.com/chenjy16/go-rocketmq-common => ./pkg/common
