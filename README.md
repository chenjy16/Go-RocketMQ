# Go-RocketMQ

## Project Overview

Go-RocketMQ is a Go language implementation that provides complete message queue functionality, including message production, consumption, routing management, and other core features. The project adopts modern Go language features and offers advantages such as simple deployment, excellent performance, and low resource consumption.

### Performance Goals
- Low latency (< 1ms)
- High throughput (> 1 million TPS)
- Horizontal scaling capability
- TB-level message storage

## Architecture Design

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Producer  │    │   Producer  │    │   Producer  │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
                   ┌──────▼──────┐
                   │ NameServer  │
                   │   Cluster   │
                   └──────┬──────┘
                          │
       ┌──────────────────┼──────────────────┐
       │                  │                  │
┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
│   Broker-1  │    │   Broker-2  │    │   Broker-3  │
│   Master    │    │   Master    │    │   Master    │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
│   Broker-1  │    │   Broker-2  │    │   Broker-3  │
│    Slave    │    │    Slave    │    │    Slave    │
└─────────────┘    └─────────────┘    └─────────────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
       ┌──────────────────┼──────────────────┐
       │                  │                  │
┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
│  Consumer   │    │  Consumer   │    │  Consumer   │
│   Group-1   │    │   Group-2   │    │   Group-3   │
└─────────────┘    └─────────────┘    └─────────────┘
```

## Core Components

### 1. NameServer
- **Location**: `pkg/nameserver/nameserver.go`
- **Functions**: 
  - Manage Broker routing information
  - Provide Topic routing data queries
  - Maintain cluster topology information
  - Detect Broker health status
- **Port**: 9876 (default)

### 2. Broker (Message Broker)
- **Location**: `pkg/broker/broker.go`
- **Functions**:
  - Message storage and management
  - Handle messages sent by producers
  - Respond to consumer pull requests
  - Topic and queue management
  - Register with NameServer and send heartbeats
- **Port**: 10911 (default), 10912 (HA service)

### 3. Producer (Producer Client)
- **Location**: `pkg/client/producer.go`
- **Functions**:
  - Synchronous message sending (SendSync)
  - Asynchronous message sending (SendAsync)
  - One-way message sending (SendOneway)
  - Automatic route selection
  - Failover

### 4. Consumer (Consumer Client)
- **Location**: `pkg/client/consumer.go`
- **Functions**:
  - Topic subscription management
  - Message pulling and consumption
  - Consumption progress management
  - Load balancing
  - Consumption retry mechanism
  - Push mode and Pull mode

## Project Structure

```
go-rocketmq/
├── cmd/                    # Main program entry
│   ├── nameserver/        # NameServer service
│   └── broker/            # Broker service
├── pkg/                   # Core packages
│   ├── client/           # Client library (independent module)
│   ├── common/           # Common data structures
│   ├── nameserver/       # NameServer implementation
│   ├── broker/           # Broker implementation
│   ├── protocol/         # Communication protocol
│   ├── store/            # Storage engine
│   ├── cluster/          # Cluster management
│   ├── failover/         # Failover
│   └── ha/               # High availability
├── examples/             # Example programs
│   ├── README.md         # Example documentation
│   ├── basic/           # Basic examples
│   │   ├── producer/    # Producer basic examples
│   │   ├── consumer/    # Consumer basic examples
│   │   └── simple-demo/ # Simple demo
│   ├── advanced/        # Advanced feature examples
│   │   ├── transaction/ # Transactional messages
│   │   ├── ordered/     # Ordered messages
│   │   ├── delayed/     # Delayed messages
│   │   ├── batch/       # Batch messages
│   │   └── filter/      # Message filtering
│   ├── cluster/         # Cluster mode examples
│   │   ├── multi-broker/# Multi-Broker cluster
│   │   ├── ha/          # High availability configuration
│   │   └── load-balance/# Load balancing
│   ├── performance/     # Performance testing
│   │   ├── benchmark/   # Benchmark testing
│   │   ├── stress-test/ # Stress testing
│   │   └── monitoring/  # Monitoring examples
│   ├── integration/     # Integration examples
│   │   ├── spring-boot/ # Spring Boot integration
│   │   ├── gin/         # Gin framework integration
│   │   └── microservice/# Microservice architecture
│   └── tools/           # Tool examples
│       ├── admin/       # Admin tools
│       ├── migration/   # Data migration
│       └── monitoring/  # Monitoring tools
├── tools/                # Toolset
│   └── monitor/          # System monitoring tools
├── scripts/              # Script files
│   ├── test_system.sh    # System test script
│   └── full_test.sh      # Full test script
├── config/               # Configuration files
│   └── config.yaml       # System configuration
├── build/                # Build output directory
│   └── bin/              # Executable files
├── logs/                 # Log directory
├── docs/                 # Documentation directory
│   ├── ARCHITECTURE.md   # Architecture documentation
│   ├── QUICKSTART.md     # Quick start guide
│   ├── CLIENT_USAGE.md   # Client usage guide (Chinese)
│   └── CLIENT_USAGE_EN.md # Client usage guide (English)
├── Makefile              # Build script
├── go.mod                # Go module file
├── go.sum                # Go dependency verification
└── LICENSE               # License file
```

## Environment Requirements

- Go 1.19 or higher
- Git
- Make (optional, for build scripts)

## Installation and Build

### Use as Third-party Library (Recommended)

If you only need RocketMQ client functionality, you can directly import the independent client library:

```bash
go get github.com/chenjy16/go-rocketmq-client
```

Import in your code:
```go
import "github.com/chenjy16/go-rocketmq-client"
```

### Full Project Development

### 1. Clone the project
```bash
git clone https://github.com/your-org/go-rocketmq.git
cd go-rocketmq
```

### 2. Install dependencies
```bash
go mod tidy
```

### 3. Build the project
```bash
make build
```

Or build manually:
```bash
# Build NameServer
go build -o build/bin/nameserver ./cmd/nameserver

# Build Broker
go build -o build/bin/broker ./cmd/broker

# Build example programs
go build -o build/bin/producer-example ./examples/producer
go build -o build/bin/consumer-example ./examples/consumer
```

## Quick Start

### 1. Start NameServer
```bash
# Using Makefile
make run-nameserver

# Or run directly
./build/bin/nameserver
```

NameServer will start on port 9876.

### 2. Start Broker
In a new terminal window:
```bash
# Using Makefile
make run-broker

# Or run directly
./build/bin/broker
```

Broker will start on port 10911 and automatically register with NameServer.

### 3. Run producer example
In a new terminal window:
```bash
# Using Makefile
make run-producer

# Or run directly
./build/bin/producer-example
```

### 4. Run consumer example
In a new terminal window:
```bash
# Using Makefile
make run-consumer

# Or run directly
./build/bin/consumer-example
```

## Basic Usage Examples

### Sending Messages

```go
package main

import (
    "fmt"
    "log"
    
    "go-rocketmq/pkg/client"
    "go-rocketmq/pkg/common"
)

func main() {
    // Create producer
    producer := client.NewProducer(nil)
    producer.SetNameServerAddr("127.0.0.1:9876")
    
    // Start producer
    err := producer.Start()
    if err != nil {
        log.Fatalf("Failed to start producer: %v", err)
    }
    defer producer.Stop()
    
    // Create message
    msg := common.NewMessage("TestTopic", []byte("Hello RocketMQ!"))
    msg.SetTags("test").SetKeys("key1")
    
    // Send message
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Fatalf("Failed to send message: %v", err)
    }
    
    fmt.Printf("Message sent successfully: %s\n", result.MsgId)
}
```

### Consuming Messages

```go
package main

import (
    "log"
    "os"
    "os/signal"
    "syscall"
    
    "go-rocketmq/pkg/client"
    "go-rocketmq/pkg/common"
)

// Message listener
type MyMessageListener struct{}

func (l *MyMessageListener) ConsumeMessage(msgs []*common.MessageExt) common.ConsumeResult {
    for _, msg := range msgs {
        log.Printf("Received: %s", string(msg.Body))
    }
    return common.ConsumeSuccess
}

func main() {
    // Create consumer
    consumer := client.NewConsumer(nil)
    consumer.SetNameServerAddr("127.0.0.1:9876")
    
    // Subscribe to Topic
    listener := &MyMessageListener{}
    err := consumer.Subscribe("TestTopic", "*", listener)
    if err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }
    
    // Start consumer
    err = consumer.Start()
    if err != nil {
        log.Fatalf("Failed to start consumer: %v", err)
    }
    defer consumer.Stop()
    
    // Wait for interrupt signal
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
}
```

## Configuration Options

### NameServer Configuration
```bash
./build/bin/nameserver -port 9876
```

### Broker Configuration
```bash
./build/bin/broker \
  -port 10911 \
  -name "broker-1" \
  -cluster "DefaultCluster" \
  -nameserver "127.0.0.1:9876" \
  -store "/tmp/rocketmq-store"
```

### Producer Configuration
```go
config := &client.ProducerConfig{
    GroupName:      "ProducerGroup",
    NameServerAddr: "127.0.0.1:9876",
    SendMsgTimeout: 3 * time.Second,
    RetryTimes:     2,
}
producer := client.NewProducer(config)
```

### Consumer Configuration
```go
config := &client.ConsumerConfig{
    GroupName:        "ConsumerGroup",
    NameServerAddr:   "127.0.0.1:9876",
    ConsumeFromWhere: common.ConsumeFromLastOffset,
    MessageModel:     common.Clustering,
    ConsumeThreadMax: 4,
    PullBatchSize:    32,
}
consumer := client.NewConsumer(config)
```

## Performance Features

### Sending Performance
- **Synchronous sending**: Supports high-reliability message sending
- **Asynchronous sending**: Supports high-throughput message sending
- **One-way sending**: Supports highest-performance message sending

### Concurrency Support
- **Multi-threaded production**: Supports multiple producers sending concurrently
- **Multi-threaded consumption**: Supports multiple consumers consuming concurrently
- **Load balancing**: Automatic message queue allocation

### Monitoring Metrics
- **TPS**: Transactions per second
- **Latency**: Message sending latency
- **Throughput**: Data transfer rate
- **System resources**: CPU, memory, disk usage

### Actual Performance Test Results

#### Synchronous Sending Mode
- **Small-scale test** (100 messages, 3 concurrent):
  - Success rate: 100%
  - TPS: 7,747.59 msg/s
  - Average latency: 0.37 ms
  - Throughput: 7.57 MB/s

- **Medium-scale test** (1000 messages, 10 concurrent):
  - Success rate: 99.90%
  - TPS: 19,366.64 msg/s
  - Average latency: 0.50 ms
  - Throughput: 18.91 MB/s

#### Asynchronous Sending Mode
- **Test results** (500 messages, 5 concurrent):
  - Success rate: 87.40%
  - TPS: 218.34 msg/s
  - Throughput: 0.21 MB/s

## Common Commands

### Build Related
```bash
make build          # Build all components
make clean          # Clean build files
make test           # Run tests
```

### Runtime Related
```bash
make run-nameserver # Run NameServer
make run-broker     # Run Broker
make run-producer   # Run producer example
make run-consumer   # Run consumer example
```

### Performance Testing
```bash
# Synchronous sending performance test
make benchmark

# Asynchronous sending performance test
make benchmark-async

# One-way sending performance test
make benchmark-oneway
```

### System Monitoring
```bash
# Command line monitoring
make monitor

# Web monitoring interface
make monitor-web
```

### Automated Testing
```bash
# Run full system test
./scripts/full_test.sh
```

### Development Related
```bash
make fmt            # Format code
make vet            # Code check
make lint           # Run linter
```

## Troubleshooting

### 1. Port Conflicts
If default ports are occupied, you can specify other ports through parameters:
```bash
./build/bin/nameserver -port 9877
./build/bin/broker -port 10912 -nameserver "127.0.0.1:9877"
```

### 2. Connection Failures
Ensure NameServer is started and network connection is normal:
```bash
# Check if NameServer is running
netstat -an | grep 9876

# Check if Broker is running
netstat -an | grep 10911
```

### 3. Message Sending Failures
Check if Topic exists and Broker is running normally.

### 4. Message Consumption Exceptions
Ensure consumer group name is unique and subscription expression is correct.

## Monitoring and Logging

### View Logs
```bash
# NameServer logs
tail -f /tmp/nameserver.log

# Broker logs
tail -f /tmp/broker.log
```

### Monitoring Metrics
- Message sending TPS
- Message consumption latency
- Queue depth
- System resource usage

## Technology Stack

- **Language**: Go 1.19+
- **Network**: TCP/HTTP
- **Serialization**: JSON/Protocol Buffers
- **Storage**: File system (planned to support multiple storage backends)
- **Logging**: Standard library log (planned to integrate logrus/zap)
- **Build**: Make
- **Testing**: Go standard testing framework

## Scalability Design

### Horizontal Scaling
- NameServer is stateless, supports multi-instance deployment
- Broker supports cluster mode, can be dynamically scaled
- Clients support automatic discovery and load balancing
- Supports multiple Producer/Consumer instances

### Plugin Architecture
- Pluggable storage engines
- Extensible serialization protocols
- Customizable load balancing strategies
- Pluggable storage engines
- Configurable serialization methods
- Custom filter support

## Core Processes

### 1. System Startup Process
1. Start NameServer
2. Start Broker, register with NameServer
3. Broker periodically sends heartbeats to NameServer
4. NameServer maintains Broker health status

### 2. Message Sending Process
1. Producer gets Topic routing information from NameServer
2. Select appropriate Broker and queue
3. Send message to Broker
4. Broker stores message and returns result

### 3. Message Consumption Process
1. Consumer subscribes to Topic
2. Get routing information from NameServer
3. Send pull request to Broker
4. Process returned messages
5. Commit consumption progress

## Development Plan

### Short-term Goals
- [ ] Improve message persistence mechanism
- [ ] Implement cluster mode support
- [ ] Add message filtering functionality
- [ ] Optimize performance and stability

### Long-term Goals
- [ ] Support transactional messages
- [ ] Implement delayed messages
- [ ] Add message tracing functionality
- [ ] Support multiple storage engines
- [ ] Complete network communication protocol
- [ ] Consumption retry mechanism
- [ ] Dead letter queue
- [ ] Monitoring and management tools

## Deployment Methods

### Development Environment
```bash
# Start NameServer
make run-nameserver

# Start Broker
make run-broker

# Run producer example
make run-producer

# Run consumer example
make run-consumer
```

### Production Environment
- Support Docker containerized deployment
- Support Kubernetes cluster deployment
- Support traditional virtual machine deployment

## Example Code

This project provides rich example code located in the `examples/` directory to help developers quickly get started and understand the project's features.

### Example Directory Structure

```
examples/
├── README.md                    # Example documentation
├── basic/                       # Basic examples
│   ├── producer/               # Producer basic examples
│   ├── consumer/               # Consumer basic examples
│   └── simple-demo/            # Simple demo
├── advanced/                   # Advanced feature examples
│   ├── transaction/            # Transactional messages
│   ├── ordered/                # Ordered messages
│   ├── delayed/                # Delayed messages
│   ├── batch/                  # Batch messages
│   └── filter/                 # Message filtering
├── cluster/                    # Cluster mode examples
│   ├── multi-broker/           # Multi-Broker cluster
│   ├── ha/                     # High availability configuration
│   └── load-balance/           # Load balancing
├── performance/                # Performance testing
│   ├── benchmark/              # Benchmark testing
│   ├── stress-test/            # Stress testing
│   └── monitoring/             # Monitoring examples
├── integration/                # Integration examples
│   ├── spring-boot/            # Spring Boot integration
│   ├── gin/                    # Gin framework integration
│   └── microservice/           # Microservice architecture
└── tools/                      # Tool examples
    ├── admin/                  # Admin tools
    ├── migration/              # Data migration
    └── monitoring/             # Monitoring tools
```

### Quick Start with Examples

#### 1. Start Services

First start NameServer and Broker:

```bash
# Start NameServer
go run cmd/nameserver/main.go

# Start Broker
go run cmd/broker/main.go
```

#### 2. Run Basic Examples

```bash
# Run producer example
go run examples/basic/producer/main.go

# Run consumer example
go run examples/basic/consumer/main.go
```

#### 3. Run Complete Demo

```bash
# Run simple demo
go run examples/basic/simple-demo/main.go
```

### Example Categories

#### Basic Examples (basic/)
- **producer/**: Shows how to create producers and send messages
- **consumer/**: Shows how to create consumers and receive messages
- **simple-demo/**: Complete producer-consumer demonstration

#### Advanced Features (advanced/)
- **transaction/**: Transactional message sending and processing
- **ordered/**: Ordered message sending and consumption
- **delayed/**: Delayed message usage
- **batch/**: Batch message processing
- **filter/**: Message filtering functionality

#### Cluster Mode (cluster/)
- **multi-broker/**: Multi-Broker cluster deployment
- **ha/**: High availability configuration and failover
- **load-balance/**: Load balancing strategies

#### Performance Testing (performance/)
- **benchmark/**: Performance benchmark testing
- **stress-test/**: Stress testing tools
- **monitoring/**: Performance monitoring examples

#### Integration Examples (integration/)
- **spring-boot/**: Integration with Spring Boot
- **gin/**: Integration with Gin framework
- **microservice/**: Usage in microservice architecture

#### Tool Examples (tools/)
- **admin/**: Admin tool usage
- **migration/**: Data migration tools
- **monitoring/**: Monitoring tool configuration

### Example Requirements

- Go 1.19+
- Running NameServer (default port: 9876)
- Running Broker (default port: 10911)

### Configuration Notes

Most examples use default configuration. For custom configuration, please refer to the `config/config.yaml` file.

### Common Issues

1. **Connection failures**: Ensure NameServer and Broker are properly started
2. **Message sending failures**: Check if Topic has been created
3. **Consumer cannot receive messages**: Confirm subscribed Topic and Tags are correct

## Getting Help

- View [Architecture Design Documentation](ARCHITECTURE.md)
- Read [Quick Start Guide](QUICKSTART.md)
- View [Project Summary](PROJECT_SUMMARY.md)
- Submit [Issues](https://github.com/your-org/go-rocketmq/issues)
- Participate in [Discussions](https://github.com/your-org/go-rocketmq/discussions)

## Contributing Guidelines

We welcome all forms of contributions, including but not limited to:

1. **Code Contributions**
   - Fork the project
   - Create feature branch
   - Commit changes
   - Push to branch
   - Create Pull Request

2. **Documentation Improvements**
   - Improve existing documentation
   - Add usage examples
   - Translate documentation

3. **Issue Feedback**
   - Report bugs
   - Suggest features
   - Performance optimization suggestions

4. **Testing Contributions**
   - Write unit tests
   - Conduct integration testing
   - Performance testing

Please ensure:
- Code is clean and understandable
- Include necessary comments
- Provide running instructions
- Follow project coding standards

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

---

**Go-RocketMQ** - High-performance distributed message queue system built with Go language