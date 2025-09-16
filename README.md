# Go-RocketMQ

Go-RocketMQ is a Go implementation of the Apache RocketMQ messaging system, designed for high-performance, reliable, and scalable message queuing.

## Features

- **High Performance**: Optimized for low latency and high throughput
- **Reliable Messaging**: Ensures message delivery with various delivery guarantees
- **Scalable Architecture**: Supports horizontal scaling with multiple brokers and nameservers
- **Rich Messaging Models**: Supports publish/subscribe, point-to-point, and request/reply messaging patterns
- **Message Filtering**: Supports SQL92-based message filtering
- **Transaction Messages**: Supports distributed transaction messages
- **Delay Messages**: Supports delayed message delivery
- **Order Messages**: Supports ordered message delivery
- **Security**: TLS encryption, ACL-based access control, and authentication
- **Performance Optimizations**: Memory pooling, batch processing, connection pooling, and compression

## Architecture

```
+------------------+     +------------------+     +------------------+
|    Producer      |     |   NameServer     |     |     Consumer     |
|                  |     |                  |     |                  |
| +--------------+ |     | +--------------+ |     | +--------------+ |
| | MessageQueue | |     | | RouteInfoMgr | |     | | MessageQueue | |
| +--------------+ |     | +--------------+ |     | +--------------+ |
|        |         |     |        |         |     |        |         |
| +--------------+ |     | +--------------+ |     | +--------------+ |
| | Communication| |<--->| | Communication| |<--->| | Communication| |
| +--------------+ |     | +--------------+ |     | +--------------+ |
|        |         |     |        |         |     |        |         |
+--------|---------+     +--------|---------+     +--------|---------+
         |                    |    |                    |
         |                    |    |                    |
         |              +-----v----v-----+              |
         |              |     Broker     |              |
         |              |                |              |
         |              | +------------+ |              |
         |              | | CommitLog  | |              |
         |              | +------------+ |              |
         |              | +------------+ |              |
         |              | | ConsumeQueue| |              |
         |              | +------------+ |              |
         |              | +------------+ |              |
         |              | | IndexFile  | |              |
         |              | +------------+ |              |
         |              +----------------+              |
         |                    |    |                    |
         |                    |    |                    |
         +--------------------+    +--------------------+
```

## Project Structure

```
go-rocketmq/
├── cmd/                    # Main program entry
│   ├── nameserver/        # NameServer service
│   └── broker/            # Broker service
├── pkg/                   # Core packages
│   ├── acl/              # Access control lists
│   ├── broker/           # Broker implementation
│   ├── client/           # Client library (independent module)
│   ├── cluster/          # Cluster management
│   ├── common/           # Common data structures (submodule)
│   ├── config/           # Configuration management
│   ├── failover/         # Failover mechanisms
│   ├── ha/               # High availability
│   ├── performance/      # Performance optimization features
│   ├── remoting/         # Network communication layer (submodule)
│   └── store/            # Storage engine
├── config/               # Configuration files
│   ├── config.yaml       # System configuration
│   └── plain_acl.yml     # ACL configuration
├── docs/                 # Documentation directory
│   ├── CLIENT_USAGE.md   # Client usage guide (Chinese)
│   ├── CLIENT_USAGE_EN.md # Client usage guide (English)
│   ├── PERFORMANCE_OPTIMIZATION.md # Performance optimization guide
│   └── TLS_SECURITY.md   # TLS security guide
├── examples/             # Example programs (not in repository)
└── tools/                # Utility tools
```

## Security Features

Go-RocketMQ provides comprehensive security features to protect your messaging system:

### TLS/SSL Encryption
All network communications can be encrypted using TLS/SSL to protect data in transit. See [TLS Security Guide](file:///Volumes/ssd/golangwork/Go-RocketMQ/docs/TLS_SECURITY.md) for detailed configuration.

### Access Control Lists (ACL)
Fine-grained access control based on user accounts with support for:
- IP address filtering (whitelist/blacklist)
- Topic and group permissions
- Rate limiting
- Resource quotas
- Account expiration
- Audit logging

### Authentication
HMAC-SHA1 signature-based authentication to verify the identity of clients.

## Performance Optimizations

Go-RocketMQ includes several performance optimizations to ensure high throughput and low latency:

### Memory Pool Management
Reduces garbage collection pressure through buffer and object pooling.

### Batch Processing
Improves throughput by batching messages for producers, consumers, and storage operations.

### Connection Pool Optimization
Efficient network connection reuse with health monitoring and automatic cleanup.

### Compression Transmission
Reduces network bandwidth usage through GZIP compression for large messages.

## Quick Start

### Prerequisites

- Go 1.16 or higher
- Make

### Building

```bash
# Clone the repository
git clone https://github.com/yourusername/go-rocketmq.git
cd go-rocketmq

# Initialize submodules
git submodule update --init --recursive

# Build all components
make build

# Or build specific components
make build-nameserver
make build-broker
```

### Running NameServer

```bash
# Start NameServer
./bin/nameserver

# Or with custom configuration
./bin/nameserver -c config/config.yaml
```

### Running Broker

```bash
# Start Broker
./bin/broker

# Or with custom configuration
./bin/broker -c config/config.yaml
```

### Configuration

The system can be configured through the `config/config.yaml` file. Key configuration options include:

```yaml
# NameServer configuration
nameserver:
  listen_port: 9876
  log_level: "info"

# Broker configuration  
broker:
  broker_name: "DefaultBroker"
  broker_id: 0
  listen_port: 10911
  nameserver_addr: "127.0.0.1:9876"
  
  # Message store configuration
  message_store:
    commit_log_file_size: 1073741824  # 1GB
    flush_disk_type: "ASYNC_FLUSH"
    
  # Performance configuration
  performance:
    send_message_thread_pool_nums: 16
```

### Security Configuration

Enable TLS and ACL for secure deployments:

```yaml
# TLS configuration
nameserver:
  tls:
    enable: true
    cert_file: "/path/to/cert.pem"
    key_file: "/path/to/key.pem"

# ACL configuration
nameserver:
  acl_enable: true
  acl_config_file: "config/plain_acl.yml"
```

## Usage Examples

### Producer

```go
// Create producer configuration
config := &producer.Config{
    NamesrvAddr: "127.0.0.1:9876",
    // TLS and ACL configuration can be added here
}

// Create producer instance
p := producer.NewProducer(config)

// Start producer
err := p.Start()
if err != nil {
    log.Fatal("Failed to start producer:", err)
}

// Send message
msg := &message.Message{
    Topic: "TestTopic",
    Body:  []byte("Hello, RocketMQ!"),
}

result, err := p.SendSync(msg)
if err != nil {
    log.Fatal("Failed to send message:", err)
}

fmt.Printf("Message sent successfully, msgId: %s\n", result.MsgID)
```

### Consumer

```go
// Create consumer configuration
config := &consumer.Config{
    NamesrvAddr:    "127.0.0.1:9876",
    ConsumerGroup:  "TestGroup",
    // TLS and ACL configuration can be added here
}

// Create consumer instance
c := consumer.NewConsumer(config)

// Subscribe to topic
err := c.Subscribe("TestTopic", consumer.MessageSelector{}, func(msg *message.MessageExt) error {
    fmt.Printf("Received message: %s\n", string(msg.Body))
    return nil
})
if err != nil {
    log.Fatal("Failed to subscribe:", err)
}

// Start consumer
err = c.Start()
if err != nil {
    log.Fatal("Failed to start consumer:", err)
}

// Keep the consumer running
select {}
```

## Documentation

- [Client Usage Guide (Chinese)](file:///Volumes/ssd/golangwork/Go-RocketMQ/docs/CLIENT_USAGE.md)
- [Client Usage Guide (English)](file:///Volumes/ssd/golangwork/Go-RocketMQ/docs/CLIENT_USAGE_EN.md)
- [Performance Optimization Guide](file:///Volumes/ssd/golangwork/Go-RocketMQ/docs/PERFORMANCE_OPTIMIZATION.md)
- [TLS Security Guide](file:///Volumes/ssd/golangwork/Go-RocketMQ/docs/TLS_SECURITY.md)
- [Enhancements Summary](file:///Volumes/ssd/golangwork/Go-RocketMQ/ENHANCEMENTS_SUMMARY.md)

## Testing

```bash
# Run all tests
make test

# Run specific test suites
make test-nameserver
make test-broker
make test-store
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](file:///Volumes/ssd/golangwork/Go-RocketMQ/LICENSE) file for details.

## Acknowledgments

- Apache RocketMQ for the original design and implementation
- All contributors who have helped with the development of this project
