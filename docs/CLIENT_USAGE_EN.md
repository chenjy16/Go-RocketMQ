# Client Library Usage Guide

## Overview

This project provides two ways to use the RocketMQ client:

1. **Independent Client Library** - Recommended for third-party project integration
2. **Local Development Mode** - Used for internal development and examples

## Independent Client Library Usage (Recommended)

### Installation

```bash
go get github.com/chenjy16/go-rocketmq-client
```

### Import

```go
import "github.com/chenjy16/go-rocketmq-client"
```

### Basic Usage Examples

#### Producer

```go
package main

import (
    "fmt"
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // Create producer
    producer, err := client.NewProducer("test_producer_group")
    if err != nil {
        log.Fatal(err)
    }
    
    // Set NameServer addresses
    producer.SetNameServers([]string{"localhost:9876"})
    
    // Start producer
    if err := producer.Start(); err != nil {
        log.Fatal(err)
    }
    defer producer.Shutdown()
    
    // Send message
    msg := &client.Message{
        Topic: "test_topic",
        Body:  []byte("Hello RocketMQ"),
    }
    
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Message sent successfully: %s\n", result.MsgId)
}
```

#### Consumer

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // Create consumer
    consumer, err := client.NewConsumer("test_consumer_group")
    if err != nil {
        log.Fatal(err)
    }
    
    // Set NameServer addresses
    consumer.SetNameServers([]string{"localhost:9876"})
    
    // Subscribe to topic
    if err := consumer.Subscribe("test_topic", "*"); err != nil {
        log.Fatal(err)
    }
    
    // Register message listener
    consumer.RegisterMessageListener(func(msgs []*client.MessageExt) client.ConsumeResult {
        for _, msg := range msgs {
            fmt.Printf("Received message: %s\n", string(msg.Body))
        }
        return client.ConsumeSuccess
    })
    
    // Start consumer
    if err := consumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer consumer.Shutdown()
    
    // Keep program running
    time.Sleep(time.Hour)
}
```

## Local Development Mode

### Use Cases

- Internal project development
- Running project examples
- Contributing code development

### Import Method

```go
import "go-rocketmq/pkg/client"
```

### Example Locations

All examples in the `examples/` directory of this project use local development mode, including:

- `examples/basic/` - Basic usage examples
- `examples/advanced/` - Advanced feature examples
- `examples/performance/` - Performance testing examples
- `examples/integration/` - Integration examples

## Selection Guidelines

### Use Independent Client Library when:

- ✅ Integrating RocketMQ functionality into your own projects
- ✅ Building microservice applications
- ✅ Developing production environment applications
- ✅ Need stable API versions

### Use Local Development Mode when:

- ✅ Learning and understanding RocketMQ implementation
- ✅ Running project example code
- ✅ Contributing code to this project
- ✅ Custom modification of client functionality

## Version Compatibility

The independent client library follows semantic versioning, and major version changes may include breaking changes. It's recommended to pin to a specific version in production environments:

```bash
go get github.com/chenjy16/go-rocketmq-client@v1.0.0
```

## Getting Help

- View [API Documentation](https://pkg.go.dev/github.com/chenjy16/go-rocketmq-client)
- Refer to [Example Code](../examples/)
- Submit [Issues](https://github.com/chenjy16/go-rocketmq-client/issues)