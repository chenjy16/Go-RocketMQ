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

##### Basic Producer

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
    
    // Create message
    msg := &client.Message{
        Topic: "test_topic",
        Body:  []byte("Hello RocketMQ"),
    }
    
    // Send message
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Message sent successfully: %s\n", result.MsgID)
}
```

##### Enable Message Tracing

```go
package main

import (
    "fmt"
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // Create producer
    producer, err := client.NewProducer("trace_producer_group")
    if err != nil {
        log.Fatal(err)
    }
    
    producer.SetNameServers([]string{"localhost:9876"})
    
    // Enable message tracing
    producer.EnableTrace("trace_topic", "producer_instance")
    
    if err := producer.Start(); err != nil {
        log.Fatal(err)
    }
    defer producer.Shutdown()
    
    // Send message with tracing
    msg := &client.Message{
        Topic: "test_topic",
        Body:  []byte("Hello RocketMQ with Trace"),
    }
    
    result, err := producer.SendSync(msg)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Message with tracing sent successfully: %s\n", result.MsgID)
}
```

##### Batch Message Sending

```go
package main

import (
    "fmt"
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // Create producer
    producer, err := client.NewProducer("batch_producer_group")
    if err != nil {
        log.Fatal(err)
    }
    
    producer.SetNameServers([]string{"localhost:9876"})
    
    if err := producer.Start(); err != nil {
        log.Fatal(err)
    }
    defer producer.Shutdown()
    
    // Create batch messages
    var messages []*client.Message
    for i := 0; i < 10; i++ {
        msg := &client.Message{
            Topic: "test_topic",
            Body:  []byte(fmt.Sprintf("Batch message %d", i)),
        }
        messages = append(messages, msg)
    }
    
    // Send batch messages
    result, err := producer.SendBatchMessages(messages)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Batch messages sent successfully: %s\n", result.MsgID)
}
```

##### Transaction Messages

```go
package main

import (
    "fmt"
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

// Implement transaction listener
type MyTransactionListener struct{}

func (l *MyTransactionListener) ExecuteLocalTransaction(msg *client.Message, arg interface{}) client.LocalTransactionState {
    // Execute local transaction logic
    fmt.Printf("Execute local transaction: %s\n", string(msg.Body))
    
    // Return transaction state based on business logic
    return client.CommitMessage // or client.RollbackMessage
}

func (l *MyTransactionListener) CheckLocalTransaction(msgExt *client.MessageExt) client.LocalTransactionState {
    // Check local transaction state
    fmt.Printf("Check local transaction: %s\n", msgExt.MsgId)
    
    // Return transaction state based on business logic
    return client.CommitMessage
}

func main() {
    // Create transaction listener
    listener := &MyTransactionListener{}
    
    // Create transaction producer
    txProducer, err := client.NewTransactionProducer("tx_producer_group", listener)
    if err != nil {
        log.Fatal(err)
    }
    
    txProducer.SetNameServers([]string{"localhost:9876"})
    
    if err := txProducer.Start(); err != nil {
        log.Fatal(err)
    }
    defer txProducer.Shutdown()
    
    // Create transaction message
    msg := &client.Message{
        Topic: "test_topic",
        Body:  []byte("Transaction Message"),
    }
    
    // Send transaction message
    result, err := txProducer.SendMessageInTransaction(msg, nil)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Transaction message sent successfully: %s\n", result.MsgID)
}

#### Consumer

##### Basic Consumer (Recommended for simple scenarios)

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // Create basic consumer
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

##### Push Consumer (Recommended for high throughput scenarios)

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // Create Push consumer
    pushConsumer := client.NewPushConsumer("push_consumer_group")
    pushConsumer.SetNameServers([]string{"localhost:9876"})
    
    // Set load balance strategy
    pushConsumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})
    
    // Subscribe to topic
    if err := pushConsumer.Subscribe("test_topic", "*"); err != nil {
        log.Fatal(err)
    }
    
    // Register message listener
    pushConsumer.RegisterMessageListener(func(msgs []*client.MessageExt) client.ConsumeResult {
        for _, msg := range msgs {
            fmt.Printf("Push consumer received message: %s\n", string(msg.Body))
        }
        return client.ConsumeSuccess
    })
    
    // Start consumer
    if err := pushConsumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer pushConsumer.Stop()
    
    time.Sleep(time.Hour)
}
```

##### Pull Consumer (For precise consumption control)

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // Create Pull consumer
    pullConsumer := client.NewPullConsumer("pull_consumer_group")
    pullConsumer.SetNameServers([]string{"localhost:9876"})
    
    // Start consumer
    if err := pullConsumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer pullConsumer.Stop()
    
    // Manually pull messages
    for {
        // Get message queues
        queues, err := pullConsumer.FetchSubscribeMessageQueues("test_topic")
        if err != nil {
            log.Printf("Failed to fetch message queues: %v", err)
            time.Sleep(5 * time.Second)
            continue
        }
        
        for _, queue := range queues {
            // Pull messages
            result, err := pullConsumer.PullBlockIfNotFound(queue, "", 0, 32)
            if err != nil {
                log.Printf("Failed to pull messages: %v", err)
                continue
            }
            
            for _, msg := range result.Messages {
                fmt.Printf("Pull consumer received message: %s\n", string(msg.Body))
            }
        }
        
        time.Sleep(1 * time.Second)
    }
}
```

##### Simple Consumer (Lightweight consumer)

```go
package main

import (
    "fmt"
    "log"
    "time"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // Create Simple consumer
    simpleConsumer := client.NewSimpleConsumer("simple_consumer_group")
    simpleConsumer.SetNameServers([]string{"localhost:9876"})
    
    // Subscribe to topic
    if err := simpleConsumer.Subscribe("test_topic", "*"); err != nil {
        log.Fatal(err)
    }
    
    // Register message listener
    simpleConsumer.RegisterMessageListener(func(msgs []*client.MessageExt) client.ConsumeResult {
        for _, msg := range msgs {
            fmt.Printf("Simple consumer received message: %s\n", string(msg.Body))
        }
        return client.ConsumeSuccess
    })
    
    // Start consumer
    if err := simpleConsumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer simpleConsumer.Stop()
    
    time.Sleep(time.Hour)
}
```

## Advanced Configuration

### Load Balance Strategies

The client supports multiple load balance strategies, choose the appropriate one based on your business needs:

```go
package main

import (
    "log"
    client "github.com/chenjy16/go-rocketmq-client"
)

func main() {
    // Create Push consumer
    pushConsumer := client.NewPushConsumer("consumer_group")
    pushConsumer.SetNameServers([]string{"localhost:9876"})
    
    // Set different load balance strategies
    
    // 1. Average allocate strategy (default)
    pushConsumer.SetLoadBalanceStrategy(&client.AverageAllocateStrategy{})
    
    // 2. Round robin allocate strategy
    pushConsumer.SetLoadBalanceStrategy(&client.RoundRobinAllocateStrategy{})
    
    // 3. Consistent hash allocate strategy
    pushConsumer.SetLoadBalanceStrategy(&client.ConsistentHashAllocateStrategy{})
    
    // 4. Config allocate strategy
    pushConsumer.SetLoadBalanceStrategy(&client.ConfigAllocateStrategy{})
    
    // 5. Machine room nearby allocate strategy
    pushConsumer.SetLoadBalanceStrategy(&client.MachineRoomNearbyAllocateStrategy{})
    
    if err := pushConsumer.Subscribe("test_topic", "*"); err != nil {
        log.Fatal(err)
    }
    
    pushConsumer.RegisterMessageListener(func(msgs []*client.MessageExt) client.ConsumeResult {
        // Process messages
        return client.ConsumeSuccess
    })
    
    if err := pushConsumer.Start(); err != nil {
        log.Fatal(err)
    }
    defer pushConsumer.Stop()
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