# Configuration Management Package

This package provides enhanced configuration management capabilities for Go-RocketMQ, including:

## Features

1. **Event-Driven Configuration Listeners**: Register listeners to be notified when configuration values change
2. **Parameter Validation**: Built-in validators for common configuration types
3. **Hot-Reload Functionality**: Automatic reloading of configuration when the config file changes
4. **Thread-Safe Operations**: All operations are thread-safe for concurrent access

## Usage

### Creating a Config Manager

```go
cm, err := config.NewConfigManager("path/to/config.yaml")
if err != nil {
    // handle error
}
defer cm.Close()
```

### Getting Configuration Values

```go
// Get string value with default
logLevel := cm.GetString("broker.log_level", "info")

// Get integer value with default
port := cm.GetInt("broker.listen_port", 10911)

// Get boolean value with default
enableMetrics := cm.GetBool("monitoring.enable_metrics", true)
```

### Setting Configuration Values

```go
err := cm.Set("broker.listen_port", 10912)
if err != nil {
    // handle validation error
}
```

### Registering Validators

```go
// Register a port validator
cm.RegisterValidator("broker.listen_port", config.Port)

// Register a custom validator
cm.RegisterValidator("custom.field", func(key string, value interface{}) error {
    // custom validation logic
    return nil
})
```

### Registering Listeners

```go
cm.RegisterListener("broker.listen_port", func(event *config.ConfigChangeEvent) {
    fmt.Printf("Configuration changed: %s = %v (was %v)\n", 
        event.Key, event.NewValue, event.OldValue)
})
```

### Saving Configuration

```go
err := cm.Save()
if err != nil {
    // handle error
}
```

## Built-in Validators

- `NotEmpty`: Ensures string values are not empty
- `Port`: Validates port numbers (1-65535)
- `PositiveInteger`: Ensures integer values are positive
- `NonNegativeInteger`: Ensures integer values are non-negative
- `FilePath`: Validates file path format
- `DirectoryPath`: Validates directory path format
- `LogLevel`: Validates log level values (debug, info, warn, error)
- `FlushDiskType`: Validates flush disk type values (SYNC_FLUSH, ASYNC_FLUSH)

## Hot-Reload

The configuration manager automatically watches the config file for changes and reloads it when modifications are detected. All registered listeners will be notified of the changes.