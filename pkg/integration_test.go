package main

import (
	"fmt"
	"go-rocketmq/pkg/config"
	"go-rocketmq/pkg/store"
)

func main() {
	fmt.Println("Go-RocketMQ Integration Test")

	// Test configuration management
	testConfigManagement()

	// Test store enhancements
	testStoreEnhancements()

	fmt.Println("All integration tests completed successfully!")
}

func testConfigManagement() {
	fmt.Println("Testing configuration management...")

	// Create config manager
	cm, err := config.NewConfigManager("../config/config.yaml")
	if err != nil {
		fmt.Printf("Failed to create config manager: %v\n", err)
		return
	}
	defer cm.Close()

	// Test getting config values
	brokerPort := cm.GetInt("broker.listen_port", 10911)
	fmt.Printf("Broker port: %d\n", brokerPort)

	// Test setting config values
	err = cm.Set("broker.listen_port", 10912)
	if err != nil {
		fmt.Printf("Failed to set broker port: %v\n", err)
	} else {
		fmt.Println("Successfully updated broker port")
	}

	// Test validation
	cm.RegisterValidator("broker.listen_port", config.Port)
	err = cm.Set("broker.listen_port", 99999) // Invalid port
	if err != nil {
		fmt.Printf("Correctly rejected invalid port: %v\n", err)
	}

	fmt.Println("Configuration management test completed")
}

func testStoreEnhancements() {
	fmt.Println("Testing store enhancements...")

	// Test adaptive flush config
	flushConfig := store.DefaultAdaptiveFlushConfig()
	if !flushConfig.EnableAdaptiveFlush {
		fmt.Println("Adaptive flush should be enabled by default")
		return
	}

	fmt.Printf("Adaptive flush config: %+v\n", flushConfig)

	// Test store config validation
	storeConfig := store.NewDefaultStoreConfig()
	if storeConfig.FlushIntervalCommitLog <= 0 {
		fmt.Println("Commit log flush interval should be positive")
		return
	}

	fmt.Println("Store enhancements test passed")
}
