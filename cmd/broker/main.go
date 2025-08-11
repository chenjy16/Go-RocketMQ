package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go-rocketmq/pkg/broker"
)

func main() {
	var (
		port           = flag.Int("port", 10911, "Broker listen port")
		brokerName     = flag.String("name", "DefaultBroker", "Broker name")
		clusterName    = flag.String("cluster", "DefaultCluster", "Cluster name")
		nameServerAddr = flag.String("nameserver", "127.0.0.1:9876", "NameServer address")
		storeDir       = flag.String("store", "/tmp/rocketmq-store", "Store directory")
	)
	flag.Parse()

	config := &broker.Config{
		BrokerName:                *brokerName,
		BrokerId:                  0,
		ClusterName:               *clusterName,
		ListenPort:                *port,
		NameServerAddr:            *nameServerAddr,
		StorePathRootDir:          *storeDir,
		SendMessageThreadPoolNums: 16,
		PullMessageThreadPoolNums: 16,
		FlushDiskType:             0, // ASYNC_FLUSH
		BrokerRole:                0, // ASYNC_MASTER
		HaListenPort:              *port + 1,
	}

	// 创建Broker实例
	b := broker.NewBroker(config)

	// 启动Broker
	if err := b.Start(); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}

	// 创建一些默认Topic用于测试
	b.CreateTopic("TestTopic", 4)
	b.CreateTopic("OrderTopic", 8)
	b.CreateTopic("BenchmarkTopic", 4)

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Broker started successfully on port %d\n", *port)
	fmt.Printf("Broker name: %s, Cluster: %s\n", *brokerName, *clusterName)
	fmt.Printf("Press Ctrl+C to stop...\n")

	<-sigChan
	fmt.Println("\nShutting down broker...")

	// 停止Broker
	if err := b.Stop(); err != nil {
		log.Printf("Error stopping broker: %v", err)
	}

	fmt.Println("Broker stopped")
}