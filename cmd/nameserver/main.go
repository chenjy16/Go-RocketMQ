package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go-rocketmq/pkg/nameserver"
)

func main() {
	var (
		port = flag.Int("port", 9876, "NameServer listen port")
	)
	flag.Parse()

	// 创建配置
	config := nameserver.DefaultConfig()
	config.ListenPort = *port

	// 创建NameServer实例
	ns := nameserver.NewNameServer(config)

	// 启动NameServer
	if err := ns.Start(); err != nil {
		log.Fatalf("Failed to start NameServer: %v", err)
	}

	log.Printf("NameServer started successfully on port %d", *port)

	// 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Received shutdown signal, stopping NameServer...")

	// 停止NameServer
	ns.Stop()
	log.Println("NameServer stopped")
}