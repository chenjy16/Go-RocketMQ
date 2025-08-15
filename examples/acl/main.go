package main

import (
	"fmt"
	"log"
	"time"

	"go-rocketmq/pkg/broker"
	"go-rocketmq/pkg/nameserver"
)

func main() {
	fmt.Println("Go-RocketMQ ACL权限控制系统示例")
	fmt.Println("================================")

	// 1. 启动NameServer（启用ACL）
	fmt.Println("1. 启动NameServer（启用ACL）...")
	nameServerConfig := nameserver.DefaultConfig()
	nameServerConfig.ListenPort = 9876
	// 启用ACL
	nameServerConfig.AclEnable = true
	nameServerConfig.AclConfigFile = "config/plain_acl.yml"
	
	ns := nameserver.NewNameServer(nameServerConfig)
	go func() {
		if err := ns.Start(); err != nil {
			log.Printf("Failed to start NameServer: %v", err)
		}
	}()
	time.Sleep(2 * time.Second)
	
	// 检查NameServer ACL状态
	if ns.IsAclEnabled() {
		fmt.Println("   ✓ NameServer ACL已启用")
	} else {
		fmt.Println("   ✗ NameServer ACL未启用")
	}

	// 2. 启动Broker（启用ACL）
	fmt.Println("2. 启动Broker（启用ACL）...")
	brokerConfig := broker.DefaultBrokerConfig()
	brokerConfig.BrokerName = "ACLBroker"
	brokerConfig.BrokerId = 0
	brokerConfig.ListenPort = 10911
	brokerConfig.NameServerAddr = "127.0.0.1:9876"
	brokerConfig.StorePathRootDir = "/tmp/rocketmq-acl-store"
	
	// 启用ACL
	brokerConfig.AclEnable = true
	brokerConfig.AclConfigFile = "config/plain_acl.yml"
	
	broker := broker.NewBroker(brokerConfig)
	go func() {
		if err := broker.Start(); err != nil {
			log.Printf("Failed to start Broker: %v", err)
		}
	}()
	time.Sleep(3 * time.Second)
	
	// 检查Broker ACL状态
	if broker.IsAclEnabled() {
		fmt.Println("   ✓ Broker ACL已启用")
	} else {
		fmt.Println("   ✗ Broker ACL未启用")
	}

	// 3. 演示ACL功能
	fmt.Println("3. ACL功能演示...")
	demonstrateAclFeatures(ns, broker)

	// 4. 清理资源
	fmt.Println("4. 清理资源...")
	broker.Stop()
	ns.Stop()
	fmt.Println("   ✓ 所有服务已停止")

	fmt.Println("\nACL权限控制系统示例完成！")
}

func demonstrateAclFeatures(ns *nameserver.NameServer, broker *broker.Broker) {
	// 演示ACL配置重载
	fmt.Println("   - 测试ACL配置重载...")
	if err := ns.ReloadAclConfig(); err != nil {
		fmt.Printf("     NameServer ACL配置重载失败: %v\n", err)
	} else {
		fmt.Println("     ✓ NameServer ACL配置重载成功")
	}
	
	if err := broker.ReloadAclConfig(); err != nil {
		fmt.Printf("     Broker ACL配置重载失败: %v\n", err)
	} else {
		fmt.Println("     ✓ Broker ACL配置重载成功")
	}

	// 演示Topic访问权限验证
	fmt.Println("   - 测试Topic访问权限验证...")
	testTopicAccess(ns)

	// 演示ACL启用/禁用
	fmt.Println("   - 测试ACL启用/禁用...")
	testAclToggle(broker)
}

func testTopicAccess(ns *nameserver.NameServer) {
	// 模拟有效的请求数据
	validRequestData := map[string]string{
		"accessKey": "testUser",
		"signature": "validSignature",
		"timestamp": fmt.Sprintf("%d", time.Now().Unix()),
	}
	
	// 测试生产者权限
	if err := ns.ValidateTopicAccess(validRequestData, "TestTopic", "PUB", "127.0.0.1"); err != nil {
		fmt.Printf("     生产者权限验证失败: %v\n", err)
	} else {
		fmt.Println("     ✓ 生产者权限验证通过")
	}
	
	// 测试消费者权限
	validRequestData["groupName"] = "TestGroup"
	if err := ns.ValidateTopicAccess(validRequestData, "TestTopic", "SUB", "127.0.0.1"); err != nil {
		fmt.Printf("     消费者权限验证失败: %v\n", err)
	} else {
		fmt.Println("     ✓ 消费者权限验证通过")
	}
	
	// 测试管理员权限
	if err := ns.ValidateTopicAccess(validRequestData, "AdminTopic", "ADMIN", "127.0.0.1"); err != nil {
		fmt.Printf("     管理员权限验证失败: %v\n", err)
	} else {
		fmt.Println("     ✓ 管理员权限验证通过")
	}
}

func testAclToggle(broker *broker.Broker) {
	// 测试ACL状态切换
	originalState := broker.IsAclEnabled()
	fmt.Printf("     原始ACL状态: %v\n", originalState)
	
	// 禁用ACL
	broker.SetAclEnabled(false)
	if !broker.IsAclEnabled() {
		fmt.Println("     ✓ ACL已成功禁用")
	} else {
		fmt.Println("     ✗ ACL禁用失败")
	}
	
	// 重新启用ACL
	broker.SetAclEnabled(true)
	if broker.IsAclEnabled() {
		fmt.Println("     ✓ ACL已成功启用")
	} else {
		fmt.Println("     ✗ ACL启用失败")
	}
	
	// 恢复原始状态
	broker.SetAclEnabled(originalState)
	fmt.Printf("     ✓ ACL状态已恢复为: %v\n", originalState)
}