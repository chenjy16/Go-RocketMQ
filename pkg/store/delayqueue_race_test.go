package store

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestDelayQueueServiceRaceCondition 测试DelayQueueService的竞态条件修复
func TestDelayQueueServiceRaceCondition(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-delay-race-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &StoreConfig{
		StorePathRootDir:         tempDir,
		StorePathCommitLog:       filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue:    filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:           filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog:   1024 * 1024,
		MapedFileSizeConsumeQueue: 300000,
		FlushDiskType:            ASYNC_FLUSH,
	}

	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}

	delayService := NewDelayQueueService(config, store)

	// 并发启动和关闭服务多次
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			// 启动服务
			err := delayService.Start()
			if err != nil {
				t.Logf("Start error (expected): %v", err)
			}
			
			// 短暂等待
			time.Sleep(10 * time.Millisecond)
			
			// 关闭服务
			delayService.Shutdown()
		}()
	}

	wg.Wait()
	t.Log("DelayQueueService race condition test completed successfully")
}

// TestDelayQueueServiceConcurrentAccess 测试DelayQueueService的并发访问
func TestDelayQueueServiceConcurrentAccess(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rocketmq-delay-concurrent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &StoreConfig{
		StorePathRootDir:         tempDir,
		StorePathCommitLog:       filepath.Join(tempDir, "commitlog"),
		StorePathConsumeQueue:    filepath.Join(tempDir, "consumequeue"),
		StorePathIndex:           filepath.Join(tempDir, "index"),
		MapedFileSizeCommitLog:   1024 * 1024,
		MapedFileSizeConsumeQueue: 300000,
		FlushDiskType:            ASYNC_FLUSH,
	}

	store, err := NewDefaultMessageStore(config)
	if err != nil {
		t.Fatalf("Failed to create message store: %v", err)
	}

	delayService := NewDelayQueueService(config, store)

	// 启动服务
	err = delayService.Start()
	if err != nil {
		t.Fatalf("Failed to start delay service: %v", err)
	}
	defer delayService.Shutdown()

	// 并发访问loadProgress和saveProgress
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		
		// 并发调用loadProgress
		go func() {
			defer wg.Done()
			delayService.loadProgress()
		}()
		
		// 并发调用saveProgress
		go func() {
			defer wg.Done()
			delayService.saveProgress()
		}()
	}

	wg.Wait()
	t.Log("DelayQueueService concurrent access test completed successfully")
}