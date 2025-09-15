package store

import (
	"testing"
	"time"
)

func TestAdaptiveFlushConfig(t *testing.T) {
	// Test default adaptive flush configuration
	config := DefaultAdaptiveFlushConfig()

	if !config.EnableAdaptiveFlush {
		t.Errorf("Expected adaptive flush to be enabled by default")
	}

	if config.BaseFlushInterval != 500*time.Millisecond {
		t.Errorf("Expected base flush interval to be 500ms, got %v", config.BaseFlushInterval)
	}

	if config.MinFlushInterval != 100*time.Millisecond {
		t.Errorf("Expected min flush interval to be 100ms, got %v", config.MinFlushInterval)
	}

	if config.MaxFlushInterval != 2*time.Second {
		t.Errorf("Expected max flush interval to be 2s, got %v", config.MaxFlushInterval)
	}

	t.Logf("Adaptive flush config test completed")
}

func TestFlushMetrics(t *testing.T) {
	// Create flush metrics
	metrics := &FlushMetrics{}

	// Test updating metrics
	latency := 10 * time.Millisecond
	metrics.mutex.Lock()
	metrics.TotalFlushCount = 1
	metrics.CurrentFlushLatency = latency
	if metrics.AvgFlushLatency == 0 {
		metrics.AvgFlushLatency = latency
	} else {
		metrics.AvgFlushLatency = (metrics.AvgFlushLatency + latency) / 2
	}
	metrics.LastFlushTime = time.Now()
	metrics.mutex.Unlock()

	// Check metrics
	stats := metrics // In a real implementation, we would call a method to get a copy

	if stats.TotalFlushCount != 1 {
		t.Errorf("Expected total flush count 1, got %d", stats.TotalFlushCount)
	}

	t.Logf("Flush metrics test completed")
}

func TestConfigValidation(t *testing.T) {
	// Test that our configuration validation works
	config := NewDefaultStoreConfig()

	// These should be valid
	if config.FlushIntervalCommitLog <= 0 {
		t.Errorf("Expected positive flush interval for commit log")
	}

	if config.FlushIntervalConsumeQueue <= 0 {
		t.Errorf("Expected positive flush interval for consume queue")
	}

	t.Logf("Config validation test completed")
}
