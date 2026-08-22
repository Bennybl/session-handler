package config

import (
	"testing"
	"time"
)

func TestDefaultsAndOverrides(t *testing.T) {
	defaults, err := FromLookup(mapLookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{HTTPAddr: ":8080", PartitionCount: 16, PartitionQueueCapacity: 64, EventRetryAttempts: 3, EventRetryDelay: 100 * time.Millisecond, StartupTimeout: 10 * time.Second, ShutdownTimeout: 10 * time.Second}
	if defaults != want {
		t.Fatalf("defaults = %+v, want %+v", defaults, want)
	}
	override, err := FromLookup(mapLookup(map[string]string{"HTTP_ADDR": "127.0.0.1:9090", "PARTITION_COUNT": "4", "PARTITION_QUEUE_CAPACITY": "8", "EVENT_RETRY_ATTEMPTS": "5", "EVENT_RETRY_DELAY": "2ms", "STARTUP_TIMEOUT": "2s", "SHUTDOWN_TIMEOUT": "3s"}))
	if err != nil {
		t.Fatal(err)
	}
	if override.PartitionCount != 4 || override.PartitionQueueCapacity != 8 || override.EventRetryAttempts != 5 {
		t.Fatalf("override = %+v", override)
	}
}

func TestInvalidConfiguration(t *testing.T) {
	for key, value := range map[string]string{"PARTITION_COUNT": "0", "PARTITION_QUEUE_CAPACITY": "0", "EVENT_RETRY_ATTEMPTS": "0", "EVENT_RETRY_DELAY": "0s", "STARTUP_TIMEOUT": "0s"} {
		if _, err := FromLookup(mapLookup(map[string]string{key: value})); err == nil {
			t.Errorf("%s=%q accepted", key, value)
		}
	}
}

func mapLookup(values map[string]string) LookupEnv {
	return func(key string) (string, bool) { value, exists := values[key]; return value, exists }
}
