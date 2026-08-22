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
	want := Config{
		HTTPAddr: ":8080", EventRetryAttempts: 3, EventRetryDelay: 100 * time.Millisecond,
		KafkaBrokers: "localhost:9092", KafkaTopic: "session-events", KafkaConsumerGroup: "session-handler", KafkaClientID: "session-handler",
		KafkaMaxPollRecords: 128,
		StartupTimeout:      10 * time.Second, ShutdownTimeout: 10 * time.Second,
	}
	if defaults != want {
		t.Fatalf("defaults = %+v, want %+v", defaults, want)
	}
	override, err := FromLookup(mapLookup(map[string]string{
		"HTTP_ADDR": "127.0.0.1:9090", "EVENT_RETRY_ATTEMPTS": "5", "EVENT_RETRY_DELAY": "2ms", "KAFKA_BROKERS": "one:9092,two:9092",
		"KAFKA_TOPIC": "events", "KAFKA_CONSUMER_GROUP": "workers", "KAFKA_CLIENT_ID": "session-test",
		"KAFKA_MAX_POLL_RECORDS": "32",
		"STARTUP_TIMEOUT":        "2s", "SHUTDOWN_TIMEOUT": "3s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if override.EventRetryAttempts != 5 || override.KafkaMaxPollRecords != 32 ||
		override.KafkaBrokers != "one:9092,two:9092" || override.KafkaTopic != "events" || override.KafkaConsumerGroup != "workers" || override.KafkaClientID != "session-test" {
		t.Fatalf("override = %+v", override)
	}
}

func TestInvalidConfiguration(t *testing.T) {
	for key, value := range map[string]string{"EVENT_RETRY_ATTEMPTS": "0", "EVENT_RETRY_DELAY": "0s", "KAFKA_MAX_POLL_RECORDS": "0", "STARTUP_TIMEOUT": "0s"} {
		if _, err := FromLookup(mapLookup(map[string]string{key: value})); err == nil {
			t.Errorf("%s=%q accepted", key, value)
		}
	}
	for name, mutate := range map[string]func(*Config){
		"brokers": func(value *Config) { value.KafkaBrokers = "," },
		"topic":   func(value *Config) { value.KafkaTopic = "" },
		"group":   func(value *Config) { value.KafkaConsumerGroup = "" },
		"client":  func(value *Config) { value.KafkaClientID = "" },
		"batch":   func(value *Config) { value.KafkaMaxPollRecords = 0 },
	} {
		configuration, err := FromLookup(mapLookup(nil))
		if err != nil {
			t.Fatal(err)
		}
		mutate(&configuration)
		if err := configuration.Validate(); err == nil {
			t.Errorf("empty Kafka %s accepted", name)
		}
	}
}

func mapLookup(values map[string]string) LookupEnv {
	return func(key string) (string, bool) { value, exists := values[key]; return value, exists }
}
