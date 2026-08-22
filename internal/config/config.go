package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type LookupEnv func(string) (string, bool)

type Config struct {
	HTTPAddr            string
	EventRetryAttempts  int
	EventRetryDelay     time.Duration
	KafkaBrokers        string
	KafkaTopic          string
	KafkaConsumerGroup  string
	KafkaClientID       string
	KafkaMaxPollRecords int
	StartupTimeout      time.Duration
	ShutdownTimeout     time.Duration
}

func Load() (Config, error) { return FromLookup(os.LookupEnv) }

func FromLookup(lookup LookupEnv) (Config, error) {
	if lookup == nil {
		return Config{}, fmt.Errorf("environment lookup is required")
	}
	configuration := Config{
		HTTPAddr:           valueOrDefault(lookup, "HTTP_ADDR", ":8080"),
		KafkaBrokers:       valueOrDefault(lookup, "KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:         valueOrDefault(lookup, "KAFKA_TOPIC", "session-events"),
		KafkaConsumerGroup: valueOrDefault(lookup, "KAFKA_CONSUMER_GROUP", "session-handler"),
		KafkaClientID:      valueOrDefault(lookup, "KAFKA_CLIENT_ID", "session-handler"),
	}
	var err error
	if configuration.EventRetryAttempts, err = integer(lookup, "EVENT_RETRY_ATTEMPTS", 3); err != nil {
		return Config{}, err
	}
	if configuration.KafkaMaxPollRecords, err = integer(lookup, "KAFKA_MAX_POLL_RECORDS", 128); err != nil {
		return Config{}, err
	}
	if configuration.EventRetryDelay, err = duration(lookup, "EVENT_RETRY_DELAY", 100*time.Millisecond); err != nil {
		return Config{}, err
	}
	if configuration.StartupTimeout, err = duration(lookup, "STARTUP_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if configuration.ShutdownTimeout, err = duration(lookup, "SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if err := configuration.Validate(); err != nil {
		return Config{}, err
	}
	return configuration, nil
}

func (configuration Config) Validate() error {
	if strings.TrimSpace(configuration.HTTPAddr) == "" {
		return fmt.Errorf("HTTP_ADDR is required")
	}
	if err := validateKafka(configuration); err != nil {
		return err
	}
	if configuration.EventRetryAttempts < 1 || configuration.EventRetryAttempts > 100 {
		return fmt.Errorf("EVENT_RETRY_ATTEMPTS must be between 1 and 100")
	}
	if configuration.EventRetryDelay <= 0 {
		return fmt.Errorf("EVENT_RETRY_DELAY must be positive")
	}
	if configuration.StartupTimeout <= 0 || configuration.ShutdownTimeout <= 0 {
		return fmt.Errorf("startup and shutdown timeouts must be positive")
	}
	return nil
}

func validateKafka(configuration Config) error {
	for _, value := range strings.Split(configuration.KafkaBrokers, ",") {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("KAFKA_BROKERS must contain comma-separated broker addresses")
		}
	}
	if strings.TrimSpace(configuration.KafkaTopic) == "" {
		return fmt.Errorf("KAFKA_TOPIC is required")
	}
	if strings.TrimSpace(configuration.KafkaConsumerGroup) == "" {
		return fmt.Errorf("KAFKA_CONSUMER_GROUP is required")
	}
	if strings.TrimSpace(configuration.KafkaClientID) == "" {
		return fmt.Errorf("KAFKA_CLIENT_ID is required")
	}
	if configuration.KafkaMaxPollRecords < 1 || configuration.KafkaMaxPollRecords > 10000 {
		return fmt.Errorf("KAFKA_MAX_POLL_RECORDS must be between 1 and 10000")
	}
	return nil
}

func value(lookup LookupEnv, key string) string { raw, _ := lookup(key); return strings.TrimSpace(raw) }
func valueOrDefault(lookup LookupEnv, key, fallback string) string {
	if raw := value(lookup, key); raw != "" {
		return raw
	}
	return fallback
}

func integer(lookup LookupEnv, key string, fallback int) (int, error) {
	raw := value(lookup, key)
	if raw == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}

func duration(lookup LookupEnv, key string, fallback time.Duration) (time.Duration, error) {
	raw := value(lookup, key)
	if raw == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}
