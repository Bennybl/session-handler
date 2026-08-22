package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	MutationGuardNone    = "none"
	MutationGuardStriped = "striped"
)

type LookupEnv func(string) (string, bool)

type Config struct {
	HTTPAddr               string
	PartitionCount         int
	PartitionQueueCapacity int
	EventRetryAttempts     int
	EventRetryDelay        time.Duration
	MutationGuard          string
	MutationGuardStripes   int
	StartupTimeout         time.Duration
	ShutdownTimeout        time.Duration
}

func Load() (Config, error) { return FromLookup(os.LookupEnv) }

func FromLookup(lookup LookupEnv) (Config, error) {
	if lookup == nil {
		return Config{}, fmt.Errorf("environment lookup is required")
	}
	configuration := Config{HTTPAddr: valueOrDefault(lookup, "HTTP_ADDR", ":8080"), MutationGuard: strings.ToLower(valueOrDefault(lookup, "SESSION_MUTATION_GUARD", MutationGuardNone))}
	var err error
	if configuration.PartitionCount, err = integer(lookup, "PARTITION_COUNT", 16); err != nil {
		return Config{}, err
	}
	if configuration.PartitionQueueCapacity, err = integer(lookup, "PARTITION_QUEUE_CAPACITY", 64); err != nil {
		return Config{}, err
	}
	if configuration.EventRetryAttempts, err = integer(lookup, "EVENT_RETRY_ATTEMPTS", 3); err != nil {
		return Config{}, err
	}
	if configuration.MutationGuardStripes, err = integer(lookup, "MUTATION_GUARD_STRIPES", 64); err != nil {
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
	if configuration.PartitionCount < 1 || configuration.PartitionCount > 1024 {
		return fmt.Errorf("PARTITION_COUNT must be between 1 and 1024")
	}
	if configuration.PartitionQueueCapacity < 1 || configuration.PartitionQueueCapacity > 65536 {
		return fmt.Errorf("PARTITION_QUEUE_CAPACITY must be between 1 and 65536")
	}
	if configuration.EventRetryAttempts < 1 || configuration.EventRetryAttempts > 100 {
		return fmt.Errorf("EVENT_RETRY_ATTEMPTS must be between 1 and 100")
	}
	if configuration.EventRetryDelay <= 0 {
		return fmt.Errorf("EVENT_RETRY_DELAY must be positive")
	}
	if configuration.MutationGuard != MutationGuardNone && configuration.MutationGuard != MutationGuardStriped {
		return fmt.Errorf("SESSION_MUTATION_GUARD must be none or striped")
	}
	if configuration.MutationGuardStripes < 1 || configuration.MutationGuardStripes > 65536 {
		return fmt.Errorf("MUTATION_GUARD_STRIPES must be between 1 and 65536")
	}
	if configuration.StartupTimeout <= 0 || configuration.ShutdownTimeout <= 0 {
		return fmt.Errorf("startup and shutdown timeouts must be positive")
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
