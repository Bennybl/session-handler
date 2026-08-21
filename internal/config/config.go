package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	defaultHTTPAddr        = ":8080"
	defaultNATSStream      = "SESSION_EVENTS"
	defaultNATSSubject     = "sessions.events"
	defaultNATSConsumer    = "session-handler"
	defaultNATSDeadLetter  = "sessions.events.dlq"
	defaultStartupTimeout  = 10 * time.Second
	defaultShutdownTimeout = 10 * time.Second
)

type StorageDriver string

const (
	StorageMemory   StorageDriver = "memory"
	StoragePostgres StorageDriver = "postgres"
)

type EventStreamDriver string

const (
	EventStreamStdin EventStreamDriver = "stdin"
	EventStreamNATS  EventStreamDriver = "nats"
)

type LookupEnv func(string) (string, bool)

type Config struct {
	StorageDriver         StorageDriver
	DatabaseURL           string
	EventStreamDriver     EventStreamDriver
	NATSURL               string
	NATSStream            string
	NATSSubject           string
	NATSConsumer          string
	NATSDeadLetterSubject string
	HTTPAddr              string
	StartupTimeout        time.Duration
	ShutdownTimeout       time.Duration
}

func Load() (Config, error) {
	return FromLookup(os.LookupEnv)
}

func FromLookup(lookup LookupEnv) (Config, error) {
	if lookup == nil {
		return Config{}, fmt.Errorf("environment lookup is required")
	}
	configuration := Config{
		StorageDriver:         StorageDriver(strings.ToLower(valueOrDefault(lookup, "SESSION_STORAGE", string(StorageMemory)))),
		DatabaseURL:           value(lookup, "DATABASE_URL"),
		EventStreamDriver:     EventStreamDriver(strings.ToLower(valueOrDefault(lookup, "EVENT_STREAM_DRIVER", string(EventStreamStdin)))),
		NATSURL:               value(lookup, "NATS_URL"),
		NATSStream:            valueOrDefault(lookup, "NATS_STREAM", defaultNATSStream),
		NATSSubject:           valueOrDefault(lookup, "NATS_SUBJECT", defaultNATSSubject),
		NATSConsumer:          valueOrDefault(lookup, "NATS_CONSUMER", defaultNATSConsumer),
		NATSDeadLetterSubject: valueOrDefault(lookup, "NATS_DLQ_SUBJECT", defaultNATSDeadLetter),
		HTTPAddr:              valueOrDefault(lookup, "HTTP_ADDR", defaultHTTPAddr),
	}
	var err error
	configuration.StartupTimeout, err = duration(lookup, "STARTUP_TIMEOUT", defaultStartupTimeout)
	if err != nil {
		return Config{}, err
	}
	configuration.ShutdownTimeout, err = duration(lookup, "SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if err != nil {
		return Config{}, err
	}
	if err := configuration.Validate(); err != nil {
		return Config{}, err
	}
	return configuration, nil
}

func (configuration Config) Validate() error {
	switch configuration.StorageDriver {
	case StorageMemory:
	case StoragePostgres:
		if strings.TrimSpace(configuration.DatabaseURL) == "" {
			return fmt.Errorf("DATABASE_URL is required for postgres storage")
		}
	default:
		return fmt.Errorf("unsupported SESSION_STORAGE %q", configuration.StorageDriver)
	}
	switch configuration.EventStreamDriver {
	case EventStreamStdin:
	case EventStreamNATS:
		if strings.TrimSpace(configuration.NATSURL) == "" {
			return fmt.Errorf("NATS_URL is required for nats event stream")
		}
	default:
		return fmt.Errorf("unsupported EVENT_STREAM_DRIVER %q", configuration.EventStreamDriver)
	}
	if strings.TrimSpace(configuration.HTTPAddr) == "" {
		return fmt.Errorf("HTTP_ADDR is required")
	}
	if strings.TrimSpace(configuration.NATSStream) == "" || strings.TrimSpace(configuration.NATSSubject) == "" ||
		strings.TrimSpace(configuration.NATSConsumer) == "" || strings.TrimSpace(configuration.NATSDeadLetterSubject) == "" {
		return fmt.Errorf("NATS stream, subject, consumer, and dead-letter subject are required")
	}
	if configuration.NATSSubject == configuration.NATSDeadLetterSubject {
		return fmt.Errorf("NATS_SUBJECT and NATS_DLQ_SUBJECT must differ")
	}
	if configuration.StartupTimeout <= 0 || configuration.ShutdownTimeout <= 0 {
		return fmt.Errorf("startup and shutdown timeouts must be positive")
	}
	return nil
}

func value(lookup LookupEnv, key string) string {
	raw, _ := lookup(key)
	return strings.TrimSpace(raw)
}

func valueOrDefault(lookup LookupEnv, key, fallback string) string {
	if raw := value(lookup, key); raw != "" {
		return raw
	}
	return fallback
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
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return parsed, nil
}
