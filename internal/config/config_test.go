package config

import (
	"testing"
	"time"
)

// With nothing configured the service runs the zero-dependency mode: an
// in-memory store fed by stdin.
func TestLoadDefaultsToMemoryStdin(t *testing.T) {
	t.Parallel()

	got, err := FromLookup(mapLookup(nil))
	if err != nil {
		t.Fatalf("FromLookup() error = %v", err)
	}

	want := Config{
		StorageDriver: StorageMemory, EventStreamDriver: EventStreamStdin, HTTPAddr: ":8080",
		NATSStream: "SESSION_EVENTS", NATSSubject: "sessions.events", NATSConsumer: "session-handler",
		NATSDeadLetterSubject: "sessions.events.dlq",
		StartupTimeout:        10 * time.Second, ShutdownTimeout: 10 * time.Second,
	}
	if got != want {
		t.Fatalf("FromLookup() = %+v, want %+v", got, want)
	}
}

// Driver names are trimmed and case-insensitive, and every setting is read from
// its documented environment variable.
func TestLoadExplicitPostgresNATSConfiguration(t *testing.T) {
	t.Parallel()

	got, err := FromLookup(mapLookup(map[string]string{
		"SESSION_STORAGE": " POSTGRES ", "DATABASE_URL": "postgres://database/session",
		"EVENT_STREAM_DRIVER": " NATS ", "NATS_URL": "nats://broker:4222",
		"NATS_STREAM": "CUSTOM_EVENTS", "NATS_SUBJECT": "custom.events",
		"NATS_CONSUMER": "custom-handler", "NATS_DLQ_SUBJECT": "custom.events.dlq",
		"HTTP_ADDR": "127.0.0.1:9090", "STARTUP_TIMEOUT": "3s", "SHUTDOWN_TIMEOUT": "4s",
	}))
	if err != nil {
		t.Fatalf("FromLookup() error = %v", err)
	}

	want := Config{
		StorageDriver: StoragePostgres, DatabaseURL: "postgres://database/session",
		EventStreamDriver: EventStreamNATS, NATSURL: "nats://broker:4222",
		NATSStream: "CUSTOM_EVENTS", NATSSubject: "custom.events", NATSConsumer: "custom-handler",
		NATSDeadLetterSubject: "custom.events.dlq", HTTPAddr: "127.0.0.1:9090",
		StartupTimeout: 3 * time.Second, ShutdownTimeout: 4 * time.Second,
	}
	if got != want {
		t.Fatalf("FromLookup() = %+v, want %+v", got, want)
	}
}

func TestLoadRejectsInvalidDriversMissingURLsAndTimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values map[string]string
	}{
		{name: "unknown storage driver", values: map[string]string{"SESSION_STORAGE": "redis"}},
		{name: "postgres without a URL", values: map[string]string{"SESSION_STORAGE": "postgres"}},
		{name: "unknown event stream driver", values: map[string]string{"EVENT_STREAM_DRIVER": "kafka"}},
		{name: "NATS without a URL", values: map[string]string{"EVENT_STREAM_DRIVER": "nats"}},
		{name: "unparsable startup timeout", values: map[string]string{"STARTUP_TIMEOUT": "soon"}},
		{name: "zero shutdown timeout", values: map[string]string{"SHUTDOWN_TIMEOUT": "0s"}},
		{name: "dead letters on the event subject", values: map[string]string{
			"EVENT_STREAM_DRIVER": "nats", "NATS_URL": "nats://broker:4222",
			"NATS_SUBJECT": "events", "NATS_DLQ_SUBJECT": "events",
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := FromLookup(mapLookup(test.values)); err == nil {
				t.Fatal("FromLookup() error = nil, want a configuration error")
			}
		})
	}
}

func mapLookup(values map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, exists := values[key]
		return value, exists
	}
}
