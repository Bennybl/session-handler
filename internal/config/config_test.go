package config

import (
	"testing"
	"time"
)

func TestLoadDefaultsToMemoryStdin(t *testing.T) {
	t.Parallel()
	got, err := FromLookup(mapLookup(nil))
	if err != nil {
		t.Fatalf("FromLookup() error = %v", err)
	}
	want := Config{
		StorageDriver: StorageMemory, EventStreamDriver: EventStreamStdin, HTTPAddr: ":8080",
		NATSStream: "SESSION_EVENTS", NATSSubject: "sessions.events", NATSConsumer: "session-handler",
		NATSDeadLetterSubject: "sessions.events.dlq", StartupTimeout: 10 * time.Second, ShutdownTimeout: 10 * time.Second,
	}
	if got != want {
		t.Fatalf("FromLookup() = %+v, want %+v", got, want)
	}
}

func TestLoadExplicitPostgresNATSConfiguration(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"SESSION_STORAGE": " POSTGRES ", "DATABASE_URL": "postgres://database/session",
		"EVENT_STREAM_DRIVER": " NATS ", "NATS_URL": "nats://broker:4222",
		"NATS_STREAM": "CUSTOM_EVENTS", "NATS_SUBJECT": "custom.events", "NATS_CONSUMER": "custom-handler",
		"NATS_DLQ_SUBJECT": "custom.events.dlq", "HTTP_ADDR": "127.0.0.1:9090",
		"STARTUP_TIMEOUT": "3s", "SHUTDOWN_TIMEOUT": "4s",
	}
	got, err := FromLookup(mapLookup(values))
	if err != nil {
		t.Fatalf("FromLookup() error = %v", err)
	}
	if got.StorageDriver != StoragePostgres || got.DatabaseURL != values["DATABASE_URL"] ||
		got.EventStreamDriver != EventStreamNATS || got.NATSURL != values["NATS_URL"] ||
		got.NATSStream != "CUSTOM_EVENTS" || got.NATSSubject != "custom.events" ||
		got.NATSConsumer != "custom-handler" || got.NATSDeadLetterSubject != "custom.events.dlq" ||
		got.HTTPAddr != "127.0.0.1:9090" || got.StartupTimeout != 3*time.Second || got.ShutdownTimeout != 4*time.Second {
		t.Fatalf("explicit configuration = %+v", got)
	}
}

func TestLoadRejectsInvalidDriversMissingURLsAndTimeouts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		values map[string]string
	}{
		{name: "invalid storage", values: map[string]string{"SESSION_STORAGE": "redis"}},
		{name: "postgres missing URL", values: map[string]string{"SESSION_STORAGE": "postgres"}},
		{name: "invalid event stream", values: map[string]string{"EVENT_STREAM_DRIVER": "kafka"}},
		{name: "NATS missing URL", values: map[string]string{"EVENT_STREAM_DRIVER": "nats"}},
		{name: "invalid startup timeout", values: map[string]string{"STARTUP_TIMEOUT": "soon"}},
		{name: "zero shutdown timeout", values: map[string]string{"SHUTDOWN_TIMEOUT": "0s"}},
		{name: "same event and DLQ subject", values: map[string]string{
			"EVENT_STREAM_DRIVER": "nats", "NATS_URL": "nats://broker:4222",
			"NATS_SUBJECT": "events", "NATS_DLQ_SUBJECT": "events",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := FromLookup(mapLookup(test.values)); err == nil {
				t.Fatal("FromLookup() error = nil")
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
