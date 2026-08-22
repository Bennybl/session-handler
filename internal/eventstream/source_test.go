package eventstream

import (
	"bytes"
	"context"
	"errors"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

type eventApplierFunc func(context.Context, service.Event) error

func (fn eventApplierFunc) ApplyEvent(ctx context.Context, event service.Event) error {
	return fn(ctx, event)
}

func TestDecodeEventUsesTheSharedStrictEnvelope(t *testing.T) {
	event, err := DecodeEvent([]byte(`{"eventId":"E0000000-0000-4000-8000-000000000001","type":" login ","tenantId":"tenant-a","username":"alice","ip":"2001:0db8::1","tags":["user"],"timestamp":"2026-08-21T10:00:00+03:00"}`))
	if err != nil {
		t.Fatal(err)
	}
	if event.EventID[0] != 'e' || event.Type != service.EventLogin || event.Key.IP != "2001:db8::1" || !reflect.DeepEqual(event.Tags, []string{"user"}) || event.Timestamp.Location().String() != "UTC" {
		t.Fatalf("decoded event = %+v", event)
	}
	for _, payload := range [][]byte{[]byte(`{"unknown":true}`), []byte(`{} {}`)} {
		if _, err := DecodeEvent(payload); !errors.Is(err, session.ErrInvalidInput) {
			t.Errorf("DecodeEvent(%q) error = %v", payload, err)
		}
	}
}

func TestProcessorAcknowledgesTerminalMessagesAndRejectsTransientFailures(t *testing.T) {
	valid := []byte(`{"eventId":"e0000000-0000-4000-8000-000000000001","type":"LOGIN","tenantId":"tenant-a","username":"alice","ip":"192.0.2.10","timestamp":"2026-08-21T10:00:00Z"}`)
	for _, test := range []struct {
		name        string
		payload     []byte
		submitError error
		wantError   bool
		wantCalls   int
		wantDead    bool
	}{
		{name: "accepted", payload: valid, wantCalls: 1},
		{name: "malformed", payload: []byte(`{"unknown":true}`), wantDead: true},
		{name: "domain rejection", payload: valid, submitError: session.ErrStaleEvent, wantCalls: 1, wantDead: true},
		{name: "transient failure", payload: valid, submitError: errors.New("storage unavailable"), wantCalls: 2, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			calls := 0
			processor, err := NewProcessor(eventApplierFunc(func(context.Context, service.Event) error {
				calls++
				return test.submitError
			}), log.New(&logs, "", 0), ProcessorOptions{RetryAttempts: 2})
			if err != nil {
				t.Fatal(err)
			}
			err = processor.Handle(context.Background(), Message{ID: "topic/0/1", Source: "kafka", Payload: test.payload})
			if (err != nil) != test.wantError || calls != test.wantCalls {
				t.Fatalf("Handle() error=%v calls=%d, want %d", err, calls, test.wantCalls)
			}
			if got := strings.Contains(logs.String(), `"kind":"dead_letter"`); got != test.wantDead {
				t.Fatalf("dead letter=%v logs=%q", got, logs.String())
			}
		})
	}
}

func TestProcessorFinishesRetryBeforeReturning(t *testing.T) {
	calls := 0
	processor, err := NewProcessor(eventApplierFunc(func(context.Context, service.Event) error {
		calls++
		if calls == 1 {
			return errors.New("temporary")
		}
		return nil
	}), log.New(&bytes.Buffer{}, "", 0), ProcessorOptions{RetryAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"eventId":"e0000000-0000-4000-8000-000000000001","type":"LOGIN","tenantId":"tenant-a","username":"alice","ip":"192.0.2.10","timestamp":"2026-08-21T10:00:00Z"}`)
	if err := processor.Handle(context.Background(), Message{Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("ApplyEvent() calls = %d, want 2", calls)
	}
}
