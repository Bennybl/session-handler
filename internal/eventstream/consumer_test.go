package eventstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository/memory"
	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// A message is acknowledged only after the repository has committed it, so a
// crash before the commit leaves it to be redelivered. A redelivery that was
// already applied is acknowledged without opening a second lifecycle, which is
// what makes at-least-once delivery safe.
func TestConsumerAcknowledgesAfterApplicationAndOnRedelivery(t *testing.T) {
	t.Parallel()

	steps := &journal{}
	message := newMessage(steps, validEventJSON())
	consumer := newConsumer(t, &sourceStub{messages: []Message{message}}, appliesWith(steps, nil), nil, Options{})
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := []string{"apply", "ack"}; !reflect.DeepEqual(steps.steps, want) {
		t.Fatalf("operations = %v, want %v", steps.steps, want)
	}

	// The same event twice, through the real service and store.
	sessionID := sessiontest.SessionID(901)
	repo := memory.New()
	t.Cleanup(func() { _ = repo.Close() })
	application, err := service.New(service.Dependencies{
		Repository:   repo,
		NewSessionID: func() (string, error) { return sessionID, nil },
	})
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	redelivered := &journal{}
	first := newMessage(redelivered, validEventJSON())
	second := newMessage(redelivered, validEventJSON())
	redeliveryConsumer := newConsumer(t, &sourceStub{messages: []Message{first, second}}, application, nil, Options{})
	if err := redeliveryConsumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if want := []string{"ack", "ack"}; !reflect.DeepEqual(redelivered.steps, want) {
		t.Fatalf("operations = %v, want both messages acknowledged", redelivered.steps)
	}
	result := sessiontest.Query(t, repo, sessiontest.Spec(sessiontest.At("10:01"),
		sessiontest.Filter("sessionId", "eq", sessionID),
	))
	if len(result.Sessions) != 1 {
		t.Fatalf("stored sessions = %+v, want exactly one lifecycle", result.Sessions)
	}
	if len(result.Sessions[0].States) != 1 {
		t.Errorf("stored states = %+v, want the redelivery to add none", result.Sessions[0].States)
	}
}

// A failure that may pass later is retried after a delay. One that never can is
// dead-lettered first and terminated only once the dead letter is stored, and if
// that store fails the original is retried rather than dropped.
func TestConsumerRetriesTransientFailuresAndDeadLettersPermanentOnes(t *testing.T) {
	t.Parallel()

	retryDelay := 7 * time.Second
	steps := &journal{}
	message := newMessage(steps, validEventJSON())
	transient := appliesWith(steps, errors.New("database unavailable"))
	consumer := newConsumer(t, &sourceStub{messages: []Message{message}}, transient, nil, Options{RetryDelay: retryDelay})
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := []string{"apply", "nak"}; !reflect.DeepEqual(steps.steps, want) {
		t.Errorf("operations = %v, want %v", steps.steps, want)
	}
	if message.nakDelay != retryDelay {
		t.Errorf("retry delay = %v, want %v", message.nakDelay, retryDelay)
	}

	permanent := []struct {
		name       string
		data       []byte
		applyError error
	}{
		{name: "permanent domain error", data: validEventJSON(), applyError: session.ErrInvalidTransition},
		{name: "malformed envelope", data: []byte(`{"eventId":`)},
	}
	for _, test := range permanent {
		letters := &journal{}
		failed := newMessage(letters, test.data)
		var got DeadLetter
		publisher := deadLetterFunc(func(_ context.Context, letter DeadLetter) error {
			letters.add("dlq")
			got = letter
			return nil
		})
		applier := applierFunc(func(context.Context, service.Event) error { return test.applyError })
		deadLetterConsumer := newConsumer(t, &sourceStub{messages: []Message{failed}}, applier, publisher, Options{})

		if err := deadLetterConsumer.Run(context.Background()); err != nil {
			t.Errorf("%s: Run() error = %v", test.name, err)
			continue
		}
		if want := []string{"dlq", "term"}; !reflect.DeepEqual(letters.steps, want) {
			t.Errorf("%s: operations = %v, want %v", test.name, letters.steps, want)
		}
		if !reflect.DeepEqual(got.Payload, test.data) {
			t.Errorf("%s: dead letter payload = %q, want the original %q", test.name, got.Payload, test.data)
		}
		if got.Reason == "" {
			t.Errorf("%s: the dead letter carries no reason", test.name)
		}
	}

	unpublishable := &journal{}
	undeliverable := newMessage(unpublishable, []byte(`not-json`))
	failingPublisher := deadLetterFunc(func(context.Context, DeadLetter) error {
		return errors.New("DLQ unavailable")
	})
	noop := applierFunc(func(context.Context, service.Event) error { return nil })
	retryConsumer := newConsumer(t, &sourceStub{messages: []Message{undeliverable}}, noop, failingPublisher, Options{RetryDelay: time.Second})
	if err := retryConsumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := []string{"nak"}; !reflect.DeepEqual(unpublishable.steps, want) {
		t.Errorf("operations = %v, want the original message retried, not terminated", unpublishable.steps)
	}
}

// Shutting down mid-message leaves it undisposed, so the broker redelivers it.
func TestConsumerCancellationLeavesInFlightMessageUnacknowledged(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	steps := &journal{}
	message := newMessage(steps, validEventJSON())
	applier := applierFunc(func(context.Context, service.Event) error {
		cancel()
		return context.Canceled
	})
	consumer := newConsumer(t, &sourceStub{messages: []Message{message}}, applier, nil, Options{})

	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if len(steps.steps) != 0 {
		t.Fatalf("operations = %v, want the message left undisposed", steps.steps)
	}
}

// journal records the operations a test observes, in order.
type journal struct{ steps []string }

func (j *journal) add(step string) { j.steps = append(j.steps, step) }

type sourceStub struct {
	messages []Message
	next     int
}

func (s *sourceStub) Next(ctx context.Context) (Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.next == len(s.messages) {
		return nil, io.EOF
	}
	message := s.messages[s.next]
	s.next++
	return message, nil
}

func (*sourceStub) Close() error { return nil }

type messageStub struct {
	data     []byte
	journal  *journal
	nakDelay time.Duration
}

func newMessage(steps *journal, data []byte) *messageStub {
	return &messageStub{data: data, journal: steps}
}

func (m *messageStub) Data() []byte { return m.data }

func (m *messageStub) Ack(context.Context) error {
	m.journal.add("ack")
	return nil
}

func (m *messageStub) NakWithDelay(delay time.Duration) error {
	m.nakDelay = delay
	m.journal.add("nak")
	return nil
}

func (m *messageStub) Term() error {
	m.journal.add("term")
	return nil
}

type applierFunc func(context.Context, service.Event) error

func (fn applierFunc) ApplyEvent(ctx context.Context, event service.Event) error {
	return fn(ctx, event)
}

// appliesWith records the application step and then reports result.
func appliesWith(steps *journal, result error) applierFunc {
	return func(context.Context, service.Event) error {
		steps.add("apply")
		return result
	}
}

type deadLetterFunc func(context.Context, DeadLetter) error

func (fn deadLetterFunc) PublishDeadLetter(ctx context.Context, letter DeadLetter) error {
	return fn(ctx, letter)
}

func newConsumer(t *testing.T, source Source, applier EventApplier, publisher DeadLetterPublisher, options Options) *Consumer {
	t.Helper()
	consumer, err := NewConsumer(source, applier, publisher, options)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	return consumer
}

// streamEventID is the event ID inside validEventJSON.
var streamEventID = sessiontest.EventID(901)

// validEventJSON is the canonical login envelope the stream tests replay.
func validEventJSON() []byte {
	return fmt.Appendf(nil,
		`{"eventId":%q,"type":"LOGIN","tenantId":"tenant-a","username":"alice","ip":"192.0.2.10","tags":["user"],"timestamp":%q}`,
		streamEventID, sessiontest.At("10:00").Format(time.RFC3339))
}
