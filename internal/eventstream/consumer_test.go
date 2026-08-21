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

// A message is acknowledged only once the repository has committed it, so a
// crash before the commit leaves the message to be redelivered.
func TestConsumerAcknowledgesOnlyAfterSuccessfulApplication(t *testing.T) {
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
}

func TestConsumerAcknowledgesSuccessfulDuplicate(t *testing.T) {
	t.Parallel()

	// The service reports a repeated event as a success, and the consumer must
	// treat that like any other success rather than retrying it.
	steps := &journal{}
	message := newMessage(steps, validEventJSON())
	consumer := newConsumer(t, &sourceStub{messages: []Message{message}}, appliesWith(steps, nil), nil, Options{})

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := []string{"apply", "ack"}; !reflect.DeepEqual(steps.steps, want) {
		t.Fatalf("operations = %v, want %v", steps.steps, want)
	}
}

// Redelivering an already applied event acknowledges it without opening a
// second lifecycle, which is what makes at-least-once delivery safe.
func TestConsumerAcknowledgesRedeliveryWithoutCreatingAnotherLifecycle(t *testing.T) {
	t.Parallel()

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

	steps := &journal{}
	first := newMessage(steps, validEventJSON())
	redelivery := newMessage(steps, validEventJSON())
	consumer := newConsumer(t, &sourceStub{messages: []Message{first, redelivery}}, application, nil, Options{})
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if want := []string{"ack", "ack"}; !reflect.DeepEqual(steps.steps, want) {
		t.Fatalf("operations = %v, want both messages acknowledged", steps.steps)
	}
	result := sessiontest.Query(t, repo, sessiontest.Spec(sessiontest.At("10:01"),
		sessiontest.Filter("sessionId", "eq", sessionID),
	))
	if len(result.Sessions) != 1 {
		t.Fatalf("stored sessions = %+v, want exactly one lifecycle", result.Sessions)
	}
	if len(result.Sessions[0].States) != 1 {
		t.Fatalf("stored states = %+v, want the redelivery to add none", result.Sessions[0].States)
	}
}

// A transient failure is retried later rather than dropped.
func TestConsumerDelaysTransientFailure(t *testing.T) {
	t.Parallel()

	retryDelay := 7 * time.Second
	steps := &journal{}
	message := newMessage(steps, validEventJSON())
	applier := appliesWith(steps, errors.New("database unavailable"))
	consumer := newConsumer(t, &sourceStub{messages: []Message{message}}, applier, nil, Options{RetryDelay: retryDelay})

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := []string{"apply", "nak"}; !reflect.DeepEqual(steps.steps, want) {
		t.Fatalf("operations = %v, want %v", steps.steps, want)
	}
	if message.nakDelay != retryDelay {
		t.Errorf("retry delay = %v, want %v", message.nakDelay, retryDelay)
	}
}

// An event that can never succeed is dead-lettered first and terminated only
// after the dead letter is safely stored.
func TestConsumerDeadLettersBeforeTerminating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		data       []byte
		applyError error
	}{
		{name: "permanent domain error", data: validEventJSON(), applyError: session.ErrInvalidTransition},
		{name: "malformed envelope", data: []byte(`{"eventId":`)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			steps := &journal{}
			message := newMessage(steps, test.data)
			var got DeadLetter
			publisher := deadLetterFunc(func(_ context.Context, letter DeadLetter) error {
				steps.add("dlq")
				got = letter
				return nil
			})
			applier := applierFunc(func(context.Context, service.Event) error { return test.applyError })
			consumer := newConsumer(t, &sourceStub{messages: []Message{message}}, applier, publisher, Options{})

			if err := consumer.Run(context.Background()); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if want := []string{"dlq", "term"}; !reflect.DeepEqual(steps.steps, want) {
				t.Fatalf("operations = %v, want %v", steps.steps, want)
			}
			if !reflect.DeepEqual(got.Payload, test.data) {
				t.Errorf("dead letter payload = %q, want the original message %q", got.Payload, test.data)
			}
			if got.Reason == "" {
				t.Error("the dead letter carries no reason")
			}
		})
	}
}

// If the dead letter cannot be published, the original message is retried
// instead of being terminated and lost.
func TestConsumerRetriesWhenDeadLetterPublishFails(t *testing.T) {
	t.Parallel()

	steps := &journal{}
	message := newMessage(steps, []byte(`not-json`))
	publisher := deadLetterFunc(func(context.Context, DeadLetter) error { return errors.New("DLQ unavailable") })
	applier := applierFunc(func(context.Context, service.Event) error { return nil })
	consumer := newConsumer(t, &sourceStub{messages: []Message{message}}, applier, publisher, Options{RetryDelay: time.Second})

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := []string{"nak"}; !reflect.DeepEqual(steps.steps, want) {
		t.Fatalf("operations = %v, want %v", steps.steps, want)
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
