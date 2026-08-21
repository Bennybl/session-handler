package eventstream

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository/memory"
	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

func TestConsumerAcknowledgesOnlyAfterSuccessfulApplication(t *testing.T) {
	t.Parallel()
	order := []string{}
	message := &messageStub{data: validEventJSON(), ack: func(context.Context) error {
		order = append(order, "ack")
		return nil
	}}
	source := &sourceStub{messages: []Message{message}}
	applier := applierFunc(func(context.Context, service.Event) error {
		order = append(order, "apply")
		return nil
	})

	consumer := mustConsumer(t, source, applier, nil, Options{})
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(order, []string{"apply", "ack"}) {
		t.Fatalf("operation order = %v, want apply then ack", order)
	}
	if message.acks != 1 || message.naks != 0 || message.terms != 0 {
		t.Fatalf("message dispositions = ack %d nak %d term %d", message.acks, message.naks, message.terms)
	}
}

func TestConsumerAcknowledgesSuccessfulDuplicate(t *testing.T) {
	t.Parallel()
	message := &messageStub{data: validEventJSON()}
	source := &sourceStub{messages: []Message{message}}
	applier := applierFunc(func(context.Context, service.Event) error { return nil })
	consumer := mustConsumer(t, source, applier, nil, Options{})
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if message.acks != 1 {
		t.Fatalf("duplicate acknowledgements = %d, want 1", message.acks)
	}
}

func TestConsumerAcknowledgesRedeliveryWithoutCreatingAnotherLifecycle(t *testing.T) {
	t.Parallel()
	repository := memory.New()
	t.Cleanup(func() { _ = repository.Close() })
	application, err := service.New(service.Dependencies{
		Repository: repository,
		NewSessionID: func() (string, error) {
			return "00000000-0000-4000-8000-000000000901", nil
		},
	})
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}

	first := &messageStub{data: validEventJSON()}
	redelivery := &messageStub{data: validEventJSON()}
	consumer := mustConsumer(t, &sourceStub{messages: []Message{first, redelivery}}, application, nil, Options{})
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if first.acks != 1 || redelivery.acks != 1 {
		t.Fatalf("acknowledgements = first %d, redelivery %d; want 1 each", first.acks, redelivery.acks)
	}

	result, err := repository.Query(context.Background(), session.QuerySpec{
		Filters:     []session.Filter{{Field: "sessionId", Operator: "eq", Value: "00000000-0000-4000-8000-000000000901"}},
		EvaluatedAt: time.Date(2026, 8, 21, 10, 1, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("repository.Query() error = %v", err)
	}
	if len(result.Sessions) != 1 || len(result.Sessions[0].States) != 1 {
		t.Fatalf("stored sessions = %+v, want one lifecycle with one state", result.Sessions)
	}
}

func TestConsumerDelaysTransientFailure(t *testing.T) {
	t.Parallel()
	retryDelay := 7 * time.Second
	message := &messageStub{data: validEventJSON()}
	source := &sourceStub{messages: []Message{message}}
	applier := applierFunc(func(context.Context, service.Event) error { return errors.New("database unavailable") })
	consumer := mustConsumer(t, source, applier, nil, Options{RetryDelay: retryDelay})

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if message.naks != 1 || message.nakDelay != retryDelay || message.acks != 0 || message.terms != 0 {
		t.Fatalf("message dispositions = ack %d nak %d delay %v term %d", message.acks, message.naks, message.nakDelay, message.terms)
	}
}

func TestConsumerDeadLettersPermanentAndMalformedEventsBeforeTermination(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		data       []byte
		applyError error
	}{
		{name: "permanent domain error", data: validEventJSON(), applyError: session.ErrInvalidTransition},
		{name: "malformed envelope", data: []byte(`{"eventId":`), applyError: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			order := []string{}
			message := &messageStub{data: tt.data, term: func() error {
				order = append(order, "term")
				return nil
			}}
			source := &sourceStub{messages: []Message{message}}
			publisher := &deadLetterStub{publish: func(_ context.Context, letter DeadLetter) error {
				order = append(order, "dlq")
				if !reflect.DeepEqual(letter.Payload, tt.data) || letter.Reason == "" {
					t.Fatalf("dead letter = %+v", letter)
				}
				return nil
			}}
			applier := applierFunc(func(context.Context, service.Event) error { return tt.applyError })
			consumer := mustConsumer(t, source, applier, publisher, Options{})

			if err := consumer.Run(context.Background()); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if !reflect.DeepEqual(order, []string{"dlq", "term"}) {
				t.Fatalf("operation order = %v, want dlq then term", order)
			}
			if message.terms != 1 || message.acks != 0 || message.naks != 0 {
				t.Fatalf("message dispositions = ack %d nak %d term %d", message.acks, message.naks, message.terms)
			}
		})
	}
}

func TestConsumerRetriesWhenDeadLetterPublishFails(t *testing.T) {
	t.Parallel()
	publishError := errors.New("DLQ unavailable")
	message := &messageStub{data: []byte(`not-json`)}
	source := &sourceStub{messages: []Message{message}}
	publisher := &deadLetterStub{publish: func(context.Context, DeadLetter) error { return publishError }}
	consumer := mustConsumer(t, source, applierFunc(func(context.Context, service.Event) error { return nil }), publisher, Options{RetryDelay: time.Second})

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if message.naks != 1 || message.terms != 0 || message.acks != 0 {
		t.Fatalf("message dispositions = ack %d nak %d term %d", message.acks, message.naks, message.terms)
	}
}

func TestConsumerCancellationLeavesInFlightMessageUnacknowledged(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	message := &messageStub{data: validEventJSON()}
	source := &sourceStub{messages: []Message{message}}
	applier := applierFunc(func(context.Context, service.Event) error {
		cancel()
		return context.Canceled
	})
	consumer := mustConsumer(t, source, applier, nil, Options{})

	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context cancellation", err)
	}
	if message.acks != 0 || message.naks != 0 || message.terms != 0 {
		t.Fatalf("cancelled message was disposed: ack %d nak %d term %d", message.acks, message.naks, message.terms)
	}
}

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
	ack      func(context.Context) error
	nak      func(time.Duration) error
	term     func() error
	acks     int
	naks     int
	terms    int
	nakDelay time.Duration
}

func (m *messageStub) Data() []byte { return m.data }

func (m *messageStub) Ack(ctx context.Context) error {
	m.acks++
	if m.ack != nil {
		return m.ack(ctx)
	}
	return nil
}

func (m *messageStub) NakWithDelay(delay time.Duration) error {
	m.naks++
	m.nakDelay = delay
	if m.nak != nil {
		return m.nak(delay)
	}
	return nil
}

func (m *messageStub) Term() error {
	m.terms++
	if m.term != nil {
		return m.term()
	}
	return nil
}

type applierFunc func(context.Context, service.Event) error

func (fn applierFunc) ApplyEvent(ctx context.Context, event service.Event) error {
	return fn(ctx, event)
}

type deadLetterStub struct {
	publish func(context.Context, DeadLetter) error
}

func (p *deadLetterStub) PublishDeadLetter(ctx context.Context, letter DeadLetter) error {
	return p.publish(ctx, letter)
}

func mustConsumer(t *testing.T, source Source, applier EventApplier, publisher DeadLetterPublisher, options Options) *Consumer {
	t.Helper()
	consumer, err := NewConsumer(source, applier, publisher, options)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	return consumer
}

func validEventJSON() []byte {
	return []byte(`{"eventId":"90000000-0000-4000-8000-000000000001","type":"LOGIN","tenantId":"tenant-a","username":"alice","ip":"192.0.2.10","tags":["user"],"timestamp":"2026-08-21T10:00:00Z"}`)
}
