package eventstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

const defaultRetryDelay = 5 * time.Second

type Message interface {
	Data() []byte
	Ack(context.Context) error
	NakWithDelay(time.Duration) error
	Term() error
}

type Source interface {
	Next(context.Context) (Message, error)
	Close() error
}

type EventApplier interface {
	ApplyEvent(context.Context, service.Event) error
}

type DeadLetter struct {
	Payload  []byte    `json:"payload"`
	Reason   string    `json:"reason"`
	FailedAt time.Time `json:"failedAt"`
}

type DeadLetterPublisher interface {
	PublishDeadLetter(context.Context, DeadLetter) error
}

type Options struct {
	RetryDelay time.Duration
	Now        func() time.Time
}

type Consumer struct {
	source     Source
	applier    EventApplier
	deadLetter DeadLetterPublisher
	retryDelay time.Duration
	now        func() time.Time
}

func NewConsumer(source Source, applier EventApplier, deadLetter DeadLetterPublisher, options Options) (*Consumer, error) {
	if source == nil {
		return nil, fmt.Errorf("%w: event source is required", session.ErrInvalidInput)
	}
	if applier == nil {
		return nil, fmt.Errorf("%w: event applier is required", session.ErrInvalidInput)
	}
	if options.RetryDelay < 0 {
		return nil, fmt.Errorf("%w: retry delay cannot be negative", session.ErrInvalidInput)
	}
	if options.RetryDelay == 0 {
		options.RetryDelay = defaultRetryDelay
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Consumer{
		source: source, applier: applier, deadLetter: deadLetter,
		retryDelay: options.RetryDelay, now: options.Now,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		message, err := c.source.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if contextError := ctx.Err(); contextError != nil {
				return contextError
			}
			return fmt.Errorf("read event source: %w", err)
		}
		if message == nil {
			return fmt.Errorf("event source returned a nil message")
		}
		if err := c.process(ctx, message); err != nil {
			return err
		}
	}
}

func (c *Consumer) process(ctx context.Context, message Message) error {
	event, err := DecodeEvent(message.Data())
	if err != nil {
		return c.handlePermanent(ctx, message, err)
	}
	if err := c.applier.ApplyEvent(ctx, event); err != nil {
		if contextError := ctx.Err(); contextError != nil {
			return contextError
		}
		if permanent(err) {
			return c.handlePermanent(ctx, message, err)
		}
		if nakError := message.NakWithDelay(c.retryDelay); nakError != nil {
			return fmt.Errorf("delay event retry after %v: %w", err, nakError)
		}
		return nil
	}
	if err := message.Ack(ctx); err != nil {
		return fmt.Errorf("acknowledge applied event: %w", err)
	}
	return nil
}

func (c *Consumer) handlePermanent(ctx context.Context, message Message, cause error) error {
	if c.deadLetter == nil {
		return fmt.Errorf("permanent event failure without dead-letter publisher: %w", cause)
	}
	letter := DeadLetter{
		Payload: append([]byte(nil), message.Data()...), Reason: cause.Error(), FailedAt: c.now().UTC(),
	}
	if err := c.deadLetter.PublishDeadLetter(ctx, letter); err != nil {
		if contextError := ctx.Err(); contextError != nil {
			return contextError
		}
		if nakError := message.NakWithDelay(c.retryDelay); nakError != nil {
			return fmt.Errorf("dead-letter publish failed (%v) and delayed retry failed: %w", err, nakError)
		}
		return nil
	}
	if err := message.Term(); err != nil {
		return fmt.Errorf("terminate dead-lettered event: %w", err)
	}
	return nil
}

func permanent(err error) bool {
	return errors.Is(err, session.ErrInvalidInput) ||
		errors.Is(err, session.ErrInvalidTransition) ||
		errors.Is(err, session.ErrStaleEvent)
}

func DecodeEvent(data []byte) (service.Event, error) {
	type envelope struct {
		EventID   string            `json:"eventId"`
		Type      service.EventType `json:"type"`
		TenantID  string            `json:"tenantId"`
		Username  string            `json:"username"`
		IP        string            `json:"ip"`
		Tags      []string          `json:"tags"`
		Timestamp time.Time         `json:"timestamp"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded envelope
	if err := decoder.Decode(&decoded); err != nil {
		return service.Event{}, fmt.Errorf("%w: malformed event envelope: %v", session.ErrInvalidInput, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return service.Event{}, fmt.Errorf("%w: event envelope contains trailing data", session.ErrInvalidInput)
	}
	return service.Event{
		EventID: decoded.EventID, Type: decoded.Type, TenantID: decoded.TenantID,
		Username: decoded.Username, IP: decoded.IP, Tags: decoded.Tags, Timestamp: decoded.Timestamp,
	}, nil
}
