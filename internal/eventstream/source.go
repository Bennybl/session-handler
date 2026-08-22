package eventstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

const MaximumEventBytes = 1024 * 1024

// Message is the transport-neutral event envelope delivered by a Source. Key
// is the source's opaque ordering key when that transport provides one.
type Message struct {
	ID      string
	Source  string
	Key     []byte
	Payload []byte
}

type Handler func(context.Context, Message) error

// Source owns broker-specific partitioning, receipt, and acknowledgment. It
// must process one partition sequentially, may process different partitions
// concurrently, and acknowledge a message only after handle returns nil. A
// non-nil handler error must leave the message unacknowledged so it can be
// redelivered after the process recovers.
//
// This is the application's only concurrency boundary for writes. Correctness
// requires every production write to arrive through a Source, producers to use
// the complete canonical SessionKey as their stable ordering key, the broker's
// partition count to stay fixed while the process runs, one worker to own each
// assigned partition, the handler to finish all retries before returning, and
// only one application process to write to the in-memory database.
type Source interface {
	Consume(context.Context, Handler) error
	Close() error
}

type EventApplier interface {
	ApplyEvent(context.Context, service.Event) error
}

type ProcessorOptions struct {
	RetryAttempts int
	RetryDelay    time.Duration
}

// Processor converts source messages into trusted events, completes all
// domain-aware retries, and applies them to the service. Partition ownership
// and ordering belong to the Source.
type Processor struct {
	applier EventApplier
	logger  *log.Logger
	options ProcessorOptions
	now     func() time.Time
}

func NewProcessor(applier EventApplier, logger *log.Logger, options ProcessorOptions) (*Processor, error) {
	if applier == nil {
		return nil, fmt.Errorf("%w: event applier is required", session.ErrInvalidInput)
	}
	if logger == nil {
		return nil, fmt.Errorf("%w: logger is required", session.ErrInvalidInput)
	}
	if options.RetryAttempts <= 0 {
		return nil, fmt.Errorf("%w: retry attempts must be positive", session.ErrInvalidInput)
	}
	if options.RetryDelay < 0 {
		return nil, fmt.Errorf("%w: retry delay cannot be negative", session.ErrInvalidInput)
	}
	return &Processor{applier: applier, logger: logger, options: options, now: time.Now}, nil
}

func (p *Processor) Handle(ctx context.Context, message Message) error {
	event, err := DecodeEvent(message.Payload)
	if err != nil {
		p.deadLetter(message, err, nil)
		return nil
	}
	var applyError error
	for attempt := 1; attempt <= p.options.RetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		applyError = p.applier.ApplyEvent(ctx, event)
		if applyError == nil {
			return nil
		}
		if permanentEventError(applyError) {
			p.deadLetter(message, applyError, &event)
			return nil
		}
		if errors.Is(applyError, context.Canceled) || errors.Is(applyError, context.DeadlineExceeded) {
			return applyError
		}
		if attempt < p.options.RetryAttempts {
			timer := time.NewTimer(p.options.RetryDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return ctx.Err()
			}
		}
	}
	return fmt.Errorf("transient event failure after %d attempts: %w", p.options.RetryAttempts, applyError)
}

// DecodeEvent strictly decodes the shared JSON event envelope and performs the
// one-time normalization into a trusted service event.
func DecodeEvent(payload []byte) (service.Event, error) {
	if len(payload) > MaximumEventBytes {
		return service.Event{}, fmt.Errorf("%w: event envelope exceeds %d bytes", session.ErrInvalidInput, MaximumEventBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var input eventInput
	if err := decoder.Decode(&input); err != nil {
		return service.Event{}, fmt.Errorf("%w: malformed event envelope: %v", session.ErrInvalidInput, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return service.Event{}, fmt.Errorf("%w: event envelope contains trailing data", session.ErrInvalidInput)
	}
	return service.NewEvent(input.toServiceInput())
}

type eventInput struct {
	EventID   string            `json:"eventId"`
	Type      service.EventType `json:"type"`
	TenantID  string            `json:"tenantId"`
	Username  string            `json:"username"`
	IP        string            `json:"ip"`
	Tags      []string          `json:"tags"`
	Timestamp time.Time         `json:"timestamp"`
}

func (input eventInput) toServiceInput() service.EventInput {
	return service.EventInput{
		EventID: input.EventID, Type: input.Type, TenantID: input.TenantID,
		Username: input.Username, IP: input.IP, Tags: input.Tags, Timestamp: input.Timestamp,
	}
}

func permanentEventError(err error) bool {
	return errors.Is(err, session.ErrInvalidInput) || errors.Is(err, session.ErrInvalidTransition) || errors.Is(err, session.ErrStaleEvent)
}

func (p *Processor) deadLetter(message Message, cause error, event *service.Event) {
	entry := map[string]any{
		"kind": "dead_letter", "messageId": message.ID, "source": message.Source,
		"payload": string(message.Payload), "reason": cause.Error(), "failedAt": p.now().UTC(),
	}
	if event != nil {
		entry["sessionKey"] = event.Key
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		p.logger.Printf("dead letter encoding failed: %v", err)
		return
	}
	p.logger.Print(string(encoded))
}
