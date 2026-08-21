package eventstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NATSConfig struct {
	StreamName        string
	Subject           string
	ConsumerName      string
	DeadLetterSubject string
	MaxAge            time.Duration
	MaxMessages       int64
	MaxBytes          int64
	MaxMessageBytes   int32
	AckWait           time.Duration
	FetchMaxWait      time.Duration
}

func DefaultNATSConfig() NATSConfig {
	return NATSConfig{
		StreamName: "SESSION_EVENTS", Subject: "sessions.events", ConsumerName: "session-handler",
		DeadLetterSubject: "sessions.events.dlq",
		MaxAge:            7 * 24 * time.Hour, MaxMessages: 100_000, MaxBytes: 256 * 1024 * 1024,
		MaxMessageBytes: maximumEventBytes, AckWait: 30 * time.Second, FetchMaxWait: time.Second,
	}
}

type NATSSource struct {
	mu         sync.RWMutex
	connection *nats.Conn
	owned      bool
	closed     bool
	jetStream  jetstream.JetStream
	stream     jetstream.Stream
	consumer   jetstream.Consumer
	config     NATSConfig
}

func OpenNATSSource(ctx context.Context, url string, config NATSConfig) (*NATSSource, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(url) == "" {
		return nil, fmt.Errorf("%w: NATS URL is required", session.ErrInvalidInput)
	}
	connection, err := nats.Connect(url, nats.Name("session-handler"))
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	source, err := NewNATSSource(ctx, connection, config)
	if err != nil {
		connection.Close()
		return nil, err
	}
	source.owned = true
	return source, nil
}

func NewNATSSource(ctx context.Context, connection *nats.Conn, config NATSConfig) (*NATSSource, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if connection == nil {
		return nil, fmt.Errorf("%w: NATS connection is required", session.ErrInvalidInput)
	}
	config = withNATSDefaults(config)
	if err := validateNATSConfig(config); err != nil {
		return nil, err
	}
	js, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf("create JetStream client: %w", err)
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: config.StreamName, Subjects: []string{config.Subject, config.DeadLetterSubject},
		Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardOld, Storage: jetstream.FileStorage,
		MaxAge: config.MaxAge, MaxMsgs: config.MaxMessages, MaxBytes: config.MaxBytes,
		MaxMsgSize: config.MaxMessageBytes, Replicas: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("create or update NATS event stream: %w", err)
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: config.ConsumerName, Durable: config.ConsumerName,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: config.AckWait,
		DeliverPolicy: jetstream.DeliverAllPolicy, ReplayPolicy: jetstream.ReplayInstantPolicy,
		FilterSubject: config.Subject, MaxAckPending: 1, MaxRequestBatch: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("create or update NATS event consumer: %w", err)
	}
	return &NATSSource{
		connection: connection, jetStream: js, stream: stream, consumer: consumer, config: config,
	}, nil
}

func (s *NATSSource) Next(ctx context.Context) (Message, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.RLock()
		if s.closed {
			s.mu.RUnlock()
			return nil, io.EOF
		}
		consumer := s.consumer
		maxWait := s.config.FetchMaxWait
		s.mu.RUnlock()
		message, err := consumer.Next(jetstream.FetchContext(ctx), jetstream.FetchMaxWait(maxWait))
		if err == nil {
			return &jetStreamMessage{message: message}, nil
		}
		if contextError := ctx.Err(); contextError != nil {
			return nil, contextError
		}
		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages) {
			continue
		}
		return nil, fmt.Errorf("fetch NATS event: %w", err)
	}
}

func (s *NATSSource) PublishDeadLetter(ctx context.Context, letter DeadLetter) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := json.Marshal(letter)
	if err != nil {
		return fmt.Errorf("encode dead letter: %w", err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return fmt.Errorf("NATS event source is closed")
	}
	if _, err := s.jetStream.Publish(ctx, s.config.DeadLetterSubject, encoded); err != nil {
		return fmt.Errorf("publish NATS dead letter: %w", err)
	}
	return nil
}

func (s *NATSSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.owned {
		s.connection.Close()
	}
	return nil
}

type jetStreamMessage struct {
	message jetstream.Msg
}

func (m *jetStreamMessage) Data() []byte { return m.message.Data() }

func (m *jetStreamMessage) Ack(ctx context.Context) error { return m.message.DoubleAck(ctx) }

func (m *jetStreamMessage) NakWithDelay(delay time.Duration) error {
	return m.message.NakWithDelay(delay)
}

func (m *jetStreamMessage) Term() error { return m.message.Term() }

func withNATSDefaults(config NATSConfig) NATSConfig {
	defaults := DefaultNATSConfig()
	if config.StreamName == "" {
		config.StreamName = defaults.StreamName
	}
	if config.Subject == "" {
		config.Subject = defaults.Subject
	}
	if config.ConsumerName == "" {
		config.ConsumerName = defaults.ConsumerName
	}
	if config.DeadLetterSubject == "" {
		config.DeadLetterSubject = defaults.DeadLetterSubject
	}
	if config.MaxAge == 0 {
		config.MaxAge = defaults.MaxAge
	}
	if config.MaxMessages == 0 {
		config.MaxMessages = defaults.MaxMessages
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = defaults.MaxBytes
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if config.AckWait == 0 {
		config.AckWait = defaults.AckWait
	}
	if config.FetchMaxWait == 0 {
		config.FetchMaxWait = defaults.FetchMaxWait
	}
	return config
}

func validateNATSConfig(config NATSConfig) error {
	if strings.TrimSpace(config.StreamName) == "" || strings.TrimSpace(config.Subject) == "" ||
		strings.TrimSpace(config.ConsumerName) == "" || strings.TrimSpace(config.DeadLetterSubject) == "" {
		return fmt.Errorf("%w: NATS stream, subject, consumer, and dead-letter subject are required", session.ErrInvalidInput)
	}
	if config.Subject == config.DeadLetterSubject {
		return fmt.Errorf("%w: event and dead-letter subjects must differ", session.ErrInvalidInput)
	}
	if config.MaxAge < 0 || config.MaxMessages < 0 || config.MaxBytes < 0 || config.MaxMessageBytes < 0 ||
		config.AckWait < 0 || config.FetchMaxWait < 0 {
		return fmt.Errorf("%w: NATS retention and wait values cannot be negative", session.ErrInvalidInput)
	}
	return nil
}
