package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Options struct {
	Brokers        []string
	Topic          string
	GroupID        string
	ClientID       string
	MaxPollRecords int
}

type Source struct {
	client         recordClient
	maxPollRecords int
	closeOnce      sync.Once
}

func New(ctx context.Context, options Options) (*Source, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(options.Brokers...),
		kgo.ConsumeTopics(options.Topic),
		kgo.ConsumerGroup(options.GroupID),
		kgo.ClientID(options.ClientID),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect to Kafka: %w", err)
	}
	return &Source{client: &franzClient{client: client}, maxPollRecords: options.MaxPollRecords}, nil
}

func (s *Source) Consume(ctx context.Context, handle eventstream.Handler) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if handle == nil {
		return fmt.Errorf("%w: message handler is required", session.ErrInvalidInput)
	}
	for {
		records, err := s.client.poll(ctx, s.maxPollRecords)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, errClientClosed) {
				return nil
			}
			return fmt.Errorf("poll Kafka: %w", err)
		}
		if err := processBatch(ctx, records, handle); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := s.client.commit(ctx, records); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit Kafka batch: %w", err)
		}
		s.client.allowRebalance()
	}
}

func (s *Source) Close() error {
	s.closeOnce.Do(s.client.close)
	return nil
}

func validateOptions(options Options) error {
	if len(options.Brokers) == 0 {
		return fmt.Errorf("%w: at least one Kafka broker is required", session.ErrInvalidInput)
	}
	for _, broker := range options.Brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("%w: Kafka brokers cannot contain an empty address", session.ErrInvalidInput)
		}
	}
	if strings.TrimSpace(options.Topic) == "" || strings.TrimSpace(options.GroupID) == "" || strings.TrimSpace(options.ClientID) == "" {
		return fmt.Errorf("%w: Kafka topic, group ID, and client ID are required", session.ErrInvalidInput)
	}
	if options.MaxPollRecords <= 0 {
		return fmt.Errorf("%w: Kafka max poll records must be positive", session.ErrInvalidInput)
	}
	return nil
}

func processBatch(ctx context.Context, records []record, handle eventstream.Handler) error {
	if len(records) == 0 {
		return fmt.Errorf("Kafka poll returned no records")
	}
	type partitionKey struct {
		topic     string
		partition int32
	}
	partitions := make(map[partitionKey][]record)
	for _, value := range records {
		key := partitionKey{topic: value.topic, partition: value.partition}
		partitions[key] = append(partitions[key], value)
	}
	errorsFound := make(chan error, len(partitions))
	var workers sync.WaitGroup
	for _, values := range partitions {
		values := values
		workers.Add(1)
		go func() {
			defer workers.Done()
			for _, value := range values {
				message := eventstream.Message{
					ID: fmt.Sprintf("%s/%d/%d", value.topic, value.partition, value.offset), Source: "kafka",
					Key: append([]byte(nil), value.key...), Payload: append([]byte(nil), value.value...),
				}
				if err := handle(ctx, message); err != nil {
					errorsFound <- fmt.Errorf("process Kafka record %s: %w", message.ID, err)
					return
				}
			}
		}()
	}
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		return err
	}
	return nil
}

var errClientClosed = errors.New("Kafka client is closed")

type record struct {
	topic     string
	partition int32
	offset    int64
	key       []byte
	value     []byte
	native    *kgo.Record
}

type recordClient interface {
	poll(context.Context, int) ([]record, error)
	commit(context.Context, []record) error
	allowRebalance()
	close()
}

type franzClient struct {
	client *kgo.Client
}

func (c *franzClient) poll(ctx context.Context, maxPollRecords int) ([]record, error) {
	fetches := c.client.PollRecords(ctx, maxPollRecords)
	if fetches.IsClientClosed() {
		return nil, errClientClosed
	}
	if err := fetches.Err(); err != nil {
		return nil, err
	}
	fetched := fetches.Records()
	if len(fetched) == 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("Kafka poll returned no records")
	}
	records := make([]record, len(fetched))
	for index, value := range fetched {
		records[index] = record{
			topic: value.Topic, partition: value.Partition, offset: value.Offset,
			key: value.Key, value: value.Value, native: value,
		}
	}
	return records, nil
}

func (c *franzClient) commit(ctx context.Context, values []record) error {
	records := make([]*kgo.Record, len(values))
	for index, value := range values {
		records[index] = value.native
	}
	return c.client.CommitRecords(ctx, records...)
}

func (c *franzClient) allowRebalance() { c.client.AllowRebalance() }
func (c *franzClient) close()          { c.client.CloseAllowingRebalance() }

var _ eventstream.Source = (*Source)(nil)
