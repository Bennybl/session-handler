package kafka

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

func TestSourceCommitsSuccessfulBatch(t *testing.T) {
	values := []record{
		{topic: "events", partition: 2, offset: 41, key: []byte("key-1"), value: []byte("first")},
		{topic: "events", partition: 2, offset: 42, key: []byte("key-1"), value: []byte("second")},
	}
	client := &fakeClient{batches: [][]record{values}}
	source := &Source{client: client, maxPollRecords: 8}
	var got []string
	if err := source.Consume(context.Background(), func(_ context.Context, message eventstream.Message) error {
		got = append(got, message.ID)
		message.Key[0] = 'X'
		message.Payload[0] = 'X'
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"events/2/41", "events/2/42"}) || len(client.committed) != 2 || client.rebalances != 1 {
		t.Fatalf("messages=%v commits=%d rebalances=%d", got, len(client.committed), client.rebalances)
	}
	if !reflect.DeepEqual(values[0].key, []byte("key-1")) || !reflect.DeepEqual(values[0].value, []byte("first")) {
		t.Fatal("source message shared Kafka record buffers")
	}
}

func TestBatchIsSequentialWithinPartitionAndConcurrentAcrossPartitions(t *testing.T) {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	otherEntered := make(chan struct{})
	values := []record{
		{topic: "events", partition: 0, offset: 1, value: []byte("first")},
		{topic: "events", partition: 0, offset: 2, value: []byte("second")},
		{topic: "events", partition: 1, offset: 1, value: []byte("other")},
	}
	result := make(chan error, 1)
	go func() {
		result <- processBatch(context.Background(), values, func(_ context.Context, message eventstream.Message) error {
			switch string(message.Payload) {
			case "first":
				close(firstEntered)
				<-releaseFirst
			case "second":
				close(secondEntered)
			case "other":
				close(otherEntered)
			}
			return nil
		})
	}()
	sessiontest.Await(t, firstEntered, "first record in partition 0")
	sessiontest.Await(t, otherEntered, "concurrent record in partition 1")
	if !sessiontest.Blocked(secondEntered) {
		t.Fatal("second record in partition 0 overtook the first")
	}
	close(releaseFirst)
	sessiontest.Await(t, secondEntered, "second record after first completed")
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestSourceLeavesFailedBatchUncommitted(t *testing.T) {
	sentinel := errors.New("processing failed")
	client := &fakeClient{batches: [][]record{{{topic: "events", partition: 0, offset: 1}, {topic: "events", partition: 1, offset: 1}}}}
	source := &Source{client: client, maxPollRecords: 8}
	err := source.Consume(context.Background(), func(_ context.Context, message eventstream.Message) error {
		if message.ID == "events/0/1" {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Consume() error = %v, want %v", err, sentinel)
	}
	if len(client.committed) != 0 || client.rebalances != 0 {
		t.Fatalf("commits=%d rebalances=%d, want zero", len(client.committed), client.rebalances)
	}
}

func TestSourceStopsWhenCommitFails(t *testing.T) {
	sentinel := errors.New("commit failed")
	client := &fakeClient{batches: [][]record{{{topic: "events", offset: 1}}}, commitError: sentinel}
	source := &Source{client: client, maxPollRecords: 8}
	err := source.Consume(context.Background(), func(context.Context, eventstream.Message) error { return nil })
	if !errors.Is(err, sentinel) {
		t.Fatalf("Consume() error = %v, want %v", err, sentinel)
	}
	if client.rebalances != 0 {
		t.Fatalf("rebalances=%d, want zero after failed commit", client.rebalances)
	}
}

func TestSourceCloseIsIdempotent(t *testing.T) {
	client := &fakeClient{}
	source := &Source{client: client}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if client.closes != 1 {
		t.Fatalf("closes=%d, want 1", client.closes)
	}
}

func TestOptionsRejectMissingKafkaIdentityAndBatchSize(t *testing.T) {
	valid := Options{Brokers: []string{"localhost:9092"}, Topic: "events", GroupID: "workers", ClientID: "session-handler", MaxPollRecords: 8}
	for name, mutate := range map[string]func(*Options){
		"brokers":    func(value *Options) { value.Brokers = nil },
		"topic":      func(value *Options) { value.Topic = "" },
		"group":      func(value *Options) { value.GroupID = "" },
		"client":     func(value *Options) { value.ClientID = "" },
		"batch size": func(value *Options) { value.MaxPollRecords = 0 },
	} {
		value := valid
		mutate(&value)
		if err := validateOptions(value); err == nil {
			t.Errorf("missing %s accepted", name)
		}
	}
}

type fakeClient struct {
	mu          sync.Mutex
	batches     [][]record
	committed   []record
	commitError error
	rebalances  int
	closes      int
}

func (c *fakeClient) poll(context.Context, int) ([]record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.batches) == 0 {
		return nil, errClientClosed
	}
	value := c.batches[0]
	c.batches = c.batches[1:]
	return value, nil
}

func (c *fakeClient) commit(_ context.Context, values []record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.commitError != nil {
		return c.commitError
	}
	c.committed = append(c.committed, values...)
	return nil
}

func (c *fakeClient) allowRebalance() {
	c.mu.Lock()
	c.rebalances++
	c.mu.Unlock()
}

func (c *fakeClient) close() {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
}
