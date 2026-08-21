package eventstream

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNATSCreatesBoundedStreamAndOrderedDurableConsumer(t *testing.T) {
	connection := testNATSConnection(t)
	config := testNATSConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	source, err := NewNATSSource(ctx, connection, config)
	if err != nil {
		t.Fatalf("NewNATSSource() error = %v", err)
	}
	t.Cleanup(func() { _ = source.jetStream.DeleteStream(context.Background(), config.StreamName) })
	t.Cleanup(func() { _ = source.Close() })

	streamInfo, err := source.stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream Info() error = %v", err)
	}
	if !reflect.DeepEqual(streamInfo.Config.Subjects, []string{config.Subject, config.DeadLetterSubject}) ||
		streamInfo.Config.MaxAge != config.MaxAge || streamInfo.Config.MaxMsgs != config.MaxMessages ||
		streamInfo.Config.MaxBytes != config.MaxBytes || streamInfo.Config.MaxMsgSize != config.MaxMessageBytes ||
		streamInfo.Config.Storage != jetstream.FileStorage {
		t.Fatalf("stream config = %+v", streamInfo.Config)
	}
	consumerInfo, err := source.consumer.Info(ctx)
	if err != nil {
		t.Fatalf("consumer Info() error = %v", err)
	}
	if consumerInfo.Config.Durable != config.ConsumerName || consumerInfo.Config.AckPolicy != jetstream.AckExplicitPolicy ||
		consumerInfo.Config.MaxAckPending != 1 || consumerInfo.Config.MaxRequestBatch != 1 ||
		consumerInfo.Config.FilterSubject != config.Subject {
		t.Fatalf("consumer config = %+v", consumerInfo.Config)
	}
}

func TestNATSDurableConsumerRedeliversUnacknowledgedMessageAfterRestart(t *testing.T) {
	connection := testNATSConnection(t)
	config := testNATSConfig(t)
	config.AckWait = 250 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	source, err := NewNATSSource(ctx, connection, config)
	if err != nil {
		t.Fatalf("NewNATSSource() error = %v", err)
	}
	t.Cleanup(func() { _ = source.jetStream.DeleteStream(context.Background(), config.StreamName) })
	if _, err := source.jetStream.Publish(ctx, config.Subject, validEventJSON()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	first, err := source.Next(ctx)
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	restarted, err := NewNATSSource(ctx, connection, config)
	if err != nil {
		t.Fatalf("restart NewNATSSource() error = %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	redelivered, err := restarted.Next(ctx)
	if err != nil {
		t.Fatalf("redelivery Next() error = %v", err)
	}
	if !reflect.DeepEqual(redelivered.Data(), first.Data()) {
		t.Fatalf("redelivered data = %q, want %q", redelivered.Data(), first.Data())
	}
	natsMessage, ok := redelivered.(*jetStreamMessage)
	if !ok {
		t.Fatalf("redelivered message type = %T", redelivered)
	}
	metadata, err := natsMessage.message.Metadata()
	if err != nil || metadata.NumDelivered < 2 {
		t.Fatalf("redelivery metadata = %+v, error = %v", metadata, err)
	}
	if err := redelivered.Ack(ctx); err != nil {
		t.Fatalf("redelivery Ack() error = %v", err)
	}
}

func TestNATSPublishesDeadLetterEnvelope(t *testing.T) {
	connection := testNATSConnection(t)
	config := testNATSConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	source, err := NewNATSSource(ctx, connection, config)
	if err != nil {
		t.Fatalf("NewNATSSource() error = %v", err)
	}
	t.Cleanup(func() { _ = source.jetStream.DeleteStream(context.Background(), config.StreamName) })
	t.Cleanup(func() { _ = source.Close() })
	subscription, err := connection.SubscribeSync(config.DeadLetterSubject)
	if err != nil {
		t.Fatalf("SubscribeSync() error = %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	want := DeadLetter{Payload: []byte(`not-json`), Reason: "malformed", FailedAt: time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)}
	if err := source.PublishDeadLetter(ctx, want); err != nil {
		t.Fatalf("PublishDeadLetter() error = %v", err)
	}
	message, err := subscription.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("dead-letter NextMsg() error = %v", err)
	}
	var got DeadLetter
	if err := json.Unmarshal(message.Data, &got); err != nil {
		t.Fatalf("decode dead letter: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dead letter = %+v, want %+v", got, want)
	}
}

func TestNATSNextHonorsCancellation(t *testing.T) {
	connection := testNATSConnection(t)
	config := testNATSConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	source, err := NewNATSSource(ctx, connection, config)
	if err != nil {
		t.Fatalf("NewNATSSource() error = %v", err)
	}
	t.Cleanup(func() { _ = source.jetStream.DeleteStream(context.Background(), config.StreamName) })
	t.Cleanup(func() { _ = source.Close() })
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := source.Next(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next() error = %v, want context cancellation", err)
	}
}

func testNATSConnection(t *testing.T) *nats.Conn {
	t.Helper()
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("TEST_NATS_URL is not set")
	}
	connection, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(connection.Close)
	return connection
}

func testNATSConfig(t *testing.T) NATSConfig {
	t.Helper()
	suffix := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	if len(suffix) > 30 {
		suffix = suffix[len(suffix)-30:]
	}
	unique := strconv.FormatInt(time.Now().UnixNano(), 10)
	return NATSConfig{
		StreamName:        "TEST_" + suffix + "_" + unique,
		Subject:           "test." + suffix + ".events." + unique,
		ConsumerName:      "consumer_" + unique,
		DeadLetterSubject: "test." + suffix + ".dlq." + unique,
		MaxAge:            time.Hour, MaxMessages: 100, MaxBytes: 1024 * 1024, MaxMessageBytes: 64 * 1024,
		AckWait: time.Second,
	}
}
