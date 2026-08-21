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

	"github.com/Bennybl/session-handler/internal/sessiontest"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The stream is created with bounded retention, and the durable consumer is
// configured to deliver one unacknowledged message at a time so same-key
// ordering holds.
func TestNATSCreatesBoundedStreamAndOrderedDurableConsumer(t *testing.T) {
	source, config := newNATSSource(t, testNATSConfig(t))
	ctx := context.Background()

	streamInfo, err := source.stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream Info() error = %v", err)
	}
	got := streamInfo.Config
	if want := []string{config.Subject, config.DeadLetterSubject}; !reflect.DeepEqual(got.Subjects, want) {
		t.Errorf("stream subjects = %v, want %v", got.Subjects, want)
	}
	if got.MaxAge != config.MaxAge || got.MaxMsgs != config.MaxMessages || got.MaxBytes != config.MaxBytes {
		t.Errorf("stream retention = age %v, messages %d, bytes %d; want %v, %d, %d",
			got.MaxAge, got.MaxMsgs, got.MaxBytes, config.MaxAge, config.MaxMessages, config.MaxBytes)
	}
	if got.MaxMsgSize != config.MaxMessageBytes {
		t.Errorf("stream MaxMsgSize = %d, want %d", got.MaxMsgSize, config.MaxMessageBytes)
	}
	if got.Storage != jetstream.FileStorage {
		t.Errorf("stream storage = %v, want file storage", got.Storage)
	}

	consumerInfo, err := source.consumer.Info(ctx)
	if err != nil {
		t.Fatalf("consumer Info() error = %v", err)
	}
	consumerConfig := consumerInfo.Config
	if consumerConfig.Durable != config.ConsumerName {
		t.Errorf("consumer durable name = %q, want %q", consumerConfig.Durable, config.ConsumerName)
	}
	if consumerConfig.FilterSubject != config.Subject {
		t.Errorf("consumer filter subject = %q, want %q", consumerConfig.FilterSubject, config.Subject)
	}
	if consumerConfig.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("consumer ack policy = %v, want explicit", consumerConfig.AckPolicy)
	}
	if consumerConfig.MaxAckPending != 1 || consumerConfig.MaxRequestBatch != 1 {
		t.Errorf("consumer MaxAckPending = %d, MaxRequestBatch = %d; want 1 and 1",
			consumerConfig.MaxAckPending, consumerConfig.MaxRequestBatch)
	}

	// A cancelled context stops a fetch instead of waiting it out.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Next(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("Next() with a cancelled context = %v, want context.Canceled", err)
	}
}

// Closing without acknowledging leaves the message on the stream, and a
// restarted durable consumer receives it again.
func TestNATSRedeliversUnacknowledgedMessagesAndPublishesDeadLetters(t *testing.T) {
	config := testNATSConfig(t)
	config.AckWait = 250 * time.Millisecond
	source, _ := newNATSSource(t, config)
	ctx := context.Background()

	if _, err := source.jetStream.Publish(ctx, config.Subject, validEventJSON()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	first, err := source.Next(ctx)
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("Close() before acknowledging: %v", err)
	}

	restarted, _ := newNATSSource(t, config)
	redelivered, err := restarted.Next(ctx)
	if err != nil {
		t.Fatalf("Next() after restart: %v", err)
	}
	if !reflect.DeepEqual(redelivered.Data(), first.Data()) {
		t.Fatalf("redelivered data = %q, want the unacknowledged %q", redelivered.Data(), first.Data())
	}

	natsMessage, ok := redelivered.(*jetStreamMessage)
	if !ok {
		t.Fatalf("redelivered message = %T, want *jetStreamMessage", redelivered)
	}
	metadata, err := natsMessage.message.Metadata()
	if err != nil {
		t.Fatalf("message Metadata() error = %v", err)
	}
	if metadata.NumDelivered < 2 {
		t.Errorf("NumDelivered = %d, want at least 2 after redelivery", metadata.NumDelivered)
	}
	if err := redelivered.Ack(ctx); err != nil {
		t.Errorf("Ack() error = %v", err)
	}

	// The same stream carries dead letters on their own subject. The first
	// source is closed by now, so publish through the restarted one.
	connection := restarted.connection
	subscription, err := connection.SubscribeSync(config.DeadLetterSubject)
	if err != nil {
		t.Fatalf("SubscribeSync() error = %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	want := DeadLetter{
		Payload:  []byte(`not-json`),
		Reason:   "malformed",
		FailedAt: sessiontest.At("10:00"),
	}
	if err := restarted.PublishDeadLetter(ctx, want); err != nil {
		t.Fatalf("PublishDeadLetter() error = %v", err)
	}

	message, err := subscription.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg() on the dead-letter subject: %v", err)
	}
	var got DeadLetter
	if err := json.Unmarshal(message.Data, &got); err != nil {
		t.Fatalf("decode dead letter %q: %v", message.Data, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dead letter = %+v, want %+v", got, want)
	}
}

// newNATSSource opens a source against the test broker, skipping the test when
// none is configured, and removes the stream when the test ends.
func newNATSSource(t *testing.T, config NATSConfig) (*NATSSource, NATSConfig) {
	t.Helper()
	connection := testNATSConnection(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	source, err := NewNATSSource(ctx, connection, config)
	if err != nil {
		t.Fatalf("NewNATSSource() error = %v", err)
	}
	t.Cleanup(func() {
		_ = source.Close()
		_ = source.jetStream.DeleteStream(context.Background(), config.StreamName)
	})
	return source, config
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

// testNATSConfig names a stream, subject, and consumer unique to this test run
// so parallel and repeated runs cannot collide on the shared broker.
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
		MaxAge:            time.Hour,
		MaxMessages:       100,
		MaxBytes:          1024 * 1024,
		MaxMessageBytes:   64 * 1024,
		AckWait:           time.Second,
	}
}
