package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/postgres/migrations"
	"github.com/Bennybl/session-handler/internal/repository/memory"
	postgresrepository "github.com/Bennybl/session-handler/internal/repository/postgres"
	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/sessiontest"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The durable path must reproduce the memory and stdin snapshot exactly, while
// also surviving a crash between the database commit and the acknowledgement,
// dead-lettering an impossible event, and outliving the process that wrote it.
func TestPostgresNATSMatchesMemoryStdinAndSurvivesRedeliveryRestart(t *testing.T) {
	databaseURL := requireEnvironment(t, "TEST_DATABASE_URL")
	natsURL := requireEnvironment(t, "TEST_NATS_URL")
	want := memoryStdinSnapshot(t)

	repo, _, schemaURL := newPostgresRepository(t, databaseURL)
	harness := newApplicationHarness(t, repo, fixtureIDGenerator(t))
	stream := newNATSStream(t, natsURL)

	// Commit Alice's login to PostgreSQL, then stop before acknowledging it.
	login := fixtureEvents()[0]
	stream.publish(t, login)
	stream.commitFirstWithoutAcknowledging(t, harness)

	// Restarting redelivers that login, which must be recognized as already
	// applied rather than opening a second lifecycle.
	observed, stopConsumer := stream.consume(t, harness, encodeEvent(t, login))
	stream.publish(t, fixtureEvents()[2:]...)

	// The trailing update has no active session and can never succeed.
	deadLetter := stream.nextDeadLetter(t)
	if !strings.Contains(deadLetter.Reason, "invalid transition") {
		t.Errorf("dead letter reason = %q, want it to name an invalid transition", deadLetter.Reason)
	}
	if !strings.Contains(string(deadLetter.Payload), invalidUpdateEventID) {
		t.Errorf("dead letter payload = %s, want the invalid update %q", deadLetter.Payload, invalidUpdateEventID)
	}

	waitUntil(t, "every message to be acknowledged or terminated", func() bool {
		return observed.acknowledged.Load() == 5 && observed.terminated.Load() == 1
	})
	stream.waitForEmptyBacklog(t)
	if got := observed.redeliveredLoginAcks.Load(); got != 1 {
		t.Errorf("acknowledgements of the redelivered login = %d, want exactly 1", got)
	}
	if got := exerciseHTTPQueries(t, harness.handler); !reflect.DeepEqual(got, want) {
		t.Fatalf("PostgreSQL and NATS snapshot = %+v, want the memory and stdin snapshot %+v", got, want)
	}
	stopConsumer(t)
	if err := repo.Close(); err != nil {
		t.Fatalf("close the first PostgreSQL repository: %v", err)
	}

	// The data outlives the repository that wrote it.
	persisted, persistedDatabase := openPostgresRepository(t, schemaURL)
	persistedHarness := newApplicationHarness(t, persisted, concurrentIDGenerator())
	if got := exerciseHTTPQueries(t, persistedHarness.handler); !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot after reopening PostgreSQL = %+v, want %+v", got, want)
	}
	assertRollbackLeavesSessionUnchanged(t, stream, persistedHarness, persistedDatabase)
}

// memoryStdinSnapshot is the baseline the durable path must reproduce.
func memoryStdinSnapshot(t *testing.T) querySnapshot {
	t.Helper()
	repo := memory.New()
	harness := newApplicationHarness(t, repo, fixtureIDGenerator(t))
	consumeStdin(t, harness.service, fixtureEvents(), &recordingDeadLetters{})
	snapshot := exerciseHTTPQueries(t, harness.handler)
	if err := repo.Close(); err != nil {
		t.Fatalf("close the baseline memory repository: %v", err)
	}
	return snapshot
}

// A failed PostgreSQL write must leave the session untouched and the message
// scheduled for a later retry rather than acknowledged.
func assertRollbackLeavesSessionUnchanged(t *testing.T, stream *natsStream, harness applicationHarness, database *sql.DB) {
	t.Helper()
	if _, err := database.Exec(`
		CREATE FUNCTION reject_integration_state() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF 'force-db-error' = ANY(NEW.tags) THEN
				RAISE EXCEPTION 'forced integration persistence failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER reject_integration_state_insert
			BEFORE INSERT ON session_states
			FOR EACH ROW EXECUTE FUNCTION reject_integration_state();
	`); err != nil {
		t.Fatalf("install the rollback trigger: %v", err)
	}

	observed, stopConsumer := stream.consume(t, harness, nil)
	stream.publish(t, service.Event{
		EventID: sessiontest.EventID(1307), Type: service.EventUpdate,
		TenantID: "tenant-a", Username: "bob", IP: "192.0.2.10",
		Tags: []string{"force-db-error"}, Timestamp: sessiontest.At("13:00"),
	})

	waitUntil(t, "the delayed retry after the PostgreSQL rollback", func() bool {
		return observed.negativeAcknowledgements.Load() == 1
	})
	bob := queryHTTP(t, harness.handler, map[string]any{"filters": []any{
		filter("sessionId", "eq", bobSessionID),
	}})
	if len(bob.Sessions) != 1 {
		t.Fatalf("Bob's sessions = %+v, want 1", bob.Sessions)
	}
	if len(bob.Sessions[0].States) != 1 {
		t.Fatalf("Bob's states = %+v, want the rolled-back insert to add none", bob.Sessions[0].States)
	}
	if want := []string{"user"}; !reflect.DeepEqual(bob.Sessions[0].States[0].Tags, want) {
		t.Fatalf("Bob's tags = %v, want the original %v", bob.Sessions[0].States[0].Tags, want)
	}
	stopConsumer(t)
}

// natsStream owns one uniquely named JetStream stream, its durable consumer,
// and a subscription to its dead-letter subject.
type natsStream struct {
	connection  *nats.Conn
	jetStream   jetstream.JetStream
	config      eventstream.NATSConfig
	first       *eventstream.NATSSource
	deadLetters *nats.Subscription
}

func newNATSStream(t *testing.T, url string) *natsStream {
	t.Helper()
	connection, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	t.Cleanup(connection.Close)
	jetStream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	// A unique name per run keeps repeated runs from sharing broker state.
	unique := fmt.Sprintf("%d", time.Now().UnixNano())
	config := eventstream.NATSConfig{
		StreamName: "INTEGRATION_" + unique, Subject: "integration." + unique + ".events",
		ConsumerName: "integration_" + unique, DeadLetterSubject: "integration." + unique + ".dlq",
		MaxAge: time.Hour, MaxMessages: 1_000, MaxBytes: 8 * 1024 * 1024, MaxMessageBytes: 1024 * 1024,
		AckWait: 300 * time.Millisecond, FetchMaxWait: 100 * time.Millisecond,
	}
	stream := &natsStream{connection: connection, jetStream: jetStream, config: config}

	// Opening the first source creates the stream and durable consumer.
	stream.first = stream.openSource(t)
	t.Cleanup(func() { _ = jetStream.DeleteStream(context.Background(), config.StreamName) })

	stream.deadLetters, err = connection.SubscribeSync(config.DeadLetterSubject)
	if err != nil {
		t.Fatalf("subscribe to the dead-letter subject: %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("flush the dead-letter subscription: %v", err)
	}
	return stream
}

func (s *natsStream) openSource(t *testing.T) *eventstream.NATSSource {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	source, err := eventstream.NewNATSSource(ctx, s.connection, s.config)
	if err != nil {
		t.Fatalf("NewNATSSource() error = %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	return source
}

func (s *natsStream) publish(t *testing.T, events ...service.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, event := range events {
		if _, err := s.jetStream.Publish(ctx, s.config.Subject, encodeEvent(t, event)); err != nil {
			t.Fatalf("publish event %s: %v", event.EventID, err)
		}
	}
}

// commitFirstWithoutAcknowledging applies one message and then drops the source,
// reproducing a crash between the database commit and the acknowledgement.
func (s *natsStream) commitFirstWithoutAcknowledging(t *testing.T, harness applicationHarness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	message, err := s.first.Next(ctx)
	if err != nil {
		t.Fatalf("fetch the message to commit: %v", err)
	}
	event, err := eventstream.DecodeEvent(message.Data())
	if err != nil {
		t.Fatalf("decode the message to commit: %v", err)
	}
	if err := harness.service.ApplyEvent(ctx, event); err != nil {
		t.Fatalf("commit the message: %v", err)
	}
	if err := s.first.Close(); err != nil {
		t.Fatalf("close the source before acknowledging: %v", err)
	}
}

// consume runs a consumer over a fresh source until the returned stop function
// is called. Pass the payload whose acknowledgements should be counted, or nil.
func (s *natsStream) consume(t *testing.T, harness applicationHarness, watched []byte) (*observingSource, func(*testing.T)) {
	t.Helper()
	source := s.openSource(t)
	observed := &observingSource{Source: source, watchedPayload: watched}
	consumer, err := eventstream.NewConsumer(observed, harness.service, source, eventstream.Options{RetryDelay: 2 * time.Second})
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { result <- consumer.Run(ctx) }()

	return observed, func(t *testing.T) {
		t.Helper()
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Errorf("consumer shutdown error = %v, want context.Canceled", err)
		}
		if err := source.Close(); err != nil {
			t.Errorf("close the consumer source: %v", err)
		}
	}
}

func (s *natsStream) nextDeadLetter(t *testing.T) eventstream.DeadLetter {
	t.Helper()
	message, err := s.deadLetters.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatalf("wait for a dead letter: %v", err)
	}
	var letter eventstream.DeadLetter
	if err := json.Unmarshal(message.Data, &letter); err != nil {
		t.Fatalf("decode the dead letter %s: %v", message.Data, err)
	}
	return letter
}

func (s *natsStream) waitForEmptyBacklog(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	durable, err := s.jetStream.Consumer(ctx, s.config.StreamName, s.config.ConsumerName)
	if err != nil {
		t.Fatalf("load the durable consumer: %v", err)
	}
	waitUntil(t, "the durable consumer backlog to drain", func() bool {
		info, err := durable.Info(context.Background())
		return err == nil && info.NumAckPending == 0 && info.NumPending == 0
	})
}

// observingSource counts how each message it hands out was disposed of.
type observingSource struct {
	eventstream.Source
	watchedPayload           []byte
	acknowledged             atomic.Int64
	redeliveredLoginAcks     atomic.Int64
	negativeAcknowledgements atomic.Int64
	terminated               atomic.Int64
}

func (s *observingSource) Next(ctx context.Context) (eventstream.Message, error) {
	message, err := s.Source.Next(ctx)
	if err != nil {
		return nil, err
	}
	return &observingMessage{Message: message, source: s}, nil
}

type observingMessage struct {
	eventstream.Message
	source *observingSource
}

func (m *observingMessage) Ack(ctx context.Context) error {
	if err := m.Message.Ack(ctx); err != nil {
		return err
	}
	m.source.acknowledged.Add(1)
	if len(m.source.watchedPayload) > 0 && reflect.DeepEqual(m.Data(), m.source.watchedPayload) {
		m.source.redeliveredLoginAcks.Add(1)
	}
	return nil
}

func (m *observingMessage) NakWithDelay(delay time.Duration) error {
	if err := m.Message.NakWithDelay(delay); err != nil {
		return err
	}
	m.source.negativeAcknowledgements.Add(1)
	return nil
}

func (m *observingMessage) Term() error {
	if err := m.Message.Term(); err != nil {
		return err
	}
	m.source.terminated.Add(1)
	return nil
}

// newPostgresRepository creates a schema unique to this run, migrates it, and
// drops it when the test ends.
func newPostgresRepository(t *testing.T, databaseURL string) (*postgresrepository.Repository, *sql.DB, string) {
	t.Helper()
	schema := fmt.Sprintf("session_handler_integration_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open the PostgreSQL administrative connection: %v", err)
	}
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		_ = admin.Close()
		t.Fatalf("create the integration schema: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("close the PostgreSQL administrative connection: %v", err)
	}
	t.Cleanup(func() {
		cleanup, err := sql.Open("pgx", databaseURL)
		if err != nil {
			t.Errorf("open the PostgreSQL cleanup connection: %v", err)
			return
		}
		defer cleanup.Close()
		if _, err := cleanup.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop the integration schema: %v", err)
		}
	})

	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	parameters := parsed.Query()
	parameters.Set("search_path", schema)
	parsed.RawQuery = parameters.Encode()

	repo, database := openPostgresRepository(t, parsed.String())
	if err := migrations.Apply(ctx, database); err != nil {
		t.Fatalf("apply the integration migrations: %v", err)
	}
	return repo, database, parsed.String()
}

func openPostgresRepository(t *testing.T, databaseURL string) (*postgresrepository.Repository, *sql.DB) {
	t.Helper()
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open the integration database: %v", err)
	}
	database.SetMaxOpenConns(20)
	if err := database.PingContext(context.Background()); err != nil {
		_ = database.Close()
		t.Fatalf("connect to the integration database: %v", err)
	}
	repo, err := postgresrepository.New(database)
	if err != nil {
		_ = database.Close()
		t.Fatalf("postgres.New() error = %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	return repo, database
}

func requireEnvironment(t *testing.T, key string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		t.Skip(key + " is not set")
	}
	return value
}
