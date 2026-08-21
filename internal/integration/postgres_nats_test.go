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
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const forcedRollbackEventID = "a1000000-0000-4000-8000-000000000007"

func TestPostgresNATSMatchesMemoryStdinAndSurvivesRedeliveryRestart(t *testing.T) {
	databaseURL := requireEnvironment(t, "TEST_DATABASE_URL")
	natsURL := requireEnvironment(t, "TEST_NATS_URL")

	memoryRepository := memory.New()
	memoryHarness := newApplicationHarness(t, memoryRepository, fixtureIDGenerator(t))
	consumeStdin(t, memoryHarness.service, fixtureEvents(), &recordingDeadLetters{})
	want := exerciseHTTPQueries(t, memoryHarness.handler)
	if err := memoryRepository.Close(); err != nil {
		t.Fatalf("close baseline memory repository: %v", err)
	}

	postgresRepository, _, schemaURL := newPostgresRepository(t, databaseURL)
	postgresHarness := newApplicationHarness(t, postgresRepository, fixtureIDGenerator(t))
	connection, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	configuration := integrationNATSConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	firstSource, err := eventstream.NewNATSSource(ctx, connection, configuration)
	if err != nil {
		t.Fatalf("first NewNATSSource() error = %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), configuration.StreamName) })
	dlqSubscription, err := connection.SubscribeSync(configuration.DeadLetterSubject)
	if err != nil {
		t.Fatalf("subscribe to DLQ: %v", err)
	}
	if err := connection.Flush(); err != nil {
		t.Fatalf("flush DLQ subscription: %v", err)
	}

	loginPayload := encodeEvent(t, fixtureEvents()[0])
	if _, err := js.Publish(ctx, configuration.Subject, loginPayload); err != nil {
		t.Fatalf("publish crash-window login: %v", err)
	}
	unacknowledged, err := firstSource.Next(ctx)
	if err != nil {
		t.Fatalf("fetch crash-window login: %v", err)
	}
	decoded, err := eventstream.DecodeEvent(unacknowledged.Data())
	if err != nil {
		t.Fatalf("decode crash-window login: %v", err)
	}
	if err := postgresHarness.service.ApplyEvent(ctx, decoded); err != nil {
		t.Fatalf("commit crash-window login: %v", err)
	}
	if err := firstSource.Close(); err != nil {
		t.Fatalf("close unacknowledged source: %v", err)
	}

	restartedSource, err := eventstream.NewNATSSource(ctx, connection, configuration)
	if err != nil {
		t.Fatalf("restart NewNATSSource() error = %v", err)
	}
	observed := &observingSource{Source: restartedSource, loginPayload: loginPayload}
	consumer, err := eventstream.NewConsumer(observed, postgresHarness.service, restartedSource, eventstream.Options{RetryDelay: 2 * time.Second})
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	consumerContext, stopConsumer := context.WithCancel(context.Background())
	t.Cleanup(stopConsumer)
	t.Cleanup(func() { _ = restartedSource.Close() })
	consumerResult := make(chan error, 1)
	go func() { consumerResult <- consumer.Run(consumerContext) }()

	for _, event := range fixtureEvents()[2:] {
		if _, err := js.Publish(ctx, configuration.Subject, encodeEvent(t, event)); err != nil {
			t.Fatalf("publish event %s: %v", event.EventID, err)
		}
	}
	dlqMessage, err := dlqSubscription.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatalf("wait for invalid-transition DLQ: %v", err)
	}
	var deadLetter eventstream.DeadLetter
	if err := json.Unmarshal(dlqMessage.Data, &deadLetter); err != nil {
		t.Fatalf("decode DLQ: %v", err)
	}
	if !strings.Contains(deadLetter.Reason, "invalid transition") || !strings.Contains(string(deadLetter.Payload), invalidUpdateEventID) {
		t.Fatalf("dead letter = %+v", deadLetter)
	}
	waitFor(t, "NATS acknowledgements and termination", func() bool {
		return observed.acknowledged.Load() == 5 && observed.terminated.Load() == 1
	})
	durable, err := js.Consumer(ctx, configuration.StreamName, configuration.ConsumerName)
	if err != nil {
		t.Fatalf("load durable consumer: %v", err)
	}
	waitFor(t, "empty durable backlog", func() bool {
		info, infoError := durable.Info(context.Background())
		return infoError == nil && info.NumAckPending == 0 && info.NumPending == 0
	})
	if observed.redeliveredLoginAcks.Load() != 1 {
		t.Fatalf("redelivered login acknowledgements = %d, want 1", observed.redeliveredLoginAcks.Load())
	}
	if got := exerciseHTTPQueries(t, postgresHarness.handler); !reflect.DeepEqual(got, want) {
		t.Fatalf("PostgreSQL/NATS snapshot = %+v, memory/stdin snapshot = %+v", got, want)
	}

	stopConsumer()
	if err := <-consumerResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("consumer shutdown error = %v", err)
	}
	if err := restartedSource.Close(); err != nil {
		t.Fatalf("close restarted source: %v", err)
	}
	if err := postgresRepository.Close(); err != nil {
		t.Fatalf("close first PostgreSQL repository: %v", err)
	}

	persistedRepository, persistedDatabase := openExistingPostgresRepository(t, schemaURL)
	persistedHarness := newApplicationHarness(t, persistedRepository, concurrentIDGenerator())
	if got := exerciseHTTPQueries(t, persistedHarness.handler); !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted PostgreSQL snapshot = %+v, want %+v", got, want)
	}
	assertPersistenceRollback(t, ctx, js, connection, configuration, persistedHarness, persistedDatabase)
}

func assertPersistenceRollback(
	t *testing.T,
	ctx context.Context,
	js jetstream.JetStream,
	connection *nats.Conn,
	configuration eventstream.NATSConfig,
	harness applicationHarness,
	database *sql.DB,
) {
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
		t.Fatalf("install rollback trigger: %v", err)
	}
	source, err := eventstream.NewNATSSource(ctx, connection, configuration)
	if err != nil {
		t.Fatalf("rollback NewNATSSource() error = %v", err)
	}
	observed := &observingSource{Source: source}
	consumer, err := eventstream.NewConsumer(observed, harness.service, source, eventstream.Options{RetryDelay: 5 * time.Second})
	if err != nil {
		t.Fatalf("rollback NewConsumer() error = %v", err)
	}
	consumerContext, cancelConsumer := context.WithCancel(context.Background())
	t.Cleanup(cancelConsumer)
	t.Cleanup(func() { _ = source.Close() })
	result := make(chan error, 1)
	go func() { result <- consumer.Run(consumerContext) }()
	rollbackEvent := service.Event{
		EventID: forcedRollbackEventID, Type: service.EventUpdate, TenantID: "tenant-a", Username: "bob",
		IP: "192.0.2.10", Tags: []string{"force-db-error"}, Timestamp: time.Date(2026, 8, 21, 13, 0, 0, 0, time.UTC),
	}
	if _, err := js.Publish(ctx, configuration.Subject, encodeEvent(t, rollbackEvent)); err != nil {
		t.Fatalf("publish rollback event: %v", err)
	}
	waitFor(t, "delayed retry after PostgreSQL rollback", func() bool { return observed.negativeAcknowledgements.Load() == 1 })
	bob := queryHTTP(t, harness.handler, map[string]any{"filters": []any{
		map[string]any{"field": "sessionId", "operator": "eq", "value": bobSessionID},
	}})
	if len(bob.Sessions) != 1 || len(bob.Sessions[0].States) != 1 || !reflect.DeepEqual(bob.Sessions[0].States[0].Tags, []string{"user"}) {
		t.Fatalf("Bob after rolled-back state insert = %+v", bob.Sessions)
	}
	cancelConsumer()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("rollback consumer shutdown error = %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("close rollback source: %v", err)
	}
}

type observingSource struct {
	eventstream.Source
	loginPayload             []byte
	acknowledged             atomic.Int64
	redeliveredLoginAcks     atomic.Int64
	negativeAcknowledgements atomic.Int64
	terminated               atomic.Int64
}

func (source *observingSource) Next(ctx context.Context) (eventstream.Message, error) {
	message, err := source.Source.Next(ctx)
	if err != nil {
		return nil, err
	}
	return &observingMessage{Message: message, source: source}, nil
}

type observingMessage struct {
	eventstream.Message
	source *observingSource
}

func (message *observingMessage) Ack(ctx context.Context) error {
	if err := message.Message.Ack(ctx); err != nil {
		return err
	}
	message.source.acknowledged.Add(1)
	if len(message.source.loginPayload) > 0 && reflect.DeepEqual(message.Data(), message.source.loginPayload) {
		message.source.redeliveredLoginAcks.Add(1)
	}
	return nil
}

func (message *observingMessage) NakWithDelay(delay time.Duration) error {
	if err := message.Message.NakWithDelay(delay); err != nil {
		return err
	}
	message.source.negativeAcknowledgements.Add(1)
	return nil
}

func (message *observingMessage) Term() error {
	if err := message.Message.Term(); err != nil {
		return err
	}
	message.source.terminated.Add(1)
	return nil
}

func integrationNATSConfig(t *testing.T) eventstream.NATSConfig {
	t.Helper()
	unique := fmt.Sprintf("%d", time.Now().UnixNano())
	return eventstream.NATSConfig{
		StreamName: "INTEGRATION_" + unique, Subject: "integration." + unique + ".events",
		ConsumerName: "integration_" + unique, DeadLetterSubject: "integration." + unique + ".dlq",
		MaxAge: time.Hour, MaxMessages: 1_000, MaxBytes: 8 * 1024 * 1024, MaxMessageBytes: 1024 * 1024,
		AckWait: 300 * time.Millisecond, FetchMaxWait: 100 * time.Millisecond,
	}
}

func newPostgresRepository(t *testing.T, databaseURL string) (*postgresrepository.Repository, *sql.DB, string) {
	t.Helper()
	schema := fmt.Sprintf("session_handler_integration_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		_ = admin.Close()
		t.Fatalf("create integration schema: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("close PostgreSQL admin connection: %v", err)
	}
	t.Cleanup(func() {
		cleanup, openError := sql.Open("pgx", databaseURL)
		if openError != nil {
			t.Errorf("open PostgreSQL cleanup connection: %v", openError)
			return
		}
		defer cleanup.Close()
		if _, dropError := cleanup.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`); dropError != nil {
			t.Errorf("drop integration schema: %v", dropError)
		}
	})
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	parameters := parsed.Query()
	parameters.Set("search_path", schema)
	parsed.RawQuery = parameters.Encode()
	repo, database := openExistingPostgresRepository(t, parsed.String())
	if err := migrations.Apply(ctx, database); err != nil {
		t.Fatalf("apply integration migrations: %v", err)
	}
	return repo, database, parsed.String()
}

func openExistingPostgresRepository(t *testing.T, databaseURL string) (*postgresrepository.Repository, *sql.DB) {
	t.Helper()
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open integration PostgreSQL: %v", err)
	}
	database.SetMaxOpenConns(20)
	if err := database.PingContext(context.Background()); err != nil {
		_ = database.Close()
		t.Fatalf("connect to integration PostgreSQL: %v", err)
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
