package service

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

var (
	generatedSessionID = sessiontest.SessionID(901)
	acceptedEventID    = sessiontest.EventID(902)
)

// Each event type reaches its own domain decision, the service canonicalizes the
// type, key, and tags on the way, and a repeat of the last accepted event is
// reported as a successful no-op.
func TestApplyEventDispatchesAndNormalizes(t *testing.T) {
	t.Parallel()

	loginAt := sessiontest.At("10:00")
	canonicalKey := sessiontest.Key("tenant-a", "alice", "2001:db8::1")
	active := sessiontest.ActiveSession(canonicalKey, generatedSessionID, sessiontest.EventID(901), loginAt, "user")

	dispatches := []struct {
		name      string
		eventType EventType
		snapshot  session.CurrentSessionSnapshot
		wantKind  session.MutationKind
	}{
		{name: "login", eventType: " login ", wantKind: session.MutationStartSession},
		{name: "update", eventType: "update", snapshot: active, wantKind: session.MutationReplaceState},
		{name: "logout", eventType: "LOGOUT", snapshot: active, wantKind: session.MutationEndSession},
	}
	for _, test := range dispatches {
		var gotKey session.SessionKey
		var gotMutation session.Mutation
		repo := &repositoryStub{mutate: func(_ context.Context, key session.SessionKey, decide repository.MutationFunc) error {
			gotKey = key
			var err error
			gotMutation, err = decide(test.snapshot)
			return err
		}}
		generated := 0
		service := newService(t, Dependencies{
			Repository:   repo,
			NewSessionID: func() (string, error) { generated++; return generatedSessionID, nil },
		})

		// The event arrives with a padded type, an expanded IPv6 address, and a
		// repeated tag.
		err := service.ApplyEvent(context.Background(), Event{
			EventID: acceptedEventID, Type: test.eventType,
			TenantID: "tenant-a", Username: "alice", IP: "2001:0db8:0:0:0:0:0:1",
			Tags: []string{"user", "admin", "user"}, Timestamp: loginAt.Add(time.Minute),
		})
		if err != nil {
			t.Errorf("%s: ApplyEvent() error = %v", test.name, err)
			continue
		}
		if gotKey != canonicalKey {
			t.Errorf("%s: mutation key = %+v, want the canonical %+v", test.name, gotKey, canonicalKey)
		}
		if gotMutation == nil || gotMutation.Kind() != test.wantKind {
			t.Errorf("%s: mutation = %#v, want kind %q", test.name, gotMutation, test.wantKind)
			continue
		}
		if got := gotMutation.AcceptedEventID(); got != acceptedEventID {
			t.Errorf("%s: accepted event ID = %q, want %q", test.name, got, acceptedEventID)
		}

		// Only a login mints a session ID and normalizes tags into a state.
		wantGenerated := 0
		if test.wantKind == session.MutationStartSession {
			wantGenerated = 1
			started := gotMutation.(session.StartSession)
			if started.Session.ID != generatedSessionID {
				t.Errorf("%s: session ID = %q, want the generated %q", test.name, started.Session.ID, generatedSessionID)
			}
			if want := []string{"admin", "user"}; !reflect.DeepEqual(started.Session.States[0].Tags, want) {
				t.Errorf("%s: tags = %v, want deduplicated and sorted %v", test.name, started.Session.States[0].Tags, want)
			}
		}
		if generated != wantGenerated {
			t.Errorf("%s: session IDs generated = %d, want %d", test.name, generated, wantGenerated)
		}
	}

	// Replaying the last accepted event changes nothing, which is what lets the
	// stream consumer acknowledge a redelivery.
	at := sessiontest.At("10:00")
	var duplicate session.Mutation
	repo := &repositoryStub{mutate: func(_ context.Context, _ session.SessionKey, decide repository.MutationFunc) error {
		var err error
		duplicate, err = decide(session.CurrentSessionSnapshot{LastEventAt: &at, LastEventID: acceptedEventID})
		return err
	}}
	service := newService(t, Dependencies{Repository: repo})
	err := service.ApplyEvent(context.Background(), Event{
		EventID: acceptedEventID, Type: EventUpdate,
		TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
		Tags: []string{"ignored"}, Timestamp: at.Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("ApplyEvent() for a repeated event error = %v", err)
	}
	if duplicate == nil || duplicate.Kind() != session.MutationDuplicateEvent {
		t.Errorf("mutation = %#v, want a duplicate-event no-op", duplicate)
	}
}

// A session ID is minted once per login, outside the callback a repository may
// retry, and a generator failure stops the event before storage is touched. The
// default generator produces canonical version 4 UUIDs.
func TestApplyEventCreatesSessionIDsOutsideRepositoryRetries(t *testing.T) {
	t.Parallel()

	var seen []string
	retrying := &repositoryStub{mutate: func(_ context.Context, _ session.SessionKey, decide repository.MutationFunc) error {
		for range 2 {
			mutation, err := decide(session.CurrentSessionSnapshot{})
			if err != nil {
				return err
			}
			seen = append(seen, mutation.(session.StartSession).Session.ID)
		}
		return nil
	}}
	generated := 0
	service := newService(t, Dependencies{
		Repository:   retrying,
		NewSessionID: func() (string, error) { generated++; return generatedSessionID, nil },
	})
	if err := service.ApplyEvent(context.Background(), loginEvent()); err != nil {
		t.Fatalf("ApplyEvent() error = %v", err)
	}
	if generated != 1 {
		t.Errorf("session IDs generated = %d, want 1 for two callback attempts", generated)
	}
	if want := []string{generatedSessionID, generatedSessionID}; !reflect.DeepEqual(seen, want) {
		t.Errorf("session IDs seen by the callback = %v, want %v", seen, want)
	}

	generationError := errors.New("entropy unavailable")
	failing := &repositoryStub{}
	failingService := newService(t, Dependencies{
		Repository:   failing,
		NewSessionID: func() (string, error) { return "", generationError },
	})
	if err := failingService.ApplyEvent(context.Background(), loginEvent()); !errors.Is(err, generationError) {
		t.Errorf("ApplyEvent() error = %v, want the generation error", err)
	}
	if failing.mutateCalls != 0 {
		t.Errorf("repository mutations = %d, want 0", failing.mutateCalls)
	}

	first, err := newUUID()
	if err != nil {
		t.Fatalf("newUUID() error = %v", err)
	}
	second, err := newUUID()
	if err != nil {
		t.Fatalf("newUUID() error = %v", err)
	}
	if first == second {
		t.Fatalf("newUUID() repeated %q", first)
	}
	for _, value := range []string{first, second} {
		normalized, err := normalizeUUID(value)
		if err != nil {
			t.Errorf("normalizeUUID(%q) error = %v", value, err)
			continue
		}
		if normalized != value {
			t.Errorf("newUUID() = %q, want the already canonical %q", value, normalized)
		}
		if value[14] != '4' {
			t.Errorf("newUUID() = %q, want version 4 at index 14", value)
		}
		if !strings.ContainsRune("89ab", rune(value[19])) {
			t.Errorf("newUUID() = %q, want an RFC 4122 variant at index 19", value)
		}
	}
}

func TestApplyEventValidatesBeforeCallingRepository(t *testing.T) {
	t.Parallel()

	repo := &repositoryStub{}
	service := newService(t, Dependencies{Repository: repo})
	tests := []struct {
		name  string
		spoil func(*Event)
	}{
		{name: "unknown type", spoil: func(event *Event) { event.Type = "CONNECT" }},
		{name: "invalid key", spoil: func(event *Event) { event.IP = "not-an-ip" }},
		{name: "invalid event ID", spoil: func(event *Event) { event.EventID = "not-a-uuid" }},
		{name: "zero timestamp", spoil: func(event *Event) { event.Timestamp = time.Time{} }},
	}

	for _, test := range tests {
		event := loginEvent()
		test.spoil(&event)
		if err := service.ApplyEvent(context.Background(), event); !errors.Is(err, session.ErrInvalidInput) {
			t.Errorf("%s: ApplyEvent() error = %v, want ErrInvalidInput", test.name, err)
		}
	}
	if repo.mutateCalls != 0 {
		t.Errorf("repository mutations = %d, want 0", repo.mutateCalls)
	}
}

// The service reads the clock once per query and forwards the filters untouched,
// so it stays unaware of what any field means. It checks only the shape of a
// query, refuses cancelled work, and passes repository failures through.
func TestQueryForwardsSpecificationValidatesShapeAndPropagatesErrors(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.FixedZone("UTC+3", 3*60*60))
	clockReads := 0
	request := QueryRequest{
		Filters: []session.Filter{sessiontest.Filter("futureField", "futureOperator", map[string]any{"opaque": true})},
		Page:    session.PageRequest{Limit: 7, Cursor: "opaque-cursor"},
	}
	wantResult := session.QueryResult{
		Sessions:   []session.Session{{ID: "second"}, {ID: "first"}},
		NextCursor: "next",
	}
	var gotSpec session.QuerySpec
	forwarding := &repositoryStub{query: func(_ context.Context, spec session.QuerySpec) (session.QueryResult, error) {
		gotSpec = spec
		return wantResult, nil
	}}
	service := newService(t, Dependencies{
		Repository: forwarding,
		Now:        func() time.Time { clockReads++; return now },
	})

	got, err := service.Query(context.Background(), request)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if !reflect.DeepEqual(got, wantResult) {
		t.Errorf("Query() = %+v, want the repository's own order %+v", got, wantResult)
	}
	if clockReads != 1 {
		t.Errorf("clock reads = %d, want 1", clockReads)
	}
	if !gotSpec.EvaluatedAt.Equal(now) || gotSpec.EvaluatedAt.Location() != time.UTC {
		t.Errorf("EvaluatedAt = %v, want %v in UTC", gotSpec.EvaluatedAt, now)
	}
	if !reflect.DeepEqual(gotSpec.Filters, request.Filters) {
		t.Errorf("forwarded filters = %+v, want %+v", gotSpec.Filters, request.Filters)
	}
	if gotSpec.Page != request.Page {
		t.Errorf("forwarded page = %+v, want %+v", gotSpec.Page, request.Page)
	}

	rejecting := &repositoryStub{}
	rejectingService := newService(t, Dependencies{Repository: rejecting})
	malformed := []struct {
		name    string
		request QueryRequest
	}{
		{name: "missing field", request: QueryRequest{Filters: []session.Filter{{Operator: "eq", Value: "value"}}}},
		{name: "missing operator", request: QueryRequest{Filters: []session.Filter{{Field: "tenantId", Value: "value"}}}},
		{name: "missing value", request: QueryRequest{Filters: []session.Filter{{Field: "tenantId", Operator: "eq"}}}},
		{name: "negative limit", request: QueryRequest{Page: session.PageRequest{Limit: -1}}},
	}
	for _, test := range malformed {
		if _, err := rejectingService.Query(context.Background(), test.request); !errors.Is(err, repository.ErrInvalidQuery) {
			t.Errorf("%s: Query() error = %v, want ErrInvalidQuery", test.name, err)
		}
	}
	if rejecting.queryCalls != 0 {
		t.Errorf("repository queries = %d, want 0", rejecting.queryCalls)
	}

	repositoryError := errors.New("repository unavailable")
	failing := &repositoryStub{
		mutate: func(context.Context, session.SessionKey, repository.MutationFunc) error { return repositoryError },
		query: func(context.Context, session.QuerySpec) (session.QueryResult, error) {
			return session.QueryResult{}, repositoryError
		},
	}
	failingService := newService(t, Dependencies{
		Repository:   failing,
		NewSessionID: func() (string, error) { return generatedSessionID, nil },
	})
	if err := failingService.ApplyEvent(context.Background(), loginEvent()); !errors.Is(err, repositoryError) {
		t.Errorf("ApplyEvent() error = %v, want the repository error", err)
	}
	if _, err := failingService.Query(context.Background(), QueryRequest{}); !errors.Is(err, repositoryError) {
		t.Errorf("Query() error = %v, want the repository error", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	mutationsBefore, queriesBefore := failing.mutateCalls, failing.queryCalls
	if err := failingService.ApplyEvent(cancelled, loginEvent()); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled ApplyEvent() error = %v, want context.Canceled", err)
	}
	if _, err := failingService.Query(cancelled, QueryRequest{}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled Query() error = %v, want context.Canceled", err)
	}
	if failing.mutateCalls != mutationsBefore || failing.queryCalls != queriesBefore {
		t.Error("a cancelled operation still reached the repository")
	}
}

// loginEvent returns a valid login that tests spoil one field at a time.
func loginEvent() Event {
	return Event{
		EventID: sessiontest.EventID(901), Type: EventLogin,
		TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
		Tags: []string{"user"}, Timestamp: sessiontest.At("10:00"),
	}
}

func newService(t *testing.T, dependencies Dependencies) *SessionService {
	t.Helper()
	service, err := New(dependencies)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

type repositoryStub struct {
	mutate      func(context.Context, session.SessionKey, repository.MutationFunc) error
	query       func(context.Context, session.QuerySpec) (session.QueryResult, error)
	mutateCalls int
	queryCalls  int
}

func (r *repositoryStub) Mutate(ctx context.Context, key session.SessionKey, decide repository.MutationFunc) error {
	r.mutateCalls++
	if r.mutate == nil {
		return nil
	}
	return r.mutate(ctx, key, decide)
}

func (r *repositoryStub) Query(ctx context.Context, spec session.QuerySpec) (session.QueryResult, error) {
	r.queryCalls++
	if r.query == nil {
		return session.QueryResult{}, nil
	}
	return r.query(ctx, spec)
}

func (*repositoryStub) Close() error { return nil }
