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
)

const (
	serviceSessionID = "00000000-0000-4000-8000-000000000901"
	serviceEvent1ID  = "90000000-0000-4000-8000-000000000001"
	serviceEvent2ID  = "90000000-0000-4000-8000-000000000002"
)

func TestApplyEventDispatchesAndNormalizes(t *testing.T) {
	t.Parallel()
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	canonicalKey := mustKey(t, "tenant-a", "alice", "2001:db8::1")

	tests := []struct {
		name     string
		typeName EventType
		snapshot session.CurrentSessionSnapshot
		wantKind session.MutationKind
	}{
		{name: "login", typeName: EventType(" login "), wantKind: session.MutationStartSession},
		{
			name: "update", typeName: EventType("update"), wantKind: session.MutationReplaceState,
			snapshot: activeSnapshot(canonicalKey, loginAt, serviceEvent1ID),
		},
		{
			name: "logout", typeName: EventType("LOGOUT"), wantKind: session.MutationEndSession,
			snapshot: activeSnapshot(canonicalKey, loginAt, serviceEvent1ID),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var gotKey session.SessionKey
			var gotMutation session.Mutation
			repo := &repositoryStub{mutate: func(_ context.Context, key session.SessionKey, fn repository.MutationFunc) error {
				gotKey = key
				var err error
				gotMutation, err = fn(tt.snapshot)
				return err
			}}
			idCalls := 0
			service := mustService(t, Dependencies{
				Repository: repo,
				NewSessionID: func() (string, error) {
					idCalls++
					return serviceSessionID, nil
				},
			})

			err := service.ApplyEvent(context.Background(), Event{
				EventID: "90000000-0000-4000-8000-000000000002",
				Type:    tt.typeName, TenantID: "tenant-a", Username: "alice",
				IP: "2001:0db8:0:0:0:0:0:1", Tags: []string{"user", "admin", "user"}, Timestamp: loginAt.Add(time.Minute),
			})
			if err != nil {
				t.Fatalf("ApplyEvent() error = %v", err)
			}
			if gotKey != canonicalKey {
				t.Fatalf("mutation key = %+v, want %+v", gotKey, canonicalKey)
			}
			if gotMutation == nil || gotMutation.Kind() != tt.wantKind || gotMutation.AcceptedEventID() != serviceEvent2ID {
				t.Fatalf("mutation = %#v, want kind %q event ID %q", gotMutation, tt.wantKind, serviceEvent2ID)
			}
			if tt.wantKind == session.MutationStartSession {
				started := gotMutation.(session.StartSession)
				if started.Session.ID != serviceSessionID || !reflect.DeepEqual(started.Session.States[0].Tags, []string{"admin", "user"}) {
					t.Fatalf("start mutation = %+v", started)
				}
				if idCalls != 1 {
					t.Fatalf("session ID calls = %d, want 1", idCalls)
				}
			} else if idCalls != 0 {
				t.Fatalf("session ID calls = %d, want 0", idCalls)
			}
		})
	}
}

func TestApplyEventCreatesOneStableSessionIDOutsideRepositoryRetries(t *testing.T) {
	t.Parallel()
	generated := 0
	var ids []string
	repo := &repositoryStub{mutate: func(_ context.Context, _ session.SessionKey, fn repository.MutationFunc) error {
		for range 2 {
			mutation, err := fn(session.CurrentSessionSnapshot{})
			if err != nil {
				return err
			}
			ids = append(ids, mutation.(session.StartSession).Session.ID)
		}
		return nil
	}}
	service := mustService(t, Dependencies{
		Repository: repo,
		NewSessionID: func() (string, error) {
			generated++
			return serviceSessionID, nil
		},
	})

	err := service.ApplyEvent(context.Background(), Event{
		EventID: serviceEvent1ID, Type: EventLogin, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
		Tags: []string{"user"}, Timestamp: mustTime(t, "2026-08-21T10:00:00Z"),
	})
	if err != nil {
		t.Fatalf("ApplyEvent() error = %v", err)
	}
	if generated != 1 || !reflect.DeepEqual(ids, []string{serviceSessionID, serviceSessionID}) {
		t.Fatalf("generated = %d, callback IDs = %v", generated, ids)
	}
}

func TestApplyEventPropagatesSessionIDGenerationFailureWithoutMutation(t *testing.T) {
	t.Parallel()
	generationError := errors.New("entropy unavailable")
	repo := &repositoryStub{}
	service := mustService(t, Dependencies{
		Repository: repo,
		NewSessionID: func() (string, error) {
			return "", generationError
		},
	})
	err := service.ApplyEvent(context.Background(), Event{
		EventID: serviceEvent1ID, Type: EventLogin, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
		Timestamp: mustTime(t, "2026-08-21T10:00:00Z"),
	})
	if !errors.Is(err, generationError) {
		t.Fatalf("ApplyEvent() error = %v, want generation error", err)
	}
	if repo.mutateCalls != 0 {
		t.Fatalf("repository mutation calls = %d, want 0", repo.mutateCalls)
	}
}

func TestDefaultSessionIDGeneratorCreatesCanonicalV4UUIDs(t *testing.T) {
	t.Parallel()
	first, err := newUUID()
	if err != nil {
		t.Fatalf("first newUUID() error = %v", err)
	}
	second, err := newUUID()
	if err != nil {
		t.Fatalf("second newUUID() error = %v", err)
	}
	if first == second {
		t.Fatalf("newUUID() repeated %q", first)
	}
	for _, value := range []string{first, second} {
		normalized, err := normalizeUUID(value)
		if err != nil || normalized != value || value[14] != '4' || !strings.ContainsRune("89ab", rune(value[19])) {
			t.Fatalf("newUUID() = %q, normalized %q, error %v", value, normalized, err)
		}
	}
}

func TestApplyEventTreatsLatestEventIDAsSuccessfulDuplicate(t *testing.T) {
	t.Parallel()
	at := mustTime(t, "2026-08-21T10:00:00Z")
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	var got session.Mutation
	repo := &repositoryStub{mutate: func(_ context.Context, _ session.SessionKey, fn repository.MutationFunc) error {
		var err error
		got, err = fn(session.CurrentSessionSnapshot{LastEventAt: &at, LastEventID: serviceEvent1ID})
		return err
	}}
	service := mustService(t, Dependencies{Repository: repo})

	err := service.ApplyEvent(context.Background(), Event{
		EventID: serviceEvent1ID, Type: EventUpdate, TenantID: key.TenantID, Username: key.Username, IP: key.IP,
		Tags: []string{"ignored"}, Timestamp: at.Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("ApplyEvent() duplicate error = %v", err)
	}
	if got == nil || got.Kind() != session.MutationDuplicateEvent {
		t.Fatalf("duplicate mutation = %#v", got)
	}
}

func TestApplyEventValidatesBeforeCallingRepository(t *testing.T) {
	t.Parallel()
	repo := &repositoryStub{}
	service := mustService(t, Dependencies{Repository: repo})
	valid := Event{
		EventID: serviceEvent1ID, Type: EventLogin, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
		Tags: []string{"user"}, Timestamp: mustTime(t, "2026-08-21T10:00:00Z"),
	}

	tests := []struct {
		name  string
		alter func(*Event)
	}{
		{name: "unknown type", alter: func(event *Event) { event.Type = "CONNECT" }},
		{name: "invalid key", alter: func(event *Event) { event.IP = "not-an-ip" }},
		{name: "invalid event ID", alter: func(event *Event) { event.EventID = "not-a-uuid" }},
		{name: "zero timestamp", alter: func(event *Event) { event.Timestamp = time.Time{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := valid
			tt.alter(&event)
			if err := service.ApplyEvent(context.Background(), event); !errors.Is(err, session.ErrInvalidInput) {
				t.Fatalf("ApplyEvent() error = %v, want ErrInvalidInput", err)
			}
		})
	}
	if repo.mutateCalls != 0 {
		t.Fatalf("repository mutation calls = %d, want 0", repo.mutateCalls)
	}
}

func TestQueryCapturesClockOnceAndForwardsGenericSpecification(t *testing.T) {
	t.Parallel()
	now := mustTime(t, "2026-08-21T12:00:00+03:00")
	clockCalls := 0
	request := QueryRequest{
		Filters: []session.Filter{{Field: "futureField", Operator: "futureOperator", Value: map[string]any{"opaque": true}}},
		Page:    session.PageRequest{Limit: 7, Cursor: "opaque-cursor"},
	}
	wantResult := session.QueryResult{Sessions: []session.Session{{ID: "second"}, {ID: "first"}}, NextCursor: "next"}
	var gotSpec session.QuerySpec
	repo := &repositoryStub{query: func(_ context.Context, spec session.QuerySpec) (session.QueryResult, error) {
		gotSpec = spec
		return wantResult, nil
	}}
	service := mustService(t, Dependencies{
		Repository: repo,
		Now: func() time.Time {
			clockCalls++
			return now
		},
	})

	got, err := service.Query(context.Background(), request)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if !reflect.DeepEqual(got, wantResult) {
		t.Fatalf("Query() = %+v, want repository order %+v", got, wantResult)
	}
	if clockCalls != 1 || !gotSpec.EvaluatedAt.Equal(now) || gotSpec.EvaluatedAt.Location() != time.UTC {
		t.Fatalf("clock calls = %d, evaluatedAt = %v", clockCalls, gotSpec.EvaluatedAt)
	}
	if !reflect.DeepEqual(gotSpec.Filters, request.Filters) || gotSpec.Page != request.Page {
		t.Fatalf("forwarded spec = %+v, want filters/page %+v", gotSpec, request)
	}
}

func TestQueryRejectsOnlyStructurallyInvalidFilters(t *testing.T) {
	t.Parallel()
	repo := &repositoryStub{}
	service := mustService(t, Dependencies{Repository: repo})
	invalid := []session.Filter{
		{Operator: "eq", Value: "value"},
		{Field: "tenantId", Value: "value"},
		{Field: "tenantId", Operator: "eq"},
	}
	for _, filter := range invalid {
		_, err := service.Query(context.Background(), QueryRequest{Filters: []session.Filter{filter}})
		if !errors.Is(err, repository.ErrInvalidQuery) {
			t.Fatalf("Query(%+v) error = %v, want ErrInvalidQuery", filter, err)
		}
	}
	if _, err := service.Query(context.Background(), QueryRequest{Page: session.PageRequest{Limit: -1}}); !errors.Is(err, repository.ErrInvalidQuery) {
		t.Fatalf("Query(negative limit) error = %v, want ErrInvalidQuery", err)
	}
	if repo.queryCalls != 0 {
		t.Fatalf("repository query calls = %d, want 0", repo.queryCalls)
	}
}

func TestServiceHonorsCancellationAndPropagatesRepositoryErrors(t *testing.T) {
	t.Parallel()
	repositoryError := errors.New("repository unavailable")
	repo := &repositoryStub{
		mutate: func(context.Context, session.SessionKey, repository.MutationFunc) error { return repositoryError },
		query: func(context.Context, session.QuerySpec) (session.QueryResult, error) {
			return session.QueryResult{}, repositoryError
		},
	}
	service := mustService(t, Dependencies{Repository: repo, NewSessionID: func() (string, error) { return serviceSessionID, nil }})
	event := Event{
		EventID: serviceEvent1ID, Type: EventLogin, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
		Timestamp: mustTime(t, "2026-08-21T10:00:00Z"),
	}
	if err := service.ApplyEvent(context.Background(), event); !errors.Is(err, repositoryError) {
		t.Fatalf("ApplyEvent() error = %v, want repository error", err)
	}
	if _, err := service.Query(context.Background(), QueryRequest{}); !errors.Is(err, repositoryError) {
		t.Fatalf("Query() error = %v, want repository error", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	mutationsBefore := repo.mutateCalls
	queriesBefore := repo.queryCalls
	if err := service.ApplyEvent(cancelled, event); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ApplyEvent() error = %v", err)
	}
	if _, err := service.Query(cancelled, QueryRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Query() error = %v", err)
	}
	if repo.mutateCalls != mutationsBefore || repo.queryCalls != queriesBefore {
		t.Fatal("cancelled operation reached repository")
	}
}

type repositoryStub struct {
	mutate      func(context.Context, session.SessionKey, repository.MutationFunc) error
	query       func(context.Context, session.QuerySpec) (session.QueryResult, error)
	mutateCalls int
	queryCalls  int
}

func (r *repositoryStub) Mutate(ctx context.Context, key session.SessionKey, fn repository.MutationFunc) error {
	r.mutateCalls++
	if r.mutate == nil {
		return nil
	}
	return r.mutate(ctx, key, fn)
}

func (r *repositoryStub) Query(ctx context.Context, spec session.QuerySpec) (session.QueryResult, error) {
	r.queryCalls++
	if r.query == nil {
		return session.QueryResult{}, nil
	}
	return r.query(ctx, spec)
}

func (*repositoryStub) Close() error { return nil }

func mustService(t *testing.T, dependencies Dependencies) *SessionService {
	t.Helper()
	service, err := New(dependencies)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

func activeSnapshot(key session.SessionKey, at time.Time, eventID string) session.CurrentSessionSnapshot {
	return session.CurrentSessionSnapshot{
		LastEventAt: &at,
		LastEventID: eventID,
		Active: &session.Session{
			ID: serviceSessionID, Key: key, LoginAt: at, LastEventID: eventID,
			States: []session.SessionState{{Tags: []string{"user"}, ValidFrom: at}},
		},
	}
}

func mustKey(t *testing.T, tenantID, username, ip string) session.SessionKey {
	t.Helper()
	key, err := session.NewSessionKey(tenantID, username, ip)
	if err != nil {
		t.Fatalf("NewSessionKey() error = %v", err)
	}
	return key
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("time.Parse(%q) error = %v", value, err)
	}
	return parsed
}
