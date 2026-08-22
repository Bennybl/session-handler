package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

func TestNewEventNormalizesOnce(t *testing.T) {
	tags := []string{"user", "user"}
	event, err := NewEvent(EventInput{EventID: "E0000000-0000-4000-8000-000000000001", Type: " login ", TenantID: "tenant-a", Username: "alice", IP: "2001:0db8:0:0::1", Tags: tags, Timestamp: sessiontest.At("10:00")})
	if err != nil {
		t.Fatal(err)
	}
	if event.EventID != "e0000000-0000-4000-8000-000000000001" || event.Type != EventLogin || event.Key.IP != "2001:db8::1" || event.Timestamp.Location().String() != "UTC" {
		t.Fatalf("normalized event = %+v", event)
	}
	tags[0] = "changed"
	if !reflect.DeepEqual(event.Tags, []string{"user", "user"}) {
		t.Fatalf("tags were not copied: %v", event.Tags)
	}
	for _, input := range []EventInput{{Type: EventLogin}, {EventID: sessiontest.EventID(1), Type: "other", TenantID: "t", Username: "u", IP: "127.0.0.1", Timestamp: sessiontest.At("10:00")}, {EventID: sessiontest.EventID(1), Type: EventLogin, TenantID: "t", Username: "u", IP: "bad", Timestamp: sessiontest.At("10:00")}} {
		if _, err := NewEvent(input); !errors.Is(err, session.ErrInvalidInput) {
			t.Errorf("input %+v error = %v", input, err)
		}
	}
}

func TestApplyEventLoadsDecidesAndApplies(t *testing.T) {
	event := mustEvent(t, EventInput{EventID: sessiontest.EventID(1), Type: EventLogin, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10", Tags: []string{"user", "admin", "user"}, Timestamp: sessiontest.At("10:00")})
	repo := &repositoryStub{}
	application, err := New(Dependencies{Repository: repo, NewSessionID: func() (string, error) { return sessiontest.SessionID(1), nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.ApplyEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if repo.key != event.Key || !reflect.DeepEqual(repo.calls, []string{"load", "apply"}) {
		t.Fatalf("key=%+v calls=%v", repo.key, repo.calls)
	}
	started := repo.mutation.(session.StartSession)
	if !reflect.DeepEqual(started.Session.States[0].Tags, []string{"admin", "user"}) {
		t.Fatalf("tags = %v", started.Session.States[0].Tags)
	}
}

func TestApplyEventPreservesDomainErrorsAndDoesNotPersist(t *testing.T) {
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	latest := sessiontest.At("11:00")
	tests := []struct {
		name     string
		snapshot session.CurrentSessionSnapshot
		at       time.Time
		want     error
	}{
		{name: "invalid transition", at: sessiontest.At("10:00"), want: session.ErrInvalidTransition},
		{name: "stale event", snapshot: session.CurrentSessionSnapshot{LastEventAt: &latest}, at: sessiontest.At("10:00"), want: session.ErrStaleEvent},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := &repositoryStub{snapshot: test.snapshot}
			application, err := New(Dependencies{Repository: repo})
			if err != nil {
				t.Fatal(err)
			}
			event := Event{EventID: sessiontest.EventID(index + 10), Type: EventUpdate, Key: key, Timestamp: test.at}
			if err := application.ApplyEvent(context.Background(), event); !errors.Is(err, test.want) {
				t.Fatalf("ApplyEvent() error = %v, want %v", err, test.want)
			}
			if !reflect.DeepEqual(repo.calls, []string{"load"}) {
				t.Fatalf("repository calls = %v, want only LoadCurrent", repo.calls)
			}
		})
	}
}

func TestApplyEventPersistsDuplicateAsNoopMutation(t *testing.T) {
	eventID := sessiontest.EventID(20)
	repo := &repositoryStub{snapshot: session.CurrentSessionSnapshot{LastEventID: eventID}}
	application, err := New(Dependencies{Repository: repo})
	if err != nil {
		t.Fatal(err)
	}
	event := Event{EventID: eventID, Type: EventUpdate, Key: sessiontest.Key("tenant-a", "alice", "192.0.2.10"), Timestamp: sessiontest.At("09:00")}
	if err := application.ApplyEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if _, ok := repo.mutation.(session.DuplicateEvent); !ok {
		t.Fatalf("mutation = %T, want session.DuplicateEvent", repo.mutation)
	}
}

func mustEvent(t *testing.T, input EventInput) Event {
	t.Helper()
	event, err := NewEvent(input)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

type repositoryStub struct {
	key      session.SessionKey
	snapshot session.CurrentSessionSnapshot
	mutation session.Mutation
	calls    []string
}

func (r *repositoryStub) LoadCurrent(_ context.Context, key session.SessionKey) (session.CurrentSessionSnapshot, error) {
	r.key = key
	r.calls = append(r.calls, "load")
	return r.snapshot, nil
}
func (r *repositoryStub) ApplyMutation(_ context.Context, key session.SessionKey, mutation session.Mutation) error {
	r.key = key
	r.calls = append(r.calls, "apply")
	r.mutation = mutation
	return nil
}
func (*repositoryStub) Query(context.Context, session.QuerySpec) (session.QueryResult, error) {
	return session.QueryResult{}, nil
}
func (*repositoryStub) Close() error { return nil }
