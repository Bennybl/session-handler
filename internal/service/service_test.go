package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
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

func TestApplyEventUsesTrustedEventAndGuard(t *testing.T) {
	event := mustEvent(t, EventInput{EventID: sessiontest.EventID(1), Type: EventLogin, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10", Tags: []string{"user", "admin", "user"}, Timestamp: sessiontest.At("10:00")})
	repo := &repositoryStub{}
	guard := &guardStub{}
	application, err := New(Dependencies{Repository: repo, MutationGuard: guard, NewSessionID: func() (string, error) { return sessiontest.SessionID(1), nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.ApplyEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if guard.calls != 1 || repo.key != event.Key {
		t.Fatalf("guard calls=%d key=%+v", guard.calls, repo.key)
	}
	started := repo.mutation.(session.StartSession)
	if !reflect.DeepEqual(started.Session.States[0].Tags, []string{"admin", "user"}) {
		t.Fatalf("tags = %v", started.Session.States[0].Tags)
	}
}

func TestStripedGuardSerializesSameKey(t *testing.T) {
	guard, err := NewStripedMutationGuard(8)
	if err != nil {
		t.Fatal(err)
	}
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	entered, release := make(chan struct{}), make(chan struct{})
	go func() {
		_ = guard.Do(context.Background(), key, func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	secondAttempt, done := make(chan struct{}), make(chan struct{})
	go func() {
		close(secondAttempt)
		_ = guard.Do(context.Background(), key, func(context.Context) error { return nil })
		close(done)
	}()
	<-secondAttempt
	select {
	case <-done:
		t.Fatal("same-key guard did not serialize")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	<-done
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
	mutation session.Mutation
}

func (r *repositoryStub) Mutate(_ context.Context, key session.SessionKey, fn repository.MutationFunc) error {
	r.key = key
	mutation, err := fn(session.CurrentSessionSnapshot{})
	r.mutation = mutation
	return err
}
func (*repositoryStub) Query(context.Context, session.QuerySpec) (session.QueryResult, error) {
	return session.QueryResult{}, nil
}
func (*repositoryStub) Close() error { return nil }

type guardStub struct{ calls int }

func (g *guardStub) Do(ctx context.Context, _ session.SessionKey, fn func(context.Context) error) error {
	g.calls++
	return fn(ctx)
}
