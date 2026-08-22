package eventstream

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

type applierFunc func(context.Context, service.Event) error

func (fn applierFunc) ApplyEvent(ctx context.Context, event service.Event) error {
	return fn(ctx, event)
}

func TestCanonicalFullKeyRouting(t *testing.T) {
	dispatcher := newDispatcher(t, applierFunc(func(context.Context, service.Event) error { return nil }), Options{PartitionCount: 17, QueueCapacity: 1, RetryAttempts: 1})
	first := trustedEvent(t, sessiontest.EventID(1), "tenant-a", "alice", "2001:0db8:0:0::1")
	second := trustedEvent(t, sessiontest.EventID(2), "tenant-a", "alice", "2001:db8::1")
	if first.Key != second.Key || dispatcher.PartitionFor(first) != dispatcher.PartitionFor(second) {
		t.Fatalf("equivalent keys routed differently: %+v %+v", first.Key, second.Key)
	}
	base := dispatcher.PartitionFor(first)
	different := false
	for index := 0; index < 100; index++ {
		candidate := trustedEvent(t, sessiontest.EventID(10+index), "tenant-b", string(rune('a'+index%26)), "192.0.2.10")
		if dispatcher.PartitionFor(candidate) != base {
			different = true
			break
		}
	}
	if !different {
		t.Fatal("could not find a different full key routed to another partition")
	}
}

func TestRetryBlocksSamePartitionButNotAnother(t *testing.T) {
	var mu sync.Mutex
	calls := make([]string, 0)
	firstFailed := make(chan struct{})
	allowRetry := make(chan struct{})
	attempts := 0
	applier := applierFunc(func(_ context.Context, event service.Event) error {
		mu.Lock()
		calls = append(calls, event.EventID)
		mu.Unlock()
		if event.EventID == sessiontest.EventID(1) {
			attempts++
			if attempts == 1 {
				close(firstFailed)
				<-allowRetry
				return errors.New("temporary")
			}
		}
		return nil
	})
	dispatcher := newDispatcher(t, applier, Options{PartitionCount: 8, QueueCapacity: 4, RetryAttempts: 2, RetryDelay: time.Millisecond})
	first := trustedEvent(t, sessiontest.EventID(1), "tenant-a", "alice", "192.0.2.10")
	same := trustedEvent(t, sessiontest.EventID(2), "tenant-a", "alice", "192.0.2.10")
	other := eventOnDifferentPartition(t, dispatcher, first)
	results := make(chan error, 3)
	go func() { results <- dispatcher.Submit(context.Background(), first) }()
	<-firstFailed
	go func() { results <- dispatcher.Submit(context.Background(), same) }()
	go func() { results <- dispatcher.Submit(context.Background(), other) }()
	deadline := time.After(time.Second)
	for {
		mu.Lock()
		otherRan := contains(calls, other.EventID)
		sameRan := contains(calls, same.EventID)
		mu.Unlock()
		if sameRan {
			t.Fatal("later same-partition event overtook retry")
		}
		if otherRan {
			break
		}
		select {
		case <-deadline:
			t.Fatal("different partition did not progress")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(allowRetry)
	for range 3 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if got := calls[len(calls)-1]; got != same.EventID {
		t.Fatalf("last call = %q, want same-partition successor %q; calls=%v", got, same.EventID, calls)
	}
}

func TestOneWorkerProcessesPartitionSequentially(t *testing.T) {
	var active, maximum atomic.Int32
	applier := applierFunc(func(context.Context, service.Event) error {
		current := active.Add(1)
		for observed := maximum.Load(); current > observed && !maximum.CompareAndSwap(observed, current); observed = maximum.Load() {
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		return nil
	})
	dispatcher := newDispatcher(t, applier, Options{PartitionCount: 1, QueueCapacity: 32, RetryAttempts: 1})
	var submissions sync.WaitGroup
	for index := 0; index < 20; index++ {
		event := trustedEvent(t, sessiontest.EventID(300+index), "tenant-a", "alice", "192.0.2.10")
		submissions.Add(1)
		go func() {
			defer submissions.Done()
			if err := dispatcher.Submit(context.Background(), event); err != nil {
				t.Errorf("Submit() error = %v", err)
			}
		}()
	}
	submissions.Wait()
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent applies in one partition = %d, want 1", maximum.Load())
	}
}

func TestUpdateCannotOvertakeLoginForSameKey(t *testing.T) {
	loginEntered := make(chan struct{})
	releaseLogin := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseLogin) })
	updateEntered := make(chan struct{})
	var orderMu sync.Mutex
	var order []service.EventType
	applier := applierFunc(func(_ context.Context, event service.Event) error {
		orderMu.Lock()
		order = append(order, event.Type)
		orderMu.Unlock()
		if event.Type == service.EventLogin {
			close(loginEntered)
			<-releaseLogin
		} else {
			close(updateEntered)
		}
		return nil
	})
	dispatcher := newDispatcher(t, applier, Options{PartitionCount: 4, QueueCapacity: 2, RetryAttempts: 1})
	login := trustedEvent(t, sessiontest.EventID(40), "tenant-a", "alice", "192.0.2.10")
	login.Type = service.EventLogin
	update := trustedEvent(t, sessiontest.EventID(41), "tenant-a", "alice", "192.0.2.10")
	results := make(chan error, 2)
	go func() { results <- dispatcher.Submit(context.Background(), login) }()
	sessiontest.Await(t, loginEntered, "LOGIN service processing")
	go func() { results <- dispatcher.Submit(context.Background(), update) }()

	deadline := time.Now().Add(time.Second)
	queue := dispatcher.queues[dispatcher.PartitionFor(update)]
	for len(queue) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(queue) == 0 {
		t.Fatal("UPDATE was not queued behind LOGIN")
	}
	if !sessiontest.Blocked(updateEntered) {
		t.Fatal("UPDATE entered service processing before LOGIN completed")
	}
	releaseOnce.Do(func() { close(releaseLogin) })
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if !reflect.DeepEqual(order, []service.EventType{service.EventLogin, service.EventUpdate}) {
		t.Fatalf("service order = %v, want LOGIN then UPDATE", order)
	}
}

func TestBoundedBackpressureCancellationAndDrain(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	dispatcher := newDispatcher(t, applierFunc(func(context.Context, service.Event) error { once.Do(func() { close(entered); <-release }); return nil }), Options{PartitionCount: 1, QueueCapacity: 1, RetryAttempts: 1})
	first := trustedEvent(t, sessiontest.EventID(1), "t", "one", "192.0.2.1")
	second := trustedEvent(t, sessiontest.EventID(2), "t", "two", "192.0.2.2")
	third := trustedEvent(t, sessiontest.EventID(3), "t", "three", "192.0.2.3")
	fourth := trustedEvent(t, sessiontest.EventID(4), "t", "four", "192.0.2.4")
	results := make(chan error, 2)
	go func() {
		results <- dispatcher.Submit(context.Background(), first)
	}()
	<-entered
	go func() {
		results <- dispatcher.Submit(context.Background(), second)
	}()
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := dispatcher.Submit(ctx, third)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("backpressured Submit error = %v", err)
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	if err := dispatcher.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Submit(context.Background(), fourth); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-shutdown Submit error = %v", err)
	}
}

func newDispatcher(t *testing.T, applier EventApplier, options Options) *Dispatcher {
	t.Helper()
	dispatcher, err := NewDispatcher(applier, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = dispatcher.Shutdown(ctx)
	})
	return dispatcher
}

func trustedEvent(t *testing.T, id, tenant, username, ip string) service.Event {
	t.Helper()
	event, err := service.NewEvent(service.EventInput{EventID: id, Type: service.EventUpdate, TenantID: tenant, Username: username, IP: ip, Timestamp: sessiontest.At("10:00")})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func eventOnDifferentPartition(t *testing.T, dispatcher *Dispatcher, base service.Event) service.Event {
	t.Helper()
	partition := dispatcher.PartitionFor(base)
	for index := 20; index < 200; index++ {
		candidate := trustedEvent(t, sessiontest.EventID(index), "tenant-b", string(rune('a'+index%26)), "192.0.2.20")
		if dispatcher.PartitionFor(candidate) != partition {
			return candidate
		}
	}
	t.Fatal("no different partition found")
	return service.Event{}
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
