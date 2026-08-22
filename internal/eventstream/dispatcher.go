package eventstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

var ErrClosed = errors.New("partition dispatcher is not accepting events")

type EventApplier interface {
	ApplyEvent(context.Context, service.Event) error
}

type Options struct {
	PartitionCount int
	QueueCapacity  int
	RetryAttempts  int
	RetryDelay     time.Duration
}

type job struct {
	ctx    context.Context
	event  service.Event
	result chan error
}

type Dispatcher struct {
	applier       EventApplier
	options       Options
	queues        []chan job
	workerContext context.Context
	cancelWorkers context.CancelFunc
	intakeClosed  chan struct{}
	failures      chan error
	workers       sync.WaitGroup
	submissions   sync.WaitGroup
	mu            sync.Mutex
	started       bool
	accepting     bool
	stopOnce      sync.Once
	closeOnce     sync.Once
}

func NewDispatcher(applier EventApplier, options Options) (*Dispatcher, error) {
	if applier == nil {
		return nil, fmt.Errorf("%w: event applier is required", session.ErrInvalidInput)
	}
	if options.PartitionCount <= 0 {
		return nil, fmt.Errorf("%w: partition count must be positive", session.ErrInvalidInput)
	}
	if options.QueueCapacity <= 0 {
		return nil, fmt.Errorf("%w: queue capacity must be positive", session.ErrInvalidInput)
	}
	if options.RetryAttempts <= 0 {
		return nil, fmt.Errorf("%w: retry attempts must be positive", session.ErrInvalidInput)
	}
	if options.RetryDelay < 0 {
		return nil, fmt.Errorf("%w: retry delay cannot be negative", session.ErrInvalidInput)
	}
	workerContext, cancelWorkers := context.WithCancel(context.Background())
	dispatcher := &Dispatcher{
		applier: applier, options: options, queues: make([]chan job, options.PartitionCount),
		workerContext: workerContext, cancelWorkers: cancelWorkers,
		intakeClosed: make(chan struct{}), failures: make(chan error, options.PartitionCount),
	}
	for index := range dispatcher.queues {
		dispatcher.queues[index] = make(chan job, options.QueueCapacity)
	}
	return dispatcher, nil
}

func (d *Dispatcher) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started {
		return fmt.Errorf("dispatcher already started")
	}
	d.started, d.accepting = true, true
	ready := make(chan struct{}, len(d.queues))
	for partitionID := range d.queues {
		d.workers.Add(1)
		go d.worker(partitionID, ready)
	}
	for range d.queues {
		<-ready
	}
	return nil
}

func (d *Dispatcher) PartitionFor(event service.Event) int {
	return int(service.HashSessionKey(event.Key) % uint64(len(d.queues)))
}

func (d *Dispatcher) Submit(ctx context.Context, event service.Event) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	d.mu.Lock()
	if !d.started || !d.accepting {
		d.mu.Unlock()
		return ErrClosed
	}
	d.submissions.Add(1)
	d.mu.Unlock()
	defer d.submissions.Done()

	job := job{ctx: ctx, event: event, result: make(chan error, 1)}
	queue := d.queues[d.PartitionFor(event)]
	select {
	case queue <- job:
	case <-ctx.Done():
		return ctx.Err()
	case <-d.intakeClosed:
		return ErrClosed
	}
	select {
	case err := <-job.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-d.workerContext.Done():
		return d.workerContext.Err()
	}
}

func (d *Dispatcher) Failures() <-chan error { return d.failures }

func (d *Dispatcher) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	d.stopOnce.Do(func() {
		d.mu.Lock()
		d.accepting = false
		close(d.intakeClosed)
		d.mu.Unlock()
	})
	submissionsDone := make(chan struct{})
	go func() { d.submissions.Wait(); close(submissionsDone) }()
	var shutdownError error
	select {
	case <-submissionsDone:
	case <-ctx.Done():
		shutdownError = ctx.Err()
		d.cancelWorkers()
		<-submissionsDone
	}
	d.closeOnce.Do(func() {
		for _, queue := range d.queues {
			close(queue)
		}
	})
	done := make(chan struct{})
	go func() { d.workers.Wait(); close(done) }()
	select {
	case <-done:
		return shutdownError
	case <-ctx.Done():
		d.cancelWorkers()
		<-done
		return ctx.Err()
	}
}

func (d *Dispatcher) worker(partitionID int, ready chan<- struct{}) {
	defer d.workers.Done()
	defer func() {
		if recovered := recover(); recovered != nil {
			d.cancelWorkers()
			d.failures <- fmt.Errorf("partition %d worker panicked: %v", partitionID, recovered)
		}
	}()
	ready <- struct{}{}
	for item := range d.queues[partitionID] {
		item.result <- d.apply(item)
	}
}

func (d *Dispatcher) apply(item job) error {
	applyContext, cancelApply := context.WithCancel(item.ctx)
	stopWorkerCancellation := context.AfterFunc(d.workerContext, cancelApply)
	defer func() { stopWorkerCancellation(); cancelApply() }()
	var err error
	for attempt := 1; attempt <= d.options.RetryAttempts; attempt++ {
		if err = contextError(item.ctx, d.workerContext); err != nil {
			return err
		}
		err = d.applier.ApplyEvent(applyContext, item.event)
		if err == nil || permanent(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if attempt == d.options.RetryAttempts {
			break
		}
		timer := time.NewTimer(d.options.RetryDelay)
		select {
		case <-timer.C:
		case <-item.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return item.ctx.Err()
		case <-d.workerContext.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return d.workerContext.Err()
		}
	}
	return fmt.Errorf("transient event failure after %d attempts: %w", d.options.RetryAttempts, err)
}

func permanent(err error) bool {
	return errors.Is(err, session.ErrInvalidInput) || errors.Is(err, session.ErrInvalidTransition) || errors.Is(err, session.ErrStaleEvent)
}

func contextError(request, worker context.Context) error {
	if err := request.Err(); err != nil {
		return err
	}
	return worker.Err()
}
