package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	appconfig "github.com/Bennybl/session-handler/internal/config"
	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/httpapi"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/repository/memory"
	postgresrepository "github.com/Bennybl/session-handler/internal/repository/postgres"
	"github.com/Bennybl/session-handler/internal/service"
)

type natsSource interface {
	eventstream.Source
	eventstream.DeadLetterPublisher
}

type managedServer interface {
	Serve() error
	Shutdown(context.Context) error
}

type runtimeDependencies struct {
	stdin          io.Reader
	stderr         io.Writer
	newMemory      func() repository.SessionRepository
	openPostgres   func(context.Context, string) (repository.SessionRepository, error)
	newStdinSource func(io.Reader) (eventstream.Source, error)
	openNATSSource func(context.Context, string, eventstream.NATSConfig) (natsSource, error)
	newServer      func(string, http.Handler) managedServer
}

type netHTTPServer struct {
	server *http.Server
}

func (server *netHTTPServer) Serve() error {
	return server.server.ListenAndServe()
}

func (server *netHTTPServer) Shutdown(ctx context.Context) error {
	return server.server.Shutdown(ctx)
}

func defaultRuntimeDependencies() runtimeDependencies {
	return runtimeDependencies{
		stdin: os.Stdin, stderr: os.Stderr,
		newMemory: func() repository.SessionRepository { return memory.New() },
		openPostgres: func(ctx context.Context, databaseURL string) (repository.SessionRepository, error) {
			return postgresrepository.Open(ctx, databaseURL)
		},
		newStdinSource: func(reader io.Reader) (eventstream.Source, error) {
			return eventstream.NewStdinSource(reader)
		},
		openNATSSource: func(ctx context.Context, url string, config eventstream.NATSConfig) (natsSource, error) {
			return eventstream.OpenNATSSource(ctx, url, config)
		},
		newServer: func(address string, handler http.Handler) managedServer {
			return &netHTTPServer{server: &http.Server{
				Addr: address, Handler: handler,
				ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
				WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
			}}
		},
	}
}

func run(ctx context.Context, configuration appconfig.Config, dependencies runtimeDependencies) error {
	if ctx == nil {
		return fmt.Errorf("runtime context is required")
	}
	if err := configuration.Validate(); err != nil {
		return err
	}
	if err := dependencies.validate(); err != nil {
		return err
	}

	startupContext, cancelStartup := context.WithTimeout(ctx, configuration.StartupTimeout)
	repo, err := openRepository(startupContext, configuration, dependencies)
	if err != nil {
		cancelStartup()
		return err
	}
	source, deadLetter, err := openEventSource(startupContext, configuration, dependencies)
	cancelStartup()
	if err != nil {
		return errors.Join(err, repo.Close())
	}

	application, err := service.New(service.Dependencies{Repository: repo})
	if err != nil {
		return errors.Join(err, source.Close(), repo.Close())
	}
	consumer, err := eventstream.NewConsumer(source, application, deadLetter, eventstream.Options{})
	if err != nil {
		return errors.Join(err, source.Close(), repo.Close())
	}
	queryHandler, err := httpapi.NewHandler(application)
	if err != nil {
		return errors.Join(err, source.Close(), repo.Close())
	}

	readiness := &atomic.Bool{}
	server := dependencies.newServer(configuration.HTTPAddr, operationalHandler(queryHandler, readiness))
	if server == nil {
		return errors.Join(fmt.Errorf("HTTP server factory returned nil"), source.Close(), repo.Close())
	}
	return serve(ctx, configuration.ShutdownTimeout, consumer, source, repo, server, readiness)
}

func (dependencies runtimeDependencies) validate() error {
	if dependencies.stdin == nil || dependencies.stderr == nil || dependencies.newMemory == nil ||
		dependencies.openPostgres == nil || dependencies.newStdinSource == nil ||
		dependencies.openNATSSource == nil || dependencies.newServer == nil {
		return fmt.Errorf("runtime dependencies are incomplete")
	}
	return nil
}

func openRepository(ctx context.Context, configuration appconfig.Config, dependencies runtimeDependencies) (repository.SessionRepository, error) {
	switch configuration.StorageDriver {
	case appconfig.StorageMemory:
		repo := dependencies.newMemory()
		if repo == nil {
			return nil, fmt.Errorf("memory repository factory returned nil")
		}
		return repo, nil
	case appconfig.StoragePostgres:
		repo, err := dependencies.openPostgres(ctx, configuration.DatabaseURL)
		if err != nil {
			return nil, fmt.Errorf("open postgres repository: %w", err)
		}
		if repo == nil {
			return nil, fmt.Errorf("postgres repository factory returned nil")
		}
		return repo, nil
	default:
		return nil, fmt.Errorf("unsupported storage driver %q", configuration.StorageDriver)
	}
}

func openEventSource(ctx context.Context, configuration appconfig.Config, dependencies runtimeDependencies) (eventstream.Source, eventstream.DeadLetterPublisher, error) {
	switch configuration.EventStreamDriver {
	case appconfig.EventStreamStdin:
		source, err := dependencies.newStdinSource(dependencies.stdin)
		if err != nil {
			return nil, nil, fmt.Errorf("open stdin event source: %w", err)
		}
		if source == nil {
			return nil, nil, fmt.Errorf("stdin event source factory returned nil")
		}
		return source, &writerDeadLetterPublisher{writer: dependencies.stderr}, nil
	case appconfig.EventStreamNATS:
		source, err := dependencies.openNATSSource(ctx, configuration.NATSURL, eventstream.NATSConfig{
			StreamName: configuration.NATSStream, Subject: configuration.NATSSubject,
			ConsumerName: configuration.NATSConsumer, DeadLetterSubject: configuration.NATSDeadLetterSubject,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("open NATS event source: %w", err)
		}
		if source == nil {
			return nil, nil, fmt.Errorf("NATS event source factory returned nil")
		}
		return source, source, nil
	default:
		return nil, nil, fmt.Errorf("unsupported event stream driver %q", configuration.EventStreamDriver)
	}
}

func serve(
	ctx context.Context,
	shutdownTimeout time.Duration,
	consumer *eventstream.Consumer,
	source eventstream.Source,
	repo repository.SessionRepository,
	server managedServer,
	readiness *atomic.Bool,
) error {
	runtimeContext, cancelRuntime := context.WithCancel(ctx)
	consumerResult := make(chan error, 1)
	serverResult := make(chan error, 1)
	go func() { consumerResult <- consumer.Run(runtimeContext) }()
	go func() { serverResult <- server.Serve() }()
	readiness.Store(true)

	var runError error
	consumerDone := false
	serverDone := false
	running := true
	for running {
		select {
		case <-ctx.Done():
			running = false
		case err := <-consumerResult:
			consumerDone = true
			consumerResult = nil
			if err != nil && ctx.Err() == nil {
				runError = fmt.Errorf("event consumer stopped: %w", err)
				running = false
			} else if ctx.Err() != nil {
				running = false
			}
		case err := <-serverResult:
			serverDone = true
			serverResult = nil
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				runError = fmt.Errorf("HTTP server stopped: %w", err)
			}
			running = false
		}
	}

	readiness.Store(false)
	cancelRuntime()
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	shutdownError := server.Shutdown(shutdownContext)
	sourceError := source.Close()

	var waitError error
	if !consumerDone {
		select {
		case err := <-consumerResult:
			if err != nil && !errors.Is(err, context.Canceled) {
				waitError = errors.Join(waitError, fmt.Errorf("event consumer shutdown: %w", err))
			}
		case <-shutdownContext.Done():
			waitError = errors.Join(waitError, fmt.Errorf("event consumer shutdown: %w", shutdownContext.Err()))
		}
	}
	if !serverDone {
		select {
		case err := <-serverResult:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				waitError = errors.Join(waitError, fmt.Errorf("HTTP server shutdown: %w", err))
			}
		case <-shutdownContext.Done():
			waitError = errors.Join(waitError, fmt.Errorf("HTTP server shutdown: %w", shutdownContext.Err()))
		}
	}
	repositoryError := repo.Close()
	return errors.Join(runError, shutdownError, sourceError, waitError, repositoryError)
}

func operationalHandler(business http.Handler, readiness *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(responseWriter http.ResponseWriter, _ *http.Request) {
		writeStatus(responseWriter, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(responseWriter http.ResponseWriter, _ *http.Request) {
		if !readiness.Load() {
			writeStatus(responseWriter, http.StatusServiceUnavailable, "not ready")
			return
		}
		writeStatus(responseWriter, http.StatusOK, "ready")
	})
	mux.Handle("/", business)
	return mux
}

func writeStatus(responseWriter http.ResponseWriter, status int, value string) {
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(status)
	_ = json.NewEncoder(responseWriter).Encode(map[string]string{"status": value})
}

type writerDeadLetterPublisher struct {
	mu     sync.Mutex
	writer io.Writer
}

func (publisher *writerDeadLetterPublisher) PublishDeadLetter(ctx context.Context, letter eventstream.DeadLetter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if err := json.NewEncoder(publisher.writer).Encode(letter); err != nil {
		return fmt.Errorf("write stdin dead letter: %w", err)
	}
	return nil
}
