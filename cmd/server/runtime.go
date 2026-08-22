package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	appconfig "github.com/Bennybl/session-handler/internal/config"
	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/httpapi"
	"github.com/Bennybl/session-handler/internal/repository/sqlite"
	"github.com/Bennybl/session-handler/internal/service"
)

func run(ctx context.Context, configuration appconfig.Config, logger *log.Logger) error {
	if ctx == nil {
		return fmt.Errorf("runtime context is required")
	}
	if logger == nil {
		return fmt.Errorf("runtime logger is required")
	}
	startupContext, cancelStartup := context.WithTimeout(ctx, configuration.StartupTimeout)
	repo, err := sqlite.Open(startupContext)
	cancelStartup()
	if err != nil {
		return err
	}
	guard, err := mutationGuard(configuration)
	if err != nil {
		return errors.Join(err, repo.Close())
	}
	application, err := service.New(service.Dependencies{Repository: repo, MutationGuard: guard})
	if err != nil {
		return errors.Join(err, repo.Close())
	}
	dispatcher, err := eventstream.NewDispatcher(application, eventstream.Options{PartitionCount: configuration.PartitionCount, QueueCapacity: configuration.PartitionQueueCapacity, RetryAttempts: configuration.EventRetryAttempts, RetryDelay: configuration.EventRetryDelay})
	if err != nil {
		return errors.Join(err, repo.Close())
	}
	if err := dispatcher.Start(); err != nil {
		return errors.Join(err, repo.Close())
	}
	queryHandler, err := httpapi.NewHandler(application)
	if err != nil {
		return errors.Join(err, shutdownDispatcher(dispatcher, configuration.ShutdownTimeout), repo.Close())
	}
	eventHandler, err := httpapi.NewEventHandler(dispatcher, logger)
	if err != nil {
		return errors.Join(err, shutdownDispatcher(dispatcher, configuration.ShutdownTimeout), repo.Close())
	}
	readiness := &atomic.Bool{}
	server := &http.Server{Addr: configuration.HTTPAddr, Handler: operationalHandler(queryHandler, eventHandler, readiness), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.ListenAndServe() }()
	readiness.Store(true)
	var runError error
	select {
	case <-ctx.Done():
	case workerError := <-dispatcher.Failures():
		runError = workerError
	case serverError := <-serverResult:
		if !errors.Is(serverError, http.ErrServerClosed) {
			runError = fmt.Errorf("HTTP server stopped: %w", serverError)
		}
	}
	readiness.Store(false)
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), configuration.ShutdownTimeout)
	defer cancelShutdown()
	dispatcherError := dispatcher.Shutdown(shutdownContext)
	serverError := server.Shutdown(shutdownContext)
	if errors.Is(serverError, http.ErrServerClosed) {
		serverError = nil
	}
	return errors.Join(runError, dispatcherError, serverError, repo.Close())
}

func mutationGuard(configuration appconfig.Config) (service.MutationGuard, error) {
	if configuration.MutationGuard == appconfig.MutationGuardNone {
		return service.NoopMutationGuard{}, nil
	}
	return service.NewStripedMutationGuard(configuration.MutationGuardStripes)
}

func shutdownDispatcher(dispatcher *eventstream.Dispatcher, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return dispatcher.Shutdown(ctx)
}

func operationalHandler(queryHandler, eventHandler http.Handler, readiness *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeStatus(w, http.StatusOK, "ok") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !readiness.Load() {
			writeStatus(w, http.StatusServiceUnavailable, "not ready")
			return
		}
		writeStatus(w, http.StatusOK, "ready")
	})
	mux.Handle("POST /v1/events", eventHandler)
	mux.Handle("POST /v1/sessions/query", queryHandler)
	return mux
}

func writeStatus(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": value})
}
