package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	appconfig "github.com/Bennybl/session-handler/internal/config"
	"github.com/Bennybl/session-handler/internal/eventstream"
	kafkaevent "github.com/Bennybl/session-handler/internal/eventstream/kafka"
	"github.com/Bennybl/session-handler/internal/httpapi"
	"github.com/Bennybl/session-handler/internal/repository/sqlite"
	"github.com/Bennybl/session-handler/internal/service"
)

func run(ctx context.Context, configuration appconfig.Config, logger *log.Logger) error {
	return runWithSourceFactory(ctx, configuration, logger, newKafkaSource)
}

type sourceFactory func(context.Context, appconfig.Config) (eventstream.Source, error)

func runWithSourceFactory(ctx context.Context, configuration appconfig.Config, logger *log.Logger, createSource sourceFactory) error {
	if ctx == nil {
		return fmt.Errorf("runtime context is required")
	}
	if logger == nil {
		return fmt.Errorf("runtime logger is required")
	}
	if createSource == nil {
		return fmt.Errorf("event source factory is required")
	}
	startupContext, cancelStartup := context.WithTimeout(ctx, configuration.StartupTimeout)
	defer cancelStartup()
	repo, err := sqlite.Open(startupContext)
	if err != nil {
		return err
	}
	application, err := service.New(service.Dependencies{Repository: repo})
	if err != nil {
		return errors.Join(err, repo.Close())
	}
	queryHandler, err := httpapi.NewHandler(application)
	if err != nil {
		return errors.Join(err, repo.Close())
	}
	source, err := createSource(startupContext, configuration)
	if err != nil {
		return errors.Join(err, repo.Close())
	}
	if source == nil {
		return errors.Join(fmt.Errorf("event source factory returned nil"), repo.Close())
	}
	processor, err := eventstream.NewProcessor(application, logger, eventstream.ProcessorOptions{RetryAttempts: configuration.EventRetryAttempts, RetryDelay: configuration.EventRetryDelay})
	if err != nil {
		return errors.Join(err, source.Close(), repo.Close())
	}
	cancelStartup()
	readiness := &atomic.Bool{}
	server := &http.Server{Addr: configuration.HTTPAddr, Handler: operationalHandler(queryHandler, readiness), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.ListenAndServe() }()
	sourceResult := make(chan error, 1)
	go func() { sourceResult <- source.Consume(ctx, processor.Handle) }()
	readiness.Store(true)
	var runError error
	sourceStopped := false
	select {
	case <-ctx.Done():
	case sourceError := <-sourceResult:
		sourceStopped = true
		if sourceError != nil {
			runError = sourceError
		} else if ctx.Err() == nil {
			runError = fmt.Errorf("event source stopped unexpectedly")
		}
	case serverError := <-serverResult:
		if !errors.Is(serverError, http.ErrServerClosed) {
			runError = fmt.Errorf("HTTP server stopped: %w", serverError)
		}
	}
	readiness.Store(false)
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), configuration.ShutdownTimeout)
	defer cancelShutdown()
	serverError := server.Shutdown(shutdownContext)
	if errors.Is(serverError, http.ErrServerClosed) {
		serverError = nil
	}
	sourceError := shutdownSource(shutdownContext, source, sourceResult, sourceStopped)
	return errors.Join(runError, serverError, sourceError, repo.Close())
}

func newKafkaSource(ctx context.Context, configuration appconfig.Config) (eventstream.Source, error) {
	return kafkaevent.New(ctx, kafkaevent.Options{
		Brokers: splitBrokers(configuration.KafkaBrokers), Topic: configuration.KafkaTopic,
		GroupID: configuration.KafkaConsumerGroup, ClientID: configuration.KafkaClientID,
		MaxPollRecords: configuration.KafkaMaxPollRecords,
	})
}

func splitBrokers(value string) []string {
	parts := strings.Split(value, ",")
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts
}

func shutdownSource(ctx context.Context, source eventstream.Source, result <-chan error, alreadyStopped bool) error {
	if source == nil {
		return nil
	}
	closeError := source.Close()
	if alreadyStopped {
		return closeError
	}
	select {
	case runError := <-result:
		return errors.Join(closeError, runError)
	case <-ctx.Done():
		return errors.Join(closeError, ctx.Err())
	}
}

func operationalHandler(queryHandler http.Handler, readiness *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeStatus(w, http.StatusOK, "ok") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !readiness.Load() {
			writeStatus(w, http.StatusServiceUnavailable, "not ready")
			return
		}
		writeStatus(w, http.StatusOK, "ready")
	})
	mux.Handle("POST /v1/sessions/query", queryHandler)
	return mux
}

func writeStatus(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": value})
}
