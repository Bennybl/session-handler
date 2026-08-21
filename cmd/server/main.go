package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	appconfig "github.com/Bennybl/session-handler/internal/config"
)

func main() {
	if err := runMain(); err != nil {
		log.Printf("session handler stopped: %v", err)
		os.Exit(1)
	}
}

func runMain() error {
	configuration, err := appconfig.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, configuration, defaultRuntimeDependencies())
}
