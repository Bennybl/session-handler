package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Bennybl/session-handler/internal/postgres/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const migrationTimeout = 30 * time.Second

func main() {
	if err := runMain(); err != nil {
		log.Printf("migration failed: %v", err)
		os.Exit(1)
	}
}

func runMain() error {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalContext, migrationTimeout)
	defer cancel()

	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open migration database: %w", err)
	}
	defer database.Close()
	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("connect migration database: %w", err)
	}
	if err := migrations.Apply(ctx, database); err != nil {
		return err
	}
	return nil
}
