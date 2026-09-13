package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"cerbero/services/cerbero-ingest/internal/ingestapp"
)

const (
	componentName        = "cerbero-ingest"
	architectureBaseline = "v1.0"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config, err := ingestapp.LoadConfig(os.Getenv)
	if err != nil {
		logger.Error("invalid cerbero-ingest startup configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := ingestapp.Run(ctx, config, logger); err != nil {
		logger.Error("cerbero-ingest stopped with error", "error", err)
		os.Exit(1)
	}
}
