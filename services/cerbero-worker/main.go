package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

const componentName = "cerbero-worker"
const architectureBaseline = "v1.0"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config, err := loadWorkerRuntimeConfig(os.Getenv)
	if err != nil {
		logger.Error("invalid cerbero-worker startup configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runWorker(ctx, config, logger); err != nil {
		logger.Error("cerbero-worker stopped with error", "error", err)
		os.Exit(1)
	}
}
