package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"cerbero/services/cerbero-raw-preserver/internal/preserver"
)

const (
	componentName        = "cerbero-raw-preserver"
	architectureBaseline = "v1.0"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config, err := preserver.LoadRuntimeConfig(os.Getenv)
	if err != nil {
		logger.Error("invalid cerbero-raw-preserver startup configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := preserver.Run(ctx, config, logger); err != nil {
		logger.Error("cerbero-raw-preserver stopped with error", "error", err)
		os.Exit(1)
	}
}
