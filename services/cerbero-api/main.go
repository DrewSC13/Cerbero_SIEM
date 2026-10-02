package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const componentName = "cerbero-api"
const architectureBaseline = "v1.0"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config, err := envConfig()
	if err != nil {
		logger.Error("configuration error", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store, err := newPostgresStore(ctx, config.dsn)
	if err != nil {
		logger.Error("postgres error", "error", err)
		os.Exit(1)
	}
	defer store.pool.Close()
	search, err := newClickHouseSearchStore(
		config.clickhouseURL,
		config.clickhouseDatabase,
		config.clickhouseUser,
		config.clickhousePassword,
	)
	if err != nil {
		logger.Error("clickhouse search configuration error", "error", err)
		os.Exit(1)
	}
	server := &http.Server{
		Addr:              config.listen,
		Handler:           newAPIServerWithSearch(store, search),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}
