package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cerbero/services/cerbero-ingest/internal/sourceapp"
	"cerbero/services/cerbero-ingest/internal/syslogingest"
)

const sourcePrefix = "CERBERO_SYSLOG"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Getenv, logger); err != nil {
		logger.Error("cerbero syslog frontend stopped with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	common, err := sourceapp.LoadConfig(getenv, syslogingest.Transport, sourcePrefix)
	if err != nil {
		return err
	}
	listenAddress := strings.TrimSpace(getenv("CERBERO_SYSLOG_LISTEN_ADDRESS"))
	containerDevelopment := parseBool(getenv("CERBERO_SYSLOG_CONTAINER_DEVELOPMENT"))
	if err := validateDevelopmentBind(listenAddress, containerDevelopment); err != nil {
		return err
	}
	connectionRate, err := parsePositiveUint(
		"CERBERO_INGEST_MAX_CONNECTION_RATE",
		getenv("CERBERO_INGEST_MAX_CONNECTION_RATE"),
	)
	if err != nil {
		return err
	}
	concurrent, err := parsePositiveUint(
		"CERBERO_INGEST_CONCURRENT_CONNECTIONS",
		getenv("CERBERO_INGEST_CONCURRENT_CONNECTIONS"),
	)
	if err != nil {
		return err
	}
	readTimeout, err := time.ParseDuration(strings.TrimSpace(getenv("CERBERO_INGEST_READ_TIMEOUT")))
	if err != nil || readTimeout <= 0 {
		return errors.New("CERBERO_INGEST_READ_TIMEOUT must be a positive Go duration")
	}

	runtime, err := sourceapp.Open(common)
	if err != nil {
		return err
	}
	defer runtime.Close()

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("listen for syslog TCP on %s: %w", listenAddress, err)
	}
	logger.Warn(
		"CERBERO DEVELOPMENT syslog TCP frontend enabled",
		"listen_address", listener.Addr().String(),
		"framing", "RFC6587-octet-counting",
	)

	return syslogingest.ServeTCP(ctx, listener, syslogingest.TCPConfig{
		Preparer:              runtime.Core,
		Acceptor:              runtime.Acceptor,
		Metadata:              runtime.Metadata,
		MaxFrameSize:          common.MaxPayloadSize,
		ReadTimeout:           readTimeout,
		MaxConnectionRate:     connectionRate,
		ConcurrentConnections: concurrent,
	})
}

func validateDevelopmentBind(address string, containerDevelopment bool) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("CERBERO_SYSLOG_LISTEN_ADDRESS must be host:port: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return errors.New("CERBERO_SYSLOG_LISTEN_ADDRESS must use an explicit IP address")
	}
	if ip.IsLoopback() {
		return nil
	}
	if containerDevelopment && ip.IsUnspecified() {
		return nil
	}
	return errors.New(
		"DEVELOPMENT syslog must bind to loopback unless CERBERO_SYSLOG_CONTAINER_DEVELOPMENT=1 permits an unspecified container address",
	)
}

func parsePositiveUint(name, value string) (uint64, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func parseBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
