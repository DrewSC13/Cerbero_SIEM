package ingestapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"cerbero/services/cerbero-ingest/internal/eventbus"
	"cerbero/services/cerbero-ingest/internal/httpingest"
	"cerbero/services/cerbero-ingest/internal/ingestcore"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const rawStreamName = "CERBERO_RAW"

// Run composes the DEVELOPMENT JSON/HTTP frontend with IngestCore and durable JetStream admission.
func Run(ctx context.Context, config Config, logger *slog.Logger) error {
	if err := config.Validate(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", config.ListenAddress, err)
	}
	return runWithListener(ctx, config, logger, listener)
}

func runWithListener(ctx context.Context, config Config, logger *slog.Logger, listener net.Listener) error {
	if err := config.Validate(); err != nil {
		_ = listener.Close()
		return err
	}
	if logger == nil {
		logger = slog.Default()
	}

	logger.Warn(
		"CERBERO insecure DEVELOPMENT ingest enabled",
		"listen_address", listener.Addr().String(),
		"ingest_path", config.IngestPath,
	)

	connection, err := nats.Connect(
		config.NATSURL,
		nats.UserInfo(config.NATSUser, config.NATSPassword),
		nats.Name("cerbero-ingest"),
		nats.Timeout(config.ReadTimeout),
	)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer connection.Close()

	js, err := jetstream.New(connection)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("create JetStream client: %w", err)
	}
	acceptor, err := eventbus.NewJetStreamAcceptor(js)
	if err != nil {
		_ = listener.Close()
		return err
	}

	identity := newDevelopmentIdentity(config)
	core, err := ingestcore.New(ingestcore.Config{
		MaxPayloadSize:   config.MaxPayloadSize,
		ComponentVersion: config.ComponentVersion,
		PipelineVersion:  config.PipelineVersion,
		InstanceID:       config.InstanceID,
		Authenticator:    identity,
		Authorizer:       identity,
		Limiter: &admissionRateLimiter{
			bucket: newTokenBucket(config.MaxEventsPerSecond, nil),
		},
	})
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("configure IngestCore: %w", err)
	}
	handler, err := httpingest.New(httpingest.Config{
		MaxPayloadSize: config.MaxPayloadSize,
		Preparer:       core,
		Acceptor:       acceptor,
		Metadata:       identity,
	})
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("configure HTTP ingest: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle(config.IngestPath, handler)
	mux.HandleFunc("/livez", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, request *http.Request) {
		if !connection.IsConnected() {
			http.Error(response, "not ready", http.StatusServiceUnavailable)
			return
		}
		readyCtx, cancel := context.WithTimeout(request.Context(), config.ReadTimeout)
		defer cancel()
		if _, err := js.Stream(readyCtx, rawStreamName); err != nil {
			http.Error(response, "not ready", http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Handler:           mux,
		ReadTimeout:       config.ReadTimeout,
		ReadHeaderTimeout: config.ReadTimeout,
		IdleTimeout:       config.IdleTimeout,
	}

	limited := newLimitedListener(listener, config.MaxConnectionRate, config.ConcurrentConnections)
	serveErr := make(chan error, 1)
	go func() {
		err := server.Serve(limited)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		if err := <-serveErr; err != nil {
			return err
		}
		return nil
	}
}
