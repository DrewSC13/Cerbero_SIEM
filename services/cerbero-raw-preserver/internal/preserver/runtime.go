package preserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Run composes the DEVELOPMENT filesystem Raw Store runtime and blocks until ctx is canceled.
func Run(ctx context.Context, config RuntimeConfig, logger *slog.Logger) error {
	if err := config.Validate(); err != nil {
		return err
	}
	if logger == nil {
		logger = slog.Default()
	}

	rawStore, err := NewFilesystemRawStore(config.RawStorePath)
	if err != nil {
		return fmt.Errorf("configure filesystem Raw Store: %w", err)
	}

	database, err := sql.Open("pgx", config.postgresDSN())
	if err != nil {
		return fmt.Errorf("open PostgreSQL: %w", err)
	}
	defer database.Close()

	startupCtx, cancelStartup := context.WithTimeout(ctx, config.ConnectTimeout)
	defer cancelStartup()
	if err := database.PingContext(startupCtx); err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}

	metadata, err := NewPostgresMetadataStore(database)
	if err != nil {
		return err
	}

	connection, err := nats.Connect(
		config.NATSURL,
		nats.UserInfo(config.NATSUser, config.NATSPassword),
		nats.Name("cerbero-raw-preserver"),
		nats.Timeout(config.ConnectTimeout),
	)
	if err != nil {
		return fmt.Errorf("connect NATS: %w", err)
	}
	defer connection.Close()

	js, err := jetstream.New(connection)
	if err != nil {
		return fmt.Errorf("create JetStream client: %w", err)
	}
	publisher, err := NewJetStreamPublisher(js)
	if err != nil {
		return err
	}
	builder, err := NewRawPersistedBuilder(RawPersistedBuilderConfig{
		ComponentVersion: config.ComponentVersion,
		InstanceID:       config.InstanceID,
		Clock:            runtimeClock{},
		IDs:              NewUUIDv7Generator(nil, nil),
	})
	if err != nil {
		return fmt.Errorf("configure raw.persisted builder: %w", err)
	}
	core, err := New(Config{
		ConsumerName: RawPreserverConsumerName,
		RawStore:     rawStore,
		Metadata:     metadata,
		Builder:      builder,
		Publisher:    publisher,
	})
	if err != nil {
		return fmt.Errorf("configure raw preservation core: %w", err)
	}

	consumer, err := js.CreateOrUpdateConsumer(startupCtx, RawStreamName, jetstream.ConsumerConfig{
		Name:          RawPreserverConsumerName,
		Durable:       RawPreserverConsumerName,
		Description:   "CERBERO raw preservation durable consumer",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: RawReceivedSubject,
	})
	if err != nil {
		return fmt.Errorf("ensure durable raw-preserver consumer: %w", err)
	}

	handler, err := NewRawConsumer(
		core,
		logger,
		config.RetryMinDelay,
		config.RetryMaxDelay,
	)
	if err != nil {
		return err
	}

	consumeContext, err := consumer.Consume(
		func(message jetstream.Msg) {
			if err := handler.Handle(ctx, message); err != nil {
				logger.Error("raw-preserver message transport action failed", "error", err)
			}
		},
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			logger.Warn("raw-preserver JetStream consume error", "error", err)
		}),
	)
	if err != nil {
		return fmt.Errorf("start raw-preserver durable consumer: %w", err)
	}

	logger.Info(
		"cerbero raw-preserver ready",
		"consumer", RawPreserverConsumerName,
		"subject", RawReceivedSubject,
		"raw_store", config.RawStorePath,
	)

	select {
	case <-ctx.Done():
		consumeContext.Stop()
		<-consumeContext.Closed()
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		return ctx.Err()
	case <-consumeContext.Closed():
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("raw-preserver durable consumer stopped unexpectedly")
	}
}

// WaitForRuntimeDependencies is a small readiness helper used by integration tests and future health wiring.
func WaitForRuntimeDependencies(ctx context.Context, database *sql.DB, js jetstream.JetStream) error {
	if database == nil {
		return errors.New("PostgreSQL database is required")
	}
	if js == nil {
		return errors.New("JetStream client is required")
	}
	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("PostgreSQL not ready: %w", err)
	}
	if _, err := js.Consumer(ctx, RawStreamName, RawPreserverConsumerName); err != nil {
		return fmt.Errorf("raw-preserver consumer not ready: %w", err)
	}
	return nil
}
