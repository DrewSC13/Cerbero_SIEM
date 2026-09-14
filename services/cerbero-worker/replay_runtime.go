package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const replayBudgetExhausted replayEligibilityReason = "BUDGET_EXHAUSTED"

const (
	rawStreamName                        = "CERBERO_RAW"
	dlqStreamName                        = "CERBERO_DLQ"
	normalizationDLQSubject              = "cerbero.v1.dlq.normalization"
	workerNormalizationDLQReplayConsumer = "worker-normalization-dlq-replay"
	workerFetchWait                      = time.Second
	workerConsumerAckWait                = 30 * time.Second
)

type replayDLQMessage interface {
	Data() []byte
	DoubleAck(context.Context) error
	NakWithDelay(time.Duration) error
}

type normalizationDLQReplayProcessor struct {
	policy   replayPolicy
	nakDelay time.Duration
	logger   *slog.Logger

	getSource      func(context.Context, uint64) (*jetstream.RawStreamMsg, error)
	reserveAttempt func(context.Context, replayReservationRequest) (replayReservation, error)
	publish        func(context.Context, *nats.Msg) (*jetstream.PubAck, error)
}

func runWorker(
	ctx context.Context,
	config workerRuntimeConfig,
	logger *slog.Logger,
) error {
	if logger == nil {
		return errors.New("worker logger is required")
	}
	if err := config.validate(); err != nil {
		return err
	}

	connection, err := nats.Connect(
		config.NATSURL,
		nats.UserInfo(config.NATSUser, config.NATSPassword),
		nats.Name(config.InstanceID),
		nats.Timeout(config.ConnectTimeout),
	)
	if err != nil {
		return fmt.Errorf("connect worker NATS: %w", err)
	}
	defer connection.Close()

	js, err := jetstream.New(connection)
	if err != nil {
		return fmt.Errorf("create worker JetStream context: %w", err)
	}

	dlqStream, err := js.Stream(ctx, dlqStreamName)
	if err != nil {
		return fmt.Errorf("open %s stream: %w", dlqStreamName, err)
	}
	consumer, err := dlqStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       workerNormalizationDLQReplayConsumer,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       workerConsumerAckWait,
		FilterSubject: normalizationDLQSubject,
		MaxAckPending: 1,
	})
	if err != nil {
		return fmt.Errorf("create normalization DLQ replay consumer: %w", err)
	}

	rawStream, err := js.Stream(ctx, rawStreamName)
	if err != nil {
		return fmt.Errorf("open %s stream: %w", rawStreamName, err)
	}

	db, err := sql.Open("pgx", config.postgresDSN())
	if err != nil {
		return fmt.Errorf("open worker PostgreSQL replay state: %w", err)
	}
	defer db.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, config.ConnectTimeout)
	defer cancelPing()
	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("ping worker PostgreSQL replay state: %w", err)
	}

	store, err := newPostgresReplayStateStore(db)
	if err != nil {
		return err
	}

	processor := normalizationDLQReplayProcessor{
		policy: replayPolicy{
			MaxAutomaticAttempts: config.MaxAutomaticAttempts,
		},
		nakDelay: config.NAKDelay,
		logger:   logger,
		getSource: func(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
			return rawStream.GetMsg(ctx, sequence)
		},
		reserveAttempt: store.reserveAttempt,
		publish: func(ctx context.Context, message *nats.Msg) (*jetstream.PubAck, error) {
			return js.PublishMsg(ctx, message)
		},
	}

	logger.Info(
		"normalization DLQ replay worker started",
		"consumer", workerNormalizationDLQReplayConsumer,
		"max_automatic_attempts", config.MaxAutomaticAttempts,
	)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		message, err := consumer.Next(jetstream.FetchMaxWait(workerFetchWait))
		if err != nil {
			if errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, nats.ErrTimeout) {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch normalization DLQ message: %w", err)
		}
		if err := processor.handle(ctx, message); err != nil {
			return err
		}
	}
}

func (p *normalizationDLQReplayProcessor) handle(
	ctx context.Context,
	message replayDLQMessage,
) error {
	if message == nil {
		return errors.New("normalization DLQ message is required")
	}
	if p.logger == nil {
		return errors.New("normalization DLQ replay logger is required")
	}
	if p.getSource == nil || p.reserveAttempt == nil || p.publish == nil {
		return errors.New("normalization DLQ replay processor dependencies are required")
	}
	if p.nakDelay <= 0 {
		return errors.New("normalization DLQ replay NAK delay must be positive")
	}

	var record normalizationDeadLetter
	if err := json.Unmarshal(message.Data(), &record); err != nil {
		p.logger.Warn("normalization DLQ replay rejected invalid JSON", "error", err)
		return p.ack(ctx, message, "", replayInvalidRecord)
	}

	decision := evaluateNormalizationReplay(record, p.policy)
	if !decision.Eligible {
		return p.ack(ctx, message, record.DLQRecordID, decision.Reason)
	}

	source, err := p.getSource(ctx, decision.SourceStreamSequence)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return p.ack(ctx, message, record.DLQRecordID, replaySourceUnavailable)
		}
		return p.nak(message, record.DLQRecordID, "raw source lookup", err)
	}
	if err := validateReplaySourceBeforeReservation(source, decision); err != nil {
		p.logger.Warn(
			"normalization DLQ replay rejected source",
			"dlq_record_id", record.DLQRecordID,
			"error", err,
		)
		return p.ack(ctx, message, record.DLQRecordID, replaySourceUnavailable)
	}

	request, err := replayReservationRequestFromDecision(decision)
	if err != nil {
		p.logger.Error(
			"normalization DLQ replay rejected invalid reservation request",
			"dlq_record_id", record.DLQRecordID,
			"error", err,
		)
		return p.ack(ctx, message, record.DLQRecordID, replayInvalidRecord)
	}

	reservation, err := p.reserveAttempt(ctx, request)
	if err != nil {
		if errors.Is(err, errReplayStateConflict) {
			p.logger.Error(
				"normalization DLQ replay state conflict",
				"dlq_record_id", record.DLQRecordID,
				"replay_root_dlq_record_id", decision.ReplayRootDLQRecordID,
				"error", err,
			)
			return p.ack(ctx, message, record.DLQRecordID, replayInvalidRecord)
		}
		return p.nak(message, record.DLQRecordID, "reserve replay attempt", err)
	}
	if !reservation.Reserved {
		return p.ack(ctx, message, record.DLQRecordID, replayBudgetExhausted)
	}

	replayMessage, err := buildSelectiveReplayMessage(source, decision, reservation)
	if err != nil {
		p.logger.Error(
			"normalization DLQ replay handoff invariant rejected",
			"dlq_record_id", record.DLQRecordID,
			"attempt", reservation.Attempt,
			"error", err,
		)
		return p.ack(ctx, message, record.DLQRecordID, replayInvalidRecord)
	}

	pubAck, err := p.publish(ctx, replayMessage)
	if err != nil {
		return p.nak(message, record.DLQRecordID, "publish selective replay", err)
	}
	if pubAck == nil || pubAck.Stream != rawStreamName || pubAck.Sequence == 0 {
		return p.nak(
			message,
			record.DLQRecordID,
			"validate selective replay PubAck",
			fmt.Errorf("unexpected PubAck: %+v", pubAck),
		)
	}

	p.logger.Info(
		"normalization DLQ replay published",
		"dlq_record_id", record.DLQRecordID,
		"replay_root_dlq_record_id", decision.ReplayRootDLQRecordID,
		"attempt", reservation.Attempt,
		"raw_stream_sequence", pubAck.Sequence,
	)
	return message.DoubleAck(ctx)
}

func validateReplaySourceBeforeReservation(
	source *jetstream.RawStreamMsg,
	decision replayDecision,
) error {
	if source == nil {
		return errors.New("raw replay source is required")
	}
	if source.Sequence != decision.SourceStreamSequence {
		return fmt.Errorf(
			"raw replay source sequence = %d, want %d",
			source.Sequence,
			decision.SourceStreamSequence,
		)
	}
	if source.Subject != rawPersistedSubject {
		return fmt.Errorf(
			"raw replay source subject = %q, want %q",
			source.Subject,
			rawPersistedSubject,
		)
	}
	if sha256LowerHex(source.Data) != decision.SourcePayloadSHA256 {
		return errors.New("raw replay source payload SHA-256 does not match DLQ record")
	}
	return nil
}

func (p *normalizationDLQReplayProcessor) ack(
	ctx context.Context,
	message replayDLQMessage,
	dlqRecordID string,
	reason replayEligibilityReason,
) error {
	p.logger.Info(
		"normalization DLQ replay not published",
		"dlq_record_id", dlqRecordID,
		"reason", reason,
	)
	if err := message.DoubleAck(ctx); err != nil {
		return fmt.Errorf("ack normalization DLQ message: %w", err)
	}
	return nil
}

func (p *normalizationDLQReplayProcessor) nak(
	message replayDLQMessage,
	dlqRecordID string,
	stage string,
	cause error,
) error {
	p.logger.Warn(
		"normalization DLQ replay deferred",
		"dlq_record_id", dlqRecordID,
		"stage", stage,
		"error", cause,
		"delay", p.nakDelay,
	)
	if err := message.NakWithDelay(p.nakDelay); err != nil {
		return fmt.Errorf("NAK normalization DLQ message after %s: %w", stage, err)
	}
	return nil
}
