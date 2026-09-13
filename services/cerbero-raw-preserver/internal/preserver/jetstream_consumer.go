package preserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	rand "math/rand/v2"
	"time"

	contractsv1 "cerbero/services/internal/contracts/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

const isolateReason = "cerbero raw-preserver permanent message failure"

type rawDeliveryProcessor interface {
	Process(context.Context, Delivery) (Result, error)
}

type rawJetStreamMessage interface {
	Data() []byte
	Headers() nats.Header
	Subject() string
	DoubleAck(context.Context) error
	NakWithDelay(time.Duration) error
	TermWithReason(string) error
}

type retryDelayFunc func(time.Duration, time.Duration) time.Duration

// RawConsumer translates one durable JetStream delivery into preservation core semantics.
type RawConsumer struct {
	processor     rawDeliveryProcessor
	logger        *slog.Logger
	retryMinDelay time.Duration
	retryMaxDelay time.Duration
	retryDelay    retryDelayFunc
}

// NewRawConsumer binds a Core-compatible processor to explicit ACK/retry/isolate transport semantics.
func NewRawConsumer(
	processor rawDeliveryProcessor,
	logger *slog.Logger,
	retryMinDelay time.Duration,
	retryMaxDelay time.Duration,
) (*RawConsumer, error) {
	return newRawConsumer(processor, logger, retryMinDelay, retryMaxDelay, randomRetryDelay)
}

func newRawConsumer(
	processor rawDeliveryProcessor,
	logger *slog.Logger,
	retryMinDelay time.Duration,
	retryMaxDelay time.Duration,
	retryDelay retryDelayFunc,
) (*RawConsumer, error) {
	if processor == nil {
		return nil, errors.New("raw preservation processor is required")
	}
	if retryMinDelay <= 0 {
		return nil, errors.New("retry minimum delay must be positive")
	}
	if retryMaxDelay < retryMinDelay {
		return nil, errors.New("retry maximum delay must be >= minimum delay")
	}
	if retryDelay == nil {
		return nil, errors.New("retry delay generator is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &RawConsumer{
		processor:     processor,
		logger:        logger,
		retryMinDelay: retryMinDelay,
		retryMaxDelay: retryMaxDelay,
		retryDelay:    retryDelay,
	}, nil
}

// Handle processes one raw.received delivery and performs only the transport action authorized by Core.
func (c *RawConsumer) Handle(ctx context.Context, message rawJetStreamMessage) error {
	if message == nil {
		return errors.New("JetStream message is required")
	}
	if message.Subject() != RawReceivedSubject {
		c.logger.Warn("isolating unexpected raw-preserver subject", "subject", message.Subject())
		return message.TermWithReason(isolateReason)
	}

	envelope := new(contractsv1.CerberoEnvelope)
	if err := proto.Unmarshal(message.Data(), envelope); err != nil {
		c.logger.Warn("isolating malformed raw.received protobuf", "error", err)
		return message.TermWithReason(isolateReason)
	}

	requestID := ""
	if headers := message.Headers(); headers != nil {
		requestID = headers.Get(RequestIDHeader)
	}
	result, processErr := c.processor.Process(ctx, Delivery{
		Envelope:  envelope,
		RequestID: requestID,
	})

	switch result.Disposition {
	case DispositionACK:
		if processErr != nil {
			c.logger.Error(
				"raw-preserver returned ACK disposition with error; retrying conservatively",
				"message_id", result.MessageID,
				"event_id", result.EventID,
				"error", processErr,
			)
			return c.retry(message)
		}
		if err := message.DoubleAck(ctx); err != nil {
			return fmt.Errorf("confirm raw.received ACK: %w", err)
		}
		return nil

	case DispositionRetry:
		c.logger.Warn(
			"raw-preserver scheduling retry",
			"message_id", result.MessageID,
			"event_id", result.EventID,
			"error", processErr,
		)
		return c.retry(message)

	case DispositionIsolate:
		c.logger.Warn(
			"raw-preserver isolating permanent message",
			"message_id", result.MessageID,
			"event_id", result.EventID,
			"error", processErr,
		)
		if err := message.TermWithReason(isolateReason); err != nil {
			return fmt.Errorf("terminate isolated raw.received delivery: %w", err)
		}
		return nil

	default:
		c.logger.Error(
			"raw-preserver returned unknown disposition; retrying conservatively",
			"message_id", result.MessageID,
			"event_id", result.EventID,
			"disposition", result.Disposition,
			"error", processErr,
		)
		return c.retry(message)
	}
}

func (c *RawConsumer) retry(message rawJetStreamMessage) error {
	delay := c.retryDelay(c.retryMinDelay, c.retryMaxDelay)
	if err := message.NakWithDelay(delay); err != nil {
		return fmt.Errorf("schedule raw.received retry after %s: %w", delay, err)
	}
	return nil
}

func randomRetryDelay(minimum, maximum time.Duration) time.Duration {
	if maximum <= minimum {
		return minimum
	}
	span := maximum - minimum
	return minimum + time.Duration(rand.Int64N(int64(span)))
}

var _ rawJetStreamMessage = (jetstream.Msg)(nil)
