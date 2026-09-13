package preserver

import (
	"context"
	"errors"
	"fmt"

	contractvalidation "cerbero/services/internal/contracts/validation"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// RawStreamName is the locked v1 JetStream stream for cerbero.v1.raw.* subjects.
	RawStreamName = "CERBERO_RAW"
	// RequestIDHeader carries ADR-0007 request correlation as transport metadata.
	RequestIDHeader = "Cerbero-Request-Id"
)

// jetStreamMessagePublisher is the synchronous JetStream boundary used by the adapter.
type jetStreamMessagePublisher interface {
	PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// JetStreamPublisher durably admits stable raw.persisted outbox publications to JetStream.
type JetStreamPublisher struct {
	publisher jetStreamMessagePublisher
}

// NewJetStreamPublisher creates the production Publisher adapter.
func NewJetStreamPublisher(publisher jetStreamMessagePublisher) (*JetStreamPublisher, error) {
	if publisher == nil {
		return nil, errors.New("JetStream publisher is required")
	}
	return &JetStreamPublisher{publisher: publisher}, nil
}

// Publish returns success only after JetStream acknowledges durable admission to CERBERO_RAW.
// Repeated calls reuse Publication.MessageID as Nats-Msg-Id, allowing JetStream duplicate
// suppression to converge crash-after-publish-before-MarkPublished recovery.
func (p *JetStreamPublisher) Publish(ctx context.Context, publication Publication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRawPersistedPublication(publication); err != nil {
		return err
	}

	message := &nats.Msg{
		Subject: publication.Subject,
		Header:  make(nats.Header),
		Data:    append([]byte(nil), publication.Payload...),
	}
	message.Header.Set(nats.MsgIdHdr, publication.MessageID)
	if publication.RequestID != "" {
		message.Header.Set(RequestIDHeader, publication.RequestID)
	}

	ack, err := p.publisher.PublishMsg(ctx, message)
	if err != nil {
		return fmt.Errorf("publish %s: %w", publication.Subject, err)
	}
	if ack == nil {
		return errors.New("publish returned no JetStream acknowledgement")
	}
	if ack.Stream != RawStreamName {
		return fmt.Errorf("publish acknowledged by unexpected stream %q", ack.Stream)
	}
	if ack.Sequence == 0 {
		return errors.New("publish returned invalid JetStream sequence 0")
	}
	return nil
}

func validateRawPersistedPublication(publication Publication) error {
	if publication.Subject != RawPersistedSubject {
		return fmt.Errorf("publication subject = %q, want %q", publication.Subject, RawPersistedSubject)
	}
	if err := contractvalidation.UUIDv7("publication.message_id", publication.MessageID); err != nil {
		return err
	}
	if publication.RequestID != "" {
		if err := contractvalidation.UUIDv7("publication.request_id", publication.RequestID); err != nil {
			return err
		}
	}
	if len(publication.Payload) == 0 {
		return errors.New("publication payload is required")
	}
	return nil
}
