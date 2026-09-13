package eventbus

import (
	"context"
	"errors"
	"fmt"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
	contractvalidation "cerbero/services/internal/contracts/validation"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

const (
	// RawReceivedSubject is the locked v1 subject for accepted RawEvent envelopes.
	RawReceivedSubject = "cerbero.v1.raw.received"
	// RequestIDHeader is the ADR-0007 request-correlation transport metadata header.
	RequestIDHeader = "Cerbero-Request-Id"
)

// Publisher is the synchronous JetStream publish boundary required by durable admission.
type Publisher interface {
	PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// JetStreamAcceptor durably admits prepared ingest envelopes into JetStream.
type JetStreamAcceptor struct {
	publisher Publisher
}

// NewJetStreamAcceptor returns a durable acceptor backed by synchronous JetStream publication.
func NewJetStreamAcceptor(publisher Publisher) (*JetStreamAcceptor, error) {
	if publisher == nil {
		return nil, errors.New("JetStream publisher is required")
	}
	return &JetStreamAcceptor{publisher: publisher}, nil
}

// Accept publishes the prepared CerberoEnvelope and returns success only after a JetStream PubAck.
func (a *JetStreamAcceptor) Accept(ctx context.Context, result *ingestcore.Result) error {
	if result == nil {
		return errors.New("ingest result is required")
	}
	if result.Envelope == nil {
		return errors.New("ingest envelope is required")
	}
	if err := contractvalidation.Envelope(result.Envelope); err != nil {
		return fmt.Errorf("validate ingest envelope before publish: %w", err)
	}
	if result.RequestID != "" {
		if err := contractvalidation.UUIDv7("request_id", result.RequestID); err != nil {
			return fmt.Errorf("validate request correlation metadata: %w", err)
		}
	}

	wire, err := proto.Marshal(result.Envelope)
	if err != nil {
		return fmt.Errorf("marshal ingest envelope: %w", err)
	}

	message := &nats.Msg{
		Subject: RawReceivedSubject,
		Header:  make(nats.Header),
		Data:    wire,
	}
	message.Header.Set(nats.MsgIdHdr, result.Envelope.GetMessageId())
	if result.RequestID != "" {
		message.Header.Set(RequestIDHeader, result.RequestID)
	}

	ack, err := a.publisher.PublishMsg(ctx, message)
	if err != nil {
		return fmt.Errorf("publish %s: %w", RawReceivedSubject, err)
	}
	if ack == nil {
		return errors.New("publish returned no JetStream acknowledgement")
	}
	return nil
}
