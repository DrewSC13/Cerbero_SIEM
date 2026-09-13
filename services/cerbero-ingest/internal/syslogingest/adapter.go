package syslogingest

import (
	"context"
	"errors"
	"time"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

const (
	// Transport identifies the common-core transport metadata for syslog sources.
	Transport = "syslog"
)

// Preparer starts common-core source admission before the framed syslog message is received.
type Preparer interface {
	Begin(context.Context, ingestcore.AdmissionMetadata) (ingestcore.Admission, error)
}

// DurableAcceptor confirms durable event-bus admission.
type DurableAcceptor interface {
	Accept(context.Context, *ingestcore.Result) error
}

// Config contains injected common-core and durability boundaries.
type Config struct {
	Preparer Preparer
	Acceptor DurableAcceptor
}

// Adapter is the transport-neutral syslog adapter skeleton.
// Network listeners, TCP framing, and UDP policy are intentionally outside this type.
type Adapter struct {
	preparer Preparer
	acceptor DurableAcceptor
}

// Message is one already-framed syslog evidence unit.
// RawPayload must contain the exact bytes supplied by the framing boundary.
type Message struct {
	ContentType    string
	Encoding       string
	EventTime      *time.Time
	SequenceNumber *uint64
	RawPayload     []byte
}

// Session is an authenticated/authorized syslog source admission.
type Session struct {
	admission ingestcore.Admission
	acceptor  DurableAcceptor
	metadata  ingestcore.AdmissionMetadata
}

// New validates the syslog adapter skeleton dependencies.
func New(config Config) (*Adapter, error) {
	if config.Preparer == nil {
		return nil, errors.New("ingest preparer is required")
	}
	if config.Acceptor == nil {
		return nil, errors.New("durable acceptor is required")
	}
	return &Adapter{
		preparer: config.Preparer,
		acceptor: config.Acceptor,
	}, nil
}

// Begin starts common-core admission before a transport implementation reads a syslog message.
func (a *Adapter) Begin(
	ctx context.Context,
	metadata ingestcore.AdmissionMetadata,
) (*Session, error) {
	metadata.Transport = Transport
	admission, err := a.preparer.Begin(ctx, metadata)
	if err != nil {
		return nil, err
	}
	return &Session{
		admission: admission,
		acceptor:  a.acceptor,
		metadata:  metadata,
	}, nil
}

// Accept prepares exact syslog bytes and returns success only after durable event-bus admission.
func (s *Session) Accept(ctx context.Context, message Message) (*ingestcore.Result, error) {
	result, err := s.admission.Prepare(ingestcore.Request{
		RequestID:      s.metadata.RequestID,
		TenantID:       s.metadata.TenantID,
		SourceID:       s.metadata.SourceID,
		SensorID:       s.metadata.SensorID,
		RemoteIdentity: s.metadata.RemoteIdentity,
		Transport:      s.metadata.Transport,
		ContentType:    message.ContentType,
		Encoding:       message.Encoding,
		EventTime:      message.EventTime,
		SequenceNumber: message.SequenceNumber,
		RawPayload:     message.RawPayload,
	})
	if err != nil {
		return nil, err
	}
	if err := s.acceptor.Accept(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}
