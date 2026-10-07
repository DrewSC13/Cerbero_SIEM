package journald

import (
	"bytes"
	"context"
	"errors"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

// Preparer starts source admission before one journald evidence unit is read.
type Preparer interface {
	Begin(context.Context, ingestcore.AdmissionMetadata) (ingestcore.Admission, error)
}

// DurableAcceptor confirms durable event-bus admission.
type DurableAcceptor interface {
	Accept(context.Context, *ingestcore.Result) error
}

// Adapter binds the journald source contract to IngestCore and durable admission.
type Adapter struct {
	preparer Preparer
	acceptor DurableAcceptor
}

// Session is an authenticated/authorized journald source admission.
type Session struct {
	admission ingestcore.Admission
	acceptor  DurableAcceptor
	metadata  ingestcore.AdmissionMetadata
}

// NewAdapter validates journald ingest dependencies.
func NewAdapter(preparer Preparer, acceptor DurableAcceptor) (*Adapter, error) {
	if preparer == nil {
		return nil, errors.New("journald ingest preparer is required")
	}
	if acceptor == nil {
		return nil, errors.New("journald durable acceptor is required")
	}
	return &Adapter{preparer: preparer, acceptor: acceptor}, nil
}

// Begin authenticates and authorizes the configured journald source before the collector reads it.
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

// Accept admits one collector-produced canonical representation without mapping the cursor to sequence semantics.
func (s *Session) Accept(ctx context.Context, entry Entry) (*ingestcore.Result, error) {
	if err := entry.Validate(); err != nil {
		return nil, err
	}
	if !sameSource(entry.Source, s.metadata) {
		return nil, errors.New("journald entry source metadata changed after admission")
	}
	if len(entry.RawRepresentation) == 0 {
		return nil, errors.New("journald runtime requires the canonical raw representation")
	}

	raw := bytes.Clone(entry.RawRepresentation)
	result, err := s.admission.Prepare(ingestcore.Request{
		RequestID:      s.metadata.RequestID,
		TenantID:       s.metadata.TenantID,
		SourceID:       s.metadata.SourceID,
		SensorID:       s.metadata.SensorID,
		RemoteIdentity: s.metadata.RemoteIdentity,
		Transport:      s.metadata.Transport,
		ContentType:    "application/json",
		Encoding:       "utf-8",
		RawPayload:     raw,
	})
	if err != nil {
		return nil, err
	}
	if err := s.acceptor.Accept(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

func sameSource(left, right ingestcore.AdmissionMetadata) bool {
	return left.TenantID == right.TenantID &&
		left.SourceID == right.SourceID &&
		left.SensorID == right.SensorID &&
		left.RemoteIdentity == right.RemoteIdentity &&
		left.Transport == right.Transport
}
