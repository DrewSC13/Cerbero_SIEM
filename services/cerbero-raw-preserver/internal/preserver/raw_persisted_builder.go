package preserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	contractsv1 "cerbero/services/internal/contracts/v1"
	contractvalidation "cerbero/services/internal/contracts/validation"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	MessageTypeRawEventPersisted     = "RawEventPersisted"
	PayloadSchemaRawEventPersistedV1 = "cerbero.raw_event_persisted.v1"
	rawPreserverComponent            = "cerbero-raw-preserver"
)

// Clock supplies processing time to the raw.persisted builder.
type Clock interface {
	Now() time.Time
}

// IDGenerator creates CERBERO-owned identifiers for derived publications.
type IDGenerator interface {
	New() (string, error)
}

// RawPersistedBuilderConfig contains the producer identity and deterministic dependencies.
type RawPersistedBuilderConfig struct {
	ComponentVersion string
	InstanceID       string
	Clock            Clock
	IDs              IDGenerator
}

// RawPersistedBuilder creates the governed CerberoEnvelope<RawEventPersisted> outbox bytes.
type RawPersistedBuilder struct {
	componentVersion string
	instanceID       string
	clock            Clock
	ids              IDGenerator
}

// NewRawPersistedBuilder validates the concrete raw.persisted builder dependencies.
func NewRawPersistedBuilder(config RawPersistedBuilderConfig) (*RawPersistedBuilder, error) {
	if config.ComponentVersion == "" {
		return nil, errors.New("raw-preserver component version is required")
	}
	if config.InstanceID == "" {
		return nil, errors.New("raw-preserver instance ID is required")
	}
	if config.Clock == nil {
		return nil, errors.New("raw.persisted builder clock is required")
	}
	if config.IDs == nil {
		return nil, errors.New("raw.persisted builder ID generator is required")
	}
	return &RawPersistedBuilder{
		componentVersion: config.ComponentVersion,
		instanceID:       config.InstanceID,
		clock:            config.Clock,
		ids:              config.IDs,
	}, nil
}

// Build creates one stable raw.persisted outbox publication.
//
// The caller persists the returned Publication in the transactional outbox. Retries must reuse
// that stored Publication rather than invoking Build again.
func (b *RawPersistedBuilder) Build(
	_ context.Context,
	incoming *contractsv1.CerberoEnvelope,
	rawEvent *contractsv1.RawEvent,
	rawObject RawObject,
	requestID string,
) (Publication, error) {
	if err := validateBuilderInput(incoming, rawEvent, rawObject, requestID); err != nil {
		return Publication{}, err
	}

	messageID, err := b.ids.New()
	if err != nil {
		return Publication{}, fmt.Errorf("generate raw.persisted message_id: %w", err)
	}
	if err := contractvalidation.UUIDv7("raw_persisted.message_id", messageID); err != nil {
		return Publication{}, fmt.Errorf("validate generated raw.persisted message_id: %w", err)
	}

	persistedAt := timestamppb.New(b.clock.Now().UTC())
	if err := contractvalidation.Timestamp("persisted_at", persistedAt); err != nil {
		return Publication{}, fmt.Errorf("validate persisted_at: %w", err)
	}

	persisted := &contractsv1.RawEventPersisted{
		EventId:          rawEvent.GetEventId(),
		TenantId:         rawEvent.GetTenantId(),
		SourceId:         rawEvent.GetSourceId(),
		SensorId:         rawEvent.GetSensorId(),
		EventTime:        cloneTimestamp(rawEvent.GetEventTime()),
		IngestTime:       cloneTimestamp(rawEvent.GetIngestTime()),
		ContentType:      rawEvent.GetContentType(),
		Encoding:         rawEvent.GetEncoding(),
		RawSize:          rawEvent.GetRawSize(),
		RawHashAlgorithm: rawEvent.GetRawHashAlgorithm(),
		RawHash:          rawEvent.GetRawHash(),
		Transport:        rawEvent.GetTransport(),
		RemoteIdentity:   rawEvent.GetRemoteIdentity(),
		SequenceNumber:   cloneUint64(rawEvent.SequenceNumber),
		IntegrityStatus:  rawEvent.GetIntegrityStatus(),
		PipelineVersion:  rawEvent.GetPipelineVersion(),
		StorageUri:       rawObject.StorageURI,
		SegmentId:        rawObject.SegmentID,
		Offset:           rawObject.Offset,
		Length:           rawObject.Length,
		PersistedAt:      cloneTimestamp(persistedAt),
	}
	if err := contractvalidation.RawEventPersisted(persisted); err != nil {
		return Publication{}, fmt.Errorf("validate RawEventPersisted payload: %w", err)
	}

	payload, err := anypb.New(persisted)
	if err != nil {
		return Publication{}, fmt.Errorf("pack RawEventPersisted payload: %w", err)
	}

	envelope := &contractsv1.CerberoEnvelope{
		ContractVersion: "1",
		MessageId:       messageID,
		MessageType:     MessageTypeRawEventPersisted,
		TenantId:        incoming.GetTenantId(),
		Producer: &contractsv1.Producer{
			Component:        rawPreserverComponent,
			ComponentVersion: b.componentVersion,
			InstanceId:       b.instanceID,
		},
		EmittedAt:     cloneTimestamp(persistedAt),
		TraceId:       incoming.GetTraceId(),
		CausationId:   incoming.GetMessageId(),
		CorrelationId: incoming.GetCorrelationId(),
		PayloadSchema: PayloadSchemaRawEventPersistedV1,
		Payload:       payload,
	}
	if err := contractvalidation.Envelope(envelope); err != nil {
		return Publication{}, fmt.Errorf("validate raw.persisted envelope: %w", err)
	}

	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(envelope)
	if err != nil {
		return Publication{}, fmt.Errorf("marshal raw.persisted envelope: %w", err)
	}

	return Publication{
		Subject:   RawPersistedSubject,
		MessageID: messageID,
		RequestID: requestID,
		Payload:   wire,
	}, nil
}

func validateBuilderInput(
	incoming *contractsv1.CerberoEnvelope,
	rawEvent *contractsv1.RawEvent,
	rawObject RawObject,
	requestID string,
) error {
	if err := contractvalidation.Envelope(incoming); err != nil {
		return fmt.Errorf("validate incoming envelope: %w", err)
	}
	if incoming.GetMessageType() != MessageTypeRawEventReceived {
		return fmt.Errorf("incoming message_type: expected %s", MessageTypeRawEventReceived)
	}
	if incoming.GetPayloadSchema() != PayloadSchemaRawEventV1 {
		return fmt.Errorf("incoming payload_schema: expected %s", PayloadSchemaRawEventV1)
	}
	if err := contractvalidation.RawEvent(rawEvent); err != nil {
		return fmt.Errorf("validate RawEvent: %w", err)
	}
	if incoming.GetTenantId() != rawEvent.GetTenantId() {
		return errors.New("tenant_id: incoming envelope and RawEvent tenants differ")
	}
	if err := validateRawObject(rawEvent, rawObject); err != nil {
		return fmt.Errorf("validate durable raw object: %w", err)
	}
	if requestID != "" {
		if err := contractvalidation.UUIDv7("request_id", requestID); err != nil {
			return err
		}
	}
	return nil
}

func cloneTimestamp(timestamp *timestamppb.Timestamp) *timestamppb.Timestamp {
	if timestamp == nil {
		return nil
	}
	return &timestamppb.Timestamp{Seconds: timestamp.Seconds, Nanos: timestamp.Nanos}
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
