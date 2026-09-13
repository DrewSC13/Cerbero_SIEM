package preserver

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	contractsv1 "cerbero/services/internal/contracts/v1"
	contractvalidation "cerbero/services/internal/contracts/validation"
	"google.golang.org/protobuf/proto"
)

type fixedBuilderClock struct {
	now time.Time
}

func (f fixedBuilderClock) Now() time.Time {
	return f.now
}

type fixedIDGenerator struct {
	value string
	err   error
	calls int
}

func (f *fixedIDGenerator) New() (string, error) {
	f.calls++
	return f.value, f.err
}

func TestRawPersistedBuilderCreatesGovernedEnvelope(t *testing.T) {
	delivery := validDelivery(t)
	delivery.Envelope.CorrelationId = "security-correlation-a"
	rawEvent := unpackRawEvent(t, delivery.Envelope)
	sequence := uint64(42)
	rawEvent.SequenceNumber = &sequence

	ids := &fixedIDGenerator{value: persistedMessageID}
	persistedAt := time.Unix(1_789_000_123, 456_000_000).UTC()
	builder := newConcreteBuilder(t, ids, fixedBuilderClock{now: persistedAt})

	publication, err := builder.Build(
		context.Background(),
		delivery.Envelope,
		rawEvent,
		validRawObject(),
		requestID,
	)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if ids.calls != 1 {
		t.Fatalf("ID generator calls = %d, want 1", ids.calls)
	}
	if publication.Subject != RawPersistedSubject {
		t.Fatalf("subject = %q", publication.Subject)
	}
	if publication.MessageID != persistedMessageID {
		t.Fatalf("message_id = %q", publication.MessageID)
	}
	if publication.RequestID != requestID {
		t.Fatalf("request_id = %q", publication.RequestID)
	}

	envelope := decodePersistedEnvelope(t, publication.Payload)
	if envelope.GetMessageId() != persistedMessageID {
		t.Fatalf("envelope message_id = %q", envelope.GetMessageId())
	}
	if envelope.GetMessageType() != MessageTypeRawEventPersisted {
		t.Fatalf("message_type = %q", envelope.GetMessageType())
	}
	if envelope.GetPayloadSchema() != PayloadSchemaRawEventPersistedV1 {
		t.Fatalf("payload_schema = %q", envelope.GetPayloadSchema())
	}
	if envelope.GetTenantId() != rawEvent.GetTenantId() {
		t.Fatalf("tenant_id = %q", envelope.GetTenantId())
	}
	if envelope.GetTraceId() != delivery.Envelope.GetTraceId() {
		t.Fatalf("trace_id = %q", envelope.GetTraceId())
	}
	if envelope.GetCausationId() != delivery.Envelope.GetMessageId() {
		t.Fatalf("causation_id = %q", envelope.GetCausationId())
	}
	if envelope.GetCorrelationId() != delivery.Envelope.GetCorrelationId() {
		t.Fatalf("correlation_id = %q", envelope.GetCorrelationId())
	}
	if envelope.GetProducer().GetComponent() != rawPreserverComponent {
		t.Fatalf("producer.component = %q", envelope.GetProducer().GetComponent())
	}
	if envelope.GetProducer().GetComponentVersion() != "0.1.0-test" {
		t.Fatalf("producer.component_version = %q", envelope.GetProducer().GetComponentVersion())
	}
	if envelope.GetProducer().GetInstanceId() != "raw-preserver-test-1" {
		t.Fatalf("producer.instance_id = %q", envelope.GetProducer().GetInstanceId())
	}
	if got := envelope.GetEmittedAt().AsTime(); !got.Equal(persistedAt) {
		t.Fatalf("emitted_at = %s, want %s", got, persistedAt)
	}

	persisted := unpackRawEventPersisted(t, envelope)
	if err := contractvalidation.RawEventPersisted(persisted); err != nil {
		t.Fatalf("RawEventPersisted validation = %v", err)
	}
	assertPersistedProjection(t, rawEvent, validRawObject(), persisted, persistedAt)
}

func TestRawPersistedBuilderDeterministicForFixedIdentityAndTime(t *testing.T) {
	delivery := validDelivery(t)
	rawEvent := unpackRawEvent(t, delivery.Envelope)
	now := time.Unix(1_789_000_123, 0).UTC()

	first, err := newConcreteBuilder(
		t,
		&fixedIDGenerator{value: persistedMessageID},
		fixedBuilderClock{now: now},
	).Build(context.Background(), delivery.Envelope, rawEvent, validRawObject(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newConcreteBuilder(
		t,
		&fixedIDGenerator{value: persistedMessageID},
		fixedBuilderClock{now: now},
	).Build(context.Background(), delivery.Envelope, rawEvent, validRawObject(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Payload, second.Payload) {
		t.Fatal("deterministic builder produced different outbox bytes")
	}
}

func TestRawPersistedBuilderRejectsGeneratorFailureAndInvalidGeneratedID(t *testing.T) {
	delivery := validDelivery(t)
	rawEvent := unpackRawEvent(t, delivery.Envelope)
	now := fixedBuilderClock{now: time.Unix(1_789_000_123, 0).UTC()}

	builder := newConcreteBuilder(
		t,
		&fixedIDGenerator{err: errors.New("entropy unavailable")},
		now,
	)
	if _, err := builder.Build(context.Background(), delivery.Envelope, rawEvent, validRawObject(), requestID); err == nil {
		t.Fatal("Build() accepted ID generator failure")
	}

	builder = newConcreteBuilder(t, &fixedIDGenerator{value: "not-a-uuid"}, now)
	if _, err := builder.Build(context.Background(), delivery.Envelope, rawEvent, validRawObject(), requestID); err == nil {
		t.Fatal("Build() accepted invalid generated UUID")
	}
}

func TestRawPersistedBuilderRequiresProducerAndDeterministicDependencies(t *testing.T) {
	tests := []RawPersistedBuilderConfig{
		{},
		{ComponentVersion: "test"},
		{ComponentVersion: "test", InstanceID: "instance-a"},
		{
			ComponentVersion: "test",
			InstanceID:       "instance-a",
			Clock:            fixedBuilderClock{now: time.Unix(1, 0)},
		},
	}
	for _, config := range tests {
		if _, err := NewRawPersistedBuilder(config); err == nil {
			t.Fatal("NewRawPersistedBuilder() accepted incomplete configuration")
		}
	}
}

func newConcreteBuilder(t *testing.T, ids IDGenerator, clock Clock) *RawPersistedBuilder {
	t.Helper()
	builder, err := NewRawPersistedBuilder(RawPersistedBuilderConfig{
		ComponentVersion: "0.1.0-test",
		InstanceID:       "raw-preserver-test-1",
		Clock:            clock,
		IDs:              ids,
	})
	if err != nil {
		t.Fatal(err)
	}
	return builder
}

func unpackRawEvent(t *testing.T, envelope *contractsv1.CerberoEnvelope) *contractsv1.RawEvent {
	t.Helper()
	event := new(contractsv1.RawEvent)
	if err := envelope.GetPayload().UnmarshalTo(event); err != nil {
		t.Fatalf("unpack RawEvent: %v", err)
	}
	return event
}

func decodePersistedEnvelope(t *testing.T, wire []byte) *contractsv1.CerberoEnvelope {
	t.Helper()
	envelope := new(contractsv1.CerberoEnvelope)
	if err := proto.Unmarshal(wire, envelope); err != nil {
		t.Fatalf("unmarshal raw.persisted envelope: %v", err)
	}
	if err := contractvalidation.Envelope(envelope); err != nil {
		t.Fatalf("validate raw.persisted envelope: %v", err)
	}
	return envelope
}

func unpackRawEventPersisted(t *testing.T, envelope *contractsv1.CerberoEnvelope) *contractsv1.RawEventPersisted {
	t.Helper()
	persisted := new(contractsv1.RawEventPersisted)
	if err := envelope.GetPayload().UnmarshalTo(persisted); err != nil {
		t.Fatalf("unpack RawEventPersisted: %v", err)
	}
	return persisted
}

func assertPersistedProjection(
	t *testing.T,
	raw *contractsv1.RawEvent,
	object RawObject,
	persisted *contractsv1.RawEventPersisted,
	persistedAt time.Time,
) {
	t.Helper()
	if persisted.GetEventId() != raw.GetEventId() ||
		persisted.GetTenantId() != raw.GetTenantId() ||
		persisted.GetSourceId() != raw.GetSourceId() ||
		persisted.GetSensorId() != raw.GetSensorId() ||
		persisted.GetContentType() != raw.GetContentType() ||
		persisted.GetEncoding() != raw.GetEncoding() ||
		persisted.GetRawSize() != raw.GetRawSize() ||
		persisted.GetRawHashAlgorithm() != raw.GetRawHashAlgorithm() ||
		persisted.GetRawHash() != raw.GetRawHash() ||
		persisted.GetTransport() != raw.GetTransport() ||
		persisted.GetRemoteIdentity() != raw.GetRemoteIdentity() ||
		persisted.GetIntegrityStatus() != raw.GetIntegrityStatus() ||
		persisted.GetPipelineVersion() != raw.GetPipelineVersion() {
		t.Fatal("RawEventPersisted did not preserve RawEvent metadata")
	}
	if persisted.SequenceNumber == nil || raw.SequenceNumber == nil ||
		persisted.GetSequenceNumber() != raw.GetSequenceNumber() {
		t.Fatal("sequence_number was not preserved")
	}
	if persisted.GetStorageUri() != object.StorageURI ||
		persisted.GetSegmentId() != object.SegmentID ||
		persisted.GetOffset() != object.Offset ||
		persisted.GetLength() != object.Length {
		t.Fatal("RawEventPersisted locator differs from durable RawObject")
	}
	if got := persisted.GetPersistedAt().AsTime(); !got.Equal(persistedAt) {
		t.Fatalf("persisted_at = %s, want %s", got, persistedAt)
	}
}
