package preserver

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	contractsv1 "cerbero/services/internal/contracts/v1"
	contractvalidation "cerbero/services/internal/contracts/validation"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	incomingMessageID  = "01995000-0000-7000-8000-000000000001"
	eventID            = "01995000-0000-7000-8000-000000000002"
	traceID            = "01995000-0000-7000-8000-000000000003"
	persistedMessageID = "01995000-0000-7000-8000-000000000004"
	requestID          = "01995000-0000-7000-8000-000000000005"
	consumerName       = "raw-preserver-test"
)

type rawStoreFake struct {
	calls    *[]string
	object   RawObject
	err      error
	received RawEvidence
}

func (f *rawStoreFake) EnsureDurable(_ context.Context, evidence RawEvidence) (RawObject, error) {
	*f.calls = append(*f.calls, "raw.ensure")
	f.received = evidence
	return f.object, f.err
}

type metadataFake struct {
	calls     *[]string
	found     bool
	record    Record
	findErr   error
	commitErr error
	markErr   error
	committed Record
}

func (f *metadataFake) Find(_ context.Context, consumer, messageID string) (Record, bool, error) {
	*f.calls = append(*f.calls, "metadata.find")
	if consumer != consumerName || messageID != incomingMessageID {
		return Record{}, false, errors.New("unexpected lookup key")
	}
	return f.record, f.found, f.findErr
}

func (f *metadataFake) CommitPreservation(_ context.Context, record Record) (Record, error) {
	*f.calls = append(*f.calls, "metadata.commit")
	f.committed = record
	if f.commitErr != nil {
		return Record{}, f.commitErr
	}
	if f.record.ConsumerName != "" {
		return f.record, nil
	}
	return record, nil
}

func (f *metadataFake) MarkPublished(_ context.Context, consumer, messageID string) error {
	*f.calls = append(*f.calls, "metadata.mark-published")
	if consumer != consumerName || messageID != incomingMessageID {
		return errors.New("unexpected mark key")
	}
	return f.markErr
}

type builderFake struct {
	calls       *[]string
	publication Publication
	err         error
}

func (f *builderFake) Build(_ context.Context, _ *contractsv1.CerberoEnvelope, _ *contractsv1.RawEvent, _ RawObject, gotRequestID string) (Publication, error) {
	*f.calls = append(*f.calls, "publication.build")
	if gotRequestID != requestID {
		return Publication{}, errors.New("request ID was not propagated")
	}
	return f.publication, f.err
}

type publisherFake struct {
	calls     *[]string
	err       error
	published Publication
}

func (f *publisherFake) Publish(_ context.Context, publication Publication) error {
	*f.calls = append(*f.calls, "publisher.publish")
	f.published = publication
	return f.err
}

func TestProcessHappyPathOrdersDurabilityBeforeACKEligibility(t *testing.T) {
	calls := []string{}
	rawStore := &rawStoreFake{calls: &calls, object: validRawObject()}
	metadata := &metadataFake{calls: &calls}
	publisher := &publisherFake{calls: &calls}
	core := newTestCore(t, rawStore, metadata, &builderFake{calls: &calls, publication: validPublication()}, publisher)

	result, err := core.Process(context.Background(), validDelivery(t))
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result.Disposition != DispositionACK {
		t.Fatalf("disposition = %v, want ACK", result.Disposition)
	}
	want := []string{"metadata.find", "raw.ensure", "publication.build", "metadata.commit", "publisher.publish", "metadata.mark-published"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("call order = %#v, want %#v", calls, want)
	}
	if !reflect.DeepEqual(rawStore.received.Bytes, []byte("exact raw bytes\n")) {
		t.Fatalf("raw bytes = %q", rawStore.received.Bytes)
	}
	if want := time.Unix(1_789_000_000, 0).UTC(); !rawStore.received.IngestTime.Equal(want) {
		t.Fatalf("raw ingest_time = %s, want %s", rawStore.received.IngestTime, want)
	}
	if metadata.committed.Publication.MessageID != persistedMessageID {
		t.Fatalf("committed derived message_id = %q", metadata.committed.Publication.MessageID)
	}
}

func TestProcessAlreadyPublishedDeliveryIsACKEligibleWithoutDuplicateSideEffects(t *testing.T) {
	calls := []string{}
	record := validRecord()
	record.Published = true
	core := newTestCore(t, &rawStoreFake{calls: &calls, object: validRawObject()}, &metadataFake{calls: &calls, found: true, record: record}, &builderFake{calls: &calls, publication: validPublication()}, &publisherFake{calls: &calls})

	result, err := core.Process(context.Background(), validDelivery(t))
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result.Disposition != DispositionACK {
		t.Fatalf("disposition = %v, want ACK", result.Disposition)
	}
	if want := []string{"metadata.find"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("call order = %#v, want %#v", calls, want)
	}
}

func TestProcessPendingOutboxRepublishesStableMessageAndThenACKs(t *testing.T) {
	calls := []string{}
	record := validRecord()
	publisher := &publisherFake{calls: &calls}
	core := newTestCore(t, &rawStoreFake{calls: &calls, object: validRawObject()}, &metadataFake{calls: &calls, found: true, record: record}, &builderFake{calls: &calls, publication: validPublication()}, publisher)

	result, err := core.Process(context.Background(), validDelivery(t))
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result.Disposition != DispositionACK {
		t.Fatalf("disposition = %v, want ACK", result.Disposition)
	}
	if publisher.published.MessageID != persistedMessageID {
		t.Fatalf("republished message_id = %q", publisher.published.MessageID)
	}
	want := []string{"metadata.find", "publisher.publish", "metadata.mark-published"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("call order = %#v, want %#v", calls, want)
	}
}

func TestProcessRawStoreFailureIsRetryableAndNeverCommitsOrPublishes(t *testing.T) {
	calls := []string{}
	core := newTestCore(t, &rawStoreFake{calls: &calls, object: validRawObject(), err: errors.New("raw store unavailable")}, &metadataFake{calls: &calls}, &builderFake{calls: &calls, publication: validPublication()}, &publisherFake{calls: &calls})

	result, err := core.Process(context.Background(), validDelivery(t))
	if err == nil {
		t.Fatal("Process() error = nil")
	}
	if result.Disposition != DispositionRetry {
		t.Fatalf("disposition = %v, want RETRY", result.Disposition)
	}
	if want := []string{"metadata.find", "raw.ensure"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("call order = %#v, want %#v", calls, want)
	}
}

func TestProcessPublishFailureIsRetryableAndNeverMarksPublished(t *testing.T) {
	calls := []string{}
	core := newTestCore(t, &rawStoreFake{calls: &calls, object: validRawObject()}, &metadataFake{calls: &calls}, &builderFake{calls: &calls, publication: validPublication()}, &publisherFake{calls: &calls, err: errors.New("nats unavailable")})

	result, err := core.Process(context.Background(), validDelivery(t))
	if err == nil {
		t.Fatal("Process() error = nil")
	}
	if result.Disposition != DispositionRetry {
		t.Fatalf("disposition = %v, want RETRY", result.Disposition)
	}
	want := []string{"metadata.find", "raw.ensure", "publication.build", "metadata.commit", "publisher.publish"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("call order = %#v, want %#v", calls, want)
	}
}

func TestProcessInvalidEnvelopeIsPermanentAndHasNoSideEffects(t *testing.T) {
	calls := []string{}
	core := newTestCore(t, &rawStoreFake{calls: &calls, object: validRawObject()}, &metadataFake{calls: &calls}, &builderFake{calls: &calls, publication: validPublication()}, &publisherFake{calls: &calls})
	delivery := validDelivery(t)
	delivery.Envelope.MessageType = "NormalizedEventCreated"

	result, err := core.Process(context.Background(), delivery)
	if err == nil {
		t.Fatal("Process() error = nil")
	}
	if result.Disposition != DispositionIsolate {
		t.Fatalf("disposition = %v, want ISOLATE", result.Disposition)
	}
	if len(calls) != 0 {
		t.Fatalf("unexpected side effects: %#v", calls)
	}
}

func TestProcessRejectsConflictingIdempotencyRecord(t *testing.T) {
	calls := []string{}
	record := validRecord()
	record.EventID = "01995000-0000-7000-8000-000000000099"
	core := newTestCore(t, &rawStoreFake{calls: &calls, object: validRawObject()}, &metadataFake{calls: &calls, found: true, record: record}, &builderFake{calls: &calls, publication: validPublication()}, &publisherFake{calls: &calls})

	result, err := core.Process(context.Background(), validDelivery(t))
	if err == nil {
		t.Fatal("Process() error = nil")
	}
	if result.Disposition != DispositionIsolate {
		t.Fatalf("disposition = %v, want ISOLATE", result.Disposition)
	}
	if want := []string{"metadata.find"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("call order = %#v, want %#v", calls, want)
	}
}

func newTestCore(t *testing.T, rawStore RawStore, metadata MetadataStore, builder PublicationBuilder, publisher Publisher) *Core {
	t.Helper()
	core, err := New(Config{ConsumerName: consumerName, RawStore: rawStore, Metadata: metadata, Builder: builder, Publisher: publisher})
	if err != nil {
		t.Fatal(err)
	}
	return core
}

func validDelivery(t *testing.T) Delivery {
	t.Helper()
	raw := []byte("exact raw bytes\n")
	event := &contractsv1.RawEvent{
		EventId:          eventID,
		TenantId:         "tenant-a",
		SourceId:         "source-a",
		SensorId:         "sensor-a",
		IngestTime:       timestamppb.New(time.Unix(1_789_000_000, 0).UTC()),
		ContentType:      "application/json",
		Encoding:         "utf-8",
		RawPayload:       append([]byte(nil), raw...),
		RawSize:          uint64(len(raw)),
		RawHashAlgorithm: "sha256",
		RawHash:          contractvalidation.SHA256LowerHex(raw),
		Transport:        "json-http",
		RemoteIdentity:   "development-static-source",
		IntegrityStatus:  contractsv1.IntegrityStatus_INTEGRITY_UNVERIFIED,
		PipelineVersion:  "ingest-v1-test",
	}
	payload, err := anypb.New(event)
	if err != nil {
		t.Fatal(err)
	}
	return Delivery{
		RequestID: requestID,
		Envelope: &contractsv1.CerberoEnvelope{
			ContractVersion: "1",
			MessageId:       incomingMessageID,
			MessageType:     MessageTypeRawEventReceived,
			TenantId:        "tenant-a",
			Producer:        &contractsv1.Producer{Component: "cerbero-ingest", ComponentVersion: "test", InstanceId: "ingest-test-1"},
			EmittedAt:       timestamppb.New(time.Unix(1_789_000_000, 0).UTC()),
			TraceId:         traceID,
			PayloadSchema:   PayloadSchemaRawEventV1,
			Payload:         payload,
		},
	}
}

func validRawObject() RawObject {
	raw := []byte("exact raw bytes\n")
	return RawObject{StorageURI: "raw://tenant-a/2026/09/13/12/segment-test", SegmentID: "segment-test", Offset: 0, Length: uint64(len(raw)), Hash: contractvalidation.SHA256LowerHex(raw)}
}

func validPublication() Publication {
	return Publication{Subject: RawPersistedSubject, MessageID: persistedMessageID, RequestID: requestID, Payload: []byte("stable-derived-envelope-bytes")}
}

func validRecord() Record {
	return Record{ConsumerName: consumerName, IncomingMessageID: incomingMessageID, EventID: eventID, RawObject: validRawObject(), Publication: validPublication(), Published: false}
}
