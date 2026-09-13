package preserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	contractsv1 "cerbero/services/internal/contracts/v1"
	contractvalidation "cerbero/services/internal/contracts/validation"
)

const (
	RawReceivedSubject  = "cerbero.v1.raw.received"
	RawPersistedSubject = "cerbero.v1.raw.persisted"

	MessageTypeRawEventReceived = "RawEventReceived"
	PayloadSchemaRawEventV1     = "cerbero.raw_event.v1"
)

// Disposition is the action the future JetStream adapter must take after Process returns.
type Disposition uint8

const (
	DispositionUnspecified Disposition = iota
	DispositionACK
	DispositionRetry
	DispositionIsolate
)

// Result describes the consumer decision without performing the JetStream ACK itself.
type Result struct {
	Disposition Disposition
	MessageID   string
	EventID     string
}

// Delivery is one raw.received bus delivery after transport-level framing.
type Delivery struct {
	Envelope  *contractsv1.CerberoEnvelope
	RequestID string
}

// RawEvidence contains the exact evidence bytes and stable identity passed to Raw Store.
type RawEvidence struct {
	TenantID      string
	EventID       string
	IngestTime    time.Time
	HashAlgorithm string
	Hash          string
	Size          uint64
	Bytes         []byte
}

// RawObject is the durable Raw Store locator returned only after exact bytes are durable
// and their hash has been verified by the RawStore implementation.
type RawObject struct {
	StorageURI string
	SegmentID  string
	Offset     uint64
	Length     uint64
	Hash       string
}

// Publication is the stable outbox publication for raw.persisted.
// Payload is opaque until the RawEventPersisted wire payload is governed.
type Publication struct {
	Subject   string
	MessageID string
	RequestID string
	Payload   []byte
}

// Record is durable PostgreSQL control-plane state for one raw.received transport message.
type Record struct {
	ConsumerName      string
	IncomingMessageID string
	EventID           string
	RawObject         RawObject
	Publication       Publication
	Published         bool
}

// RawStore owns authoritative exact raw evidence bytes.
type RawStore interface {
	// EnsureDurable creates or discovers the RawEvent evidence object idempotently.
	// Success means the exact bytes are durable and hash-verified.
	EnsureDurable(context.Context, RawEvidence) (RawObject, error)
}

// MetadataStore owns raw locator metadata, processed-message idempotency, and the outbox.
type MetadataStore interface {
	Find(context.Context, string, string) (Record, bool, error)
	// CommitPreservation must transactionally persist locator, processed-message state,
	// and one stable outbox publication. An existing key returns the existing record.
	CommitPreservation(context.Context, Record) (Record, error)
	MarkPublished(context.Context, string, string) error
}

// PublicationBuilder constructs the stable raw.persisted outbox message after evidence is durable.
// RawPersistedBuilder is the governed production implementation; the interface keeps the core testable.
type PublicationBuilder interface {
	Build(context.Context, *contractsv1.CerberoEnvelope, *contractsv1.RawEvent, RawObject, string) (Publication, error)
}

// Publisher durably admits an outbox publication to the event bus.
type Publisher interface {
	Publish(context.Context, Publication) error
}

// Config contains mandatory preservation dependencies and an explicit durable-consumer identity.
type Config struct {
	ConsumerName string
	RawStore     RawStore
	Metadata     MetadataStore
	Builder      PublicationBuilder
	Publisher    Publisher
}

// Core orchestrates preservation while leaving storage/NATS adapters outside the domain core.
type Core struct {
	consumerName string
	rawStore     RawStore
	metadata     MetadataStore
	builder      PublicationBuilder
	publisher    Publisher
}

// New validates core dependencies.
func New(config Config) (*Core, error) {
	if config.ConsumerName == "" {
		return nil, errors.New("consumer name is required")
	}
	if config.RawStore == nil {
		return nil, errors.New("raw store is required")
	}
	if config.Metadata == nil {
		return nil, errors.New("metadata store is required")
	}
	if config.Builder == nil {
		return nil, errors.New("publication builder is required")
	}
	if config.Publisher == nil {
		return nil, errors.New("publisher is required")
	}
	return &Core{
		consumerName: config.ConsumerName,
		rawStore:     config.RawStore,
		metadata:     config.Metadata,
		builder:      config.Builder,
		publisher:    config.Publisher,
	}, nil
}

// Process advances one raw.received delivery to ACK eligibility.
// The caller may ACK only when the returned disposition is DispositionACK.
func (c *Core) Process(ctx context.Context, delivery Delivery) (Result, error) {
	envelope, rawEvent, err := validateDelivery(delivery)
	if err != nil {
		return resultFor(delivery.Envelope, nil, DispositionIsolate), err
	}

	result := Result{
		Disposition: DispositionRetry,
		MessageID:   envelope.GetMessageId(),
		EventID:     rawEvent.GetEventId(),
	}

	record, found, err := c.metadata.Find(ctx, c.consumerName, envelope.GetMessageId())
	if err != nil {
		return result, fmt.Errorf("find preservation state: %w", err)
	}
	if found {
		if err := validateExistingRecord(c.consumerName, envelope, rawEvent, record); err != nil {
			result.Disposition = DispositionIsolate
			return result, err
		}
		if record.Published {
			result.Disposition = DispositionACK
			return result, nil
		}
		return c.publishAndMark(ctx, result, record)
	}

	rawObject, err := c.rawStore.EnsureDurable(ctx, RawEvidence{
		TenantID:      rawEvent.GetTenantId(),
		EventID:       rawEvent.GetEventId(),
		IngestTime:    rawEvent.GetIngestTime().AsTime().UTC(),
		HashAlgorithm: rawEvent.GetRawHashAlgorithm(),
		Hash:          rawEvent.GetRawHash(),
		Size:          rawEvent.GetRawSize(),
		Bytes:         append([]byte(nil), rawEvent.GetRawPayload()...),
	})
	if err != nil {
		return result, fmt.Errorf("persist raw evidence: %w", err)
	}
	if err := validateRawObject(rawEvent, rawObject); err != nil {
		result.Disposition = DispositionIsolate
		return result, err
	}

	publication, err := c.builder.Build(ctx, envelope, rawEvent, rawObject, delivery.RequestID)
	if err != nil {
		return result, fmt.Errorf("build raw.persisted publication: %w", err)
	}
	if err := validatePublication(publication); err != nil {
		result.Disposition = DispositionIsolate
		return result, err
	}

	record, err = c.metadata.CommitPreservation(ctx, Record{
		ConsumerName:      c.consumerName,
		IncomingMessageID: envelope.GetMessageId(),
		EventID:           rawEvent.GetEventId(),
		RawObject:         rawObject,
		Publication:       copyPublication(publication),
		Published:         false,
	})
	if err != nil {
		return result, fmt.Errorf("commit preservation metadata/outbox: %w", err)
	}
	if err := validateExistingRecord(c.consumerName, envelope, rawEvent, record); err != nil {
		result.Disposition = DispositionIsolate
		return result, err
	}
	if record.Published {
		result.Disposition = DispositionACK
		return result, nil
	}
	return c.publishAndMark(ctx, result, record)
}

func (c *Core) publishAndMark(ctx context.Context, result Result, record Record) (Result, error) {
	if err := validatePublication(record.Publication); err != nil {
		result.Disposition = DispositionIsolate
		return result, err
	}
	if err := c.publisher.Publish(ctx, copyPublication(record.Publication)); err != nil {
		return result, fmt.Errorf("publish raw.persisted: %w", err)
	}
	if err := c.metadata.MarkPublished(ctx, c.consumerName, record.IncomingMessageID); err != nil {
		return result, fmt.Errorf("mark raw.persisted publication complete: %w", err)
	}
	result.Disposition = DispositionACK
	return result, nil
}

func validateDelivery(delivery Delivery) (*contractsv1.CerberoEnvelope, *contractsv1.RawEvent, error) {
	envelope := delivery.Envelope
	if err := contractvalidation.Envelope(envelope); err != nil {
		return envelope, nil, fmt.Errorf("validate envelope: %w", err)
	}
	if envelope.GetMessageType() != MessageTypeRawEventReceived {
		return envelope, nil, fmt.Errorf("message_type: expected %s", MessageTypeRawEventReceived)
	}
	if envelope.GetPayloadSchema() != PayloadSchemaRawEventV1 {
		return envelope, nil, fmt.Errorf("payload_schema: expected %s", PayloadSchemaRawEventV1)
	}
	if envelope.GetTenantId() == "" {
		return envelope, nil, errors.New("tenant_id: envelope tenant is required")
	}
	if delivery.RequestID != "" {
		if err := contractvalidation.UUIDv7("request_id", delivery.RequestID); err != nil {
			return envelope, nil, err
		}
	}

	rawEvent := &contractsv1.RawEvent{}
	if err := envelope.GetPayload().UnmarshalTo(rawEvent); err != nil {
		return envelope, nil, fmt.Errorf("payload: unpack RawEvent: %w", err)
	}
	if err := contractvalidation.RawEvent(rawEvent); err != nil {
		return envelope, rawEvent, fmt.Errorf("validate RawEvent: %w", err)
	}
	if rawEvent.GetTenantId() == "" {
		return envelope, rawEvent, errors.New("tenant_id: RawEvent tenant is required")
	}
	if rawEvent.GetTenantId() != envelope.GetTenantId() {
		return envelope, rawEvent, errors.New("tenant_id: envelope and RawEvent tenants differ")
	}
	return envelope, rawEvent, nil
}

func validateRawObject(event *contractsv1.RawEvent, object RawObject) error {
	if object.StorageURI == "" {
		return errors.New("raw object storage_uri is required")
	}
	if object.SegmentID == "" {
		return errors.New("raw object segment_id is required")
	}
	if object.Length != event.GetRawSize() {
		return errors.New("raw object length does not match RawEvent raw_size")
	}
	if object.Hash != event.GetRawHash() {
		return errors.New("raw object hash does not match RawEvent raw_hash")
	}
	return nil
}

func validatePublication(publication Publication) error {
	if publication.Subject != RawPersistedSubject {
		return fmt.Errorf("outbox subject: expected %s", RawPersistedSubject)
	}
	if err := contractvalidation.UUIDv7("raw_persisted.message_id", publication.MessageID); err != nil {
		return err
	}
	if publication.RequestID != "" {
		if err := contractvalidation.UUIDv7("raw_persisted.request_id", publication.RequestID); err != nil {
			return err
		}
	}
	if len(publication.Payload) == 0 {
		return errors.New("raw.persisted outbox payload is required")
	}
	return nil
}

func validateExistingRecord(consumerName string, envelope *contractsv1.CerberoEnvelope, event *contractsv1.RawEvent, record Record) error {
	if record.ConsumerName != consumerName {
		return errors.New("existing preservation record has a different consumer identity")
	}
	if record.IncomingMessageID != envelope.GetMessageId() {
		return errors.New("existing preservation record has a different incoming message_id")
	}
	if record.EventID != event.GetEventId() {
		return errors.New("existing preservation record has a different event_id")
	}
	if record.RawObject.Hash != event.GetRawHash() {
		return errors.New("existing preservation record has a different raw hash")
	}
	if err := validateRawObject(event, record.RawObject); err != nil {
		return fmt.Errorf("existing preservation record raw object: %w", err)
	}
	if err := validatePublication(record.Publication); err != nil {
		return fmt.Errorf("existing preservation record publication: %w", err)
	}
	return nil
}

func resultFor(envelope *contractsv1.CerberoEnvelope, event *contractsv1.RawEvent, disposition Disposition) Result {
	result := Result{Disposition: disposition}
	if envelope != nil {
		result.MessageID = envelope.GetMessageId()
	}
	if event != nil {
		result.EventID = event.GetEventId()
	}
	return result
}

func copyPublication(publication Publication) Publication {
	copy := publication
	copy.Payload = append([]byte(nil), publication.Payload...)
	return copy
}
