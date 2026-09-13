package ingestcore

import (
	"context"
	"errors"
	"fmt"
	"time"

	contractsv1 "cerbero/services/internal/contracts/v1"
	contractvalidation "cerbero/services/internal/contracts/validation"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// PermissionEventsIngest is the locked ingest authorization capability.
	PermissionEventsIngest = "events.ingest"
	// MessageTypeRawEventReceived is the v1 domain event carried by the initial ingest envelope.
	MessageTypeRawEventReceived = "RawEventReceived"
	// PayloadSchemaRawEventV1 is the versioned RawEvent payload schema name.
	PayloadSchemaRawEventV1 = "cerbero.raw_event.v1"
	// RawHashAlgorithm is the locked digest algorithm for original evidence bytes.
	RawHashAlgorithm = "sha256"
)

// Principal is the authenticated transport identity presented to authorization policy.
type Principal struct {
	ID string
}

// AdmissionMetadata contains source and transport metadata available before raw-event construction.
type AdmissionMetadata struct {
	RequestID      string
	TenantID       string
	SourceID       string
	SensorID       string
	RemoteIdentity string
	Transport      string
}

// Authenticator is the transport-specific identity hook used by the common ingest core.
type Authenticator interface {
	Authenticate(context.Context, AdmissionMetadata) (Principal, error)
}

// Authorizer enforces the locked events.ingest capability and any tenant/source/sensor/protocol scope.
type Authorizer interface {
	Authorize(context.Context, Principal, string, AdmissionMetadata) error
}

// AdmissionLimiter enforces event-admission rate policy after authn/authz and before payload receipt.
type AdmissionLimiter interface {
	Allow(context.Context, Principal, AdmissionMetadata) bool
}

// Config contains policy and provenance values required by IngestCore.
type Config struct {
	MaxPayloadSize       uint64
	AllowMissingSensorID bool
	ComponentVersion     string
	PipelineVersion      string
	InstanceID           string
	Clock                Clock
	IDs                  IDGenerator
	Authenticator        Authenticator
	Authorizer           Authorizer
	Limiter              AdmissionLimiter
}

// Request is the frontend-neutral input accepted by IngestCore after transport framing.
type Request struct {
	RequestID      string
	TenantID       string
	SourceID       string
	SensorID       string
	RemoteIdentity string
	Transport      string
	ContentType    string
	Encoding       string
	EventTime      *time.Time
	SequenceNumber *uint64
	RawPayload     []byte
}

// Admission is an authenticated and authorized ingest session bound to source metadata.
type Admission interface {
	Prepare(Request) (*Result, error)
}

type authorizedAdmission struct {
	core     *Core
	metadata AdmissionMetadata
}

// Result contains the immutable RawEvent and its publish-ready common envelope.
// It is not an acceptance response: durable JetStream admission is intentionally outside M2 Step 1.
type Result struct {
	RequestID string
	RawEvent  *contractsv1.RawEvent
	Envelope  *contractsv1.CerberoEnvelope
}

// Core implements the common acceptance semantics shared by ingest frontends.
type Core struct {
	maxPayloadSize       uint64
	allowMissingSensorID bool
	componentVersion     string
	pipelineVersion      string
	instanceID           string
	clock                Clock
	ids                  IDGenerator
	authenticator        Authenticator
	authorizer           Authorizer
	limiter              AdmissionLimiter
}

// New validates ingest-core configuration and returns a reusable Core.
func New(config Config) (*Core, error) {
	if config.MaxPayloadSize == 0 {
		return nil, errors.New("max payload size must be explicitly configured and greater than zero")
	}
	if config.ComponentVersion == "" {
		return nil, errors.New("component version is required")
	}
	if config.PipelineVersion == "" {
		return nil, errors.New("pipeline version is required")
	}
	if config.InstanceID == "" {
		return nil, errors.New("producer instance ID is required")
	}
	if config.Authenticator == nil {
		return nil, errors.New("authenticator hook is required")
	}
	if config.Authorizer == nil {
		return nil, errors.New("authorizer hook is required")
	}
	if config.Limiter == nil {
		return nil, errors.New("admission limiter is required")
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	if config.IDs == nil {
		generator := NewUUIDv7Generator(config.Clock, nil)
		config.IDs = generator
	}

	return &Core{
		maxPayloadSize:       config.MaxPayloadSize,
		allowMissingSensorID: config.AllowMissingSensorID,
		componentVersion:     config.ComponentVersion,
		pipelineVersion:      config.PipelineVersion,
		instanceID:           config.InstanceID,
		clock:                config.Clock,
		ids:                  config.IDs,
		authenticator:        config.Authenticator,
		authorizer:           config.Authorizer,
		limiter:              config.Limiter,
	}, nil
}

// Begin authenticates and authorizes source metadata before a frontend receives the raw payload.
// The returned Admission is bound to the authorized metadata and cannot be reused with different identity metadata.
func (c *Core) Begin(ctx context.Context, metadata AdmissionMetadata) (Admission, error) {
	principal, err := c.authenticator.Authenticate(ctx, metadata)
	if err != nil || principal.ID == "" {
		if err == nil {
			err = errors.New("authenticator returned an empty principal ID")
		}
		return nil, newError(
			codeUnauthenticated,
			contractsv1.ErrorCategory_AUTHENTICATION,
			"source authentication failed",
			false,
			metadata.RequestID,
			err,
			nil,
		)
	}
	if err := c.authorizer.Authorize(ctx, principal, PermissionEventsIngest, metadata); err != nil {
		return nil, newError(
			codeForbidden,
			contractsv1.ErrorCategory_AUTHORIZATION,
			"source is not authorized to ingest events",
			false,
			metadata.RequestID,
			err,
			nil,
		)
	}
	if err := c.validateAdmissionMetadata(metadata); err != nil {
		return nil, err
	}
	if !c.limiter.Allow(ctx, principal, metadata) {
		return nil, newError(
			codeRateLimited,
			contractsv1.ErrorCategory_RATE_LIMIT,
			"ingest admission rate limit exceeded",
			true,
			metadata.RequestID,
			nil,
			nil,
		)
	}

	return &authorizedAdmission{
		core:     c,
		metadata: metadata,
	}, nil
}

// Prepare is the one-shot compatibility path for frontends that already have the complete payload.
// Streaming/request-response frontends must call Begin before receiving the payload.
func (c *Core) Prepare(ctx context.Context, request Request) (*Result, error) {
	admission, err := c.Begin(ctx, requestAdmissionMetadata(request))
	if err != nil {
		return nil, err
	}
	return admission.Prepare(request)
}

// Prepare constructs a validated RawEvent and envelope after source admission has completed.
func (a *authorizedAdmission) Prepare(request Request) (*Result, error) {
	if requestAdmissionMetadata(request) != a.metadata {
		return nil, a.core.invalidPayload(
			a.metadata.RequestID,
			"admission_metadata",
			"ingest metadata changed after authentication and authorization",
			nil,
		)
	}
	return a.core.prepare(request)
}

func (c *Core) prepare(request Request) (*Result, error) {
	if err := c.validatePayload(request); err != nil {
		return nil, err
	}

	eventID, err := c.generateID(request.RequestID, "event_id")
	if err != nil {
		return nil, err
	}
	messageID, err := c.generateID(request.RequestID, "message_id")
	if err != nil {
		return nil, err
	}
	traceID, err := c.generateID(request.RequestID, "trace_id")
	if err != nil {
		return nil, err
	}

	ingestAt := c.clock.Now().UTC()
	ingestTime := timestamppb.New(ingestAt)
	if err := ingestTime.CheckValid(); err != nil {
		return nil, c.internalError(request.RequestID, "capture ingest_time", err)
	}

	var eventTime *timestamppb.Timestamp
	if request.EventTime != nil {
		eventTime = timestamppb.New(request.EventTime.UTC())
		if err := eventTime.CheckValid(); err != nil {
			return nil, c.invalidPayload(request.RequestID, "event_time", "source event_time is outside the Protobuf timestamp range", err)
		}
	}

	rawBytes := append([]byte(nil), request.RawPayload...)
	rawEvent := &contractsv1.RawEvent{
		EventId:          eventID,
		TenantId:         request.TenantID,
		SourceId:         request.SourceID,
		SensorId:         request.SensorID,
		EventTime:        eventTime,
		IngestTime:       ingestTime,
		ContentType:      request.ContentType,
		Encoding:         request.Encoding,
		RawPayload:       rawBytes,
		RawSize:          uint64(len(rawBytes)),
		RawHashAlgorithm: RawHashAlgorithm,
		RawHash:          contractvalidation.SHA256LowerHex(rawBytes),
		Transport:        request.Transport,
		RemoteIdentity:   request.RemoteIdentity,
		SequenceNumber:   copyOptionalUint64(request.SequenceNumber),
		IntegrityStatus:  contractsv1.IntegrityStatus_INTEGRITY_UNVERIFIED,
		PipelineVersion:  c.pipelineVersion,
	}
	if err := contractvalidation.RawEvent(rawEvent); err != nil {
		return nil, c.contractValidationError(request.RequestID, err)
	}

	payload, err := anypb.New(rawEvent)
	if err != nil {
		return nil, c.internalError(request.RequestID, "pack RawEvent payload", err)
	}
	envelope := &contractsv1.CerberoEnvelope{
		ContractVersion: "1",
		MessageId:       messageID,
		MessageType:     MessageTypeRawEventReceived,
		TenantId:        request.TenantID,
		Producer: &contractsv1.Producer{
			Component:        componentCerberoIngest,
			ComponentVersion: c.componentVersion,
			InstanceId:       c.instanceID,
		},
		EmittedAt:     timestamppb.New(ingestAt),
		TraceId:       traceID,
		CausationId:   "",
		CorrelationId: "",
		PayloadSchema: PayloadSchemaRawEventV1,
		Payload:       payload,
	}
	if err := contractvalidation.Envelope(envelope); err != nil {
		return nil, c.contractValidationError(request.RequestID, err)
	}

	return &Result{
		RequestID: request.RequestID,
		RawEvent:  rawEvent,
		Envelope:  envelope,
	}, nil
}

func (c *Core) generateID(requestID, field string) (string, error) {
	value, err := c.ids.New()
	if err != nil {
		return "", c.internalError(requestID, "generate "+field, err)
	}
	if err := contractvalidation.UUIDv7(field, value); err != nil {
		return "", c.internalError(requestID, "validate generated "+field, err)
	}
	return value, nil
}

func (c *Core) validateAdmissionMetadata(metadata AdmissionMetadata) error {
	required := []struct {
		field string
		value string
	}{
		{field: "tenant_id", value: metadata.TenantID},
		{field: "source_id", value: metadata.SourceID},
		{field: "remote_identity", value: metadata.RemoteIdentity},
		{field: "transport", value: metadata.Transport},
	}
	for _, item := range required {
		if item.value == "" {
			return c.invalidPayload(metadata.RequestID, item.field, "required ingest metadata is missing", nil)
		}
	}
	if metadata.SensorID == "" && !c.allowMissingSensorID {
		return c.invalidPayload(metadata.RequestID, "sensor_id", "sensor identity is required by this source policy", nil)
	}
	return nil
}

func (c *Core) validatePayload(request Request) error {
	if uint64(len(request.RawPayload)) > c.maxPayloadSize {
		return newError(
			codePayloadTooLarge,
			contractsv1.ErrorCategory_VALIDATION,
			"payload exceeds the configured ingest limit",
			false,
			request.RequestID,
			nil,
			map[string]string{
				"field":            "raw_payload",
				"max_payload_size": fmt.Sprintf("%d", c.maxPayloadSize),
			},
		)
	}
	return nil
}

func requestAdmissionMetadata(request Request) AdmissionMetadata {
	return AdmissionMetadata{
		RequestID:      request.RequestID,
		TenantID:       request.TenantID,
		SourceID:       request.SourceID,
		SensorID:       request.SensorID,
		RemoteIdentity: request.RemoteIdentity,
		Transport:      request.Transport,
	}
}

func (c *Core) invalidPayload(requestID, field, message string, cause error) *Error {
	return newError(
		codeInvalidPayload,
		contractsv1.ErrorCategory_VALIDATION,
		message,
		false,
		requestID,
		cause,
		map[string]string{"field": field},
	)
}

func (c *Core) internalError(requestID, operation string, cause error) *Error {
	return newError(
		codeSystemInternal,
		contractsv1.ErrorCategory_INTERNAL,
		"internal ingest failure",
		false,
		requestID,
		cause,
		map[string]string{"operation": operation},
	)
}

func (c *Core) contractValidationError(requestID string, err error) *Error {
	metadata := map[string]string{}
	var violation contractvalidation.Violation
	if errors.As(err, &violation) {
		metadata["field"] = violation.Field
	}
	return newError(
		codeInvalidPayload,
		contractsv1.ErrorCategory_VALIDATION,
		"constructed ingest contract failed validation",
		false,
		requestID,
		err,
		metadata,
	)
}

func copyOptionalUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
