package ingestcore

import (
	"context"
	"errors"
	"testing"
	"time"

	contractsv1 "cerbero/services/internal/contracts/v1"
	contractvalidation "cerbero/services/internal/contracts/validation"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type fixedClock struct {
	value time.Time
}

func (c fixedClock) Now() time.Time {
	return c.value
}

type sequenceIDs struct {
	values []string
	calls  int
	errAt  int
}

func (g *sequenceIDs) New() (string, error) {
	g.calls++
	if g.errAt != 0 && g.calls == g.errAt {
		return "", errors.New("synthetic ID failure")
	}
	if g.calls > len(g.values) {
		return "", errors.New("no fixed IDs remaining")
	}
	return g.values[g.calls-1], nil
}

type fixedAuthenticator struct {
	principal Principal
	err       error
	calls     int
}

func (a *fixedAuthenticator) Authenticate(context.Context, AdmissionMetadata) (Principal, error) {
	a.calls++
	return a.principal, a.err
}

type fixedAuthorizer struct {
	err        error
	calls      int
	permission string
	metadata   AdmissionMetadata
}

func (a *fixedAuthorizer) Authorize(_ context.Context, _ Principal, permission string, metadata AdmissionMetadata) error {
	a.calls++
	a.permission = permission
	a.metadata = metadata
	return a.err
}

func testCore(t *testing.T, maxPayload uint64, allowMissingSensor bool) (*Core, *sequenceIDs, *fixedAuthenticator, *fixedAuthorizer) {
	t.Helper()
	ids := &sequenceIDs{values: []string{
		"018f22d7-4c00-7000-8000-000000000001",
		"018f22d7-4c00-7000-8000-000000000002",
		"018f22d7-4c00-7000-8000-000000000003",
	}}
	authenticator := &fixedAuthenticator{principal: Principal{ID: "sensor-cert:lab-linux-01"}}
	authorizer := &fixedAuthorizer{}
	core, err := New(Config{
		MaxPayloadSize:       maxPayload,
		AllowMissingSensorID: allowMissingSensor,
		ComponentVersion:     "0.1.0-test",
		PipelineVersion:      "pipeline-test-v1",
		InstanceID:           "ingest-test-1",
		Clock: fixedClock{value: time.Date(
			2026, time.September, 13, 0, 30, 0, 123_000_000, time.UTC,
		)},
		IDs:           ids,
		Authenticator: authenticator,
		Authorizer:    authorizer,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return core, ids, authenticator, authorizer
}

func validRequest() Request {
	eventTime := time.Date(2026, time.September, 12, 23, 59, 58, 0, time.FixedZone("source", -4*60*60))
	sequence := uint64(41)
	return Request{
		RequestID:      "0199428a-2c00-7000-8000-000000000010",
		TenantID:       "tenant-lab",
		SourceID:       "lab-linux-01",
		SensorID:       "sensor-01",
		RemoteIdentity: "spiffe://cerbero.test/sensor/lab-linux-01",
		Transport:      "json-http",
		ContentType:    "application/json",
		Encoding:       "utf-8",
		EventTime:      &eventTime,
		SequenceNumber: &sequence,
		RawPayload:     []byte("line1\r\n line2  \n"),
	}
}

func TestPrepareBuildsValidatedRawEventAndEnvelopeFromExactBytes(t *testing.T) {
	core, ids, authenticator, authorizer := testCore(t, 1024, false)
	request := validRequest()
	original := append([]byte(nil), request.RawPayload...)

	result, err := core.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if ids.calls != 3 {
		t.Fatalf("ID calls = %d, want 3", ids.calls)
	}
	if authenticator.calls != 1 || authorizer.calls != 1 {
		t.Fatalf("hook calls = authn:%d authz:%d, want 1 each", authenticator.calls, authorizer.calls)
	}
	if authorizer.permission != PermissionEventsIngest {
		t.Fatalf("permission = %q, want %q", authorizer.permission, PermissionEventsIngest)
	}
	if authorizer.metadata.SourceID != request.SourceID {
		t.Fatalf("authorized source = %q, want %q", authorizer.metadata.SourceID, request.SourceID)
	}

	raw := result.RawEvent
	if err := contractvalidation.RawEvent(raw); err != nil {
		t.Fatalf("RawEvent validation failed: %v", err)
	}
	if raw.GetEventId() != "018f22d7-4c00-7000-8000-000000000001" {
		t.Fatalf("event_id = %q", raw.GetEventId())
	}
	if raw.GetRawSize() != uint64(len(original)) {
		t.Fatalf("raw_size = %d, want %d", raw.GetRawSize(), len(original))
	}
	if raw.GetRawHashAlgorithm() != RawHashAlgorithm {
		t.Fatalf("raw_hash_algorithm = %q", raw.GetRawHashAlgorithm())
	}
	if raw.GetRawHash() != contractvalidation.SHA256LowerHex(original) {
		t.Fatalf("raw_hash = %q", raw.GetRawHash())
	}
	if raw.GetIntegrityStatus() != contractsv1.IntegrityStatus_INTEGRITY_UNVERIFIED {
		t.Fatalf("integrity_status = %v", raw.GetIntegrityStatus())
	}
	if raw.GetPipelineVersion() != "pipeline-test-v1" {
		t.Fatalf("pipeline_version = %q", raw.GetPipelineVersion())
	}
	if raw.GetIngestTime().AsTime() != time.Date(2026, time.September, 13, 0, 30, 0, 123_000_000, time.UTC) {
		t.Fatalf("ingest_time = %s", raw.GetIngestTime().AsTime())
	}
	if raw.GetEventTime().AsTime() != request.EventTime.UTC() {
		t.Fatalf("event_time = %s, want %s", raw.GetEventTime().AsTime(), request.EventTime.UTC())
	}
	if raw.GetSequenceNumber() != 41 {
		t.Fatalf("sequence_number = %d", raw.GetSequenceNumber())
	}

	envelope := result.Envelope
	if err := contractvalidation.Envelope(envelope); err != nil {
		t.Fatalf("Envelope validation failed: %v", err)
	}
	if envelope.GetMessageId() != "018f22d7-4c00-7000-8000-000000000002" {
		t.Fatalf("message_id = %q", envelope.GetMessageId())
	}
	if envelope.GetTraceId() != "018f22d7-4c00-7000-8000-000000000003" {
		t.Fatalf("trace_id = %q", envelope.GetTraceId())
	}
	if envelope.GetMessageType() != MessageTypeRawEventReceived {
		t.Fatalf("message_type = %q", envelope.GetMessageType())
	}
	if envelope.GetPayloadSchema() != PayloadSchemaRawEventV1 {
		t.Fatalf("payload_schema = %q", envelope.GetPayloadSchema())
	}
	if envelope.GetCausationId() != "" || envelope.GetCorrelationId() != "" {
		t.Fatalf("new ingress envelope must not invent causation/correlation IDs")
	}
	if envelope.GetProducer().GetComponent() != componentCerberoIngest ||
		envelope.GetProducer().GetComponentVersion() != "0.1.0-test" ||
		envelope.GetProducer().GetInstanceId() != "ingest-test-1" {
		t.Fatalf("unexpected producer: %+v", envelope.GetProducer())
	}
	if result.RequestID != request.RequestID {
		t.Fatalf("request_id = %q, want %q", result.RequestID, request.RequestID)
	}

	unpacked := &contractsv1.RawEvent{}
	if err := anypb.UnmarshalTo(envelope.GetPayload(), unpacked, proto.UnmarshalOptions{}); err != nil {
		t.Fatalf("unpack RawEvent: %v", err)
	}
	if !proto.Equal(unpacked, raw) {
		t.Fatalf("envelope payload differs from RawEvent")
	}

	request.RawPayload[0] = 'X'
	*request.SequenceNumber = 99
	if string(raw.GetRawPayload()) != string(original) {
		t.Fatalf("RawEvent bytes changed after caller mutated input: %q", raw.GetRawPayload())
	}
	if raw.GetSequenceNumber() != 41 {
		t.Fatalf("RawEvent sequence changed after caller mutation: %d", raw.GetSequenceNumber())
	}
}

func TestPrepareKeepsEventTimeAbsentWhenSourceDoesNotDeclareOne(t *testing.T) {
	core, _, _, _ := testCore(t, 1024, false)
	request := validRequest()
	request.EventTime = nil

	result, err := core.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if result.RawEvent.GetEventTime() != nil {
		t.Fatalf("event_time = %v, want nil", result.RawEvent.GetEventTime())
	}
}

func TestPreparePreservesUnknownEncodingAsMetadata(t *testing.T) {
	core, _, _, _ := testCore(t, 1024, false)
	request := validRequest()
	request.Encoding = "vendor-x-opaque"

	result, err := core.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if result.RawEvent.GetEncoding() != "vendor-x-opaque" {
		t.Fatalf("encoding = %q", result.RawEvent.GetEncoding())
	}
}

func TestPrepareRejectsOversizePayloadWithoutGeneratingIDs(t *testing.T) {
	core, ids, authenticator, authorizer := testCore(t, 4, false)
	request := validRequest()
	request.RawPayload = []byte("12345")

	_, err := core.Prepare(context.Background(), request)
	assertContractError(t, err, codePayloadTooLarge, contractsv1.ErrorCategory_VALIDATION, false)
	if ids.calls != 0 {
		t.Fatalf("ID calls = %d, want 0 after payload rejection", ids.calls)
	}
	if authenticator.calls != 1 || authorizer.calls != 1 {
		t.Fatalf("hook calls = authn:%d authz:%d, want 1 each", authenticator.calls, authorizer.calls)
	}
}

func TestPrepareEnforcesSensorPolicyWithoutInventingIdentity(t *testing.T) {
	request := validRequest()
	request.SensorID = ""

	strictCore, _, _, _ := testCore(t, 1024, false)
	_, err := strictCore.Prepare(context.Background(), request)
	assertContractError(t, err, codeInvalidPayload, contractsv1.ErrorCategory_VALIDATION, false)

	permissiveCore, _, _, _ := testCore(t, 1024, true)
	result, err := permissiveCore.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("Prepare() with allowed missing sensor error = %v", err)
	}
	if result.RawEvent.GetSensorId() != "" {
		t.Fatalf("sensor_id = %q, want empty rather than invented", result.RawEvent.GetSensorId())
	}
}

func TestPrepareMapsAuthenticationAndAuthorizationFailures(t *testing.T) {
	request := validRequest()

	core, ids, authenticator, authorizer := testCore(t, 1024, false)
	authenticator.err = errors.New("bad certificate")
	_, err := core.Prepare(context.Background(), request)
	assertContractError(t, err, codeUnauthenticated, contractsv1.ErrorCategory_AUTHENTICATION, false)
	if authorizer.calls != 0 || ids.calls != 0 {
		t.Fatalf("auth failure should stop before authorization and IDs: authz=%d ids=%d", authorizer.calls, ids.calls)
	}

	core, ids, authenticator, authorizer = testCore(t, 1024, false)
	authorizer.err = errors.New("scope mismatch")
	_, err = core.Prepare(context.Background(), request)
	assertContractError(t, err, codeForbidden, contractsv1.ErrorCategory_AUTHORIZATION, false)
	if authenticator.calls != 1 || ids.calls != 0 {
		t.Fatalf("authorization failure should stop before IDs: authn=%d ids=%d", authenticator.calls, ids.calls)
	}
}

func TestNewRequiresExplicitPolicyAndProvenanceConfiguration(t *testing.T) {
	authenticator := &fixedAuthenticator{principal: Principal{ID: "principal"}}
	authorizer := &fixedAuthorizer{}
	base := Config{
		MaxPayloadSize:   1024,
		ComponentVersion: "0.1.0",
		PipelineVersion:  "pipeline-v1",
		InstanceID:       "instance-1",
		Authenticator:    authenticator,
		Authorizer:       authorizer,
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "payload size", mutate: func(config *Config) { config.MaxPayloadSize = 0 }},
		{name: "component version", mutate: func(config *Config) { config.ComponentVersion = "" }},
		{name: "pipeline version", mutate: func(config *Config) { config.PipelineVersion = "" }},
		{name: "instance ID", mutate: func(config *Config) { config.InstanceID = "" }},
		{name: "authenticator", mutate: func(config *Config) { config.Authenticator = nil }},
		{name: "authorizer", mutate: func(config *Config) { config.Authorizer = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := base
			test.mutate(&config)
			if _, err := New(config); err == nil {
				t.Fatalf("New() error = nil, want configuration error")
			}
		})
	}
}

func TestPrepareMapsIDGenerationFailureToStableInternalError(t *testing.T) {
	core, ids, _, _ := testCore(t, 1024, false)
	ids.errAt = 2

	_, err := core.Prepare(context.Background(), validRequest())
	assertContractError(t, err, codeSystemInternal, contractsv1.ErrorCategory_INTERNAL, false)
	if ids.calls != 2 {
		t.Fatalf("ID calls = %d, want 2", ids.calls)
	}
}

func assertContractError(
	t *testing.T,
	err error,
	code string,
	category contractsv1.ErrorCategory,
	retryable bool,
) {
	t.Helper()
	var ingestErr *Error
	if !errors.As(err, &ingestErr) {
		t.Fatalf("error = %T %v, want *ingestcore.Error", err, err)
	}
	if ingestErr.Contract.GetCode() != code {
		t.Fatalf("error code = %q, want %q", ingestErr.Contract.GetCode(), code)
	}
	if ingestErr.Contract.GetCategory() != category {
		t.Fatalf("error category = %v, want %v", ingestErr.Contract.GetCategory(), category)
	}
	if ingestErr.Contract.GetRetryable() != retryable {
		t.Fatalf("retryable = %v, want %v", ingestErr.Contract.GetRetryable(), retryable)
	}
	if ingestErr.Contract.GetComponent() != componentCerberoIngest {
		t.Fatalf("component = %q", ingestErr.Contract.GetComponent())
	}
	if ingestErr.Contract.GetRequestId() != validRequest().RequestID {
		t.Fatalf("request_id = %q", ingestErr.Contract.GetRequestId())
	}
}

func TestPrepareTreatsInvalidGeneratedIdentityAsInternalFailure(t *testing.T) {
	core, ids, _, _ := testCore(t, 1024, false)
	ids.values[0] = "67e55044-10b1-426f-9247-bb680e5fe0c8"

	_, err := core.Prepare(context.Background(), validRequest())
	assertContractError(t, err, codeSystemInternal, contractsv1.ErrorCategory_INTERNAL, false)
	if ids.calls != 1 {
		t.Fatalf("ID calls = %d, want 1", ids.calls)
	}
}
