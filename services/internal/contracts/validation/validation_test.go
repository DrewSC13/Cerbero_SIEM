package validation

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	contractsv1 "cerbero/services/internal/contracts/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	eventID          = "018f47d0-7b5c-7cc0-98c0-3f2b9859d3e1"
	messageID        = "018f47d0-7b5c-7cc1-98c0-3f2b9859d3e2"
	transformationID = "018f47d0-7b5c-7cc2-98c0-3f2b9859d3e3"
)

func fixtureTimestamp() *timestamppb.Timestamp {
	return &timestamppb.Timestamp{Seconds: 1}
}

func fixtureRawEvent() *contractsv1.RawEvent {
	sequence := uint64(42)
	payload := []byte("abc")
	return &contractsv1.RawEvent{
		EventId:          eventID,
		TenantId:         "tenant-a",
		SourceId:         "source-a",
		SensorId:         "sensor-a",
		IngestTime:       fixtureTimestamp(),
		ContentType:      "text/plain",
		Encoding:         "utf-8",
		RawPayload:       payload,
		RawSize:          uint64(len(payload)),
		RawHashAlgorithm: "sha256",
		RawHash:          SHA256LowerHex(payload),
		Transport:        "test",
		RemoteIdentity:   "fixture",
		SequenceNumber:   &sequence,
		IntegrityStatus:  contractsv1.IntegrityStatus(1),
		PipelineVersion:  "v1",
	}
}

func sharedFixture(t *testing.T) []byte {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source),
		"../../../../tests/fixtures/contracts/v1/raw_event_minimal.hex",
	))
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read shared fixture: %v", err)
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(contents)))
	if err != nil {
		t.Fatalf("decode shared fixture: %v", err)
	}
	return decoded
}

func TestSharedWireFixtureRoundTrips(t *testing.T) {
	fixture := sharedFixture(t)
	event := new(contractsv1.RawEvent)
	if err := proto.Unmarshal(fixture, event); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	if err := RawEvent(event); err != nil {
		t.Fatalf("validate fixture: %v", err)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(event)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if string(encoded) != string(fixture) {
		t.Fatal("deterministic Go encoding differs from the shared wire fixture")
	}
}

func TestRawHashMismatchIsRejected(t *testing.T) {
	event := fixtureRawEvent()
	event.RawPayload = append(event.RawPayload, '\n')
	event.RawSize++
	if err := RawEvent(event); err == nil {
		t.Fatal("expected hash mismatch")
	} else if got := err.(Violation).Field; got != "raw_hash" {
		t.Fatalf("unexpected violation field %q", got)
	}
}

func TestRawSizeMismatchIsRejected(t *testing.T) {
	event := fixtureRawEvent()
	event.RawSize++
	if err := RawEvent(event); err == nil {
		t.Fatal("expected size mismatch")
	} else if got := err.(Violation).Field; got != "raw_size" {
		t.Fatalf("unexpected violation field %q", got)
	}
}

func TestDuplicateDeliveryKeepsMessageID(t *testing.T) {
	rawBytes, err := proto.Marshal(fixtureRawEvent())
	if err != nil {
		t.Fatalf("marshal raw event: %v", err)
	}
	envelope := &contractsv1.CerberoEnvelope{
		ContractVersion: "1",
		MessageId:       messageID,
		MessageType:     "RawEventReceived",
		TenantId:        "tenant-a",
		Producer: &contractsv1.Producer{
			Component:        "cerbero-ingest",
			ComponentVersion: "0.0.0",
			InstanceId:       "instance-a",
		},
		EmittedAt:     fixtureTimestamp(),
		TraceId:       "trace-a",
		PayloadSchema: "cerbero.raw_event.v1",
		Payload: &anypb.Any{
			TypeUrl: "type.googleapis.com/cerbero.contracts.v1.RawEvent",
			Value:   rawBytes,
		},
	}
	if err := Envelope(envelope); err != nil {
		t.Fatalf("validate envelope: %v", err)
	}
	encoded, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	redelivery := new(contractsv1.CerberoEnvelope)
	if err := proto.Unmarshal(encoded, redelivery); err != nil {
		t.Fatalf("unmarshal redelivery: %v", err)
	}
	if redelivery.GetMessageId() != messageID {
		t.Fatalf("message_id changed across redelivery: %q", redelivery.GetMessageId())
	}
}

func TestExecutionModesSurviveSerialization(t *testing.T) {
	for _, executionMode := range []contractsv1.ExecutionMode{1, 2, 3} {
		transformation := &contractsv1.Transformation{
			TransformationId:  transformationID,
			InputObjectId:     eventID,
			InputObjectType:   "RawEvent",
			Component:         "cerbero-normalizer",
			ComponentVersion:  "0.0.0",
			ConfigurationHash: "fixture",
			StartedAt:         fixtureTimestamp(),
			CompletedAt:       fixtureTimestamp(),
			Status:            contractsv1.TransformationStatus(1),
			ExecutionMode:     executionMode,
		}
		if err := Transformation(transformation); err != nil {
			t.Fatalf("validate execution transformation: %v", err)
		}
		encoded, err := proto.Marshal(transformation)
		if err != nil {
			t.Fatalf("marshal transformation: %v", err)
		}
		decoded := new(contractsv1.Transformation)
		if err := proto.Unmarshal(encoded, decoded); err != nil {
			t.Fatalf("unmarshal transformation: %v", err)
		}
		if decoded.GetExecutionMode() != executionMode {
			t.Fatalf("execution mode changed: %v", decoded.GetExecutionMode())
		}
	}
}

func TestInvalidUUIDTimestampAndMissingPayloadAreRejected(t *testing.T) {
	if err := UUIDv7("message_id", "67e55044-10b1-426f-9247-bb680e5fe0c8"); err == nil {
		t.Fatal("expected UUIDv4 rejection")
	}
	if err := Timestamp("ingest_time", &timestamppb.Timestamp{Seconds: 1, Nanos: -1}); err == nil {
		t.Fatal("expected invalid timestamp rejection")
	}
	envelope := &contractsv1.CerberoEnvelope{
		ContractVersion: "1",
		MessageId:       messageID,
		MessageType:     "RawEventReceived",
		Producer: &contractsv1.Producer{
			Component:        "cerbero-ingest",
			ComponentVersion: "0.0.0",
		},
		EmittedAt:     fixtureTimestamp(),
		PayloadSchema: "cerbero.raw_event.v1",
	}
	if err := Envelope(envelope); err == nil {
		t.Fatal("expected missing payload rejection")
	}
}

func TestParserFailurePreservesRawEventAndErrorProvenance(t *testing.T) {
	raw := fixtureRawEvent()
	before, err := proto.MarshalOptions{Deterministic: true}.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal raw event: %v", err)
	}
	transformation := &contractsv1.Transformation{
		TransformationId:  transformationID,
		InputObjectId:     raw.GetEventId(),
		InputObjectType:   "RawEvent",
		Component:         "cerbero-normalizer",
		ComponentVersion:  "0.0.0",
		ConfigurationHash: "fixture",
		StartedAt:         fixtureTimestamp(),
		CompletedAt:       fixtureTimestamp(),
		Status:            contractsv1.TransformationStatus(2),
		Error: &contractsv1.CerberoError{
			Code:      "CER-PARSE-FAILED",
			Category:  contractsv1.ErrorCategory(6),
			Message:   "fixture parser failure",
			Retryable: false,
			Component: "cerbero-normalizer",
		},
		ExecutionMode: contractsv1.ExecutionMode(1),
	}
	if err := Transformation(transformation); err != nil {
		t.Fatalf("validate failure provenance: %v", err)
	}
	after, err := proto.MarshalOptions{Deterministic: true}.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal raw event after failure: %v", err)
	}
	if string(after) != string(before) {
		t.Fatal("parser failure mutated RawEvent")
	}
}
