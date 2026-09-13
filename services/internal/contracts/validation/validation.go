package validation

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	contractsv1 "cerbero/services/internal/contracts/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Violation describes a stable contract validation failure.
type Violation struct {
	Field  string
	Reason string
}

func (v Violation) Error() string {
	return fmt.Sprintf("%s: %s", v.Field, v.Reason)
}

func violation(field, reason string) error {
	return Violation{Field: field, Reason: reason}
}

// UUIDv7 validates the canonical hyphenated RFC 9562 UUIDv7 representation.
func UUIDv7(field, value string) error {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return violation(field, "must be a canonical UUID string")
	}
	for index := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !isHex(value[index]) {
			return violation(field, "contains non-hexadecimal characters")
		}
	}
	if value[14] != '7' {
		return violation(field, "must use UUID version 7")
	}
	variant := strings.ToLower(value[19:20])[0]
	if variant != '8' && variant != '9' && variant != 'a' && variant != 'b' {
		return violation(field, "must use the RFC UUID variant")
	}
	return nil
}

func isHex(value byte) bool {
	return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f') || (value >= 'A' && value <= 'F')
}

// Timestamp validates a Protobuf timestamp value.
func Timestamp(field string, timestamp *timestamppb.Timestamp) error {
	if timestamp == nil {
		return violation(field, "is required")
	}
	if err := timestamp.CheckValid(); err != nil {
		return violation(field, err.Error())
	}
	return nil
}

// SHA256LowerHex returns the lowercase SHA-256 digest for the exact supplied bytes.
func SHA256LowerHex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validateSHA256(algorithmField, hashField, algorithm, hash string, data []byte) error {
	if algorithm != "sha256" {
		return violation(algorithmField, "must equal sha256")
	}
	if len(hash) != 64 {
		return violation(hashField, "must be a 64-character lowercase hexadecimal SHA-256 digest")
	}
	for index := range hash {
		value := hash[index]
		if !((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f')) {
			return violation(hashField, "must be a 64-character lowercase hexadecimal SHA-256 digest")
		}
	}
	if SHA256LowerHex(data) != hash {
		return violation(hashField, "does not match the exact raw payload bytes")
	}
	return nil
}

// Envelope validates the M1 invariants of an event-bus envelope.
func Envelope(envelope *contractsv1.CerberoEnvelope) error {
	if envelope == nil {
		return violation("envelope", "is required")
	}
	if envelope.GetContractVersion() != "1" {
		return violation("contract_version", "must equal 1 for v1 envelopes")
	}
	if err := UUIDv7("message_id", envelope.GetMessageId()); err != nil {
		return err
	}
	if envelope.GetMessageType() == "" {
		return violation("message_type", "is required")
	}
	producer := envelope.GetProducer()
	if producer == nil {
		return violation("producer", "is required")
	}
	if producer.GetComponent() == "" {
		return violation("producer.component", "is required")
	}
	if producer.GetComponentVersion() == "" {
		return violation("producer.component_version", "is required")
	}
	if err := Timestamp("emitted_at", envelope.GetEmittedAt()); err != nil {
		return err
	}
	if envelope.GetPayloadSchema() == "" {
		return violation("payload_schema", "is required")
	}
	if envelope.GetPayload() == nil {
		return violation("payload", "is required")
	}
	return nil
}

// RawEvent validates the immutable raw-evidence invariants available in M1.
func RawEvent(event *contractsv1.RawEvent) error {
	if event == nil {
		return violation("raw_event", "is required")
	}
	if err := UUIDv7("event_id", event.GetEventId()); err != nil {
		return err
	}
	if event.GetEventTime() != nil {
		if err := Timestamp("event_time", event.GetEventTime()); err != nil {
			return err
		}
	}
	if err := Timestamp("ingest_time", event.GetIngestTime()); err != nil {
		return err
	}
	if event.GetRawSize() != uint64(len(event.GetRawPayload())) {
		return violation("raw_size", "does not match raw_payload byte length")
	}
	return validateSHA256(
		"raw_hash_algorithm",
		"raw_hash",
		event.GetRawHashAlgorithm(),
		event.GetRawHash(),
		event.GetRawPayload(),
	)
}

// NormalizedEvent validates M1 identity and timestamp invariants of a normalized derivation.
func NormalizedEvent(event *contractsv1.NormalizedEvent) error {
	if event == nil {
		return violation("normalized_event", "is required")
	}
	if err := UUIDv7("normalized_event_id", event.GetNormalizedEventId()); err != nil {
		return err
	}
	if err := UUIDv7("raw_event_id", event.GetRawEventId()); err != nil {
		return err
	}
	return Timestamp("normalized_at", event.GetNormalizedAt())
}

// Transformation validates M1 provenance invariants of a transformation.
func Transformation(transformation *contractsv1.Transformation) error {
	if transformation == nil {
		return violation("transformation", "is required")
	}
	if err := UUIDv7("transformation_id", transformation.GetTransformationId()); err != nil {
		return err
	}
	if err := Timestamp("started_at", transformation.GetStartedAt()); err != nil {
		return err
	}
	if err := Timestamp("completed_at", transformation.GetCompletedAt()); err != nil {
		return err
	}
	if transformation.GetStatus() == contractsv1.TransformationStatus(0) {
		return violation("status", "must not be unspecified")
	}
	if transformation.GetExecutionMode() == contractsv1.ExecutionMode(0) {
		return violation("execution_mode", "must distinguish LIVE, REPLAY, or TEST")
	}
	if transformation.GetStatus() == contractsv1.TransformationStatus(2) && transformation.GetError() == nil {
		return violation("error", "is required when transformation status is FAILED")
	}
	return nil
}
