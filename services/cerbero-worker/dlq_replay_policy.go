package main

import "strings"

const (
	normalizationDLQSchema = "cerbero.normalization_dlq.v1"
	rawPersistedSubject    = "cerbero.v1.raw.persisted"
	replayExecutionMode    = "REPLAY"
)

type replayEligibilityReason string

const (
	replayEligible          replayEligibilityReason = "ELIGIBLE"
	replayDisabled          replayEligibilityReason = "DISABLED"
	replayInvalidRecord     replayEligibilityReason = "INVALID_RECORD"
	replayUnsupportedOrigin replayEligibilityReason = "UNSUPPORTED_ORIGIN"
	replayPermanentFailure  replayEligibilityReason = "PERMANENT_FAILURE"
	replaySourceUnavailable replayEligibilityReason = "SOURCE_UNAVAILABLE"
)

type normalizationDeadLetter struct {
	SchemaVersion         string  `json:"schema_version"`
	DLQRecordID           string  `json:"dlq_record_id"`
	OriginalMessageID     *string `json:"original_message_id"`
	OriginalSubject       string  `json:"original_subject"`
	OriginalPayloadSHA256 string  `json:"original_payload_sha256"`
	Retryable             bool    `json:"retryable"`
	StreamSequence        *uint64 `json:"stream_sequence"`
	ReplayRootDLQRecordID *string `json:"replay_root_dlq_record_id"`
}

type replayPolicy struct {
	MaxAutomaticAttempts uint32
}

type replayDecision struct {
	Eligible              bool
	Reason                replayEligibilityReason
	MaxAttempts           uint32
	ExecutionMode         string
	LifecycleKey          string
	ReplayRootDLQRecordID string
	SourceDLQRecordID     string
	SourceStreamSequence  uint64
	SourcePayloadSHA256   string
}

func evaluateNormalizationReplay(
	record normalizationDeadLetter,
	policy replayPolicy,
) replayDecision {
	decision := replayDecision{
		Reason:            replayInvalidRecord,
		MaxAttempts:       policy.MaxAutomaticAttempts,
		ExecutionMode:     replayExecutionMode,
		SourceDLQRecordID: record.DLQRecordID,
	}

	if policy.MaxAutomaticAttempts == 0 {
		decision.Reason = replayDisabled
		return decision
	}
	if record.SchemaVersion != normalizationDLQSchema ||
		strings.TrimSpace(record.DLQRecordID) == "" ||
		!isLowerHexSHA256(record.OriginalPayloadSHA256) {
		return decision
	}
	if record.OriginalSubject != rawPersistedSubject {
		decision.Reason = replayUnsupportedOrigin
		return decision
	}
	if record.StreamSequence == nil || *record.StreamSequence == 0 {
		decision.Reason = replaySourceUnavailable
		return decision
	}

	replayRoot := strings.TrimSpace(record.DLQRecordID)
	if record.ReplayRootDLQRecordID != nil {
		replayRoot = strings.TrimSpace(*record.ReplayRootDLQRecordID)
		if replayRoot == "" {
			return decision
		}
	}
	decision.LifecycleKey = replayLifecycleKey(replayRoot)
	decision.ReplayRootDLQRecordID = replayRoot
	decision.SourceStreamSequence = *record.StreamSequence
	decision.SourcePayloadSHA256 = record.OriginalPayloadSHA256
	if !record.Retryable {
		decision.Reason = replayPermanentFailure
		return decision
	}

	decision.Eligible = true
	decision.Reason = replayEligible
	return decision
}

func replayLifecycleKey(replayRootDLQRecordID string) string {
	return "normalization-replay:v1:dlq:" + replayRootDLQRecordID
}

func isLowerHexSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
