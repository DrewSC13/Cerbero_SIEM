package main

import "testing"

func TestEvaluateNormalizationReplayEligible(t *testing.T) {
	record := validRetryableDeadLetter()
	decision := evaluateNormalizationReplay(record, replayPolicy{MaxAutomaticAttempts: 2})

	if !decision.Eligible {
		t.Fatalf("expected eligible replay, got reason %q", decision.Reason)
	}
	if decision.Reason != replayEligible {
		t.Fatalf("unexpected reason: %q", decision.Reason)
	}
	if decision.MaxAttempts != 2 {
		t.Fatalf("unexpected replay max attempts: %d", decision.MaxAttempts)
	}
	if decision.ExecutionMode != replayExecutionMode {
		t.Fatalf("unexpected execution mode: %q", decision.ExecutionMode)
	}
	expectedLifecycleKey := replayLifecycleKey(record.DLQRecordID)
	if decision.LifecycleKey != expectedLifecycleKey {
		t.Fatalf("unexpected lifecycle key: %q", decision.LifecycleKey)
	}
	if decision.ReplayRootDLQRecordID != record.DLQRecordID {
		t.Fatalf("unexpected replay root: %q", decision.ReplayRootDLQRecordID)
	}
	if decision.SourceDLQRecordID != record.DLQRecordID {
		t.Fatalf("unexpected source DLQ record: %q", decision.SourceDLQRecordID)
	}
	if decision.SourceStreamSequence != *record.StreamSequence {
		t.Fatalf("unexpected stream sequence: %d", decision.SourceStreamSequence)
	}
	if decision.SourcePayloadSHA256 != record.OriginalPayloadSHA256 {
		t.Fatalf("unexpected source payload hash: %q", decision.SourcePayloadSHA256)
	}
}

func TestReplayLifecycleIdentityFollowsExplicitRootAcrossNewDLQRecords(t *testing.T) {
	first := validRetryableDeadLetter()
	second := validRetryableDeadLetter()
	second.DLQRecordID = "01995000-0000-7000-8000-000000000003"
	second.StreamSequence = uint64Pointer(84)
	second.ReplayRootDLQRecordID = &first.DLQRecordID

	firstDecision := evaluateNormalizationReplay(
		first,
		replayPolicy{MaxAutomaticAttempts: 2},
	)
	secondDecision := evaluateNormalizationReplay(
		second,
		replayPolicy{MaxAutomaticAttempts: 2},
	)

	if firstDecision.LifecycleKey != secondDecision.LifecycleKey {
		t.Fatalf(
			"replay lineage must survive new DLQ records and stream sequences: first=%q second=%q",
			firstDecision.LifecycleKey,
			secondDecision.LifecycleKey,
		)
	}
	if secondDecision.ReplayRootDLQRecordID != first.DLQRecordID {
		t.Fatalf("unexpected replay root: %q", secondDecision.ReplayRootDLQRecordID)
	}

	independent := validRetryableDeadLetter()
	independent.DLQRecordID = "01995000-0000-7000-8000-000000000004"
	independentDecision := evaluateNormalizationReplay(
		independent,
		replayPolicy{MaxAutomaticAttempts: 2},
	)
	if firstDecision.LifecycleKey == independentDecision.LifecycleKey {
		t.Fatal("independent initial DLQ records must not share replay lifecycle")
	}
}

func TestEvaluateNormalizationReplayRejectsPermanentFailure(t *testing.T) {
	record := validRetryableDeadLetter()
	record.Retryable = false

	decision := evaluateNormalizationReplay(record, replayPolicy{MaxAutomaticAttempts: 2})

	if decision.Eligible || decision.Reason != replayPermanentFailure {
		t.Fatalf("expected permanent failure rejection, got %+v", decision)
	}
}

func TestEvaluateNormalizationReplayRejectsUnavailableSource(t *testing.T) {
	record := validRetryableDeadLetter()
	record.StreamSequence = nil

	decision := evaluateNormalizationReplay(record, replayPolicy{MaxAutomaticAttempts: 2})

	if decision.Eligible || decision.Reason != replaySourceUnavailable {
		t.Fatalf("expected unavailable source rejection, got %+v", decision)
	}
}

func TestEvaluateNormalizationReplayRejectsInvalidContractIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*normalizationDeadLetter)
		reason replayEligibilityReason
	}{
		{
			name: "schema",
			mutate: func(record *normalizationDeadLetter) {
				record.SchemaVersion = "cerbero.normalization_dlq.v2"
			},
			reason: replayInvalidRecord,
		},
		{
			name: "dlq record id",
			mutate: func(record *normalizationDeadLetter) {
				record.DLQRecordID = " "
			},
			reason: replayInvalidRecord,
		},
		{
			name: "payload hash",
			mutate: func(record *normalizationDeadLetter) {
				record.OriginalPayloadSHA256 = "ABC"
			},
			reason: replayInvalidRecord,
		},
		{
			name: "origin subject",
			mutate: func(record *normalizationDeadLetter) {
				record.OriginalSubject = "cerbero.v1.raw.received"
			},
			reason: replayUnsupportedOrigin,
		},
		{
			name: "blank replay root",
			mutate: func(record *normalizationDeadLetter) {
				blank := " "
				record.ReplayRootDLQRecordID = &blank
			},
			reason: replayInvalidRecord,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := validRetryableDeadLetter()
			test.mutate(&record)

			decision := evaluateNormalizationReplay(
				record,
				replayPolicy{MaxAutomaticAttempts: 2},
			)

			if decision.Eligible || decision.Reason != test.reason {
				t.Fatalf("unexpected replay decision: %+v", decision)
			}
		})
	}
}

func TestEvaluateNormalizationReplayCanBeDisabled(t *testing.T) {
	record := validRetryableDeadLetter()

	decision := evaluateNormalizationReplay(record, replayPolicy{})

	if decision.Eligible || decision.Reason != replayDisabled {
		t.Fatalf("expected disabled replay policy, got %+v", decision)
	}
}

func validRetryableDeadLetter() normalizationDeadLetter {
	messageID := "01995000-0000-7000-8000-000000000001"
	streamSequence := uint64(42)
	return normalizationDeadLetter{
		SchemaVersion:         normalizationDLQSchema,
		DLQRecordID:           "01995000-0000-7000-8000-000000000002",
		OriginalMessageID:     &messageID,
		OriginalSubject:       rawPersistedSubject,
		OriginalPayloadSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Retryable:             true,
		StreamSequence:        &streamSequence,
	}
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}
