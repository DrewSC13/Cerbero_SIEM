package main

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestBuildSelectiveReplayMessage(t *testing.T) {
	sourcePayload := []byte("stable raw.persisted envelope bytes")
	source := &jetstream.RawStreamMsg{
		Subject:  rawPersistedSubject,
		Sequence: 42,
		Header:   make(nats.Header),
		Data:     append([]byte(nil), sourcePayload...),
	}
	source.Header.Set(nats.MsgIdHdr, "original-publication-id")
	source.Header.Set(requestIDHeader, "01995000-0000-7000-8000-000000000010")

	decision := replayDecision{
		Eligible:              true,
		Reason:                replayEligible,
		MaxAttempts:           2,
		ExecutionMode:         replayExecutionMode,
		LifecycleKey:          replayLifecycleKey("root-1"),
		ReplayRootDLQRecordID: "root-1",
		SourceDLQRecordID:     "dlq-source-1",
		SourceStreamSequence:  42,
		SourcePayloadSHA256:   sha256LowerHex(sourcePayload),
	}
	reservation := replayReservation{
		Reserved:              true,
		Attempt:               1,
		MaxAttempts:           2,
		LifecycleKey:          decision.LifecycleKey,
		ReplayRootDLQRecordID: decision.ReplayRootDLQRecordID,
	}

	message, err := buildSelectiveReplayMessage(source, decision, reservation)
	if err != nil {
		t.Fatalf("buildSelectiveReplayMessage: %v", err)
	}
	if message.Subject != selectiveReplaySubject {
		t.Fatalf("subject = %q", message.Subject)
	}
	if string(message.Data) != string(sourcePayload) {
		t.Fatalf("payload changed: %q", message.Data)
	}
	if message.Header.Get(executionModeHeader) != replayExecutionMode {
		t.Fatalf("execution mode header = %q", message.Header.Get(executionModeHeader))
	}
	if message.Header.Get(replayRootDLQRecordIDHeader) != "root-1" {
		t.Fatalf("replay root header = %q", message.Header.Get(replayRootDLQRecordIDHeader))
	}
	if message.Header.Get(replayAttemptHeader) != "1" {
		t.Fatalf("replay attempt header = %q", message.Header.Get(replayAttemptHeader))
	}
	if message.Header.Get(replaySourceDLQRecordIDHeader) != "dlq-source-1" {
		t.Fatalf(
			"source DLQ header = %q",
			message.Header.Get(replaySourceDLQRecordIDHeader),
		)
	}
	if message.Header.Get(replaySourceStreamSequenceHeader) != "42" {
		t.Fatalf("source stream sequence header = %q", message.Header.Get(replaySourceStreamSequenceHeader))
	}
	if message.Header.Get(requestIDHeader) != source.Header.Get(requestIDHeader) {
		t.Fatalf("request id header was not preserved")
	}
	if message.Header.Get(nats.MsgIdHdr) == source.Header.Get(nats.MsgIdHdr) {
		t.Fatal("replay must not reuse the original Nats-Msg-Id")
	}
	if message.Header.Get(nats.MsgIdHdr) != replayNATSMessageID(decision, 1) {
		t.Fatalf("replay Nats-Msg-Id = %q", message.Header.Get(nats.MsgIdHdr))
	}

	source.Data[0] = 'X'
	if string(message.Data) != string(sourcePayload) {
		t.Fatal("replay payload aliases source storage")
	}
}

func TestBuildSelectiveReplayMessageRejectsSourceMismatch(t *testing.T) {
	payload := []byte("raw persisted")
	decision := replayDecision{
		Eligible:              true,
		MaxAttempts:           2,
		LifecycleKey:          replayLifecycleKey("root-1"),
		ReplayRootDLQRecordID: "root-1",
		SourceDLQRecordID:     "dlq-source-1",
		SourceStreamSequence:  42,
		SourcePayloadSHA256:   sha256LowerHex(payload),
	}
	reservation := replayReservation{
		Reserved:              true,
		Attempt:               1,
		MaxAttempts:           2,
		LifecycleKey:          decision.LifecycleKey,
		ReplayRootDLQRecordID: decision.ReplayRootDLQRecordID,
	}

	tests := []struct {
		name   string
		source *jetstream.RawStreamMsg
	}{
		{
			name: "sequence",
			source: &jetstream.RawStreamMsg{
				Subject:  rawPersistedSubject,
				Sequence: 41,
				Data:     payload,
			},
		},
		{
			name: "subject",
			source: &jetstream.RawStreamMsg{
				Subject:  "cerbero.v1.raw.received",
				Sequence: 42,
				Data:     payload,
			},
		},
		{
			name: "payload hash",
			source: &jetstream.RawStreamMsg{
				Subject:  rawPersistedSubject,
				Sequence: 42,
				Data:     []byte("different payload"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := buildSelectiveReplayMessage(
				test.source,
				decision,
				reservation,
			); err == nil {
				t.Fatal("expected replay source mismatch to fail")
			}
		})
	}
}

func TestBuildSelectiveReplayMessageRejectsReservationMismatch(t *testing.T) {
	payload := []byte("raw persisted")
	source := &jetstream.RawStreamMsg{
		Subject:  rawPersistedSubject,
		Sequence: 42,
		Data:     payload,
	}
	decision := replayDecision{
		Eligible:              true,
		MaxAttempts:           2,
		LifecycleKey:          replayLifecycleKey("root-1"),
		ReplayRootDLQRecordID: "root-1",
		SourceDLQRecordID:     "dlq-source-1",
		SourceStreamSequence:  42,
		SourcePayloadSHA256:   sha256LowerHex(payload),
	}

	tests := []replayReservation{
		{},
		{
			Reserved:              true,
			Attempt:               1,
			MaxAttempts:           2,
			LifecycleKey:          replayLifecycleKey("other-root"),
			ReplayRootDLQRecordID: "other-root",
		},
		{
			Reserved:              true,
			Attempt:               3,
			MaxAttempts:           2,
			LifecycleKey:          decision.LifecycleKey,
			ReplayRootDLQRecordID: decision.ReplayRootDLQRecordID,
		},
	}

	for _, reservation := range tests {
		if _, err := buildSelectiveReplayMessage(
			source,
			decision,
			reservation,
		); err == nil {
			t.Fatalf("expected invalid reservation to fail: %+v", reservation)
		}
	}
}
