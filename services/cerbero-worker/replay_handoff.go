package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	selectiveReplaySubject           = "cerbero.v1.raw.replay"
	requestIDHeader                  = "Cerbero-Request-Id"
	executionModeHeader              = "Cerbero-Execution-Mode"
	replayRootDLQRecordIDHeader      = "Cerbero-Replay-Root-DLQ-Record-Id"
	replayAttemptHeader              = "Cerbero-Replay-Attempt"
	replaySourceDLQRecordIDHeader    = "Cerbero-Replay-Source-DLQ-Record-Id"
	replaySourceStreamSequenceHeader = "Cerbero-Replay-Source-Stream-Sequence"
)

func buildSelectiveReplayMessage(
	source *jetstream.RawStreamMsg,
	decision replayDecision,
	reservation replayReservation,
) (*nats.Msg, error) {
	if !decision.Eligible {
		return nil, errors.New("replay decision is not eligible")
	}
	if !reservation.Reserved {
		return nil, errors.New("replay attempt was not reserved")
	}
	if reservation.LifecycleKey != decision.LifecycleKey ||
		reservation.ReplayRootDLQRecordID != decision.ReplayRootDLQRecordID ||
		reservation.MaxAttempts != decision.MaxAttempts {
		return nil, errors.New("replay reservation does not match replay decision")
	}
	if reservation.Attempt == 0 || reservation.Attempt > reservation.MaxAttempts {
		return nil, errors.New("reserved replay attempt is outside captured budget")
	}
	if source == nil {
		return nil, errors.New("raw replay source is required")
	}
	if source.Sequence != decision.SourceStreamSequence {
		return nil, fmt.Errorf(
			"raw replay source sequence = %d, want %d",
			source.Sequence,
			decision.SourceStreamSequence,
		)
	}
	if source.Subject != rawPersistedSubject {
		return nil, fmt.Errorf(
			"raw replay source subject = %q, want %q",
			source.Subject,
			rawPersistedSubject,
		)
	}
	if sha256LowerHex(source.Data) != decision.SourcePayloadSHA256 {
		return nil, errors.New("raw replay source payload SHA-256 does not match DLQ record")
	}

	headers := make(nats.Header)
	if requestID := source.Header.Get(requestIDHeader); requestID != "" {
		headers.Set(requestIDHeader, requestID)
	}
	headers.Set(nats.MsgIdHdr, replayNATSMessageID(decision, reservation.Attempt))
	headers.Set(executionModeHeader, replayExecutionMode)
	headers.Set(replayRootDLQRecordIDHeader, decision.ReplayRootDLQRecordID)
	headers.Set(replayAttemptHeader, strconv.FormatUint(uint64(reservation.Attempt), 10))
	headers.Set(replaySourceDLQRecordIDHeader, decision.SourceDLQRecordID)
	headers.Set(replaySourceStreamSequenceHeader, strconv.FormatUint(decision.SourceStreamSequence, 10))

	return &nats.Msg{
		Subject: selectiveReplaySubject,
		Header:  headers,
		Data:    append([]byte(nil), source.Data...),
	}, nil
}

func replayNATSMessageID(decision replayDecision, attempt uint32) string {
	return fmt.Sprintf(
		"normalization-replay:v1:%s:%d",
		decision.ReplayRootDLQRecordID,
		attempt,
	)
}

func sha256LowerHex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
