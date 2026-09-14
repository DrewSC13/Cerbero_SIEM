package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeReplayDLQMessage struct {
	data         []byte
	order        *[]string
	ackCount     int
	nakCount     int
	nakDelay     time.Duration
	doubleAckErr error
	nakErr       error
}

func (m *fakeReplayDLQMessage) Data() []byte {
	return m.data
}

func (m *fakeReplayDLQMessage) DoubleAck(context.Context) error {
	m.ackCount++
	if m.order != nil {
		*m.order = append(*m.order, "ack")
	}
	return m.doubleAckErr
}

func (m *fakeReplayDLQMessage) NakWithDelay(delay time.Duration) error {
	m.nakCount++
	m.nakDelay = delay
	if m.order != nil {
		*m.order = append(*m.order, "nak")
	}
	return m.nakErr
}

func TestNormalizationDLQReplayProcessorHappyPathOrdersSideEffects(t *testing.T) {
	payload := []byte("stable raw.persisted bytes")
	record := replayableDLQRecord(payload, 42)
	wire, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}

	order := []string{}
	message := &fakeReplayDLQMessage{data: wire, order: &order}
	source := &jetstream.RawStreamMsg{
		Subject:  rawPersistedSubject,
		Sequence: 42,
		Header:   make(nats.Header),
		Data:     append([]byte(nil), payload...),
	}
	source.Header.Set(requestIDHeader, "request-1")

	processor := testReplayProcessor()
	processor.getSource = func(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
		order = append(order, "get-source")
		return source, nil
	}
	processor.reserveAttempt = func(
		_ context.Context,
		request replayReservationRequest,
	) (replayReservation, error) {
		order = append(order, "reserve")
		if request.SourceStreamSequence != 42 {
			t.Fatalf("reservation source sequence = %d", request.SourceStreamSequence)
		}
		return replayReservation{
			Reserved:              true,
			Attempt:               1,
			MaxAttempts:           request.MaxAttempts,
			LifecycleKey:          request.LifecycleKey,
			ReplayRootDLQRecordID: request.ReplayRootDLQRecordID,
		}, nil
	}
	processor.publish = func(_ context.Context, replay *nats.Msg) (*jetstream.PubAck, error) {
		order = append(order, "publish")
		if replay.Subject != selectiveReplaySubject {
			t.Fatalf("replay subject = %q", replay.Subject)
		}
		if replay.Header.Get(replayAttemptHeader) != "1" {
			t.Fatalf("replay attempt = %q", replay.Header.Get(replayAttemptHeader))
		}
		return &jetstream.PubAck{
			Stream:   rawStreamName,
			Sequence: 77,
		}, nil
	}

	if err := processor.handle(context.Background(), message); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if message.ackCount != 1 || message.nakCount != 0 {
		t.Fatalf("ack/nak = %d/%d, want 1/0", message.ackCount, message.nakCount)
	}
	if want := []string{"get-source", "reserve", "publish", "ack"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("side-effect order = %v, want %v", order, want)
	}
}

func TestNormalizationDLQReplayProcessorRejectsSourceBeforeReservation(t *testing.T) {
	payload := []byte("stable raw.persisted bytes")
	record := replayableDLQRecord(payload, 42)
	wire, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	message := &fakeReplayDLQMessage{data: wire}

	processor := testReplayProcessor()
	processor.getSource = func(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
		return &jetstream.RawStreamMsg{
			Subject:  rawPersistedSubject,
			Sequence: 42,
			Data:     []byte("tampered"),
		}, nil
	}
	processor.reserveAttempt = func(
		context.Context,
		replayReservationRequest,
	) (replayReservation, error) {
		t.Fatal("reservation must not occur for a rejected source")
		return replayReservation{}, nil
	}
	processor.publish = func(context.Context, *nats.Msg) (*jetstream.PubAck, error) {
		t.Fatal("publish must not occur for a rejected source")
		return nil, nil
	}

	if err := processor.handle(context.Background(), message); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if message.ackCount != 1 || message.nakCount != 0 {
		t.Fatalf("ack/nak = %d/%d, want 1/0", message.ackCount, message.nakCount)
	}
}

func TestNormalizationDLQReplayProcessorRetriesTransportWithSameReservedAttempt(t *testing.T) {
	payload := []byte("stable raw.persisted bytes")
	record := replayableDLQRecord(payload, 42)
	wire, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	message := &fakeReplayDLQMessage{data: wire}

	processor := testReplayProcessor()
	processor.getSource = func(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
		return &jetstream.RawStreamMsg{
			Subject:  rawPersistedSubject,
			Sequence: 42,
			Header:   make(nats.Header),
			Data:     payload,
		}, nil
	}
	processor.reserveAttempt = func(
		_ context.Context,
		request replayReservationRequest,
	) (replayReservation, error) {
		return replayReservation{
			Reserved:              true,
			Attempt:               1,
			MaxAttempts:           request.MaxAttempts,
			LifecycleKey:          request.LifecycleKey,
			ReplayRootDLQRecordID: request.ReplayRootDLQRecordID,
		}, nil
	}
	processor.publish = func(context.Context, *nats.Msg) (*jetstream.PubAck, error) {
		return nil, errors.New("NATS unavailable")
	}

	if err := processor.handle(context.Background(), message); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if message.ackCount != 0 || message.nakCount != 1 {
		t.Fatalf("ack/nak = %d/%d, want 0/1", message.ackCount, message.nakCount)
	}
	if message.nakDelay != processor.nakDelay {
		t.Fatalf("NAK delay = %s, want %s", message.nakDelay, processor.nakDelay)
	}
}

func TestNormalizationDLQReplayProcessorAcksPermanentAndExhaustedRecords(t *testing.T) {
	payload := []byte("stable raw.persisted bytes")

	t.Run("permanent", func(t *testing.T) {
		record := replayableDLQRecord(payload, 42)
		record.Retryable = false
		wire, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("marshal record: %v", err)
		}
		message := &fakeReplayDLQMessage{data: wire}
		processor := testReplayProcessor()
		processor.getSource = func(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
			t.Fatal("permanent failure must not load source")
			return nil, nil
		}
		if err := processor.handle(context.Background(), message); err != nil {
			t.Fatalf("handle: %v", err)
		}
		if message.ackCount != 1 || message.nakCount != 0 {
			t.Fatalf("ack/nak = %d/%d, want 1/0", message.ackCount, message.nakCount)
		}
	})

	t.Run("exhausted", func(t *testing.T) {
		record := replayableDLQRecord(payload, 42)
		wire, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("marshal record: %v", err)
		}
		message := &fakeReplayDLQMessage{data: wire}
		processor := testReplayProcessor()
		processor.getSource = func(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
			return &jetstream.RawStreamMsg{
				Subject:  rawPersistedSubject,
				Sequence: 42,
				Data:     payload,
			}, nil
		}
		processor.reserveAttempt = func(
			_ context.Context,
			request replayReservationRequest,
		) (replayReservation, error) {
			return replayReservation{
				Reserved:              false,
				Attempt:               request.MaxAttempts,
				MaxAttempts:           request.MaxAttempts,
				LifecycleKey:          request.LifecycleKey,
				ReplayRootDLQRecordID: request.ReplayRootDLQRecordID,
			}, nil
		}
		processor.publish = func(context.Context, *nats.Msg) (*jetstream.PubAck, error) {
			t.Fatal("exhausted lifecycle must not publish")
			return nil, nil
		}

		if err := processor.handle(context.Background(), message); err != nil {
			t.Fatalf("handle: %v", err)
		}
		if message.ackCount != 1 || message.nakCount != 0 {
			t.Fatalf("ack/nak = %d/%d, want 1/0", message.ackCount, message.nakCount)
		}
	})
}

func TestNormalizationDLQReplayProcessorDefersTransientSourceLookup(t *testing.T) {
	payload := []byte("stable raw.persisted bytes")
	record := replayableDLQRecord(payload, 42)
	wire, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	message := &fakeReplayDLQMessage{data: wire}

	processor := testReplayProcessor()
	processor.getSource = func(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
		return nil, errors.New("JetStream unavailable")
	}
	processor.reserveAttempt = func(
		context.Context,
		replayReservationRequest,
	) (replayReservation, error) {
		t.Fatal("transient source lookup failure must not reserve budget")
		return replayReservation{}, nil
	}

	if err := processor.handle(context.Background(), message); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if message.ackCount != 0 || message.nakCount != 1 {
		t.Fatalf("ack/nak = %d/%d, want 0/1", message.ackCount, message.nakCount)
	}
}

func testReplayProcessor() normalizationDLQReplayProcessor {
	return normalizationDLQReplayProcessor{
		policy: replayPolicy{
			MaxAutomaticAttempts: 2,
		},
		nakDelay: 5 * time.Second,
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		getSource: func(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
			return nil, jetstream.ErrMsgNotFound
		},
		reserveAttempt: func(
			context.Context,
			replayReservationRequest,
		) (replayReservation, error) {
			return replayReservation{}, errors.New("unexpected reservation")
		},
		publish: func(context.Context, *nats.Msg) (*jetstream.PubAck, error) {
			return nil, errors.New("unexpected publish")
		},
	}
}

func replayableDLQRecord(payload []byte, sequence uint64) normalizationDeadLetter {
	return normalizationDeadLetter{
		SchemaVersion:         normalizationDLQSchema,
		DLQRecordID:           "dlq-record-1",
		OriginalSubject:       rawPersistedSubject,
		OriginalPayloadSHA256: sha256LowerHex(payload),
		Retryable:             true,
		StreamSequence:        &sequence,
	}
}
