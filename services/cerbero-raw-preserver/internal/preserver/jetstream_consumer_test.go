package preserver

import (
	"context"
	"errors"
	"testing"
	"time"

	contractsv1 "cerbero/services/internal/contracts/v1"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type processorFake struct {
	result   Result
	err      error
	delivery Delivery
	calls    int
}

func (f *processorFake) Process(_ context.Context, delivery Delivery) (Result, error) {
	f.calls++
	f.delivery = delivery
	return f.result, f.err
}

type rawMessageFake struct {
	subject    string
	data       []byte
	headers    nats.Header
	acked      int
	naked      int
	terminated int
	nakDelay   time.Duration
	ackErr     error
	nakErr     error
	termErr    error
}

func (f *rawMessageFake) Data() []byte         { return f.data }
func (f *rawMessageFake) Headers() nats.Header { return f.headers }
func (f *rawMessageFake) Subject() string      { return f.subject }
func (f *rawMessageFake) DoubleAck(context.Context) error {
	f.acked++
	return f.ackErr
}
func (f *rawMessageFake) NakWithDelay(delay time.Duration) error {
	f.naked++
	f.nakDelay = delay
	return f.nakErr
}
func (f *rawMessageFake) TermWithReason(string) error {
	f.terminated++
	return f.termErr
}

func TestRawConsumerACKRequiresCoreACKDisposition(t *testing.T) {
	processor := &processorFake{result: Result{
		Disposition: DispositionACK,
		MessageID:   incomingMessageID,
		EventID:     eventID,
	}}
	consumer := newTestRawConsumer(t, processor)
	message := validRuntimeMessage(t)
	if err := consumer.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if message.acked != 1 || message.naked != 0 || message.terminated != 0 {
		t.Fatalf("transport actions ack=%d nak=%d term=%d", message.acked, message.naked, message.terminated)
	}
	if processor.delivery.RequestID != requestID {
		t.Fatalf("request_id = %q", processor.delivery.RequestID)
	}
}

func TestRawConsumerRetryUsesDelayedNAK(t *testing.T) {
	processor := &processorFake{
		result: Result{Disposition: DispositionRetry, MessageID: incomingMessageID},
		err:    errors.New("temporary dependency outage"),
	}
	consumer := newTestRawConsumer(t, processor)
	message := validRuntimeMessage(t)
	if err := consumer.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if message.naked != 1 || message.nakDelay != 3*time.Second {
		t.Fatalf("NAK calls=%d delay=%s", message.naked, message.nakDelay)
	}
	if message.acked != 0 || message.terminated != 0 {
		t.Fatalf("unexpected ack/term: %d/%d", message.acked, message.terminated)
	}
}

func TestRawConsumerIsolateTerminatesWithoutRetry(t *testing.T) {
	processor := &processorFake{
		result: Result{Disposition: DispositionIsolate, MessageID: incomingMessageID},
		err:    errors.New("invalid contract"),
	}
	consumer := newTestRawConsumer(t, processor)
	message := validRuntimeMessage(t)
	if err := consumer.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if message.terminated != 1 || message.acked != 0 || message.naked != 0 {
		t.Fatalf("transport actions ack=%d nak=%d term=%d", message.acked, message.naked, message.terminated)
	}
}

func TestRawConsumerMalformedProtobufIsIsolatedBeforeCore(t *testing.T) {
	processor := &processorFake{}
	consumer := newTestRawConsumer(t, processor)
	message := &rawMessageFake{
		subject: RawReceivedSubject,
		data:    []byte{0xff, 0xff, 0xff},
		headers: make(nats.Header),
	}
	if err := consumer.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if processor.calls != 0 {
		t.Fatalf("processor calls = %d", processor.calls)
	}
	if message.terminated != 1 {
		t.Fatalf("Term calls = %d", message.terminated)
	}
}

func TestRawConsumerContradictoryACKWithErrorRetries(t *testing.T) {
	processor := &processorFake{
		result: Result{Disposition: DispositionACK, MessageID: incomingMessageID},
		err:    errors.New("unexpected core error"),
	}
	consumer := newTestRawConsumer(t, processor)
	message := validRuntimeMessage(t)
	if err := consumer.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if message.naked != 1 || message.acked != 0 {
		t.Fatalf("transport actions ack=%d nak=%d", message.acked, message.naked)
	}
}

func newTestRawConsumer(t *testing.T, processor rawDeliveryProcessor) *RawConsumer {
	t.Helper()
	consumer, err := newRawConsumer(
		processor,
		nil,
		time.Second,
		5*time.Second,
		func(time.Duration, time.Duration) time.Duration { return 3 * time.Second },
	)
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func validRuntimeMessage(t *testing.T) *rawMessageFake {
	t.Helper()
	wire, err := proto.Marshal(&contractsv1.CerberoEnvelope{MessageId: incomingMessageID})
	if err != nil {
		t.Fatal(err)
	}
	headers := make(nats.Header)
	headers.Set(RequestIDHeader, requestID)
	return &rawMessageFake{
		subject: RawReceivedSubject,
		data:    wire,
		headers: headers,
	}
}
