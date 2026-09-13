package eventbus

import (
	"context"
	"errors"
	"testing"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
	contractsv1 "cerbero/services/internal/contracts/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testRequestID = "018f47d3-2c6a-7b10-8f00-000000000001"
	testEventID   = "018f47d3-2c6a-7b10-8f00-000000000002"
	testMessageID = "018f47d3-2c6a-7b10-8f00-000000000003"
	testTraceID   = "018f47d3-2c6a-7b10-8f00-000000000004"
)

type publisherFake struct {
	calls   int
	message *nats.Msg
	ack     *jetstream.PubAck
	err     error
}

func (f *publisherFake) PublishMsg(
	_ context.Context,
	message *nats.Msg,
	_ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	f.calls++
	f.message = cloneMessage(message)
	if f.err != nil {
		return nil, f.err
	}
	return f.ack, nil
}

func cloneMessage(message *nats.Msg) *nats.Msg {
	if message == nil {
		return nil
	}
	cloned := &nats.Msg{
		Subject: message.Subject,
		Header:  make(nats.Header),
		Data:    append([]byte(nil), message.Data...),
	}
	for key, values := range message.Header {
		cloned.Header[key] = append([]string(nil), values...)
	}
	return cloned
}

func validResult(t *testing.T, requestID string) *ingestcore.Result {
	t.Helper()

	rawEvent := &contractsv1.RawEvent{
		EventId:          testEventID,
		TenantId:         "tenant-a",
		SourceId:         "source-a",
		SensorId:         "sensor-a",
		IngestTime:       timestamppb.Now(),
		ContentType:      "application/json",
		Encoding:         "utf-8",
		RawPayload:       []byte(`{"ok":true}`),
		RawSize:          uint64(len(`{"ok":true}`)),
		RawHashAlgorithm: "sha256",
		RawHash:          "4062edaf750fb8074e7e83e0c9028c94e32468a8b6f1614774328ef045150f93",
		Transport:        "json-http",
		RemoteIdentity:   "principal-a",
		IntegrityStatus:  contractsv1.IntegrityStatus_INTEGRITY_UNVERIFIED,
		PipelineVersion:  "test",
	}
	payload, err := anypb.New(rawEvent)
	if err != nil {
		t.Fatalf("pack RawEvent: %v", err)
	}

	envelope := &contractsv1.CerberoEnvelope{
		ContractVersion: "1",
		MessageId:       testMessageID,
		MessageType:     "RawEventReceived",
		TenantId:        "tenant-a",
		Producer: &contractsv1.Producer{
			Component:        "cerbero-ingest",
			ComponentVersion: "test",
			InstanceId:       "ingest-test-1",
		},
		EmittedAt:     timestamppb.Now(),
		TraceId:       testTraceID,
		PayloadSchema: "cerbero.raw_event.v1",
		Payload:       payload,
	}
	return &ingestcore.Result{
		RequestID: requestID,
		RawEvent:  rawEvent,
		Envelope:  envelope,
	}
}

func TestJetStreamAcceptorPublishesEnvelopeWithTransportMetadata(t *testing.T) {
	publisher := &publisherFake{
		ack: &jetstream.PubAck{Stream: "CERBERO_RAW", Sequence: 7},
	}
	acceptor, err := NewJetStreamAcceptor(publisher)
	if err != nil {
		t.Fatalf("NewJetStreamAcceptor() error = %v", err)
	}
	result := validResult(t, testRequestID)

	if err := acceptor.Accept(context.Background(), result); err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if publisher.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.calls)
	}
	if publisher.message.Subject != RawReceivedSubject {
		t.Fatalf("subject = %q", publisher.message.Subject)
	}
	if got := publisher.message.Header.Get(nats.MsgIdHdr); got != testMessageID {
		t.Fatalf("%s = %q, want %q", nats.MsgIdHdr, got, testMessageID)
	}
	if got := publisher.message.Header.Get(RequestIDHeader); got != testRequestID {
		t.Fatalf("%s = %q, want %q", RequestIDHeader, got, testRequestID)
	}

	var decoded contractsv1.CerberoEnvelope
	if err := proto.Unmarshal(publisher.message.Data, &decoded); err != nil {
		t.Fatalf("unmarshal published envelope: %v", err)
	}
	if decoded.GetMessageId() != result.Envelope.GetMessageId() {
		t.Fatalf("published message_id = %q, want %q", decoded.GetMessageId(), result.Envelope.GetMessageId())
	}
	if decoded.GetPayloadSchema() != result.Envelope.GetPayloadSchema() {
		t.Fatalf("published payload_schema = %q, want %q", decoded.GetPayloadSchema(), result.Envelope.GetPayloadSchema())
	}
}

func TestJetStreamAcceptorOmitsRequestHeaderWhenRequestIDIsAbsent(t *testing.T) {
	publisher := &publisherFake{
		ack: &jetstream.PubAck{Stream: "CERBERO_RAW", Sequence: 8},
	}
	acceptor, err := NewJetStreamAcceptor(publisher)
	if err != nil {
		t.Fatal(err)
	}

	if err := acceptor.Accept(context.Background(), validResult(t, "")); err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if got := publisher.message.Header.Get(RequestIDHeader); got != "" {
		t.Fatalf("unexpected request header %q", got)
	}
}

func TestJetStreamAcceptorRejectsInvalidRequestIDBeforePublish(t *testing.T) {
	publisher := &publisherFake{
		ack: &jetstream.PubAck{Stream: "CERBERO_RAW", Sequence: 9},
	}
	acceptor, err := NewJetStreamAcceptor(publisher)
	if err != nil {
		t.Fatal(err)
	}

	err = acceptor.Accept(context.Background(), validResult(t, "not-a-uuid"))
	if err == nil {
		t.Fatal("Accept() error = nil, want request_id validation error")
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.calls)
	}
}

func TestJetStreamAcceptorPropagatesPublishFailure(t *testing.T) {
	publisher := &publisherFake{err: errors.New("nats unavailable")}
	acceptor, err := NewJetStreamAcceptor(publisher)
	if err != nil {
		t.Fatal(err)
	}

	err = acceptor.Accept(context.Background(), validResult(t, testRequestID))
	if err == nil {
		t.Fatal("Accept() error = nil, want publish error")
	}
}

func TestJetStreamAcceptorRequiresPubAck(t *testing.T) {
	publisher := &publisherFake{}
	acceptor, err := NewJetStreamAcceptor(publisher)
	if err != nil {
		t.Fatal(err)
	}

	err = acceptor.Accept(context.Background(), validResult(t, testRequestID))
	if err == nil {
		t.Fatal("Accept() error = nil, want missing-ack error")
	}
}

func TestJetStreamAcceptorTreatsDuplicateAckAsDurableSuccess(t *testing.T) {
	publisher := &publisherFake{
		ack: &jetstream.PubAck{
			Stream:    "CERBERO_RAW",
			Sequence:  10,
			Duplicate: true,
		},
	}
	acceptor, err := NewJetStreamAcceptor(publisher)
	if err != nil {
		t.Fatal(err)
	}

	if err := acceptor.Accept(context.Background(), validResult(t, testRequestID)); err != nil {
		t.Fatalf("Accept() duplicate acknowledgement error = %v", err)
	}
}

func TestJetStreamAcceptorRejectsMissingEnvelope(t *testing.T) {
	publisher := &publisherFake{
		ack: &jetstream.PubAck{Stream: "CERBERO_RAW", Sequence: 11},
	}
	acceptor, err := NewJetStreamAcceptor(publisher)
	if err != nil {
		t.Fatal(err)
	}

	err = acceptor.Accept(context.Background(), &ingestcore.Result{RequestID: testRequestID})
	if err == nil {
		t.Fatal("Accept() error = nil, want missing-envelope error")
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.calls)
	}
}
