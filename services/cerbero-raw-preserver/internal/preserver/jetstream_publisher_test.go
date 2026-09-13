package preserver

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNewJetStreamPublisherRequiresPublisher(t *testing.T) {
	if _, err := NewJetStreamPublisher(nil); err == nil {
		t.Fatal("NewJetStreamPublisher(nil) succeeded")
	}
}

func TestJetStreamPublisherPublishesStableTransportIdentity(t *testing.T) {
	fake := &fakeJetStreamMessagePublisher{
		ack: &jetstream.PubAck{Stream: RawStreamName, Sequence: 41},
	}
	publisher, err := NewJetStreamPublisher(fake)
	if err != nil {
		t.Fatalf("NewJetStreamPublisher: %v", err)
	}

	publication := validPublication()
	originalPayload := append([]byte(nil), publication.Payload...)
	if err := publisher.Publish(context.Background(), publication); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("PublishMsg calls = %d, want 1", fake.calls)
	}
	if fake.message.Subject != RawPersistedSubject {
		t.Fatalf("subject = %q", fake.message.Subject)
	}
	if got := fake.message.Header.Get(nats.MsgIdHdr); got != persistedMessageID {
		t.Fatalf("%s = %q, want %q", nats.MsgIdHdr, got, persistedMessageID)
	}
	if got := fake.message.Header.Get(RequestIDHeader); got != requestID {
		t.Fatalf("%s = %q, want %q", RequestIDHeader, got, requestID)
	}
	if string(fake.message.Data) != string(originalPayload) {
		t.Fatalf("payload = %q, want %q", fake.message.Data, originalPayload)
	}

	publication.Payload[0] ^= 0xff
	if string(fake.message.Data) != string(originalPayload) {
		t.Fatal("published message payload aliases caller-owned bytes")
	}
}

func TestJetStreamPublisherAcceptsDuplicateAcknowledgement(t *testing.T) {
	fake := &fakeJetStreamMessagePublisher{
		ack: &jetstream.PubAck{Stream: RawStreamName, Sequence: 7, Duplicate: true},
	}
	publisher, err := NewJetStreamPublisher(fake)
	if err != nil {
		t.Fatalf("NewJetStreamPublisher: %v", err)
	}
	if err := publisher.Publish(context.Background(), validPublication()); err != nil {
		t.Fatalf("duplicate acknowledgement must be success: %v", err)
	}
}

func TestJetStreamPublisherRejectsInvalidPublicationBeforePublish(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Publication)
	}{
		{name: "subject", mutate: func(p *Publication) { p.Subject = "cerbero.v1.raw.received" }},
		{name: "message id", mutate: func(p *Publication) { p.MessageID = "not-a-uuid" }},
		{name: "request id", mutate: func(p *Publication) { p.RequestID = "not-a-uuid" }},
		{name: "payload", mutate: func(p *Publication) { p.Payload = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeJetStreamMessagePublisher{ack: &jetstream.PubAck{Stream: RawStreamName, Sequence: 1}}
			publisher, err := NewJetStreamPublisher(fake)
			if err != nil {
				t.Fatalf("NewJetStreamPublisher: %v", err)
			}
			publication := validPublication()
			test.mutate(&publication)
			if err := publisher.Publish(context.Background(), publication); err == nil {
				t.Fatal("Publish succeeded")
			}
			if fake.calls != 0 {
				t.Fatalf("PublishMsg calls = %d, want 0", fake.calls)
			}
		})
	}
}

func TestJetStreamPublisherRequiresValidAcknowledgement(t *testing.T) {
	tests := []struct {
		name string
		ack  *jetstream.PubAck
	}{
		{name: "nil", ack: nil},
		{name: "wrong stream", ack: &jetstream.PubAck{Stream: "CERBERO_SYSTEM", Sequence: 1}},
		{name: "zero sequence", ack: &jetstream.PubAck{Stream: RawStreamName, Sequence: 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeJetStreamMessagePublisher{ack: test.ack}
			publisher, err := NewJetStreamPublisher(fake)
			if err != nil {
				t.Fatalf("NewJetStreamPublisher: %v", err)
			}
			if err := publisher.Publish(context.Background(), validPublication()); err == nil {
				t.Fatal("Publish succeeded")
			}
		})
	}
}

func TestJetStreamPublisherPropagatesPublishFailure(t *testing.T) {
	want := errors.New("nats unavailable")
	fake := &fakeJetStreamMessagePublisher{err: want}
	publisher, err := NewJetStreamPublisher(fake)
	if err != nil {
		t.Fatalf("NewJetStreamPublisher: %v", err)
	}
	if err := publisher.Publish(context.Background(), validPublication()); !errors.Is(err, want) {
		t.Fatalf("Publish error = %v, want wrapped %v", err, want)
	}
}

type fakeJetStreamMessagePublisher struct {
	calls   int
	message *nats.Msg
	ack     *jetstream.PubAck
	err     error
}

func (f *fakeJetStreamMessagePublisher) PublishMsg(_ context.Context, message *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.calls++
	f.message = message
	return f.ack, f.err
}
