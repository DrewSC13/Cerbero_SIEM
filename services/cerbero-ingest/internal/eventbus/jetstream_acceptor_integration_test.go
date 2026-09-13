//go:build integration

package eventbus

import (
	"context"
	"os"
	"testing"
	"time"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
	contractsv1 "cerbero/services/internal/contracts/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

func TestJetStreamAcceptorIntegration(t *testing.T) {
	natsURL := requiredIntegrationEnv(t, "CERBERO_NATS_URL")
	ingestUser := requiredIntegrationEnv(t, "NATS_INGEST_USER")
	ingestPassword := requiredIntegrationEnv(t, "NATS_INGEST_PASSWORD")
	adminUser := requiredIntegrationEnv(t, "NATS_ADMIN_USER")
	adminPassword := requiredIntegrationEnv(t, "NATS_ADMIN_PASSWORD")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ingestConnection, err := nats.Connect(
		natsURL,
		nats.UserInfo(ingestUser, ingestPassword),
		nats.Name("cerbero-ingest-integration"),
	)
	if err != nil {
		t.Fatalf("connect ingest identity: %v", err)
	}
	defer ingestConnection.Close()

	ingestJetStream, err := jetstream.New(ingestConnection)
	if err != nil {
		t.Fatalf("create ingest JetStream client: %v", err)
	}
	acceptor, err := NewJetStreamAcceptor(ingestJetStream)
	if err != nil {
		t.Fatalf("create JetStream acceptor: %v", err)
	}

	result := validResult(t, testRequestID)
	messageIDs := ingestcore.NewUUIDv7Generator(nil, nil)
	messageID, err := messageIDs.New()
	if err != nil {
		t.Fatalf("generate integration message_id: %v", err)
	}
	result.Envelope.MessageId = messageID
	if err := acceptor.Accept(ctx, result); err != nil {
		t.Fatalf("durable accept: %v", err)
	}

	adminConnection, err := nats.Connect(
		natsURL,
		nats.UserInfo(adminUser, adminPassword),
		nats.Name("cerbero-ingest-integration-admin"),
	)
	if err != nil {
		t.Fatalf("connect admin identity: %v", err)
	}
	defer adminConnection.Close()

	adminJetStream, err := jetstream.New(adminConnection)
	if err != nil {
		t.Fatalf("create admin JetStream client: %v", err)
	}
	stream, err := adminJetStream.Stream(ctx, "CERBERO_RAW")
	if err != nil {
		t.Fatalf("open CERBERO_RAW stream: %v", err)
	}
	stored, err := stream.GetLastMsgForSubject(ctx, RawReceivedSubject)
	if err != nil {
		t.Fatalf("read stored raw.received message: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := stream.DeleteMsg(cleanupCtx, stored.Sequence); err != nil {
			t.Errorf("delete integration message sequence %d: %v", stored.Sequence, err)
		}
	}()

	if stored.Subject != RawReceivedSubject {
		t.Fatalf("stored subject = %q", stored.Subject)
	}
	if got := stored.Header.Get(nats.MsgIdHdr); got != messageID {
		t.Fatalf("stored %s = %q, want %q", nats.MsgIdHdr, got, messageID)
	}
	if got := stored.Header.Get(RequestIDHeader); got != testRequestID {
		t.Fatalf("stored %s = %q, want %q", RequestIDHeader, got, testRequestID)
	}

	var decoded contractsv1.CerberoEnvelope
	if err := proto.Unmarshal(stored.Data, &decoded); err != nil {
		t.Fatalf("decode stored CerberoEnvelope: %v", err)
	}
	if decoded.GetMessageId() != messageID {
		t.Fatalf("stored message_id = %q, want %q", decoded.GetMessageId(), messageID)
	}
}

func requiredIntegrationEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for integration test", name)
	}
	return value
}
