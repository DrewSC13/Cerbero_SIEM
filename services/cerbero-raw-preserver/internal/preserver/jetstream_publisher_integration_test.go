//go:build integration

package preserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamPublisherIntegration(t *testing.T) {
	natsURL := requiredRawPreserverIntegrationEnv(t, "CERBERO_NATS_URL")
	preserverUser := requiredRawPreserverIntegrationEnv(t, "NATS_RAW_PRESERVER_USER")
	preserverPassword := requiredRawPreserverIntegrationEnv(t, "NATS_RAW_PRESERVER_PASSWORD")
	adminUser := requiredRawPreserverIntegrationEnv(t, "NATS_ADMIN_USER")
	adminPassword := requiredRawPreserverIntegrationEnv(t, "NATS_ADMIN_PASSWORD")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	preserverConnection, err := nats.Connect(
		natsURL,
		nats.UserInfo(preserverUser, preserverPassword),
		nats.Name("cerbero-raw-preserver-publisher-integration"),
	)
	if err != nil {
		t.Fatalf("connect raw-preserver identity: %v", err)
	}
	defer preserverConnection.Close()

	preserverJetStream, err := jetstream.New(preserverConnection)
	if err != nil {
		t.Fatalf("create raw-preserver JetStream client: %v", err)
	}
	publisher, err := NewJetStreamPublisher(preserverJetStream)
	if err != nil {
		t.Fatalf("create JetStream publisher: %v", err)
	}

	messageID := integrationUUIDv7(t)
	requestID := integrationUUIDv7(t)
	publication := Publication{
		Subject:   RawPersistedSubject,
		MessageID: messageID,
		RequestID: requestID,
		Payload:   []byte("cerbero raw.persisted integration payload"),
	}
	if err := publisher.Publish(ctx, publication); err != nil {
		t.Fatalf("publish raw.persisted: %v", err)
	}

	adminConnection, err := nats.Connect(
		natsURL,
		nats.UserInfo(adminUser, adminPassword),
		nats.Name("cerbero-raw-preserver-publisher-integration-admin"),
	)
	if err != nil {
		t.Fatalf("connect admin identity: %v", err)
	}
	defer adminConnection.Close()

	adminJetStream, err := jetstream.New(adminConnection)
	if err != nil {
		t.Fatalf("create admin JetStream client: %v", err)
	}
	stream, err := adminJetStream.Stream(ctx, RawStreamName)
	if err != nil {
		t.Fatalf("open %s stream: %v", RawStreamName, err)
	}
	stored, err := stream.GetLastMsgForSubject(ctx, RawPersistedSubject)
	if err != nil {
		t.Fatalf("read stored raw.persisted message: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := stream.DeleteMsg(cleanupCtx, stored.Sequence); err != nil {
			t.Errorf("delete integration message sequence %d: %v", stored.Sequence, err)
		}
	}()

	if got := stored.Header.Get(nats.MsgIdHdr); got != messageID {
		t.Fatalf("stored %s = %q, want %q", nats.MsgIdHdr, got, messageID)
	}
	if got := stored.Header.Get(RequestIDHeader); got != requestID {
		t.Fatalf("stored %s = %q, want %q", RequestIDHeader, got, requestID)
	}
	if string(stored.Data) != string(publication.Payload) {
		t.Fatalf("stored payload = %q, want %q", stored.Data, publication.Payload)
	}

	// A crash after JetStream admission but before PostgreSQL MarkPublished must be
	// recoverable by republishing the same stable outbox message_id. JetStream must
	// accept that retry without the adapter treating duplicate acknowledgement as failure.
	if err := publisher.Publish(ctx, publication); err != nil {
		t.Fatalf("republish stable message_id: %v", err)
	}
}

func requiredRawPreserverIntegrationEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for integration test", name)
	}
	return value
}

func integrationUUIDv7(t *testing.T) string {
	t.Helper()
	var value [16]byte
	millis := uint64(time.Now().UnixMilli())
	value[0] = byte(millis >> 40)
	value[1] = byte(millis >> 32)
	value[2] = byte(millis >> 24)
	value[3] = byte(millis >> 16)
	value[4] = byte(millis >> 8)
	value[5] = byte(millis)
	if _, err := rand.Read(value[6:]); err != nil {
		t.Fatalf("generate UUIDv7 random bytes: %v", err)
	}
	value[6] = (value[6] & 0x0f) | 0x70
	value[8] = (value[8] & 0x3f) | 0x80

	hexValue := hex.EncodeToString(value[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexValue[0:8], hexValue[8:12], hexValue[12:16], hexValue[16:20], hexValue[20:32])
}
