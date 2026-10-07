//go:build integration

package journald_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cerbero/services/cerbero-ingest/internal/eventbus"
	"cerbero/services/cerbero-ingest/internal/journald"
	"cerbero/services/cerbero-ingest/internal/sourceapp"
	contractsv1 "cerbero/services/internal/contracts/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestStep34JournaldExportToJetStream(t *testing.T) {
	runtime, err := sourceapp.Open(sourceapp.Config{
		SecurityProfile:     "DEVELOPMENT",
		InsecureDevelopment: true,
		Transport:           journald.Transport,
		TenantID:            "018f47a2-4b00-7a00-8000-00000000d034",
		SourceID:            "step34-journald",
		SensorID:            "step34-journal-sensor",
		RemoteIdentity:      "step34-host",
		MaxPayloadSize:      4096,
		MaxEventsPerSecond:  100,
		ComponentVersion:    "0.1.0-step34-test",
		PipelineVersion:     "ingest-v1-step34-test",
		InstanceID:          "step34-journald-test",
		NATSURL:             requiredEnv(t, "CERBERO_NATS_URL"),
		NATSUser:            requiredEnv(t, "NATS_INGEST_USER"),
		NATSPassword:        requiredEnv(t, "NATS_INGEST_PASSWORD"),
		ConnectTimeout:      3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	collector, err := journald.NewExportCollector(
		strings.NewReader("__CURSOR=s=step34;i=1\nMESSAGE=authentication failed\n_SYSTEMD_UNIT=sshd.service\n\n"),
		runtime.Metadata,
		4096,
	)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := journald.NewAdapter(runtime.Core, runtime.Acceptor)
	if err != nil {
		t.Fatal(err)
	}
	session, err := adapter.Begin(context.Background(), runtime.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := collector.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Accept(context.Background(), entry); err != nil {
		t.Fatal(err)
	}

	stored := lastRawMessage(t)
	defer deleteRawMessage(t, stored.Sequence)

	var envelope contractsv1.CerberoEnvelope
	if err := proto.Unmarshal(stored.Data, &envelope); err != nil {
		t.Fatal(err)
	}
	var event contractsv1.RawEvent
	if err := anypb.UnmarshalTo(envelope.GetPayload(), &event, proto.UnmarshalOptions{}); err != nil {
		t.Fatal(err)
	}
	if event.GetTransport() != journald.Transport {
		t.Fatalf("transport = %q", event.GetTransport())
	}
	if !bytes.Equal(event.GetRawPayload(), entry.RawRepresentation) {
		t.Fatal("journald canonical bytes changed before durable admission")
	}
	var canonical map[string]any
	if err := json.Unmarshal(event.GetRawPayload(), &canonical); err != nil {
		t.Fatal(err)
	}
	if canonical["cerbero_journald_version"] != float64(1) {
		t.Fatalf("unexpected canonical version: %v", canonical)
	}
	fmt.Println("STEP34_JOURNALD_INGEST_PASS")
}

func lastRawMessage(t *testing.T) *jetstream.RawStreamMsg {
	t.Helper()
	connection, err := nats.Connect(
		requiredEnv(t, "CERBERO_NATS_URL"),
		nats.UserInfo(requiredEnv(t, "NATS_ADMIN_USER"), requiredEnv(t, "NATS_ADMIN_PASSWORD")),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	stream, err := js.Stream(ctx, "CERBERO_RAW")
	if err != nil {
		t.Fatal(err)
	}
	message, err := stream.GetLastMsgForSubject(ctx, eventbus.RawReceivedSubject)
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func deleteRawMessage(t *testing.T, sequence uint64) {
	t.Helper()
	connection, err := nats.Connect(
		requiredEnv(t, "CERBERO_NATS_URL"),
		nats.UserInfo(requiredEnv(t, "NATS_ADMIN_USER"), requiredEnv(t, "NATS_ADMIN_PASSWORD")),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "CERBERO_RAW")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.DeleteMsg(ctx, sequence); err != nil {
		t.Fatal(err)
	}
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}
