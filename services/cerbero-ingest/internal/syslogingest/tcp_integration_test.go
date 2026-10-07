//go:build integration

package syslogingest_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"cerbero/services/cerbero-ingest/internal/eventbus"
	"cerbero/services/cerbero-ingest/internal/sourceapp"
	"cerbero/services/cerbero-ingest/internal/syslogingest"
	contractsv1 "cerbero/services/internal/contracts/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestStep34SyslogTCPToJetStream(t *testing.T) {
	runtime, err := sourceapp.Open(sourceapp.Config{
		SecurityProfile:     "DEVELOPMENT",
		InsecureDevelopment: true,
		Transport:           syslogingest.Transport,
		TenantID:            "018f47a2-4b00-7a00-8000-00000000d034",
		SourceID:            "step34-syslog",
		SensorID:            "step34-sensor",
		RemoteIdentity:      "step34-tcp-peer",
		MaxPayloadSize:      4096,
		MaxEventsPerSecond:  100,
		ComponentVersion:    "0.1.0-step34-test",
		PipelineVersion:     "ingest-v1-step34-test",
		InstanceID:          "step34-syslog-test",
		NATSURL:             requiredEnv(t, "CERBERO_NATS_URL"),
		NATSUser:            requiredEnv(t, "NATS_INGEST_USER"),
		NATSPassword:        requiredEnv(t, "NATS_INGEST_PASSWORD"),
		ConnectTimeout:      3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- syslogingest.ServeTCP(ctx, listener, syslogingest.TCPConfig{
			Preparer:              runtime.Core,
			Acceptor:              runtime.Acceptor,
			Metadata:              runtime.Metadata,
			MaxFrameSize:          4096,
			ReadTimeout:           3 * time.Second,
			MaxConnectionRate:     100,
			ConcurrentConnections: 4,
		})
	}()

	raw := []byte("Failed password for invalid user step34 from 10.34.0.8 port 50341 ssh2")
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(connection, "%d ", len(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write(raw); err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()

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
	if !bytes.Equal(event.GetRawPayload(), raw) {
		t.Fatalf("raw payload changed: got %q want %q", event.GetRawPayload(), raw)
	}
	if event.GetTransport() != syslogingest.Transport {
		t.Fatalf("transport = %q", event.GetTransport())
	}
	if event.GetSourceId() != "step34-syslog" {
		t.Fatalf("source_id = %q", event.GetSourceId())
	}

	cancel()
	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("syslog server did not stop")
	}
	fmt.Println("STEP34_SYSLOG_TCP_INGEST_PASS")
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
