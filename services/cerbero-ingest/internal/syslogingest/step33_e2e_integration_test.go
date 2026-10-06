//go:build integration

package syslogingest

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"cerbero/services/cerbero-ingest/internal/eventbus"
	"cerbero/services/cerbero-ingest/internal/ingestcore"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type step33Authenticator struct{}

func (step33Authenticator) Authenticate(
	_ context.Context,
	_ ingestcore.AdmissionMetadata,
) (ingestcore.Principal, error) {
	return ingestcore.Principal{ID: "step33-synthetic-source"}, nil
}

type step33Authorizer struct{}

func (step33Authorizer) Authorize(
	_ context.Context,
	_ ingestcore.Principal,
	_ string,
	_ ingestcore.AdmissionMetadata,
) error {
	return nil
}

type step33Limiter struct{}

func (step33Limiter) Allow(
	_ context.Context,
	_ ingestcore.Principal,
	_ ingestcore.AdmissionMetadata,
) bool {
	return true
}

type step33FixtureEvent struct {
	EventTime string `json:"event_time"`
	Raw       string `json:"raw"`
}

func TestStep33SyslogSourceToJetStream(t *testing.T) {
	datasetPath := os.Getenv("STEP33_E2E_DATASET")
	tenantID := os.Getenv("STEP33_E2E_TENANT_ID")
	natsURL := os.Getenv("CERBERO_NATS_URL")
	natsUser := os.Getenv("NATS_INGEST_USER")
	natsPassword := os.Getenv("NATS_INGEST_PASSWORD")
	if datasetPath == "" || tenantID == "" || natsURL == "" || natsUser == "" || natsPassword == "" {
		t.Fatal("STEP33 dataset/tenant/NATS environment is incomplete")
	}

	connection, err := nats.Connect(
		natsURL,
		nats.UserInfo(natsUser, natsPassword),
		nats.Name("step33-e2e-syslog-source"),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("connect ingest NATS: %v", err)
	}
	defer connection.Close()

	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}
	acceptor, err := eventbus.NewJetStreamAcceptor(js)
	if err != nil {
		t.Fatal(err)
	}
	core, err := ingestcore.New(ingestcore.Config{
		MaxPayloadSize:       65_536,
		AllowMissingSensorID: false,
		ComponentVersion:     "step33-e2e",
		PipelineVersion:      "step33-e2e",
		InstanceID:           "step33-e2e-ingest",
		Authenticator:        step33Authenticator{},
		Authorizer:           step33Authorizer{},
		Limiter:              step33Limiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(Config{Preparer: core, Acceptor: acceptor})
	if err != nil {
		t.Fatal(err)
	}
	session, err := adapter.Begin(context.Background(), ingestcore.AdmissionMetadata{
		TenantID:       tenantID,
		SourceID:       "step33-linux-sshd",
		SensorID:       "step33-synthetic-sensor",
		RemoteIdentity: "step33-synthetic-source",
	})
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(datasetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		if scanner.Text() == "" {
			continue
		}
		var fixture step33FixtureEvent
		if err := json.Unmarshal(scanner.Bytes(), &fixture); err != nil {
			t.Fatalf("decode fixture row %d: %v", count+1, err)
		}
		eventTime, err := time.Parse(time.RFC3339, fixture.EventTime)
		if err != nil {
			t.Fatalf("parse event_time row %d: %v", count+1, err)
		}
		sequence := uint64(count + 1)
		result, err := session.Accept(context.Background(), Message{
			ContentType:    "text/plain",
			Encoding:       "utf-8",
			EventTime:      &eventTime,
			SequenceNumber: &sequence,
			RawPayload:     []byte(fixture.Raw),
		})
		if err != nil {
			t.Fatalf("accept fixture row %d: %v", count+1, err)
		}
		if result.RawEvent.GetRawSize() != uint64(len(fixture.Raw)) {
			t.Fatalf("row %d raw size drift", count+1)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 10 {
		t.Fatalf("source fixture count = %d, want 10", count)
	}
	t.Logf(
		"STEP33_SOURCE_INGEST_PASS tenant=%s events=%s",
		tenantID,
		strconv.Itoa(count),
	)
}
