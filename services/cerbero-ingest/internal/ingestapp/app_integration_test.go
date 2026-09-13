//go:build integration

package ingestapp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"cerbero/services/cerbero-ingest/internal/eventbus"
	"cerbero/services/cerbero-ingest/internal/ingestmetrics"
	contractsv1 "cerbero/services/internal/contracts/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestDevelopmentRuntimeHTTPToJetStream(t *testing.T) {
	natsURL := requiredEnv(t, "CERBERO_NATS_URL")
	ingestUser := requiredEnv(t, "NATS_INGEST_USER")
	ingestPassword := requiredEnv(t, "NATS_INGEST_PASSWORD")
	adminUser := requiredEnv(t, "NATS_ADMIN_USER")
	adminPassword := requiredEnv(t, "NATS_ADMIN_PASSWORD")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	config := Config{
		SecurityProfile:           ProfileDevelopment,
		InsecureDevelopment:       true,
		ListenAddress:             listener.Addr().String(),
		IngestPath:                "/ingest/v1/events",
		MaxPayloadSize:            1024,
		MaxConnectionRate:         100,
		MaxEventsPerSecond:        100,
		ReadTimeout:               3 * time.Second,
		IdleTimeout:               5 * time.Second,
		ConcurrentConnections:     20,
		ShutdownTimeout:           3 * time.Second,
		ComponentVersion:          "0.1.0-test",
		PipelineVersion:           "ingest-v1-test",
		InstanceID:                "ingest-runtime-test",
		NATSURL:                   natsURL,
		NATSUser:                  ingestUser,
		NATSPassword:              ingestPassword,
		DevelopmentTenantID:       "tenant-runtime-test",
		DevelopmentSourceID:       "source-runtime-test",
		DevelopmentSensorID:       "sensor-runtime-test",
		DevelopmentRemoteIdentity: "development-runtime-test",
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runErr := make(chan error, 1)
	go func() {
		runErr <- runWithListener(ctx, config, slog.New(slog.NewTextHandler(io.Discard, nil)), listener)
	}()

	baseURL := "http://" + listener.Addr().String()
	waitForReady(t, baseURL+"/readyz")

	raw := []byte("{\n  \"message\": \"runtime exact bytes\"\n}\n")
	request, err := http.NewRequest(http.MethodPost, baseURL+config.IngestPath, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d, body = %s", response.StatusCode, body)
	}
	var accepted map[string]string
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil {
		t.Fatalf("decode acceptance: %v", err)
	}

	adminConnection, err := nats.Connect(natsURL, nats.UserInfo(adminUser, adminPassword))
	if err != nil {
		t.Fatalf("connect admin NATS: %v", err)
	}
	defer adminConnection.Close()
	adminJS, err := jetstream.New(adminConnection)
	if err != nil {
		t.Fatal(err)
	}
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer checkCancel()
	stream, err := adminJS.Stream(checkCtx, rawStreamName)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := stream.GetLastMsgForSubject(checkCtx, eventbus.RawReceivedSubject)
	if err != nil {
		t.Fatalf("read stored runtime event: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := stream.DeleteMsg(cleanupCtx, stored.Sequence); err != nil {
			t.Errorf("delete runtime integration message %d: %v", stored.Sequence, err)
		}
	}()

	if stored.Header.Get(nats.MsgIdHdr) != accepted["message_id"] {
		t.Fatalf("stored Nats-Msg-Id = %q, accepted message_id = %q", stored.Header.Get(nats.MsgIdHdr), accepted["message_id"])
	}
	if stored.Header.Get(eventbus.RequestIDHeader) != accepted["request_id"] {
		t.Fatalf("stored request correlation differs from HTTP response")
	}

	var envelope contractsv1.CerberoEnvelope
	if err := proto.Unmarshal(stored.Data, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	var rawEvent contractsv1.RawEvent
	if err := anypb.UnmarshalTo(envelope.GetPayload(), &rawEvent, proto.UnmarshalOptions{}); err != nil {
		t.Fatalf("unpack RawEvent: %v", err)
	}
	if !bytes.Equal(rawEvent.GetRawPayload(), raw) {
		t.Fatalf("raw bytes changed: got %q want %q", rawEvent.GetRawPayload(), raw)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("runtime shutdown error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not shut down after cancellation")
	}
}

func waitForReady(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(url)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("runtime never became ready: %s", url)
}

func TestDevelopmentRuntimeNATSOutageRejectsAcceptance(t *testing.T) {
	natsURL := requiredEnv(t, "CERBERO_NATS_URL")
	ingestUser := requiredEnv(t, "NATS_INGEST_USER")
	ingestPassword := requiredEnv(t, "NATS_INGEST_PASSWORD")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	config := Config{
		SecurityProfile:           ProfileDevelopment,
		InsecureDevelopment:       true,
		ListenAddress:             listener.Addr().String(),
		IngestPath:                "/ingest/v1/events",
		MaxPayloadSize:            1024,
		MaxConnectionRate:         100,
		MaxEventsPerSecond:        100,
		ReadTimeout:               3 * time.Second,
		IdleTimeout:               5 * time.Second,
		ConcurrentConnections:     20,
		ShutdownTimeout:           3 * time.Second,
		ComponentVersion:          "0.1.0-test",
		PipelineVersion:           "ingest-v1-test",
		InstanceID:                "ingest-outage-test",
		NATSURL:                   natsURL,
		NATSUser:                  ingestUser,
		NATSPassword:              ingestPassword,
		DevelopmentTenantID:       "tenant-outage-test",
		DevelopmentSourceID:       "source-outage-test",
		DevelopmentSensorID:       "sensor-outage-test",
		DevelopmentRemoteIdentity: "development-outage-test",
	}

	connection, err := nats.Connect(
		natsURL,
		nats.UserInfo(ingestUser, ingestPassword),
		nats.Timeout(config.ReadTimeout),
	)
	if err != nil {
		t.Fatalf("connect ingest NATS: %v", err)
	}
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatal(err)
	}

	metrics := ingestmetrics.NewMemoryRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runErr := make(chan error, 1)
	go func() {
		runErr <- runWithDependencies(
			ctx,
			config,
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			listener,
			connection,
			js,
			metrics,
		)
	}()

	baseURL := "http://" + listener.Addr().String()
	waitForReady(t, baseURL+"/readyz")

	transport := &http.Transport{DisableKeepAlives: true}
	client := &http.Client{Transport: transport}
	t.Cleanup(transport.CloseIdleConnections)

	connection.Close()
	waitForStatusWithClient(t, client, baseURL+"/readyz", http.StatusServiceUnavailable)

	request, err := http.NewRequest(
		http.MethodPost,
		baseURL+config.IngestPath,
		bytes.NewReader([]byte(`{"message":"nats outage"}`)),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("POST during NATS outage: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status during NATS outage = %d, want %d: %s",
			response.StatusCode,
			http.StatusServiceUnavailable,
			body,
		)
	}
	var payload struct {
		Error *contractsv1.CerberoError `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode outage response: %v", err)
	}
	if payload.Error == nil ||
		payload.Error.GetCode() != "CER-BUS-PUBLISH-FAILED" ||
		!payload.Error.GetRetryable() {
		t.Fatalf("unexpected outage error contract: %+v", payload.Error)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatalf("drain outage response: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close outage response: %v", err)
	}

	snapshot := metrics.Snapshot()
	if snapshot.EventsReceivedTotal != 1 ||
		snapshot.EventsAcceptedTotal != 0 ||
		snapshot.EventsRejectedTotal != 1 ||
		snapshot.PublishFailuresTotal != 1 {
		t.Fatalf("unexpected outage metrics: %+v", snapshot)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("runtime shutdown error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not shut down after outage test cancellation")
	}
}

func waitForStatusWithClient(t *testing.T, client *http.Client, url string, status int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == status {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("endpoint never reached status %d: %s", status, url)
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}
