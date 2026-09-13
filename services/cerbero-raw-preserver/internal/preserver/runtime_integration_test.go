//go:build integration

package preserver

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	contractsv1 "cerbero/services/internal/contracts/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestDevelopmentRawPreserverRuntime(t *testing.T) {
	config, err := LoadRuntimeConfig(os.Getenv)
	if err != nil {
		t.Fatalf("load runtime config: %v", err)
	}
	config.RawStorePath = t.TempDir()
	config.InstanceID = "raw-preserver-integration-1"
	config.RetryMinDelay = 100 * time.Millisecond
	config.RetryMaxDelay = 250 * time.Millisecond

	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	runtimeDone := make(chan error, 1)
	runtimeStopped := false
	go func() {
		runtimeDone <- Run(
			runtimeCtx,
			config,
			slog.New(slog.NewTextHandler(os.Stderr, nil)),
		)
	}()
	t.Cleanup(func() {
		if runtimeStopped {
			return
		}
		cancelRuntime()
		select {
		case err := <-runtimeDone:
			if err != nil {
				t.Errorf("raw-preserver runtime shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("raw-preserver runtime did not stop")
		}
	})

	adminConnection := connectRuntimeNATS(
		t,
		config.NATSURL,
		os.Getenv("NATS_ADMIN_USER"),
		os.Getenv("NATS_ADMIN_PASSWORD"),
		"raw-preserver-integration-admin",
	)
	defer adminConnection.Close()
	adminJS, err := jetstream.New(adminConnection)
	if err != nil {
		t.Fatalf("create admin JetStream client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waitForConsumer(t, ctx, adminJS)

	normalizerConnection := connectRuntimeNATS(
		t,
		config.NATSURL,
		os.Getenv("NATS_NORMALIZER_USER"),
		os.Getenv("NATS_NORMALIZER_PASSWORD"),
		"raw-preserver-integration-observer",
	)
	defer normalizerConnection.Close()
	persistedSubscription, err := normalizerConnection.SubscribeSync(RawPersistedSubject)
	if err != nil {
		t.Fatalf("subscribe raw.persisted: %v", err)
	}
	defer persistedSubscription.Unsubscribe()
	if err := normalizerConnection.Flush(); err != nil {
		t.Fatalf("flush raw.persisted subscription: %v", err)
	}

	ingestConnection := connectRuntimeNATS(
		t,
		config.NATSURL,
		os.Getenv("NATS_INGEST_USER"),
		os.Getenv("NATS_INGEST_PASSWORD"),
		"raw-preserver-integration-ingest",
	)
	defer ingestConnection.Close()
	ingestJS, err := jetstream.New(ingestConnection)
	if err != nil {
		t.Fatalf("create ingest JetStream client: %v", err)
	}

	delivery := freshIntegrationDelivery(t)
	wire, err := proto.Marshal(delivery.Envelope)
	if err != nil {
		t.Fatalf("marshal raw.received envelope: %v", err)
	}
	message := &nats.Msg{
		Subject: RawReceivedSubject,
		Header:  make(nats.Header),
		Data:    wire,
	}
	message.Header.Set(nats.MsgIdHdr, delivery.Envelope.GetMessageId())
	message.Header.Set(RequestIDHeader, delivery.RequestID)

	publishAck, err := ingestJS.PublishMsg(ctx, message)
	if err != nil {
		t.Fatalf("publish raw.received with ingest identity: %v", err)
	}
	if publishAck == nil || publishAck.Stream != RawStreamName || publishAck.Sequence == 0 {
		t.Fatalf("raw.received PubAck = %#v", publishAck)
	}

	persistedMessage, err := persistedSubscription.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("wait raw.persisted: %v", err)
	}
	persistedEnvelope := new(contractsv1.CerberoEnvelope)
	if err := proto.Unmarshal(persistedMessage.Data, persistedEnvelope); err != nil {
		t.Fatalf("unmarshal raw.persisted envelope: %v", err)
	}
	if persistedEnvelope.GetCausationId() != delivery.Envelope.GetMessageId() {
		t.Fatalf(
			"raw.persisted causation_id = %q, want %q",
			persistedEnvelope.GetCausationId(),
			delivery.Envelope.GetMessageId(),
		)
	}

	database, err := sql.Open("pgx", config.postgresDSN())
	if err != nil {
		t.Fatalf("open raw-preserver PostgreSQL connection: %v", err)
	}
	defer database.Close()
	metadata, err := NewPostgresMetadataStore(database)
	if err != nil {
		t.Fatal(err)
	}

	record := waitForPublishedRecord(t, ctx, metadata, delivery.Envelope.GetMessageId())
	if record.Publication.MessageID != persistedEnvelope.GetMessageId() {
		t.Fatalf(
			"outbox message_id = %q, raw.persisted message_id = %q",
			record.Publication.MessageID,
			persistedEnvelope.GetMessageId(),
		)
	}

	rawEvent := unpackRawEvent(t, delivery.Envelope)
	assertSingleRawObject(t, config.RawStorePath, rawEvent.GetRawPayload())

	reset, err := adminJS.ResetConsumerToSequence(
		ctx,
		RawStreamName,
		RawPreserverConsumerName,
		publishAck.Sequence,
	)
	if err != nil {
		t.Fatalf("reset raw-preserver consumer for redelivery: %v", err)
	}
	if reset == nil || reset.ResetSeq != publishAck.Sequence {
		t.Fatalf("consumer reset response = %#v, want reset_seq=%d", reset, publishAck.Sequence)
	}
	waitForRedeliveryACK(t, ctx, adminJS, publishAck.Sequence)

	duplicateRecord := waitForPublishedRecord(t, ctx, metadata, delivery.Envelope.GetMessageId())
	if duplicateRecord.Publication.MessageID != record.Publication.MessageID {
		t.Fatalf(
			"redelivery publication changed from %q to %q",
			record.Publication.MessageID,
			duplicateRecord.Publication.MessageID,
		)
	}
	assertSingleRawObject(t, config.RawStorePath, rawEvent.GetRawPayload())

	if extra, err := persistedSubscription.NextMsg(500 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
		if err == nil {
			t.Fatalf("redelivery emitted duplicate raw.persisted message: %d bytes", len(extra.Data))
		}
		t.Fatalf("wait for duplicate raw.persisted: %v", err)
	}

	cancelRuntime()
	select {
	case err := <-runtimeDone:
		if err != nil {
			t.Fatalf("raw-preserver runtime returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("raw-preserver runtime did not stop after cancellation")
	}
	runtimeStopped = true
}

func freshIntegrationDelivery(t *testing.T) Delivery {
	t.Helper()
	delivery := validDelivery(t)
	generator := NewUUIDv7Generator(nil, nil)
	delivery.Envelope.MessageId = mustRuntimeUUID(t, generator)
	delivery.Envelope.TraceId = mustRuntimeUUID(t, generator)
	delivery.RequestID = mustRuntimeUUID(t, generator)

	rawEvent := unpackRawEvent(t, delivery.Envelope)
	rawEvent.EventId = mustRuntimeUUID(t, generator)
	payload, err := anypb.New(rawEvent)
	if err != nil {
		t.Fatalf("repack integration RawEvent: %v", err)
	}
	delivery.Envelope.Payload = payload
	return delivery
}

func mustRuntimeUUID(t *testing.T, generator UUIDv7Generator) string {
	t.Helper()
	value, err := generator.New()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func connectRuntimeNATS(t *testing.T, url, user, password, name string) *nats.Conn {
	t.Helper()
	if user == "" || password == "" {
		t.Fatalf("NATS credentials missing for %s", name)
	}
	connection, err := nats.Connect(
		url,
		nats.UserInfo(user, password),
		nats.Name(name),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("connect %s: %v", name, err)
	}
	return connection
}

func waitForConsumer(t *testing.T, ctx context.Context, js jetstream.JetStream) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := js.Consumer(ctx, RawStreamName, RawPreserverConsumerName); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("raw-preserver consumer not ready: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForPublishedRecord(
	t *testing.T,
	ctx context.Context,
	metadata *PostgresMetadataStore,
	messageID string,
) Record {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		record, found, err := metadata.Find(ctx, RawPreserverConsumerName, messageID)
		if err == nil && found && record.Published {
			return record
		}
		if err != nil {
			t.Fatalf("find published preservation record: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("published preservation record not visible: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForRedeliveryACK(
	t *testing.T,
	ctx context.Context,
	js jetstream.JetStream,
	streamSequence uint64,
) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		consumer, err := js.Consumer(ctx, RawStreamName, RawPreserverConsumerName)
		if err == nil {
			info, infoErr := consumer.Info(ctx)
			if infoErr == nil && info.AckFloor.Stream >= streamSequence && info.NumAckPending == 0 {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("raw.received redelivery not acknowledged: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertSingleRawObject(t *testing.T, root string, want []byte) {
	t.Helper()
	var rawFiles []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && entry.Name() == filesystemRawDataFilename {
			rawFiles = append(rawFiles, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk raw store: %v", err)
	}
	if len(rawFiles) != 1 {
		t.Fatalf("raw.bin count = %d, want 1 (%v)", len(rawFiles), rawFiles)
	}
	got, err := os.ReadFile(rawFiles[0])
	if err != nil {
		t.Fatalf("read raw evidence: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("raw evidence bytes changed")
	}
}
