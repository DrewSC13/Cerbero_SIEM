//go:build integration

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const step19IsolatedNATSURL = "nats://127.0.0.1:44222"

func TestWorkerSelectiveReplayNATSIntegration(t *testing.T) {
	if os.Getenv("CERBERO_STEP19_NATS_INTEGRATION") != "isolated" {
		t.Skip("set CERBERO_STEP19_NATS_INTEGRATION=isolated to run the destructive isolated-NATS integration")
	}

	config, err := loadWorkerRuntimeConfig(os.Getenv)
	if err != nil {
		t.Fatalf("load worker runtime config: %v", err)
	}
	if config.NATSURL != step19IsolatedNATSURL {
		t.Fatalf(
			"refusing to run against non-isolated NATS URL %q; want %q",
			config.NATSURL,
			step19IsolatedNATSURL,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	adminConnection, err := nats.Connect(
		config.NATSURL,
		nats.UserInfo(
			requiredStep19IntegrationEnv(t, "NATS_ADMIN_USER"),
			requiredStep19IntegrationEnv(t, "NATS_ADMIN_PASSWORD"),
		),
		nats.Name("cerbero-step19-integration-admin"),
		nats.Timeout(3*time.Second),
	)
	if err != nil {
		t.Fatalf("connect admin NATS: %v", err)
	}
	defer adminConnection.Close()

	adminJS, err := jetstream.New(adminConnection)
	if err != nil {
		t.Fatalf("create admin JetStream context: %v", err)
	}

	rawStream, err := adminJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        rawStreamName,
		Subjects:    []string{"cerbero.v1.raw.*"},
		Storage:     jetstream.MemoryStorage,
		Duplicates:  2 * time.Minute,
		AllowDirect: true,
	})
	if err != nil {
		t.Fatalf("create isolated raw stream: %v", err)
	}
	dlqStream, err := adminJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     dlqStreamName,
		Subjects: []string{"cerbero.v1.dlq.*"},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create isolated DLQ stream: %v", err)
	}
	if _, err := adminJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "CERBERO_ANALYTICS",
		Subjects: []string{"cerbero.v1.normalized.*"},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("create isolated analytics stream: %v", err)
	}

	assertWorkerNATSACL(t, ctx, config)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	rootDLQRecordID := "it-worker-" + suffix
	lifecycleKey := replayLifecycleKey(rootDLQRecordID)

	adminDB, err := sql.Open("pgx", step19AdminPostgresDSN(t))
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("ping PostgreSQL admin connection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := adminDB.ExecContext(
			cleanupCtx,
			"DELETE FROM system.normalization_dlq_replay_attempt WHERE lifecycle_key = $1",
			lifecycleKey,
		); err != nil {
			t.Errorf("cleanup replay attempt ledger: %v", err)
		}
		if _, err := adminDB.ExecContext(
			cleanupCtx,
			"DELETE FROM system.normalization_dlq_replay_state WHERE lifecycle_key = $1",
			lifecycleKey,
		); err != nil {
			t.Errorf("cleanup replay state: %v", err)
		}
		if err := adminDB.Close(); err != nil {
			t.Errorf("close PostgreSQL admin connection: %v", err)
		}
	})

	observerName := "step19-observer-" + suffix
	observer, err := rawStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       observerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		FilterSubject: selectiveReplaySubject,
		MaxAckPending: 1,
	})
	if err != nil {
		t.Fatalf("create selective replay observer: %v", err)
	}

	rawBefore, err := rawStream.Info(ctx)
	if err != nil {
		t.Fatalf("read raw stream before replay: %v", err)
	}
	rawMessagesBefore := rawBefore.State.Msgs

	workerCtx, workerCancel := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- runWorker(
			workerCtx,
			config,
			slog.New(slog.NewTextHandler(io.Discard, nil)),
		)
	}()
	defer workerCancel()

	workerConsumer, baselineAckFloor := waitForStep19WorkerConsumer(
		t,
		ctx,
		dlqStream,
		workerDone,
	)
	workerConsumerInfo, err := workerConsumer.Info(ctx)
	if err != nil {
		t.Fatalf("read worker DLQ consumer config: %v", err)
	}
	if workerConsumerInfo.Config.DeliverPolicy != jetstream.DeliverNewPolicy {
		t.Fatalf(
			"worker DLQ deliver policy = %v, want DeliverNewPolicy",
			workerConsumerInfo.Config.DeliverPolicy,
		)
	}

	sourcePayload := []byte("step19-selective-replay-source-" + suffix)
	requestID := "step19-request-" + suffix
	source := nats.NewMsg(rawPersistedSubject)
	source.Data = append([]byte(nil), sourcePayload...)
	source.Header.Set(requestIDHeader, requestID)
	source.Header.Set(nats.MsgIdHdr, "step19-source-"+suffix)

	sourceAck, err := adminJS.PublishMsg(ctx, source)
	if err != nil {
		t.Fatalf("publish raw.persisted source: %v", err)
	}
	if sourceAck.Stream != rawStreamName || sourceAck.Sequence == 0 {
		t.Fatalf("unexpected source PubAck: %+v", sourceAck)
	}

	sourceSequence := sourceAck.Sequence
	record := normalizationDeadLetter{
		SchemaVersion:         normalizationDLQSchema,
		DLQRecordID:           rootDLQRecordID,
		OriginalSubject:       rawPersistedSubject,
		OriginalPayloadSHA256: sha256LowerHex(sourcePayload),
		Retryable:             true,
		StreamSequence:        &sourceSequence,
	}
	wire, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal replayable DLQ record: %v", err)
	}

	publishStep19DLQ(t, ctx, adminJS, wire, "step19-dlq-"+suffix+"-1")

	replayMessage, err := observer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("receive selective replay: %v", err)
	}
	if replayMessage.Subject() != selectiveReplaySubject {
		t.Fatalf("replay subject = %q", replayMessage.Subject())
	}
	if !bytes.Equal(replayMessage.Data(), sourcePayload) {
		t.Fatal("selective replay payload differs from original raw.persisted bytes")
	}

	headers := replayMessage.Headers()
	if headers.Get(requestIDHeader) != requestID {
		t.Fatalf("replay request ID = %q", headers.Get(requestIDHeader))
	}
	if headers.Get(executionModeHeader) != replayExecutionMode {
		t.Fatalf("replay execution mode = %q", headers.Get(executionModeHeader))
	}
	if headers.Get(replayRootDLQRecordIDHeader) != rootDLQRecordID {
		t.Fatalf(
			"replay root DLQ record ID = %q",
			headers.Get(replayRootDLQRecordIDHeader),
		)
	}
	if headers.Get(replayAttemptHeader) != "1" {
		t.Fatalf("replay attempt = %q", headers.Get(replayAttemptHeader))
	}
	if headers.Get(replaySourceDLQRecordIDHeader) != rootDLQRecordID {
		t.Fatalf(
			"replay source DLQ record ID = %q",
			headers.Get(replaySourceDLQRecordIDHeader),
		)
	}
	if headers.Get(replaySourceStreamSequenceHeader) != strconv.FormatUint(sourceSequence, 10) {
		t.Fatalf(
			"replay source stream sequence = %q",
			headers.Get(replaySourceStreamSequenceHeader),
		)
	}
	wantMessageID := fmt.Sprintf(
		"normalization-replay:v1:%s:1",
		rootDLQRecordID,
	)
	if headers.Get(nats.MsgIdHdr) != wantMessageID {
		t.Fatalf("replay Nats-Msg-Id = %q", headers.Get(nats.MsgIdHdr))
	}
	if err := replayMessage.DoubleAck(ctx); err != nil {
		t.Fatalf("ack selective replay observer: %v", err)
	}

	waitForStep19WorkerAckFloor(
		t,
		ctx,
		workerConsumer,
		baselineAckFloor+1,
	)

	assertStep19ReplayState(t, ctx, config, lifecycleKey, 1, 1)

	rawAfterFirst, err := rawStream.Info(ctx)
	if err != nil {
		t.Fatalf("read raw stream after first replay: %v", err)
	}
	if rawAfterFirst.State.Msgs != rawMessagesBefore+2 {
		t.Fatalf(
			"raw messages after first replay = %d, want %d",
			rawAfterFirst.State.Msgs,
			rawMessagesBefore+2,
		)
	}

	publishStep19DLQ(t, ctx, adminJS, wire, "step19-dlq-"+suffix+"-2")
	waitForStep19WorkerAckFloor(
		t,
		ctx,
		workerConsumer,
		baselineAckFloor+2,
	)

	rawAfterRedelivery, err := rawStream.Info(ctx)
	if err != nil {
		t.Fatalf("read raw stream after replay redelivery: %v", err)
	}
	if rawAfterRedelivery.State.Msgs != rawAfterFirst.State.Msgs {
		t.Fatalf(
			"same DLQ record created another raw replay message: before=%d after=%d",
			rawAfterFirst.State.Msgs,
			rawAfterRedelivery.State.Msgs,
		)
	}
	assertStep19ReplayState(t, ctx, config, lifecycleKey, 1, 1)

	workerCancel()
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("worker stopped with error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}

func assertWorkerNATSACL(
	t *testing.T,
	ctx context.Context,
	config workerRuntimeConfig,
) {
	t.Helper()

	connection, err := nats.Connect(
		config.NATSURL,
		nats.UserInfo(config.NATSUser, config.NATSPassword),
		nats.Name("cerbero-step19-worker-acl-check"),
		nats.Timeout(3*time.Second),
	)
	if err != nil {
		t.Fatalf("connect with worker NATS identity: %v", err)
	}
	defer connection.Close()

	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("create worker ACL JetStream context: %v", err)
	}

	if _, err := js.Stream(ctx, rawStreamName); err != nil {
		t.Fatalf("worker cannot read raw stream metadata: %v", err)
	}
	if _, err := js.Stream(ctx, dlqStreamName); err != nil {
		t.Fatalf("worker cannot read DLQ stream metadata: %v", err)
	}

	deniedCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := js.Stream(deniedCtx, "CERBERO_ANALYTICS"); err == nil {
		t.Fatal("worker unexpectedly read analytics stream metadata")
	}

	forbidden := nats.NewMsg(rawPersistedSubject)
	forbidden.Data = []byte("worker must not publish raw.persisted")
	forbidden.Header.Set(nats.MsgIdHdr, "step19-forbidden-raw-persisted")
	deniedPublishCtx, cancelPublish := context.WithTimeout(ctx, 2*time.Second)
	defer cancelPublish()
	if _, err := js.PublishMsg(deniedPublishCtx, forbidden); err == nil {
		t.Fatal("worker unexpectedly published raw.persisted")
	}
}

func publishStep19DLQ(
	t *testing.T,
	ctx context.Context,
	js jetstream.JetStream,
	wire []byte,
	messageID string,
) {
	t.Helper()

	message := nats.NewMsg(normalizationDLQSubject)
	message.Data = append([]byte(nil), wire...)
	message.Header.Set(nats.MsgIdHdr, messageID)
	ack, err := js.PublishMsg(ctx, message)
	if err != nil {
		t.Fatalf("publish normalization DLQ record: %v", err)
	}
	if ack.Stream != dlqStreamName || ack.Sequence == 0 {
		t.Fatalf("unexpected normalization DLQ PubAck: %+v", ack)
	}
}

func waitForStep19WorkerConsumer(
	t *testing.T,
	ctx context.Context,
	stream jetstream.Stream,
	workerDone <-chan error,
) (jetstream.Consumer, uint64) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-workerDone:
			t.Fatalf("worker stopped before creating DLQ consumer: %v", err)
		default:
		}

		consumer, err := stream.Consumer(ctx, workerNormalizationDLQReplayConsumer)
		if err == nil {
			info, infoErr := consumer.Info(ctx)
			if infoErr == nil {
				return consumer, info.AckFloor.Consumer
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("context ended while waiting for worker consumer: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("worker DLQ consumer was not created")
	return nil, 0
}

func waitForStep19WorkerAckFloor(
	t *testing.T,
	ctx context.Context,
	consumer jetstream.Consumer,
	want uint64,
) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		info, err := consumer.Info(ctx)
		if err == nil &&
			info.AckFloor.Consumer >= want &&
			info.NumAckPending == 0 &&
			info.NumPending == 0 {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("context ended while waiting for worker ACK floor: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatalf("worker ACK floor did not reach %d", want)
}

func assertStep19ReplayState(
	t *testing.T,
	ctx context.Context,
	config workerRuntimeConfig,
	lifecycleKey string,
	wantConsumed int,
	wantLedgerRows int,
) {
	t.Helper()

	db, err := sql.Open("pgx", config.postgresDSN())
	if err != nil {
		t.Fatalf("open worker PostgreSQL verification connection: %v", err)
	}
	defer db.Close()

	var (
		consumed int
		maximum  int
	)
	if err := db.QueryRowContext(
		ctx,
		`SELECT consumed_attempts, max_attempts
FROM system.normalization_dlq_replay_state
WHERE lifecycle_key = $1`,
		lifecycleKey,
	).Scan(&consumed, &maximum); err != nil {
		t.Fatalf("read replay state: %v", err)
	}
	if consumed != wantConsumed {
		t.Fatalf("consumed replay attempts = %d, want %d", consumed, wantConsumed)
	}
	if maximum != int(config.MaxAutomaticAttempts) {
		t.Fatalf(
			"captured replay max attempts = %d, want %d",
			maximum,
			config.MaxAutomaticAttempts,
		)
	}

	var ledgerRows int
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*)
FROM system.normalization_dlq_replay_attempt
WHERE lifecycle_key = $1`,
		lifecycleKey,
	).Scan(&ledgerRows); err != nil {
		t.Fatalf("read replay attempt ledger: %v", err)
	}
	if ledgerRows != wantLedgerRows {
		t.Fatalf(
			"replay attempt ledger rows = %d, want %d",
			ledgerRows,
			wantLedgerRows,
		)
	}
}

func step19AdminPostgresDSN(t *testing.T) string {
	t.Helper()

	connection := &url.URL{
		Scheme: "postgres",
		User: url.UserPassword(
			requiredStep19IntegrationEnv(t, "POSTGRES_USER"),
			requiredStep19IntegrationEnv(t, "POSTGRES_PASSWORD"),
		),
		Host: net.JoinHostPort(
			requiredStep19IntegrationEnv(t, "POSTGRES_HOST"),
			requiredStep19IntegrationEnv(t, "POSTGRES_PORT"),
		),
		Path: requiredStep19IntegrationEnv(t, "POSTGRES_DB"),
	}
	query := connection.Query()
	query.Set("sslmode", "disable")
	connection.RawQuery = query.Encode()
	return connection.String()
}

func requiredStep19IntegrationEnv(t *testing.T, name string) string {
	t.Helper()

	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required for Step 19 NATS integration", name)
	}
	return value
}
