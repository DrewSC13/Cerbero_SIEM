//go:build integration

package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresReplayStateStorePersistsExhaustsAndEnforcesRBAC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminDB := openWorkerPostgresIntegrationDB(
		t,
		requiredWorkerIntegrationEnv(t, "POSTGRES_USER"),
		requiredWorkerIntegrationEnv(t, "POSTGRES_PASSWORD"),
	)
	defer adminDB.Close()

	roleName := "cerbero_worker_it_" + workerIntegrationToken(t, 8)
	rolePassword := workerIntegrationToken(t, 24)
	createWorkerIntegrationRole(t, ctx, adminDB, roleName, rolePassword)

	var appDB *sql.DB
	defer func() {
		if appDB != nil {
			_ = appDB.Close()
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := adminDB.ExecContext(
			cleanupCtx,
			"DELETE FROM system.normalization_dlq_replay_attempt WHERE lifecycle_key LIKE $1",
			"normalization-replay:v1:dlq:it-"+roleName+"%",
		); err != nil {
			t.Errorf("cleanup replay attempt ledger: %v", err)
		}
		if _, err := adminDB.ExecContext(
			cleanupCtx,
			"DELETE FROM system.normalization_dlq_replay_state WHERE replay_root_dlq_record_id LIKE $1",
			"it-"+roleName+"%",
		); err != nil {
			t.Errorf("cleanup replay state: %v", err)
		}
		if _, err := adminDB.ExecContext(
			cleanupCtx,
			fmt.Sprintf("DROP ROLE IF EXISTS %s", roleName),
		); err != nil {
			t.Errorf("drop integration role: %v", err)
		}
	}()

	appDB = openWorkerPostgresIntegrationDB(t, roleName, rolePassword)
	store, err := newPostgresReplayStateStore(appDB)
	if err != nil {
		t.Fatalf("newPostgresReplayStateStore: %v", err)
	}

	root := "it-" + roleName + "-root"
	request := replayReservationRequest{
		LifecycleKey:          replayLifecycleKey(root),
		ReplayRootDLQRecordID: root,
		SourceDLQRecordID:     "it-" + roleName + "-source-1",
		SourceStreamSequence:  math.MaxUint64,
		MaxAttempts:           2,
	}

	first, err := store.reserveAttempt(ctx, request)
	if err != nil {
		t.Fatalf("reserve first attempt: %v", err)
	}
	if !first.Reserved || first.Attempt != 1 || first.MaxAttempts != 2 {
		t.Fatalf("unexpected first reservation: %+v", first)
	}

	if err := appDB.Close(); err != nil {
		t.Fatalf("close first worker connection: %v", err)
	}
	appDB = openWorkerPostgresIntegrationDB(t, roleName, rolePassword)
	store, err = newPostgresReplayStateStore(appDB)
	if err != nil {
		t.Fatalf("newPostgresReplayStateStore after reconnect: %v", err)
	}

	repeatedFirst, err := store.reserveAttempt(ctx, request)
	if err != nil {
		t.Fatalf("reuse first source after reconnect: %v", err)
	}
	if !repeatedFirst.Reserved || repeatedFirst.Attempt != 1 || repeatedFirst.MaxAttempts != 2 {
		t.Fatalf("unexpected repeated first reservation: %+v", repeatedFirst)
	}

	request.SourceDLQRecordID = "it-" + roleName + "-source-2"
	request.SourceStreamSequence = math.MaxUint64 - 1

	second, err := store.reserveAttempt(ctx, request)
	if err != nil {
		t.Fatalf("reserve second attempt after reconnect: %v", err)
	}
	if !second.Reserved || second.Attempt != 2 || second.MaxAttempts != 2 {
		t.Fatalf("unexpected second reservation: %+v", second)
	}

	repeatedSecond, err := store.reserveAttempt(ctx, request)
	if err != nil {
		t.Fatalf("reuse second source: %v", err)
	}
	if !repeatedSecond.Reserved || repeatedSecond.Attempt != 2 || repeatedSecond.MaxAttempts != 2 {
		t.Fatalf("unexpected repeated second reservation: %+v", repeatedSecond)
	}

	request.SourceDLQRecordID = "it-" + roleName + "-source-3"
	request.SourceStreamSequence = math.MaxUint64 - 2
	exhausted, err := store.reserveAttempt(ctx, request)
	if err != nil {
		t.Fatalf("inspect exhausted budget with new source: %v", err)
	}
	if exhausted.Reserved || exhausted.Attempt != 2 || exhausted.MaxAttempts != 2 {
		t.Fatalf("unexpected exhausted reservation: %+v", exhausted)
	}

	conflict := request
	conflict.MaxAttempts = 3
	if _, err := store.reserveAttempt(ctx, conflict); !errors.Is(err, errReplayStateConflict) {
		t.Fatalf("budget conflict error = %v, want errReplayStateConflict", err)
	}

	var (
		consumed     int
		storedBudget int
		lastSequence string
	)
	if err := appDB.QueryRowContext(ctx, `
SELECT consumed_attempts, max_attempts, last_source_stream_sequence::text
FROM system.normalization_dlq_replay_state
WHERE lifecycle_key = $1`, request.LifecycleKey).Scan(
		&consumed,
		&storedBudget,
		&lastSequence,
	); err != nil {
		t.Fatalf("read replay state with worker role: %v", err)
	}
	if consumed != 2 || storedBudget != 2 {
		t.Fatalf("durable budget state = consumed %d max %d, want 2/2", consumed, storedBudget)
	}
	if lastSequence != fmt.Sprint(uint64(math.MaxUint64-1)) {
		t.Fatalf("stored stream sequence = %q", lastSequence)
	}

	var attemptRows int
	if err := appDB.QueryRowContext(
		ctx,
		"SELECT count(*) FROM system.normalization_dlq_replay_attempt WHERE lifecycle_key = $1",
		request.LifecycleKey,
	).Scan(&attemptRows); err != nil {
		t.Fatalf("read replay attempt ledger with worker role: %v", err)
	}
	if attemptRows != 2 {
		t.Fatalf("replay attempt ledger rows = %d, want 2", attemptRows)
	}

	if _, err := appDB.ExecContext(
		ctx,
		"DELETE FROM system.normalization_dlq_replay_attempt WHERE lifecycle_key = $1",
		request.LifecycleKey,
	); err == nil {
		t.Fatal("worker role unexpectedly deleted replay attempt ledger")
	}
	if _, err := appDB.ExecContext(
		ctx,
		"DELETE FROM system.normalization_dlq_replay_state WHERE lifecycle_key = $1",
		request.LifecycleKey,
	); err == nil {
		t.Fatal("worker role unexpectedly deleted replay state")
	}
	if err := appDB.QueryRowContext(
		ctx,
		"SELECT count(*) FROM system.raw_objects",
	).Scan(new(int)); err == nil {
		t.Fatal("worker role unexpectedly read raw-preserver metadata")
	}
	if err := appDB.QueryRowContext(
		ctx,
		"SELECT count(*) FROM system.normalizer_retry_state",
	).Scan(new(int)); err == nil {
		t.Fatal("worker role unexpectedly read normalizer retry state")
	}
}

func TestPostgresReplayStateStoreConcurrentReservationConsumesBudgetOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminDB := openWorkerPostgresIntegrationDB(
		t,
		requiredWorkerIntegrationEnv(t, "POSTGRES_USER"),
		requiredWorkerIntegrationEnv(t, "POSTGRES_PASSWORD"),
	)
	defer adminDB.Close()

	roleName := "cerbero_worker_it_" + workerIntegrationToken(t, 8)
	rolePassword := workerIntegrationToken(t, 24)
	createWorkerIntegrationRole(t, ctx, adminDB, roleName, rolePassword)

	root := "it-" + roleName + "-concurrent"
	requestA := replayReservationRequest{
		LifecycleKey:          replayLifecycleKey(root),
		ReplayRootDLQRecordID: root,
		SourceDLQRecordID:     "it-" + roleName + "-source-a",
		SourceStreamSequence:  42,
		MaxAttempts:           1,
	}
	requestB := requestA
	requestB.SourceDLQRecordID = "it-" + roleName + "-source-b"
	requestB.SourceStreamSequence = 43

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := adminDB.ExecContext(
			cleanupCtx,
			"DELETE FROM system.normalization_dlq_replay_attempt WHERE lifecycle_key = $1",
			requestA.LifecycleKey,
		); err != nil {
			t.Errorf("cleanup concurrent replay attempt ledger: %v", err)
		}
		if _, err := adminDB.ExecContext(
			cleanupCtx,
			"DELETE FROM system.normalization_dlq_replay_state WHERE lifecycle_key = $1",
			requestA.LifecycleKey,
		); err != nil {
			t.Errorf("cleanup concurrent replay state: %v", err)
		}
		if _, err := adminDB.ExecContext(
			cleanupCtx,
			fmt.Sprintf("DROP ROLE IF EXISTS %s", roleName),
		); err != nil {
			t.Errorf("drop concurrent integration role: %v", err)
		}
	}()

	appDB := openWorkerPostgresIntegrationDB(t, roleName, rolePassword)
	defer appDB.Close()

	store, err := newPostgresReplayStateStore(appDB)
	if err != nil {
		t.Fatalf("newPostgresReplayStateStore: %v", err)
	}

	requests := []replayReservationRequest{requestA, requestB}
	results := make(chan replayReservation, len(requests))
	errs := make(chan error, len(requests))

	var wg sync.WaitGroup
	wg.Add(len(requests))
	for _, request := range requests {
		go func(request replayReservationRequest) {
			defer wg.Done()
			reservation, reserveErr := store.reserveAttempt(ctx, request)
			if reserveErr != nil {
				errs <- reserveErr
				return
			}
			results <- reservation
		}(request)
	}
	wg.Wait()
	close(results)
	close(errs)

	for reserveErr := range errs {
		t.Fatalf("concurrent reserveAttempt: %v", reserveErr)
	}

	reserved := 0
	exhausted := 0
	for reservation := range results {
		if reservation.Reserved {
			reserved++
			if reservation.Attempt != 1 {
				t.Fatalf("reserved attempt = %d, want 1", reservation.Attempt)
			}
		} else {
			exhausted++
			if reservation.Attempt != 1 {
				t.Fatalf("exhausted attempt = %d, want 1", reservation.Attempt)
			}
		}
	}
	if reserved != 1 || exhausted != 1 {
		t.Fatalf("concurrent results: reserved=%d exhausted=%d, want 1/1", reserved, exhausted)
	}
}

func createWorkerIntegrationRole(
	t *testing.T,
	ctx context.Context,
	adminDB *sql.DB,
	roleName string,
	rolePassword string,
) {
	t.Helper()
	if _, err := adminDB.ExecContext(
		ctx,
		fmt.Sprintf(
			"CREATE ROLE %s LOGIN PASSWORD '%s' IN ROLE cerbero_worker",
			roleName,
			rolePassword,
		),
	); err != nil {
		t.Fatalf("create integration role: %v", err)
	}
}

func openWorkerPostgresIntegrationDB(t *testing.T, user, password string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", workerPostgresIntegrationDSN(t, user, password))
	if err != nil {
		t.Fatalf("open PostgreSQL connection: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("ping PostgreSQL connection: %v", err)
	}
	return db
}

func workerPostgresIntegrationDSN(t *testing.T, user, password string) string {
	t.Helper()
	host := strings.TrimSpace(os.Getenv("POSTGRES_HOST"))
	if host == "" {
		host = "127.0.0.1"
	}
	port := requiredWorkerIntegrationEnv(t, "POSTGRES_PORT")
	database := requiredWorkerIntegrationEnv(t, "POSTGRES_DB")
	query := url.Values{}
	query.Set("sslmode", "disable")
	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort(host, port),
		Path:     database,
		RawQuery: query.Encode(),
	}).String()
}

func requiredWorkerIntegrationEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required for PostgreSQL worker integration tests", name)
	}
	return value
}

func workerIntegrationToken(t *testing.T, bytes int) string {
	t.Helper()
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate integration token: %v", err)
	}
	return hex.EncodeToString(value)
}
