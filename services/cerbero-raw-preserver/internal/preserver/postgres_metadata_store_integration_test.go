//go:build integration

package preserver

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
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresMetadataStoreIntegration(t *testing.T) {
	adminDSN := postgresIntegrationDSN(
		t,
		requiredRawPreserverIntegrationEnv(t, "POSTGRES_USER"),
		requiredRawPreserverIntegrationEnv(t, "POSTGRES_PASSWORD"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	adminDB, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	defer adminDB.Close()
	if err := adminDB.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL admin connection: %v", err)
	}

	roleName := "cerbero_raw_preserver_it_" + strings.ReplaceAll(integrationUUIDv7(t), "-", "")
	rolePassword := integrationPassword(t)
	if _, err := adminDB.ExecContext(
		ctx,
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s' IN ROLE cerbero_raw_preserver", roleName, rolePassword),
	); err != nil {
		t.Fatalf("create integration role: %v", err)
	}

	appDB, err := sql.Open("pgx", postgresIntegrationDSN(t, roleName, rolePassword))
	if err != nil {
		t.Fatalf("open PostgreSQL raw-preserver connection: %v", err)
	}
	defer func() {
		_ = appDB.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := adminDB.ExecContext(cleanupCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", roleName)); err != nil {
			t.Errorf("drop integration role: %v", err)
		}
	}()
	if err := appDB.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL raw-preserver connection: %v", err)
	}

	store, err := NewPostgresMetadataStore(appDB)
	if err != nil {
		t.Fatalf("NewPostgresMetadataStore: %v", err)
	}

	record := integrationRecord(t)
	defer cleanupIntegrationRecord(t, adminDB, record)

	committed, err := store.CommitPreservation(ctx, record)
	if err != nil {
		t.Fatalf("CommitPreservation: %v", err)
	}
	if committed.Published {
		t.Fatal("new record unexpectedly published")
	}

	found, ok, err := store.Find(ctx, record.ConsumerName, record.IncomingMessageID)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if !ok {
		t.Fatal("Find did not return committed record")
	}
	if found.RawObject.Offset != math.MaxUint64-1 || found.RawObject.Length != math.MaxUint64 {
		t.Fatalf("uint64 locator = (%d,%d)", found.RawObject.Offset, found.RawObject.Length)
	}
	if found.Publication.MessageID != record.Publication.MessageID || string(found.Publication.Payload) != string(record.Publication.Payload) {
		t.Fatal("Find did not reconstruct stable outbox publication")
	}

	duplicate, err := store.CommitPreservation(ctx, record)
	if err != nil {
		t.Fatalf("idempotent CommitPreservation: %v", err)
	}
	if duplicate.Publication.MessageID != record.Publication.MessageID {
		t.Fatalf("duplicate publication message_id = %q", duplicate.Publication.MessageID)
	}

	if err := store.MarkPublished(ctx, record.ConsumerName, record.IncomingMessageID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if err := store.MarkPublished(ctx, record.ConsumerName, record.IncomingMessageID); err != nil {
		t.Fatalf("idempotent MarkPublished: %v", err)
	}
	published, ok, err := store.Find(ctx, record.ConsumerName, record.IncomingMessageID)
	if err != nil || !ok {
		t.Fatalf("Find published record: ok=%v err=%v", ok, err)
	}
	if !published.Published {
		t.Fatal("published record not marked published")
	}

	if _, err := appDB.ExecContext(ctx, "DELETE FROM system.raw_objects WHERE event_id = $1::uuid", record.EventID); err == nil {
		t.Fatal("least-privilege integration role unexpectedly deleted raw_objects")
	}

	conflict := integrationRecord(t)
	conflict.Publication.MessageID = record.Publication.MessageID
	conflict.Publication.Payload = []byte("conflicting outbox payload")
	if _, err := store.CommitPreservation(ctx, conflict); !errors.Is(err, ErrMetadataConflict) {
		t.Fatalf("outbox conflict error = %v, want ErrMetadataConflict", err)
	}

	var orphanCount int
	if err := adminDB.QueryRowContext(
		ctx,
		"SELECT count(*) FROM system.raw_objects WHERE event_id = $1::uuid",
		conflict.EventID,
	).Scan(&orphanCount); err != nil {
		t.Fatalf("query conflict raw object: %v", err)
	}
	if orphanCount != 0 {
		t.Fatalf("conflict left %d orphan raw_objects rows", orphanCount)
	}
}

func integrationRecord(t *testing.T) Record {
	t.Helper()
	record := validRecord()
	record.IncomingMessageID = integrationUUIDv7(t)
	record.EventID = integrationUUIDv7(t)
	record.Publication.MessageID = integrationUUIDv7(t)
	record.Publication.RequestID = integrationUUIDv7(t)
	record.Publication.Payload = []byte("stable postgres integration outbox envelope")
	record.RawObject.StorageURI = "raw:///tenant-a/2026/09/13/12/" + record.EventID + "/raw.bin"
	record.RawObject.SegmentID = record.EventID
	record.RawObject.Offset = math.MaxUint64 - 1
	record.RawObject.Length = math.MaxUint64
	return record
}

func cleanupIntegrationRecord(t *testing.T, adminDB *sql.DB, record Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := adminDB.ExecContext(
		ctx,
		"DELETE FROM system.processed_messages WHERE consumer_name = $1 AND message_id = $2::uuid",
		record.ConsumerName,
		record.IncomingMessageID,
	); err != nil {
		t.Errorf("integration cleanup processed_messages: %v", err)
	}
	if _, err := adminDB.ExecContext(ctx, "DELETE FROM system.outbox WHERE message_id = $1::uuid", record.Publication.MessageID); err != nil {
		t.Errorf("integration cleanup outbox: %v", err)
	}
	if _, err := adminDB.ExecContext(ctx, "DELETE FROM system.raw_objects WHERE event_id = $1::uuid", record.EventID); err != nil {
		t.Errorf("integration cleanup raw_objects: %v", err)
	}
}

func postgresIntegrationDSN(t *testing.T, user, password string) string {
	t.Helper()
	port := requiredRawPreserverIntegrationEnv(t, "POSTGRES_PORT")
	database := requiredRawPreserverIntegrationEnv(t, "POSTGRES_DB")
	query := url.Values{}
	query.Set("sslmode", "disable")
	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort("127.0.0.1", port),
		Path:     database,
		RawQuery: query.Encode(),
	}).String()
}

func integrationPassword(t *testing.T) string {
	t.Helper()
	var value [24]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("generate integration password: %v", err)
	}
	return hex.EncodeToString(value[:])
}
