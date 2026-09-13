package preserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

type fakeSQLResult struct {
	rows int64
	err  error
}

func (r fakeSQLResult) LastInsertId() (int64, error) { return 0, errors.New("unsupported") }
func (r fakeSQLResult) RowsAffected() (int64, error) { return r.rows, r.err }

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan destinations = %d, values = %d", len(dest), len(r.values))
	}
	for i := range dest {
		if err := assignFakeScan(dest[i], r.values[i]); err != nil {
			return fmt.Errorf("scan value %d: %w", i, err)
		}
	}
	return nil
}

func assignFakeScan(dest, value any) error {
	switch target := dest.(type) {
	case *string:
		got, ok := value.(string)
		if !ok {
			return fmt.Errorf("value %T is not string", value)
		}
		*target = got
	case *[]byte:
		got, ok := value.([]byte)
		if !ok {
			return fmt.Errorf("value %T is not []byte", value)
		}
		*target = append((*target)[:0], got...)
	case *bool:
		got, ok := value.(bool)
		if !ok {
			return fmt.Errorf("value %T is not bool", value)
		}
		*target = got
	default:
		return fmt.Errorf("unsupported destination %T", dest)
	}
	return nil
}

type queryExpectation struct {
	contains string
	row      rowScanner
}

type execExpectation struct {
	contains string
	result   sql.Result
	err      error
}

type fakeMetadataDB struct {
	queries []queryExpectation
	execs   []execExpectation
	tx      *fakeMetadataTx
	begins  int
}

func (d *fakeMetadataDB) QueryRowContext(_ context.Context, query string, _ ...any) rowScanner {
	if len(d.queries) == 0 {
		return fakeRow{err: errors.New("unexpected database query")}
	}
	expectation := d.queries[0]
	d.queries = d.queries[1:]
	if !strings.Contains(query, expectation.contains) {
		return fakeRow{err: fmt.Errorf("query %q does not contain %q", query, expectation.contains)}
	}
	return expectation.row
}

func (d *fakeMetadataDB) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	if len(d.execs) == 0 {
		return nil, errors.New("unexpected database exec")
	}
	expectation := d.execs[0]
	d.execs = d.execs[1:]
	if !strings.Contains(query, expectation.contains) {
		return nil, fmt.Errorf("exec %q does not contain %q", query, expectation.contains)
	}
	return expectation.result, expectation.err
}

func (d *fakeMetadataDB) BeginTx(_ context.Context, options *sql.TxOptions) (metadataTx, error) {
	d.begins++
	if options == nil || options.Isolation != sql.LevelReadCommitted {
		return nil, errors.New("unexpected transaction options")
	}
	if d.tx == nil {
		return nil, errors.New("transaction unavailable")
	}
	return d.tx, nil
}

type fakeMetadataTx struct {
	queries     []queryExpectation
	execs       []execExpectation
	committed   bool
	rollbacks   int
	commitErr   error
	rollbackErr error
}

func (tx *fakeMetadataTx) QueryRowContext(_ context.Context, query string, _ ...any) rowScanner {
	if len(tx.queries) == 0 {
		return fakeRow{err: errors.New("unexpected transaction query")}
	}
	expectation := tx.queries[0]
	tx.queries = tx.queries[1:]
	if !strings.Contains(query, expectation.contains) {
		return fakeRow{err: fmt.Errorf("query %q does not contain %q", query, expectation.contains)}
	}
	return expectation.row
}

func (tx *fakeMetadataTx) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	if len(tx.execs) == 0 {
		return nil, errors.New("unexpected transaction exec")
	}
	expectation := tx.execs[0]
	tx.execs = tx.execs[1:]
	if !strings.Contains(query, expectation.contains) {
		return nil, fmt.Errorf("exec %q does not contain %q", query, expectation.contains)
	}
	return expectation.result, expectation.err
}

func (tx *fakeMetadataTx) Commit() error {
	tx.committed = true
	return tx.commitErr
}

func (tx *fakeMetadataTx) Rollback() error {
	tx.rollbacks++
	return tx.rollbackErr
}

func TestPostgresMetadataStoreFindReconstructsStableRecordWithoutNarrowingUint64(t *testing.T) {
	record := validRecord()
	record.RawObject.Offset = math.MaxUint64
	record.RawObject.Length = math.MaxUint64
	record.Published = true

	db := &fakeMetadataDB{queries: []queryExpectation{{
		contains: "FROM system.processed_messages",
		row:      metadataRecordRow(record),
	}}}
	store := newPostgresMetadataStore(db)

	got, found, err := store.Find(context.Background(), consumerName, incomingMessageID)
	if err != nil {
		t.Fatalf("Find() error = %v", err)
	}
	if !found {
		t.Fatal("Find() found = false")
	}
	if !reflect.DeepEqual(got, record) {
		t.Fatalf("Find() record = %#v, want %#v", got, record)
	}
}

func TestPostgresMetadataStoreCommitPreservationCommitsAllDurableStateAtomically(t *testing.T) {
	record := validRecord()
	tx := &fakeMetadataTx{
		queries: []queryExpectation{
			{contains: "FROM system.processed_messages", row: fakeRow{err: sql.ErrNoRows}},
			{contains: "FROM system.raw_objects", row: rawObjectRow(record.RawObject)},
			{contains: "FROM system.outbox", row: outboxRow(record.Publication, false)},
		},
		execs: []execExpectation{
			{contains: "INSERT INTO system.raw_objects", result: fakeSQLResult{rows: 1}},
			{contains: "INSERT INTO system.outbox", result: fakeSQLResult{rows: 1}},
			{contains: "INSERT INTO system.processed_messages", result: fakeSQLResult{rows: 1}},
		},
	}
	store := newPostgresMetadataStore(&fakeMetadataDB{tx: tx})

	got, err := store.CommitPreservation(context.Background(), record)
	if err != nil {
		t.Fatalf("CommitPreservation() error = %v", err)
	}
	if !reflect.DeepEqual(got, record) {
		t.Fatalf("CommitPreservation() record = %#v, want %#v", got, record)
	}
	if !tx.committed {
		t.Fatal("transaction was not committed")
	}
	if tx.rollbacks == 0 {
		t.Fatal("deferred rollback safety was not exercised")
	}
	if len(tx.queries) != 0 || len(tx.execs) != 0 {
		t.Fatalf("unused scripted operations: queries=%d execs=%d", len(tx.queries), len(tx.execs))
	}
}

func TestPostgresMetadataStoreCommitPreservationReturnsExistingRecordWithoutWrites(t *testing.T) {
	existing := validRecord()
	tx := &fakeMetadataTx{queries: []queryExpectation{{
		contains: "FROM system.processed_messages",
		row:      metadataRecordRow(existing),
	}}}
	store := newPostgresMetadataStore(&fakeMetadataDB{tx: tx})

	got, err := store.CommitPreservation(context.Background(), validRecord())
	if err != nil {
		t.Fatalf("CommitPreservation() error = %v", err)
	}
	if !reflect.DeepEqual(got, existing) {
		t.Fatalf("CommitPreservation() record = %#v, want %#v", got, existing)
	}
	if tx.committed {
		t.Fatal("duplicate transaction unexpectedly committed")
	}
	if tx.rollbacks == 0 {
		t.Fatal("duplicate transaction was not rolled back")
	}
	if len(tx.execs) != 0 {
		t.Fatalf("duplicate transaction executed %d writes", len(tx.execs))
	}
}

func TestPostgresMetadataStoreCommitPreservationRollsBackConcurrentLoser(t *testing.T) {
	record := validRecord()
	winner := validRecord()
	tx := &fakeMetadataTx{
		queries: []queryExpectation{
			{contains: "FROM system.processed_messages", row: fakeRow{err: sql.ErrNoRows}},
			{contains: "FROM system.raw_objects", row: rawObjectRow(record.RawObject)},
			{contains: "FROM system.outbox", row: outboxRow(record.Publication, false)},
			{contains: "FROM system.processed_messages", row: metadataRecordRow(winner)},
		},
		execs: []execExpectation{
			{contains: "INSERT INTO system.raw_objects", result: fakeSQLResult{rows: 1}},
			{contains: "INSERT INTO system.outbox", result: fakeSQLResult{rows: 1}},
			{contains: "INSERT INTO system.processed_messages", result: fakeSQLResult{rows: 0}},
		},
	}
	store := newPostgresMetadataStore(&fakeMetadataDB{tx: tx})

	got, err := store.CommitPreservation(context.Background(), record)
	if err != nil {
		t.Fatalf("CommitPreservation() error = %v", err)
	}
	if !reflect.DeepEqual(got, winner) {
		t.Fatalf("CommitPreservation() winner = %#v, want %#v", got, winner)
	}
	if tx.committed {
		t.Fatal("concurrent loser unexpectedly committed tentative rows")
	}
	if tx.rollbacks == 0 {
		t.Fatal("concurrent loser did not roll back tentative rows")
	}
}

func TestPostgresMetadataStoreCommitPreservationRejectsConflictingRawLocator(t *testing.T) {
	record := validRecord()
	conflicting := record.RawObject
	conflicting.StorageURI = "raw://different/object"
	tx := &fakeMetadataTx{
		queries: []queryExpectation{
			{contains: "FROM system.processed_messages", row: fakeRow{err: sql.ErrNoRows}},
			{contains: "FROM system.raw_objects", row: rawObjectRow(conflicting)},
		},
		execs: []execExpectation{{contains: "INSERT INTO system.raw_objects", result: fakeSQLResult{rows: 0}}},
	}
	store := newPostgresMetadataStore(&fakeMetadataDB{tx: tx})

	_, err := store.CommitPreservation(context.Background(), record)
	if !errors.Is(err, ErrMetadataConflict) {
		t.Fatalf("CommitPreservation() error = %v, want metadata conflict", err)
	}
	if tx.committed {
		t.Fatal("conflicting locator transaction unexpectedly committed")
	}
	if tx.rollbacks == 0 {
		t.Fatal("conflicting locator transaction was not rolled back")
	}
}

func TestPostgresMetadataStoreMarkPublishedIsIdempotentAndRequiresExistingRecord(t *testing.T) {
	db := &fakeMetadataDB{execs: []execExpectation{
		{contains: "UPDATE system.outbox", result: fakeSQLResult{rows: 1}},
		{contains: "UPDATE system.outbox", result: fakeSQLResult{rows: 1}},
		{contains: "UPDATE system.outbox", result: fakeSQLResult{rows: 0}},
	}}
	store := newPostgresMetadataStore(db)

	if err := store.MarkPublished(context.Background(), consumerName, incomingMessageID); err != nil {
		t.Fatalf("first MarkPublished() error = %v", err)
	}
	if err := store.MarkPublished(context.Background(), consumerName, incomingMessageID); err != nil {
		t.Fatalf("second MarkPublished() error = %v", err)
	}
	if err := store.MarkPublished(context.Background(), consumerName, incomingMessageID); !errors.Is(err, ErrMetadataRecordNotFound) {
		t.Fatalf("missing MarkPublished() error = %v, want not found", err)
	}
}

func metadataRecordRow(record Record) rowScanner {
	return fakeRow{values: []any{
		record.ConsumerName,
		record.IncomingMessageID,
		record.EventID,
		record.RawObject.StorageURI,
		record.RawObject.SegmentID,
		fmt.Sprintf("%d", record.RawObject.Offset),
		fmt.Sprintf("%d", record.RawObject.Length),
		record.RawObject.Hash,
		record.Publication.Subject,
		record.Publication.MessageID,
		record.Publication.RequestID,
		append([]byte(nil), record.Publication.Payload...),
		record.Published,
	}}
}

func rawObjectRow(object RawObject) rowScanner {
	return fakeRow{values: []any{
		object.StorageURI,
		object.SegmentID,
		fmt.Sprintf("%d", object.Offset),
		fmt.Sprintf("%d", object.Length),
		object.Hash,
	}}
}

func outboxRow(publication Publication, published bool) rowScanner {
	return fakeRow{values: []any{
		publication.Subject,
		publication.MessageID,
		publication.RequestID,
		append([]byte(nil), publication.Payload...),
		published,
	}}
}
