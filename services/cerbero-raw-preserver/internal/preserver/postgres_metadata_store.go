package preserver

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const processedResultPreserved = "PRESERVED"

var (
	// ErrMetadataRecordNotFound means the durable processed-message/outbox state is missing.
	ErrMetadataRecordNotFound = errors.New("raw preservation metadata record not found")
	// ErrMetadataConflict means durable state exists but does not match the preservation record.
	ErrMetadataConflict = errors.New("raw preservation metadata conflict")
)

type rowScanner interface {
	Scan(...any) error
}

type metadataQueryer interface {
	QueryRowContext(context.Context, string, ...any) rowScanner
}

type metadataTx interface {
	metadataQueryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

type metadataDatabase interface {
	metadataQueryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	BeginTx(context.Context, *sql.TxOptions) (metadataTx, error)
}

type stdMetadataDatabase struct {
	db *sql.DB
}

type stdMetadataTx struct {
	tx *sql.Tx
}

func (d stdMetadataDatabase) QueryRowContext(ctx context.Context, query string, args ...any) rowScanner {
	return d.db.QueryRowContext(ctx, query, args...)
}

func (d stdMetadataDatabase) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.db.ExecContext(ctx, query, args...)
}

func (d stdMetadataDatabase) BeginTx(ctx context.Context, options *sql.TxOptions) (metadataTx, error) {
	tx, err := d.db.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return stdMetadataTx{tx: tx}, nil
}

func (tx stdMetadataTx) QueryRowContext(ctx context.Context, query string, args ...any) rowScanner {
	return tx.tx.QueryRowContext(ctx, query, args...)
}

func (tx stdMetadataTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.tx.ExecContext(ctx, query, args...)
}

func (tx stdMetadataTx) Commit() error {
	return tx.tx.Commit()
}

func (tx stdMetadataTx) Rollback() error {
	return tx.tx.Rollback()
}

// PostgresMetadataStore persists raw locator metadata, critical-consumer idempotency,
// and the stable raw.persisted transactional outbox publication.
type PostgresMetadataStore struct {
	db metadataDatabase
}

// NewPostgresMetadataStore binds the adapter to an already-open database/sql handle.
// Runtime driver selection and connection lifecycle remain outside the preservation core.
func NewPostgresMetadataStore(db *sql.DB) (*PostgresMetadataStore, error) {
	if db == nil {
		return nil, errors.New("postgres metadata database is required")
	}
	return newPostgresMetadataStore(stdMetadataDatabase{db: db}), nil
}

func newPostgresMetadataStore(db metadataDatabase) *PostgresMetadataStore {
	return &PostgresMetadataStore{db: db}
}

// Find returns the complete durable preservation state for one transport message.
func (s *PostgresMetadataStore) Find(ctx context.Context, consumerName, messageID string) (Record, bool, error) {
	if err := validateMetadataLookupKey(consumerName, messageID); err != nil {
		return Record{}, false, err
	}

	record, found, err := findMetadataRecord(ctx, s.db, consumerName, messageID)
	if err != nil {
		return Record{}, false, fmt.Errorf("query preservation record: %w", err)
	}
	return record, found, nil
}

// CommitPreservation atomically commits raw locator, processed-message, and outbox state.
// If another transaction wins the same critical-consumer key, this transaction rolls back
// all tentative inserts and returns the already-committed record.
func (s *PostgresMetadataStore) CommitPreservation(ctx context.Context, record Record) (Record, error) {
	if err := validateMetadataRecord(record); err != nil {
		return Record{}, err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Record{}, fmt.Errorf("begin preservation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if existing, found, err := findMetadataRecord(ctx, tx, record.ConsumerName, record.IncomingMessageID); err != nil {
		return Record{}, fmt.Errorf("check existing preservation record: %w", err)
	} else if found {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			return Record{}, fmt.Errorf("rollback duplicate preservation transaction: %w", err)
		}
		return existing, nil
	}

	if err := ensureRawObject(ctx, tx, record); err != nil {
		return Record{}, err
	}
	if err := ensureOutboxPublication(ctx, tx, record.Publication); err != nil {
		return Record{}, err
	}

	result, err := tx.ExecContext(ctx, `
INSERT INTO system.processed_messages (
    consumer_name,
    message_id,
    event_id,
    publication_message_id,
    result
) VALUES ($1, $2::uuid, $3::uuid, $4::uuid, $5)
ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		record.ConsumerName,
		record.IncomingMessageID,
		record.EventID,
		record.Publication.MessageID,
		processedResultPreserved,
	)
	if err != nil {
		return Record{}, fmt.Errorf("insert processed message: %w", err)
	}

	inserted, err := rowsAffected(result)
	if err != nil {
		return Record{}, fmt.Errorf("inspect processed-message insert: %w", err)
	}
	if !inserted {
		existing, found, err := findMetadataRecord(ctx, tx, record.ConsumerName, record.IncomingMessageID)
		if err != nil {
			return Record{}, fmt.Errorf("load concurrent preservation winner: %w", err)
		}
		if !found {
			return Record{}, errors.New("processed-message conflict without a visible winner")
		}
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			return Record{}, fmt.Errorf("rollback concurrent preservation loser: %w", err)
		}
		return existing, nil
	}

	if err := tx.Commit(); err != nil {
		return Record{}, fmt.Errorf("commit preservation transaction: %w", err)
	}
	return copyRecord(record), nil
}

// MarkPublished records the first successful durable raw.persisted publication.
// Repeated calls preserve the original published_at timestamp.
func (s *PostgresMetadataStore) MarkPublished(ctx context.Context, consumerName, messageID string) error {
	if err := validateMetadataLookupKey(consumerName, messageID); err != nil {
		return err
	}

	result, err := s.db.ExecContext(ctx, `
UPDATE system.outbox AS o
SET published_at = COALESCE(o.published_at, now())
FROM system.processed_messages AS p
WHERE p.consumer_name = $1
  AND p.message_id = $2::uuid
  AND p.publication_message_id = o.message_id`, consumerName, messageID)
	if err != nil {
		return fmt.Errorf("mark outbox publication complete: %w", err)
	}

	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("inspect outbox publication update: %w", err)
	}
	if !updated {
		return ErrMetadataRecordNotFound
	}
	return nil
}

func findMetadataRecord(ctx context.Context, queryer metadataQueryer, consumerName, messageID string) (Record, bool, error) {
	row := queryer.QueryRowContext(ctx, `
SELECT
    p.consumer_name,
    p.message_id::text,
    p.event_id::text,
    r.storage_uri,
    r.segment_id,
    r.byte_offset::text,
    r.byte_length::text,
    r.raw_hash,
    o.subject,
    o.message_id::text,
    COALESCE(o.request_id::text, ''),
    o.payload,
    (o.published_at IS NOT NULL)
FROM system.processed_messages AS p
JOIN system.raw_objects AS r
  ON r.event_id = p.event_id
JOIN system.outbox AS o
  ON o.message_id = p.publication_message_id
WHERE p.consumer_name = $1
  AND p.message_id = $2::uuid`, consumerName, messageID)

	var (
		record       Record
		offsetText   string
		lengthText   string
		requestID    string
		payloadBytes []byte
	)
	if err := row.Scan(
		&record.ConsumerName,
		&record.IncomingMessageID,
		&record.EventID,
		&record.RawObject.StorageURI,
		&record.RawObject.SegmentID,
		&offsetText,
		&lengthText,
		&record.RawObject.Hash,
		&record.Publication.Subject,
		&record.Publication.MessageID,
		&requestID,
		&payloadBytes,
		&record.Published,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, false, nil
		}
		return Record{}, false, err
	}

	offset, err := strconv.ParseUint(offsetText, 10, 64)
	if err != nil {
		return Record{}, false, fmt.Errorf("decode raw byte offset %q: %w", offsetText, err)
	}
	length, err := strconv.ParseUint(lengthText, 10, 64)
	if err != nil {
		return Record{}, false, fmt.Errorf("decode raw byte length %q: %w", lengthText, err)
	}
	record.RawObject.Offset = offset
	record.RawObject.Length = length
	record.Publication.RequestID = requestID
	record.Publication.Payload = append([]byte(nil), payloadBytes...)
	return record, true, nil
}

func ensureRawObject(ctx context.Context, tx metadataTx, record Record) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO system.raw_objects (
    event_id,
    storage_uri,
    segment_id,
    byte_offset,
    byte_length,
    raw_hash
) VALUES ($1::uuid, $2, $3, $4::numeric, $5::numeric, $6)
ON CONFLICT (event_id) DO NOTHING`,
		record.EventID,
		record.RawObject.StorageURI,
		record.RawObject.SegmentID,
		strconv.FormatUint(record.RawObject.Offset, 10),
		strconv.FormatUint(record.RawObject.Length, 10),
		record.RawObject.Hash,
	); err != nil {
		return fmt.Errorf("insert raw locator: %w", err)
	}

	var (
		actual     RawObject
		offsetText string
		lengthText string
	)
	if err := tx.QueryRowContext(ctx, `
SELECT storage_uri, segment_id, byte_offset::text, byte_length::text, raw_hash
FROM system.raw_objects
WHERE event_id = $1::uuid`, record.EventID).Scan(
		&actual.StorageURI,
		&actual.SegmentID,
		&offsetText,
		&lengthText,
		&actual.Hash,
	); err != nil {
		return fmt.Errorf("verify raw locator: %w", err)
	}

	offset, err := strconv.ParseUint(offsetText, 10, 64)
	if err != nil {
		return fmt.Errorf("decode stored raw byte offset %q: %w", offsetText, err)
	}
	length, err := strconv.ParseUint(lengthText, 10, 64)
	if err != nil {
		return fmt.Errorf("decode stored raw byte length %q: %w", lengthText, err)
	}
	actual.Offset = offset
	actual.Length = length
	if actual != record.RawObject {
		return fmt.Errorf("%w: event_id %s maps to different raw locator metadata", ErrMetadataConflict, record.EventID)
	}
	return nil
}

func ensureOutboxPublication(ctx context.Context, tx metadataTx, publication Publication) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO system.outbox (
    message_id,
    subject,
    request_id,
    payload
) VALUES ($1::uuid, $2, NULLIF($3, '')::uuid, $4)
ON CONFLICT (message_id) DO NOTHING`,
		publication.MessageID,
		publication.Subject,
		publication.RequestID,
		publication.Payload,
	); err != nil {
		return fmt.Errorf("insert outbox publication: %w", err)
	}

	var (
		actual    Publication
		requestID string
		published bool
	)
	if err := tx.QueryRowContext(ctx, `
SELECT subject, message_id::text, COALESCE(request_id::text, ''), payload, (published_at IS NOT NULL)
FROM system.outbox
WHERE message_id = $1::uuid`, publication.MessageID).Scan(
		&actual.Subject,
		&actual.MessageID,
		&requestID,
		&actual.Payload,
		&published,
	); err != nil {
		return fmt.Errorf("verify outbox publication: %w", err)
	}
	actual.RequestID = requestID

	if published || !equalPublication(actual, publication) {
		return fmt.Errorf("%w: message_id %s maps to different or already-published outbox state", ErrMetadataConflict, publication.MessageID)
	}
	return nil
}

func validateMetadataLookupKey(consumerName, messageID string) error {
	if strings.TrimSpace(consumerName) == "" {
		return errors.New("metadata consumer name is required")
	}
	if strings.TrimSpace(messageID) == "" {
		return errors.New("metadata incoming message_id is required")
	}
	return nil
}

func validateMetadataRecord(record Record) error {
	if err := validateMetadataLookupKey(record.ConsumerName, record.IncomingMessageID); err != nil {
		return err
	}
	if strings.TrimSpace(record.EventID) == "" {
		return errors.New("metadata event_id is required")
	}
	if strings.TrimSpace(record.RawObject.StorageURI) == "" {
		return errors.New("metadata raw storage_uri is required")
	}
	if strings.TrimSpace(record.RawObject.SegmentID) == "" {
		return errors.New("metadata raw segment_id is required")
	}
	if !validLowerSHA256(record.RawObject.Hash) {
		return errors.New("metadata raw hash must be lowercase SHA-256 hex")
	}
	if err := validatePublication(record.Publication); err != nil {
		return fmt.Errorf("metadata outbox publication: %w", err)
	}
	if record.Published {
		return errors.New("CommitPreservation cannot pre-mark an outbox publication as published")
	}
	return nil
}

func validLowerSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func rowsAffected(result sql.Result) (bool, error) {
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func equalPublication(left, right Publication) bool {
	return left.Subject == right.Subject &&
		left.MessageID == right.MessageID &&
		left.RequestID == right.RequestID &&
		string(left.Payload) == string(right.Payload)
}

func copyRecord(record Record) Record {
	copy := record
	copy.Publication = copyPublication(record.Publication)
	return copy
}
