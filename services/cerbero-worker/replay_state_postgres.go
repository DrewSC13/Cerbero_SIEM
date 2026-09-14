package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var errReplayStateConflict = errors.New("normalization DLQ replay state conflict")

type replayReservationRequest struct {
	LifecycleKey          string
	ReplayRootDLQRecordID string
	SourceDLQRecordID     string
	SourceStreamSequence  uint64
	MaxAttempts           uint32
}

type replayReservation struct {
	Reserved              bool
	Attempt               uint32
	MaxAttempts           uint32
	LifecycleKey          string
	ReplayRootDLQRecordID string
}

type postgresReplayStateStore struct {
	db *sql.DB
}

func newPostgresReplayStateStore(db *sql.DB) (*postgresReplayStateStore, error) {
	if db == nil {
		return nil, errors.New("PostgreSQL replay state database is required")
	}
	return &postgresReplayStateStore{db: db}, nil
}

func (s *postgresReplayStateStore) reserveAttempt(
	ctx context.Context,
	request replayReservationRequest,
) (replayReservation, error) {
	if err := validateReplayReservationRequest(request); err != nil {
		return replayReservation{}, err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return replayReservation{}, fmt.Errorf("begin replay reservation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
INSERT INTO system.normalization_dlq_replay_state (
    lifecycle_key,
    replay_root_dlq_record_id,
    consumed_attempts,
    max_attempts,
    last_source_dlq_record_id,
    last_source_stream_sequence
) VALUES ($1, $2, 0, $3, $4, $5::numeric)
ON CONFLICT (lifecycle_key) DO NOTHING`,
		request.LifecycleKey,
		request.ReplayRootDLQRecordID,
		request.MaxAttempts,
		request.SourceDLQRecordID,
		strconv.FormatUint(request.SourceStreamSequence, 10),
	); err != nil {
		return replayReservation{}, fmt.Errorf("initialize replay state: %w", err)
	}

	var (
		storedRoot       string
		consumedAttempts int64
		maxAttempts      int64
	)
	if err := tx.QueryRowContext(ctx, `
SELECT replay_root_dlq_record_id, consumed_attempts, max_attempts
FROM system.normalization_dlq_replay_state
WHERE lifecycle_key = $1
FOR UPDATE`, request.LifecycleKey).Scan(
		&storedRoot,
		&consumedAttempts,
		&maxAttempts,
	); err != nil {
		return replayReservation{}, fmt.Errorf("lock replay state: %w", err)
	}

	if storedRoot != request.ReplayRootDLQRecordID ||
		maxAttempts != int64(request.MaxAttempts) {
		return replayReservation{}, fmt.Errorf(
			"%w: lifecycle %q does not match captured root/budget",
			errReplayStateConflict,
			request.LifecycleKey,
		)
	}
	if consumedAttempts < 0 || consumedAttempts > maxAttempts {
		return replayReservation{}, fmt.Errorf(
			"%w: invalid consumed/max state for lifecycle %q",
			errReplayStateConflict,
			request.LifecycleKey,
		)
	}

	var (
		existingAttempt        int64
		existingSourceSequence string
	)
	err = tx.QueryRowContext(ctx, `
SELECT attempt, source_stream_sequence::text
FROM system.normalization_dlq_replay_attempt
WHERE lifecycle_key = $1
  AND source_dlq_record_id = $2`,
		request.LifecycleKey,
		request.SourceDLQRecordID,
	).Scan(&existingAttempt, &existingSourceSequence)
	switch {
	case err == nil:
		if existingSourceSequence != strconv.FormatUint(request.SourceStreamSequence, 10) ||
			existingAttempt <= 0 ||
			existingAttempt > maxAttempts ||
			existingAttempt > consumedAttempts {
			return replayReservation{}, fmt.Errorf(
				"%w: source DLQ record %q conflicts with captured replay attempt",
				errReplayStateConflict,
				request.SourceDLQRecordID,
			)
		}
		return replayReservation{
			Reserved:              true,
			Attempt:               uint32(existingAttempt),
			MaxAttempts:           request.MaxAttempts,
			LifecycleKey:          request.LifecycleKey,
			ReplayRootDLQRecordID: request.ReplayRootDLQRecordID,
		}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return replayReservation{}, fmt.Errorf("load replay attempt ledger: %w", err)
	}

	if consumedAttempts >= maxAttempts {
		return replayReservation{
			Reserved:              false,
			Attempt:               uint32(consumedAttempts),
			MaxAttempts:           request.MaxAttempts,
			LifecycleKey:          request.LifecycleKey,
			ReplayRootDLQRecordID: request.ReplayRootDLQRecordID,
		}, nil
	}

	var reservedAttempt int64
	if err := tx.QueryRowContext(ctx, `
UPDATE system.normalization_dlq_replay_state
SET
    consumed_attempts = consumed_attempts + 1,
    first_reserved_at = COALESCE(first_reserved_at, now()),
    last_reserved_at = now(),
    last_source_dlq_record_id = $2,
    last_source_stream_sequence = $3::numeric,
    updated_at = now()
WHERE lifecycle_key = $1
RETURNING consumed_attempts`,
		request.LifecycleKey,
		request.SourceDLQRecordID,
		strconv.FormatUint(request.SourceStreamSequence, 10),
	).Scan(&reservedAttempt); err != nil {
		return replayReservation{}, fmt.Errorf("reserve replay attempt: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO system.normalization_dlq_replay_attempt (
    lifecycle_key,
    source_dlq_record_id,
    attempt,
    source_stream_sequence
) VALUES ($1, $2, $3, $4::numeric)`,
		request.LifecycleKey,
		request.SourceDLQRecordID,
		reservedAttempt,
		strconv.FormatUint(request.SourceStreamSequence, 10),
	); err != nil {
		return replayReservation{}, fmt.Errorf("record replay attempt ledger: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return replayReservation{}, fmt.Errorf("commit replay reservation: %w", err)
	}
	return replayReservation{
		Reserved:              true,
		Attempt:               uint32(reservedAttempt),
		MaxAttempts:           request.MaxAttempts,
		LifecycleKey:          request.LifecycleKey,
		ReplayRootDLQRecordID: request.ReplayRootDLQRecordID,
	}, nil
}

func replayReservationRequestFromDecision(
	decision replayDecision,
) (replayReservationRequest, error) {
	if !decision.Eligible {
		return replayReservationRequest{}, errors.New("replay decision is not eligible")
	}
	request := replayReservationRequest{
		LifecycleKey:          decision.LifecycleKey,
		ReplayRootDLQRecordID: decision.ReplayRootDLQRecordID,
		SourceDLQRecordID:     decision.SourceDLQRecordID,
		SourceStreamSequence:  decision.SourceStreamSequence,
		MaxAttempts:           decision.MaxAttempts,
	}
	if err := validateReplayReservationRequest(request); err != nil {
		return replayReservationRequest{}, err
	}
	return request, nil
}

func validateReplayReservationRequest(request replayReservationRequest) error {
	if strings.TrimSpace(request.ReplayRootDLQRecordID) == "" {
		return errors.New("replay root DLQ record id is required")
	}
	if request.LifecycleKey != replayLifecycleKey(request.ReplayRootDLQRecordID) {
		return errors.New("replay lifecycle key does not match replay root")
	}
	if strings.TrimSpace(request.SourceDLQRecordID) == "" {
		return errors.New("source DLQ record id is required")
	}
	if request.SourceStreamSequence == 0 {
		return errors.New("source stream sequence must be positive")
	}
	if request.MaxAttempts == 0 {
		return errors.New("replay max attempts must be positive")
	}
	return nil
}
