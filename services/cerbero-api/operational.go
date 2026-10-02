package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const developmentActorHeader = "X-Cerbero-Actor-ID"

var (
	errOperationalConflict = errors.New("operational concurrency conflict")
	errOperationalInvalid  = errors.New("invalid operational mutation")
)

type rowScanner interface {
	Scan(dest ...any) error
}

type entityView struct {
	EntityID     string          `json:"entity_id"`
	TenantID     string          `json:"tenant_id"`
	EntityType   string          `json:"entity_type"`
	CanonicalKey string          `json:"canonical_key"`
	FirstSeen    time.Time       `json:"first_seen"`
	LastSeen     time.Time       `json:"last_seen"`
	Criticality  string          `json:"criticality,omitempty"`
	Attributes   json.RawMessage `json:"attributes"`
	RiskScore    string          `json:"risk_score"`
}

type riskContributionView struct {
	ContributionID string     `json:"contribution_id"`
	TenantID       string     `json:"tenant_id"`
	EntityID       string     `json:"entity_id"`
	FindingID      string     `json:"finding_id"`
	Amount         string     `json:"amount"`
	Severity       string     `json:"severity"`
	Confidence     string     `json:"confidence,omitempty"`
	Reason         string     `json:"reason"`
	RuleID         string     `json:"rule_id,omitempty"`
	RuleVersion    string     `json:"rule_version,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty"`
}

type findingEntityView struct {
	FindingID  string    `json:"finding_id"`
	EntityID   string    `json:"entity_id"`
	Role       string    `json:"role"`
	Confidence string    `json:"confidence,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type incidentView struct {
	IncidentID             string     `json:"incident_id"`
	TenantID               string     `json:"tenant_id"`
	Status                 string     `json:"status"`
	Severity               string     `json:"severity"`
	Confidence             string     `json:"confidence,omitempty"`
	Title                  string     `json:"title"`
	Description            string     `json:"description"`
	FirstSeen              time.Time  `json:"first_seen"`
	LastSeen               time.Time  `json:"last_seen"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
	CorrelationReason      string     `json:"correlation_reason,omitempty"`
	CorrelationRuleID      string     `json:"correlation_rule_id,omitempty"`
	CorrelationRuleVersion string     `json:"correlation_rule_version,omitempty"`
	WindowStart            *time.Time `json:"window_start,omitempty"`
	WindowEnd              *time.Time `json:"window_end,omitempty"`
	OwnerUserID            string     `json:"owner_user_id,omitempty"`
	Disposition            string     `json:"disposition,omitempty"`
	Version                int64      `json:"version"`
	FindingIDs             []string   `json:"finding_ids"`
}

type caseView struct {
	CaseID        string     `json:"case_id"`
	TenantID      string     `json:"tenant_id"`
	CaseNumber    string     `json:"case_number"`
	Status        string     `json:"status"`
	Priority      string     `json:"priority"`
	Title         string     `json:"title"`
	Description   string     `json:"description"`
	OwnerUserID   string     `json:"owner_user_id,omitempty"`
	CreatedBy     string     `json:"created_by"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	ClosedAt      *time.Time `json:"closed_at,omitempty"`
	Disposition   string     `json:"disposition"`
	ClosureReason string     `json:"closure_reason,omitempty"`
	Version       int64      `json:"version"`
	IncidentIDs   []string   `json:"incident_ids"`
	FindingIDs    []string   `json:"finding_ids"`
	EntityIDs     []string   `json:"entity_ids"`
}

type auditEventView struct {
	AuditEventID string          `json:"audit_event_id"`
	TenantID     string          `json:"tenant_id"`
	EventType    string          `json:"event_type"`
	ActorType    string          `json:"actor_type"`
	ActorID      string          `json:"actor_id"`
	Action       string          `json:"action"`
	ObjectType   string          `json:"object_type"`
	ObjectID     string          `json:"object_id"`
	RequestID    string          `json:"request_id,omitempty"`
	OccurredAt   time.Time       `json:"occurred_at"`
	Reason       string          `json:"reason,omitempty"`
	BeforeState  json.RawMessage `json:"before_state"`
	AfterState   json.RawMessage `json:"after_state"`
	Result       string          `json:"result"`
	Metadata     json.RawMessage `json:"metadata"`
}

type timelineEntryView struct {
	Timestamp time.Time       `json:"timestamp"`
	EntryType string          `json:"entry_type"`
	ObjectID  string          `json:"object_id"`
	Title     string          `json:"title"`
	Actor     string          `json:"actor,omitempty"`
	Source    string          `json:"source"`
	Metadata  json.RawMessage `json:"metadata"`
}

type incidentPatch struct {
	ExpectedVersion int64   `json:"expected_version"`
	Status          *string `json:"status,omitempty"`
	Title           *string `json:"title,omitempty"`
	Description     *string `json:"description,omitempty"`
	OwnerUserID     *string `json:"owner_user_id,omitempty"`
	Disposition     *string `json:"disposition,omitempty"`
	Reason          string  `json:"reason,omitempty"`
}

type caseCreate struct {
	Title       string  `json:"title"`
	Description string  `json:"description,omitempty"`
	Priority    string  `json:"priority"`
	OwnerUserID *string `json:"owner_user_id,omitempty"`
	IncidentID  *string `json:"incident_id,omitempty"`
}

type casePatch struct {
	ExpectedVersion int64   `json:"expected_version"`
	Status          *string `json:"status,omitempty"`
	Priority        *string `json:"priority,omitempty"`
	Title           *string `json:"title,omitempty"`
	Description     *string `json:"description,omitempty"`
	OwnerUserID     *string `json:"owner_user_id,omitempty"`
	Disposition     *string `json:"disposition,omitempty"`
	ClosureReason   *string `json:"closure_reason,omitempty"`
	Reason          string  `json:"reason,omitempty"`
	ReopenReason    string  `json:"reopen_reason,omitempty"`
}

type operationalStore interface {
	listEntities(context.Context, uuid.UUID, int, uuid.UUID) ([]entityView, []uuid.UUID, error)
	getEntity(context.Context, uuid.UUID, uuid.UUID) (entityView, error)
	listEntityRiskContributions(context.Context, uuid.UUID, uuid.UUID) ([]riskContributionView, error)
	listFindingEntities(context.Context, uuid.UUID, uuid.UUID) ([]findingEntityView, bool, error)
	listIncidents(context.Context, uuid.UUID, int, uuid.UUID) ([]incidentView, []uuid.UUID, error)
	getIncident(context.Context, uuid.UUID, uuid.UUID) (incidentView, error)
	updateIncident(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, incidentPatch) (incidentView, error)
	createCase(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, caseCreate) (caseView, error)
	listCases(context.Context, uuid.UUID, int, uuid.UUID) ([]caseView, []uuid.UUID, error)
	getCase(context.Context, uuid.UUID, uuid.UUID) (caseView, error)
	updateCase(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, casePatch) (caseView, error)
	caseTimeline(context.Context, uuid.UUID, uuid.UUID) ([]timelineEntryView, error)
	listAudit(context.Context, uuid.UUID, string, string, int, uuid.UUID) ([]auditEventView, []uuid.UUID, error)
}

const incidentSelect = `
SELECT
    i.incident_id::text,
    i.tenant_id::text,
    i.status,
    i.severity,
    COALESCE(i.confidence, ''),
    i.title,
    i.description,
    i.first_seen,
    i.last_seen,
    i.created_at,
    i.updated_at,
    COALESCE(i.correlation_reason, ''),
    COALESCE(i.correlation_rule_id, ''),
    COALESCE(i.correlation_rule_version, ''),
    i.window_start,
    i.window_end,
    COALESCE(i.owner_user_id::text, ''),
    COALESCE(i.disposition, ''),
    i.version,
    ARRAY(
        SELECT relation.finding_id::text
        FROM investigation.incident_findings AS relation
        WHERE relation.tenant_id = i.tenant_id
          AND relation.incident_id = i.incident_id
        ORDER BY relation.finding_id
    )
FROM investigation.incidents AS i`

const caseSelect = `
SELECT
    c.case_id::text,
    c.tenant_id::text,
    c.case_number,
    c.status,
    c.priority,
    c.title,
    c.description,
    COALESCE(c.owner_user_id::text, ''),
    c.created_by::text,
    c.created_at,
    c.updated_at,
    c.closed_at,
    c.disposition,
    COALESCE(c.closure_reason, ''),
    c.version,
    COALESCE((
        SELECT jsonb_agg(relation.incident_id::text ORDER BY relation.incident_id)
        FROM investigation.case_incidents AS relation
        WHERE relation.tenant_id = c.tenant_id
          AND relation.case_id = c.case_id
    ), '[]'::jsonb),
    COALESCE((
        SELECT jsonb_agg(relation.finding_id::text ORDER BY relation.finding_id)
        FROM investigation.case_findings AS relation
        WHERE relation.tenant_id = c.tenant_id
          AND relation.case_id = c.case_id
    ), '[]'::jsonb),
    COALESCE((
        SELECT jsonb_agg(relation.entity_id::text ORDER BY relation.entity_id)
        FROM investigation.case_entities AS relation
        WHERE relation.tenant_id = c.tenant_id
          AND relation.case_id = c.case_id
    ), '[]'::jsonb)
FROM investigation.cases AS c`

func scanEntity(scanner rowScanner) (entityView, uuid.UUID, error) {
	var item entityView
	var id uuid.UUID
	var tenantID uuid.UUID
	var attributes []byte
	if err := scanner.Scan(
		&id,
		&tenantID,
		&item.EntityType,
		&item.CanonicalKey,
		&item.FirstSeen,
		&item.LastSeen,
		&item.Criticality,
		&attributes,
		&item.RiskScore,
	); err != nil {
		return entityView{}, uuid.Nil, err
	}
	item.EntityID = id.String()
	item.TenantID = tenantID.String()
	item.Attributes = json.RawMessage(attributes)
	return item, id, nil
}

func scanIncident(scanner rowScanner) (incidentView, uuid.UUID, error) {
	var item incidentView
	var id uuid.UUID
	if err := scanner.Scan(
		&item.IncidentID,
		&item.TenantID,
		&item.Status,
		&item.Severity,
		&item.Confidence,
		&item.Title,
		&item.Description,
		&item.FirstSeen,
		&item.LastSeen,
		&item.CreatedAt,
		&item.UpdatedAt,
		&item.CorrelationReason,
		&item.CorrelationRuleID,
		&item.CorrelationRuleVersion,
		&item.WindowStart,
		&item.WindowEnd,
		&item.OwnerUserID,
		&item.Disposition,
		&item.Version,
		&item.FindingIDs,
	); err != nil {
		return incidentView{}, uuid.Nil, err
	}
	parsed, err := uuid.Parse(item.IncidentID)
	if err != nil {
		return incidentView{}, uuid.Nil, err
	}
	id = parsed
	if item.FindingIDs == nil {
		item.FindingIDs = []string{}
	}
	return item, id, nil
}

func scanCase(scanner rowScanner) (caseView, uuid.UUID, error) {
	var item caseView
	var incidentIDsJSON json.RawMessage
	var findingIDsJSON json.RawMessage
	var entityIDsJSON json.RawMessage
	if err := scanner.Scan(
		&item.CaseID,
		&item.TenantID,
		&item.CaseNumber,
		&item.Status,
		&item.Priority,
		&item.Title,
		&item.Description,
		&item.OwnerUserID,
		&item.CreatedBy,
		&item.CreatedAt,
		&item.UpdatedAt,
		&item.ClosedAt,
		&item.Disposition,
		&item.ClosureReason,
		&item.Version,
		&incidentIDsJSON,
		&findingIDsJSON,
		&entityIDsJSON,
	); err != nil {
		return caseView{}, uuid.Nil, fmt.Errorf("scan Case row: %w", err)
	}
	if err := json.Unmarshal(incidentIDsJSON, &item.IncidentIDs); err != nil {
		return caseView{}, uuid.Nil, fmt.Errorf("decode Case incident_ids: %w", err)
	}
	if err := json.Unmarshal(findingIDsJSON, &item.FindingIDs); err != nil {
		return caseView{}, uuid.Nil, fmt.Errorf("decode Case finding_ids: %w", err)
	}
	if err := json.Unmarshal(entityIDsJSON, &item.EntityIDs); err != nil {
		return caseView{}, uuid.Nil, fmt.Errorf("decode Case entity_ids: %w", err)
	}
	parsed, err := uuid.Parse(item.CaseID)
	if err != nil {
		return caseView{}, uuid.Nil, fmt.Errorf("parse Case id: %w", err)
	}
	return item, parsed, nil
}

func (store *postgresStore) listEntities(ctx context.Context, tenantID uuid.UUID, limit int, after uuid.UUID) ([]entityView, []uuid.UUID, error) {
	rows, err := store.pool.Query(ctx, `
SELECT entity_id, tenant_id, entity_type, canonical_key, first_seen, last_seen,
       COALESCE(criticality, ''), attributes, risk_score::text
FROM risk.entities
WHERE tenant_id = $1
  AND ($2::uuid = '00000000-0000-0000-0000-000000000000'::uuid OR entity_id > $2)
ORDER BY entity_id
LIMIT $3`, tenantID, after, limit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	items := make([]entityView, 0)
	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		item, id, scanErr := scanEntity(rows)
		if scanErr != nil {
			return nil, nil, scanErr
		}
		items = append(items, item)
		ids = append(ids, id)
	}
	return items, ids, rows.Err()
}

func (store *postgresStore) getEntity(ctx context.Context, tenantID uuid.UUID, entityID uuid.UUID) (entityView, error) {
	item, _, err := scanEntity(store.pool.QueryRow(ctx, `
SELECT entity_id, tenant_id, entity_type, canonical_key, first_seen, last_seen,
       COALESCE(criticality, ''), attributes, risk_score::text
FROM risk.entities
WHERE tenant_id = $1 AND entity_id = $2`, tenantID, entityID))
	return item, err
}

func (store *postgresStore) listEntityRiskContributions(ctx context.Context, tenantID uuid.UUID, entityID uuid.UUID) ([]riskContributionView, error) {
	if _, err := store.getEntity(ctx, tenantID, entityID); err != nil {
		return nil, err
	}
	rows, err := store.pool.Query(ctx, `
SELECT contribution_id::text, tenant_id::text, entity_id::text, finding_id::text,
       amount::text, severity, COALESCE(confidence, ''), reason,
       COALESCE(rule_id, ''), COALESCE(rule_version, ''), created_at, expires_at
FROM risk.contributions
WHERE tenant_id = $1 AND entity_id = $2
ORDER BY created_at DESC, contribution_id DESC
LIMIT 200`, tenantID, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]riskContributionView, 0)
	for rows.Next() {
		var item riskContributionView
		if err := rows.Scan(
			&item.ContributionID,
			&item.TenantID,
			&item.EntityID,
			&item.FindingID,
			&item.Amount,
			&item.Severity,
			&item.Confidence,
			&item.Reason,
			&item.RuleID,
			&item.RuleVersion,
			&item.CreatedAt,
			&item.ExpiresAt,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (store *postgresStore) listFindingEntities(ctx context.Context, tenantID uuid.UUID, findingID uuid.UUID) ([]findingEntityView, bool, error) {
	var exists bool
	if err := store.pool.QueryRow(ctx, `
SELECT EXISTS(
    SELECT 1 FROM investigation.findings
    WHERE tenant_id = $1 AND finding_id = $2
)`, tenantID, findingID).Scan(&exists); err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	rows, err := store.pool.Query(ctx, `
SELECT finding_id::text, entity_id::text, role, COALESCE(confidence, ''), created_at
FROM investigation.finding_entities
WHERE tenant_id = $1 AND finding_id = $2
ORDER BY entity_id, role`, tenantID, findingID)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	items := make([]findingEntityView, 0)
	for rows.Next() {
		var item findingEntityView
		if err := rows.Scan(&item.FindingID, &item.EntityID, &item.Role, &item.Confidence, &item.CreatedAt); err != nil {
			return nil, false, err
		}
		items = append(items, item)
	}
	return items, true, rows.Err()
}

func (store *postgresStore) listIncidents(ctx context.Context, tenantID uuid.UUID, limit int, after uuid.UUID) ([]incidentView, []uuid.UUID, error) {
	query := incidentSelect + `
WHERE i.tenant_id = $1
  AND ($2::uuid = '00000000-0000-0000-0000-000000000000'::uuid OR i.incident_id > $2)
ORDER BY i.incident_id
LIMIT $3`
	rows, err := store.pool.Query(ctx, query, tenantID, after, limit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	items := make([]incidentView, 0)
	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		item, id, scanErr := scanIncident(rows)
		if scanErr != nil {
			return nil, nil, scanErr
		}
		items = append(items, item)
		ids = append(ids, id)
	}
	return items, ids, rows.Err()
}

func (store *postgresStore) getIncident(ctx context.Context, tenantID uuid.UUID, incidentID uuid.UUID) (incidentView, error) {
	query := incidentSelect + ` WHERE i.tenant_id = $1 AND i.incident_id = $2`
	item, _, err := scanIncident(store.pool.QueryRow(ctx, query, tenantID, incidentID))
	return item, err
}

func (store *postgresStore) updateIncident(ctx context.Context, tenantID, incidentID, actorID, requestID uuid.UUID, patch incidentPatch) (incidentView, error) {
	if patch.ExpectedVersion < 1 {
		return incidentView{}, fmt.Errorf("%w: expected_version must be >= 1", errOperationalInvalid)
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return incidentView{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := incidentSelect + ` WHERE i.tenant_id = $1 AND i.incident_id = $2 FOR UPDATE`
	before, _, err := scanIncident(tx.QueryRow(ctx, query, tenantID, incidentID))
	if err != nil {
		return incidentView{}, err
	}
	if before.Version != patch.ExpectedVersion {
		return incidentView{}, errOperationalConflict
	}

	after := before
	if patch.Status != nil {
		if !validIncidentStatus(*patch.Status) {
			return incidentView{}, fmt.Errorf("%w: unsupported incident status", errOperationalInvalid)
		}
		if *patch.Status != before.Status && !incidentTransitionAllowed(before.Status, *patch.Status) {
			return incidentView{}, fmt.Errorf("%w: invalid incident state transition", errOperationalInvalid)
		}
		after.Status = *patch.Status
	}
	if patch.Title != nil {
		if strings.TrimSpace(*patch.Title) == "" {
			return incidentView{}, fmt.Errorf("%w: title must not be empty", errOperationalInvalid)
		}
		after.Title = *patch.Title
	}
	if patch.Description != nil {
		after.Description = *patch.Description
	}
	if patch.OwnerUserID != nil {
		if *patch.OwnerUserID != "" {
			if parsed, parseErr := uuid.Parse(*patch.OwnerUserID); parseErr != nil || parsed.Version() != 7 {
				return incidentView{}, fmt.Errorf("%w: owner_user_id must be UUIDv7 or empty", errOperationalInvalid)
			}
		}
		after.OwnerUserID = *patch.OwnerUserID
	}
	if patch.Disposition != nil {
		after.Disposition = *patch.Disposition
	}
	after.Version = before.Version + 1

	if _, err = tx.Exec(ctx, `
UPDATE investigation.incidents
SET status = $3,
    title = $4,
    description = $5,
    owner_user_id = NULLIF($6, '')::uuid,
    disposition = NULLIF($7, ''),
    updated_at = now(),
    version = $8
WHERE tenant_id = $1 AND incident_id = $2`,
		tenantID,
		incidentID,
		after.Status,
		after.Title,
		after.Description,
		after.OwnerUserID,
		after.Disposition,
		after.Version,
	); err != nil {
		return incidentView{}, err
	}

	eventType := "incident.updated"
	action := "update"
	if after.Status != before.Status {
		eventType = "incident.status_changed"
		action = "status_changed"
	}
	if err := insertAuditTx(ctx, tx, tenantID, actorID, requestID, eventType, action, "incident", incidentID.String(), patch.Reason, before, after); err != nil {
		return incidentView{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return incidentView{}, err
	}
	return store.getIncident(ctx, tenantID, incidentID)
}

func (store *postgresStore) createCase(ctx context.Context, tenantID, actorID, requestID uuid.UUID, input caseCreate) (caseView, error) {
	if strings.TrimSpace(input.Title) == "" {
		return caseView{}, fmt.Errorf("%w: title is required", errOperationalInvalid)
	}
	if !validCasePriority(input.Priority) {
		return caseView{}, fmt.Errorf("%w: unsupported case priority", errOperationalInvalid)
	}
	owner := ""
	if input.OwnerUserID != nil {
		owner = *input.OwnerUserID
		if owner != "" {
			parsed, err := uuid.Parse(owner)
			if err != nil || parsed.Version() != 7 {
				return caseView{}, fmt.Errorf("%w: owner_user_id must be UUIDv7 or empty", errOperationalInvalid)
			}
		}
	}
	var incidentID uuid.UUID
	if input.IncidentID != nil {
		parsed, err := uuid.Parse(*input.IncidentID)
		if err != nil || parsed.Version() != 7 {
			return caseView{}, fmt.Errorf("%w: incident_id must be UUIDv7", errOperationalInvalid)
		}
		incidentID = parsed
	}

	caseID, err := uuid.NewV7()
	if err != nil {
		return caseView{}, err
	}
	compactID := strings.ReplaceAll(caseID.String(), "-", "")
	caseNumber := "CASE-" + strings.ToUpper(compactID[:12])

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return caseView{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if incidentID != uuid.Nil {
		var exists bool
		if err := tx.QueryRow(ctx, `
SELECT EXISTS(
    SELECT 1 FROM investigation.incidents
    WHERE tenant_id = $1 AND incident_id = $2
)`, tenantID, incidentID).Scan(&exists); err != nil {
			return caseView{}, fmt.Errorf("create Case incident lookup: %w", err)
		}
		if !exists {
			return caseView{}, pgx.ErrNoRows
		}
	}

	if _, err := tx.Exec(ctx, `
INSERT INTO investigation.cases(
    case_id, tenant_id, case_number, status, priority, title, description,
    owner_user_id, created_by, disposition, version
) VALUES ($1, $2, $3, 'OPEN', $4, $5, $6, NULLIF($7, '')::uuid, $8, 'UNDETERMINED', 1)`,
		caseID,
		tenantID,
		caseNumber,
		input.Priority,
		input.Title,
		input.Description,
		owner,
		actorID,
	); err != nil {
		return caseView{}, fmt.Errorf("create Case row insert: %w", err)
	}

	if incidentID != uuid.Nil {
		if _, err := tx.Exec(ctx, `
INSERT INTO investigation.case_incidents(tenant_id, case_id, incident_id, relation, added_by)
VALUES ($1::uuid, $2::uuid, $3::uuid, 'SOURCE_INCIDENT', $4::uuid)`, tenantID, caseID, incidentID, actorID); err != nil {
			return caseView{}, fmt.Errorf("create Case incident relation: %w", err)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO investigation.case_findings(tenant_id, case_id, finding_id, relation, added_by)
SELECT tenant_id, $2::uuid, finding_id, 'INCIDENT_FINDING', $3::uuid
FROM investigation.incident_findings
WHERE tenant_id = $1::uuid AND incident_id = $4::uuid
ON CONFLICT (case_id, finding_id) DO NOTHING`, tenantID, caseID, actorID, incidentID); err != nil {
			return caseView{}, fmt.Errorf("create Case finding relations: %w", err)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO investigation.case_entities(tenant_id, case_id, entity_id, relation, added_by)
SELECT DISTINCT relation.tenant_id, $2::uuid, relation.entity_id, 'FINDING_ENTITY', $3::uuid
FROM investigation.finding_entities AS relation
JOIN investigation.incident_findings AS incident_relation
  ON incident_relation.tenant_id = relation.tenant_id
 AND incident_relation.finding_id = relation.finding_id
WHERE relation.tenant_id = $1::uuid AND incident_relation.incident_id = $4::uuid
ON CONFLICT (case_id, entity_id) DO NOTHING`, tenantID, caseID, actorID, incidentID); err != nil {
			return caseView{}, fmt.Errorf("create Case entity relations: %w", err)
		}
	}

	after := map[string]any{
		"case_id":          caseID.String(),
		"case_number":      caseNumber,
		"status":           "OPEN",
		"priority":         input.Priority,
		"title":            input.Title,
		"incident_id":      input.IncidentID,
		"created_by":       actorID.String(),
		"expected_version": 1,
	}
	if err := insertAuditTx(ctx, tx, tenantID, actorID, requestID, "case.created", "create", "case", caseID.String(), "", nil, after); err != nil {
		return caseView{}, fmt.Errorf("create Case audit event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return caseView{}, fmt.Errorf("create Case commit: %w", err)
	}
	item, err := store.getCase(ctx, tenantID, caseID)
	if err != nil {
		return caseView{}, fmt.Errorf("create Case readback: %w", err)
	}
	return item, nil
}

func (store *postgresStore) listCases(ctx context.Context, tenantID uuid.UUID, limit int, after uuid.UUID) ([]caseView, []uuid.UUID, error) {
	query := caseSelect + `
WHERE c.tenant_id = $1
  AND ($2::uuid = '00000000-0000-0000-0000-000000000000'::uuid OR c.case_id > $2)
ORDER BY c.case_id
LIMIT $3`
	rows, err := store.pool.Query(ctx, query, tenantID, after, limit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	items := make([]caseView, 0)
	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		item, id, scanErr := scanCase(rows)
		if scanErr != nil {
			return nil, nil, scanErr
		}
		items = append(items, item)
		ids = append(ids, id)
	}
	return items, ids, rows.Err()
}

func (store *postgresStore) getCase(ctx context.Context, tenantID uuid.UUID, caseID uuid.UUID) (caseView, error) {
	query := caseSelect + ` WHERE c.tenant_id = $1 AND c.case_id = $2`
	item, _, err := scanCase(store.pool.QueryRow(ctx, query, tenantID, caseID))
	return item, err
}

func (store *postgresStore) updateCase(ctx context.Context, tenantID, caseID, actorID, requestID uuid.UUID, patch casePatch) (caseView, error) {
	if patch.ExpectedVersion < 1 {
		return caseView{}, fmt.Errorf("%w: expected_version must be >= 1", errOperationalInvalid)
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return caseView{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := caseSelect + ` WHERE c.tenant_id = $1 AND c.case_id = $2 FOR UPDATE`
	before, _, err := scanCase(tx.QueryRow(ctx, query, tenantID, caseID))
	if err != nil {
		return caseView{}, err
	}
	if before.Version != patch.ExpectedVersion {
		return caseView{}, errOperationalConflict
	}

	after := before
	if patch.Status != nil {
		if !validCaseStatus(*patch.Status) {
			return caseView{}, fmt.Errorf("%w: unsupported case status", errOperationalInvalid)
		}
		if *patch.Status != before.Status && !caseTransitionAllowed(before.Status, *patch.Status) {
			return caseView{}, fmt.Errorf("%w: invalid case state transition", errOperationalInvalid)
		}
		after.Status = *patch.Status
	}
	if patch.Priority != nil {
		if !validCasePriority(*patch.Priority) {
			return caseView{}, fmt.Errorf("%w: unsupported case priority", errOperationalInvalid)
		}
		after.Priority = *patch.Priority
	}
	if patch.Title != nil {
		if strings.TrimSpace(*patch.Title) == "" {
			return caseView{}, fmt.Errorf("%w: title must not be empty", errOperationalInvalid)
		}
		after.Title = *patch.Title
	}
	if patch.Description != nil {
		after.Description = *patch.Description
	}
	if patch.OwnerUserID != nil {
		if *patch.OwnerUserID != "" {
			parsed, parseErr := uuid.Parse(*patch.OwnerUserID)
			if parseErr != nil || parsed.Version() != 7 {
				return caseView{}, fmt.Errorf("%w: owner_user_id must be UUIDv7 or empty", errOperationalInvalid)
			}
		}
		after.OwnerUserID = *patch.OwnerUserID
	}
	if patch.Disposition != nil {
		if !validCaseDisposition(*patch.Disposition) {
			return caseView{}, fmt.Errorf("%w: unsupported case disposition", errOperationalInvalid)
		}
		after.Disposition = *patch.Disposition
	}
	if patch.ClosureReason != nil {
		after.ClosureReason = *patch.ClosureReason
	}

	now := time.Now().UTC()
	if after.Status == "CLOSED" && before.Status != "CLOSED" {
		if after.Disposition == "UNDETERMINED" || strings.TrimSpace(after.ClosureReason) == "" {
			return caseView{}, fmt.Errorf("%w: closing a Case requires disposition and closure_reason", errOperationalInvalid)
		}
		after.ClosedAt = &now
	}
	if before.Status == "CLOSED" && after.Status == "INVESTIGATING" {
		if strings.TrimSpace(patch.ReopenReason) == "" {
			return caseView{}, fmt.Errorf("%w: reopening a Case requires reopen_reason", errOperationalInvalid)
		}
		after.ClosedAt = nil
		after.Disposition = "UNDETERMINED"
		after.ClosureReason = ""
	}
	after.Version = before.Version + 1

	if _, err := tx.Exec(ctx, `
UPDATE investigation.cases
SET status = $3,
    priority = $4,
    title = $5,
    description = $6,
    owner_user_id = NULLIF($7, '')::uuid,
    disposition = $8,
    closure_reason = NULLIF($9, ''),
    closed_at = $10,
    updated_at = now(),
    version = $11
WHERE tenant_id = $1 AND case_id = $2`,
		tenantID,
		caseID,
		after.Status,
		after.Priority,
		after.Title,
		after.Description,
		after.OwnerUserID,
		after.Disposition,
		after.ClosureReason,
		after.ClosedAt,
		after.Version,
	); err != nil {
		return caseView{}, err
	}

	eventType := "case.updated"
	action := "update"
	reason := patch.Reason
	switch {
	case before.Status == "CLOSED" && after.Status == "INVESTIGATING":
		eventType = "case.reopened"
		action = "reopen"
		reason = patch.ReopenReason
	case before.Status != "CLOSED" && after.Status == "CLOSED":
		eventType = "case.closed"
		action = "close"
		if reason == "" {
			reason = after.ClosureReason
		}
	case before.Status != after.Status:
		eventType = "case.status_changed"
		action = "status_changed"
	case before.OwnerUserID != after.OwnerUserID:
		eventType = "case.owner_changed"
		action = "owner_changed"
	}
	if err := insertAuditTx(ctx, tx, tenantID, actorID, requestID, eventType, action, "case", caseID.String(), reason, before, after); err != nil {
		return caseView{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return caseView{}, err
	}
	return store.getCase(ctx, tenantID, caseID)
}

func (store *postgresStore) caseTimeline(ctx context.Context, tenantID, caseID uuid.UUID) ([]timelineEntryView, error) {
	if _, err := store.getCase(ctx, tenantID, caseID); err != nil {
		return nil, err
	}
	rows, err := store.pool.Query(ctx, `
SELECT timestamp, entry_type, object_id, title, actor, source, metadata
FROM (
    SELECT
        incident.created_at AS timestamp,
        'INCIDENT'::text AS entry_type,
        incident.incident_id::text AS object_id,
        incident.title,
        ''::text AS actor,
        'case_incidents'::text AS source,
        jsonb_build_object('status', incident.status, 'version', incident.version) AS metadata
    FROM investigation.case_incidents AS relation
    JOIN investigation.incidents AS incident
      ON incident.tenant_id = relation.tenant_id
     AND incident.incident_id = relation.incident_id
    WHERE relation.tenant_id = $1 AND relation.case_id = $2

    UNION ALL

    SELECT
        finding.created_at,
        'FINDING'::text,
        finding.finding_id::text,
        'Finding ' || finding.finding_id::text,
        ''::text,
        'case_findings'::text,
        '{}'::jsonb
    FROM investigation.case_findings AS relation
    JOIN investigation.findings AS finding
      ON finding.tenant_id = relation.tenant_id
     AND finding.finding_id = relation.finding_id
    WHERE relation.tenant_id = $1 AND relation.case_id = $2

    UNION ALL

    SELECT
        entity.first_seen,
        'ENTITY'::text,
        entity.entity_id::text,
        entity.canonical_key,
        ''::text,
        'case_entities'::text,
        jsonb_build_object('entity_type', entity.entity_type, 'risk_score', entity.risk_score)
    FROM investigation.case_entities AS relation
    JOIN risk.entities AS entity
      ON entity.tenant_id = relation.tenant_id
     AND entity.entity_id = relation.entity_id
    WHERE relation.tenant_id = $1 AND relation.case_id = $2

    UNION ALL

    SELECT
        event.occurred_at,
        'AUDIT'::text,
        event.audit_event_id::text,
        event.event_type,
        event.actor_id,
        'audit.events'::text,
        jsonb_build_object('action', event.action, 'result', event.result)
    FROM audit.events AS event
    WHERE event.tenant_id = $1
      AND event.object_type = 'case'
      AND event.object_id = $2::uuid::text
) AS timeline
ORDER BY timestamp, entry_type, object_id`, tenantID, caseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]timelineEntryView, 0)
	for rows.Next() {
		var item timelineEntryView
		var metadata []byte
		if err := rows.Scan(&item.Timestamp, &item.EntryType, &item.ObjectID, &item.Title, &item.Actor, &item.Source, &metadata); err != nil {
			return nil, err
		}
		item.Metadata = json.RawMessage(metadata)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (store *postgresStore) listAudit(ctx context.Context, tenantID uuid.UUID, objectType, objectID string, limit int, after uuid.UUID) ([]auditEventView, []uuid.UUID, error) {
	rows, err := store.pool.Query(ctx, `
SELECT audit_event_id, tenant_id::text, event_type, actor_type, actor_id, action,
       object_type, object_id, COALESCE(request_id::text, ''), occurred_at,
       COALESCE(reason, ''), COALESCE(before_state, 'null'::jsonb),
       COALESCE(after_state, 'null'::jsonb), result, metadata
FROM audit.events
WHERE tenant_id = $1
  AND ($2 = '' OR object_type = $2)
  AND ($3 = '' OR object_id = $3)
  AND ($4::uuid = '00000000-0000-0000-0000-000000000000'::uuid OR audit_event_id > $4)
ORDER BY audit_event_id
LIMIT $5`, tenantID, objectType, objectID, after, limit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	items := make([]auditEventView, 0)
	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		var item auditEventView
		var id uuid.UUID
		var before []byte
		var afterState []byte
		var metadata []byte
		if err := rows.Scan(
			&id,
			&item.TenantID,
			&item.EventType,
			&item.ActorType,
			&item.ActorID,
			&item.Action,
			&item.ObjectType,
			&item.ObjectID,
			&item.RequestID,
			&item.OccurredAt,
			&item.Reason,
			&before,
			&afterState,
			&item.Result,
			&metadata,
		); err != nil {
			return nil, nil, err
		}
		item.AuditEventID = id.String()
		item.BeforeState = json.RawMessage(before)
		item.AfterState = json.RawMessage(afterState)
		item.Metadata = json.RawMessage(metadata)
		items = append(items, item)
		ids = append(ids, id)
	}
	return items, ids, rows.Err()
}

func insertAuditTx(ctx context.Context, tx pgx.Tx, tenantID, actorID, requestID uuid.UUID, eventType, action, objectType, objectID, reason string, before, after any) error {
	auditID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	beforeJSON, err := json.Marshal(before)
	if err != nil {
		return err
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
INSERT INTO audit.events(
    audit_event_id, tenant_id, event_type, actor_type, actor_id, action,
    object_type, object_id, request_id, reason, before_state, after_state,
    result, metadata
) VALUES (
    $1, $2, $3, 'DEVELOPMENT_USER', $4, $5,
    $6, $7, $8, NULLIF($9, ''), $10::jsonb, $11::jsonb,
    'SUCCESS', '{}'::jsonb
)`, auditID, tenantID, eventType, actorID.String(), action, objectType, objectID, requestID, reason, string(beforeJSON), string(afterJSON))
	return err
}

func validIncidentStatus(status string) bool {
	switch status {
	case "OPEN", "TRIAGED", "INVESTIGATING", "CONTAINED", "RESOLVED", "CLOSED", "INVALIDATED":
		return true
	default:
		return false
	}
}

func incidentTransitionAllowed(from, to string) bool {
	allowed := map[string]map[string]bool{
		"OPEN":          {"TRIAGED": true, "INVALIDATED": true},
		"TRIAGED":       {"INVESTIGATING": true, "RESOLVED": true, "INVALIDATED": true},
		"INVESTIGATING": {"CONTAINED": true, "RESOLVED": true, "INVALIDATED": true},
		"CONTAINED":     {"RESOLVED": true},
		"RESOLVED":      {"CLOSED": true, "INVESTIGATING": true},
		"CLOSED":        {"INVESTIGATING": true},
		"INVALIDATED":   {},
	}
	return allowed[from][to]
}

func validCaseStatus(status string) bool {
	switch status {
	case "OPEN", "TRIAGE", "INVESTIGATING", "ON_HOLD", "RESPONSE", "RESOLVED", "CLOSED":
		return true
	default:
		return false
	}
}

func caseTransitionAllowed(from, to string) bool {
	allowed := map[string]map[string]bool{
		"OPEN":          {"TRIAGE": true},
		"TRIAGE":        {"INVESTIGATING": true, "ON_HOLD": true, "RESOLVED": true},
		"INVESTIGATING": {"ON_HOLD": true, "RESPONSE": true, "RESOLVED": true},
		"ON_HOLD":       {"TRIAGE": true, "INVESTIGATING": true, "RESPONSE": true},
		"RESPONSE":      {"ON_HOLD": true, "RESOLVED": true},
		"RESOLVED":      {"CLOSED": true, "INVESTIGATING": true},
		"CLOSED":        {"INVESTIGATING": true},
	}
	return allowed[from][to]
}

func validCasePriority(priority string) bool {
	switch priority {
	case "P1_CRITICAL", "P2_HIGH", "P3_MEDIUM", "P4_LOW":
		return true
	default:
		return false
	}
}

func validCaseDisposition(disposition string) bool {
	switch disposition {
	case "UNDETERMINED", "CONFIRMED_INCIDENT", "BENIGN_ACTIVITY", "FALSE_POSITIVE", "DUPLICATE", "TEST", "OTHER":
		return true
	default:
		return false
	}
}

func (server *apiServer) listEntities(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	limit, after, ok := operationalListParams(w, r)
	if !ok {
		return
	}
	items, ids, err := server.operational.listEntities(r.Context(), tenantID, limit+1, after)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeOperationalPage(w, items, ids, limit)
}

func (server *apiServer) getEntity(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	entityID, ok := pathUUID(w, r)
	if !ok {
		return
	}
	item, err := server.operational.getEntity(r.Context(), tenantID, entityID)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (server *apiServer) listEntityRiskContributions(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	entityID, ok := pathUUID(w, r)
	if !ok {
		return
	}
	items, err := server.operational.listEntityRiskContributions(r.Context(), tenantID, entityID)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (server *apiServer) listFindingEntities(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	findingID, ok := pathUUID(w, r)
	if !ok {
		return
	}
	items, found, err := server.operational.listFindingEntities(r.Context(), tenantID, findingID)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (server *apiServer) listIncidents(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	limit, after, ok := operationalListParams(w, r)
	if !ok {
		return
	}
	items, ids, err := server.operational.listIncidents(r.Context(), tenantID, limit+1, after)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeOperationalPage(w, items, ids, limit)
}

func (server *apiServer) getIncident(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	incidentID, ok := pathUUID(w, r)
	if !ok {
		return
	}
	item, err := server.operational.getIncident(r.Context(), tenantID, incidentID)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (server *apiServer) patchIncident(w http.ResponseWriter, r *http.Request) {
	tenantID, actorID, requestID, ok := mutationIdentity(w, r)
	if !ok {
		return
	}
	incidentID, ok := pathUUID(w, r)
	if !ok {
		return
	}
	var patch incidentPatch
	if !decodeJSONBody(w, r, &patch) {
		return
	}
	item, err := server.operational.updateIncident(r.Context(), tenantID, incidentID, actorID, requestID, patch)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (server *apiServer) createCase(w http.ResponseWriter, r *http.Request) {
	tenantID, actorID, requestID, ok := mutationIdentity(w, r)
	if !ok {
		return
	}
	var input caseCreate
	if !decodeJSONBody(w, r, &input) {
		return
	}
	item, err := server.operational.createCase(r.Context(), tenantID, actorID, requestID, input)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, item)
}

func (server *apiServer) listCases(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	limit, after, ok := operationalListParams(w, r)
	if !ok {
		return
	}
	items, ids, err := server.operational.listCases(r.Context(), tenantID, limit+1, after)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeOperationalPage(w, items, ids, limit)
}

func (server *apiServer) getCase(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	caseID, ok := pathUUID(w, r)
	if !ok {
		return
	}
	item, err := server.operational.getCase(r.Context(), tenantID, caseID)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (server *apiServer) patchCase(w http.ResponseWriter, r *http.Request) {
	tenantID, actorID, requestID, ok := mutationIdentity(w, r)
	if !ok {
		return
	}
	caseID, ok := pathUUID(w, r)
	if !ok {
		return
	}
	var patch casePatch
	if !decodeJSONBody(w, r, &patch) {
		return
	}
	item, err := server.operational.updateCase(r.Context(), tenantID, caseID, actorID, requestID, patch)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (server *apiServer) getCaseTimeline(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	caseID, ok := pathUUID(w, r)
	if !ok {
		return
	}
	items, err := server.operational.caseTimeline(r.Context(), tenantID, caseID)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (server *apiServer) listAudit(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	limit, after, ok := operationalListParams(w, r)
	if !ok {
		return
	}
	objectType := r.URL.Query().Get("object_type")
	objectID := r.URL.Query().Get("object_id")
	if objectID != "" && objectType == "" {
		http.Error(w, "object_id requires object_type", http.StatusBadRequest)
		return
	}
	items, ids, err := server.operational.listAudit(r.Context(), tenantID, objectType, objectID, limit+1, after)
	if err != nil {
		operationalError(w, r, err)
		return
	}
	writeOperationalPage(w, items, ids, limit)
}

func mutationIdentity(w http.ResponseWriter, r *http.Request) (uuid.UUID, uuid.UUID, uuid.UUID, bool) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return uuid.Nil, uuid.Nil, uuid.Nil, false
	}
	actorID, err := uuid.Parse(r.Header.Get(developmentActorHeader))
	if err != nil || actorID.Version() != 7 {
		http.Error(w, "valid UUIDv7 development actor is required", http.StatusUnauthorized)
		return uuid.Nil, uuid.Nil, uuid.Nil, false
	}
	requestID, err := uuid.Parse(requestIDFromContext(r.Context()))
	if err != nil || requestID.Version() != 7 {
		http.Error(w, "request id failure", http.StatusInternalServerError)
		return uuid.Nil, uuid.Nil, uuid.Nil, false
	}
	return tenantID, actorID, requestID, true
}

func pathUUID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil || id.Version() != 7 {
		http.Error(w, "valid UUIDv7 id is required", http.StatusBadRequest)
		return uuid.Nil, false
	}
	return id, true
}

func operationalListParams(w http.ResponseWriter, r *http.Request) (int, uuid.UUID, bool) {
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 200 {
			http.Error(w, "limit must be 1..200", http.StatusBadRequest)
			return 0, uuid.Nil, false
		}
		limit = value
	}
	after, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, "invalid cursor", http.StatusBadRequest)
		return 0, uuid.Nil, false
	}
	return limit, after, true
}

func writeOperationalPage[T any](w http.ResponseWriter, items []T, ids []uuid.UUID, limit int) {
	next := ""
	if len(items) > limit {
		items = items[:limit]
		ids = ids[:limit]
		next = base64.RawURLEncoding.EncodeToString([]byte(ids[len(ids)-1].String()))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, destination any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(w, "JSON body must contain one object", http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func operationalError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		http.NotFound(w, r)
	case errors.Is(err, errOperationalConflict):
		http.Error(w, "expected_version does not match current version", http.StatusConflict)
	case errors.Is(err, errOperationalInvalid):
		http.Error(w, strings.TrimPrefix(err.Error(), errOperationalInvalid.Error()+": "), http.StatusBadRequest)
	default:
		fmt.Printf("STEP31_OPERATIONAL_ERROR method=%s path=%s error=%v\n", r.Method, r.URL.Path, err)
		http.Error(w, "operational query failed", http.StatusInternalServerError)
	}
}
