package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	contractsv1 "cerbero/services/internal/contracts/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const tenantHeader = "X-Cerbero-Tenant-ID"

type analyticalStore interface {
	list(context.Context, string, uuid.UUID, int, uuid.UUID) ([][]byte, []uuid.UUID, error)
	get(context.Context, string, uuid.UUID, uuid.UUID) ([]byte, error)
}
type postgresStore struct{ pool *pgxpool.Pool }

func newPostgresStore(ctx context.Context, dsn string) (*postgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &postgresStore{pool: pool}, nil
}
func (store *postgresStore) list(ctx context.Context, kind string, tenant uuid.UUID, limit int, after uuid.UUID) ([][]byte, []uuid.UUID, error) {
	table, idColumn, err := tableFor(kind)
	if err != nil {
		return nil, nil, err
	}
	query := "SELECT " + idColumn + ", payload FROM " + table + " WHERE tenant_id = $1 AND ($2::uuid = '00000000-0000-0000-0000-000000000000'::uuid OR " + idColumn + " > $2) ORDER BY " + idColumn + " LIMIT $3"
	rows, err := store.pool.Query(ctx, query, tenant, after, limit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var payloads [][]byte
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		var payload []byte
		if err := rows.Scan(&id, &payload); err != nil {
			return nil, nil, err
		}
		ids = append(ids, id)
		payloads = append(payloads, payload)
	}
	return payloads, ids, rows.Err()
}
func (store *postgresStore) get(ctx context.Context, kind string, tenant uuid.UUID, id uuid.UUID) ([]byte, error) {
	table, idColumn, err := tableFor(kind)
	if err != nil {
		return nil, err
	}
	var payload []byte
	err = store.pool.QueryRow(ctx, "SELECT payload FROM "+table+" WHERE tenant_id = $1 AND "+idColumn+" = $2", tenant, id).Scan(&payload)
	return payload, err
}
func tableFor(kind string) (string, string, error) {
	switch kind {
	case "signals":
		return "detection.signals", "signal_id", nil
	case "findings":
		return "investigation.findings", "finding_id", nil
	default:
		return "", "", errors.New("unsupported kind")
	}
}

type apiServer struct {
	store       analyticalStore
	operational operationalStore
}

func newAPIServer(store analyticalStore) http.Handler {
	server := &apiServer{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/signals", server.list("signals"))
	mux.HandleFunc("GET /api/v1/signals/{id}", server.get("signals"))
	mux.HandleFunc("GET /api/v1/findings", server.list("findings"))
	mux.HandleFunc("GET /api/v1/findings/{id}", server.get("findings"))
	if operational, ok := store.(operationalStore); ok {
		server.operational = operational
		mux.HandleFunc("GET /api/v1/entities", server.listEntities)
		mux.HandleFunc("GET /api/v1/entities/{id}", server.getEntity)
		mux.HandleFunc("GET /api/v1/entities/{id}/risk-contributions", server.listEntityRiskContributions)
		mux.HandleFunc("GET /api/v1/findings/{id}/entities", server.listFindingEntities)
		mux.HandleFunc("GET /api/v1/incidents", server.listIncidents)
		mux.HandleFunc("GET /api/v1/incidents/{id}", server.getIncident)
		mux.HandleFunc("PATCH /api/v1/incidents/{id}", server.patchIncident)
		mux.HandleFunc("POST /api/v1/cases", server.createCase)
		mux.HandleFunc("GET /api/v1/cases", server.listCases)
		mux.HandleFunc("GET /api/v1/cases/{id}", server.getCase)
		mux.HandleFunc("PATCH /api/v1/cases/{id}", server.patchCase)
		mux.HandleFunc("GET /api/v1/cases/{id}/timeline", server.getCaseTimeline)
		mux.HandleFunc("GET /api/v1/audit", server.listAudit)
	}
	return requestID(mux)
}

type requestIDContextKey struct{}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if _, err := uuid.Parse(id); err != nil {
			generated, genErr := uuid.NewV7()
			if genErr != nil {
				http.Error(w, "request id failure", 500)
				return
			}
			id = generated.String()
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDContextKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDContextKey{}).(string)
	return id
}
func (server *apiServer) list(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := tenant(w, r)
		if !ok {
			return
		}
		limit := 50
		if raw := r.URL.Query().Get("limit"); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value < 1 || value > 200 {
				http.Error(w, "limit must be 1..200", 400)
				return
			}
			limit = value
		}
		after, err := decodeCursor(r.URL.Query().Get("cursor"))
		if err != nil {
			http.Error(w, "invalid cursor", 400)
			return
		}
		payloads, ids, err := server.store.list(r.Context(), kind, tenant, limit+1, after)
		if err != nil {
			http.Error(w, "query failed", 500)
			return
		}
		next := ""
		if len(payloads) > limit {
			payloads = payloads[:limit]
			ids = ids[:limit]
			next = base64.RawURLEncoding.EncodeToString([]byte(ids[len(ids)-1].String()))
		}
		items := make([]json.RawMessage, 0, len(payloads))
		for _, payload := range payloads {
			item, err := payloadJSON(kind, payload)
			if err != nil {
				http.Error(w, "invalid stored object", 500)
				return
			}
			items = append(items, item)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "next_cursor": next})
	}
}
func (server *apiServer) get(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := tenant(w, r)
		if !ok {
			return
		}
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			http.Error(w, "invalid id", 400)
			return
		}
		payload, err := server.store.get(r.Context(), kind, tenant, id)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		body, err := payloadJSON(kind, payload)
		if err != nil {
			http.Error(w, "invalid stored object", 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
}
func tenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.Header.Get(tenantHeader))
	if err != nil || id.Version() != 7 {
		http.Error(w, "valid UUIDv7 tenant is required", http.StatusUnauthorized)
		return uuid.Nil, false
	}
	return id, true
}
func decodeCursor(raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, nil
	}
	value, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(string(value))
}
func payloadJSON(kind string, payload []byte) ([]byte, error) {
	options := protojson.MarshalOptions{UseProtoNames: true}
	switch kind {
	case "signals":
		message := &contractsv1.Signal{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return nil, err
		}
		return options.Marshal(message)
	case "findings":
		message := &contractsv1.Finding{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return nil, err
		}
		return options.Marshal(message)
	default:
		return nil, errors.New("unsupported kind")
	}
}
