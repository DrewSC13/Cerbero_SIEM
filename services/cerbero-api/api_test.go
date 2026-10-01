package main

import (
	contractsv1 "cerbero/services/internal/contracts/v1"
	"context"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeStore struct{ signal []byte }

func (store fakeStore) list(context.Context, string, uuid.UUID, int, uuid.UUID) ([][]byte, []uuid.UUID, error) {
	id := uuid.MustParse("018f47a2-4b00-7a00-8000-00000000d101")
	return [][]byte{store.signal}, []uuid.UUID{id}, nil
}
func (store fakeStore) get(context.Context, string, uuid.UUID, uuid.UUID) ([]byte, error) {
	return store.signal, nil
}
func TestTenantScopeAndCanonicalSignalRead(t *testing.T) {
	payload, err := proto.Marshal(&contractsv1.Signal{SignalId: "018f47a2-4b00-7a00-8000-00000000d101", TenantId: "018f47a2-4b00-7a00-8000-00000000d001", RuleId: "CER-DET-000101"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newAPIServer(fakeStore{signal: payload})
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, httptest.NewRequest(http.MethodGet, "/api/v1/signals", nil))
	if denied.Code != http.StatusUnauthorized {
		t.Fatalf("expected deny, got %d", denied.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/signals", nil)
	req.Header.Set(tenantHeader, "018f47a2-4b00-7a00-8000-00000000d001")
	ok := httptest.NewRecorder()
	handler.ServeHTTP(ok, req)
	if ok.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", ok.Code, ok.Body.String())
	}
	if ok.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing request id")
	}
}
