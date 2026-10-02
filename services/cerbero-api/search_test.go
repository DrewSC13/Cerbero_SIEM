package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeEventSearchStore struct {
	page searchPage
	item eventView
	last preparedSearch
}

func (store *fakeEventSearchStore) search(_ context.Context, _ uuid.UUID, prepared preparedSearch) (searchPage, error) {
	store.last = prepared
	return store.page, nil
}

func (store *fakeEventSearchStore) getEvent(_ context.Context, _ uuid.UUID, _ uuid.UUID) (eventView, error) {
	return store.item, nil
}

func TestSearchParserCompilesControlledCanonicalFields(t *testing.T) {
	expression, err := parseQuery(`user.name == "jdoe" AND (severity_id >= 3 OR src_endpoint.ip starts_with "10.")`)
	if err != nil {
		t.Fatal(err)
	}
	query, parameters, err := compileExpression(expression)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"JSONExtractString(ocsf_event_json, 'user', 'name') = {q0:String}",
		"severity >= {q1:Float64}",
		"startsWith(JSONExtractString(ocsf_event_json, 'src_endpoint', 'ip'), {q2:String})",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("compiled query missing %q: %s", expected, query)
		}
	}
	if parameters.Get("param_q0") != "jdoe" || parameters.Get("param_q1") != "3" || parameters.Get("param_q2") != "10." {
		t.Fatalf("unexpected query parameters: %#v", parameters)
	}
}

func TestSearchParserRejectsUnknownFieldTypeMismatchAndInjection(t *testing.T) {
	cases := []string{
		`physical_column == "secret"`,
		`event_time contains "powershell"`,
		`severity_id contains "high"`,
		`user.name == "x"; SELECT 1`,
		`normalized_event_id == "not-a-uuid"`,
	}
	for _, raw := range cases {
		expression, err := parseQuery(raw)
		if err == nil {
			_, _, err = compileExpression(expression)
		}
		if err == nil {
			t.Fatalf("expected query %q to be rejected", raw)
		}
	}
}

func TestPrepareSearchRequiresBoundedTimeRangeAndOpaqueCursorBinding(t *testing.T) {
	base := searchRequest{
		Query: `user.name == "jdoe"`,
		TimeRange: searchTimeRange{
			From: "2033-05-18T03:33:00Z",
			To:   "2033-05-18T03:34:00Z",
		},
		Limit: 1,
	}
	prepared, err := prepareSearch(base)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := encodeSearchCursor(searchCursor{
		Seconds:     2_000_000_000,
		Nanos:       0,
		EventID:     "018f47a2-4b00-7a00-8000-00000000f101",
		Direction:   prepared.Direction,
		Fingerprint: prepared.Fingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	base.Cursor = cursor
	if _, err := prepareSearch(base); err != nil {
		t.Fatalf("expected matching cursor to validate: %v", err)
	}
	base.Query = `user.name == "other"`
	if _, err := prepareSearch(base); err == nil || !strings.Contains(err.Error(), "cursor does not belong") {
		t.Fatalf("expected cursor/query mismatch, got %v", err)
	}
	if _, err := prepareSearch(searchRequest{}); err == nil {
		t.Fatal("missing time range must fail")
	}
	tooWide := searchRequest{TimeRange: searchTimeRange{From: "2033-05-18T00:00:00Z", To: "2033-05-20T00:00:00Z"}}
	if _, err := prepareSearch(tooWide); err == nil {
		t.Fatal("unbounded time range must fail")
	}
}

func TestEventLookupUsesCanonicalUUIDStringComparison(t *testing.T) {
	query := eventLookupQuery("cerbero")
	if strings.Contains(query, "{event_id:UUID}") || strings.Contains(query, "toUUID(") {
		t.Fatalf("event lookup must use the proven String parameter path: %s", query)
	}
	if !strings.Contains(query, "toString(normalized_event_id) = {event_id:String}") {
		t.Fatalf("event lookup must compare canonical UUID strings: %s", query)
	}
}

func TestCursorKeysetUsesCanonicalUUIDStringTieBreaker(t *testing.T) {
	for _, direction := range []string{"asc", "desc"} {
		predicate := cursorKeysetPredicate(direction)
		if strings.Contains(predicate, "tuple(") {
			t.Fatalf("cursor predicate must not depend on UUID tuple ordering: %s", predicate)
		}
		if !strings.Contains(predicate, "toString(normalized_event_id)") ||
			!strings.Contains(predicate, "{cursor_id:String}") {
			t.Fatalf("cursor predicate must use canonical UUID string tie-breaker: %s", predicate)
		}
	}
}

func TestSearchHTTPEnforcesTenantAndReturnsExecutionMetadata(t *testing.T) {
	store := &fakeEventSearchStore{page: searchPage{
		Items: []eventView{{
			NormalizedEventID: "018f47a2-4b00-7a00-8000-00000000f101",
			TenantID:          "018f47a2-4b00-7a00-8000-00000000f001",
			OCSFEvent:         json.RawMessage(`{"user":{"name":"jdoe"}}`),
		}},
	}}
	handler := newAPIServerWithSearch(fakeStore{}, store)
	body := `{"query":"user.name == \"jdoe\"","time_range":{"from":"2033-05-18T03:33:00Z","to":"2033-05-18T03:34:00Z"},"limit":10}`

	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, httptest.NewRequest(http.MethodPost, "/api/v1/search", strings.NewReader(body)))
	if denied.Code != http.StatusUnauthorized {
		t.Fatalf("expected tenant denial, got %d", denied.Code)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/search", strings.NewReader(body))
	request.Header.Set(tenantHeader, "018f47a2-4b00-7a00-8000-00000000f001")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing request id")
	}
	var decoded searchResponse
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Items) != 1 || decoded.Execution.QueryID == "" || decoded.Execution.RequestID == "" {
		t.Fatalf("unexpected response: %#v", decoded)
	}
	if store.last.From != time.Date(2033, 5, 18, 3, 33, 0, 0, time.UTC) {
		t.Fatalf("unexpected prepared range: %s", store.last.From)
	}
}
