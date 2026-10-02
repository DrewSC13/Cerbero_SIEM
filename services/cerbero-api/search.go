package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

const (
	defaultSearchLimit = 50
	maxSearchLimit     = 200
	maxSearchRange     = 24 * time.Hour
	searchTimeout      = 5 * time.Second
)

var (
	errSearchNotFound    = errors.New("search object not found")
	errSearchUnavailable = errors.New("search backend unavailable")
)

type searchTimeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type searchSort struct {
	Field     string `json:"field"`
	Direction string `json:"direction"`
}

type searchRequest struct {
	Query     string          `json:"query"`
	TimeRange searchTimeRange `json:"time_range"`
	Cursor    string          `json:"cursor,omitempty"`
	Limit     int             `json:"limit,omitempty"`
	Sort      []searchSort    `json:"sort,omitempty"`
}

type searchExecution struct {
	QueryID         string `json:"query_id"`
	RequestID       string `json:"request_id"`
	ExecutionTimeMS int64  `json:"execution_time_ms"`
	PartialResult   bool   `json:"partial_result"`
	Truncated       bool   `json:"truncated"`
	TimedOut        bool   `json:"timed_out"`
}

type searchResponse struct {
	Items      []eventView     `json:"items"`
	NextCursor string          `json:"next_cursor"`
	Execution  searchExecution `json:"execution"`
}

type eventView struct {
	NormalizedEventID string          `json:"normalized_event_id"`
	RawEventID        string          `json:"raw_event_id"`
	TenantID          string          `json:"tenant_id"`
	EventTime         string          `json:"event_time,omitempty"`
	IngestTime        string          `json:"ingest_time"`
	OCSFVersion       string          `json:"ocsf_version"`
	ClassUID          uint32          `json:"class_uid"`
	CategoryUID       uint32          `json:"category_uid"`
	ActivityID        *uint32         `json:"activity_id,omitempty"`
	SeverityID        *uint32         `json:"severity_id,omitempty"`
	ParserID          string          `json:"parser_id"`
	ParserVersion     string          `json:"parser_version"`
	MappingID         string          `json:"mapping_id"`
	MappingVersion    string          `json:"mapping_version"`
	NormalizedHash    string          `json:"normalized_hash"`
	ExecutionMode     int32           `json:"execution_mode"`
	OCSFEvent         json.RawMessage `json:"ocsf_event"`
}

type searchPage struct {
	Items      []eventView
	NextCursor string
	Truncated  bool
}

type searchStore interface {
	search(context.Context, uuid.UUID, preparedSearch) (searchPage, error)
	getEvent(context.Context, uuid.UUID, uuid.UUID) (eventView, error)
}

type preparedSearch struct {
	Request     searchRequest
	Expression  *queryExpression
	From        time.Time
	To          time.Time
	Direction   string
	Fingerprint string
	Cursor      *searchCursor
}

type searchCursor struct {
	Seconds     int64  `json:"seconds"`
	Nanos       int32  `json:"nanos"`
	EventID     string `json:"event_id"`
	Direction   string `json:"direction"`
	Fingerprint string `json:"fingerprint"`
}

type clickHouseSearchStore struct {
	client   *http.Client
	endpoint string
	database string
	user     string
	password string
}

type clickHouseEventRow struct {
	NormalizedEventID string  `json:"normalized_event_id"`
	RawEventID        string  `json:"raw_event_id"`
	TenantID          string  `json:"tenant_id"`
	EventTimePresent  uint8   `json:"event_time_present"`
	EventTimeSeconds  int64   `json:"event_time_seconds"`
	EventTimeNanos    int32   `json:"event_time_nanos"`
	IngestTimeSeconds int64   `json:"ingest_time_seconds"`
	IngestTimeNanos   int32   `json:"ingest_time_nanos"`
	OCSFVersion       string  `json:"ocsf_version"`
	ClassUID          uint32  `json:"class_uid"`
	CategoryUID       uint32  `json:"category_uid"`
	ActivityID        *uint32 `json:"activity_id"`
	Severity          *uint32 `json:"severity"`
	ParserID          string  `json:"parser_id"`
	ParserVersion     string  `json:"parser_version"`
	MappingID         string  `json:"mapping_id"`
	MappingVersion    string  `json:"mapping_version"`
	NormalizedHash    string  `json:"normalized_hash"`
	ExecutionMode     int32   `json:"execution_mode"`
	OCSFEventJSON     string  `json:"ocsf_event_json"`
}

func newClickHouseSearchStore(endpoint, database, user, password string) (*clickHouseSearchStore, error) {
	if !simpleClickHouseIdentifier(database) {
		return nil, fmt.Errorf("CLICKHOUSE_DB must be a simple ClickHouse identifier")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.Path != "" {
		return nil, fmt.Errorf("invalid ClickHouse HTTP endpoint")
	}
	return &clickHouseSearchStore{
		client:   &http.Client{Timeout: searchTimeout},
		endpoint: strings.TrimRight(endpoint, "/"),
		database: database,
		user:     user,
		password: password,
	}, nil
}

func (server *apiServer) searchEvents(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	var input searchRequest
	if !decodeJSONBody(w, r, &input) {
		return
	}
	prepared, err := prepareSearch(input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), searchTimeout)
	defer cancel()
	page, err := server.search.search(ctx, tenantID, prepared)
	if err != nil {
		searchError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Items:      page.Items,
		NextCursor: page.NextCursor,
		Execution: searchExecution{
			QueryID:         prepared.Fingerprint,
			RequestID:       requestIDFromContext(r.Context()),
			ExecutionTimeMS: time.Since(started).Milliseconds(),
			PartialResult:   page.Truncated,
			Truncated:       page.Truncated,
			TimedOut:        false,
		},
	})
}

func (server *apiServer) listEvents(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	input, err := searchRequestFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	prepared, err := prepareSearch(input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), searchTimeout)
	defer cancel()
	page, err := server.search.search(ctx, tenantID, prepared)
	if err != nil {
		searchError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Items:      page.Items,
		NextCursor: page.NextCursor,
		Execution: searchExecution{
			QueryID:         prepared.Fingerprint,
			RequestID:       requestIDFromContext(r.Context()),
			ExecutionTimeMS: time.Since(started).Milliseconds(),
			PartialResult:   page.Truncated,
			Truncated:       page.Truncated,
		},
	})
}

func (server *apiServer) getEvent(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenant(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil || id.Version() != 7 {
		http.Error(w, "valid UUIDv7 id is required", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), searchTimeout)
	defer cancel()
	item, err := server.search.getEvent(ctx, tenantID, id)
	if err != nil {
		searchError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func searchRequestFromQuery(r *http.Request) (searchRequest, error) {
	values := r.URL.Query()
	from, to := values.Get("from"), values.Get("to")
	if from == "" && to == "" {
		now := time.Now().UTC()
		to = now.Format(time.RFC3339Nano)
		from = now.Add(-15 * time.Minute).Format(time.RFC3339Nano)
	} else if from == "" || to == "" {
		return searchRequest{}, fmt.Errorf("from and to must be provided together")
	}
	limit := defaultSearchLimit
	if raw := values.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return searchRequest{}, fmt.Errorf("limit must be an integer")
		}
		limit = parsed
	}
	direction := values.Get("sort")
	if direction == "" {
		direction = "desc"
	}
	return searchRequest{
		Query:     values.Get("query"),
		TimeRange: searchTimeRange{From: from, To: to},
		Cursor:    values.Get("cursor"),
		Limit:     limit,
		Sort:      []searchSort{{Field: "event_time", Direction: direction}},
	}, nil
}

func prepareSearch(input searchRequest) (preparedSearch, error) {
	if strings.TrimSpace(input.TimeRange.From) == "" || strings.TrimSpace(input.TimeRange.To) == "" {
		return preparedSearch{}, fmt.Errorf("time_range.from and time_range.to are required")
	}
	from, err := time.Parse(time.RFC3339Nano, input.TimeRange.From)
	if err != nil {
		return preparedSearch{}, fmt.Errorf("time_range.from must be RFC3339")
	}
	to, err := time.Parse(time.RFC3339Nano, input.TimeRange.To)
	if err != nil {
		return preparedSearch{}, fmt.Errorf("time_range.to must be RFC3339")
	}
	if !from.Before(to) {
		return preparedSearch{}, fmt.Errorf("time_range.from must be before time_range.to")
	}
	if to.Sub(from) > maxSearchRange {
		return preparedSearch{}, fmt.Errorf("time range exceeds the 24h MVP search policy")
	}
	if input.Limit == 0 {
		input.Limit = defaultSearchLimit
	}
	if input.Limit < 1 || input.Limit > maxSearchLimit {
		return preparedSearch{}, fmt.Errorf("limit must be 1..200")
	}
	if len(input.Sort) == 0 {
		input.Sort = []searchSort{{Field: "event_time", Direction: "desc"}}
	}
	if len(input.Sort) != 1 || input.Sort[0].Field != "event_time" {
		return preparedSearch{}, fmt.Errorf("Step 32 v1 supports sorting only by event_time")
	}
	direction := strings.ToLower(input.Sort[0].Direction)
	if direction != "asc" && direction != "desc" {
		return preparedSearch{}, fmt.Errorf("sort direction must be asc or desc")
	}
	input.Sort[0].Direction = direction
	expression, err := parseQuery(strings.TrimSpace(input.Query))
	if err != nil {
		return preparedSearch{}, err
	}
	if _, _, err := compileExpression(expression); err != nil {
		return preparedSearch{}, err
	}
	fingerprint, err := searchFingerprint(input)
	if err != nil {
		return preparedSearch{}, err
	}
	var cursor *searchCursor
	if input.Cursor != "" {
		decoded, err := decodeSearchCursor(input.Cursor)
		if err != nil {
			return preparedSearch{}, fmt.Errorf("invalid cursor")
		}
		if decoded.Fingerprint != fingerprint || decoded.Direction != direction {
			return preparedSearch{}, fmt.Errorf("cursor does not belong to this query")
		}
		cursor = &decoded
	}
	return preparedSearch{
		Request:     input,
		Expression:  expression,
		From:        from.UTC(),
		To:          to.UTC(),
		Direction:   direction,
		Fingerprint: fingerprint,
		Cursor:      cursor,
	}, nil
}

func searchFingerprint(input searchRequest) (string, error) {
	canonical := struct {
		Query     string          `json:"query"`
		TimeRange searchTimeRange `json:"time_range"`
		Sort      []searchSort    `json:"sort"`
	}{Query: strings.TrimSpace(input.Query), TimeRange: input.TimeRange, Sort: input.Sort}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("encode query fingerprint: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func decodeSearchCursor(raw string) (searchCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return searchCursor{}, err
	}
	var cursor searchCursor
	decoder := json.NewDecoder(strings.NewReader(string(decoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return searchCursor{}, err
	}
	id, err := uuid.Parse(cursor.EventID)
	if err != nil || id.Version() != 7 || cursor.Fingerprint == "" {
		return searchCursor{}, fmt.Errorf("invalid cursor payload")
	}
	return cursor, nil
}

func encodeSearchCursor(cursor searchCursor) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func (store *clickHouseSearchStore) search(ctx context.Context, tenantID uuid.UUID, prepared preparedSearch) (searchPage, error) {
	where, parameters, err := compileExpression(prepared.Expression)
	if err != nil {
		return searchPage{}, err
	}
	parameters.Set("param_tenant", tenantID.String())
	parameters.Set("param_from_seconds", strconv.FormatInt(prepared.From.Unix(), 10))
	parameters.Set("param_from_nanos", strconv.Itoa(prepared.From.Nanosecond()))
	parameters.Set("param_to_seconds", strconv.FormatInt(prepared.To.Unix(), 10))
	parameters.Set("param_to_nanos", strconv.Itoa(prepared.To.Nanosecond()))

	predicates := []string{
		"tenant_id = {tenant:String}",
		"event_time_present = 1",
		"tuple(event_time_seconds, event_time_nanos) >= tuple({from_seconds:Int64}, {from_nanos:Int32})",
		"tuple(event_time_seconds, event_time_nanos) < tuple({to_seconds:Int64}, {to_nanos:Int32})",
	}
	if where != "" {
		predicates = append(predicates, where)
	}
	if prepared.Cursor != nil {
		parameters.Set("param_cursor_seconds", strconv.FormatInt(prepared.Cursor.Seconds, 10))
		parameters.Set("param_cursor_nanos", strconv.Itoa(int(prepared.Cursor.Nanos)))
		parameters.Set("param_cursor_id", prepared.Cursor.EventID)
		predicates = append(predicates, cursorKeysetPredicate(prepared.Direction))
	}
	order := "DESC"
	if prepared.Direction == "asc" {
		order = "ASC"
	}
	query := fmt.Sprintf(
		"%s FROM %s.normalized_events WHERE %s ORDER BY event_time_seconds %s, event_time_nanos %s, toString(normalized_event_id) %s LIMIT %d FORMAT JSONEachRow",
		searchSelectColumns(), store.database, strings.Join(predicates, " AND "), order, order, order, prepared.Request.Limit+1,
	)
	rows, err := store.queryRows(ctx, query, parameters)
	if err != nil {
		return searchPage{}, err
	}
	truncated := len(rows) > prepared.Request.Limit
	if truncated {
		rows = rows[:prepared.Request.Limit]
	}
	items := make([]eventView, 0, len(rows))
	for _, row := range rows {
		item, err := eventViewFromRow(row)
		if err != nil {
			return searchPage{}, err
		}
		items = append(items, item)
	}
	next := ""
	if truncated && len(rows) > 0 {
		last := rows[len(rows)-1]
		next, err = encodeSearchCursor(searchCursor{
			Seconds:     last.EventTimeSeconds,
			Nanos:       last.EventTimeNanos,
			EventID:     last.NormalizedEventID,
			Direction:   prepared.Direction,
			Fingerprint: prepared.Fingerprint,
		})
		if err != nil {
			return searchPage{}, err
		}
	}
	return searchPage{Items: items, NextCursor: next, Truncated: truncated}, nil
}

func cursorKeysetPredicate(direction string) string {
	operator := "<"
	if direction == "asc" {
		operator = ">"
	}
	return fmt.Sprintf(
		"(event_time_seconds %s {cursor_seconds:Int64} OR "+
			"(event_time_seconds = {cursor_seconds:Int64} AND event_time_nanos %s {cursor_nanos:Int32}) OR "+
			"(event_time_seconds = {cursor_seconds:Int64} AND event_time_nanos = {cursor_nanos:Int32} AND toString(normalized_event_id) %s {cursor_id:String}))",
		operator, operator, operator,
	)
}

func (store *clickHouseSearchStore) getEvent(ctx context.Context, tenantID, id uuid.UUID) (eventView, error) {
	parameters := url.Values{}
	parameters.Set("param_tenant", tenantID.String())
	parameters.Set("param_event_id", id.String())
	query := eventLookupQuery(store.database)
	rows, err := store.queryRows(ctx, query, parameters)
	if err != nil {
		return eventView{}, err
	}
	if len(rows) == 0 {
		return eventView{}, errSearchNotFound
	}
	return eventViewFromRow(rows[0])
}

func eventLookupQuery(database string) string {
	return fmt.Sprintf(
		"%s FROM %s.normalized_events WHERE tenant_id = {tenant:String} AND toString(normalized_event_id) = {event_id:String} LIMIT 1 FORMAT JSONEachRow",
		searchSelectColumns(), database,
	)
}

func searchSelectColumns() string {
	return `SELECT
    toString(normalized_event_id) AS normalized_event_id,
    toString(raw_event_id) AS raw_event_id,
    tenant_id,
    event_time_present,
    event_time_seconds,
    event_time_nanos,
    ingest_time_seconds,
    ingest_time_nanos,
    ocsf_version,
    class_uid,
    category_uid,
    activity_id,
    severity,
    parser_id,
    parser_version,
    mapping_id,
    mapping_version,
    normalized_hash,
    execution_mode,
    ocsf_event_json`
}

func (store *clickHouseSearchStore) queryRows(ctx context.Context, query string, parameters url.Values) ([]clickHouseEventRow, error) {
	parameters.Set("query", query)
	parameters.Set("max_execution_time", "4")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, store.endpoint+"/?"+parameters.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("build ClickHouse request: %w", err)
	}
	request.SetBasicAuth(store.user, store.password)
	response, err := store.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errSearchUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("%w: ClickHouse HTTP %d: %s", errSearchUnavailable, response.StatusCode, strings.TrimSpace(string(body)))
	}
	scanner := bufio.NewScanner(io.LimitReader(response.Body, 8<<20))
	scanner.Buffer(make([]byte, 64*1024), 2<<20)
	rows := make([]clickHouseEventRow, 0)
	for scanner.Scan() {
		var row clickHouseEventRow
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, fmt.Errorf("decode ClickHouse event row: %w", err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read ClickHouse event rows: %w", err)
	}
	return rows, nil
}

func eventViewFromRow(row clickHouseEventRow) (eventView, error) {
	id, err := uuid.Parse(row.NormalizedEventID)
	if err != nil || id.Version() != 7 {
		return eventView{}, fmt.Errorf("stored normalized_event_id is not UUIDv7")
	}
	if !json.Valid([]byte(row.OCSFEventJSON)) {
		return eventView{}, fmt.Errorf("stored OCSF event is not valid JSON")
	}
	eventTime := ""
	if row.EventTimePresent == 1 {
		eventTime = time.Unix(row.EventTimeSeconds, int64(row.EventTimeNanos)).UTC().Format(time.RFC3339Nano)
	}
	return eventView{
		NormalizedEventID: row.NormalizedEventID,
		RawEventID:        row.RawEventID,
		TenantID:          row.TenantID,
		EventTime:         eventTime,
		IngestTime:        time.Unix(row.IngestTimeSeconds, int64(row.IngestTimeNanos)).UTC().Format(time.RFC3339Nano),
		OCSFVersion:       row.OCSFVersion,
		ClassUID:          row.ClassUID,
		CategoryUID:       row.CategoryUID,
		ActivityID:        row.ActivityID,
		SeverityID:        row.Severity,
		ParserID:          row.ParserID,
		ParserVersion:     row.ParserVersion,
		MappingID:         row.MappingID,
		MappingVersion:    row.MappingVersion,
		NormalizedHash:    row.NormalizedHash,
		ExecutionMode:     row.ExecutionMode,
		OCSFEvent:         json.RawMessage(row.OCSFEventJSON),
	}, nil
}

func searchError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, errSearchNotFound):
		http.NotFound(w, r)
	case errors.Is(err, errSearchUnavailable), errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "search backend unavailable", http.StatusServiceUnavailable)
	default:
		http.Error(w, "search query failed", http.StatusInternalServerError)
	}
}

type queryValueKind uint8

const (
	queryString queryValueKind = iota + 1
	queryNumber
)

type fieldSpec struct {
	Expression string
	Kind       queryValueKind
	Nullable   bool
	UUID       bool
}

var searchFields = map[string]fieldSpec{
	"user.name":           {Expression: "JSONExtractString(ocsf_event_json, 'user', 'name')", Kind: queryString},
	"src_endpoint.ip":     {Expression: "JSONExtractString(ocsf_event_json, 'src_endpoint', 'ip')", Kind: queryString},
	"service.name":        {Expression: "JSONExtractString(ocsf_event_json, 'service', 'name')", Kind: queryString},
	"status":              {Expression: "JSONExtractString(ocsf_event_json, 'status')", Kind: queryString},
	"message":             {Expression: "JSONExtractString(ocsf_event_json, 'message')", Kind: queryString},
	"class_uid":           {Expression: "class_uid", Kind: queryNumber},
	"category_uid":        {Expression: "category_uid", Kind: queryNumber},
	"activity_id":         {Expression: "activity_id", Kind: queryNumber, Nullable: true},
	"severity_id":         {Expression: "severity", Kind: queryNumber, Nullable: true},
	"normalized_event_id": {Expression: "toString(normalized_event_id)", Kind: queryString, UUID: true},
	"raw_event_id":        {Expression: "toString(raw_event_id)", Kind: queryString, UUID: true},
	"parser_id":           {Expression: "parser_id", Kind: queryString},
	"parser_version":      {Expression: "parser_version", Kind: queryString},
	"mapping_id":          {Expression: "mapping_id", Kind: queryString},
	"mapping_version":     {Expression: "mapping_version", Kind: queryString},
	"execution_mode":      {Expression: "execution_mode", Kind: queryNumber},
}

type queryExpression struct {
	Kind   string
	Field  string
	Op     string
	Value  queryLiteral
	Values []queryLiteral
	Left   *queryExpression
	Right  *queryExpression
}

type queryLiteral struct {
	Kind queryValueKind
	Text string
}

type queryToken struct {
	Kind string
	Text string
}

type queryParser struct {
	tokens []queryToken
	index  int
}

func parseQuery(raw string) (*queryExpression, error) {
	if raw == "" {
		return nil, nil
	}
	tokens, err := lexQuery(raw)
	if err != nil {
		return nil, err
	}
	parser := &queryParser{tokens: tokens}
	expression, err := parser.parseOr()
	if err != nil {
		return nil, err
	}
	if parser.peek().Kind != "eof" {
		return nil, fmt.Errorf("unexpected token %q", parser.peek().Text)
	}
	return expression, nil
}

func lexQuery(raw string) ([]queryToken, error) {
	tokens := make([]queryToken, 0)
	for index := 0; index < len(raw); {
		r := rune(raw[index])
		if unicode.IsSpace(r) {
			index++
			continue
		}
		switch raw[index] {
		case '(':
			tokens = append(tokens, queryToken{Kind: "lparen", Text: "("})
			index++
			continue
		case ')':
			tokens = append(tokens, queryToken{Kind: "rparen", Text: ")"})
			index++
			continue
		case '[':
			tokens = append(tokens, queryToken{Kind: "lbracket", Text: "["})
			index++
			continue
		case ']':
			tokens = append(tokens, queryToken{Kind: "rbracket", Text: "]"})
			index++
			continue
		case ',':
			tokens = append(tokens, queryToken{Kind: "comma", Text: ","})
			index++
			continue
		case '"':
			start := index
			index++
			escaped := false
			for index < len(raw) {
				if !escaped && raw[index] == '"' {
					index++
					break
				}
				if !escaped && raw[index] == '\\' {
					escaped = true
					index++
					continue
				}
				escaped = false
				index++
			}
			if index > len(raw) || raw[index-1] != '"' {
				return nil, fmt.Errorf("unterminated string literal")
			}
			decoded, err := strconv.Unquote(raw[start:index])
			if err != nil {
				return nil, fmt.Errorf("invalid string literal")
			}
			tokens = append(tokens, queryToken{Kind: "string", Text: decoded})
			continue
		}
		if strings.ContainsRune("=!<>", rune(raw[index])) {
			start := index
			index++
			if index < len(raw) && raw[index] == '=' {
				index++
			}
			op := raw[start:index]
			if op != "==" && op != "!=" && op != ">" && op != ">=" && op != "<" && op != "<=" {
				return nil, fmt.Errorf("unsupported operator %q", op)
			}
			tokens = append(tokens, queryToken{Kind: "operator", Text: op})
			continue
		}
		if isIdentifierStart(raw[index]) {
			start := index
			index++
			for index < len(raw) && isIdentifierPart(raw[index]) {
				index++
			}
			text := raw[start:index]
			lower := strings.ToLower(text)
			switch lower {
			case "and", "or", "not", "contains", "starts_with", "ends_with", "exists", "in":
				tokens = append(tokens, queryToken{Kind: lower, Text: lower})
			default:
				tokens = append(tokens, queryToken{Kind: "identifier", Text: text})
			}
			continue
		}
		if raw[index] == '-' || (raw[index] >= '0' && raw[index] <= '9') {
			start := index
			index++
			for index < len(raw) && ((raw[index] >= '0' && raw[index] <= '9') || raw[index] == '.') {
				index++
			}
			text := raw[start:index]
			if _, err := strconv.ParseFloat(text, 64); err != nil {
				return nil, fmt.Errorf("invalid numeric literal %q", text)
			}
			tokens = append(tokens, queryToken{Kind: "number", Text: text})
			continue
		}
		return nil, fmt.Errorf("unexpected character %q", raw[index])
	}
	tokens = append(tokens, queryToken{Kind: "eof"})
	return tokens, nil
}

func isIdentifierStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func isIdentifierPart(value byte) bool {
	return isIdentifierStart(value) || value >= '0' && value <= '9' || value == '.'
}

func (parser *queryParser) peek() queryToken {
	return parser.tokens[parser.index]
}

func (parser *queryParser) consume(kind string) (queryToken, error) {
	token := parser.peek()
	if token.Kind != kind {
		return queryToken{}, fmt.Errorf("expected %s, got %q", kind, token.Text)
	}
	parser.index++
	return token, nil
}

func (parser *queryParser) match(kind string) bool {
	if parser.peek().Kind != kind {
		return false
	}
	parser.index++
	return true
}

func (parser *queryParser) parseOr() (*queryExpression, error) {
	left, err := parser.parseAnd()
	if err != nil {
		return nil, err
	}
	for parser.match("or") {
		right, err := parser.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &queryExpression{Kind: "or", Left: left, Right: right}
	}
	return left, nil
}

func (parser *queryParser) parseAnd() (*queryExpression, error) {
	left, err := parser.parseNot()
	if err != nil {
		return nil, err
	}
	for parser.match("and") {
		right, err := parser.parseNot()
		if err != nil {
			return nil, err
		}
		left = &queryExpression{Kind: "and", Left: left, Right: right}
	}
	return left, nil
}

func (parser *queryParser) parseNot() (*queryExpression, error) {
	if parser.match("not") {
		child, err := parser.parseNot()
		if err != nil {
			return nil, err
		}
		return &queryExpression{Kind: "not", Left: child}, nil
	}
	return parser.parsePrimary()
}

func (parser *queryParser) parsePrimary() (*queryExpression, error) {
	if parser.match("lparen") {
		expression, err := parser.parseOr()
		if err != nil {
			return nil, err
		}
		if _, err := parser.consume("rparen"); err != nil {
			return nil, err
		}
		return expression, nil
	}
	return parser.parsePredicate()
}

func (parser *queryParser) parsePredicate() (*queryExpression, error) {
	field, err := parser.consume("identifier")
	if err != nil {
		return nil, err
	}
	if parser.match("exists") {
		return &queryExpression{Kind: "predicate", Field: field.Text, Op: "exists"}, nil
	}
	op := parser.peek()
	if op.Kind != "operator" && op.Kind != "contains" && op.Kind != "starts_with" && op.Kind != "ends_with" && op.Kind != "in" {
		return nil, fmt.Errorf("expected comparison operator after %s", field.Text)
	}
	parser.index++
	if op.Kind == "in" {
		if _, err := parser.consume("lbracket"); err != nil {
			return nil, err
		}
		values := make([]queryLiteral, 0)
		for {
			value, err := parser.parseLiteral()
			if err != nil {
				return nil, err
			}
			values = append(values, value)
			if parser.match("rbracket") {
				break
			}
			if _, err := parser.consume("comma"); err != nil {
				return nil, err
			}
		}
		if len(values) == 0 || len(values) > 32 {
			return nil, fmt.Errorf("in requires 1..32 literals")
		}
		return &queryExpression{Kind: "predicate", Field: field.Text, Op: "in", Values: values}, nil
	}
	value, err := parser.parseLiteral()
	if err != nil {
		return nil, err
	}
	operator := op.Text
	if op.Kind != "operator" {
		operator = op.Kind
	}
	return &queryExpression{Kind: "predicate", Field: field.Text, Op: operator, Value: value}, nil
}

func (parser *queryParser) parseLiteral() (queryLiteral, error) {
	token := parser.peek()
	switch token.Kind {
	case "string":
		parser.index++
		return queryLiteral{Kind: queryString, Text: token.Text}, nil
	case "number":
		parser.index++
		return queryLiteral{Kind: queryNumber, Text: token.Text}, nil
	default:
		return queryLiteral{}, fmt.Errorf("expected string or number literal, got %q", token.Text)
	}
}

func compileExpression(expression *queryExpression) (string, url.Values, error) {
	parameters := url.Values{}
	counter := 0
	compiled, err := compileExpressionInto(expression, parameters, &counter)
	return compiled, parameters, err
}

func compileExpressionInto(expression *queryExpression, parameters url.Values, counter *int) (string, error) {
	if expression == nil {
		return "", nil
	}
	switch expression.Kind {
	case "and", "or":
		left, err := compileExpressionInto(expression.Left, parameters, counter)
		if err != nil {
			return "", err
		}
		right, err := compileExpressionInto(expression.Right, parameters, counter)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s %s %s)", left, strings.ToUpper(expression.Kind), right), nil
	case "not":
		child, err := compileExpressionInto(expression.Left, parameters, counter)
		if err != nil {
			return "", err
		}
		return "(NOT " + child + ")", nil
	case "predicate":
		return compilePredicate(expression, parameters, counter)
	default:
		return "", fmt.Errorf("unsupported expression kind")
	}
}

func compilePredicate(expression *queryExpression, parameters url.Values, counter *int) (string, error) {
	spec, ok := searchFields[expression.Field]
	if !ok {
		return "", fmt.Errorf("unknown query field %q", expression.Field)
	}
	if expression.Op == "exists" {
		if spec.Nullable {
			return "isNotNull(" + spec.Expression + ")", nil
		}
		if spec.Kind == queryString {
			return spec.Expression + " != ''", nil
		}
		return "", fmt.Errorf("exists is not meaningful for required field %q", expression.Field)
	}
	if expression.Op == "contains" || expression.Op == "starts_with" || expression.Op == "ends_with" {
		if spec.Kind != queryString || expression.Value.Kind != queryString {
			return "", fmt.Errorf("%s requires a string field and string literal", expression.Op)
		}
		placeholder := addSearchParameter(parameters, counter, "String", expression.Value.Text)
		switch expression.Op {
		case "contains":
			return fmt.Sprintf("positionCaseSensitive(%s, %s) > 0", spec.Expression, placeholder), nil
		case "starts_with":
			return fmt.Sprintf("startsWith(%s, %s)", spec.Expression, placeholder), nil
		default:
			return fmt.Sprintf("endsWith(%s, %s)", spec.Expression, placeholder), nil
		}
	}
	if expression.Op == "in" {
		if len(expression.Values) == 0 {
			return "", fmt.Errorf("in requires at least one value")
		}
		placeholders := make([]string, 0, len(expression.Values))
		for _, value := range expression.Values {
			placeholder, err := typedSearchParameter(parameters, counter, spec, value)
			if err != nil {
				return "", err
			}
			placeholders = append(placeholders, placeholder)
		}
		return fmt.Sprintf("%s IN (%s)", spec.Expression, strings.Join(placeholders, ", ")), nil
	}
	if expression.Op != "==" && expression.Op != "!=" && expression.Op != ">" && expression.Op != ">=" && expression.Op != "<" && expression.Op != "<=" {
		return "", fmt.Errorf("unsupported operator %q", expression.Op)
	}
	if spec.Kind == queryString && expression.Op != "==" && expression.Op != "!=" {
		return "", fmt.Errorf("operator %s is not valid for string field %q", expression.Op, expression.Field)
	}
	placeholder, err := typedSearchParameter(parameters, counter, spec, expression.Value)
	if err != nil {
		return "", err
	}
	operator := expression.Op
	if operator == "==" {
		operator = "="
	}
	return fmt.Sprintf("%s %s %s", spec.Expression, operator, placeholder), nil
}

func typedSearchParameter(parameters url.Values, counter *int, spec fieldSpec, value queryLiteral) (string, error) {
	if spec.Kind != value.Kind {
		return "", fmt.Errorf("literal type does not match query field")
	}
	if spec.UUID {
		parsed, err := uuid.Parse(value.Text)
		if err != nil || parsed.Version() != 7 {
			return "", fmt.Errorf("UUID query field requires UUIDv7 string literal")
		}
	}
	if spec.Kind == queryNumber {
		if _, err := strconv.ParseFloat(value.Text, 64); err != nil {
			return "", fmt.Errorf("invalid numeric literal")
		}
		return addSearchParameter(parameters, counter, "Float64", value.Text), nil
	}
	return addSearchParameter(parameters, counter, "String", value.Text), nil
}

func addSearchParameter(parameters url.Values, counter *int, parameterType, value string) string {
	name := fmt.Sprintf("q%d", *counter)
	*counter++
	parameters.Set("param_"+name, value)
	return "{" + name + ":" + parameterType + "}"
}

func simpleClickHouseIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for index, character := range value {
		if index == 0 {
			if character != '_' && !unicode.IsLetter(character) {
				return false
			}
			continue
		}
		if character != '_' && !unicode.IsLetter(character) && !unicode.IsDigit(character) {
			return false
		}
	}
	return true
}
