# Cerbero Step 32 Search API v1 vertical

This document freezes the first implemented hunting/search vertical while preserving the broader `TUI & QUERY v1.0` contract.

## Endpoint

`POST /api/v1/search` is the dedicated Step 32 query endpoint. `GET /api/v1/events` exposes the same bounded NormalizedEvent search model through query parameters, and `GET /api/v1/events/{id}` returns one tenant-scoped NormalizedEvent detail.

The client never supplies SQL, table names, physical ClickHouse columns, or arbitrary sort columns. The API maps a closed catalog of canonical OCSF/Cerbero fields to backend expressions.

## Request

```json
{
  "query": "user.name == \"jdoe\" AND severity_id >= 3",
  "time_range": {
    "from": "2033-05-18T03:33:00Z",
    "to": "2033-05-18T03:34:00Z"
  },
  "cursor": "opaque-server-cursor",
  "limit": 50,
  "sort": [{"field": "event_time", "direction": "desc"}]
}
```

`time_range.from` and `time_range.to` are mandatory for `POST /api/v1/search`. The first vertical enforces a 24-hour maximum range and a `1..200` page limit as an MVP resource policy. `GET /api/v1/events` uses a controlled 15-minute default only when neither `from` nor `to` is supplied.

The first vertical intentionally freezes sorting to `event_time asc|desc`. The cursor is opaque, query-bound, and invalid when reused with a different query, time range, or sort direction.

## Query syntax

The parser implements boolean `AND`, `OR`, `NOT`, grouping parentheses, comparisons `== != > >= < <=`, string operators `contains`, `starts_with`, `ends_with`, postfix `exists`, and `in [literal, ...]`.

Initial canonical field catalog:

- `user.name`
- `src_endpoint.ip`
- `service.name`
- `status`
- `message`
- `class_uid`
- `category_uid`
- `activity_id`
- `severity_id`
- `normalized_event_id`
- `raw_event_id`
- `parser_id`
- `parser_version`
- `mapping_id`
- `mapping_version`
- `execution_mode`

Unknown fields, invalid operator/type combinations, malformed input, and SQL-like injection syntax fail before the ClickHouse request is issued.

## Response

The response contains `items`, `next_cursor`, and execution metadata with `query_id`, `request_id`, `execution_time_ms`, `partial_result`, `truncated`, and `timed_out`.

When the requested page limit is reached and more rows exist, `partial_result=true` and `truncated=true` are explicit and `next_cursor` is non-empty. Backend failures do not silently return complete-looking results.

## Tenant and storage boundary

Every ClickHouse query injects the authenticated-development tenant context as a server-side predicate. The ClickHouse API identity has `SELECT` only on `cerbero.normalized_events`. The physical table layout remains an implementation detail.

The current API remains DEVELOPMENT-only until the frozen authentication/RBAC layer is wired. This vertical does not weaken that guard.

## TUI vertical

`cerbero-tui` is a Rust + Ratatui API client. The canonical v1 primary navigation is present, while Search and Events are the operational Step 32 vertical. Essential navigation is keyboard-only, query editing is centralized through one keymap, API requests execute on an async worker, tenant context and request IDs are visible, and the TUI has no database credentials or direct database access.
