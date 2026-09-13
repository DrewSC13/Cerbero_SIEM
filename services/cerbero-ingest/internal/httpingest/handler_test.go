package httpingest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
	contractsv1 "cerbero/services/internal/contracts/v1"
)

const (
	testRequestID = "018f47d3-2c6a-7b10-8f00-000000000001"
	testEventID   = "018f47d3-2c6a-7b10-8f00-000000000002"
	testMessageID = "018f47d3-2c6a-7b10-8f00-000000000003"
)

type fixedIDs struct {
	value string
}

func (f fixedIDs) New() (string, error) { return f.value, nil }

type prepareFake struct {
	request ingestcore.Request
	err     error
}

func (f *prepareFake) Prepare(_ context.Context, request ingestcore.Request) (*ingestcore.Result, error) {
	f.request = request
	if f.err != nil {
		return nil, f.err
	}
	return &ingestcore.Result{
		RequestID: request.RequestID,
		RawEvent:  &contractsv1.RawEvent{EventId: testEventID},
		Envelope:  &contractsv1.CerberoEnvelope{MessageId: testMessageID},
	}, nil
}

type acceptFake struct {
	called bool
	err    error
}

func (f *acceptFake) Accept(_ context.Context, _ *ingestcore.Result) error {
	f.called = true
	return f.err
}

type metadataFake struct {
	err error
}

func (f metadataFake) Resolve(_ *http.Request) (ingestcore.AdmissionMetadata, error) {
	if f.err != nil {
		return ingestcore.AdmissionMetadata{}, f.err
	}
	return ingestcore.AdmissionMetadata{
		TenantID:       "tenant-a",
		SourceID:       "source-a",
		SensorID:       "sensor-a",
		RemoteIdentity: "principal-a",
	}, nil
}

func newTestHandler(t *testing.T, preparer *prepareFake, acceptor *acceptFake) *Handler {
	t.Helper()
	handler, err := New(Config{
		MaxPayloadSize: 1024,
		Preparer:       preparer,
		Acceptor:       acceptor,
		Metadata:       metadataFake{},
		RequestIDs:     fixedIDs{value: testRequestID},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return handler
}

func TestHandlerPreservesExactJSONBytesAndRequiresDurableAcceptance(t *testing.T) {
	preparer := &prepareFake{}
	acceptor := &acceptFake{}
	handler := newTestHandler(t, preparer, acceptor)
	body := "{\n  \"message\": \"failed login\", \"n\": 1\n}\n"

	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusAccepted, response.Body.String())
	}
	if !acceptor.called {
		t.Fatal("durable acceptor was not called")
	}
	if string(preparer.request.RawPayload) != body {
		t.Fatalf("raw payload changed: got %q want %q", string(preparer.request.RawPayload), body)
	}
	if preparer.request.RequestID != testRequestID {
		t.Fatalf("request_id = %q, want %q", preparer.request.RequestID, testRequestID)
	}
	if preparer.request.Transport != "json-http" {
		t.Fatalf("transport = %q", preparer.request.Transport)
	}
	if response.Header().Get(requestIDHeader) != testRequestID {
		t.Fatalf("response X-Request-ID = %q", response.Header().Get(requestIDHeader))
	}

	var payload map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode success response: %v", err)
	}
	if payload["event_id"] != testEventID || payload["message_id"] != testMessageID {
		t.Fatalf("unexpected success payload: %#v", payload)
	}
}

func TestHandlerUsesValidSuppliedRequestID(t *testing.T) {
	preparer := &prepareFake{}
	handler := newTestHandler(t, preparer, &acceptFake{})
	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(`{"ok":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(requestIDHeader, testRequestID)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if preparer.request.RequestID != testRequestID {
		t.Fatalf("request_id = %q", preparer.request.RequestID)
	}
}

func TestHandlerRejectsMalformedJSONBeforePreparation(t *testing.T) {
	preparer := &prepareFake{}
	acceptor := &acceptFake{}
	handler := newTestHandler(t, preparer, acceptor)
	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(`{"broken":`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", response.Code)
	}
	if acceptor.called {
		t.Fatal("durable acceptor must not be called for invalid JSON")
	}
}

func TestHandlerRejectsOversizedBodyBeforePreparation(t *testing.T) {
	preparer := &prepareFake{}
	acceptor := &acceptFake{}
	handler, err := New(Config{
		MaxPayloadSize: 4,
		Preparer:       preparer,
		Acceptor:       acceptor,
		Metadata:       metadataFake{},
		RequestIDs:     fixedIDs{value: testRequestID},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(`{"a":1}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", response.Code)
	}
	if acceptor.called {
		t.Fatal("durable acceptor must not be called for oversized body")
	}
}

func TestHandlerMapsDurableFailureToRetryable503(t *testing.T) {
	preparer := &prepareFake{}
	acceptor := &acceptFake{err: errors.New("nats unavailable")}
	handler := newTestHandler(t, preparer, acceptor)
	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(`{"ok":true}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}

	var payload struct {
		Error contractsv1.CerberoError `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if payload.Error.GetCode() != "CER-BUS-PUBLISH-FAILED" || !payload.Error.GetRetryable() {
		t.Fatalf("unexpected error contract: %#v", payload.Error)
	}
}

func TestHandlerMapsCoreAuthorizationFailureTo403(t *testing.T) {
	preparer := &prepareFake{
		err: &ingestcore.Error{Contract: &contractsv1.CerberoError{
			Code:      "CER-AUTH-FORBIDDEN",
			Category:  contractsv1.ErrorCategory_AUTHORIZATION,
			Message:   "source is not authorized to ingest events",
			Component: "cerbero-ingest",
			RequestId: testRequestID,
			Metadata:  map[string]string{},
		}},
	}
	handler := newTestHandler(t, preparer, &acceptFake{})
	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(`{"ok":true}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}
