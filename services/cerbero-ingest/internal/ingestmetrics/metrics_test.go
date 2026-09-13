package ingestmetrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInstrumentRecordsAcceptedBytesAndLatency(t *testing.T) {
	recorder := NewMemoryRecorder()
	handler := Instrument(recorder, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if _, err := io.ReadAll(request.Body); err != nil {
			t.Fatal(err)
		}
		response.WriteHeader(http.StatusAccepted)
	}))
	body := "{\"message\":\"exact\"}\n"
	request := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	snapshot := recorder.Snapshot()
	if snapshot.EventsReceivedTotal != 1 || snapshot.EventsAcceptedTotal != 1 || snapshot.EventsRejectedTotal != 0 {
		t.Fatalf("unexpected event counters: %+v", snapshot)
	}
	if snapshot.BytesReceivedTotal != uint64(len(body)) {
		t.Fatalf("bytes_received = %d, want %d", snapshot.BytesReceivedTotal, len(body))
	}
	if snapshot.LatencyObservations != 1 {
		t.Fatalf("latency observations = %d, want 1", snapshot.LatencyObservations)
	}
}

func TestInstrumentClassifiesStableFailureCounters(t *testing.T) {
	tests := []struct {
		name   string
		status int
		check  func(Snapshot) uint64
	}{
		{name: "authentication", status: http.StatusUnauthorized, check: func(s Snapshot) uint64 { return s.AuthenticationFailures }},
		{name: "authorization", status: http.StatusForbidden, check: func(s Snapshot) uint64 { return s.AuthorizationFailures }},
		{name: "payload", status: http.StatusRequestEntityTooLarge, check: func(s Snapshot) uint64 { return s.PayloadTooLargeTotal }},
		{name: "rate", status: http.StatusTooManyRequests, check: func(s Snapshot) uint64 { return s.RateLimitedTotal }},
		{name: "publish", status: http.StatusServiceUnavailable, check: func(s Snapshot) uint64 { return s.PublishFailuresTotal }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := NewMemoryRecorder()
			handler := Instrument(recorder, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.WriteHeader(test.status)
			}))
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/ingest", nil))

			snapshot := recorder.Snapshot()
			if snapshot.EventsReceivedTotal != 1 || snapshot.EventsRejectedTotal != 1 || snapshot.EventsAcceptedTotal != 0 {
				t.Fatalf("unexpected event counters: %+v", snapshot)
			}
			if test.check(snapshot) != 1 {
				t.Fatalf("specific failure counter not incremented: %+v", snapshot)
			}
		})
	}
}
