package ingestmetrics

import (
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// Semantic metric identifiers are backend-neutral. A future exporter may map
// them to a backend-specific naming convention without changing ingest code.
const (
	EventsReceivedTotal    = "events_received_total"
	EventsAcceptedTotal    = "events_accepted_total"
	EventsRejectedTotal    = "events_rejected_total"
	BytesReceivedTotal     = "bytes_received_total"
	IngestRate             = "ingest_rate"
	IngestLatency          = "ingest_latency"
	PayloadTooLargeTotal   = "payload_too_large_total"
	AuthenticationFailures = "authentication_failures_total"
	AuthorizationFailures  = "authorization_failures_total"
	RateLimitedTotal       = "rate_limited_total"
	PublishFailuresTotal   = "publish_failures_total"
)

// Recorder is the backend-neutral ingest metrics boundary.
type Recorder interface {
	EventReceived()
	EventAccepted()
	EventRejected()
	BytesReceived(uint64)
	ObserveLatency(time.Duration)
	PayloadTooLarge()
	AuthenticationFailure()
	AuthorizationFailure()
	RateLimited()
	PublishFailure()
}

// Snapshot is a point-in-time view suitable for tests or future exporters.
// IngestRate is intentionally derived by exporters from event counters over time.
type Snapshot struct {
	EventsReceivedTotal     uint64
	EventsAcceptedTotal     uint64
	EventsRejectedTotal     uint64
	BytesReceivedTotal      uint64
	LatencyObservations     uint64
	LatencyTotalNanoseconds uint64
	PayloadTooLargeTotal    uint64
	AuthenticationFailures  uint64
	AuthorizationFailures   uint64
	RateLimitedTotal        uint64
	PublishFailuresTotal    uint64
}

// MemoryRecorder provides concurrency-safe instrumentation without selecting a metrics backend.
type MemoryRecorder struct {
	eventsReceived      atomic.Uint64
	eventsAccepted      atomic.Uint64
	eventsRejected      atomic.Uint64
	bytesReceived       atomic.Uint64
	latencyObservations atomic.Uint64
	latencyNanoseconds  atomic.Uint64
	payloadTooLarge     atomic.Uint64
	authenticationFails atomic.Uint64
	authorizationFails  atomic.Uint64
	rateLimited         atomic.Uint64
	publishFailures     atomic.Uint64
}

// NewMemoryRecorder returns the backend-neutral runtime recorder used until an exporter is selected.
func NewMemoryRecorder() *MemoryRecorder {
	return &MemoryRecorder{}
}

func (m *MemoryRecorder) EventReceived()             { m.eventsReceived.Add(1) }
func (m *MemoryRecorder) EventAccepted()             { m.eventsAccepted.Add(1) }
func (m *MemoryRecorder) EventRejected()             { m.eventsRejected.Add(1) }
func (m *MemoryRecorder) BytesReceived(value uint64) { m.bytesReceived.Add(value) }
func (m *MemoryRecorder) PayloadTooLarge()           { m.payloadTooLarge.Add(1) }
func (m *MemoryRecorder) AuthenticationFailure()     { m.authenticationFails.Add(1) }
func (m *MemoryRecorder) AuthorizationFailure()      { m.authorizationFails.Add(1) }
func (m *MemoryRecorder) RateLimited()               { m.rateLimited.Add(1) }
func (m *MemoryRecorder) PublishFailure()            { m.publishFailures.Add(1) }

func (m *MemoryRecorder) ObserveLatency(value time.Duration) {
	if value < 0 {
		value = 0
	}
	m.latencyObservations.Add(1)
	m.latencyNanoseconds.Add(uint64(value.Nanoseconds()))
}

// Snapshot returns counters without exposing event/source identifiers as labels.
func (m *MemoryRecorder) Snapshot() Snapshot {
	return Snapshot{
		EventsReceivedTotal:     m.eventsReceived.Load(),
		EventsAcceptedTotal:     m.eventsAccepted.Load(),
		EventsRejectedTotal:     m.eventsRejected.Load(),
		BytesReceivedTotal:      m.bytesReceived.Load(),
		LatencyObservations:     m.latencyObservations.Load(),
		LatencyTotalNanoseconds: m.latencyNanoseconds.Load(),
		PayloadTooLargeTotal:    m.payloadTooLarge.Load(),
		AuthenticationFailures:  m.authenticationFails.Load(),
		AuthorizationFailures:   m.authorizationFails.Load(),
		RateLimitedTotal:        m.rateLimited.Load(),
		PublishFailuresTotal:    m.publishFailures.Load(),
	}
}

// Instrument wraps only the ingest handler. Health endpoints are deliberately excluded.
func Instrument(recorder Recorder, next http.Handler) http.Handler {
	if recorder == nil {
		recorder = NewMemoryRecorder()
	}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		started := time.Now()
		recorder.EventReceived()

		body := &countingReadCloser{ReadCloser: request.Body}
		if request.Body != nil {
			request.Body = body
		}
		writer := &statusWriter{ResponseWriter: response, status: http.StatusOK}

		next.ServeHTTP(writer, request)

		if request.Body != nil {
			recorder.BytesReceived(body.count)
		}
		recorder.ObserveLatency(time.Since(started))

		if writer.status >= 200 && writer.status < 300 {
			recorder.EventAccepted()
			return
		}
		recorder.EventRejected()
		switch writer.status {
		case http.StatusUnauthorized:
			recorder.AuthenticationFailure()
		case http.StatusForbidden:
			recorder.AuthorizationFailure()
		case http.StatusRequestEntityTooLarge:
			recorder.PayloadTooLarge()
		case http.StatusTooManyRequests:
			recorder.RateLimited()
		case http.StatusServiceUnavailable:
			recorder.PublishFailure()
		}
	})
}

type countingReadCloser struct {
	io.ReadCloser
	count uint64
}

func (r *countingReadCloser) Read(buffer []byte) (int, error) {
	count, err := r.ReadCloser.Read(buffer)
	r.count += uint64(count)
	return count, err
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(payload []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(payload)
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
