package sourcegap

import (
	"math"
	"sync"
	"time"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

// Detection is an in-process source-integrity observation, not a bus payload contract.
type Detection struct {
	TenantID         string
	SourceID         string
	SensorID         string
	ExpectedSequence uint64
	ObservedSequence uint64
	DetectedAt       time.Time
}

type sourceKey struct {
	tenantID string
	sourceID string
	sensorID string
}

// Detector tracks native source sequences independently per tenant/source/sensor.
type Detector struct {
	mu   sync.Mutex
	last map[sourceKey]uint64
	now  func() time.Time
}

// New creates a detector. A nil clock uses time.Now.
func New(now func() time.Time) *Detector {
	if now == nil {
		now = time.Now
	}
	return &Detector{
		last: make(map[sourceKey]uint64),
		now:  now,
	}
}

// Observe returns a gap only for a forward jump greater than one.
// Nil sequence values, duplicates, and backward/replayed values do not create gaps.
func (d *Detector) Observe(metadata ingestcore.AdmissionMetadata, sequence *uint64) *Detection {
	if sequence == nil {
		return nil
	}

	key := sourceKey{
		tenantID: metadata.TenantID,
		sourceID: metadata.SourceID,
		sensorID: metadata.SensorID,
	}
	observed := *sequence

	d.mu.Lock()
	defer d.mu.Unlock()

	previous, exists := d.last[key]
	if !exists {
		d.last[key] = observed
		return nil
	}
	if observed <= previous {
		return nil
	}
	if previous == math.MaxUint64 {
		d.last[key] = observed
		return nil
	}

	expected := previous + 1
	d.last[key] = observed
	if observed <= expected {
		return nil
	}
	return &Detection{
		TenantID:         metadata.TenantID,
		SourceID:         metadata.SourceID,
		SensorID:         metadata.SensorID,
		ExpectedSequence: expected,
		ObservedSequence: observed,
		DetectedAt:       d.now().UTC(),
	}
}
