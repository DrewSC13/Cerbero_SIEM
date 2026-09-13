package sourcegap

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

type gapFixture struct {
	TenantID         string   `json:"tenant_id"`
	SourceID         string   `json:"source_id"`
	SensorID         string   `json:"sensor_id"`
	Sequences        []uint64 `json:"sequences"`
	ExpectedSequence uint64   `json:"expected_sequence"`
	ObservedSequence uint64   `json:"observed_sequence"`
}

func TestDetectorMatchesNativeSequenceGapFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/native-sequence-gap.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture gapFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Sequences) < 2 {
		t.Fatal("fixture requires at least two source sequence values")
	}

	detectedAt := time.Date(2026, time.September, 13, 14, 0, 0, 0, time.UTC)
	detector := New(func() time.Time { return detectedAt })
	metadata := ingestcore.AdmissionMetadata{
		TenantID: fixture.TenantID,
		SourceID: fixture.SourceID,
		SensorID: fixture.SensorID,
	}

	var detection *Detection
	for index := range fixture.Sequences {
		sequence := fixture.Sequences[index]
		detection = detector.Observe(metadata, &sequence)
	}
	if detection == nil {
		t.Fatal("fixture did not produce a gap")
	}
	if detection.ExpectedSequence != fixture.ExpectedSequence || detection.ObservedSequence != fixture.ObservedSequence {
		t.Fatalf("gap = expected:%d observed:%d, want expected:%d observed:%d",
			detection.ExpectedSequence,
			detection.ObservedSequence,
			fixture.ExpectedSequence,
			fixture.ObservedSequence,
		)
	}
	if detection.DetectedAt != detectedAt {
		t.Fatalf("detected_at = %s, want %s", detection.DetectedAt, detectedAt)
	}
}

func TestDetectorDoesNotInventSequenceOrTreatReplayAsGap(t *testing.T) {
	detector := New(nil)
	metadata := ingestcore.AdmissionMetadata{TenantID: "tenant", SourceID: "source", SensorID: "sensor"}
	if detection := detector.Observe(metadata, nil); detection != nil {
		t.Fatal("nil native sequence produced a gap")
	}
	sequence := uint64(10)
	if detection := detector.Observe(metadata, &sequence); detection != nil {
		t.Fatal("first sequence produced a gap")
	}
	sequence = 10
	if detection := detector.Observe(metadata, &sequence); detection != nil {
		t.Fatal("duplicate sequence produced a gap")
	}
	sequence = 9
	if detection := detector.Observe(metadata, &sequence); detection != nil {
		t.Fatal("backward/replayed sequence produced a gap")
	}
}

func TestDetectorKeepsSourcesIndependent(t *testing.T) {
	detector := New(nil)
	first := ingestcore.AdmissionMetadata{TenantID: "tenant", SourceID: "source-a", SensorID: "sensor"}
	second := ingestcore.AdmissionMetadata{TenantID: "tenant", SourceID: "source-b", SensorID: "sensor"}
	sequence := uint64(5)
	detector.Observe(first, &sequence)
	sequence = 100
	detector.Observe(second, &sequence)
	sequence = 6
	if detection := detector.Observe(first, &sequence); detection != nil {
		t.Fatalf("independent source state produced false gap: %+v", detection)
	}
}
