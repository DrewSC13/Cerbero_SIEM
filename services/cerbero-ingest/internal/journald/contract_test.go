package journald

import (
	"bytes"
	"testing"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

func journaldSource() ingestcore.AdmissionMetadata {
	return ingestcore.AdmissionMetadata{
		TenantID:       "tenant-a",
		SourceID:       "journald-host-a",
		SensorID:       "sensor-a",
		RemoteIdentity: "host-a",
		Transport:      Transport,
	}
}

func TestEntryAcceptsOriginalFieldSetWithoutCanonicalizingValues(t *testing.T) {
	message := []byte{'f', 'a', 'i', 'l', 'e', 'd', 0, 'l', 'o', 'g', 'i', 'n'}
	entry := Entry{
		Source: journaldSource(),
		Cursor: "s=0123456789abcdef;i=42",
		OriginalFields: map[string][]byte{
			"MESSAGE":       message,
			"_SYSTEMD_UNIT": []byte("sshd.service"),
		},
	}

	if err := entry.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !bytes.Equal(entry.OriginalFields["MESSAGE"], message) {
		t.Fatal("journald MESSAGE bytes were changed")
	}
	if entry.Cursor != "s=0123456789abcdef;i=42" {
		t.Fatalf("cursor = %q", entry.Cursor)
	}
}

func TestEntryAcceptsPreexistingCanonicalRawRepresentation(t *testing.T) {
	raw := []byte("collector-defined-canonical-bytes\n")
	entry := Entry{
		Source:            journaldSource(),
		RawRepresentation: raw,
	}
	if err := entry.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !bytes.Equal(entry.RawRepresentation, raw) {
		t.Fatal("canonical raw representation changed")
	}
}

func TestEntryRejectsAmbiguousOrMissingEvidenceForms(t *testing.T) {
	tests := []Entry{
		{Source: journaldSource()},
		{
			Source:            journaldSource(),
			OriginalFields:    map[string][]byte{"MESSAGE": []byte("x")},
			RawRepresentation: []byte("x"),
		},
	}
	for _, entry := range tests {
		if err := entry.Validate(); err == nil {
			t.Fatal("Validate() accepted ambiguous or missing journald evidence")
		}
	}
}

func TestEntryRequiresJournaldTransport(t *testing.T) {
	entry := Entry{
		Source: ingestcore.AdmissionMetadata{Transport: "json-http"},
		OriginalFields: map[string][]byte{
			"MESSAGE": []byte("x"),
		},
	}
	if err := entry.Validate(); err == nil {
		t.Fatal("Validate() accepted non-journald transport")
	}
}
