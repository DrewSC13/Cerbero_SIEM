package journald

import (
	"context"
	"errors"
	"fmt"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

const (
	// Transport identifies journald source metadata at the ingest boundary.
	Transport = "journald"
)

// Entry is the M2 collector contract for one journald evidence unit.
//
// A collector preserves exactly one evidence form:
//   - OriginalFields: the original journald field set with byte values; or
//   - RawRepresentation: a canonical raw representation defined by a future
//     source-specific collector contract.
//
// This contract intentionally does not define field canonicalization, cursor to
// sequence-number mapping, OCSF mapping, or host journal access.
type Entry struct {
	Source            ingestcore.AdmissionMetadata
	Cursor            string
	OriginalFields    map[string][]byte
	RawRepresentation []byte
}

// Collector is the source boundary implemented by a future journald reader.
type Collector interface {
	Next(context.Context) (Entry, error)
}

// Validate checks only the M2 evidence-shape invariants without inventing
// source-specific canonicalization policy.
func (e Entry) Validate() error {
	if e.Source.Transport != Transport {
		return fmt.Errorf("journald source transport must be %q", Transport)
	}

	hasFields := e.OriginalFields != nil
	hasRaw := e.RawRepresentation != nil
	if hasFields == hasRaw {
		return errors.New("journald entry must preserve exactly one evidence form")
	}
	if hasFields {
		if len(e.OriginalFields) == 0 {
			return errors.New("journald original field set must not be empty")
		}
		for name := range e.OriginalFields {
			if name == "" {
				return errors.New("journald field name must not be empty")
			}
		}
		return nil
	}
	if len(e.RawRepresentation) == 0 {
		return errors.New("journald canonical raw representation must not be empty")
	}
	return nil
}
