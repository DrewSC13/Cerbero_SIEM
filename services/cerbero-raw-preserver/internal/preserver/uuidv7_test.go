package preserver

import (
	"bytes"
	"strings"
	"testing"
	"time"

	contractvalidation "cerbero/services/internal/contracts/validation"
)

type uuidFixedClock struct{ now time.Time }

func (c uuidFixedClock) Now() time.Time { return c.now }

func TestUUIDv7GeneratorCreatesGovernedIdentity(t *testing.T) {
	generator := NewUUIDv7Generator(
		uuidFixedClock{now: time.UnixMilli(1_789_000_000_123).UTC()},
		bytes.NewReader(bytes.Repeat([]byte{0x42}, 16)),
	)
	value, err := generator.New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := contractvalidation.UUIDv7("generated", value); err != nil {
		t.Fatalf("UUIDv7 validation = %v", err)
	}
	if value != strings.ToLower(value) {
		t.Fatalf("UUIDv7 = %q, expected lowercase", value)
	}
}

func TestUUIDv7GeneratorRejectsEntropyFailure(t *testing.T) {
	generator := NewUUIDv7Generator(
		uuidFixedClock{now: time.UnixMilli(1_789_000_000_123).UTC()},
		bytes.NewReader(nil),
	)
	if _, err := generator.New(); err == nil {
		t.Fatal("New() accepted missing randomness")
	}
}
