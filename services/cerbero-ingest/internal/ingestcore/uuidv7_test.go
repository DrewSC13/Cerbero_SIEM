package ingestcore

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	contractvalidation "cerbero/services/internal/contracts/validation"
)

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("randomness unavailable")
}

func TestUUIDv7GeneratorUsesUnixMillisecondsVersionAndRFCVariant(t *testing.T) {
	instant := time.Date(2026, time.September, 13, 0, 30, 0, 0, time.UTC)
	generator := NewUUIDv7Generator(fixedClock{value: instant}, bytes.NewReader(make([]byte, 16)))

	value, err := generator.New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := contractvalidation.UUIDv7("id", value); err != nil {
		t.Fatalf("generated UUID is not valid UUIDv7: %v", err)
	}
	compact := strings.ReplaceAll(value, "-", "")
	wantTimestamp := hex12(uint64(instant.UnixMilli()))
	if compact[:12] != wantTimestamp {
		t.Fatalf("UUID timestamp prefix = %q, want %q", compact[:12], wantTimestamp)
	}
	if value[14] != '7' {
		t.Fatalf("version nibble = %q, want 7", value[14])
	}
	if value[19] != '8' {
		t.Fatalf("variant nibble = %q, want 8 for zero random input", value[19])
	}
}

func TestUUIDv7GeneratorFailsClosedWhenRandomnessIsUnavailable(t *testing.T) {
	generator := NewUUIDv7Generator(
		fixedClock{value: time.Date(2026, time.September, 13, 0, 30, 0, 0, time.UTC)},
		failingReader{},
	)
	if _, err := generator.New(); err == nil {
		t.Fatalf("New() error = nil, want randomness error")
	}
}

func hex12(value uint64) string {
	const alphabet = "0123456789abcdef"
	var encoded [12]byte
	for index := len(encoded) - 1; index >= 0; index-- {
		encoded[index] = alphabet[value&0xf]
		value >>= 4
	}
	return string(encoded[:])
}
