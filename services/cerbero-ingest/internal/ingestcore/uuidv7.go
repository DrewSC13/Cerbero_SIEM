package ingestcore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

const maxUUIDv7UnixMilliseconds = int64(1<<48 - 1)

// Clock supplies wall-clock time to ingest code and is injectable for deterministic tests.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

// IDGenerator creates CERBERO-owned identifiers.
type IDGenerator interface {
	New() (string, error)
}

// UUIDv7Generator creates RFC 9562 UUIDv7 values with cryptographic randomness.
type UUIDv7Generator struct {
	clock  Clock
	random io.Reader
}

// NewUUIDv7Generator returns a UUIDv7 generator. Nil dependencies use the system clock and crypto/rand.
func NewUUIDv7Generator(clock Clock, random io.Reader) UUIDv7Generator {
	if clock == nil {
		clock = systemClock{}
	}
	if random == nil {
		random = rand.Reader
	}
	return UUIDv7Generator{clock: clock, random: random}
}

// New creates one canonical lowercase RFC 9562 UUIDv7 value.
func (g UUIDv7Generator) New() (string, error) {
	milliseconds := g.clock.Now().UTC().UnixMilli()
	if milliseconds < 0 || milliseconds > maxUUIDv7UnixMilliseconds {
		return "", fmt.Errorf("UUIDv7 timestamp out of 48-bit Unix-millisecond range: %d", milliseconds)
	}

	var value [16]byte
	if _, err := io.ReadFull(g.random, value[:]); err != nil {
		return "", fmt.Errorf("read UUIDv7 randomness: %w", err)
	}

	timestamp := uint64(milliseconds)
	value[0] = byte(timestamp >> 40)
	value[1] = byte(timestamp >> 32)
	value[2] = byte(timestamp >> 24)
	value[3] = byte(timestamp >> 16)
	value[4] = byte(timestamp >> 8)
	value[5] = byte(timestamp)
	value[6] = value[6]&0x0f | 0x70
	value[8] = value[8]&0x3f | 0x80

	var encoded [36]byte
	hex.Encode(encoded[0:8], value[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], value[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], value[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], value[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], value[10:16])
	return string(encoded[:]), nil
}
