package ingestapp

import (
	"testing"
	"time"
)

func TestTokenBucketEnforcesConfiguredRateAndRefills(t *testing.T) {
	now := time.Unix(0, 0)
	bucket := newTokenBucket(2, func() time.Time { return now })

	if !bucket.take() || !bucket.take() {
		t.Fatal("initial token bucket capacity was not available")
	}
	if bucket.take() {
		t.Fatal("token bucket allowed more than configured burst")
	}

	now = now.Add(500 * time.Millisecond)
	if !bucket.take() {
		t.Fatal("token bucket did not refill one token after half a second at 2/s")
	}
	if bucket.take() {
		t.Fatal("token bucket refilled too many tokens")
	}
}
