package ingestapp

import (
	"context"
	"net"
	"sync"
	"time"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

type tokenBucket struct {
	mu       sync.Mutex
	rate     float64
	capacity float64
	tokens   float64
	last     time.Time
	now      func() time.Time
}

func newTokenBucket(rate uint64, now func() time.Time) *tokenBucket {
	if now == nil {
		now = time.Now
	}
	current := now()
	return &tokenBucket{
		rate:     float64(rate),
		capacity: float64(rate),
		tokens:   float64(rate),
		last:     current,
		now:      now,
	}
}

func (b *tokenBucket) take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

type admissionRateLimiter struct {
	bucket *tokenBucket
}

func (l *admissionRateLimiter) Allow(context.Context, ingestcore.Principal, ingestcore.AdmissionMetadata) bool {
	return l.bucket.take()
}

type limitedListener struct {
	net.Listener
	connectionRate *tokenBucket
	concurrent     chan struct{}
}

func newLimitedListener(listener net.Listener, connectionRate, concurrent uint64) net.Listener {
	return &limitedListener{
		Listener:       listener,
		connectionRate: newTokenBucket(connectionRate, nil),
		concurrent:     make(chan struct{}, concurrent),
	}
}

func (l *limitedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if !l.connectionRate.take() {
			_ = conn.Close()
			continue
		}
		select {
		case l.concurrent <- struct{}{}:
			return &trackedConn{Conn: conn, release: func() { <-l.concurrent }}, nil
		default:
			_ = conn.Close()
		}
	}
}

type trackedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
