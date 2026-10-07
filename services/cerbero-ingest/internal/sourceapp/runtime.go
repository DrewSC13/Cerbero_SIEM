package sourceapp

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"cerbero/services/cerbero-ingest/internal/eventbus"
	"cerbero/services/cerbero-ingest/internal/ingestcore"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const developmentProfile = "DEVELOPMENT"

// Config contains the common DEVELOPMENT composition used by non-HTTP source frontends.
type Config struct {
	SecurityProfile     string
	InsecureDevelopment bool
	Transport           string
	TenantID            string
	SourceID            string
	SensorID            string
	RemoteIdentity      string
	MaxPayloadSize      uint64
	MaxEventsPerSecond  uint64
	ComponentVersion    string
	PipelineVersion     string
	InstanceID          string
	NATSURL             string
	NATSUser            string
	NATSPassword        string
	ConnectTimeout      time.Duration
}

// Runtime owns a common IngestCore, durable JetStream acceptor, and source metadata.
type Runtime struct {
	Core     *ingestcore.Core
	Acceptor *eventbus.JetStreamAcceptor
	Metadata ingestcore.AdmissionMetadata

	connection *nats.Conn
}

// Open validates the explicit DEVELOPMENT source boundary and connects it to JetStream.
func Open(config Config) (*Runtime, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	connection, err := nats.Connect(
		config.NATSURL,
		nats.UserInfo(config.NATSUser, config.NATSPassword),
		nats.Name(config.InstanceID),
		nats.Timeout(config.ConnectTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("connect source frontend to NATS: %w", err)
	}

	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("create source frontend JetStream client: %w", err)
	}
	acceptor, err := eventbus.NewJetStreamAcceptor(js)
	if err != nil {
		connection.Close()
		return nil, err
	}

	metadata := ingestcore.AdmissionMetadata{
		TenantID:       config.TenantID,
		SourceID:       config.SourceID,
		SensorID:       config.SensorID,
		RemoteIdentity: config.RemoteIdentity,
		Transport:      config.Transport,
	}
	identity := &staticIdentity{metadata: metadata}
	core, err := ingestcore.New(ingestcore.Config{
		MaxPayloadSize:   config.MaxPayloadSize,
		ComponentVersion: config.ComponentVersion,
		PipelineVersion:  config.PipelineVersion,
		InstanceID:       config.InstanceID,
		Authenticator:    identity,
		Authorizer:       identity,
		Limiter: &rateLimiter{
			bucket: newTokenBucket(config.MaxEventsPerSecond),
		},
	})
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("configure source IngestCore: %w", err)
	}

	return &Runtime{
		Core:       core,
		Acceptor:   acceptor,
		Metadata:   metadata,
		connection: connection,
	}, nil
}

// Close releases the NATS connection.
func (r *Runtime) Close() {
	if r != nil && r.connection != nil {
		r.connection.Close()
	}
}

// Validate rejects unsupported or incomplete source runtime configuration.
func (c Config) Validate() error {
	if c.SecurityProfile != developmentProfile {
		return fmt.Errorf("source frontend requires CERBERO_SECURITY_PROFILE=%s", developmentProfile)
	}
	if !c.InsecureDevelopment {
		return errors.New("source frontend DEVELOPMENT profile requires explicit insecure-development opt-in")
	}
	if c.Transport == "" || c.TenantID == "" || c.SourceID == "" ||
		c.SensorID == "" || c.RemoteIdentity == "" {
		return errors.New("source frontend transport and identity metadata are required")
	}
	if c.MaxPayloadSize == 0 || c.MaxEventsPerSecond == 0 {
		return errors.New("source frontend payload and event-rate limits must be greater than zero")
	}
	if c.ComponentVersion == "" || c.PipelineVersion == "" || c.InstanceID == "" {
		return errors.New("source frontend provenance metadata are required")
	}
	parsed, err := url.Parse(c.NATSURL)
	if err != nil || parsed.Scheme != "nats" || parsed.Host == "" {
		return errors.New("CERBERO_NATS_URL must be a nats:// URL")
	}
	if c.NATSUser == "" || c.NATSPassword == "" {
		return errors.New("source frontend NATS credentials are required")
	}
	if c.ConnectTimeout <= 0 {
		return errors.New("source frontend connect timeout must be positive")
	}
	return nil
}

type staticIdentity struct {
	metadata ingestcore.AdmissionMetadata
}

func (i *staticIdentity) Authenticate(
	_ context.Context,
	metadata ingestcore.AdmissionMetadata,
) (ingestcore.Principal, error) {
	if metadata != i.metadata {
		return ingestcore.Principal{}, errors.New("development source identity mismatch")
	}
	return ingestcore.Principal{ID: i.metadata.RemoteIdentity}, nil
}

func (i *staticIdentity) Authorize(
	_ context.Context,
	principal ingestcore.Principal,
	permission string,
	metadata ingestcore.AdmissionMetadata,
) error {
	if permission != ingestcore.PermissionEventsIngest {
		return errors.New("unsupported development source permission")
	}
	if principal.ID != i.metadata.RemoteIdentity || metadata != i.metadata {
		return errors.New("development source authorization scope mismatch")
	}
	return nil
}

type tokenBucket struct {
	mu       sync.Mutex
	rate     float64
	capacity float64
	tokens   float64
	last     time.Time
}

func newTokenBucket(rate uint64) *tokenBucket {
	now := time.Now()
	value := float64(rate)
	return &tokenBucket{
		rate:     value,
		capacity: value,
		tokens:   value,
		last:     now,
	}
}

func (b *tokenBucket) take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
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

type rateLimiter struct {
	bucket *tokenBucket
}

func (l *rateLimiter) Allow(
	context.Context,
	ingestcore.Principal,
	ingestcore.AdmissionMetadata,
) bool {
	return l.bucket.take()
}
