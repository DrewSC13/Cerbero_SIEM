package ingestapp

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	ProfileDevelopment = "DEVELOPMENT"
	ProfileProduction  = "PRODUCTION"
)

// Config contains runtime-only composition and frontend limits.
// Numeric values are deployment configuration, not contract constants.
type Config struct {
	SecurityProfile           string
	InsecureDevelopment       bool
	ListenAddress             string
	IngestPath                string
	MaxPayloadSize            uint64
	MaxConnectionRate         uint64
	MaxEventsPerSecond        uint64
	ReadTimeout               time.Duration
	IdleTimeout               time.Duration
	ConcurrentConnections     uint64
	ShutdownTimeout           time.Duration
	ComponentVersion          string
	PipelineVersion           string
	InstanceID                string
	NATSURL                   string
	NATSUser                  string
	NATSPassword              string
	DevelopmentTenantID       string
	DevelopmentSourceID       string
	DevelopmentSensorID       string
	DevelopmentRemoteIdentity string
}

// LoadConfig loads the ingest runtime from environment-style key lookup.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("environment lookup is required")
	}

	profile := strings.ToUpper(strings.TrimSpace(getenv("CERBERO_SECURITY_PROFILE")))
	if profile == ProfileProduction {
		return Config{}, errors.New("production JSON/HTTP source authentication is not implemented; refusing startup")
	}

	config := Config{
		SecurityProfile:           profile,
		InsecureDevelopment:       parseBool(getenv("CERBERO_INGEST_INSECURE_DEVELOPMENT")),
		ListenAddress:             strings.TrimSpace(getenv("CERBERO_INGEST_LISTEN_ADDRESS")),
		IngestPath:                strings.TrimSpace(getenv("CERBERO_INGEST_HTTP_PATH")),
		ComponentVersion:          strings.TrimSpace(getenv("CERBERO_INGEST_COMPONENT_VERSION")),
		PipelineVersion:           strings.TrimSpace(getenv("CERBERO_INGEST_PIPELINE_VERSION")),
		InstanceID:                strings.TrimSpace(getenv("CERBERO_INGEST_INSTANCE_ID")),
		NATSURL:                   strings.TrimSpace(getenv("CERBERO_NATS_URL")),
		NATSUser:                  strings.TrimSpace(getenv("NATS_INGEST_USER")),
		NATSPassword:              getenv("NATS_INGEST_PASSWORD"),
		DevelopmentTenantID:       strings.TrimSpace(getenv("CERBERO_INGEST_DEV_TENANT_ID")),
		DevelopmentSourceID:       strings.TrimSpace(getenv("CERBERO_INGEST_DEV_SOURCE_ID")),
		DevelopmentSensorID:       strings.TrimSpace(getenv("CERBERO_INGEST_DEV_SENSOR_ID")),
		DevelopmentRemoteIdentity: strings.TrimSpace(getenv("CERBERO_INGEST_DEV_REMOTE_IDENTITY")),
	}

	var err error
	if config.MaxPayloadSize, err = requiredUint(getenv, "CERBERO_INGEST_MAX_PAYLOAD_SIZE"); err != nil {
		return Config{}, err
	}
	if config.MaxConnectionRate, err = requiredUint(getenv, "CERBERO_INGEST_MAX_CONNECTION_RATE"); err != nil {
		return Config{}, err
	}
	if config.MaxEventsPerSecond, err = requiredUint(getenv, "CERBERO_INGEST_MAX_EVENTS_PER_SECOND"); err != nil {
		return Config{}, err
	}
	if config.ConcurrentConnections, err = requiredUint(getenv, "CERBERO_INGEST_CONCURRENT_CONNECTIONS"); err != nil {
		return Config{}, err
	}
	if config.ReadTimeout, err = requiredDuration(getenv, "CERBERO_INGEST_READ_TIMEOUT"); err != nil {
		return Config{}, err
	}
	if config.IdleTimeout, err = requiredDuration(getenv, "CERBERO_INGEST_IDLE_TIMEOUT"); err != nil {
		return Config{}, err
	}
	if config.ShutdownTimeout, err = requiredDuration(getenv, "CERBERO_INGEST_SHUTDOWN_TIMEOUT"); err != nil {
		return Config{}, err
	}

	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Validate rejects insecure or incomplete startup configuration.
func (c Config) Validate() error {
	if c.SecurityProfile == "" {
		return errors.New("CERBERO_SECURITY_PROFILE is required")
	}
	if c.SecurityProfile == ProfileProduction {
		return errors.New("production JSON/HTTP source authentication is not implemented; refusing startup")
	}
	if c.SecurityProfile != ProfileDevelopment {
		return fmt.Errorf("unsupported CERBERO_SECURITY_PROFILE %q", c.SecurityProfile)
	}
	if !c.InsecureDevelopment {
		return errors.New("DEVELOPMENT profile requires explicit CERBERO_INGEST_INSECURE_DEVELOPMENT=1")
	}
	if err := validateLoopbackAddress(c.ListenAddress); err != nil {
		return err
	}
	if c.IngestPath == "" || !strings.HasPrefix(c.IngestPath, "/") {
		return errors.New("CERBERO_INGEST_HTTP_PATH must be an absolute path")
	}
	if c.IngestPath == "/livez" || c.IngestPath == "/readyz" {
		return errors.New("CERBERO_INGEST_HTTP_PATH conflicts with a health endpoint")
	}

	required := []struct {
		name  string
		value string
	}{
		{"CERBERO_INGEST_COMPONENT_VERSION", c.ComponentVersion},
		{"CERBERO_INGEST_PIPELINE_VERSION", c.PipelineVersion},
		{"CERBERO_INGEST_INSTANCE_ID", c.InstanceID},
		{"CERBERO_NATS_URL", c.NATSURL},
		{"NATS_INGEST_USER", c.NATSUser},
		{"NATS_INGEST_PASSWORD", c.NATSPassword},
		{"CERBERO_INGEST_DEV_TENANT_ID", c.DevelopmentTenantID},
		{"CERBERO_INGEST_DEV_SOURCE_ID", c.DevelopmentSourceID},
		{"CERBERO_INGEST_DEV_SENSOR_ID", c.DevelopmentSensorID},
		{"CERBERO_INGEST_DEV_REMOTE_IDENTITY", c.DevelopmentRemoteIdentity},
	}
	for _, item := range required {
		if item.value == "" {
			return fmt.Errorf("%s is required", item.name)
		}
	}
	if c.MaxPayloadSize == 0 || c.MaxConnectionRate == 0 || c.MaxEventsPerSecond == 0 ||
		c.ConcurrentConnections == 0 || c.ReadTimeout <= 0 || c.IdleTimeout <= 0 || c.ShutdownTimeout <= 0 {
		return errors.New("all ingest limits and timeouts must be greater than zero")
	}
	return nil
}

func validateLoopbackAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("CERBERO_INGEST_LISTEN_ADDRESS must be host:port: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("insecure DEVELOPMENT ingest must bind to an explicit loopback IP")
	}
	return nil
}

func requiredUint(getenv func(string) string, name string) (uint64, error) {
	raw := strings.TrimSpace(getenv(name))
	if raw == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func requiredDuration(getenv func(string) string, name string) (time.Duration, error) {
	raw := strings.TrimSpace(getenv(name))
	if raw == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return value, nil
}

func parseBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
