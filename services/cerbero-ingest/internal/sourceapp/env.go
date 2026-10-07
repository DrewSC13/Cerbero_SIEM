package sourceapp

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// LoadConfig loads common non-HTTP source runtime values from environment-style lookup.
func LoadConfig(
	getenv func(string) string,
	transport string,
	prefix string,
) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("environment lookup is required")
	}
	if transport == "" || prefix == "" {
		return Config{}, errors.New("source transport and environment prefix are required")
	}

	maxPayload, err := requiredUint(getenv, "CERBERO_INGEST_MAX_PAYLOAD_SIZE")
	if err != nil {
		return Config{}, err
	}
	maxEvents, err := requiredUint(getenv, "CERBERO_INGEST_MAX_EVENTS_PER_SECOND")
	if err != nil {
		return Config{}, err
	}
	connectTimeout, err := requiredDuration(getenv, "CERBERO_INGEST_READ_TIMEOUT")
	if err != nil {
		return Config{}, err
	}

	config := Config{
		SecurityProfile:     strings.TrimSpace(getenv("CERBERO_SECURITY_PROFILE")),
		InsecureDevelopment: parseBool(getenv("CERBERO_INGEST_INSECURE_DEVELOPMENT")),
		Transport:           transport,
		TenantID:            strings.TrimSpace(getenv(prefix + "_DEV_TENANT_ID")),
		SourceID:            strings.TrimSpace(getenv(prefix + "_DEV_SOURCE_ID")),
		SensorID:            strings.TrimSpace(getenv(prefix + "_DEV_SENSOR_ID")),
		RemoteIdentity:      strings.TrimSpace(getenv(prefix + "_DEV_REMOTE_IDENTITY")),
		MaxPayloadSize:      maxPayload,
		MaxEventsPerSecond:  maxEvents,
		ComponentVersion:    strings.TrimSpace(getenv("CERBERO_INGEST_COMPONENT_VERSION")),
		PipelineVersion:     strings.TrimSpace(getenv("CERBERO_INGEST_PIPELINE_VERSION")),
		InstanceID:          strings.TrimSpace(getenv(prefix + "_INSTANCE_ID")),
		NATSURL:             strings.TrimSpace(getenv("CERBERO_NATS_URL")),
		NATSUser:            strings.TrimSpace(getenv("NATS_INGEST_USER")),
		NATSPassword:        getenv("NATS_INGEST_PASSWORD"),
		ConnectTimeout:      connectTimeout,
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
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
