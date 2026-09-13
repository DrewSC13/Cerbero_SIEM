package preserver

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// RawPreserverConsumerName is the stable durable-consumer and idempotency identity.
	RawPreserverConsumerName = "raw-preserver"

	developmentSecurityProfile = "DEVELOPMENT"
)

// RuntimeConfig is the DEVELOPMENT filesystem Raw Store runtime configuration.
// Production storage, secret delivery, and deployment topology remain outside this M3 runtime.
type RuntimeConfig struct {
	DevelopmentMode bool
	SecurityProfile string

	NATSURL      string
	NATSUser     string
	NATSPassword string

	PostgresHost     string
	PostgresPort     uint16
	PostgresDatabase string
	PostgresUser     string
	PostgresPassword string

	RawStorePath     string
	ComponentVersion string
	InstanceID       string

	ConnectTimeout time.Duration
	RetryMinDelay  time.Duration
	RetryMaxDelay  time.Duration
}

// LoadRuntimeConfig loads the explicit development runtime contract from environment lookup.
func LoadRuntimeConfig(getenv func(string) string) (RuntimeConfig, error) {
	if getenv == nil {
		return RuntimeConfig{}, errors.New("environment lookup is required")
	}

	port, err := parsePort(getenv("POSTGRES_PORT"))
	if err != nil {
		return RuntimeConfig{}, err
	}
	connectTimeout, err := parsePositiveDuration(
		"CERBERO_RAW_PRESERVER_CONNECT_TIMEOUT",
		getenv("CERBERO_RAW_PRESERVER_CONNECT_TIMEOUT"),
	)
	if err != nil {
		return RuntimeConfig{}, err
	}
	retryMin, err := parsePositiveDuration(
		"CERBERO_RAW_PRESERVER_RETRY_MIN_DELAY",
		getenv("CERBERO_RAW_PRESERVER_RETRY_MIN_DELAY"),
	)
	if err != nil {
		return RuntimeConfig{}, err
	}
	retryMax, err := parsePositiveDuration(
		"CERBERO_RAW_PRESERVER_RETRY_MAX_DELAY",
		getenv("CERBERO_RAW_PRESERVER_RETRY_MAX_DELAY"),
	)
	if err != nil {
		return RuntimeConfig{}, err
	}

	config := RuntimeConfig{
		DevelopmentMode: getenv("CERBERO_DEV_MODE") == "1",
		SecurityProfile: strings.TrimSpace(getenv("CERBERO_SECURITY_PROFILE")),

		NATSURL:      strings.TrimSpace(getenv("CERBERO_NATS_URL")),
		NATSUser:     strings.TrimSpace(getenv("NATS_RAW_PRESERVER_USER")),
		NATSPassword: getenv("NATS_RAW_PRESERVER_PASSWORD"),

		PostgresHost:     strings.TrimSpace(getenv("POSTGRES_HOST")),
		PostgresPort:     port,
		PostgresDatabase: strings.TrimSpace(getenv("POSTGRES_DB")),
		PostgresUser:     strings.TrimSpace(getenv("POSTGRES_RAW_PRESERVER_USER")),
		PostgresPassword: getenv("POSTGRES_RAW_PRESERVER_PASSWORD"),

		RawStorePath:     strings.TrimSpace(getenv("CERBERO_RAW_STORE_PATH")),
		ComponentVersion: strings.TrimSpace(getenv("CERBERO_RAW_PRESERVER_COMPONENT_VERSION")),
		InstanceID:       strings.TrimSpace(getenv("CERBERO_RAW_PRESERVER_INSTANCE_ID")),

		ConnectTimeout: connectTimeout,
		RetryMinDelay:  retryMin,
		RetryMaxDelay:  retryMax,
	}
	if err := config.Validate(); err != nil {
		return RuntimeConfig{}, err
	}
	return config, nil
}

// Validate rejects incomplete or non-development composition.
func (c RuntimeConfig) Validate() error {
	if !c.DevelopmentMode {
		return errors.New("raw-preserver filesystem runtime requires CERBERO_DEV_MODE=1")
	}
	if c.SecurityProfile != developmentSecurityProfile {
		return fmt.Errorf("raw-preserver filesystem runtime requires CERBERO_SECURITY_PROFILE=%s", developmentSecurityProfile)
	}
	if c.NATSURL == "" {
		return errors.New("CERBERO_NATS_URL is required")
	}
	parsedNATS, err := url.Parse(c.NATSURL)
	if err != nil || parsedNATS.Scheme != "nats" || parsedNATS.Host == "" {
		return errors.New("CERBERO_NATS_URL must be a nats:// URL")
	}
	if c.NATSUser == "" {
		return errors.New("NATS_RAW_PRESERVER_USER is required")
	}
	if c.NATSPassword == "" {
		return errors.New("NATS_RAW_PRESERVER_PASSWORD is required")
	}
	if c.PostgresHost == "" {
		return errors.New("POSTGRES_HOST is required")
	}
	if c.PostgresPort == 0 {
		return errors.New("POSTGRES_PORT is required")
	}
	if c.PostgresDatabase == "" {
		return errors.New("POSTGRES_DB is required")
	}
	if c.PostgresUser == "" {
		return errors.New("POSTGRES_RAW_PRESERVER_USER is required")
	}
	if c.PostgresPassword == "" {
		return errors.New("POSTGRES_RAW_PRESERVER_PASSWORD is required")
	}
	if c.RawStorePath == "" {
		return errors.New("CERBERO_RAW_STORE_PATH is required")
	}
	if c.ComponentVersion == "" {
		return errors.New("CERBERO_RAW_PRESERVER_COMPONENT_VERSION is required")
	}
	if c.InstanceID == "" {
		return errors.New("CERBERO_RAW_PRESERVER_INSTANCE_ID is required")
	}
	if c.ConnectTimeout <= 0 {
		return errors.New("raw-preserver connect timeout must be positive")
	}
	if c.RetryMinDelay <= 0 {
		return errors.New("raw-preserver retry minimum delay must be positive")
	}
	if c.RetryMaxDelay < c.RetryMinDelay {
		return errors.New("raw-preserver retry maximum delay must be >= minimum delay")
	}
	return nil
}

func (c RuntimeConfig) postgresDSN() string {
	connection := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.PostgresUser, c.PostgresPassword),
		Host:   net.JoinHostPort(c.PostgresHost, strconv.Itoa(int(c.PostgresPort))),
		Path:   c.PostgresDatabase,
	}
	query := connection.Query()
	query.Set("sslmode", "disable")
	connection.RawQuery = query.Encode()
	return connection.String()
}

func parsePort(value string) (uint16, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("POSTGRES_PORT is required")
	}
	parsed, err := strconv.ParseUint(value, 10, 16)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("POSTGRES_PORT must be an integer in 1..65535")
	}
	return uint16(parsed), nil
}

func parsePositiveDuration(name, value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return duration, nil
}
