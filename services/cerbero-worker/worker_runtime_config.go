package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const workerDevelopmentSecurityProfile = "DEVELOPMENT"

type workerRuntimeConfig struct {
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

	ComponentVersion string
	InstanceID       string

	ConnectTimeout       time.Duration
	MaxAutomaticAttempts uint32
	NAKDelay             time.Duration
}

func loadWorkerRuntimeConfig(getenv func(string) string) (workerRuntimeConfig, error) {
	if getenv == nil {
		return workerRuntimeConfig{}, errors.New("environment lookup is required")
	}

	port, err := parseWorkerPort(getenv("POSTGRES_PORT"))
	if err != nil {
		return workerRuntimeConfig{}, err
	}
	connectTimeout, err := parseWorkerPositiveDuration(
		"CERBERO_WORKER_CONNECT_TIMEOUT",
		getenv("CERBERO_WORKER_CONNECT_TIMEOUT"),
	)
	if err != nil {
		return workerRuntimeConfig{}, err
	}
	maxAttempts, err := parseWorkerUint32(
		"CERBERO_WORKER_REPLAY_MAX_AUTOMATIC_ATTEMPTS",
		getenv("CERBERO_WORKER_REPLAY_MAX_AUTOMATIC_ATTEMPTS"),
	)
	if err != nil {
		return workerRuntimeConfig{}, err
	}
	nakDelay, err := parseWorkerPositiveDuration(
		"CERBERO_WORKER_REPLAY_NAK_DELAY",
		getenv("CERBERO_WORKER_REPLAY_NAK_DELAY"),
	)
	if err != nil {
		return workerRuntimeConfig{}, err
	}

	config := workerRuntimeConfig{
		DevelopmentMode: getenv("CERBERO_DEV_MODE") == "1",
		SecurityProfile: strings.TrimSpace(getenv("CERBERO_SECURITY_PROFILE")),

		NATSURL:      strings.TrimSpace(getenv("CERBERO_NATS_URL")),
		NATSUser:     strings.TrimSpace(getenv("NATS_WORKER_USER")),
		NATSPassword: getenv("NATS_WORKER_PASSWORD"),

		PostgresHost:     strings.TrimSpace(getenv("POSTGRES_HOST")),
		PostgresPort:     port,
		PostgresDatabase: strings.TrimSpace(getenv("POSTGRES_DB")),
		PostgresUser:     strings.TrimSpace(getenv("POSTGRES_WORKER_USER")),
		PostgresPassword: getenv("POSTGRES_WORKER_PASSWORD"),

		ComponentVersion: strings.TrimSpace(getenv("CERBERO_WORKER_COMPONENT_VERSION")),
		InstanceID:       strings.TrimSpace(getenv("CERBERO_WORKER_INSTANCE_ID")),

		ConnectTimeout:       connectTimeout,
		MaxAutomaticAttempts: maxAttempts,
		NAKDelay:             nakDelay,
	}
	if err := config.validate(); err != nil {
		return workerRuntimeConfig{}, err
	}
	return config, nil
}

func (c workerRuntimeConfig) validate() error {
	if !c.DevelopmentMode {
		return errors.New("cerbero-worker replay runtime requires CERBERO_DEV_MODE=1")
	}
	if c.SecurityProfile != workerDevelopmentSecurityProfile {
		return fmt.Errorf(
			"cerbero-worker replay runtime requires CERBERO_SECURITY_PROFILE=%s",
			workerDevelopmentSecurityProfile,
		)
	}
	if c.NATSURL == "" {
		return errors.New("CERBERO_NATS_URL is required")
	}
	parsedNATS, err := url.Parse(c.NATSURL)
	if err != nil || parsedNATS.Scheme != "nats" || parsedNATS.Host == "" {
		return errors.New("CERBERO_NATS_URL must be a nats:// URL")
	}
	if c.NATSUser == "" {
		return errors.New("NATS_WORKER_USER is required")
	}
	if c.NATSPassword == "" {
		return errors.New("NATS_WORKER_PASSWORD is required")
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
		return errors.New("POSTGRES_WORKER_USER is required")
	}
	if c.PostgresPassword == "" {
		return errors.New("POSTGRES_WORKER_PASSWORD is required")
	}
	if c.ComponentVersion == "" {
		return errors.New("CERBERO_WORKER_COMPONENT_VERSION is required")
	}
	if c.InstanceID == "" {
		return errors.New("CERBERO_WORKER_INSTANCE_ID is required")
	}
	if c.ConnectTimeout <= 0 {
		return errors.New("worker connect timeout must be positive")
	}
	if c.NAKDelay <= 0 {
		return errors.New("worker replay NAK delay must be positive")
	}
	return nil
}

func (c workerRuntimeConfig) postgresDSN() string {
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

func parseWorkerPort(value string) (uint16, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("POSTGRES_PORT is required")
	}
	parsed, err := strconv.ParseUint(value, 10, 16)
	if err != nil || parsed == 0 {
		return 0, errors.New("POSTGRES_PORT must be an integer in 1..65535")
	}
	return uint16(parsed), nil
}

func parseWorkerPositiveDuration(name, value string) (time.Duration, error) {
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

func parseWorkerUint32(name, value string) (uint32, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer in 0..4294967295", name)
	}
	return uint32(parsed), nil
}
