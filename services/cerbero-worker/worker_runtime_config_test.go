package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadWorkerRuntimeConfig(t *testing.T) {
	values := map[string]string{
		"CERBERO_DEV_MODE":                             "1",
		"CERBERO_SECURITY_PROFILE":                     "DEVELOPMENT",
		"CERBERO_NATS_URL":                             "nats://127.0.0.1:4222",
		"NATS_WORKER_USER":                             "cerbero_worker",
		"NATS_WORKER_PASSWORD":                         "secret",
		"POSTGRES_HOST":                                "127.0.0.1",
		"POSTGRES_PORT":                                "55432",
		"POSTGRES_DB":                                  "cerbero",
		"POSTGRES_WORKER_USER":                         "cerbero_worker_dev",
		"POSTGRES_WORKER_PASSWORD":                     "pg-secret",
		"CERBERO_WORKER_COMPONENT_VERSION":             "0.1.0-dev",
		"CERBERO_WORKER_INSTANCE_ID":                   "worker-dev-1",
		"CERBERO_WORKER_CONNECT_TIMEOUT":               "5s",
		"CERBERO_WORKER_REPLAY_MAX_AUTOMATIC_ATTEMPTS": "2",
		"CERBERO_WORKER_REPLAY_NAK_DELAY":              "7s",
	}
	config, err := loadWorkerRuntimeConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("loadWorkerRuntimeConfig: %v", err)
	}
	if config.MaxAutomaticAttempts != 2 {
		t.Fatalf("max attempts = %d", config.MaxAutomaticAttempts)
	}
	if config.NAKDelay != 7*time.Second {
		t.Fatalf("NAK delay = %s", config.NAKDelay)
	}
	if !strings.Contains(config.postgresDSN(), "cerbero_worker_dev") {
		t.Fatalf("worker PostgreSQL DSN does not contain worker user: %q", config.postgresDSN())
	}
}

func TestLoadWorkerRuntimeConfigAllowsAutomaticReplayDisabled(t *testing.T) {
	values := validWorkerRuntimeEnv()
	values["CERBERO_WORKER_REPLAY_MAX_AUTOMATIC_ATTEMPTS"] = "0"
	config, err := loadWorkerRuntimeConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("zero automatic replay budget should be a valid kill switch: %v", err)
	}
	if config.MaxAutomaticAttempts != 0 {
		t.Fatalf("max attempts = %d", config.MaxAutomaticAttempts)
	}
}

func TestLoadWorkerRuntimeConfigRejectsNonDevelopmentProfile(t *testing.T) {
	values := validWorkerRuntimeEnv()
	values["CERBERO_SECURITY_PROFILE"] = "PRODUCTION"
	if _, err := loadWorkerRuntimeConfig(func(name string) string { return values[name] }); err == nil {
		t.Fatal("expected non-development security profile to fail")
	}
}

func validWorkerRuntimeEnv() map[string]string {
	return map[string]string{
		"CERBERO_DEV_MODE":                             "1",
		"CERBERO_SECURITY_PROFILE":                     "DEVELOPMENT",
		"CERBERO_NATS_URL":                             "nats://127.0.0.1:4222",
		"NATS_WORKER_USER":                             "cerbero_worker",
		"NATS_WORKER_PASSWORD":                         "secret",
		"POSTGRES_HOST":                                "127.0.0.1",
		"POSTGRES_PORT":                                "55432",
		"POSTGRES_DB":                                  "cerbero",
		"POSTGRES_WORKER_USER":                         "cerbero_worker_dev",
		"POSTGRES_WORKER_PASSWORD":                     "pg-secret",
		"CERBERO_WORKER_COMPONENT_VERSION":             "0.1.0-dev",
		"CERBERO_WORKER_INSTANCE_ID":                   "worker-dev-1",
		"CERBERO_WORKER_CONNECT_TIMEOUT":               "5s",
		"CERBERO_WORKER_REPLAY_MAX_AUTOMATIC_ATTEMPTS": "2",
		"CERBERO_WORKER_REPLAY_NAK_DELAY":              "5s",
	}
}
