package ingestapp

import (
	"strings"
	"testing"
)

func validEnvironment() map[string]string {
	return map[string]string{
		"CERBERO_SECURITY_PROFILE":              "DEVELOPMENT",
		"CERBERO_INGEST_INSECURE_DEVELOPMENT":   "1",
		"CERBERO_INGEST_LISTEN_ADDRESS":         "127.0.0.1:19080",
		"CERBERO_INGEST_HTTP_PATH":              "/ingest/v1/events",
		"CERBERO_INGEST_MAX_PAYLOAD_SIZE":       "1048576",
		"CERBERO_INGEST_MAX_CONNECTION_RATE":    "100",
		"CERBERO_INGEST_MAX_EVENTS_PER_SECOND":  "1000",
		"CERBERO_INGEST_READ_TIMEOUT":           "5s",
		"CERBERO_INGEST_IDLE_TIMEOUT":           "30s",
		"CERBERO_INGEST_CONCURRENT_CONNECTIONS": "100",
		"CERBERO_INGEST_SHUTDOWN_TIMEOUT":       "5s",
		"CERBERO_INGEST_COMPONENT_VERSION":      "0.1.0-dev",
		"CERBERO_INGEST_PIPELINE_VERSION":       "ingest-v1-dev",
		"CERBERO_INGEST_INSTANCE_ID":            "ingest-dev-1",
		"CERBERO_NATS_URL":                      "nats://127.0.0.1:4222",
		"NATS_INGEST_USER":                      "cerbero_ingest",
		"NATS_INGEST_PASSWORD":                  "dev-secret",
		"CERBERO_INGEST_DEV_TENANT_ID":          "tenant-dev",
		"CERBERO_INGEST_DEV_SOURCE_ID":          "source-dev",
		"CERBERO_INGEST_DEV_SENSOR_ID":          "sensor-dev",
		"CERBERO_INGEST_DEV_REMOTE_IDENTITY":    "development-static-source",
	}
}

func envLookup(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadConfigAcceptsExplicitLoopbackDevelopmentProfile(t *testing.T) {
	config, err := LoadConfig(envLookup(validEnvironment()))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.SecurityProfile != ProfileDevelopment || !config.InsecureDevelopment {
		t.Fatalf("unexpected development profile: %+v", config)
	}
}

func TestLoadConfigFailsClosedForProductionWithoutPKIAuthenticator(t *testing.T) {
	values := validEnvironment()
	values["CERBERO_SECURITY_PROFILE"] = "PRODUCTION"

	_, err := LoadConfig(envLookup(values))
	if err == nil || !strings.Contains(err.Error(), "production JSON/HTTP source authentication is not implemented") {
		t.Fatalf("LoadConfig() error = %v, want production fail-closed error", err)
	}
}

func TestLoadConfigRequiresExplicitInsecureDevelopmentAndLoopback(t *testing.T) {
	values := validEnvironment()
	delete(values, "CERBERO_INGEST_INSECURE_DEVELOPMENT")
	if _, err := LoadConfig(envLookup(values)); err == nil {
		t.Fatal("LoadConfig() accepted implicit insecure development")
	}

	values = validEnvironment()
	values["CERBERO_INGEST_LISTEN_ADDRESS"] = "0.0.0.0:19080"
	if _, err := LoadConfig(envLookup(values)); err == nil {
		t.Fatal("LoadConfig() accepted non-loopback insecure development bind")
	}
}

func TestLoadConfigRequiresEveryFrontendLimit(t *testing.T) {
	for _, key := range []string{
		"CERBERO_INGEST_MAX_PAYLOAD_SIZE",
		"CERBERO_INGEST_MAX_CONNECTION_RATE",
		"CERBERO_INGEST_MAX_EVENTS_PER_SECOND",
		"CERBERO_INGEST_READ_TIMEOUT",
		"CERBERO_INGEST_IDLE_TIMEOUT",
		"CERBERO_INGEST_CONCURRENT_CONNECTIONS",
	} {
		t.Run(key, func(t *testing.T) {
			values := validEnvironment()
			delete(values, key)
			if _, err := LoadConfig(envLookup(values)); err == nil {
				t.Fatalf("LoadConfig() accepted missing %s", key)
			}
		})
	}
}
