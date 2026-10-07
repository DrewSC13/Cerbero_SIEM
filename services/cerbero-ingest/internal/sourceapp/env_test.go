package sourceapp

import (
	"testing"
)

func TestLoadConfigRequiresExplicitDevelopmentProfile(t *testing.T) {
	values := map[string]string{
		"CERBERO_SECURITY_PROFILE":             "DEVELOPMENT",
		"CERBERO_INGEST_INSECURE_DEVELOPMENT":  "1",
		"CERBERO_INGEST_MAX_PAYLOAD_SIZE":      "1048576",
		"CERBERO_INGEST_MAX_EVENTS_PER_SECOND": "1000",
		"CERBERO_INGEST_READ_TIMEOUT":          "5s",
		"CERBERO_INGEST_COMPONENT_VERSION":     "0.1.0-dev",
		"CERBERO_INGEST_PIPELINE_VERSION":      "ingest-v1-dev",
		"CERBERO_NATS_URL":                     "nats://127.0.0.1:4222",
		"NATS_INGEST_USER":                     "ingest",
		"NATS_INGEST_PASSWORD":                 "secret",
		"CERBERO_SYSLOG_INSTANCE_ID":           "syslog-dev-1",
		"CERBERO_SYSLOG_DEV_TENANT_ID":         "tenant-a",
		"CERBERO_SYSLOG_DEV_SOURCE_ID":         "source-a",
		"CERBERO_SYSLOG_DEV_SENSOR_ID":         "sensor-a",
		"CERBERO_SYSLOG_DEV_REMOTE_IDENTITY":   "peer-a",
	}
	getenv := func(name string) string { return values[name] }

	config, err := LoadConfig(getenv, "syslog", "CERBERO_SYSLOG")
	if err != nil {
		t.Fatal(err)
	}
	if config.Transport != "syslog" || config.SourceID != "source-a" {
		t.Fatalf("unexpected config: %+v", config)
	}

	values["CERBERO_SECURITY_PROFILE"] = "PRODUCTION"
	if _, err := LoadConfig(getenv, "syslog", "CERBERO_SYSLOG"); err == nil {
		t.Fatal("LoadConfig accepted unsupported production source frontend")
	}
}
