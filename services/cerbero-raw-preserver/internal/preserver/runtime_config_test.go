package preserver

import (
	"testing"
	"time"
)

func TestLoadRuntimeConfigAcceptsExplicitDevelopmentComposition(t *testing.T) {
	env := validRuntimeEnvironment()
	config, err := LoadRuntimeConfig(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadRuntimeConfig() error = %v", err)
	}
	if config.PostgresPort != 55432 {
		t.Fatalf("PostgresPort = %d", config.PostgresPort)
	}
	if config.RetryMinDelay != time.Second || config.RetryMaxDelay != 5*time.Second {
		t.Fatalf("retry range = %s..%s", config.RetryMinDelay, config.RetryMaxDelay)
	}
	if config.PostgresUser != "cerbero_raw_preserver_dev" {
		t.Fatalf("PostgresUser = %q", config.PostgresUser)
	}
}

func TestLoadRuntimeConfigRejectsUnsafeOrIncompleteComposition(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "development mode", key: "CERBERO_DEV_MODE", value: "0"},
		{name: "security profile", key: "CERBERO_SECURITY_PROFILE", value: "PRODUCTION"},
		{name: "nats URL", key: "CERBERO_NATS_URL", value: "http://127.0.0.1:4222"},
		{name: "service database user", key: "POSTGRES_RAW_PRESERVER_USER", value: ""},
		{name: "raw store path", key: "CERBERO_RAW_STORE_PATH", value: ""},
		{name: "component version", key: "CERBERO_RAW_PRESERVER_COMPONENT_VERSION", value: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := validRuntimeEnvironment()
			env[test.key] = test.value
			if _, err := LoadRuntimeConfig(func(key string) string { return env[key] }); err == nil {
				t.Fatalf("LoadRuntimeConfig() accepted %s=%q", test.key, test.value)
			}
		})
	}
}

func TestLoadRuntimeConfigRejectsInvalidRetryRange(t *testing.T) {
	env := validRuntimeEnvironment()
	env["CERBERO_RAW_PRESERVER_RETRY_MIN_DELAY"] = "6s"
	env["CERBERO_RAW_PRESERVER_RETRY_MAX_DELAY"] = "5s"
	if _, err := LoadRuntimeConfig(func(key string) string { return env[key] }); err == nil {
		t.Fatal("LoadRuntimeConfig() accepted retry maximum below minimum")
	}
}

func validRuntimeEnvironment() map[string]string {
	return map[string]string{
		"CERBERO_DEV_MODE":                        "1",
		"CERBERO_SECURITY_PROFILE":                "DEVELOPMENT",
		"CERBERO_NATS_URL":                        "nats://127.0.0.1:4222",
		"NATS_RAW_PRESERVER_USER":                 "cerbero_raw_preserver",
		"NATS_RAW_PRESERVER_PASSWORD":             "development-nats-password",
		"POSTGRES_HOST":                           "127.0.0.1",
		"POSTGRES_PORT":                           "55432",
		"POSTGRES_DB":                             "cerbero",
		"POSTGRES_RAW_PRESERVER_USER":             "cerbero_raw_preserver_dev",
		"POSTGRES_RAW_PRESERVER_PASSWORD":         "development-postgres-password",
		"CERBERO_RAW_STORE_PATH":                  "./var/raw",
		"CERBERO_RAW_PRESERVER_COMPONENT_VERSION": "0.1.0-test",
		"CERBERO_RAW_PRESERVER_INSTANCE_ID":       "raw-preserver-test-1",
		"CERBERO_RAW_PRESERVER_CONNECT_TIMEOUT":   "5s",
		"CERBERO_RAW_PRESERVER_RETRY_MIN_DELAY":   "1s",
		"CERBERO_RAW_PRESERVER_RETRY_MAX_DELAY":   "5s",
	}
}
