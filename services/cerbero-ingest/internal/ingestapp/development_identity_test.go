package ingestapp

import (
	"context"
	"testing"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

func TestDevelopmentIdentityIsStrictlyScoped(t *testing.T) {
	config, err := LoadConfig(envLookup(validEnvironment()))
	if err != nil {
		t.Fatal(err)
	}
	identity := newDevelopmentIdentity(config)
	metadata := identity.metadata
	metadata.RequestID = "018f47d3-2c6a-7b10-8f00-000000000001"
	metadata.Transport = "json-http"

	principal, err := identity.Authenticate(context.Background(), metadata)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if err := identity.Authorize(context.Background(), principal, ingestcore.PermissionEventsIngest, metadata); err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}

	metadata.SourceID = "other-source"
	if _, err := identity.Authenticate(context.Background(), metadata); err == nil {
		t.Fatal("development identity accepted a different source")
	}
}
