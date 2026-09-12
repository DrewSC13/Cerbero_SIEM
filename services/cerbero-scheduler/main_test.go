package main

import "testing"

func TestBootstrapIdentity(t *testing.T) {
	if componentName != "cerbero-scheduler" {
		t.Fatalf("unexpected component name: %q", componentName)
	}
	if architectureBaseline != "v1.0" {
		t.Fatalf("unexpected architecture baseline: %q", architectureBaseline)
	}
}
