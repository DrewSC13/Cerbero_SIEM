package main

import "testing"

func TestComponentIdentity(t *testing.T) {
	if componentName != "cerbero-raw-preserver" {
		t.Fatalf("componentName = %q", componentName)
	}
	if architectureBaseline != "v1.0" {
		t.Fatalf("architectureBaseline = %q", architectureBaseline)
	}
}
