package main

import "testing"

func TestValidateDevelopmentBindAllowsLoopbackAndExplicitContainerWildcard(t *testing.T) {
	if err := validateDevelopmentBind("127.0.0.1:5514", false); err != nil {
		t.Fatalf("loopback bind rejected: %v", err)
	}
	if err := validateDevelopmentBind("0.0.0.0:5514", true); err != nil {
		t.Fatalf("explicit container wildcard rejected: %v", err)
	}
	if err := validateDevelopmentBind("0.0.0.0:5514", false); err == nil {
		t.Fatal("wildcard bind accepted without explicit container development")
	}
	if err := validateDevelopmentBind("192.0.2.10:5514", true); err == nil {
		t.Fatal("specific non-loopback address accepted in DEVELOPMENT")
	}
}
