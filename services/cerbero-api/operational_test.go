package main

import "testing"

func TestIncidentStateMachine(t *testing.T) {
	allowed := [][2]string{
		{"OPEN", "TRIAGED"},
		{"OPEN", "INVALIDATED"},
		{"TRIAGED", "INVESTIGATING"},
		{"TRIAGED", "RESOLVED"},
		{"TRIAGED", "INVALIDATED"},
		{"INVESTIGATING", "CONTAINED"},
		{"INVESTIGATING", "RESOLVED"},
		{"INVESTIGATING", "INVALIDATED"},
		{"CONTAINED", "RESOLVED"},
		{"RESOLVED", "CLOSED"},
		{"RESOLVED", "INVESTIGATING"},
		{"CLOSED", "INVESTIGATING"},
	}
	for _, transition := range allowed {
		if !incidentTransitionAllowed(transition[0], transition[1]) {
			t.Fatalf("expected incident transition %s -> %s to be allowed", transition[0], transition[1])
		}
	}
	for _, transition := range [][2]string{{"OPEN", "CLOSED"}, {"CONTAINED", "TRIAGED"}, {"INVALIDATED", "OPEN"}} {
		if incidentTransitionAllowed(transition[0], transition[1]) {
			t.Fatalf("expected incident transition %s -> %s to be rejected", transition[0], transition[1])
		}
	}
}

func TestCaseStateMachineAndEnums(t *testing.T) {
	allowed := [][2]string{
		{"OPEN", "TRIAGE"},
		{"TRIAGE", "INVESTIGATING"},
		{"INVESTIGATING", "RESPONSE"},
		{"RESPONSE", "RESOLVED"},
		{"RESOLVED", "CLOSED"},
		{"CLOSED", "INVESTIGATING"},
		{"TRIAGE", "ON_HOLD"},
		{"ON_HOLD", "INVESTIGATING"},
	}
	for _, transition := range allowed {
		if !caseTransitionAllowed(transition[0], transition[1]) {
			t.Fatalf("expected case transition %s -> %s to be allowed", transition[0], transition[1])
		}
	}
	for _, transition := range [][2]string{{"OPEN", "CLOSED"}, {"CLOSED", "TRIAGE"}, {"RESPONSE", "OPEN"}} {
		if caseTransitionAllowed(transition[0], transition[1]) {
			t.Fatalf("expected case transition %s -> %s to be rejected", transition[0], transition[1])
		}
	}
	for _, priority := range []string{"P1_CRITICAL", "P2_HIGH", "P3_MEDIUM", "P4_LOW"} {
		if !validCasePriority(priority) {
			t.Fatalf("expected valid priority %q", priority)
		}
	}
	if validCasePriority("HIGH") {
		t.Fatal("severity must not be accepted as Case priority")
	}
	for _, disposition := range []string{"UNDETERMINED", "CONFIRMED_INCIDENT", "BENIGN_ACTIVITY", "FALSE_POSITIVE", "DUPLICATE", "TEST", "OTHER"} {
		if !validCaseDisposition(disposition) {
			t.Fatalf("expected valid disposition %q", disposition)
		}
	}
}
