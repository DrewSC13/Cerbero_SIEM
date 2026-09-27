package validation

import (
	"testing"

	contractsv1 "cerbero/services/internal/contracts/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	signalID = "018f47a2-4b00-7a00-8000-000000000101"
	tenantID = "018f47a2-4b00-7a00-8000-000000000102"
	inputID  = "018f47a2-4b00-7a00-8000-000000000103"
)

func signalTimestamp(seconds int64) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{Seconds: seconds}
}

func fixtureSignal() *contractsv1.Signal {
	confidence := "LOW"
	return &contractsv1.Signal{
		SignalId:        signalID,
		TenantId:        tenantID,
		RuleId:          "CER-DET-000001",
		RuleVersion:     "7",
		RuleType:        contractsv1.SignalRuleType(1),
		Severity:        "HIGH",
		Confidence:      &confidence,
		Status:          contractsv1.SignalStatus(1),
		FirstObservedAt: signalTimestamp(10),
		LastObservedAt:  signalTimestamp(10),
		CreatedAt:       signalTimestamp(12),
		ExecutionMode:   contractsv1.ExecutionMode(2),
		SourceCount:     1,
		EventCount:      1,
		Summary:         "failed SSH login matched",
		Provenance: &contractsv1.SignalProvenance{
			EvaluatedAt:      signalTimestamp(11),
			ExecutionBackend: contractsv1.DetectionExecutionBackend(1),
		},
		Inputs: []*contractsv1.SignalInput{
			{
				SignalId:  signalID,
				InputType: contractsv1.SignalInputType(1),
				InputId:   inputID,
				Relation:  "MATCHED",
				Ordinal:   0,
			},
		},
	}
}

func TestCanonicalEventSignalValidates(t *testing.T) {
	if err := Signal(fixtureSignal()); err != nil {
		t.Fatalf("validate canonical EVENT Signal: %v", err)
	}
}

func TestObservedTimePresenceAndOrderFailClosed(t *testing.T) {
	signal := fixtureSignal()
	signal.LastObservedAt = nil
	if err := Signal(signal); err == nil {
		t.Fatal("expected half-present observed time rejection")
	} else if got := err.(Violation).Field; got != "first_observed_at" {
		t.Fatalf("unexpected violation field %q", got)
	}

	signal = fixtureSignal()
	signal.FirstObservedAt = signalTimestamp(20)
	if err := Signal(signal); err == nil {
		t.Fatal("expected reversed observed time rejection")
	} else if got := err.(Violation).Field; got != "first_observed_at" {
		t.Fatalf("unexpected violation field %q", got)
	}

	signal = fixtureSignal()
	signal.FirstObservedAt = nil
	signal.LastObservedAt = nil
	if err := Signal(signal); err != nil {
		t.Fatalf("missing source event_time must remain representable: %v", err)
	}
}

func TestEventSignalInputSemanticsFailClosed(t *testing.T) {
	signal := fixtureSignal()
	signal.EventCount = 2
	if err := Signal(signal); err == nil {
		t.Fatal("expected EVENT event_count rejection")
	} else if got := err.(Violation).Field; got != "event_count" {
		t.Fatalf("unexpected violation field %q", got)
	}

	signal = fixtureSignal()
	signal.Inputs[0].InputType = contractsv1.SignalInputType(3)
	if err := Signal(signal); err == nil {
		t.Fatal("expected missing NormalizedEvent input rejection")
	} else if got := err.(Violation).Field; got != "event_count" {
		t.Fatalf("unexpected violation field %q", got)
	}
}

func TestDuplicateAndMisorderedInputsFailClosed(t *testing.T) {
	signal := fixtureSignal()
	duplicate := proto.Clone(signal.Inputs[0]).(*contractsv1.SignalInput)
	duplicate.Ordinal = 1
	signal.Inputs = append(signal.Inputs, duplicate)
	if err := Signal(signal); err == nil {
		t.Fatal("expected duplicate Signal input rejection")
	} else if got := err.(Violation).Field; got != "inputs" {
		t.Fatalf("unexpected violation field %q", got)
	}

	signal = fixtureSignal()
	signal.Inputs[0].Ordinal = 1
	if err := Signal(signal); err == nil {
		t.Fatal("expected input ordinal rejection")
	} else if got := err.(Violation).Field; got != "inputs.ordinal" {
		t.Fatalf("unexpected violation field %q", got)
	}
}

func TestUnspecifiedExecutionProvenanceFailsClosed(t *testing.T) {
	signal := fixtureSignal()
	signal.ExecutionMode = contractsv1.ExecutionMode(0)
	if err := Signal(signal); err == nil {
		t.Fatal("expected unspecified execution mode rejection")
	} else if got := err.(Violation).Field; got != "execution_mode" {
		t.Fatalf("unexpected violation field %q", got)
	}

	signal = fixtureSignal()
	signal.Provenance.ExecutionBackend = contractsv1.DetectionExecutionBackend(0)
	if err := Signal(signal); err == nil {
		t.Fatal("expected unspecified execution backend rejection")
	} else if got := err.(Violation).Field; got != "provenance.execution_backend" {
		t.Fatalf("unexpected violation field %q", got)
	}
}
