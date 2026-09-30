package validation

import (
	"testing"

	contractsv1 "cerbero/services/internal/contracts/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	findingID       = "018f47a2-4b00-7a00-8000-000000000601"
	findingTenantID = "018f47a2-4b00-7a00-8000-000000000602"
	findingSignalA  = "018f47a2-4b00-7a00-8000-000000000603"
	findingSignalB  = "018f47a2-4b00-7a00-8000-000000000604"
	findingSignalC  = "018f47a2-4b00-7a00-8000-000000000605"
)

func findingTimestamp(seconds int64) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{Seconds: seconds}
}

func fixtureFinding() *contractsv1.Finding {
	confidence := "MEDIUM"
	correlationRuleID := "CER-COR-0003"
	correlationRuleVersion := "2"
	inputIDs := []string{findingSignalA, findingSignalB, findingSignalC}
	inputs := make([]*contractsv1.FindingInput, 0, len(inputIDs))
	for _, inputID := range inputIDs {
		inputs = append(inputs, &contractsv1.FindingInput{
			FindingId: findingID,
			InputType: "SIGNAL",
			InputId:   inputID,
			Relation:  "CORRELATION_INPUT",
		})
	}
	return &contractsv1.Finding{
		FindingId:              findingID,
		TenantId:               findingTenantID,
		FindingType:            contractsv1.FindingType(3),
		Status:                 contractsv1.FindingStatus(1),
		Severity:               "HIGH",
		Confidence:             &confidence,
		Title:                  "successful login after failures followed by privilege escalation",
		Description:            "three-stage bounded sequence correlation matched",
		FirstSeen:              findingTimestamp(100),
		LastSeen:               findingTimestamp(220),
		CreatedAt:              findingTimestamp(230),
		UpdatedAt:              findingTimestamp(230),
		CorrelationRuleId:      &correlationRuleID,
		CorrelationRuleVersion: &correlationRuleVersion,
		Disposition:            contractsv1.FindingDisposition(1),
		ExecutionMode:          contractsv1.ExecutionMode(1),
		Inputs:                 inputs,
		CorrelationProvenance: &contractsv1.FindingCorrelationProvenance{
			CorrelationRuleId:      correlationRuleID,
			CorrelationRuleVersion: correlationRuleVersion,
			WindowStart:            findingTimestamp(100),
			WindowEnd:              findingTimestamp(220),
			InputIds:               append([]string(nil), inputIDs...),
			OutputFindingId:        findingID,
			ExecutionBackend:       contractsv1.DetectionExecutionBackend(1),
			ExecutionMode:          contractsv1.ExecutionMode(1),
			ConfigurationHash:      "sequence-config-sha256",
		},
	}
}

func TestCanonicalCorrelationFindingValidates(t *testing.T) {
	if err := Finding(fixtureFinding()); err != nil {
		t.Fatalf("validate canonical CORRELATION Finding: %v", err)
	}
}

func TestFindingSeenTimePresenceAndOrderFailClosed(t *testing.T) {
	finding := fixtureFinding()
	finding.LastSeen = nil
	if err := Finding(finding); err == nil {
		t.Fatal("expected half-present seen-time rejection")
	} else if got := err.(Violation).Field; got != "first_seen" {
		t.Fatalf("unexpected violation field %q", got)
	}

	finding = fixtureFinding()
	finding.FirstSeen = findingTimestamp(300)
	if err := Finding(finding); err == nil {
		t.Fatal("expected reversed seen-time rejection")
	} else if got := err.(Violation).Field; got != "first_seen" {
		t.Fatalf("unexpected violation field %q", got)
	}
}

func TestFindingInputsFailClosed(t *testing.T) {
	finding := fixtureFinding()
	finding.Inputs[0].FindingId = findingSignalA
	if err := Finding(finding); err == nil {
		t.Fatal("expected containing identity rejection")
	} else if got := err.(Violation).Field; got != "inputs.finding_id" {
		t.Fatalf("unexpected violation field %q", got)
	}

	finding = fixtureFinding()
	finding.Inputs = append(finding.Inputs, proto.Clone(finding.Inputs[0]).(*contractsv1.FindingInput))
	if err := Finding(finding); err == nil {
		t.Fatal("expected exact duplicate relation rejection")
	} else if got := err.(Violation).Field; got != "inputs" {
		t.Fatalf("unexpected violation field %q", got)
	}
}

func TestFindingCorrelationProvenanceFailsClosed(t *testing.T) {
	finding := fixtureFinding()
	finding.CorrelationProvenance.OutputFindingId = findingSignalA
	if err := Finding(finding); err == nil {
		t.Fatal("expected output identity rejection")
	} else if got := err.(Violation).Field; got != "correlation_provenance.output_finding_id" {
		t.Fatalf("unexpected violation field %q", got)
	}

	finding = fixtureFinding()
	finding.CorrelationProvenance.InputIds[0] = "018f47a2-4b00-7a00-8000-000000000699"
	if err := Finding(finding); err == nil {
		t.Fatal("expected unbacked input rejection")
	} else if got := err.(Violation).Field; got != "correlation_provenance.input_ids" {
		t.Fatalf("unexpected violation field %q", got)
	}

	finding = fixtureFinding()
	finding.CorrelationProvenance.ExecutionMode = contractsv1.ExecutionMode(2)
	if err := Finding(finding); err == nil {
		t.Fatal("expected execution-domain mismatch rejection")
	} else if got := err.(Violation).Field; got != "correlation_provenance.execution_mode" {
		t.Fatalf("unexpected violation field %q", got)
	}
}

func TestNonCorrelationFindingCannotCarryCorrelationProvenance(t *testing.T) {
	finding := fixtureFinding()
	finding.FindingType = contractsv1.FindingType(4)
	finding.CorrelationRuleId = nil
	finding.CorrelationRuleVersion = nil
	if err := Finding(finding); err == nil {
		t.Fatal("expected non-correlation provenance rejection")
	} else if got := err.(Violation).Field; got != "correlation_provenance" {
		t.Fatalf("unexpected violation field %q", got)
	}
}
