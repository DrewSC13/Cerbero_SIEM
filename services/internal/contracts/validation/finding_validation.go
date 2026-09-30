package validation

import (
	"strings"

	contractsv1 "cerbero/services/internal/contracts/v1"
)

const (
	findingTypeDetection   = int32(1)
	findingTypeThreshold   = int32(2)
	findingTypeCorrelation = int32(3)
	findingTypeAnalytical  = int32(4)
)

func requireFindingText(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return violation(field, "is required")
	}
	return nil
}

func timestampLE(left, right interface {
	GetSeconds() int64
	GetNanos() int32
}) bool {
	if left.GetSeconds() != right.GetSeconds() {
		return left.GetSeconds() < right.GetSeconds()
	}
	return left.GetNanos() <= right.GetNanos()
}

func validateFindingPair(firstField string, first *string, secondField string, second *string) error {
	switch {
	case first == nil && second == nil:
		return nil
	case first != nil && second != nil:
		if err := requireFindingText(firstField, *first); err != nil {
			return err
		}
		return requireFindingText(secondField, *second)
	default:
		return violation(firstField, firstField+" and "+secondField+" must both be present or both be absent")
	}
}

func validateFindingInput(findingID string, input *contractsv1.FindingInput) error {
	if input == nil {
		return violation("inputs", "must not contain nil entries")
	}
	if input.GetFindingId() != findingID {
		return violation("inputs.finding_id", "must equal the containing Finding.finding_id")
	}
	if err := requireFindingText("inputs.input_type", input.GetInputType()); err != nil {
		return err
	}
	if err := UUIDv7("inputs.input_id", input.GetInputId()); err != nil {
		return err
	}
	return requireFindingText("inputs.relation", input.GetRelation())
}

func validateFindingCorrelationProvenance(
	finding *contractsv1.Finding,
	provenance *contractsv1.FindingCorrelationProvenance,
	findingInputIDs map[string]struct{},
) error {
	if finding.CorrelationRuleId == nil {
		return violation("correlation_rule_id", "is required for CORRELATION Findings")
	}
	if finding.CorrelationRuleVersion == nil {
		return violation("correlation_rule_version", "is required for CORRELATION Findings")
	}
	if provenance.GetCorrelationRuleId() != finding.GetCorrelationRuleId() {
		return violation("correlation_provenance.correlation_rule_id", "must equal Finding.correlation_rule_id")
	}
	if provenance.GetCorrelationRuleVersion() != finding.GetCorrelationRuleVersion() {
		return violation("correlation_provenance.correlation_rule_version", "must equal Finding.correlation_rule_version")
	}
	if err := Timestamp("correlation_provenance.window_start", provenance.GetWindowStart()); err != nil {
		return err
	}
	if err := Timestamp("correlation_provenance.window_end", provenance.GetWindowEnd()); err != nil {
		return err
	}
	if !timestampLE(provenance.GetWindowStart(), provenance.GetWindowEnd()) {
		return violation("correlation_provenance.window_start", "must be less than or equal to window_end")
	}

	inputIDs := provenance.GetInputIds()
	if len(inputIDs) == 0 {
		return violation("correlation_provenance.input_ids", "must preserve at least one contributing object identity")
	}
	seen := make(map[string]struct{}, len(inputIDs))
	for _, inputID := range inputIDs {
		if err := UUIDv7("correlation_provenance.input_ids", inputID); err != nil {
			return err
		}
		if _, exists := seen[inputID]; exists {
			return violation("correlation_provenance.input_ids", "must not contain duplicate object identities")
		}
		seen[inputID] = struct{}{}
		if _, exists := findingInputIDs[inputID]; !exists {
			return violation("correlation_provenance.input_ids", "every correlation input_id must be backed by Finding.inputs")
		}
	}

	if provenance.GetOutputFindingId() != finding.GetFindingId() {
		return violation("correlation_provenance.output_finding_id", "must equal Finding.finding_id")
	}
	backend := int32(provenance.GetExecutionBackend())
	if backend < 1 || backend > 2 {
		return violation("correlation_provenance.execution_backend", "must be CLICKHOUSE or STREAM")
	}
	if provenance.GetExecutionMode() != finding.GetExecutionMode() {
		return violation("correlation_provenance.execution_mode", "must equal Finding.execution_mode")
	}
	return requireFindingText("correlation_provenance.configuration_hash", provenance.GetConfigurationHash())
}

// Finding validates the canonical Finding v1 wire invariants from ADR-0017.
func Finding(finding *contractsv1.Finding) error {
	if finding == nil {
		return violation("finding", "is required")
	}
	if err := UUIDv7("finding_id", finding.GetFindingId()); err != nil {
		return err
	}
	if err := UUIDv7("tenant_id", finding.GetTenantId()); err != nil {
		return err
	}
	findingType := int32(finding.GetFindingType())
	if findingType < findingTypeDetection || findingType > findingTypeAnalytical {
		return violation("finding_type", "must be DETECTION, THRESHOLD, CORRELATION, or ANALYTICAL")
	}
	status := int32(finding.GetStatus())
	if status < 1 || status > 5 {
		return violation("status", "must be OPEN, ACKNOWLEDGED, SUPPRESSED, RESOLVED, or INVALIDATED")
	}
	if err := requireFindingText("severity", finding.GetSeverity()); err != nil {
		return err
	}
	if finding.Confidence != nil {
		if err := requireFindingText("confidence", finding.GetConfidence()); err != nil {
			return err
		}
	}
	if err := requireFindingText("title", finding.GetTitle()); err != nil {
		return err
	}
	if err := requireFindingText("description", finding.GetDescription()); err != nil {
		return err
	}

	first := finding.GetFirstSeen()
	last := finding.GetLastSeen()
	switch {
	case first == nil && last == nil:
	case first != nil && last != nil:
		if err := Timestamp("first_seen", first); err != nil {
			return err
		}
		if err := Timestamp("last_seen", last); err != nil {
			return err
		}
		if !timestampLE(first, last) {
			return violation("first_seen", "must be less than or equal to last_seen")
		}
	default:
		return violation("first_seen", "first_seen and last_seen must both be present or both be absent")
	}

	if err := Timestamp("created_at", finding.GetCreatedAt()); err != nil {
		return err
	}
	if err := Timestamp("updated_at", finding.GetUpdatedAt()); err != nil {
		return err
	}
	if err := validateFindingPair("primary_rule_id", finding.PrimaryRuleId, "primary_rule_version", finding.PrimaryRuleVersion); err != nil {
		return err
	}
	if err := validateFindingPair("correlation_rule_id", finding.CorrelationRuleId, "correlation_rule_version", finding.CorrelationRuleVersion); err != nil {
		return err
	}
	disposition := int32(finding.GetDisposition())
	if disposition < 1 || disposition > 6 {
		return violation("disposition", "must be UNDETERMINED, TRUE_POSITIVE, BENIGN_TRUE_POSITIVE, FALSE_POSITIVE, DUPLICATE, or TEST_ACTIVITY")
	}
	executionMode := int32(finding.GetExecutionMode())
	if executionMode < 1 || executionMode > 3 {
		return violation("execution_mode", "must distinguish LIVE, REPLAY, or TEST")
	}

	inputs := finding.GetInputs()
	if len(inputs) == 0 {
		return violation("inputs", "must preserve at least one contributing object")
	}
	exactRelations := make(map[struct {
		inputType string
		inputID   string
		relation  string
	}]struct{}, len(inputs))
	findingInputIDs := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if err := validateFindingInput(finding.GetFindingId(), input); err != nil {
			return err
		}
		key := struct {
			inputType string
			inputID   string
			relation  string
		}{input.GetInputType(), input.GetInputId(), input.GetRelation()}
		if _, exists := exactRelations[key]; exists {
			return violation("inputs", "must not contain duplicate input_type/input_id/relation entries")
		}
		exactRelations[key] = struct{}{}
		findingInputIDs[input.GetInputId()] = struct{}{}
	}

	switch findingType {
	case findingTypeDetection, findingTypeThreshold:
		if finding.PrimaryRuleId == nil || finding.PrimaryRuleVersion == nil {
			return violation("primary_rule_id", "DETECTION and THRESHOLD Findings require primary rule identity/version")
		}
		if finding.GetCorrelationProvenance() != nil {
			return violation("correlation_provenance", "is only valid for CORRELATION Findings")
		}
	case findingTypeCorrelation:
		provenance := finding.GetCorrelationProvenance()
		if provenance == nil {
			return violation("correlation_provenance", "is required for CORRELATION Findings")
		}
		if err := validateFindingCorrelationProvenance(finding, provenance, findingInputIDs); err != nil {
			return err
		}
	case findingTypeAnalytical:
		if finding.GetCorrelationProvenance() != nil {
			return violation("correlation_provenance", "is only valid for CORRELATION Findings")
		}
	}

	return nil
}
