package validation

import (
	"strings"

	contractsv1 "cerbero/services/internal/contracts/v1"
)

const (
	signalRuleTypeEvent            = int32(1)
	signalRuleTypeThreshold        = int32(2)
	signalRuleTypeCorrelation      = int32(3)
	signalInputTypeNormalizedEvent = int32(1)
)

func requireSignalText(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return violation(field, "is required")
	}
	return nil
}

func validateDetectionRuleID(value string) error {
	const prefix = "CER-DET-"
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+6 {
		return violation("rule_id", "must match CER-DET-XXXXXX with exactly six ASCII digits")
	}
	for _, value := range value[len(prefix):] {
		if value < '0' || value > '9' {
			return violation("rule_id", "must match CER-DET-XXXXXX with exactly six ASCII digits")
		}
	}
	return nil
}

func validateSignalInput(signalID string, input *contractsv1.SignalInput, expectedOrdinal uint32) error {
	if input == nil {
		return violation("inputs", "must not contain nil entries")
	}
	if input.GetSignalId() != signalID {
		return violation("inputs.signal_id", "must equal the containing Signal.signal_id")
	}
	inputType := int32(input.GetInputType())
	if inputType < 1 || inputType > 3 {
		return violation("inputs.input_type", "must be NORMALIZED_EVENT, SIGNAL, or ENTITY")
	}
	if err := UUIDv7("inputs.input_id", input.GetInputId()); err != nil {
		return err
	}
	if err := requireSignalText("inputs.relation", input.GetRelation()); err != nil {
		return err
	}
	if input.GetOrdinal() != expectedOrdinal {
		return violation("inputs.ordinal", "must equal its zero-based input position")
	}
	return nil
}

// Signal validates the canonical Signal v1 wire invariants from ADR-0016.
func Signal(signal *contractsv1.Signal) error {
	if signal == nil {
		return violation("signal", "is required")
	}
	if err := UUIDv7("signal_id", signal.GetSignalId()); err != nil {
		return err
	}
	if err := UUIDv7("tenant_id", signal.GetTenantId()); err != nil {
		return err
	}
	if err := validateDetectionRuleID(signal.GetRuleId()); err != nil {
		return err
	}
	if err := requireSignalText("rule_version", signal.GetRuleVersion()); err != nil {
		return err
	}
	ruleType := int32(signal.GetRuleType())
	if ruleType < signalRuleTypeEvent || ruleType > signalRuleTypeCorrelation {
		return violation("rule_type", "must be EVENT, THRESHOLD, or CORRELATION")
	}
	if err := requireSignalText("severity", signal.GetSeverity()); err != nil {
		return err
	}
	if signal.Confidence != nil {
		if err := requireSignalText("confidence", signal.GetConfidence()); err != nil {
			return err
		}
	}
	signalStatus := int32(signal.GetStatus())
	if signalStatus < 1 || signalStatus > 4 {
		return violation("status", "must be ACTIVE, SUPPRESSED, PROMOTED, or INVALIDATED")
	}

	first := signal.GetFirstObservedAt()
	last := signal.GetLastObservedAt()
	switch {
	case first == nil && last == nil:
	case first != nil && last != nil:
		if err := Timestamp("first_observed_at", first); err != nil {
			return err
		}
		if err := Timestamp("last_observed_at", last); err != nil {
			return err
		}
		if first.Seconds > last.Seconds || (first.Seconds == last.Seconds && first.Nanos > last.Nanos) {
			return violation("first_observed_at", "must be less than or equal to last_observed_at")
		}
	default:
		return violation("first_observed_at", "first_observed_at and last_observed_at must both be present or both be absent")
	}

	if err := Timestamp("created_at", signal.GetCreatedAt()); err != nil {
		return err
	}
	executionMode := int32(signal.GetExecutionMode())
	if executionMode < 1 || executionMode > 3 {
		return violation("execution_mode", "must distinguish LIVE, REPLAY, or TEST")
	}
	if signal.GetSourceCount() == 0 {
		return violation("source_count", "must be greater than zero")
	}
	if signal.GetEventCount() == 0 {
		return violation("event_count", "must be greater than zero")
	}
	if err := requireSignalText("summary", signal.GetSummary()); err != nil {
		return err
	}

	provenance := signal.GetProvenance()
	if provenance == nil {
		return violation("provenance", "is required")
	}
	if err := Timestamp("provenance.evaluated_at", provenance.GetEvaluatedAt()); err != nil {
		return err
	}
	backend := int32(provenance.GetExecutionBackend())
	if backend < 1 || backend > 2 {
		return violation("provenance.execution_backend", "must be CLICKHOUSE or STREAM")
	}

	inputs := signal.GetInputs()
	if len(inputs) == 0 {
		return violation("inputs", "must preserve at least one contributing object")
	}
	seen := make(map[struct {
		inputType int32
		inputID   string
	}]struct{}, len(inputs))
	var normalizedEventInputs uint64
	for index, input := range inputs {
		if uint64(index) > uint64(^uint32(0)) {
			return violation("inputs.ordinal", "input list exceeds uint32 ordinal capacity")
		}
		if err := validateSignalInput(signal.GetSignalId(), input, uint32(index)); err != nil {
			return err
		}
		key := struct {
			inputType int32
			inputID   string
		}{int32(input.GetInputType()), input.GetInputId()}
		if _, exists := seen[key]; exists {
			return violation("inputs", "must not contain duplicate input_type/input_id pairs")
		}
		seen[key] = struct{}{}
		if int32(input.GetInputType()) == signalInputTypeNormalizedEvent {
			normalizedEventInputs++
		}
	}

	switch ruleType {
	case signalRuleTypeEvent:
		if normalizedEventInputs != 1 || signal.GetEventCount() != 1 {
			return violation("event_count", "EVENT Signals require exactly one NORMALIZED_EVENT input and event_count = 1")
		}
		if signal.GetSourceCount() > signal.GetEventCount() {
			return violation("source_count", "cannot exceed event_count for EVENT Signals")
		}
	case signalRuleTypeThreshold:
		if normalizedEventInputs == 0 || normalizedEventInputs != signal.GetEventCount() {
			return violation("event_count", "THRESHOLD Signals require one input per contributing NormalizedEvent")
		}
		if signal.GetSourceCount() > signal.GetEventCount() {
			return violation("source_count", "cannot exceed event_count for THRESHOLD Signals")
		}
	case signalRuleTypeCorrelation:
	}

	return nil
}
