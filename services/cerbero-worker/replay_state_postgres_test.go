package main

import "testing"

func TestReplayReservationRequestFromEligibleDecision(t *testing.T) {
	record := validRetryableDeadLetter()
	decision := evaluateNormalizationReplay(
		record,
		replayPolicy{MaxAutomaticAttempts: 2},
	)

	request, err := replayReservationRequestFromDecision(decision)
	if err != nil {
		t.Fatalf("replayReservationRequestFromDecision: %v", err)
	}
	if request.LifecycleKey != decision.LifecycleKey ||
		request.ReplayRootDLQRecordID != decision.ReplayRootDLQRecordID ||
		request.SourceDLQRecordID != decision.SourceDLQRecordID ||
		request.SourceStreamSequence != decision.SourceStreamSequence ||
		request.MaxAttempts != decision.MaxAttempts {
		t.Fatalf("unexpected reservation request: %+v", request)
	}
}

func TestReplayReservationRequestRejectsIneligibleDecision(t *testing.T) {
	_, err := replayReservationRequestFromDecision(replayDecision{})
	if err == nil {
		t.Fatal("expected ineligible replay decision to be rejected")
	}
}

func TestValidateReplayReservationRequest(t *testing.T) {
	valid := replayReservationRequest{
		LifecycleKey:          replayLifecycleKey("root-1"),
		ReplayRootDLQRecordID: "root-1",
		SourceDLQRecordID:     "dlq-1",
		SourceStreamSequence:  42,
		MaxAttempts:           2,
	}

	tests := []struct {
		name   string
		mutate func(*replayReservationRequest)
	}{
		{
			name: "blank root",
			mutate: func(request *replayReservationRequest) {
				request.ReplayRootDLQRecordID = " "
			},
		},
		{
			name: "mismatched lifecycle",
			mutate: func(request *replayReservationRequest) {
				request.LifecycleKey = replayLifecycleKey("other-root")
			},
		},
		{
			name: "blank source record",
			mutate: func(request *replayReservationRequest) {
				request.SourceDLQRecordID = " "
			},
		},
		{
			name: "zero stream sequence",
			mutate: func(request *replayReservationRequest) {
				request.SourceStreamSequence = 0
			},
		},
		{
			name: "zero budget",
			mutate: func(request *replayReservationRequest) {
				request.MaxAttempts = 0
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			if err := validateReplayReservationRequest(request); err == nil {
				t.Fatalf("expected invalid request to fail: %+v", request)
			}
		})
	}

	if err := validateReplayReservationRequest(valid); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
}

func TestNewPostgresReplayStateStoreRequiresDatabase(t *testing.T) {
	store, err := newPostgresReplayStateStore(nil)
	if err == nil || store != nil {
		t.Fatalf("expected nil database rejection, store=%v err=%v", store, err)
	}
}
