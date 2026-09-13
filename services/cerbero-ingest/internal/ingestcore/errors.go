package ingestcore

import (
	"fmt"

	contractsv1 "cerbero/services/internal/contracts/v1"
)

const (
	codeInvalidPayload     = "CER-ING-INVALID-PAYLOAD"
	codePayloadTooLarge    = "CER-ING-PAYLOAD-TOO-LARGE"
	codeRateLimited        = "CER-ING-RATE-LIMITED"
	codeUnauthenticated    = "CER-AUTH-UNAUTHENTICATED"
	codeForbidden          = "CER-AUTH-FORBIDDEN"
	codeSystemInternal     = "CER-SYSTEM-INTERNAL"
	componentCerberoIngest = "cerbero-ingest"
)

// Error wraps the stable CerberoError contract with an implementation cause.
type Error struct {
	Contract *contractsv1.CerberoError
	Cause    error
}

func (e *Error) Error() string {
	if e == nil || e.Contract == nil {
		return "cerbero ingest error"
	}
	return fmt.Sprintf("%s: %s", e.Contract.GetCode(), e.Contract.GetMessage())
}

// Unwrap exposes the implementation cause without placing it in the wire error message.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func newError(
	code string,
	category contractsv1.ErrorCategory,
	message string,
	retryable bool,
	requestID string,
	cause error,
	metadata map[string]string,
) *Error {
	if metadata == nil {
		metadata = map[string]string{}
	}
	return &Error{
		Contract: &contractsv1.CerberoError{
			Code:      code,
			Category:  category,
			Message:   message,
			Retryable: retryable,
			Component: componentCerberoIngest,
			RequestId: requestID,
			Metadata:  metadata,
		},
		Cause: cause,
	}
}
