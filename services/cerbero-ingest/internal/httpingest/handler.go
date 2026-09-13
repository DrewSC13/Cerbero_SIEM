package httpingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
	contractsv1 "cerbero/services/internal/contracts/v1"
	contractvalidation "cerbero/services/internal/contracts/validation"
)

const (
	requestIDHeader = "X-Request-ID"
	jsonMediaType   = "application/json"
)

// Preparer starts the common ingest admission flow before the HTTP body is received.
type Preparer interface {
	Begin(context.Context, ingestcore.AdmissionMetadata) (ingestcore.Admission, error)
}

// DurableAcceptor confirms that a prepared envelope has been admitted durably.
// Implementations must return nil only after the event-bus durability requirement is satisfied.
type DurableAcceptor interface {
	Accept(context.Context, *ingestcore.Result) error
}

// MetadataResolver derives transport metadata and presented identity material without owning authentication policy.
type MetadataResolver interface {
	Resolve(*http.Request) (ingestcore.AdmissionMetadata, error)
}

// Config contains HTTP-adapter policy and injected boundaries.
type Config struct {
	MaxPayloadSize uint64
	Preparer       Preparer
	Acceptor       DurableAcceptor
	Metadata       MetadataResolver
	RequestIDs     ingestcore.IDGenerator
}

// Handler implements the JSON/HTTP ingest frontend.
type Handler struct {
	maxPayloadSize uint64
	preparer       Preparer
	acceptor       DurableAcceptor
	metadata       MetadataResolver
	requestIDs     ingestcore.IDGenerator
}

// New validates HTTP-adapter configuration.
func New(config Config) (*Handler, error) {
	if config.MaxPayloadSize == 0 {
		return nil, errors.New("HTTP max payload size must be explicitly configured and greater than zero")
	}
	if config.Preparer == nil {
		return nil, errors.New("ingest preparer is required")
	}
	if config.Acceptor == nil {
		return nil, errors.New("durable acceptor is required")
	}
	if config.Metadata == nil {
		return nil, errors.New("metadata resolver is required")
	}
	if config.RequestIDs == nil {
		generator := ingestcore.NewUUIDv7Generator(nil, nil)
		config.RequestIDs = generator
	}
	return &Handler{
		maxPayloadSize: config.MaxPayloadSize,
		preparer:       config.Preparer,
		acceptor:       config.Acceptor,
		metadata:       config.Metadata,
		requestIDs:     config.RequestIDs,
	}, nil
}

// ServeHTTP accepts a JSON event only after the injected durability boundary succeeds.
func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	requestID, err := h.requestID(request)
	if err != nil {
		h.writeInternal(response, requestID, "generate request_id", err)
		return
	}
	response.Header().Set(requestIDHeader, requestID)

	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		h.writeError(response, http.StatusMethodNotAllowed, newWireError(
			"CER-ING-INVALID-PAYLOAD",
			contractsv1.ErrorCategory_VALIDATION,
			"only POST is supported for JSON ingest",
			false,
			requestID,
		))
		return
	}
	if !isJSONMediaType(request.Header.Get("Content-Type")) {
		h.writeError(response, http.StatusUnsupportedMediaType, newWireError(
			"CER-ING-UNSUPPORTED-ENCODING",
			contractsv1.ErrorCategory_VALIDATION,
			"Content-Type must be application/json",
			false,
			requestID,
		))
		return
	}

	metadata, err := h.metadata.Resolve(request)
	if err != nil {
		h.writeError(response, http.StatusUnauthorized, newWireError(
			"CER-AUTH-UNAUTHENTICATED",
			contractsv1.ErrorCategory_AUTHENTICATION,
			"source authentication metadata could not be resolved",
			false,
			requestID,
		))
		return
	}
	metadata.RequestID = requestID
	metadata.Transport = "json-http"

	admission, err := h.preparer.Begin(request.Context(), metadata)
	if err != nil {
		h.writePrepareError(response, requestID, err)
		return
	}

	rawPayload, err := readBounded(request.Body, h.maxPayloadSize)
	if err != nil {
		var tooLarge payloadTooLargeError
		if errors.As(err, &tooLarge) {
			h.writeError(response, http.StatusRequestEntityTooLarge, newWireError(
				"CER-ING-PAYLOAD-TOO-LARGE",
				contractsv1.ErrorCategory_VALIDATION,
				"payload exceeds the configured HTTP ingest limit",
				false,
				requestID,
			))
			return
		}
		h.writeInternal(response, requestID, "read request body", err)
		return
	}
	if !json.Valid(rawPayload) {
		h.writeError(response, http.StatusBadRequest, newWireError(
			"CER-ING-INVALID-PAYLOAD",
			contractsv1.ErrorCategory_VALIDATION,
			"request body must contain valid JSON",
			false,
			requestID,
		))
		return
	}

	result, err := admission.Prepare(ingestcore.Request{
		RequestID:      metadata.RequestID,
		TenantID:       metadata.TenantID,
		SourceID:       metadata.SourceID,
		SensorID:       metadata.SensorID,
		RemoteIdentity: metadata.RemoteIdentity,
		Transport:      metadata.Transport,
		ContentType:    jsonMediaType,
		Encoding:       "utf-8",
		RawPayload:     rawPayload,
	})
	if err != nil {
		h.writePrepareError(response, requestID, err)
		return
	}

	if err := h.acceptor.Accept(request.Context(), result); err != nil {
		h.writeError(response, http.StatusServiceUnavailable, newWireError(
			"CER-BUS-PUBLISH-FAILED",
			contractsv1.ErrorCategory_TRANSPORT,
			"event could not be durably admitted to the event bus",
			true,
			requestID,
		))
		return
	}

	h.writeJSON(response, http.StatusAccepted, map[string]string{
		"request_id": requestID,
		"event_id":   result.RawEvent.GetEventId(),
		"message_id": result.Envelope.GetMessageId(),
	})
}

func (h *Handler) requestID(request *http.Request) (string, error) {
	if supplied := strings.TrimSpace(request.Header.Get(requestIDHeader)); supplied != "" {
		if contractvalidation.UUIDv7("request_id", supplied) == nil {
			return supplied, nil
		}
	}
	generated, err := h.requestIDs.New()
	if err != nil {
		return "", err
	}
	if err := contractvalidation.UUIDv7("request_id", generated); err != nil {
		return "", fmt.Errorf("generated request_id is not UUIDv7: %w", err)
	}
	return generated, nil
}

func (h *Handler) writePrepareError(response http.ResponseWriter, requestID string, err error) {
	var ingestErr *ingestcore.Error
	if !errors.As(err, &ingestErr) || ingestErr.Contract == nil {
		h.writeInternal(response, requestID, "prepare ingest event", err)
		return
	}

	status := http.StatusBadRequest
	switch ingestErr.Contract.GetCategory() {
	case contractsv1.ErrorCategory_AUTHENTICATION:
		status = http.StatusUnauthorized
	case contractsv1.ErrorCategory_AUTHORIZATION:
		status = http.StatusForbidden
	case contractsv1.ErrorCategory_RATE_LIMIT:
		status = http.StatusTooManyRequests
	case contractsv1.ErrorCategory_INTERNAL, contractsv1.ErrorCategory_DEPENDENCY,
		contractsv1.ErrorCategory_STORAGE, contractsv1.ErrorCategory_TRANSPORT:
		status = http.StatusInternalServerError
	}
	if ingestErr.Contract.GetCode() == "CER-ING-PAYLOAD-TOO-LARGE" {
		status = http.StatusRequestEntityTooLarge
	}
	h.writeError(response, status, ingestErr.Contract)
}

func (h *Handler) writeInternal(response http.ResponseWriter, requestID, operation string, _ error) {
	wireErr := newWireError(
		"CER-SYSTEM-INTERNAL",
		contractsv1.ErrorCategory_INTERNAL,
		"internal ingest failure",
		false,
		requestID,
	)
	wireErr.Metadata["operation"] = operation
	h.writeError(response, http.StatusInternalServerError, wireErr)
}

func (h *Handler) writeError(response http.ResponseWriter, status int, wireErr *contractsv1.CerberoError) {
	h.writeJSON(response, status, map[string]any{"error": wireErr})
}

func (h *Handler) writeJSON(response http.ResponseWriter, status int, payload any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	if err := json.NewEncoder(response).Encode(payload); err != nil {
		return
	}
}

func isJSONMediaType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, jsonMediaType)
}

type payloadTooLargeError struct {
	limit uint64
}

func (e payloadTooLargeError) Error() string {
	return fmt.Sprintf("payload exceeds %d bytes", e.limit)
}

func readBounded(body io.ReadCloser, limit uint64) ([]byte, error) {
	defer body.Close()
	reader := io.LimitReader(body, int64(limit)+1)
	payload, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if uint64(len(payload)) > limit {
		return nil, payloadTooLargeError{limit: limit}
	}
	return payload, nil
}

func newWireError(
	code string,
	category contractsv1.ErrorCategory,
	message string,
	retryable bool,
	requestID string,
) *contractsv1.CerberoError {
	return &contractsv1.CerberoError{
		Code:      code,
		Category:  category,
		Message:   message,
		Retryable: retryable,
		Component: "cerbero-ingest",
		RequestId: requestID,
		Metadata:  map[string]string{},
	}
}
