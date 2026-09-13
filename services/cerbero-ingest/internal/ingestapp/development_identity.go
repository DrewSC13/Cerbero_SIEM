package ingestapp

import (
	"context"
	"errors"
	"net/http"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

type developmentIdentity struct {
	metadata ingestcore.AdmissionMetadata
}

func newDevelopmentIdentity(config Config) *developmentIdentity {
	return &developmentIdentity{metadata: ingestcore.AdmissionMetadata{
		TenantID:       config.DevelopmentTenantID,
		SourceID:       config.DevelopmentSourceID,
		SensorID:       config.DevelopmentSensorID,
		RemoteIdentity: config.DevelopmentRemoteIdentity,
		Transport:      "json-http",
	}}
}

func (i *developmentIdentity) Resolve(*http.Request) (ingestcore.AdmissionMetadata, error) {
	return i.metadata, nil
}

func (i *developmentIdentity) Authenticate(
	_ context.Context,
	metadata ingestcore.AdmissionMetadata,
) (ingestcore.Principal, error) {
	if !samePresentedIdentity(metadata, i.metadata) {
		return ingestcore.Principal{}, errors.New("development identity mismatch")
	}
	return ingestcore.Principal{ID: i.metadata.RemoteIdentity}, nil
}

func (i *developmentIdentity) Authorize(
	_ context.Context,
	principal ingestcore.Principal,
	permission string,
	metadata ingestcore.AdmissionMetadata,
) error {
	if permission != ingestcore.PermissionEventsIngest {
		return errors.New("unsupported development permission")
	}
	if principal.ID != i.metadata.RemoteIdentity || !samePresentedIdentity(metadata, i.metadata) {
		return errors.New("development authorization scope mismatch")
	}
	return nil
}

func samePresentedIdentity(left, right ingestcore.AdmissionMetadata) bool {
	return left.TenantID == right.TenantID &&
		left.SourceID == right.SourceID &&
		left.SensorID == right.SensorID &&
		left.RemoteIdentity == right.RemoteIdentity &&
		left.Transport == right.Transport
}
