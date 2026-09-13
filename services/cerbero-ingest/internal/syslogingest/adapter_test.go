package syslogingest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
	contractsv1 "cerbero/services/internal/contracts/v1"
)

type preparerFake struct {
	metadata  ingestcore.AdmissionMetadata
	admission *admissionFake
	err       error
}

func (f *preparerFake) Begin(
	_ context.Context,
	metadata ingestcore.AdmissionMetadata,
) (ingestcore.Admission, error) {
	f.metadata = metadata
	if f.err != nil {
		return nil, f.err
	}
	return f.admission, nil
}

type admissionFake struct {
	request ingestcore.Request
	err     error
}

func (f *admissionFake) Prepare(request ingestcore.Request) (*ingestcore.Result, error) {
	f.request = request
	if f.err != nil {
		return nil, f.err
	}
	return &ingestcore.Result{
		RawEvent: &contractsv1.RawEvent{EventId: "event-test"},
		Envelope: &contractsv1.CerberoEnvelope{MessageId: "message-test"},
	}, nil
}

type acceptorFake struct {
	called bool
	err    error
}

func (f *acceptorFake) Accept(context.Context, *ingestcore.Result) error {
	f.called = true
	return f.err
}

func TestAdapterPreservesExactFramedSyslogBytesAndUsesCommonCore(t *testing.T) {
	admission := &admissionFake{}
	preparer := &preparerFake{admission: admission}
	acceptor := &acceptorFake{}
	adapter, err := New(Config{Preparer: preparer, Acceptor: acceptor})
	if err != nil {
		t.Fatal(err)
	}

	session, err := adapter.Begin(context.Background(), ingestcore.AdmissionMetadata{
		TenantID:       "tenant-a",
		SourceID:       "syslog-source-a",
		SensorID:       "sensor-a",
		RemoteIdentity: "syslog-peer-a",
		Transport:      "caller-must-not-select-transport",
	})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if preparer.metadata.Transport != Transport {
		t.Fatalf("transport = %q, want %q", preparer.metadata.Transport, Transport)
	}

	raw := []byte("<34>1 2026-09-13T04:20:00Z host app 123 ID47 - message  \r\n")
	result, err := session.Accept(context.Background(), Message{
		ContentType: "text/plain",
		Encoding:    "utf-8",
		RawPayload:  raw,
	})
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if result == nil || !acceptor.called {
		t.Fatal("durable acceptor was not completed")
	}
	if !bytes.Equal(admission.request.RawPayload, raw) {
		t.Fatalf("syslog bytes changed: got %q want %q", admission.request.RawPayload, raw)
	}
	if admission.request.ContentType != "text/plain" || admission.request.Encoding != "utf-8" {
		t.Fatalf(
			"declared metadata changed: content_type=%q encoding=%q",
			admission.request.ContentType,
			admission.request.Encoding,
		)
	}
}

func TestAdapterReturnsNoAcceptedResultWhenDurabilityFails(t *testing.T) {
	admission := &admissionFake{}
	adapter, err := New(Config{
		Preparer: &preparerFake{admission: admission},
		Acceptor: &acceptorFake{err: errors.New("jetstream unavailable")},
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := adapter.Begin(context.Background(), ingestcore.AdmissionMetadata{
		TenantID:       "tenant-a",
		SourceID:       "source-a",
		SensorID:       "sensor-a",
		RemoteIdentity: "peer-a",
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := session.Accept(context.Background(), Message{RawPayload: []byte("raw")})
	if err == nil {
		t.Fatal("Accept() error = nil, want durability failure")
	}
	if result != nil {
		t.Fatal("Accept() returned a result despite durability failure")
	}
}

func TestAdapterRequiresInjectedBoundaries(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New() accepted missing dependencies")
	}
	if _, err := New(Config{Preparer: &preparerFake{}}); err == nil {
		t.Fatal("New() accepted missing durable acceptor")
	}
}
