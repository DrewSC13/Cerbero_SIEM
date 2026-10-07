package syslogingest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

type noReadDeadlineConn struct {
	net.Conn
}

func (noReadDeadlineConn) SetReadDeadline(time.Time) error {
	return nil
}

func TestHandleTCPConnectionPreservesRFC6587FrameBytes(t *testing.T) {
	admission := &admissionFake{}
	preparer := &preparerFake{admission: admission}
	acceptor := &acceptorFake{}
	server, client := net.Pipe()
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})

	raw := []byte("Failed password for invalid user admin from 10.0.0.8 port 50341 ssh2")
	result := make(chan error, 1)
	go func() {
		result <- HandleTCPConnection(context.Background(), noReadDeadlineConn{Conn: server}, TCPConfig{
			Preparer:              preparer,
			Acceptor:              acceptor,
			Metadata:              syslogTCPMetadata(),
			MaxFrameSize:          1024,
			ReadTimeout:           time.Second,
			MaxConnectionRate:     10,
			ConcurrentConnections: 1,
		})
	}()

	frame := append([]byte(fmt.Sprintf("%d ", len(raw))), raw...)
	if _, err := client.Write(frame); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("HandleTCPConnection() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HandleTCPConnection() did not finish")
	}

	if !acceptor.called {
		t.Fatal("durable acceptor was not called")
	}
	if !bytes.Equal(admission.request.RawPayload, raw) {
		t.Fatalf("raw frame changed: got %q want %q", admission.request.RawPayload, raw)
	}
	if admission.request.Transport != Transport {
		t.Fatalf("transport = %q, want %q", admission.request.Transport, Transport)
	}
}

func TestReadOctetCountingFrameRejectsMalformedAndOversizedFrames(t *testing.T) {
	tests := []struct {
		name string
		wire string
		max  uint64
	}{
		{name: "non-decimal", wire: "x hello", max: 64},
		{name: "empty-prefix", wire: " hello", max: 64},
		{name: "zero", wire: "0 ", max: 64},
		{name: "oversized", wire: "5 hello", max: 4},
		{name: "short-body", wire: "5 hi", max: 64},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := readOctetCountingFrame(
				bufio.NewReader(bytes.NewBufferString(test.wire)),
				test.max,
			); err == nil {
				t.Fatal("readOctetCountingFrame() error = nil")
			}
		})
	}
}

func syslogTCPMetadata() ingestcore.AdmissionMetadata {
	return ingestcore.AdmissionMetadata{
		TenantID:       "tenant-a",
		SourceID:       "syslog-a",
		SensorID:       "sensor-a",
		RemoteIdentity: "peer-a",
	}
}
