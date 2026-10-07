package journald

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
	contractsv1 "cerbero/services/internal/contracts/v1"
)

func TestExportCollectorPreservesOrderDuplicatesBinaryAndCursor(t *testing.T) {
	var stream bytes.Buffer
	stream.WriteString("__CURSOR=s=abc;i=42\n")
	stream.WriteString("MESSAGE=first\n")
	stream.WriteString("MESSAGE=second\n")
	stream.WriteString("_BINARY\n")
	if err := binary.Write(&stream, binary.LittleEndian, uint64(4)); err != nil {
		t.Fatal(err)
	}
	stream.Write([]byte{0, 1, 2, 0xff})
	stream.WriteByte('\n')
	stream.WriteByte('\n')

	collector, err := NewExportCollector(&stream, journaldSource(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := collector.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if entry.Cursor != "s=abc;i=42" {
		t.Fatalf("cursor = %q", entry.Cursor)
	}

	var canonical struct {
		Version int     `json:"cerbero_journald_version"`
		Cursor  *string `json:"cursor"`
		Fields  []struct {
			Name     string `json:"name"`
			Encoding string `json:"encoding"`
			Value    string `json:"value"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(entry.RawRepresentation, &canonical); err != nil {
		t.Fatal(err)
	}
	if canonical.Version != 1 || canonical.Cursor == nil || *canonical.Cursor != entry.Cursor {
		t.Fatalf("unexpected canonical metadata: %+v", canonical)
	}
	if len(canonical.Fields) != 4 {
		t.Fatalf("field count = %d", len(canonical.Fields))
	}
	if canonical.Fields[1].Name != "MESSAGE" ||
		canonical.Fields[1].Value != "first" ||
		canonical.Fields[2].Name != "MESSAGE" ||
		canonical.Fields[2].Value != "second" {
		t.Fatalf("duplicate field order changed: %+v", canonical.Fields)
	}
	if canonical.Fields[3].Encoding != "base64" ||
		canonical.Fields[3].Value != "AAEC/w==" {
		t.Fatalf("binary field encoding changed: %+v", canonical.Fields[3])
	}
}

func TestExportCollectorRejectsOversizedEntry(t *testing.T) {
	collector, err := NewExportCollector(
		bytes.NewBufferString("MESSAGE=1234567890\n\n"),
		journaldSource(),
		8,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collector.Next(context.Background()); err == nil {
		t.Fatal("Next() accepted oversized journal export entry")
	}
}

type journalPreparerFake struct {
	metadata ingestcore.AdmissionMetadata
	session  *journalAdmissionFake
}

func (f *journalPreparerFake) Begin(
	_ context.Context,
	metadata ingestcore.AdmissionMetadata,
) (ingestcore.Admission, error) {
	f.metadata = metadata
	return f.session, nil
}

type journalAdmissionFake struct {
	request ingestcore.Request
}

func (f *journalAdmissionFake) Prepare(request ingestcore.Request) (*ingestcore.Result, error) {
	f.request = request
	return &ingestcore.Result{
		RawEvent: &contractsv1.RawEvent{EventId: "event-test"},
		Envelope: &contractsv1.CerberoEnvelope{MessageId: "message-test"},
	}, nil
}

type journalAcceptorFake struct {
	err error
}

func (f *journalAcceptorFake) Accept(context.Context, *ingestcore.Result) error {
	return f.err
}

func TestAdapterAdmitsCanonicalBytesOnlyAfterDurability(t *testing.T) {
	admission := &journalAdmissionFake{}
	preparer := &journalPreparerFake{session: admission}
	acceptor := &journalAcceptorFake{}
	adapter, err := NewAdapter(preparer, acceptor)
	if err != nil {
		t.Fatal(err)
	}

	session, err := adapter.Begin(context.Background(), journaldSource())
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"cerbero_journald_version":1,"cursor":null,"fields":[{"name":"MESSAGE","encoding":"utf8","value":"x"}]}`)
	entry := Entry{
		Source:            journaldSource(),
		RawRepresentation: raw,
	}
	if _, err := session.Accept(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(admission.request.RawPayload, raw) {
		t.Fatalf("canonical bytes changed: got %q want %q", admission.request.RawPayload, raw)
	}
	if admission.request.SequenceNumber != nil {
		t.Fatal("journald cursor was incorrectly mapped to sequence_number")
	}

	acceptor.err = errors.New("nats unavailable")
	if result, err := session.Accept(context.Background(), entry); err == nil || result != nil {
		t.Fatal("durability failure returned accepted journald result")
	}
}

func TestJournalctlArgsStartAtLiveTailOrAfterDurableCursor(t *testing.T) {
	withoutCursor := journalctlArgs("")
	if withoutCursor[len(withoutCursor)-1] != "--lines=0" {
		t.Fatalf("journalctl args without cursor = %v", withoutCursor)
	}
	withCursor := journalctlArgs("s=step34;i=42")
	if withCursor[len(withCursor)-1] != "--after-cursor=s=step34;i=42" {
		t.Fatalf("journalctl args with cursor = %v", withCursor)
	}
}
