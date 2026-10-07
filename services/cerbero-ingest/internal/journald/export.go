package journald

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"unicode/utf8"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

const canonicalVersion = 1

// ExportCollector parses the systemd Journal Export Format without normalizing field values.
type ExportCollector struct {
	reader        *bufio.Reader
	source        ingestcore.AdmissionMetadata
	maxEntryBytes uint64
}

// NewExportCollector binds a journal export stream to one configured CERBERO source.
func NewExportCollector(
	reader io.Reader,
	source ingestcore.AdmissionMetadata,
	maxEntryBytes uint64,
) (*ExportCollector, error) {
	if reader == nil {
		return nil, errors.New("journald export reader is required")
	}
	source.Transport = Transport
	if source.TenantID == "" || source.SourceID == "" ||
		source.SensorID == "" || source.RemoteIdentity == "" {
		return nil, errors.New("journald source identity metadata are required")
	}
	if maxEntryBytes == 0 {
		return nil, errors.New("journald max entry size must be greater than zero")
	}
	return &ExportCollector{
		reader:        bufio.NewReader(reader),
		source:        source,
		maxEntryBytes: maxEntryBytes,
	}, nil
}

type exportField struct {
	Name  string
	Value []byte
}

type canonicalEntry struct {
	Version int              `json:"cerbero_journald_version"`
	Cursor  *string          `json:"cursor"`
	Fields  []canonicalField `json:"fields"`
}

type canonicalField struct {
	Name     string `json:"name"`
	Encoding string `json:"encoding"`
	Value    string `json:"value"`
}

// Next reads one complete journal export entry and converts it to ADR-0012 canonical bytes.
func (c *ExportCollector) Next(ctx context.Context) (Entry, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}

	fields, cursor, err := c.readEntry()
	if err != nil {
		return Entry{}, err
	}
	raw, err := encodeCanonical(cursor, fields)
	if err != nil {
		return Entry{}, err
	}
	if uint64(len(raw)) > c.maxEntryBytes {
		return Entry{}, fmt.Errorf(
			"journald canonical entry size %d exceeds configured limit %d",
			len(raw),
			c.maxEntryBytes,
		)
	}

	entry := Entry{
		Source:            c.source,
		Cursor:            cursor,
		RawRepresentation: raw,
	}
	if err := entry.Validate(); err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func (c *ExportCollector) readEntry() ([]exportField, string, error) {
	fields := make([]exportField, 0, 16)
	var cursor string
	var total uint64

	for {
		line, err := c.reader.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, "", fmt.Errorf("read journald export line: %w", err)
		}
		if len(line) == 0 && errors.Is(err, io.EOF) {
			if len(fields) == 0 {
				return nil, "", io.EOF
			}
			break
		}

		total += uint64(len(line))
		if total > c.maxEntryBytes {
			return nil, "", fmt.Errorf(
				"journald export entry exceeds configured limit %d",
				c.maxEntryBytes,
			)
		}

		if line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			if len(fields) == 0 {
				if errors.Is(err, io.EOF) {
					return nil, "", io.EOF
				}
				continue
			}
			break
		}

		if index := bytesIndexByte(line, '='); index >= 0 {
			name := string(line[:index])
			value := append([]byte(nil), line[index+1:]...)
			if err := validateExportFieldName(name); err != nil {
				return nil, "", err
			}
			fields = append(fields, exportField{Name: name, Value: value})
			if name == "__CURSOR" && utf8.Valid(value) {
				cursor = string(value)
			}
		} else {
			name := string(line)
			if err := validateExportFieldName(name); err != nil {
				return nil, "", err
			}
			value, consumed, err := c.readBinaryValue()
			if err != nil {
				return nil, "", err
			}
			total += consumed
			if total > c.maxEntryBytes {
				return nil, "", fmt.Errorf(
					"journald export entry exceeds configured limit %d",
					c.maxEntryBytes,
				)
			}
			fields = append(fields, exportField{Name: name, Value: value})
			if name == "__CURSOR" && utf8.Valid(value) {
				cursor = string(value)
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	if len(fields) == 0 {
		return nil, "", io.EOF
	}
	return fields, cursor, nil
}

func (c *ExportCollector) readBinaryValue() ([]byte, uint64, error) {
	var lengthBytes [8]byte
	if _, err := io.ReadFull(c.reader, lengthBytes[:]); err != nil {
		return nil, 0, fmt.Errorf("read journald binary length: %w", err)
	}
	length := binary.LittleEndian.Uint64(lengthBytes[:])
	if length > c.maxEntryBytes {
		return nil, 0, fmt.Errorf(
			"journald binary field size %d exceeds configured limit %d",
			length,
			c.maxEntryBytes,
		)
	}
	size, err := intFromUint64(length)
	if err != nil {
		return nil, 0, err
	}
	value := make([]byte, size)
	if _, err := io.ReadFull(c.reader, value); err != nil {
		return nil, 0, fmt.Errorf("read journald binary value: %w", err)
	}
	terminator, err := c.reader.ReadByte()
	if err != nil {
		return nil, 0, fmt.Errorf("read journald binary terminator: %w", err)
	}
	if terminator != '\n' {
		return nil, 0, errors.New("journald binary field must end with newline")
	}
	return value, 8 + length + 1, nil
}

func encodeCanonical(cursor string, fields []exportField) ([]byte, error) {
	canonicalFields := make([]canonicalField, 0, len(fields))
	for _, field := range fields {
		encoded := canonicalField{Name: field.Name}
		if utf8.Valid(field.Value) {
			encoded.Encoding = "utf8"
			encoded.Value = string(field.Value)
		} else {
			encoded.Encoding = "base64"
			encoded.Value = base64.StdEncoding.EncodeToString(field.Value)
		}
		canonicalFields = append(canonicalFields, encoded)
	}

	var cursorValue *string
	if cursor != "" {
		copy := cursor
		cursorValue = &copy
	}
	raw, err := json.Marshal(canonicalEntry{
		Version: canonicalVersion,
		Cursor:  cursorValue,
		Fields:  canonicalFields,
	})
	if err != nil {
		return nil, fmt.Errorf("encode journald canonical representation: %w", err)
	}
	return raw, nil
}

func validateExportFieldName(name string) error {
	if name == "" || len(name) > 64 {
		return errors.New("journald export field name must contain 1..64 bytes")
	}
	for _, value := range []byte(name) {
		if value != '_' && (value < 'A' || value > 'Z') && (value < '0' || value > '9') {
			return fmt.Errorf("invalid journald export field name %q", name)
		}
	}
	return nil
}

func bytesIndexByte(value []byte, target byte) int {
	for index, current := range value {
		if current == target {
			return index
		}
	}
	return -1
}

func intFromUint64(value uint64) (int, error) {
	converted := int(value)
	if converted < 0 || uint64(converted) != value {
		return 0, errors.New("journald field length exceeds platform integer range")
	}
	return converted, nil
}

// JournalctlCollector owns a journalctl --output=export subprocess.
type JournalctlCollector struct {
	command   *exec.Cmd
	collector *ExportCollector
	waited    bool
}

// NewJournalctlCollector starts the host-local journal reader using the governed export format.
func NewJournalctlCollector(
	ctx context.Context,
	source ingestcore.AdmissionMetadata,
	maxEntryBytes uint64,
	afterCursor string,
) (*JournalctlCollector, error) {
	command := exec.CommandContext(ctx, "journalctl", journalctlArgs(afterCursor)...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("open journalctl stdout: %w", err)
	}
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start journalctl: %w", err)
	}
	collector, err := NewExportCollector(stdout, source, maxEntryBytes)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return nil, err
	}
	return &JournalctlCollector{command: command, collector: collector}, nil
}

func journalctlArgs(afterCursor string) []string {
	args := []string{"--output=export", "--follow", "--no-pager"}
	if afterCursor == "" {
		return append(args, "--lines=0")
	}
	return append(args, "--after-cursor="+afterCursor)
}

// Next returns one host journal entry. Command termination is surfaced after buffered entries drain.
func (c *JournalctlCollector) Next(ctx context.Context) (Entry, error) {
	entry, err := c.collector.Next(ctx)
	if err == nil {
		return entry, nil
	}
	if !errors.Is(err, io.EOF) || c.waited {
		return Entry{}, err
	}
	c.waited = true
	if waitErr := c.command.Wait(); waitErr != nil && ctx.Err() == nil {
		return Entry{}, fmt.Errorf("journalctl stopped: %w", waitErr)
	}
	if ctx.Err() != nil {
		return Entry{}, ctx.Err()
	}
	return Entry{}, io.EOF
}
