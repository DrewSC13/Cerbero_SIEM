package syslogingest

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"cerbero/services/cerbero-ingest/internal/ingestcore"
)

const syslogContentType = "application/syslog"

// TCPConfig contains the locked RFC6587 octet-counting transport boundary.
type TCPConfig struct {
	Preparer              Preparer
	Acceptor              DurableAcceptor
	Metadata              ingestcore.AdmissionMetadata
	MaxFrameSize          uint64
	ReadTimeout           time.Duration
	MaxConnectionRate     uint64
	ConcurrentConnections uint64
}

// ServeTCP accepts RFC6587 octet-counted TCP syslog connections until ctx is cancelled.
func ServeTCP(ctx context.Context, listener net.Listener, config TCPConfig) error {
	if listener == nil {
		return errors.New("syslog TCP listener is required")
	}
	if err := validateTCPConfig(config); err != nil {
		_ = listener.Close()
		return err
	}

	sem := make(chan struct{}, config.ConcurrentConnections)
	connectionRate := newConnectionTokenBucket(config.MaxConnectionRate)
	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				wg.Wait()
				return nil
			}
			return fmt.Errorf("accept syslog TCP connection: %w", err)
		}

		if !connectionRate.take() {
			_ = connection.Close()
			continue
		}

		select {
		case sem <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				defer connection.Close()
				_ = HandleTCPConnection(ctx, connection, config)
			}()
		default:
			_ = connection.Close()
		}
	}
}

// HandleTCPConnection processes frames serially so a later frame is not consumed before the
// previous frame has completed durable JetStream admission.
func HandleTCPConnection(ctx context.Context, connection net.Conn, config TCPConfig) error {
	if connection == nil {
		return errors.New("syslog TCP connection is required")
	}
	if err := validateTCPConfig(config); err != nil {
		return err
	}

	adapter, err := New(Config{Preparer: config.Preparer, Acceptor: config.Acceptor})
	if err != nil {
		return err
	}
	reader := bufio.NewReader(connection)

	for {
		if err := connection.SetReadDeadline(time.Now().Add(config.ReadTimeout)); err != nil {
			return fmt.Errorf("set syslog TCP read deadline: %w", err)
		}

		session, err := adapter.Begin(ctx, config.Metadata)
		if err != nil {
			return err
		}
		frame, err := readOctetCountingFrame(reader, config.MaxFrameSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		encoding := "binary"
		if utf8.Valid(frame) {
			encoding = "utf-8"
		}
		acceptCtx, cancel := context.WithTimeout(ctx, config.ReadTimeout)
		_, acceptErr := session.Accept(acceptCtx, Message{
			ContentType: syslogContentType,
			Encoding:    encoding,
			RawPayload:  frame,
		})
		cancel()
		if acceptErr != nil {
			return acceptErr
		}
	}
}

func validateTCPConfig(config TCPConfig) error {
	if config.Preparer == nil || config.Acceptor == nil {
		return errors.New("syslog TCP ingest boundaries are required")
	}
	if config.MaxFrameSize == 0 {
		return errors.New("syslog TCP max frame size must be greater than zero")
	}
	if config.ReadTimeout <= 0 {
		return errors.New("syslog TCP read timeout must be positive")
	}
	if config.MaxConnectionRate == 0 {
		return errors.New("syslog TCP connection-rate limit must be greater than zero")
	}
	if config.ConcurrentConnections == 0 {
		return errors.New("syslog TCP concurrent connection limit must be greater than zero")
	}
	if config.Metadata.TenantID == "" || config.Metadata.SourceID == "" ||
		config.Metadata.SensorID == "" || config.Metadata.RemoteIdentity == "" {
		return errors.New("syslog TCP source identity metadata are required")
	}
	return nil
}

type connectionTokenBucket struct {
	mu       sync.Mutex
	rate     float64
	capacity float64
	tokens   float64
	last     time.Time
}

func newConnectionTokenBucket(rate uint64) *connectionTokenBucket {
	now := time.Now()
	value := float64(rate)
	return &connectionTokenBucket{
		rate:     value,
		capacity: value,
		tokens:   value,
		last:     now,
	}
}

func (b *connectionTokenBucket) take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func readOctetCountingFrame(reader *bufio.Reader, limit uint64) ([]byte, error) {
	if reader == nil {
		return nil, errors.New("syslog TCP reader is required")
	}

	var prefix []byte
	for {
		value, err := reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && len(prefix) == 0 {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("read RFC6587 length prefix: %w", err)
		}
		if value == ' ' {
			break
		}
		if value < '0' || value > '9' {
			return nil, errors.New("RFC6587 octet-counting prefix must contain only decimal digits")
		}
		if len(prefix) >= 20 {
			return nil, errors.New("RFC6587 octet-counting length prefix is too long")
		}
		prefix = append(prefix, value)
	}
	if len(prefix) == 0 {
		return nil, errors.New("RFC6587 octet-counting length prefix is empty")
	}

	length, err := strconv.ParseUint(string(prefix), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse RFC6587 length prefix: %w", err)
	}
	if length == 0 {
		return nil, errors.New("RFC6587 octet-counted syslog frame must not be empty")
	}
	if length > limit {
		return nil, fmt.Errorf("RFC6587 frame length %d exceeds configured limit %d", length, limit)
	}
	size, err := intFromUint64(length)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, size)
	if _, err := io.ReadFull(reader, frame); err != nil {
		return nil, fmt.Errorf("read RFC6587 frame payload: %w", err)
	}
	return frame, nil
}

func intFromUint64(value uint64) (int, error) {
	converted := int(value)
	if converted < 0 || uint64(converted) != value {
		return 0, errors.New("RFC6587 frame length exceeds platform integer range")
	}
	return converted, nil
}
