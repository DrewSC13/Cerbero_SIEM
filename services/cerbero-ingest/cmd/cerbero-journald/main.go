package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"cerbero/services/cerbero-ingest/internal/journald"
	"cerbero/services/cerbero-ingest/internal/sourceapp"
)

const sourcePrefix = "CERBERO_JOURNALD"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Getenv, logger); err != nil {
		logger.Error("cerbero journald collector stopped with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	common, err := sourceapp.LoadConfig(getenv, journald.Transport, sourcePrefix)
	if err != nil {
		return err
	}
	runtime, err := sourceapp.Open(common)
	if err != nil {
		return err
	}
	defer runtime.Close()

	adapter, err := journald.NewAdapter(runtime.Core, runtime.Acceptor)
	if err != nil {
		return err
	}

	var collector journald.Collector
	exportFile := strings.TrimSpace(getenv("CERBERO_JOURNALD_EXPORT_FILE"))
	if exportFile != "" {
		file, err := os.Open(exportFile)
		if err != nil {
			return err
		}
		defer file.Close()
		collector, err = journald.NewExportCollector(file, runtime.Metadata, common.MaxPayloadSize)
		if err != nil {
			return err
		}
		logger.Warn(
			"CERBERO DEVELOPMENT journald fixture collector enabled",
			"export_file", exportFile,
		)
	} else {
		cursorFile := strings.TrimSpace(getenv("CERBERO_JOURNALD_CURSOR_FILE"))
		if cursorFile == "" {
			return errors.New("CERBERO_JOURNALD_CURSOR_FILE is required for the live journald collector")
		}
		cursor, err := loadCursor(cursorFile)
		if err != nil {
			return err
		}
		hostCollector, err := journald.NewJournalctlCollector(
			ctx,
			runtime.Metadata,
			common.MaxPayloadSize,
			cursor,
		)
		if err != nil {
			return err
		}
		collector = hostCollector
		logger.Info(
			"CERBERO journald host collector enabled",
			"source", runtime.Metadata.SourceID,
			"resume_cursor_present", cursor != "",
		)
	}

	for {
		session, err := adapter.Begin(ctx, runtime.Metadata)
		if err != nil {
			return err
		}
		entry, err := collector.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) && exportFile != "" {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if _, err := session.Accept(ctx, entry); err != nil {
			return err
		}
		if exportFile == "" {
			if entry.Cursor == "" {
				return errors.New("live journald entry is missing __CURSOR; durable resume cannot advance")
			}
			if err := storeCursor(
				strings.TrimSpace(getenv("CERBERO_JOURNALD_CURSOR_FILE")),
				entry.Cursor,
			); err != nil {
				return err
			}
		}
	}
}

func loadCursor(path string) (string, error) {
	value, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}

func storeCursor(path, cursor string) error {
	if path == "" || cursor == "" {
		return errors.New("journald cursor path and value are required")
	}
	temp := path + ".tmp"
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(cursor + "\n"); err != nil {
		_ = file.Close()
		_ = os.Remove(temp)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(temp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(temp)
		return err
	}
	return os.Rename(temp, path)
}
