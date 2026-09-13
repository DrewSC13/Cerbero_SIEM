package preserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	contractvalidation "cerbero/services/internal/contracts/validation"
)

const (
	filesystemRawDataFilename = "raw.bin"
	rawHashAlgorithmSHA256    = "sha256"
)

var (
	// ErrRawStoreConflict means an existing development Raw Store object does not
	// match the immutable RawEvent evidence identified by the same locator.
	ErrRawStoreConflict = errors.New("raw store evidence conflict")
)

// FilesystemRawStore is the ADR-0003 development-only Raw Store backend.
// It persists exact bytes beneath the configured Raw Store root without selecting
// a production object-storage provider.
type FilesystemRawStore struct {
	root string
}

// NewFilesystemRawStore binds the development adapter to an existing filesystem root.
// The repository development root is var/raw; runtime composition chooses the path.
func NewFilesystemRawStore(root string) (*FilesystemRawStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("filesystem raw store root is required")
	}

	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve filesystem raw store root: %w", err)
	}

	info, err := os.Lstat(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect filesystem raw store root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("filesystem raw store root must not be a symlink")
	}
	if !info.IsDir() {
		return nil, errors.New("filesystem raw store root must be a directory")
	}

	return &FilesystemRawStore{root: absoluteRoot}, nil
}

// EnsureDurable creates or discovers one immutable development raw object.
// Success means the exact bytes are readable from the final locator, size/hash verified,
// and the containing directory entry has been synchronized.
func (s *FilesystemRawStore) EnsureDurable(ctx context.Context, evidence RawEvidence) (RawObject, error) {
	if err := ctx.Err(); err != nil {
		return RawObject{}, err
	}
	if err := validateFilesystemRawEvidence(evidence); err != nil {
		return RawObject{}, err
	}

	when := evidence.IngestTime.UTC()
	segmentID := evidence.EventID
	parts := []string{
		evidence.TenantID,
		when.Format("2006"),
		when.Format("01"),
		when.Format("02"),
		when.Format("15"),
		segmentID,
	}

	segmentDir, err := ensureDurableDirectories(s.root, parts...)
	if err != nil {
		return RawObject{}, fmt.Errorf("prepare raw segment directory: %w", err)
	}

	finalPath := filepath.Join(segmentDir, filesystemRawDataFilename)
	if err := ensureDurableRawFile(ctx, segmentDir, finalPath, evidence); err != nil {
		return RawObject{}, err
	}
	if err := ensureClosedFilesystemSegment(ctx, segmentDir, evidence); err != nil {
		return RawObject{}, fmt.Errorf("close raw segment: %w", err)
	}

	uriPath := append(append([]string(nil), parts...), filesystemRawDataFilename)
	storageURI := (&url.URL{Scheme: "raw", Path: "/" + path.Join(uriPath...)}).String()

	return RawObject{
		StorageURI: storageURI,
		SegmentID:  segmentID,
		Offset:     0,
		Length:     evidence.Size,
		Hash:       evidence.Hash,
	}, nil
}

func validateFilesystemRawEvidence(evidence RawEvidence) error {
	if err := validateFilesystemPathComponent("tenant_id", evidence.TenantID); err != nil {
		return err
	}
	if err := contractvalidation.UUIDv7("raw_store.event_id", evidence.EventID); err != nil {
		return err
	}
	if evidence.IngestTime.IsZero() {
		return errors.New("raw store ingest_time is required")
	}
	if evidence.HashAlgorithm != rawHashAlgorithmSHA256 {
		return fmt.Errorf("raw store hash algorithm: expected %s", rawHashAlgorithmSHA256)
	}
	if evidence.Size != uint64(len(evidence.Bytes)) {
		return errors.New("raw store evidence size does not match exact byte length")
	}

	actualHash := contractvalidation.SHA256LowerHex(evidence.Bytes)
	if evidence.Hash != actualHash {
		return errors.New("raw store evidence hash does not match exact bytes")
	}
	return nil
}

func validateFilesystemPathComponent(name, value string) error {
	if value == "" {
		return fmt.Errorf("raw store %s is required", name)
	}
	if value == "." || value == ".." || strings.ContainsAny(value, `/\\`) || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("raw store %s is not a safe path component", name)
	}
	return nil
}

func ensureDurableDirectories(root string, parts ...string) (string, error) {
	current := root
	for _, part := range parts {
		if err := validateFilesystemPathComponent("path component", part); err != nil {
			return "", err
		}

		next := filepath.Join(current, part)
		err := os.Mkdir(next, 0o750)
		if err != nil && !errors.Is(err, fs.ErrExist) {
			return "", fmt.Errorf("create directory %q: %w", next, err)
		}

		info, err := os.Lstat(next)
		if err != nil {
			return "", fmt.Errorf("inspect directory %q: %w", next, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", fmt.Errorf("raw store path %q is not a real directory", next)
		}
		if err := syncDirectory(current); err != nil {
			return "", fmt.Errorf("sync parent directory %q: %w", current, err)
		}
		current = next
	}
	return current, nil
}

func ensureDurableRawFile(ctx context.Context, segmentDir, finalPath string, evidence RawEvidence) error {
	if _, err := os.Lstat(finalPath); err == nil {
		if err := syncDirectory(segmentDir); err != nil {
			return fmt.Errorf("sync existing raw segment directory: %w", err)
		}
		return verifyFilesystemRawFile(finalPath, evidence)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("inspect existing raw object: %w", err)
	}

	temp, err := os.CreateTemp(segmentDir, ".raw-tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary raw object: %w", err)
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}()

	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("set temporary raw object permissions: %w", err)
	}
	if _, err := io.Copy(temp, bytes.NewReader(evidence.Bytes)); err != nil {
		return fmt.Errorf("write exact raw bytes: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync temporary raw object: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temporary raw object: %w", err)
	}

	if err := os.Link(tempPath, finalPath); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("publish immutable raw object: %w", err)
		}
		if err := syncDirectory(segmentDir); err != nil {
			return fmt.Errorf("sync concurrent raw segment directory: %w", err)
		}
		return verifyFilesystemRawFile(finalPath, evidence)
	}

	if err := os.Remove(tempPath); err != nil {
		return fmt.Errorf("remove temporary raw object link: %w", err)
	}
	if err := syncDirectory(segmentDir); err != nil {
		return fmt.Errorf("sync raw segment directory: %w", err)
	}
	if err := verifyFilesystemRawFile(finalPath, evidence); err != nil {
		return err
	}
	return nil
}

func verifyFilesystemRawFile(filename string, evidence RawEvidence) error {
	info, err := os.Lstat(filename)
	if err != nil {
		return fmt.Errorf("inspect durable raw object: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: raw object is not a regular file", ErrRawStoreConflict)
	}
	if uint64(info.Size()) != evidence.Size {
		return fmt.Errorf("%w: raw object size differs", ErrRawStoreConflict)
	}

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open durable raw object: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return fmt.Errorf("hash durable raw object: %w", err)
	}
	if hash := fmt.Sprintf("%x", hasher.Sum(nil)); hash != evidence.Hash {
		return fmt.Errorf("%w: raw object hash differs", ErrRawStoreConflict)
	}
	return nil
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
