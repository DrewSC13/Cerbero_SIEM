package preserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	filesystemManifestFilename     = "manifest.json"
	filesystemManifestHashFilename = "manifest.sha256"
	rawSegmentManifestVersion      = 1
)

type rawSegmentManifestV1 struct {
	ManifestVersion     int     `json:"manifest_version"`
	SegmentID           string  `json:"segment_id"`
	TenantID            string  `json:"tenant_id"`
	EventCount          uint64  `json:"event_count"`
	FirstEventID        string  `json:"first_event_id"`
	LastEventID         string  `json:"last_event_id"`
	CreatedAt           string  `json:"created_at"`
	PreviousSegmentHash *string `json:"previous_segment_hash"`
	ContentHash         string  `json:"content_hash"`
}

// ensureClosedFilesystemSegment completes the DEVELOPMENT closed-segment artifacts.
// The current one-event backend intentionally leaves previous_segment_hash null until
// multi-event rotation and cross-process chain ordering are separately governed.
func ensureClosedFilesystemSegment(ctx context.Context, segmentDir string, evidence RawEvidence) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	manifestPath := filepath.Join(segmentDir, filesystemManifestFilename)
	manifestBytes, err := ensureFilesystemSegmentManifest(ctx, segmentDir, manifestPath, evidence)
	if err != nil {
		return err
	}

	digest := sha256.Sum256(manifestBytes)
	hashBytes := []byte(fmt.Sprintf("%x\n", digest[:]))
	hashPath := filepath.Join(segmentDir, filesystemManifestHashFilename)
	if err := ensureExactFilesystemArtifact(ctx, segmentDir, hashPath, ".manifest-hash-tmp-*", hashBytes); err != nil {
		return fmt.Errorf("persist manifest hash: %w", err)
	}

	if err := verifyFilesystemSegmentClosure(segmentDir, evidence); err != nil {
		return err
	}
	if err := syncDirectory(segmentDir); err != nil {
		return fmt.Errorf("sync closed raw segment directory: %w", err)
	}
	return nil
}

func ensureFilesystemSegmentManifest(
	ctx context.Context,
	segmentDir string,
	manifestPath string,
	evidence RawEvidence,
) ([]byte, error) {
	if _, err := os.Lstat(manifestPath); err == nil {
		manifestBytes, err := readRegularFilesystemArtifact(manifestPath, "raw segment manifest")
		if err != nil {
			return nil, err
		}
		if err := validateFilesystemSegmentManifest(manifestBytes, evidence); err != nil {
			return nil, err
		}
		return manifestBytes, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("inspect raw segment manifest: %w", err)
	}

	manifest := rawSegmentManifestV1{
		ManifestVersion:     rawSegmentManifestVersion,
		SegmentID:           evidence.EventID,
		TenantID:            evidence.TenantID,
		EventCount:          1,
		FirstEventID:        evidence.EventID,
		LastEventID:         evidence.EventID,
		CreatedAt:           time.Now().UTC().Format(time.RFC3339Nano),
		PreviousSegmentHash: nil,
		ContentHash:         "sha256:" + evidence.Hash,
	}
	candidate, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshal raw segment manifest: %w", err)
	}
	candidate = append(candidate, '\n')

	if _, err := publishImmutableFilesystemArtifact(
		ctx,
		segmentDir,
		manifestPath,
		".manifest-tmp-*",
		candidate,
	); err != nil {
		return nil, fmt.Errorf("publish raw segment manifest: %w", err)
	}

	manifestBytes, err := readRegularFilesystemArtifact(manifestPath, "raw segment manifest")
	if err != nil {
		return nil, err
	}
	if err := validateFilesystemSegmentManifest(manifestBytes, evidence); err != nil {
		return nil, err
	}
	return manifestBytes, nil
}

func ensureExactFilesystemArtifact(
	ctx context.Context,
	segmentDir string,
	finalPath string,
	tempPattern string,
	expected []byte,
) error {
	if _, err := publishImmutableFilesystemArtifact(ctx, segmentDir, finalPath, tempPattern, expected); err != nil {
		return err
	}
	actual, err := readRegularFilesystemArtifact(finalPath, filepath.Base(finalPath))
	if err != nil {
		return err
	}
	if !bytes.Equal(actual, expected) {
		return fmt.Errorf("%w: %s differs", ErrRawStoreConflict, filepath.Base(finalPath))
	}
	return nil
}

func publishImmutableFilesystemArtifact(
	ctx context.Context,
	segmentDir string,
	finalPath string,
	tempPattern string,
	payload []byte,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := os.Lstat(finalPath); err == nil {
		if err := syncDirectory(segmentDir); err != nil {
			return false, fmt.Errorf("sync existing segment artifact directory: %w", err)
		}
		return false, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, fmt.Errorf("inspect segment artifact: %w", err)
	}

	temp, err := os.CreateTemp(segmentDir, tempPattern)
	if err != nil {
		return false, fmt.Errorf("create temporary segment artifact: %w", err)
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}()

	if err := temp.Chmod(0o600); err != nil {
		return false, fmt.Errorf("set temporary segment artifact permissions: %w", err)
	}
	if _, err := io.Copy(temp, bytes.NewReader(payload)); err != nil {
		return false, fmt.Errorf("write temporary segment artifact: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := temp.Sync(); err != nil {
		return false, fmt.Errorf("sync temporary segment artifact: %w", err)
	}
	if err := temp.Close(); err != nil {
		return false, fmt.Errorf("close temporary segment artifact: %w", err)
	}

	if err := os.Link(tempPath, finalPath); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return false, fmt.Errorf("publish immutable segment artifact: %w", err)
		}
		if err := syncDirectory(segmentDir); err != nil {
			return false, fmt.Errorf("sync concurrent segment artifact directory: %w", err)
		}
		return false, nil
	}

	if err := os.Remove(tempPath); err != nil {
		return false, fmt.Errorf("remove temporary segment artifact link: %w", err)
	}
	if err := syncDirectory(segmentDir); err != nil {
		return false, fmt.Errorf("sync segment artifact directory: %w", err)
	}
	return true, nil
}

func verifyFilesystemSegmentClosure(segmentDir string, evidence RawEvidence) error {
	manifestPath := filepath.Join(segmentDir, filesystemManifestFilename)
	manifestBytes, err := readRegularFilesystemArtifact(manifestPath, "raw segment manifest")
	if err != nil {
		return err
	}
	if err := validateFilesystemSegmentManifest(manifestBytes, evidence); err != nil {
		return err
	}

	digest := sha256.Sum256(manifestBytes)
	expectedHash := []byte(fmt.Sprintf("%x\n", digest[:]))
	hashPath := filepath.Join(segmentDir, filesystemManifestHashFilename)
	actualHash, err := readRegularFilesystemArtifact(hashPath, "raw segment manifest hash")
	if err != nil {
		return err
	}
	if !bytes.Equal(actualHash, expectedHash) {
		return fmt.Errorf("%w: manifest hash does not match exact manifest bytes", ErrRawStoreConflict)
	}
	return nil
}

func validateFilesystemSegmentManifest(manifestBytes []byte, evidence RawEvidence) error {
	if len(manifestBytes) == 0 || manifestBytes[len(manifestBytes)-1] != '\n' {
		return fmt.Errorf("%w: manifest must end with exactly one LF", ErrRawStoreConflict)
	}
	if len(manifestBytes) >= 2 && manifestBytes[len(manifestBytes)-2] == '\n' {
		return fmt.Errorf("%w: manifest has more than one trailing LF", ErrRawStoreConflict)
	}

	decoder := json.NewDecoder(bytes.NewReader(manifestBytes))
	decoder.DisallowUnknownFields()
	var manifest rawSegmentManifestV1
	if err := decoder.Decode(&manifest); err != nil {
		return fmt.Errorf("%w: decode raw segment manifest: %v", ErrRawStoreConflict, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: raw segment manifest contains trailing JSON data", ErrRawStoreConflict)
	}

	canonical, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal raw segment manifest for verification: %w", err)
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(canonical, manifestBytes) {
		return fmt.Errorf("%w: raw segment manifest is not in canonical producer form", ErrRawStoreConflict)
	}

	if manifest.ManifestVersion != rawSegmentManifestVersion {
		return fmt.Errorf("%w: manifest_version differs", ErrRawStoreConflict)
	}
	if manifest.SegmentID != evidence.EventID || manifest.TenantID != evidence.TenantID {
		return fmt.Errorf("%w: manifest identity differs", ErrRawStoreConflict)
	}
	if manifest.EventCount != 1 || manifest.FirstEventID != evidence.EventID || manifest.LastEventID != evidence.EventID {
		return fmt.Errorf("%w: manifest event range differs", ErrRawStoreConflict)
	}
	if manifest.PreviousSegmentHash != nil {
		return fmt.Errorf("%w: development manifest unexpectedly claims a previous segment hash", ErrRawStoreConflict)
	}
	if manifest.ContentHash != "sha256:"+evidence.Hash {
		return fmt.Errorf("%w: manifest content hash differs", ErrRawStoreConflict)
	}
	if !strings.HasSuffix(manifest.CreatedAt, "Z") {
		return fmt.Errorf("%w: manifest created_at must be UTC", ErrRawStoreConflict)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt)
	if err != nil || createdAt.IsZero() {
		return fmt.Errorf("%w: manifest created_at is invalid", ErrRawStoreConflict)
	}
	return nil
}

func readRegularFilesystemArtifact(filename, description string) ([]byte, error) {
	info, err := os.Lstat(filename)
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", description, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrRawStoreConflict, description)
	}
	payload, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", description, err)
	}
	return payload, nil
}
