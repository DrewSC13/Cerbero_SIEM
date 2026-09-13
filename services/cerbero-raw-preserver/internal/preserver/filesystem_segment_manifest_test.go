package preserver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFilesystemRawStoreClosesSegmentWithManifestAndHash(t *testing.T) {
	root := t.TempDir()
	store := newFilesystemRawStoreForTest(t, root)
	evidence := filesystemRawStoreTestEvidence()

	if _, err := store.EnsureDurable(context.Background(), evidence); err != nil {
		t.Fatalf("EnsureDurable() error = %v", err)
	}

	segmentDir := filepath.Dir(filesystemRawStoreTestPath(root, evidence))
	manifestPath := filepath.Join(segmentDir, filesystemManifestFilename)
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}

	var manifest rawSegmentManifestV1
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.ManifestVersion != rawSegmentManifestVersion ||
		manifest.SegmentID != evidence.EventID ||
		manifest.TenantID != evidence.TenantID ||
		manifest.EventCount != 1 ||
		manifest.FirstEventID != evidence.EventID ||
		manifest.LastEventID != evidence.EventID ||
		manifest.PreviousSegmentHash != nil ||
		manifest.ContentHash != "sha256:"+evidence.Hash {
		t.Fatalf("manifest = %#v", manifest)
	}
	if _, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt); err != nil {
		t.Fatalf("created_at = %q: %v", manifest.CreatedAt, err)
	}

	digest := sha256.Sum256(manifestBytes)
	wantHash := fmt.Sprintf("%x\n", digest[:])
	hashBytes, err := os.ReadFile(filepath.Join(segmentDir, filesystemManifestHashFilename))
	if err != nil {
		t.Fatal(err)
	}
	if string(hashBytes) != wantHash {
		t.Fatalf("manifest hash = %q, want %q", string(hashBytes), wantHash)
	}

	for _, filename := range []string{manifestPath, filepath.Join(segmentDir, filesystemManifestHashFilename)} {
		info, err := os.Stat(filename)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode = %04o, want 0600", filepath.Base(filename), info.Mode().Perm())
		}
	}
}

func TestFilesystemRawStoreRedeliveryDoesNotRewriteManifestArtifacts(t *testing.T) {
	root := t.TempDir()
	store := newFilesystemRawStoreForTest(t, root)
	evidence := filesystemRawStoreTestEvidence()

	if _, err := store.EnsureDurable(context.Background(), evidence); err != nil {
		t.Fatal(err)
	}

	segmentDir := filepath.Dir(filesystemRawStoreTestPath(root, evidence))
	marker := time.Unix(1_600_000_100, 0).UTC()
	paths := []string{
		filepath.Join(segmentDir, filesystemManifestFilename),
		filepath.Join(segmentDir, filesystemManifestHashFilename),
	}
	for _, filename := range paths {
		if err := os.Chtimes(filename, marker, marker); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := store.EnsureDurable(context.Background(), evidence); err != nil {
		t.Fatalf("redelivery EnsureDurable() error = %v", err)
	}

	for _, filename := range paths {
		info, err := os.Stat(filename)
		if err != nil {
			t.Fatal(err)
		}
		if !info.ModTime().Equal(marker) {
			t.Fatalf("redelivery rewrote %s: modtime = %s", filepath.Base(filename), info.ModTime())
		}
	}
}

func TestFilesystemRawStoreRejectsTamperedManifestArtifactsWithoutOverwrite(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		tamper   func([]byte) []byte
	}{
		{
			name:     "manifest",
			filename: filesystemManifestFilename,
			tamper: func(payload []byte) []byte {
				return []byte(`{"manifest_version":1,"segment_id":"01995000-0000-7000-8000-000000000002","tenant_id":"other","event_count":1,"first_event_id":"01995000-0000-7000-8000-000000000002","last_event_id":"01995000-0000-7000-8000-000000000002","created_at":"2026-09-13T18:30:00Z","previous_segment_hash":null,"content_hash":"sha256:20f2c91b72cba17589a59e1261c7670488866cb4187354078d90916507608694"}` + "\n")
			},
		},
		{
			name:     "manifest hash",
			filename: filesystemManifestHashFilename,
			tamper: func(payload []byte) []byte {
				copy := append([]byte(nil), payload...)
				copy[0] = 'f'
				if payload[0] == 'f' {
					copy[0] = '0'
				}
				return copy
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			store := newFilesystemRawStoreForTest(t, root)
			evidence := filesystemRawStoreTestEvidence()
			if _, err := store.EnsureDurable(context.Background(), evidence); err != nil {
				t.Fatal(err)
			}

			segmentDir := filepath.Dir(filesystemRawStoreTestPath(root, evidence))
			filename := filepath.Join(segmentDir, tt.filename)
			original, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			tampered := tt.tamper(original)
			if err := os.WriteFile(filename, tampered, 0o600); err != nil {
				t.Fatal(err)
			}

			_, err = store.EnsureDurable(context.Background(), evidence)
			if !errors.Is(err, ErrRawStoreConflict) {
				t.Fatalf("EnsureDurable() error = %v, want ErrRawStoreConflict", err)
			}
			got, readErr := os.ReadFile(filename)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if string(got) != string(tampered) {
				t.Fatalf("tampered %s was overwritten", tt.filename)
			}
		})
	}
}
