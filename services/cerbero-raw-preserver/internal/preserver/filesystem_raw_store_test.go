package preserver

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

const filesystemRawStoreTestEventID = "01995000-0000-7000-8000-000000000002"

func TestFilesystemRawStorePersistsExactBytesWithDeterministicDevelopmentLocator(t *testing.T) {
	root := t.TempDir()
	store := newFilesystemRawStoreForTest(t, root)
	evidence := filesystemRawStoreTestEvidence()

	object, err := store.EnsureDurable(context.Background(), evidence)
	if err != nil {
		t.Fatalf("EnsureDurable() error = %v", err)
	}

	wantURI := "raw:///tenant-a/2026/09/13/18/" + filesystemRawStoreTestEventID + "/raw.bin"
	wantObject := RawObject{
		StorageURI: wantURI,
		SegmentID:  filesystemRawStoreTestEventID,
		Offset:     0,
		Length:     evidence.Size,
		Hash:       evidence.Hash,
	}
	if !reflect.DeepEqual(object, wantObject) {
		t.Fatalf("EnsureDurable() object = %#v, want %#v", object, wantObject)
	}

	filename := filesystemRawStoreTestPath(root, evidence)
	got, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !reflect.DeepEqual(got, evidence.Bytes) {
		t.Fatalf("durable bytes = %v, want %v", got, evidence.Bytes)
	}

	info, err := os.Stat(filename)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("raw object mode = %04o, want 0600", info.Mode().Perm())
	}
}

func TestFilesystemRawStoreRedeliveryReusesVerifiedObjectWithoutRewrite(t *testing.T) {
	root := t.TempDir()
	store := newFilesystemRawStoreForTest(t, root)
	evidence := filesystemRawStoreTestEvidence()

	first, err := store.EnsureDurable(context.Background(), evidence)
	if err != nil {
		t.Fatalf("first EnsureDurable() error = %v", err)
	}

	filename := filesystemRawStoreTestPath(root, evidence)
	markerTime := time.Unix(1_600_000_000, 0).UTC()
	if err := os.Chtimes(filename, markerTime, markerTime); err != nil {
		t.Fatal(err)
	}

	second, err := store.EnsureDurable(context.Background(), evidence)
	if err != nil {
		t.Fatalf("second EnsureDurable() error = %v", err)
	}
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("redelivery object = %#v, want %#v", second, first)
	}

	info, err := os.Stat(filename)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(markerTime) {
		t.Fatalf("redelivery rewrote raw object: modtime = %s, want %s", info.ModTime(), markerTime)
	}
}

func TestFilesystemRawStoreRejectsInvalidEvidenceBeforeCreatingTenantPath(t *testing.T) {
	base := filesystemRawStoreTestEvidence()
	tests := []struct {
		name   string
		mutate func(*RawEvidence)
	}{
		{name: "unsafe tenant", mutate: func(e *RawEvidence) { e.TenantID = "../escape" }},
		{name: "invalid event id", mutate: func(e *RawEvidence) { e.EventID = "not-a-uuid" }},
		{name: "missing ingest time", mutate: func(e *RawEvidence) { e.IngestTime = time.Time{} }},
		{name: "wrong hash algorithm", mutate: func(e *RawEvidence) { e.HashAlgorithm = "sha512" }},
		{name: "wrong size", mutate: func(e *RawEvidence) { e.Size++ }},
		{name: "wrong hash", mutate: func(e *RawEvidence) { e.Hash = fmt.Sprintf("%064x", 1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			store := newFilesystemRawStoreForTest(t, root)
			evidence := base
			evidence.Bytes = append([]byte(nil), base.Bytes...)
			tt.mutate(&evidence)

			if _, err := store.EnsureDurable(context.Background(), evidence); err == nil {
				t.Fatal("EnsureDurable() error = nil")
			}

			entries, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("invalid evidence created %d root entries", len(entries))
			}
		})
	}
}

func TestFilesystemRawStoreRejectsExistingConflictingObjectWithoutOverwrite(t *testing.T) {
	root := t.TempDir()
	store := newFilesystemRawStoreForTest(t, root)
	evidence := filesystemRawStoreTestEvidence()

	if _, err := store.EnsureDurable(context.Background(), evidence); err != nil {
		t.Fatalf("initial EnsureDurable() error = %v", err)
	}

	filename := filesystemRawStoreTestPath(root, evidence)
	conflicting := append([]byte(nil), evidence.Bytes...)
	conflicting[0] ^= 0xff
	if err := os.WriteFile(filename, conflicting, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := store.EnsureDurable(context.Background(), evidence)
	if !errors.Is(err, ErrRawStoreConflict) {
		t.Fatalf("EnsureDurable() error = %v, want raw store conflict", err)
	}

	got, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, conflicting) {
		t.Fatal("conflicting durable object was overwritten")
	}
}

func TestFilesystemRawStoreConcurrentEnsureDurableConvergesOnSingleObject(t *testing.T) {
	root := t.TempDir()
	store := newFilesystemRawStoreForTest(t, root)
	evidence := filesystemRawStoreTestEvidence()

	const workers = 8
	results := make([]RawObject, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(index int) {
			defer wg.Done()
			<-start
			results[index], errs[index] = store.EnsureDurable(context.Background(), evidence)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d EnsureDurable() error = %v", i, err)
		}
		if !reflect.DeepEqual(results[i], results[0]) {
			t.Fatalf("worker %d object = %#v, want %#v", i, results[i], results[0])
		}
	}

	segmentDir := filepath.Dir(filesystemRawStoreTestPath(root, evidence))
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		t.Fatal(err)
	}
	wantEntries := []string{filesystemManifestFilename, filesystemManifestHashFilename, filesystemRawDataFilename}
	if got := entryNames(entries); !reflect.DeepEqual(got, wantEntries) {
		t.Fatalf("segment entries = %#v, want %#v", got, wantEntries)
	}
}

func TestFilesystemRawStoreCanceledContextLeavesNoEvidenceObject(t *testing.T) {
	root := t.TempDir()
	store := newFilesystemRawStoreForTest(t, root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.EnsureDurable(ctx, filesystemRawStoreTestEvidence())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureDurable() error = %v, want context canceled", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("canceled write created %d root entries", len(entries))
	}
}

func TestNewFilesystemRawStoreRequiresExistingRealDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := NewFilesystemRawStore(missing); err == nil {
		t.Fatal("NewFilesystemRawStore(missing) error = nil")
	}

	root := t.TempDir()
	file := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFilesystemRawStore(file); err == nil {
		t.Fatal("NewFilesystemRawStore(file) error = nil")
	}
}

func newFilesystemRawStoreForTest(t *testing.T, root string) *FilesystemRawStore {
	t.Helper()
	store, err := NewFilesystemRawStore(root)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func filesystemRawStoreTestEvidence() RawEvidence {
	raw := []byte{'{', '"', 'x', '"', ':', '1', '}', '\r', '\n', 0x00, 0xff}
	sum := sha256.Sum256(raw)
	return RawEvidence{
		TenantID:      "tenant-a",
		EventID:       filesystemRawStoreTestEventID,
		IngestTime:    time.Date(2026, 9, 13, 14, 30, 0, 0, time.FixedZone("-04", -4*60*60)),
		HashAlgorithm: rawHashAlgorithmSHA256,
		Hash:          fmt.Sprintf("%x", sum[:]),
		Size:          uint64(len(raw)),
		Bytes:         append([]byte(nil), raw...),
	}
}

func filesystemRawStoreTestPath(root string, evidence RawEvidence) string {
	when := evidence.IngestTime.UTC()
	return filepath.Join(
		root,
		evidence.TenantID,
		when.Format("2006"),
		when.Format("01"),
		when.Format("02"),
		when.Format("15"),
		evidence.EventID,
		filesystemRawDataFilename,
	)
}

func entryNames(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
