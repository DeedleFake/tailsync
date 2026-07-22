package scan_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/scan"
)

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestScanDetectsAddModifyDelete(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "keep.txt", "keep")
	writeFile(t, root, "gone.txt", "gone")
	writeFile(t, root, "change.txt", "v1")

	fixed := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	opts := &scan.Options{Now: func() time.Time { return fixed }}

	idx := index.New()
	// Empty index: all files are adds.
	res, err := scan.Scan(context.Background(), root, idx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Changes) != 3 {
		t.Fatalf("want 3 adds, got %d: %+v", len(res.Changes), res.Changes)
	}
	for _, c := range res.Changes {
		if c.Kind != scan.Added {
			t.Fatalf("want Added, got %v for %s", c.Kind, c.Path)
		}
	}
	scan.Apply(idx, res)

	// Modify change.txt and delete gone.txt on disk.
	writeFile(t, root, "change.txt", "v2")
	if err := os.Remove(filepath.Join(root, "gone.txt")); err != nil {
		t.Fatal(err)
	}
	// Add new file.
	writeFile(t, root, "new.txt", "new")

	later := fixed.Add(time.Hour)
	opts.Now = func() time.Time { return later }
	res2, err := scan.Scan(context.Background(), root, idx, opts)
	if err != nil {
		t.Fatal(err)
	}

	kinds := map[string]scan.ChangeKind{}
	for _, c := range res2.Changes {
		kinds[c.Path] = c.Kind
	}
	if kinds["change.txt"] != scan.Modified {
		t.Fatalf("change.txt: %v", kinds["change.txt"])
	}
	if kinds["gone.txt"] != scan.Deleted {
		t.Fatalf("gone.txt: %v (offline deletion)", kinds["gone.txt"])
	}
	if kinds["new.txt"] != scan.Added {
		t.Fatalf("new.txt: %v", kinds["new.txt"])
	}
	if _, ok := kinds["keep.txt"]; ok {
		t.Fatal("keep.txt should be unchanged")
	}

	scan.Apply(idx, res2)
	e, ok := idx.Get("gone.txt")
	if !ok || !e.Deleted {
		t.Fatalf("tombstone missing: %+v", e)
	}
	if !e.DeletedAt.Equal(later) {
		t.Fatalf("deleted_at %v", e.DeletedAt)
	}
}

func TestOfflineDeletionOnlyInIndex(t *testing.T) {
	// Simulate: index has file, disk never had it this run (deleted while offline).
	root := t.TempDir()
	idx := index.New()
	idx.Set(index.Entry{
		Path:      "offline-deleted.txt",
		Size:      10,
		Hash:      "deadbeef",
		ModTime:   time.Now().Add(-time.Hour),
		UpdatedAt: time.Now().Add(-time.Hour),
	})

	res, err := scan.Scan(context.Background(), root, idx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Changes) != 1 {
		t.Fatalf("changes: %+v", res.Changes)
	}
	c := res.Changes[0]
	if c.Kind != scan.Deleted || c.Path != "offline-deleted.txt" {
		t.Fatalf("got %+v", c)
	}
	if !c.Entry.Deleted {
		t.Fatal("entry should be tombstone")
	}
}

func TestSkipsTailsyncStateDir(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "ok.txt", "ok")
	writeFile(t, root, ".tailsync/index.json", `{}`)
	writeFile(t, root, ".tailsync-tmp/x", "x")

	idx := index.New()
	res, err := scan.Scan(context.Background(), root, idx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Changes) != 1 || res.Changes[0].Path != "ok.txt" {
		t.Fatalf("changes: %+v", res.Changes)
	}
}

func TestResurrectionFromTombstone(t *testing.T) {
	root := t.TempDir()
	idx := index.New()
	idx.MarkDeleted("back.txt", time.Now().Add(-time.Hour))
	writeFile(t, root, "back.txt", "alive")

	res, err := scan.Scan(context.Background(), root, idx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Changes) != 1 || res.Changes[0].Kind != scan.Added {
		t.Fatalf("want Added resurrection, got %+v", res.Changes)
	}
}

func TestHashFile(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "f")
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	h, err := scan.HashFile(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	// sha256("hello")
	want := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if h != want {
		t.Fatalf("got %s want %s", h, want)
	}
}

func TestModeOnlyChange(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "m.txt", "same")
	idx := index.New()
	res, err := scan.Scan(context.Background(), root, idx, nil)
	if err != nil {
		t.Fatal(err)
	}
	scan.Apply(idx, res)
	p := filepath.Join(root, "m.txt")
	if err := os.Chmod(p, 0o600); err != nil {
		t.Fatal(err)
	}
	// Preserve mtime so hash fast-path still applies; mode still differs.
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := idx.Get("m.txt")
	_ = os.Chtimes(p, e.ModTime, e.ModTime)
	_ = fi

	res2, err := scan.Scan(context.Background(), root, idx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Changes) != 1 || res2.Changes[0].Kind != scan.Modified {
		t.Fatalf("want mode Modified, got %+v", res2.Changes)
	}
	if res2.Changes[0].Entry.Mode != 0o600 {
		t.Fatalf("mode=%o", res2.Changes[0].Entry.Mode)
	}
}

func TestApplyDoesNotClobberNewer(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "f.txt", "v1")
	idx := index.New()
	res, err := scan.Scan(context.Background(), root, idx, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Peer applied a newer entry before Apply.
	newer := res.Changes[0].Entry
	newer.Hash = "peer-newer"
	newer.UpdatedAt = newer.UpdatedAt.Add(time.Hour)
	idx.Set(newer)

	n := scan.Apply(idx, res)
	if n != 0 {
		t.Fatalf("stale scan should not apply, n=%d", n)
	}
	e, _ := idx.Get("f.txt")
	if e.Hash != "peer-newer" {
		t.Fatalf("got hash %s", e.Hash)
	}
}
