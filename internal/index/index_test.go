package index_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"deedles.dev/tailsync/internal/index"
)

func TestLoadSaveRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.json")

	idx := index.New()
	idx.SetPath(path)
	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	idx.Set(index.Entry{
		Path:      "foo/bar.txt",
		Size:      42,
		ModTime:   now,
		Mode:      0o644,
		Hash:      "abc123",
		UpdatedAt: now,
	})
	if err := idx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := index.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	e, ok := loaded.Get("foo/bar.txt")
	if !ok {
		t.Fatal("missing entry")
	}
	if e.Size != 42 || e.Hash != "abc123" || e.Deleted {
		t.Fatalf("unexpected entry: %+v", e)
	}
	if !e.ModTime.Equal(now) {
		t.Fatalf("modtime: got %v want %v", e.ModTime, now)
	}
}

func TestLoadMissingCreatesEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	idx, err := index.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if idx.Len() != 0 {
		t.Fatalf("len=%d", idx.Len())
	}
	if idx.Path() != path {
		t.Fatalf("path=%q", idx.Path())
	}
}

func TestMarkDeleted(t *testing.T) {
	idx := index.New()
	idx.Set(index.Entry{Path: "a", Size: 1, Hash: "h", UpdatedAt: time.Now()})
	at := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	tomb := idx.MarkDeleted("a", at)
	if !tomb.Deleted || tomb.Hash != "" || tomb.Size != 0 {
		t.Fatalf("tombstone: %+v", tomb)
	}
	if !tomb.DeletedAt.Equal(at) || !tomb.UpdatedAt.Equal(at) {
		t.Fatalf("times: %+v", tomb)
	}
	e, ok := idx.Get("a")
	if !ok || !e.Deleted {
		t.Fatal("expected tombstone in index")
	}
	live := idx.Live()
	if _, ok := live["a"]; ok {
		t.Fatal("live should omit deleted")
	}
}

func TestWinsLWW(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	local := index.Entry{Path: "f", Hash: "a", UpdatedAt: t0}
	remote := index.Entry{Path: "f", Hash: "b", UpdatedAt: t1}
	if !index.Wins(local, remote) {
		t.Fatal("remote newer should win")
	}
	if index.Wins(remote, local) {
		t.Fatal("local older should not win as remote")
	}
	// Same timestamp: higher hash wins (stable total order; both peers converge).
	hi := index.Entry{Path: "f", Hash: "c", UpdatedAt: t0}
	lo := index.Entry{Path: "f", Hash: "a", UpdatedAt: t0}
	if !index.Wins(lo, hi) {
		t.Fatal("higher hash should win at equal time")
	}
	if index.Wins(hi, lo) {
		t.Fatal("lower hash must not win at equal time")
	}
	// Identical entries: no win.
	if index.Wins(lo, lo) {
		t.Fatal("identical should not win")
	}
}

func TestWinsDeleteTie(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	live := index.Entry{Path: "f", Hash: "abc", UpdatedAt: t0, Deleted: false}
	tomb := index.Entry{Path: "f", UpdatedAt: t0, Deleted: true, DeletedAt: t0}
	// At equal clock, tombstone sorts above live (prefer delete on tie).
	if !index.Wins(live, tomb) {
		t.Fatal("tombstone should win over live at equal time")
	}
	if index.Wins(tomb, live) {
		t.Fatal("live must not win over tombstone at equal time")
	}
	// Newer live resurrects.
	newerLive := index.Entry{Path: "f", Hash: "abc", UpdatedAt: t0.Add(time.Hour), Deleted: false}
	if !index.Wins(tomb, newerLive) {
		t.Fatal("newer live should win over older tombstone")
	}
}

func TestWinsMtimeTie(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mt0 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	mt1 := mt0.Add(time.Hour)
	// Same UpdatedAt, hash, mode — only ModTime differs: later mtime wins.
	older := index.Entry{Path: "f", Hash: "abc", Mode: 0o644, ModTime: mt0, UpdatedAt: t0}
	newer := index.Entry{Path: "f", Hash: "abc", Mode: 0o644, ModTime: mt1, UpdatedAt: t0}
	if !index.Wins(older, newer) {
		t.Fatal("later ModTime should win at equal UpdatedAt")
	}
	if index.Wins(newer, older) {
		t.Fatal("earlier ModTime must not win at equal UpdatedAt")
	}
	// Identical including ModTime: no win.
	if index.Wins(newer, newer) {
		t.Fatal("identical entries including ModTime should not win")
	}
	// Mtime-only difference still counts as non-identical for Wins equality.
	sameClockDiffMT := index.Entry{Path: "f", Hash: "abc", Mode: 0o644, ModTime: mt0, UpdatedAt: t0}
	other := index.Entry{Path: "f", Hash: "abc", Mode: 0o644, ModTime: mt1, UpdatedAt: t0}
	if sameClockDiffMT.ModTime.Equal(other.ModTime) {
		t.Fatal("test setup")
	}
	// Higher UpdatedAt still beats mtime order.
	staleMtime := index.Entry{Path: "f", Hash: "abc", Mode: 0o644, ModTime: mt1, UpdatedAt: t0}
	freshClock := index.Entry{Path: "f", Hash: "abc", Mode: 0o644, ModTime: mt0, UpdatedAt: t0.Add(time.Minute)}
	if !index.Wins(staleMtime, freshClock) {
		t.Fatal("higher UpdatedAt should win even with older ModTime")
	}
	// Mode is ordered before ModTime at equal UpdatedAt/hash.
	// Higher mode + earlier mtime beats lower mode + later mtime.
	hiMode := index.Entry{Path: "f", Hash: "abc", Mode: 0o755, ModTime: mt0, UpdatedAt: t0}
	loModeLaterMT := index.Entry{Path: "f", Hash: "abc", Mode: 0o644, ModTime: mt1, UpdatedAt: t0}
	if !index.Wins(loModeLaterMT, hiMode) {
		t.Fatal("higher Mode should win before ModTime is considered")
	}
	if index.Wins(hiMode, loModeLaterMT) {
		t.Fatal("lower Mode must not win even with later ModTime")
	}
}

func TestSetIfWins(t *testing.T) {
	idx := index.New()
	t0 := time.Now()
	idx.Set(index.Entry{Path: "f", Hash: "a", UpdatedAt: t0})
	if idx.SetIfWins(index.Entry{Path: "f", Hash: "b", UpdatedAt: t0.Add(-time.Hour)}) {
		t.Fatal("older should not win")
	}
	if !idx.SetIfWins(index.Entry{Path: "f", Hash: "b", UpdatedAt: t0.Add(time.Hour)}) {
		t.Fatal("newer should win")
	}
	e, _ := idx.Get("f")
	if e.Hash != "b" {
		t.Fatalf("hash=%s", e.Hash)
	}
}

func TestGCTombstones(t *testing.T) {
	idx := index.New()
	now := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	idx.Set(index.Entry{Path: "old", Deleted: true, DeletedAt: now.Add(-40 * 24 * time.Hour), UpdatedAt: now.Add(-40 * 24 * time.Hour)})
	idx.Set(index.Entry{Path: "new", Deleted: true, DeletedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)})
	idx.Set(index.Entry{Path: "live", Hash: "x", UpdatedAt: now})
	n := idx.GCTombstones(now, 30*24*time.Hour)
	if n != 1 {
		t.Fatalf("removed %d", n)
	}
	if _, ok := idx.Get("old"); ok {
		t.Fatal("old tombstone should be gone")
	}
	if _, ok := idx.Get("new"); !ok {
		t.Fatal("new tombstone should remain")
	}
	if _, ok := idx.Get("live"); !ok {
		t.Fatal("live should remain")
	}
}

func TestSaveAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.json")
	idx := index.New()
	idx.SetPath(path)
	idx.Set(index.Entry{Path: "x", Hash: "1", UpdatedAt: time.Now()})
	if err := idx.Save(); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if e.Name() != "index.json" {
			t.Fatalf("unexpected file %q", e.Name())
		}
	}
}

func TestManifest(t *testing.T) {
	idx := index.New()
	idx.Set(index.Entry{Path: "a", Hash: "1", UpdatedAt: time.Now()})
	idx.MarkDeleted("b", time.Now())
	m := idx.Manifest()
	if len(m) != 2 {
		t.Fatalf("manifest len %d", len(m))
	}
}
