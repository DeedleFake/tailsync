package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"deedles.dev/tailsync/internal/index"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// testDaemon builds a minimal Daemon with root+index for unit tests (no listen).
func testDaemon(t *testing.T) *Daemon {
	t.Helper()
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return &Daemon{
		cfg: Config{
			Dir:          dir,
			MaxFileBytes: DefaultMaxFileBytes,
			BlockSize:    4096,
		},
		log:  slog.Default(),
		idx:  index.New(),
		root: root,
	}
}

// simulateUnlockedContentApply mirrors applyRemote's unlock-during-pull pattern
// for content without network: decide under lock → sleep → re-lock → commitContent.
func simulateUnlockedContentApply(d *Daemon, re index.Entry, data []byte, delay time.Duration) (bool, error) {
	d.syncMu.Lock()
	if cur, ok := d.idx.Get(re.Path); ok && !index.Wins(cur, re) {
		d.syncMu.Unlock()
		return false, nil
	}
	d.syncMu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	got := sha256Hex(data)
	if got != re.Hash {
		return false, peerLogical("hash mismatch in test harness")
	}

	d.syncMu.Lock()
	defer d.syncMu.Unlock()
	return d.commitContent(re, data, got)
}

func TestDecideApply(t *testing.T) {
	now := time.Now()
	older := now.Add(-time.Hour)
	newer := now.Add(time.Hour)

	t.Run("tombstone absent", func(t *testing.T) {
		re := index.Entry{Path: "a", Deleted: true, UpdatedAt: now}
		d := decideApply(index.Entry{}, false, re, false)
		if d.kind != applyTombstone {
			t.Fatalf("kind %v want tombstone", d.kind)
		}
	})

	t.Run("tombstone on local tombstone", func(t *testing.T) {
		local := index.Entry{Path: "a", Deleted: true, UpdatedAt: older}
		re := index.Entry{Path: "a", Deleted: true, UpdatedAt: newer}
		d := decideApply(local, true, re, false)
		if d.kind != applyTombstone {
			t.Fatalf("kind %v want tombstone", d.kind)
		}
	})

	t.Run("delete live when remote wins", func(t *testing.T) {
		local := index.Entry{Path: "a", Hash: "x", UpdatedAt: older}
		re := index.Entry{Path: "a", Deleted: true, UpdatedAt: newer}
		d := decideApply(local, true, re, true)
		if d.kind != applyDeleteLive {
			t.Fatalf("kind %v want deleteLive", d.kind)
		}
	})

	t.Run("noop delete when local wins", func(t *testing.T) {
		local := index.Entry{Path: "a", Hash: "x", UpdatedAt: newer}
		re := index.Entry{Path: "a", Deleted: true, UpdatedAt: older}
		d := decideApply(local, true, re, true)
		if d.kind != applyNoop {
			t.Fatalf("kind %v want noop", d.kind)
		}
	})

	t.Run("same hash meta only", func(t *testing.T) {
		local := index.Entry{Path: "a", Hash: "h", Mode: 0o644, UpdatedAt: older, ModTime: older}
		re := index.Entry{Path: "a", Hash: "h", Mode: 0o600, UpdatedAt: newer, ModTime: newer}
		d := decideApply(local, true, re, true)
		if d.kind != applyMetaOnly {
			t.Fatalf("kind %v want metaOnly", d.kind)
		}
	})

	t.Run("same hash missing disk pulls content", func(t *testing.T) {
		local := index.Entry{Path: "a", Hash: "h", Mode: 0o644, UpdatedAt: older}
		re := index.Entry{Path: "a", Hash: "h", Mode: 0o600, UpdatedAt: newer}
		d := decideApply(local, true, re, false)
		if d.kind != applyContent || d.useDelta {
			t.Fatalf("kind %v useDelta %v want content full", d.kind, d.useDelta)
		}
	})

	t.Run("content with delta", func(t *testing.T) {
		local := index.Entry{Path: "a", Hash: "old", UpdatedAt: older}
		re := index.Entry{Path: "a", Hash: "new", UpdatedAt: newer}
		d := decideApply(local, true, re, true)
		if d.kind != applyContent || !d.useDelta {
			t.Fatalf("kind %v useDelta %v want content delta", d.kind, d.useDelta)
		}
	})

	t.Run("content full when no local", func(t *testing.T) {
		re := index.Entry{Path: "a", Hash: "new", UpdatedAt: newer}
		d := decideApply(index.Entry{}, false, re, false)
		if d.kind != applyContent || d.useDelta {
			t.Fatalf("kind %v useDelta %v want content full", d.kind, d.useDelta)
		}
	})

	t.Run("noop when local wins content", func(t *testing.T) {
		local := index.Entry{Path: "a", Hash: "local", UpdatedAt: newer}
		re := index.Entry{Path: "a", Hash: "remote", UpdatedAt: older}
		d := decideApply(local, true, re, true)
		if d.kind != applyNoop {
			t.Fatalf("kind %v want noop", d.kind)
		}
	})
}

func TestFileMode(t *testing.T) {
	if fileMode(0) != 0o644 {
		t.Fatalf("zero → 0o644")
	}
	if fileMode(0o755) != 0o755 {
		t.Fatalf("preserve non-zero")
	}
}

// TestConcurrentSamePathCommitLWW races two unlock-style content applies for one
// path: loser finishes last but must not overwrite the LWW winner on disk/index.
func TestConcurrentSamePathCommitLWW(t *testing.T) {
	d := testDaemon(t)
	path := "conflict.txt"
	older := time.Now().Add(-time.Hour).UTC()
	newer := time.Now().UTC()

	winnerData := []byte("winner-content")
	loserData := []byte("loser-content!!")
	winHash := sha256Hex(winnerData)
	loseHash := sha256Hex(loserData)

	winner := index.Entry{
		Path: path, Hash: winHash, Size: int64(len(winnerData)),
		UpdatedAt: newer, Mode: 0o644, ModTime: newer,
	}
	loser := index.Entry{
		Path: path, Hash: loseHash, Size: int64(len(loserData)),
		UpdatedAt: older, Mode: 0o644, ModTime: older,
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		// Loser "pulls" longer so it re-locks after the winner commits.
		_, _ = simulateUnlockedContentApply(d, loser, loserData, 40*time.Millisecond)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, _ = simulateUnlockedContentApply(d, winner, winnerData, 0)
	}()
	close(start)
	wg.Wait()

	e, ok := d.idx.Get(path)
	if !ok || e.Deleted || e.Hash != winHash {
		t.Fatalf("index entry %+v want hash %s", e, winHash)
	}
	data, err := d.root.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(winnerData) {
		t.Fatalf("disk %q want %q", data, winnerData)
	}
}

// TestConcurrentContentVsTombstone: winning tombstone vs late content pull.
// Late content must not resurrect the file when the tombstone still wins LWW.
func TestConcurrentContentVsTombstone(t *testing.T) {
	d := testDaemon(t)
	path := "gone.txt"
	older := time.Now().Add(-time.Hour).UTC()
	newer := time.Now().UTC()

	// Seed a live file that a late content apply might try to replace.
	seedData := []byte("seed")
	seedHash := sha256Hex(seedData)
	d.syncMu.Lock()
	if _, err := d.commitContent(index.Entry{
		Path: path, Hash: seedHash, Size: int64(len(seedData)),
		UpdatedAt: older, Mode: 0o644, ModTime: older,
	}, seedData, seedHash); err != nil {
		d.syncMu.Unlock()
		t.Fatal(err)
	}
	d.syncMu.Unlock()

	tomb := index.Entry{
		Path: path, Deleted: true, UpdatedAt: newer, DeletedAt: newer,
	}
	lateData := []byte("should-not-land")
	lateHash := sha256Hex(lateData)
	late := index.Entry{
		Path: path, Hash: lateHash, Size: int64(len(lateData)),
		UpdatedAt: older.Add(time.Minute), Mode: 0o644, // still loses to tomb
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		// Content pull holds no lock during delay; tombstone commits first.
		_, _ = simulateUnlockedContentApply(d, late, lateData, 40*time.Millisecond)
	}()
	go func() {
		defer wg.Done()
		<-start
		d.syncMu.Lock()
		defer d.syncMu.Unlock()
		_, _ = d.execDeleteLive(tomb)
	}()
	close(start)
	wg.Wait()

	e, ok := d.idx.Get(path)
	if !ok || !e.Deleted {
		t.Fatalf("want tombstone, got ok=%v entry=%+v", ok, e)
	}
	if _, err := d.root.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file should be gone, stat err=%v", err)
	}
}

// TestPostPullLWWDropLeavesDisk: after a winner is on disk, a late loser's
// commitContent is a no-op (no write) and disk bytes stay the winner.
func TestPostPullLWWDropLeavesDisk(t *testing.T) {
	d := testDaemon(t)
	path := "stable.txt"
	older := time.Now().Add(-time.Hour).UTC()
	newer := time.Now().UTC()

	winnerData := []byte("stable-winner")
	winHash := sha256Hex(winnerData)
	winner := index.Entry{
		Path: path, Hash: winHash, Size: int64(len(winnerData)),
		UpdatedAt: newer, Mode: 0o644, ModTime: newer,
	}
	d.syncMu.Lock()
	if _, err := d.commitContent(winner, winnerData, winHash); err != nil {
		d.syncMu.Unlock()
		t.Fatal(err)
	}
	d.syncMu.Unlock()

	loserData := []byte("overwrite-me?")
	loseHash := sha256Hex(loserData)
	loser := index.Entry{
		Path: path, Hash: loseHash, Size: int64(len(loserData)),
		UpdatedAt: older, Mode: 0o644, ModTime: older,
	}
	did, err := simulateUnlockedContentApply(d, loser, loserData, 0)
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatal("loser should not commit")
	}
	data, err := os.ReadFile(filepath.Join(d.cfg.Dir, path))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(winnerData) {
		t.Fatalf("disk changed: %q", data)
	}
}
