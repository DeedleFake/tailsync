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
		log:        slog.Default(),
		idx:        index.New(),
		root:       root,
		notifySeen: newNotifyTracker(),
		needPull:   newSignal(),
		pullSem:    make(chan struct{}, DefaultPullStreamConcurrency),
		serveSem:   make(chan struct{}, DefaultPullStreamConcurrency),
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

// TestCommitContentDoesNotClobberUnindexedFile: a local file not yet in the
// index (scan still pending) must not be overwritten by a winning remote pull.
func TestCommitContentDoesNotClobberUnindexedFile(t *testing.T) {
	d := testDaemon(t)
	path := "new-local.txt"
	localData := []byte("user-created-this-file-before-scan")
	if err := os.WriteFile(filepath.Join(d.cfg.Dir, path), localData, 0o644); err != nil {
		t.Fatal(err)
	}

	remoteData := []byte("peer-bytes")
	now := time.Now().UTC()
	re := index.Entry{
		Path: path, Hash: sha256Hex(remoteData), Size: int64(len(remoteData)),
		UpdatedAt: now, Mode: 0o644, ModTime: now,
	}
	d.syncMu.Lock()
	did, err := d.commitContent(re, remoteData, re.Hash)
	d.syncMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatal("commit should skip unsynced local create")
	}
	got, err := os.ReadFile(filepath.Join(d.cfg.Dir, path))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(localData) {
		t.Fatalf("local file overwritten: %q", got)
	}
	if _, ok := d.idx.Get(path); ok {
		t.Fatal("index should not record skipped commit")
	}
}

// TestCommitContentDoesNotClobberUnsyncedEdit: index has a live entry but the
// user edited the file (size drift) before scan. Remote must not overwrite.
func TestCommitContentDoesNotClobberUnsyncedEdit(t *testing.T) {
	d := testDaemon(t)
	path := "edited.txt"
	seed := []byte("seed")
	seedHash := sha256Hex(seed)
	older := time.Now().Add(-time.Hour).UTC()
	d.syncMu.Lock()
	if _, err := d.commitContent(index.Entry{
		Path: path, Hash: seedHash, Size: int64(len(seed)),
		UpdatedAt: older, Mode: 0o644, ModTime: older,
	}, seed, seedHash); err != nil {
		d.syncMu.Unlock()
		t.Fatal(err)
	}
	d.syncMu.Unlock()

	localData := []byte("much-longer-local-edit-so-size-drifts")
	if err := os.WriteFile(filepath.Join(d.cfg.Dir, path), localData, 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := d.root.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := d.idx.Get(path)
	if fi.Size() == e.Size && fi.ModTime().Equal(e.ModTime) {
		t.Fatal("test setup: edit did not drift from index")
	}

	remoteData := []byte("peer-newer")
	newer := time.Now().UTC()
	re := index.Entry{
		Path: path, Hash: sha256Hex(remoteData), Size: int64(len(remoteData)),
		UpdatedAt: newer, Mode: 0o644, ModTime: newer,
	}
	d.syncMu.Lock()
	did, err := d.commitContent(re, remoteData, re.Hash)
	d.syncMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatal("commit should skip unsynced local edit")
	}
	got, err := os.ReadFile(filepath.Join(d.cfg.Dir, path))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(localData) {
		t.Fatalf("local edit overwritten: %q", got)
	}
	e2, _ := d.idx.Get(path)
	if e2.Hash != seedHash {
		t.Fatalf("index hash moved to %s", e2.Hash)
	}
}

// TestDeleteLiveDoesNotRemoveUnsyncedEdit: a winning remote tombstone must not
// delete a local file that has drifted from the index.
func TestDeleteLiveDoesNotRemoveUnsyncedEdit(t *testing.T) {
	d := testDaemon(t)
	path := "keep-me.txt"
	seed := []byte("seed")
	seedHash := sha256Hex(seed)
	older := time.Now().Add(-time.Hour).UTC()
	d.syncMu.Lock()
	if _, err := d.commitContent(index.Entry{
		Path: path, Hash: seedHash, Size: int64(len(seed)),
		UpdatedAt: older, Mode: 0o644, ModTime: older,
	}, seed, seedHash); err != nil {
		d.syncMu.Unlock()
		t.Fatal(err)
	}
	d.syncMu.Unlock()

	localData := []byte("edited-after-index-snapshot")
	if err := os.WriteFile(filepath.Join(d.cfg.Dir, path), localData, 0o644); err != nil {
		t.Fatal(err)
	}

	newer := time.Now().UTC()
	tomb := index.Entry{Path: path, Deleted: true, UpdatedAt: newer, DeletedAt: newer}
	d.syncMu.Lock()
	did, err := d.execDeleteLive(tomb)
	d.syncMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatal("delete should skip unsynced local edit")
	}
	got, err := os.ReadFile(filepath.Join(d.cfg.Dir, path))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(localData) {
		t.Fatalf("file removed or changed: %q", got)
	}
	e, ok := d.idx.Get(path)
	if !ok || e.Deleted {
		t.Fatalf("index should stay live, got ok=%v %+v", ok, e)
	}
}

func TestDiskUnsynced(t *testing.T) {
	d := testDaemon(t)
	path := "x.txt"
	unsynced, err := d.diskUnsynced(path, index.Entry{}, false)
	if err != nil || unsynced {
		t.Fatalf("missing path: unsynced=%v err=%v", unsynced, err)
	}
	if err := os.WriteFile(filepath.Join(d.cfg.Dir, path), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	unsynced, err = d.diskUnsynced(path, index.Entry{}, false)
	if err != nil || !unsynced {
		t.Fatalf("unindexed file: unsynced=%v err=%v", unsynced, err)
	}
	fi, err := d.root.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	cur := index.Entry{Path: path, Size: fi.Size(), ModTime: fi.ModTime()}
	unsynced, err = d.diskUnsynced(path, cur, true)
	if err != nil || unsynced {
		t.Fatalf("matching live: unsynced=%v err=%v", unsynced, err)
	}
	if err := os.WriteFile(filepath.Join(d.cfg.Dir, path), []byte("abcd"), 0o644); err != nil {
		t.Fatal(err)
	}
	unsynced, err = d.diskUnsynced(path, cur, true)
	if err != nil || !unsynced {
		t.Fatalf("size drift: unsynced=%v err=%v", unsynced, err)
	}
}
