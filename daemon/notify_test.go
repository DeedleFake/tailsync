package daemon

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"deedles.dev/tailsync/internal/index"
)

func TestNotifyTrackerPendingAndMark(t *testing.T) {
	tr := newNotifyTracker()
	now := time.Now()
	h1 := index.Entry{Path: "a", Hash: "h1", UpdatedAt: now}
	h2 := index.Entry{Path: "a", Hash: "h1", UpdatedAt: now} // same content key
	h3 := index.Entry{Path: "a", Hash: "h2", UpdatedAt: now} // new version

	got := tr.pending([]index.ManifestEntry{h1})
	if len(got) != 1 {
		t.Fatalf("first pending: %d", len(got))
	}
	// pending does not mark
	got = tr.pending([]index.ManifestEntry{h2})
	if len(got) != 1 {
		t.Fatalf("unmarked still pending: %d", len(got))
	}
	tr.mark([]index.ManifestEntry{h1})
	got = tr.pending([]index.ManifestEntry{h2})
	if len(got) != 0 {
		t.Fatalf("after mark, duplicate filtered: %d", len(got))
	}
	got = tr.pending([]index.ManifestEntry{h3})
	if len(got) != 1 {
		t.Fatalf("new hash should notify: %d", len(got))
	}
}

func TestNotifyTrackerClaimPreventsDoubleFanout(t *testing.T) {
	tr := newNotifyTracker()
	now := time.Now()
	h := index.Entry{Path: "a", Hash: "h1", UpdatedAt: now}
	got := tr.claim([]index.ManifestEntry{h})
	if len(got) != 1 {
		t.Fatalf("first claim: %d", len(got))
	}
	got = tr.claim([]index.ManifestEntry{h})
	if len(got) != 0 {
		t.Fatalf("second claim while in-flight should be empty: %d", len(got))
	}
	if len(tr.pending([]index.ManifestEntry{h})) != 0 {
		t.Fatal("pending should hide in-flight keys")
	}
	tr.release([]index.ManifestEntry{h})
	got = tr.claim([]index.ManifestEntry{h})
	if len(got) != 1 {
		t.Fatalf("after release claim again: %d", len(got))
	}
	tr.mark([]index.ManifestEntry{h})
	got = tr.claim([]index.ManifestEntry{h})
	if len(got) != 0 {
		t.Fatalf("after mark claim should be empty: %d", len(got))
	}
}

func TestAlreadyHaveAll(t *testing.T) {
	d := testDaemon(t)
	now := time.Now()
	older := now.Add(-time.Hour)
	d.idx.Set(index.Entry{Path: "a", Hash: "x", UpdatedAt: older, Mode: 0o644, ModTime: older})

	// Same content, same meta, equal LWW → already have.
	if !d.alreadyHaveAll([]index.ManifestEntry{{Path: "a", Hash: "x", UpdatedAt: older, Mode: 0o644, ModTime: older}}) {
		t.Fatal("expected already have identical entry")
	}
	// Newer UpdatedAt same hash (meta-only style) → need pull.
	if d.alreadyHaveAll([]index.ManifestEntry{{Path: "a", Hash: "x", UpdatedAt: now, Mode: 0o644, ModTime: now}}) {
		t.Fatal("meta/LWW win should need pull")
	}
	if d.alreadyHaveAll([]index.ManifestEntry{{Path: "a", Hash: "y", UpdatedAt: now}}) {
		t.Fatal("hash mismatch should need pull")
	}
	if d.alreadyHaveAll([]index.ManifestEntry{{Path: "b", Hash: "x", UpdatedAt: now}}) {
		t.Fatal("missing path should need pull")
	}
}

func TestScheduleNotifyNoCandidatesDoesNotMark(t *testing.T) {
	d := testDaemon(t)
	d.nodeID = "local"
	d.cfg.NetMode = NetModePlain
	d.cfg.Peers = nil
	// No mesh → zero connected sessions.
	now := time.Now()
	hints := []index.ManifestEntry{{Path: "a", Hash: "h", UpdatedAt: now}}
	if d.scheduleNotify(t.Context(), hints) {
		t.Fatal("expected no fan-out with no mesh")
	}
	// Keys must remain pending so a later peer can be notified.
	if got := d.notifySeen.pending(hints); len(got) != 1 {
		t.Fatalf("keys should not be marked without successful notify, pending=%d", len(got))
	}
}

func TestRequestPullSafeAfterNilDaemon(t *testing.T) {
	var d *Daemon
	d.requestPull() // must not panic
	d = testDaemon(t)
	d.requestPull() // needPull from New; must not panic
}

// TestLateRequestPullAndOnNotifyAfterRun locks the lifecycle gate: after Run
// returns, needPull is still non-nil and late requestPull/onNotify must not panic.
func TestLateRequestPullAndOnNotifyAfterRun(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := pc.LocalAddr().(*net.UDPAddr).Port
	_ = pc.Close()

	ready := make(chan struct{})
	d, err := New(Config{
		Dir:          dir,
		StateDir:     state,
		Hostname:     "late-cb",
		Port:         port,
		NetMode:      NetModePlain,
		ListenHost:   "127.0.0.1",
		ScanInterval: time.Hour,
		SyncInterval: time.Hour,
		DisableWatch: true,
		OnReady:      func() { close(ready) },
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()

	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("Run: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ready timeout")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run after cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit")
	}

	// Late callbacks after shutdown (handleStream / infect-and-die race with cancel).
	d.requestPull()
	d.onNotify("peer-x", []index.ManifestEntry{
		{Path: "late.txt", Hash: "abc", UpdatedAt: time.Now()},
	})
	// needPull must still be the channel from New (never niled).
	if d.needPull == nil {
		t.Fatal("needPull was niled after Run")
	}
	_ = os.RemoveAll(dir)
}

func TestDiscoveryCandidatesPins(t *testing.T) {
	d := testDaemon(t)
	d.cfg.Peers = []string{"127.0.0.1:1", "127.0.0.1:2"}
	cands := d.discoveryCandidates(context.Background())
	if len(cands) != 2 {
		t.Fatalf("pins: %v", cands)
	}
}
