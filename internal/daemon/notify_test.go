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

func TestPeerMemSoftFailNeverDeletes(t *testing.T) {
	p := newPeerMem()
	p.remember("n1", "127.0.0.1:1")
	if ids := p.snapshotIDs(); len(ids) != 1 {
		t.Fatalf("ids %v", ids)
	}
	p.softFailAddr("127.0.0.1:1")
	// Still present; in addr backoff so hotAddrs empty.
	if ids := p.snapshotIDs(); len(ids) != 1 || ids[0] != "n1" {
		t.Fatalf("soft-fail must not delete: %v", ids)
	}
	if len(p.hotAddrs()) != 0 {
		t.Fatal("expected hot addr hidden by backoff")
	}
	// remember clears backoff.
	p.remember("n1", "127.0.0.1:1")
	addrs := p.hotAddrs()
	if len(addrs) != 1 || addrs[0] != "127.0.0.1:1" {
		t.Fatalf("after remember: %v", addrs)
	}
}

func TestPeerMemSoftFailAddrAcrossSources(t *testing.T) {
	p := newPeerMem()
	// Soft-fail without prior hot entry (status-only peer).
	p.softFailAddr("127.0.0.1:9")
	if !p.inBackoff("127.0.0.1:9") {
		t.Fatal("expected addr backoff without hot set")
	}
	p.remember("n1", "127.0.0.1:9")
	if p.inBackoff("127.0.0.1:9") {
		t.Fatal("remember must clear addr backoff")
	}
	p.softFailAddr("127.0.0.1:9")
	if len(p.hotAddrs()) != 0 {
		t.Fatal("expected backoff to hide hot addr")
	}
	if len(p.snapshotIDs()) != 1 {
		t.Fatal("must still remember node")
	}
}

func TestScheduleNotifyNoCandidatesDoesNotMark(t *testing.T) {
	d := testDaemon(t)
	d.nodeID = "local"
	d.cfg.NetMode = NetModePlain
	d.cfg.Peers = nil
	// No peers, no hot set, plain status empty → zero candidates.
	now := time.Now()
	hints := []index.ManifestEntry{{Path: "a", Hash: "h", UpdatedAt: now}}
	addrs := d.candidateAddrs(t.Context())
	if len(addrs) != 0 {
		t.Fatalf("expected empty candidates, got %v", addrs)
	}
	if d.scheduleNotify(t.Context(), hints) {
		t.Fatal("expected no fan-out with zero candidates")
	}
	// Keys must remain pending so a later peer can be notified.
	if got := d.notifySeen.pending(hints); len(got) != 1 {
		t.Fatalf("keys should not be marked without successful notify, pending=%d", len(got))
	}
}

func TestDialBackAddrUsesAdvertisedPort(t *testing.T) {
	got := dialBackAddr("127.0.0.1:54321", 19001, 5960)
	if got != "127.0.0.1:19001" {
		t.Fatalf("got %q", got)
	}
	// Missing advertised port falls back to local default (same-port mesh).
	got = dialBackAddr("100.64.0.2:9999", 0, 5960)
	if got != "100.64.0.2:5960" {
		t.Fatalf("got %q", got)
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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

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

	// Late callbacks after shutdown (handleConn / infect-and-die race with cancel).
	d.requestPull()
	d.onNotify("peer-x", "127.0.0.1:9", 9, []index.ManifestEntry{
		{Path: "late.txt", Hash: "abc", UpdatedAt: time.Now()},
	})
	// needPull must still be the channel from New (never niled).
	if d.needPull == nil {
		t.Fatal("needPull was niled after Run")
	}
	// Root is closed; ensure we did not leave a dirty panic path. Re-create root
	// is not required for requestPull; onNotify only touches peers + needPull when
	// alreadyHaveAll is false (empty index → schedules pull).
	_ = os.RemoveAll(dir) // dir unused after Run
}
