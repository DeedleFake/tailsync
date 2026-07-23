package daemon_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"deedles.dev/tailsync/internal/daemon"
	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/scan"
)

func waitFile(t *testing.T, path, want string, timeout time.Duration, errCh ...<-chan error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s == %q", path, want)
		}
		data, err := os.ReadFile(path)
		if err == nil && string(data) == want {
			return
		}
		for _, ch := range errCh {
			select {
			case err := <-ch:
				if err != nil {
					t.Fatalf("daemon exited: %v", err)
				}
			default:
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitGone(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s to be removed", path)
		}
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestTwoNodesPlainTCP spins up two daemons on localhost and verifies
// file sync, content update, and deletion propagation.
func TestTwoNodesPlainTCP(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()

	if err := os.WriteFile(filepath.Join(dirA, "hello.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Unique high ports per test run to reduce collisions.
	portA := 19010 + (os.Getpid() % 1000)
	portB := portA + 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfgA := daemon.Config{
		Dir:          dirA,
		StateDir:     stateA,
		Hostname:     "node-a",
		Port:         portA,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portB)},
		ScanInterval: 150 * time.Millisecond,
		SyncInterval: 150 * time.Millisecond,
	}
	cfgB := daemon.Config{
		Dir:          dirB,
		StateDir:     stateB,
		Hostname:     "node-b",
		Port:         portB,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portA)},
		ScanInterval: 150 * time.Millisecond,
		SyncInterval: 150 * time.Millisecond,
	}

	da, err := daemon.New(cfgA)
	if err != nil {
		t.Fatal(err)
	}
	db, err := daemon.New(cfgB)
	if err != nil {
		t.Fatal(err)
	}

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- da.Run(ctx) }()
	go func() { errB <- db.Run(ctx) }()

	waitFile(t, filepath.Join(dirB, "hello.txt"), "hello", 15*time.Second, errA, errB)

	// Modify on A; scan + sync should pull to B.
	if err := os.WriteFile(filepath.Join(dirA, "hello.txt"), []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(dirB, "hello.txt"), "hello world", 15*time.Second, errA, errB)

	// Delete on A; B should remove.
	if err := os.Remove(filepath.Join(dirA, "hello.txt")); err != nil {
		t.Fatal(err)
	}
	waitGone(t, filepath.Join(dirB, "hello.txt"), 15*time.Second)

	cancel()
	<-errA
	<-errB
}

// TestTwoNodesMtimeOnlySync verifies that a touch (mtime-only) on A propagates to B.
func TestTwoNodesMtimeOnlySync(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()

	content := []byte("touch-me")
	pathA := filepath.Join(dirA, "meta.txt")
	if err := os.WriteFile(pathA, content, 0o644); err != nil {
		t.Fatal(err)
	}

	portA := 20010 + (os.Getpid() % 1000)
	portB := portA + 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfgA := daemon.Config{
		Dir:          dirA,
		StateDir:     stateA,
		Hostname:     "node-a-mtime",
		Port:         portA,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portB)},
		ScanInterval: 150 * time.Millisecond,
		SyncInterval: 150 * time.Millisecond,
	}
	cfgB := daemon.Config{
		Dir:          dirB,
		StateDir:     stateB,
		Hostname:     "node-b-mtime",
		Port:         portB,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portA)},
		ScanInterval: 150 * time.Millisecond,
		SyncInterval: 150 * time.Millisecond,
	}

	da, err := daemon.New(cfgA)
	if err != nil {
		t.Fatal(err)
	}
	db, err := daemon.New(cfgB)
	if err != nil {
		t.Fatal(err)
	}

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- da.Run(ctx) }()
	go func() { errB <- db.Run(ctx) }()

	pathB := filepath.Join(dirB, "meta.txt")
	waitFile(t, pathB, string(content), 15*time.Second, errA, errB)

	// Record B's mtime after initial sync.
	fiB, err := os.Stat(pathB)
	if err != nil {
		t.Fatal(err)
	}
	mtimeBefore := fiB.ModTime()

	// touch on A: content unchanged, mtime advanced.
	newMT := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(pathA, newMT, newMT); err != nil {
		t.Fatal(err)
	}

	// Wait until B's mtime matches A's (within filesystem precision).
	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			fiB, _ = os.Stat(pathB)
			t.Fatalf("timeout waiting for mtime sync: B=%v want≈%v (before=%v)", fiB.ModTime(), newMT, mtimeBefore)
		}
		for _, ch := range []<-chan error{errA, errB} {
			select {
			case err := <-ch:
				if err != nil {
					t.Fatalf("daemon exited: %v", err)
				}
			default:
			}
		}
		fiB, err = os.Stat(pathB)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// Exact match, or second-precision match on coarse filesystems.
		mtimeOK := fiB.ModTime().Equal(newMT) ||
			(fiB.ModTime().Truncate(time.Second).Equal(newMT.Truncate(time.Second)) &&
				!fiB.ModTime().Equal(mtimeBefore))
		if !mtimeOK {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// Content must still match on every success path.
		data, err := os.ReadFile(pathB)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != string(content) {
			t.Fatalf("content changed: %q", data)
		}
		break
	}

	cancel()
	<-errA
	<-errB
}

func TestReconcileOfflineDeleteViaIndex(t *testing.T) {
	root := t.TempDir()
	idx := index.New()
	idx.Set(index.Entry{
		Path:      "x.txt",
		Hash:      "aa",
		Size:      2,
		UpdatedAt: time.Now().Add(-time.Hour),
	})
	r, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	res, err := scan.Scan(context.Background(), r, idx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Changes) != 1 || res.Changes[0].Kind != scan.Deleted {
		t.Fatalf("%+v", res.Changes)
	}
}

// TestRunCancelAcceptLoopRace starts a plain daemon, waits until listening,
// then cancels — repeated to exercise acceptLoop vs listener close (race clean).
func TestRunCancelAcceptLoopRace(t *testing.T) {
	for i := range 30 {
		dir := t.TempDir()
		state := t.TempDir()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		ready := make(chan struct{})
		var readyOnce sync.Once
		d, err := daemon.New(daemon.Config{
			Dir:          dir,
			StateDir:     state,
			Hostname:     "race-" + strconv.Itoa(i),
			Port:         port,
			NetMode:      daemon.NetModePlain,
			ListenHost:   "127.0.0.1",
			ScanInterval: time.Hour,
			SyncInterval: time.Hour,
			OnReady: func() {
				readyOnce.Do(func() { close(ready) })
			},
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
			cancel()
			t.Fatalf("iter %d: Run exited before ready: %v", i, err)
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("iter %d: timeout waiting for ready", i)
		}

		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("iter %d: Run after cancel: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("iter %d: Run did not exit after cancel", i)
		}
	}
}

// freePort binds 127.0.0.1:0 and returns the chosen port after closing.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// TestFSWatchSyncOnChange verifies watch + sync-on-change deliver a post-start
// write to a peer when BOTH sides use a very long SyncInterval (and ScanInterval).
// That only passes with a bidirectional session: A dials after local change and
// B reverse-pulls A's file without waiting for B's sync ticker.
func TestFSWatchSyncOnChange(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()

	portA := freePort(t)
	portB := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var readyA, readyB sync.Once
	ready := make(chan struct{}, 2)
	markReady := func() {
		ready <- struct{}{}
	}

	// Long intervals on both sides: success under 5s proves watch → reconcile →
	// sync-on-change bidirectional delivery, not peer tickers.
	cfgA := daemon.Config{
		Dir:           dirA,
		StateDir:      stateA,
		Hostname:      "watch-a",
		Port:          portA,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         []string{"127.0.0.1:" + strconv.Itoa(portB)},
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		WatchDebounce: 50 * time.Millisecond,
		OnReady:       func() { readyA.Do(markReady) },
	}
	cfgB := daemon.Config{
		Dir:           dirB,
		StateDir:      stateB,
		Hostname:      "watch-b",
		Port:          portB,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         []string{"127.0.0.1:" + strconv.Itoa(portA)},
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		WatchDebounce: 50 * time.Millisecond,
		OnReady:       func() { readyB.Do(markReady) },
	}

	da, err := daemon.New(cfgA)
	if err != nil {
		t.Fatal(err)
	}
	db, err := daemon.New(cfgB)
	if err != nil {
		t.Fatal(err)
	}

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- da.Run(ctx) }()
	go func() { errB <- db.Run(ctx) }()

	for range 2 {
		select {
		case <-ready:
		case err := <-errA:
			t.Fatalf("A exited: %v", err)
		case err := <-errB:
			t.Fatalf("B exited: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for ready")
		}
	}

	if err := os.WriteFile(filepath.Join(dirA, "fast.txt"), []byte("fast-sync"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(dirB, "fast.txt"), "fast-sync", 5*time.Second, errA, errB)

	cancel()
	<-errA
	<-errB
}

// TestSyncOnChangeDeliversToPeer asserts that a local write on A reaches B via
// sync-on-change when both SyncIntervals are hours (B never dials on its ticker).
func TestSyncOnChangeDeliversToPeer(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()

	portA := freePort(t)
	portB := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Barrier: first AfterSyncPeers after OnReady is the initial post-listen sync.
	initialSyncDone := make(chan struct{})
	var initialOnce sync.Once
	ready := make(chan struct{})
	var readyOnce sync.Once

	cfgA := daemon.Config{
		Dir:           dirA,
		StateDir:      stateA,
		Hostname:      "sync-a",
		Port:          portA,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         []string{"127.0.0.1:" + strconv.Itoa(portB)},
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		WatchDebounce: 50 * time.Millisecond,
		OnReady: func() {
			readyOnce.Do(func() { close(ready) })
		},
		AfterSyncPeers: func() {
			initialOnce.Do(func() { close(initialSyncDone) })
		},
	}
	cfgB := daemon.Config{
		Dir:          dirB,
		StateDir:     stateB,
		Hostname:     "sync-b",
		Port:         portB,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portA)},
		ScanInterval: time.Hour,
		SyncInterval: time.Hour,
		DisableWatch: true,
	}

	da, err := daemon.New(cfgA)
	if err != nil {
		t.Fatal(err)
	}
	db, err := daemon.New(cfgB)
	if err != nil {
		t.Fatal(err)
	}

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- da.Run(ctx) }()
	go func() { errB <- db.Run(ctx) }()

	select {
	case <-ready:
	case err := <-errA:
		t.Fatalf("A exited: %v", err)
	case err := <-errB:
		t.Fatalf("B exited: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ready")
	}

	select {
	case <-initialSyncDone:
	case err := <-errA:
		t.Fatalf("A exited before initial sync: %v", err)
	case err := <-errB:
		t.Fatalf("B exited before initial sync: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for initial syncPeers")
	}

	if err := os.WriteFile(filepath.Join(dirA, "kick.txt"), []byte("kick"), 0o644); err != nil {
		t.Fatal(err)
	}
	// B must receive content via A's sync-on-change reverse-pull phase.
	waitFile(t, filepath.Join(dirB, "kick.txt"), "kick", 5*time.Second, errA, errB)

	cancel()
	<-errA
	<-errB
}

// TestWatchDebounceCoalesces ensures a burst of FS events produces one (or few)
// reconciles rather than one per event.
func TestWatchDebounceCoalesces(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := freePort(t)

	var mu sync.Mutex
	reconciles := 0
	changedN := 0

	ready := make(chan struct{})
	var readyOnce sync.Once

	d, err := daemon.New(daemon.Config{
		Dir:           dir,
		StateDir:      state,
		Hostname:      "debounce",
		Port:          port,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		WatchDebounce: 100 * time.Millisecond,
		OnReady: func() {
			readyOnce.Do(func() { close(ready) })
		},
		AfterReconcile: func(changed bool) {
			mu.Lock()
			reconciles++
			if changed {
				changedN++
			}
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()

	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("Run exited: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ready")
	}

	mu.Lock()
	afterReady := reconciles
	mu.Unlock()

	// Burst of creates while debounce window keeps resetting.
	for i := range 20 {
		name := filepath.Join(dir, "f"+strconv.Itoa(i)+".txt")
		if err := os.WriteFile(name, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Wait past debounce + processing; should not approach 20 reconciles.
	deadline := time.Now().Add(2 * time.Second)
	var final, finalChanged int
	for {
		mu.Lock()
		final = reconciles
		finalChanged = changedN
		mu.Unlock()
		// At least one post-ready reconcile with changes should land.
		if final > afterReady && finalChanged > 0 {
			// Give a little extra quiet time so a second debounce cannot still be pending.
			time.Sleep(250 * time.Millisecond)
			mu.Lock()
			final = reconciles
			finalChanged = changedN
			mu.Unlock()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for debounced reconcile: reconciles=%d afterReady=%d changed=%d",
				final, afterReady, finalChanged)
		}
		select {
		case err := <-errCh:
			t.Fatalf("daemon exited: %v", err)
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}

	extra := final - afterReady
	if extra > 5 {
		t.Fatalf("debounce failed to coalesce: %d reconciles after burst (afterReady=%d total=%d)",
			extra, afterReady, final)
	}
	if finalChanged < 1 {
		t.Fatal("expected at least one changed reconcile after writes")
	}

	cancel()
	<-errCh
}

// TestDisableWatchFallsBackToScanInterval ensures timer-only mode still syncs.
func TestDisableWatchFallsBackToScanInterval(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()

	portA := freePort(t)
	portB := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfgA := daemon.Config{
		Dir:          dirA,
		StateDir:     stateA,
		Hostname:     "nowatch-a",
		Port:         portA,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portB)},
		ScanInterval: 100 * time.Millisecond,
		SyncInterval: 100 * time.Millisecond,
		DisableWatch: true,
	}
	cfgB := daemon.Config{
		Dir:          dirB,
		StateDir:     stateB,
		Hostname:     "nowatch-b",
		Port:         portB,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portA)},
		ScanInterval: 100 * time.Millisecond,
		SyncInterval: 100 * time.Millisecond,
		DisableWatch: true,
	}

	if err := os.WriteFile(filepath.Join(dirA, "timer.txt"), []byte("timer"), 0o644); err != nil {
		t.Fatal(err)
	}

	da, err := daemon.New(cfgA)
	if err != nil {
		t.Fatal(err)
	}
	db, err := daemon.New(cfgB)
	if err != nil {
		t.Fatal(err)
	}

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- da.Run(ctx) }()
	go func() { errB <- db.Run(ctx) }()

	waitFile(t, filepath.Join(dirB, "timer.txt"), "timer", 10*time.Second, errA, errB)

	// Post-start write still propagates via scan + sync intervals.
	if err := os.WriteFile(filepath.Join(dirA, "timer2.txt"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(dirB, "timer2.txt"), "two", 10*time.Second, errA, errB)

	cancel()
	<-errA
	<-errB
}

// TestReconcileNoChangeNoSyncThrash writes nothing after ready and checks that
// AfterReconcile is not flooded with changed=true (remote/self thrash guard).
func TestReconcileIdleNoChanged(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := freePort(t)

	var mu sync.Mutex
	changedN := 0

	ready := make(chan struct{})
	var readyOnce sync.Once

	d, err := daemon.New(daemon.Config{
		Dir:           dir,
		StateDir:      state,
		Hostname:      "idle",
		Port:          port,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		ScanInterval:  50 * time.Millisecond,
		SyncInterval:  time.Hour,
		WatchDebounce: 20 * time.Millisecond,
		DisableWatch:  true, // force periodic reconcile only
		OnReady: func() {
			readyOnce.Do(func() { close(ready) })
		},
		AfterReconcile: func(changed bool) {
			if !changed {
				return
			}
			mu.Lock()
			changedN++
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()

	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("Run exited: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ready")
	}

	// Empty dir: initial reconcile may be changed=false; further scans stay quiet.
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	n := changedN
	mu.Unlock()
	if n != 0 {
		t.Fatalf("idle reconciles reported changed=%d want 0", n)
	}

	cancel()
	<-errCh
}
