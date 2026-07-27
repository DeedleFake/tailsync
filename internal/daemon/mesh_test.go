package daemon_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"deedles.dev/tailsync/internal/daemon"
)

// TestNotifyPullDeliversFile: A writes → notify → B pulls (long SyncInterval).
func TestNotifyPullDeliversFile(t *testing.T) {
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
	markReady := func() { ready <- struct{}{} }

	cfgA := daemon.Config{
		Dir:           dirA,
		StateDir:      stateA,
		Hostname:      "notify-a",
		Port:          portA,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         []string{"127.0.0.1:" + strconv.Itoa(portB)},
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		WatchDebounce: 40 * time.Millisecond,
		OnReady:       func() { readyA.Do(markReady) },
	}
	cfgB := daemon.Config{
		Dir:           dirB,
		StateDir:      stateB,
		Hostname:      "notify-b",
		Port:          portB,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         []string{"127.0.0.1:" + strconv.Itoa(portA)},
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		WatchDebounce: 40 * time.Millisecond,
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

	if err := os.WriteFile(filepath.Join(dirA, "n.txt"), []byte("from-a"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(dirB, "n.txt"), "from-a", 8*time.Second, errA, errB)

	cancel()
	<-errA
	<-errB
}

// TestWriterNotBlockedByDeadPeers: reconcile + notify launch finish quickly
// even when a configured peer is unreachable.
func TestWriterNotBlockedByDeadPeers(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := freePort(t)
	// Closed port: dial soft-fails.
	dead := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	var readyOnce sync.Once
	var notifyN atomic.Int32
	notified := make(chan struct{}, 8)

	d, err := daemon.New(daemon.Config{
		Dir:           dir,
		StateDir:      state,
		Hostname:      "writer",
		Port:          port,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         []string{"127.0.0.1:" + strconv.Itoa(dead)},
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		DialTimeout:   200 * time.Millisecond,
		WatchDebounce: 30 * time.Millisecond,
		OnReady: func() {
			readyOnce.Do(func() { close(ready) })
		},
		AfterNotify: func() {
			notifyN.Add(1)
			select {
			case notified <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()

	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("Run exited: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout ready")
	}

	start := time.Now()
	if err := os.WriteFile(filepath.Join(dir, "w.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// AfterNotify must fire without waiting for dead peer dials to finish.
	select {
	case <-notified:
	case err := <-errCh:
		t.Fatalf("Run exited: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for AfterNotify (writer blocked?)")
	}
	// scheduleNotify returns before dials complete; allow some debounce but
	// far below many dial timeouts.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("notify path took %v; writer should not wait on dead peers", elapsed)
	}

	cancel()
	<-errCh
}

// TestThreeNodeNotifyDedupe: A writes; B and C both end with the same content
// without requiring reverse-pull. Long intervals so delivery is notify+pull.
func TestThreeNodeNotifyDedupe(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	dirC := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()
	stateC := t.TempDir()
	portA := freePort(t)
	portB := freePort(t)
	portC := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := func(ports ...int) []string {
		var out []string
		for _, p := range ports {
			out = append(out, "127.0.0.1:"+strconv.Itoa(p))
		}
		return out
	}

	readyN := make(chan struct{}, 3)
	mk := func(dir, state, host string, port int, peers []string) *daemon.Daemon {
		d, err := daemon.New(daemon.Config{
			Dir:           dir,
			StateDir:      state,
			Hostname:      host,
			Port:          port,
			NetMode:       daemon.NetModePlain,
			ListenHost:    "127.0.0.1",
			Peers:         peers,
			ScanInterval:  time.Hour,
			SyncInterval:  time.Hour,
			WatchDebounce: 40 * time.Millisecond,
			OnReady:       func() { readyN <- struct{}{} },
		})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}

	da := mk(dirA, stateA, "mesh-a", portA, peer(portB, portC))
	db := mk(dirB, stateB, "mesh-b", portB, peer(portA, portC))
	dc := mk(dirC, stateC, "mesh-c", portC, peer(portA, portB))

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	errC := make(chan error, 1)
	go func() { errA <- da.Run(ctx) }()
	go func() { errB <- db.Run(ctx) }()
	go func() { errC <- dc.Run(ctx) }()

	for range 3 {
		select {
		case <-readyN:
		case err := <-errA:
			t.Fatalf("A: %v", err)
		case err := <-errB:
			t.Fatalf("B: %v", err)
		case err := <-errC:
			t.Fatalf("C: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("ready timeout")
		}
	}

	// Let bootstrap pulls settle so hot sets form.
	time.Sleep(200 * time.Millisecond)

	if err := os.WriteFile(filepath.Join(dirA, "mesh.txt"), []byte("mesh-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(dirB, "mesh.txt"), "mesh-content", 10*time.Second, errA, errB, errC)
	waitFile(t, filepath.Join(dirC, "mesh.txt"), "mesh-content", 10*time.Second, errA, errB, errC)

	cancel()
	<-errA
	<-errB
	<-errC
}

// TestStaleNotifyThenNewerFile: B eventually has the latest content after A
// updates again (pull uses current manifest, not notify hints).
func TestStaleNotifyThenNewerFile(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()
	portA := freePort(t)
	portB := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{}, 2)
	cfgA := daemon.Config{
		Dir:           dirA,
		StateDir:      stateA,
		Hostname:      "stale-a",
		Port:          portA,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         []string{"127.0.0.1:" + strconv.Itoa(portB)},
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		WatchDebounce: 30 * time.Millisecond,
		OnReady:       func() { ready <- struct{}{} },
	}
	cfgB := daemon.Config{
		Dir:           dirB,
		StateDir:      stateB,
		Hostname:      "stale-b",
		Port:          portB,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         []string{"127.0.0.1:" + strconv.Itoa(portA)},
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		WatchDebounce: 30 * time.Millisecond,
		OnReady:       func() { ready <- struct{}{} },
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
			t.Fatalf("A: %v", err)
		case err := <-errB:
			t.Fatalf("B: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("ready timeout")
		}
	}

	pathA := filepath.Join(dirA, "v.txt")
	if err := os.WriteFile(pathA, []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(dirB, "v.txt"), "v1", 8*time.Second, errA, errB)

	if err := os.WriteFile(pathA, []byte("v2-final"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(dirB, "v.txt"), "v2-final", 8*time.Second, errA, errB)

	cancel()
	<-errA
	<-errB
}

// TestLateJoinerPullCatchUp: B starts after A already has content; interval or
// bootstrap pull converges without permanent ban after soft-fails.
func TestLateJoinerPullCatchUp(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()
	ports := freePorts(t, 2)
	portA, portB := ports[0], ports[1]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := os.WriteFile(filepath.Join(dirA, "late.txt"), []byte("late-join"), 0o644); err != nil {
		t.Fatal(err)
	}

	readyA := make(chan struct{})
	var onceA sync.Once
	da, err := daemon.New(daemon.Config{
		Dir:          dirA,
		StateDir:     stateA,
		Hostname:     "late-a",
		Port:         portA,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portB)},
		ScanInterval: 200 * time.Millisecond,
		SyncInterval: 200 * time.Millisecond,
		DialTimeout:  300 * time.Millisecond,
		DisableWatch: true,
		OnReady:      func() { onceA.Do(func() { close(readyA) }) },
	})
	if err != nil {
		t.Fatal(err)
	}
	errA := make(chan error, 1)
	go func() { errA <- da.Run(ctx) }()

	select {
	case <-readyA:
	case err := <-errA:
		t.Fatalf("A: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("A ready timeout")
	}

	// Soft-fails while B is down must not permanently ban; B joins later.
	time.Sleep(500 * time.Millisecond)

	db, err := daemon.New(daemon.Config{
		Dir:          dirB,
		StateDir:     stateB,
		Hostname:     "late-b",
		Port:         portB,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portA)},
		ScanInterval: 200 * time.Millisecond,
		SyncInterval: 200 * time.Millisecond,
		DisableWatch: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	errB := make(chan error, 1)
	go func() { errB <- db.Run(ctx) }()

	waitFile(t, filepath.Join(dirB, "late.txt"), "late-join", 10*time.Second, errA, errB)

	cancel()
	<-errA
	<-errB
}

// TestNotifyDoesNotStorm: many rapid local writes still complete; no hang from
// notify/pull loops (content-id dedupe + single-flight pull).
func TestNotifyDoesNotStorm(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()
	portA := freePort(t)
	portB := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{}, 2)
	cfg := func(dir, state, host string, port, peer int) daemon.Config {
		return daemon.Config{
			Dir:           dir,
			StateDir:      state,
			Hostname:      host,
			Port:          port,
			NetMode:       daemon.NetModePlain,
			ListenHost:    "127.0.0.1",
			Peers:         []string{"127.0.0.1:" + strconv.Itoa(peer)},
			ScanInterval:  time.Hour,
			SyncInterval:  time.Hour,
			WatchDebounce: 40 * time.Millisecond,
			OnReady:       func() { ready <- struct{}{} },
		}
	}

	da, err := daemon.New(cfg(dirA, stateA, "storm-a", portA, portB))
	if err != nil {
		t.Fatal(err)
	}
	db, err := daemon.New(cfg(dirB, stateB, "storm-b", portB, portA))
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
			t.Fatalf("A: %v", err)
		case err := <-errB:
			t.Fatalf("B: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("ready")
		}
	}

	for i := range 15 {
		name := filepath.Join(dirA, "s"+strconv.Itoa(i)+".txt")
		if err := os.WriteFile(name, []byte("x"+strconv.Itoa(i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	waitFile(t, filepath.Join(dirB, "s14.txt"), "x14", 15*time.Second, errA, errB)

	// Daemons must still be healthy (not stuck in notify storm).
	select {
	case err := <-errA:
		t.Fatalf("A exited early: %v", err)
	case err := <-errB:
		t.Fatalf("B exited early: %v", err)
	default:
	}

	cancel()
	<-errA
	<-errB
}

// Ensure freePort still works when many tests run (sanity for mesh suite).
func TestMeshFreePortDistinct(t *testing.T) {
	a := freePort(t)
	b := freePort(t)
	if a == b {
		// Extremely unlikely with :0; if it happens re-bind check.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
		if a == c && b == c {
			t.Fatal("ports not distinct")
		}
	}
}

// TestMetaOnlyNotifyPull: touch (mtime-only) on A reaches B via notify→pull
// with huge SyncIntervals (would fail if alreadyHaveAll ignored meta LWW).
func TestMetaOnlyNotifyPull(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()
	portA := freePort(t)
	portB := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	content := []byte("touch-meta")
	pathA := filepath.Join(dirA, "meta.txt")
	if err := os.WriteFile(pathA, content, 0o644); err != nil {
		t.Fatal(err)
	}

	ready := make(chan struct{}, 2)
	cfg := func(dir, state, host string, port, peer int) daemon.Config {
		return daemon.Config{
			Dir:           dir,
			StateDir:      state,
			Hostname:      host,
			Port:          port,
			NetMode:       daemon.NetModePlain,
			ListenHost:    "127.0.0.1",
			Peers:         []string{"127.0.0.1:" + strconv.Itoa(peer)},
			ScanInterval:  time.Hour,
			SyncInterval:  time.Hour,
			WatchDebounce: 40 * time.Millisecond,
			OnReady:       func() { ready <- struct{}{} },
		}
	}

	da, err := daemon.New(cfg(dirA, stateA, "meta-a", portA, portB))
	if err != nil {
		t.Fatal(err)
	}
	db, err := daemon.New(cfg(dirB, stateB, "meta-b", portB, portA))
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
			t.Fatalf("A: %v", err)
		case err := <-errB:
			t.Fatalf("B: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("ready timeout")
		}
	}

	pathB := filepath.Join(dirB, "meta.txt")
	waitFile(t, pathB, string(content), 8*time.Second, errA, errB)

	fiB, err := os.Stat(pathB)
	if err != nil {
		t.Fatal(err)
	}
	mtimeBefore := fiB.ModTime()

	newMT := time.Now().Add(3 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(pathA, newMT, newMT); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(8 * time.Second)
	for {
		if time.Now().After(deadline) {
			fiB, _ = os.Stat(pathB)
			t.Fatalf("timeout waiting for mtime via notify+pull: B=%v want≈%v", fiB.ModTime(), newMT)
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
			time.Sleep(40 * time.Millisecond)
			continue
		}
		mtimeOK := fiB.ModTime().Equal(newMT) ||
			(fiB.ModTime().Truncate(time.Second).Equal(newMT.Truncate(time.Second)) &&
				!fiB.ModTime().Equal(mtimeBefore))
		if !mtimeOK {
			time.Sleep(40 * time.Millisecond)
			continue
		}
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

// TestNotifyDuringSlowPull: local write schedules AfterNotify while a slow
// pull batch (dead peers) is in flight — main loop must not wait on pull.
func TestNotifyDuringSlowPull(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := freePort(t)
	// Several closed ports so pull batch takes DialTimeout * N / parallelism.
	var dead []string
	for range 6 {
		dead = append(dead, "127.0.0.1:"+strconv.Itoa(freePort(t)))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	var readyOnce sync.Once
	notified := make(chan struct{}, 4)

	d, err := daemon.New(daemon.Config{
		Dir:           dir,
		StateDir:      state,
		Hostname:      "slow-pull",
		Port:          port,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         dead,
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		DialTimeout:   400 * time.Millisecond,
		WatchDebounce: 30 * time.Millisecond,
		OnReady: func() {
			readyOnce.Do(func() { close(ready) })
		},
		AfterNotify: func() {
			select {
			case notified <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()

	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("Run: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ready")
	}

	// Bootstrap pull is in flight (or done); write while dead-peer dials may still run.
	// AfterNotify must arrive without waiting for a multi-second dial batch.
	// Do not wait for pullStarted — that would serialize with the slow pull.

	start := time.Now()
	if err := os.WriteFile(filepath.Join(dir, "fast.txt"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notified:
	case err := <-errCh:
		t.Fatalf("Run: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("AfterNotify delayed behind pull batch")
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("notify path took %v; pull should not block reconcile→notify", elapsed)
	}

	cancel()
	<-errCh
}
