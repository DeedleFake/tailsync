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
