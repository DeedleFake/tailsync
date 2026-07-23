package daemon

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestWatchIgnore(t *testing.T) {
	root := "/tmp/sync"
	cases := []struct {
		name string
		path string
		want bool
	}{
		{"root itself", root, false},
		{"regular file", filepath.Join(root, "a.txt"), false},
		{"nested file", filepath.Join(root, "sub", "b.txt"), false},
		{"state dir", filepath.Join(root, ".tailsync"), true},
		{"state file", filepath.Join(root, ".tailsync", "index.json"), true},
		{"prefixed state", filepath.Join(root, ".tailsync-tmp", "x"), true},
		{"nested reserved", filepath.Join(root, "sub", ".tailsync", "x"), true},
		{"empty", "", true},
		{"outside", "/other/path", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := watchIgnore(root, tc.path); got != tc.want {
				t.Fatalf("watchIgnore(%q, %q)=%v want %v", root, tc.path, got, tc.want)
			}
		})
	}
}

// TestFSWatchStopWithoutParentCancel ensures stop() unblocks even when the
// parent context is still live (accept-failure Run exit path).
func TestFSWatchStopWithoutParentCancel(t *testing.T) {
	dir := t.TempDir()
	d, err := New(Config{
		Dir:          dir,
		StateDir:     t.TempDir(),
		Hostname:     "watch-stop",
		NetMode:      NetModePlain,
		ListenHost:   "127.0.0.1",
		Port:         1, // unused; we never listen
		ScanInterval: time.Hour,
		SyncInterval: time.Hour,
		Logger:       slog.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Never cancelled — stop must cancel its own watchCtx.
	parent := context.Background()
	stop := d.startFSWatch(parent, func() {})
	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stopWatch hung with live parent context")
	}
}

// TestAddWatchTreeRootAddFailure proves root Add errors are fatal (not silent
// success): a closed watcher cannot Add, so addWatchTree returns err and added=0.
func TestAddWatchTreeRootAddFailure(t *testing.T) {
	dir := t.TempDir()
	w, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	added, err := addWatchTree(w, dir, dir)
	if err == nil {
		t.Fatal("expected error when watching with a closed fsnotify.Watcher")
	}
	if added != 0 {
		t.Fatalf("added=%d want 0 on root Add failure", added)
	}
}

// TestAddWatchTreeNoDirsWhenRootIsFile covers the added==0 path: WalkDir of a
// regular file never Adds a directory, so setup fails instead of reporting success.
func TestAddWatchTreeNoDirsWhenRootIsFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })

	added, err := addWatchTree(w, f, f)
	if err == nil {
		t.Fatal("expected error when watch root is a file")
	}
	if added != 0 {
		t.Fatalf("added=%d want 0", added)
	}
}

// TestStartFSWatchSetupFailureFallsBack ensures startFSWatch does not hang and
// returns a noop stop when the sync root cannot be watched (file, not dir).
func TestStartFSWatchSetupFailureFallsBack(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file-root")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	d, err := New(Config{
		Dir:      f,
		StateDir: t.TempDir(),
		Hostname: "bad-root",
		NetMode:  NetModePlain,
		Port:     1,
		Logger:   slog.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Setup fails (no directory watches); stop must be a non-blocking noop.
	stop := d.startFSWatch(context.Background(), func() {
		t.Error("reconcile should not be requested from a failed watch setup")
	})
	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stop after failed watch setup hung")
	}
}

// TestDisableWatchStopIsNoop verifies DisableWatch skips the watcher entirely.
func TestDisableWatchStopIsNoop(t *testing.T) {
	d, err := New(Config{
		Dir:          t.TempDir(),
		StateDir:     t.TempDir(),
		Hostname:     "nowatch",
		DisableWatch: true,
		NetMode:      NetModePlain,
		Port:         1,
		Logger:       slog.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := d.startFSWatch(context.Background(), func() {})
	stop() // must not hang
}
