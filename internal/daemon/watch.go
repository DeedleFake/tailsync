package daemon

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"

	"deedles.dev/tailsync/internal/pathutil"
)

// watchIgnore reports whether an absolute path under the sync root should be
// ignored by the filesystem watcher (reserved state dirs and non-local paths).
func watchIgnore(root, name string) bool {
	if name == "" {
		return true
	}
	rel, err := filepath.Rel(root, name)
	if err != nil {
		return true
	}
	if rel == "." {
		return false
	}
	if !filepath.IsLocal(rel) {
		return true
	}
	for part := range strings.SplitSeq(filepath.ToSlash(rel), "/") {
		if pathutil.IsReservedComponent(part) {
			return true
		}
	}
	return false
}

// addWatchTree recursively adds watches for dir and its subdirectories,
// skipping reserved components. dir must be absolute and under the sync root.
//
// Adding the walk root (dir itself) is required; failure there returns an
// error so callers can fall back to timer-only scan. Nested directory Add
// failures are best-effort (counted but not fatal).
func addWatchTree(w *fsnotify.Watcher, root, dir string) (added int, err error) {
	walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Unreadable subtree: skip rather than abort watching entirely.
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if watchIgnore(root, path) {
			return fs.SkipDir
		}
		if addErr := w.Add(path); addErr != nil {
			// Root of this tree must succeed; nested dirs are best-effort.
			if path == dir {
				return fmt.Errorf("watch %s: %w", path, addErr)
			}
			return nil
		}
		added++
		return nil
	})
	if walkErr != nil {
		return added, walkErr
	}
	if added == 0 {
		return 0, fmt.Errorf("no directories watched under %s", dir)
	}
	return added, nil
}

// startFSWatch begins recursive filesystem watching of cfg.Dir. Events are
// debounced and delivered via requestReconcile. On unsupported platforms or
// setup failure, logs a warning and returns a no-op stop (timer-only scan).
//
// stop cancels the watch loop (even if parent ctx is still live) and waits
// for the goroutine to Close the watcher — safe on accept-failure Run exits.
func (d *Daemon) startFSWatch(ctx context.Context, requestReconcile func()) (stop func()) {
	noop := func() {}
	if d.cfg.DisableWatch {
		d.log.Debug("fs watch disabled")
		return noop
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		d.log.Warn("fs watch unavailable; using scan interval only", "err", err)
		return noop
	}
	added, err := addWatchTree(w, d.cfg.Dir, d.cfg.Dir)
	if err != nil || added == 0 {
		_ = w.Close()
		d.log.Warn("fs watch setup failed; using scan interval only", "err", err, "added", added)
		return noop
	}

	// Independent of parent ctx so stop() unblocks when Run exits on accept
	// failure without cancelling the caller's context.
	watchCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		// Close the watcher only from this goroutine after the loop exits so
		// Events/Errors handlers never observe a use-after-close.
		defer close(done)
		defer func() { _ = w.Close() }()
		d.watchLoop(watchCtx, w, requestReconcile)
	}()

	d.log.Info("fs watch started", "dir", d.cfg.Dir, "debounce", d.cfg.WatchDebounce, "dirs", added)
	return func() {
		cancel()
		<-done
	}
}

func (d *Daemon) watchLoop(ctx context.Context, w *fsnotify.Watcher, requestReconcile func()) {
	debounce := time.NewTimer(0)
	if !debounce.Stop() {
		<-debounce.C
	}
	defer debounce.Stop()

	pending := false
	armDebounce := func() {
		pending = true
		if !debounce.Stop() {
			select {
			case <-debounce.C:
			default:
			}
		}
		debounce.Reset(d.cfg.WatchDebounce)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			if watchIgnore(d.cfg.Dir, ev.Name) {
				continue
			}
			// Track new directories so nested creates are observed.
			if ev.Has(fsnotify.Create) {
				if fi, err := os.Stat(ev.Name); err == nil && fi.IsDir() {
					_, _ = addWatchTree(w, d.cfg.Dir, ev.Name)
				}
			}
			// Drop watches for removed/renamed paths when possible.
			if ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename) {
				_ = w.Remove(ev.Name)
			}
			armDebounce()
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			if err != nil {
				d.log.Debug("fs watch error", "err", err)
			}
		case <-debounce.C:
			if !pending {
				continue
			}
			pending = false
			requestReconcile()
		}
	}
}
