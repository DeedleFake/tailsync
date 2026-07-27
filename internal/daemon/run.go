package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"deedles.dev/tailsync/internal/index"
)

// signal coalesces multiple requests into a single buffered wake-up so the
// Run loop never blocks a producer and never stacks unbounded work.
type signal chan struct{}

func newSignal() signal {
	return make(chan struct{}, 1)
}

func (s signal) request() {
	select {
	case s <- struct{}{}:
	default:
	}
}

// Run starts the daemon until ctx is cancelled.
//
// Main loop roles:
//   - FS watch (debounced) and ScanInterval both request reconcile
//   - successful reconcile with peer-visible changes fans out best-effort
//     notifies (fire-and-forget; does not block the loop on peer dials)
//   - SyncInterval and inbound notifies schedule pull on a single-flight
//     background worker so long pull batches do not delay reconcile→notify
func (d *Daemon) Run(ctx context.Context) error {
	if err := os.MkdirAll(d.cfg.StateDir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	if err := os.MkdirAll(d.cfg.Dir, 0o755); err != nil {
		return fmt.Errorf("create sync dir: %w", err)
	}

	root, err := os.OpenRoot(d.cfg.Dir)
	if err != nil {
		return fmt.Errorf("open sync root: %w", err)
	}
	d.root = root
	// Close root only after in-flight handlers finish (see network defer below).
	// Use the local root variable so we never race a nil field on method calls.
	defer func() {
		_ = root.Close()
		d.root = nil
	}()

	indexPath := filepath.Join(d.cfg.StateDir, "index.json")
	idx, err := index.Load(indexPath)
	if err != nil {
		return fmt.Errorf("load index: %w", err)
	}
	d.idx = idx
	// nodeID may be refined during listen (host mode uses LocalAPI Self).
	d.nodeID = d.cfg.Hostname

	// Initial reconcile: detect offline deletions and local changes.
	// Peer pull runs after listen; if this applied offline changes, the
	// immediate pull + notify catch-up exchanges without waiting for
	// SyncInterval alone.
	changed, hints, err := d.reconcile(ctx)
	if err != nil {
		return fmt.Errorf("initial reconcile: %w", err)
	}
	if d.cfg.AfterReconcile != nil {
		d.cfg.AfterReconcile(changed)
	}

	if err := d.listen(ctx); err != nil {
		return err
	}

	// Capture listener for acceptLoop so Close/nil of d.ln cannot race Accept.
	ln := d.ln
	acceptDone := make(chan struct{})
	var acceptErr error
	go func() {
		defer close(acceptDone)
		acceptErr = d.acceptLoop(ctx, ln)
	}()

	// On every exit path: close the listener to unblock Accept, wait for the
	// accept goroutine to finish, drain in-flight handleConn, pull worker, and
	// notify dials, then tear down tsnet/local client state. Registered after
	// the root-close defer so this runs first (LIFO) and Wait completes before
	// root.Close. Never nil d.ln / d.root while handlers may still use them.
	defer func() {
		d.closeNetListener()
		<-acceptDone
		d.connWG.Wait()
		d.pullWG.Wait()
		d.notifyWG.Wait()
		d.closeNetworkBackend()
	}()

	needReconcile := newSignal()
	// needPull is allocated in New and never niled (avoids races with onNotify).

	// FS watch goroutine: must stop before root closes. stop waits for the
	// watcher to exit and Close itself (no use-after-close).
	stopWatch := d.startFSWatch(ctx, needReconcile.request)
	defer stopWatch()

	d.log.Info("tailsync started",
		"dir", d.cfg.Dir,
		"state", d.cfg.StateDir,
		"hostname", d.nodeID,
		"port", d.cfg.Port,
		"net_mode", d.cfg.NetMode.String(),
		"index_entries", d.idx.Len(),
		"max_file_bytes", d.cfg.MaxFileBytes,
		"watch", !d.cfg.DisableWatch,
	)
	if d.cfg.OnReady != nil {
		d.cfg.OnReady()
	}

	scanTick := time.NewTicker(d.cfg.ScanInterval)
	defer scanTick.Stop()
	syncTick := time.NewTicker(d.cfg.SyncInterval)
	defer syncTick.Stop()

	// schedulePull runs pullPeers on a background single-flight worker so the
	// main loop can keep reconcile→notify responsive during long dial batches.
	schedulePull := func() {
		if !d.pullFlight.tryStart() {
			return
		}
		d.pullWG.Go(func() {
			for {
				d.pullPeers(ctx)
				if d.cfg.AfterSyncPeers != nil {
					d.cfg.AfterSyncPeers()
				}
				if !d.pullFlight.finish() {
					return
				}
				// Follow-up batch for work coalesced while we were pulling.
			}
		})
	}

	// Notify for initial local changes (if any), then bootstrap pull.
	if changed {
		if d.scheduleNotify(ctx, hints) && d.cfg.AfterNotify != nil {
			d.cfg.AfterNotify()
		}
	}
	schedulePull()

	doReconcile := func() {
		changed, hints, err := d.reconcile(ctx)
		if err != nil {
			d.log.Error("reconcile", "err", err)
			return
		}
		if d.cfg.AfterReconcile != nil {
			d.cfg.AfterReconcile(changed)
		}
		if changed {
			// Fire-and-forget notify; independent of in-flight pull batches.
			if d.scheduleNotify(ctx, hints) && d.cfg.AfterNotify != nil {
				d.cfg.AfterNotify()
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			d.log.Info("shutting down")
			d.syncMu.Lock()
			if err := d.idx.Save(); err != nil {
				d.log.Error("save index on shutdown", "err", err)
			}
			d.syncMu.Unlock()
			return nil
		case <-acceptDone:
			if acceptErr != nil && ctx.Err() == nil {
				return acceptErr
			}
			return nil
		case <-scanTick.C:
			// Safety-net full rescan (also used when watch is disabled).
			doReconcile()
		case <-needReconcile:
			// Debounced FS events.
			doReconcile()
		case <-syncTick.C:
			// Backup/catch-up peer pull (background single-flight).
			schedulePull()
		case <-d.needPull:
			// Pull scheduled by inbound notify (and coalesced follow-ups).
			schedulePull()
		}
	}
}

// acceptLoop accepts connections on ln until ln is closed or a fatal Accept
// error occurs. ln must be non-nil and remain valid for the duration (caller
// closes it to unblock, then waits for this function to return).
func (d *Daemon) acceptLoop(ctx context.Context, ln net.Listener) error {
	if ln == nil {
		return nil
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Closed listener and/or cancelled context are normal shutdown.
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		d.connWG.Go(func() {
			d.handleConn(ctx, conn)
		})
	}
}
