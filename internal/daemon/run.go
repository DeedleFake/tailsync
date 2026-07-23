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

// Run starts the daemon until ctx is cancelled.
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
	if err := d.reconcile(ctx); err != nil {
		return fmt.Errorf("initial reconcile: %w", err)
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
	// accept goroutine to finish, drain in-flight handleConn, then tear down
	// tsnet/local client state. Registered after the root-close defer so this
	// runs first (LIFO) and connWG.Wait completes before root.Close.
	// Never nil d.ln while acceptLoop may still call Accept on a shared field.
	defer func() {
		d.closeNetListener()
		<-acceptDone
		d.connWG.Wait()
		d.closeNetworkBackend()
	}()

	d.log.Info("tailsync started",
		"dir", d.cfg.Dir,
		"state", d.cfg.StateDir,
		"hostname", d.nodeID,
		"port", d.cfg.Port,
		"net_mode", d.cfg.NetMode.String(),
		"index_entries", d.idx.Len(),
		"max_file_bytes", d.cfg.MaxFileBytes,
	)
	if d.cfg.OnReady != nil {
		d.cfg.OnReady()
	}

	scanTick := time.NewTicker(d.cfg.ScanInterval)
	defer scanTick.Stop()
	syncTick := time.NewTicker(d.cfg.SyncInterval)
	defer syncTick.Stop()

	// Immediate peer sync attempt.
	d.syncPeers(ctx)

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
			if err := d.reconcile(ctx); err != nil {
				d.log.Error("reconcile", "err", err)
			}
		case <-syncTick.C:
			d.syncPeers(ctx)
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
