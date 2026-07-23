package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"deedles.dev/tailsync/internal/atomicfile"
	"deedles.dev/tailsync/internal/delta"
	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/proto"
)

// errTransport marks framing / connection failures that invalidate the session.
// errPeerLogical marks TypeError responses and other per-entry logical failures
// that should not abort the rest of a peer sync.
var (
	errTransport   = errors.New("transport error")
	errPeerLogical = errors.New("peer logical error")
)

func isTransportErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errTransport) || errors.Is(err, io.EOF)
}

func peerLogical(msg string) error {
	return fmt.Errorf("%w: %s", errPeerLogical, msg)
}

// applyKind is the pure decision outcome of reconciling one remote entry.
type applyKind int

const (
	applyNoop       applyKind = iota
	applyTombstone            // remote deleted; local absent or already tombstoned
	applyDeleteLive           // remote deleted; local live file must be removed
	applyMetaOnly             // same content hash; adopt remote mode/mtime
	applyContent              // need bytes from peer (full or delta)
)

// applyDecision is a pure (or disk-presence-informed) plan for applyRemote.
type applyDecision struct {
	kind     applyKind
	remote   index.Entry
	local    index.Entry
	hasLocal bool
	useDelta bool
}

// decideApply chooses how to apply remote given the current local index entry
// and whether the live file exists on disk (only consulted for same-hash meta).
// Callers must reject empty-hash live remotes before calling.
func decideApply(local index.Entry, hasLocal bool, remote index.Entry, diskPresent bool) applyDecision {
	if remote.Deleted {
		if !hasLocal || local.Deleted {
			return applyDecision{kind: applyTombstone, remote: remote, local: local, hasLocal: hasLocal}
		}
		if !index.Wins(local, remote) {
			return applyDecision{kind: applyNoop}
		}
		return applyDecision{kind: applyDeleteLive, remote: remote, local: local, hasLocal: hasLocal}
	}

	if hasLocal && !local.Deleted && local.Hash == remote.Hash {
		mtimeDiffers := !remote.ModTime.IsZero() && mtimesDiffer(remote.ModTime, local.ModTime)
		metaDiffers := remote.Mode != local.Mode || mtimeDiffers ||
			remote.UpdatedAt.After(local.UpdatedAt)
		needMeta := metaDiffers && index.Wins(local, remote)
		if !needMeta {
			return applyDecision{kind: applyNoop}
		}
		if !diskPresent {
			// File missing: pull content rather than commit metadata-only.
			return applyDecision{kind: applyContent, remote: remote, local: local, hasLocal: hasLocal, useDelta: false}
		}
		return applyDecision{kind: applyMetaOnly, remote: remote, local: local, hasLocal: hasLocal}
	}

	if hasLocal && !index.Wins(local, remote) {
		return applyDecision{kind: applyNoop}
	}

	useDelta := hasLocal && !local.Deleted
	return applyDecision{kind: applyContent, remote: remote, local: local, hasLocal: hasLocal, useDelta: useDelta}
}

// syncPeers dials and syncs independent peers in parallel, capped by
// maxParallelPeerSyncs (each in-flight content pull may buffer up to
// MaxFileBytes). Disk and index commits remain serialized via syncMu.
// The main Run loop waits for this call to return before the next reconcile.
//
// Each session is bidirectional (see syncWith): both sides exchange manifests
// and pull missing content on the same connection, so a dial after local
// changes delivers those changes to the peer without waiting for the peer's
// SyncInterval.
func (d *Daemon) syncPeers(ctx context.Context) {
	peers, err := d.listPeers(ctx)
	if err != nil {
		d.log.Debug("list peers", "err", err)
		return
	}
	if len(peers) == 0 {
		return
	}

	sem := make(chan struct{}, maxParallelPeerSyncs)
	var wg sync.WaitGroup
	for _, addr := range peers {
		if err := ctx.Err(); err != nil {
			break
		}
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			if err := d.syncWith(ctx, addr); err != nil {
				d.log.Debug("sync peer", "addr", addr, "err", err)
			}
		})
	}
	wg.Wait()
}

// syncWith opens a bidirectional sync session with addr:
//
//  1. Hello / HelloOK
//  2. Dialer pulls listener (ManifestReq → apply entries, FileReq/DeltaReq as needed)
//  3. Dialer sends SyncDone
//  4. Dialer serves while listener pulls (ManifestReq / FileReq / DeltaReq)
//  5. Listener sends SyncDone; both sides close
//
// This delivers the dialer's local index changes to the peer in one session.
func (d *Daemon) syncWith(ctx context.Context, addr string) error {
	conn, err := d.dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	d.setConnDeadline(conn, ctx)

	if err := proto.Encode(conn, proto.NewHello(d.nodeID)); err != nil {
		return err
	}
	resp, err := proto.Decode(conn)
	if err != nil {
		return fmt.Errorf("hello response: %w", err)
	}
	if resp.Header.Type == proto.TypeError {
		return fmt.Errorf("hello error: %s", resp.Header.Error)
	}
	if resp.Header.Type != proto.TypeHelloOK {
		return fmt.Errorf("unexpected hello response %q", resp.Header.Type)
	}
	if resp.Header.NodeID == d.nodeID {
		return fmt.Errorf("connected to self")
	}

	// Phase 1: pull from peer.
	n, err := d.pullFromConn(ctx, conn)
	if err != nil {
		return err
	}
	// End pull phase so the peer can reverse-pull our manifest.
	if err := proto.Encode(conn, proto.NewSyncDone()); err != nil {
		return fmt.Errorf("sync_done: %w", err)
	}
	// Phase 2: serve peer's reverse pull until their SyncDone (or EOF).
	if err := d.servePullPhase(ctx, conn); err != nil {
		return err
	}
	d.log.Info("synced peer", "addr", addr, "remote_node", resp.Header.NodeID, "pulled_entries", n)
	return nil
}

// pullFromConn requests the peer's manifest and applies each entry (pulling
// file/delta content as needed on conn). Returns the number of manifest
// entries received. On transport failure during apply, aborts early.
func (d *Daemon) pullFromConn(ctx context.Context, conn net.Conn) (int, error) {
	if err := proto.Encode(conn, proto.NewManifestReq()); err != nil {
		return 0, err
	}
	man, err := proto.Decode(conn)
	if err != nil {
		return 0, fmt.Errorf("manifest: %w", err)
	}
	if man.Header.Type == proto.TypeError {
		return 0, fmt.Errorf("manifest error: %s", man.Header.Error)
	}
	if man.Header.Type != proto.TypeManifest {
		return 0, fmt.Errorf("expected manifest, got %q", man.Header.Type)
	}

	changed := false
	var transportErr error
	for _, remote := range man.Header.Entries {
		if err := ctx.Err(); err != nil {
			return len(man.Header.Entries), err
		}
		d.setConnDeadline(conn, ctx)
		did, err := d.applyRemote(ctx, conn, remote)
		if err != nil {
			// Per-entry logical errors: continue. Transport/decode failures abort.
			if isTransportErr(err) {
				transportErr = err
				d.log.Warn("peer transport error, aborting pull", "err", err)
				break
			}
			d.log.Warn("apply remote entry", "path", remote.Path, "err", err)
			continue
		}
		if did {
			changed = true
		}
	}
	if changed {
		d.syncMu.Lock()
		if err := d.idx.Save(); err != nil {
			d.syncMu.Unlock()
			return len(man.Header.Entries), fmt.Errorf("save index: %w", err)
		}
		d.appliesSinceSave = 0
		d.syncMu.Unlock()
	}
	if transportErr != nil {
		return len(man.Header.Entries), transportErr
	}
	return len(man.Header.Entries), nil
}

// servePullPhase answers ManifestReq / FileReq / DeltaReq until the peer
// sends SyncDone or the connection ends. Used by the dialer after its own
// pull completes so the listener can reverse-pull.
func (d *Daemon) servePullPhase(ctx context.Context, conn net.Conn) error {
	remote := conn.RemoteAddr().String()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		d.setConnDeadline(conn, ctx)
		msg, err := proto.Decode(conn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("serve pull phase: %w", err)
		}
		if msg.Header.Type == proto.TypeSyncDone {
			return nil
		}
		if err := d.serveMsg(ctx, conn, msg); err != nil {
			d.log.Debug("serve message", "remote", remote, "type", msg.Header.Type, "err", err)
			if encodeErr := proto.Encode(conn, proto.NewError(err.Error())); encodeErr != nil {
				return encodeErr
			}
			if msg.Header.Type == "" || errors.Is(err, errUnexpectedMsgType) {
				return err
			}
		}
	}
}

// applyRemote reconciles one remote manifest entry.
//
// Pattern: decide under syncMu → release for network transfer → re-lock,
// re-check LWW, then atomic write + index commit. Other peer applies may run
// during the unlocked pull; commits re-take syncMu. The main Run loop does not
// reconcile until syncPeers returns.
func (d *Daemon) applyRemote(ctx context.Context, conn net.Conn, remote index.ManifestEntry) (bool, error) {
	rel, err := d.relPath(remote.Path)
	if err != nil {
		return false, fmt.Errorf("reject path: %w", err)
	}
	remote.Path = rel
	re := index.EntryFromManifest(remote)

	if !remote.Deleted && remote.Hash == "" {
		return false, peerLogical(fmt.Sprintf("live entry %q missing content hash", remote.Path))
	}

	d.syncMu.Lock()
	if err := ctx.Err(); err != nil {
		d.syncMu.Unlock()
		return false, err
	}

	local, hasLocal := d.idx.Get(remote.Path)
	diskPresent := false
	if hasLocal && !local.Deleted && !remote.Deleted && local.Hash == remote.Hash {
		if _, err := d.root.Stat(remote.Path); err == nil {
			diskPresent = true
		} else if !os.IsNotExist(err) {
			d.syncMu.Unlock()
			return false, fmt.Errorf("stat %s: %w", remote.Path, err)
		}
	}

	dec := decideApply(local, hasLocal, re, diskPresent)

	switch dec.kind {
	case applyNoop:
		d.syncMu.Unlock()
		return false, nil

	case applyTombstone:
		ok := d.idx.SetIfWins(re)
		if ok {
			d.appliesSinceSave++
		}
		d.syncMu.Unlock()
		return ok, nil

	case applyDeleteLive:
		changed, err := d.execDeleteLive(re)
		d.syncMu.Unlock()
		return changed, err

	case applyMetaOnly:
		changed, err := d.execMetaOnly(local, re)
		d.syncMu.Unlock()
		return changed, err

	case applyContent:
		useDelta := dec.useDelta
		d.syncMu.Unlock()

		data, got, err := d.pullAndVerify(ctx, conn, remote, useDelta)
		if err != nil {
			return false, err
		}
		if err := d.checkFileSize(remote.Path, int64(len(data))); err != nil {
			return false, err
		}

		d.syncMu.Lock()
		defer d.syncMu.Unlock()
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return d.commitContent(re, data, got)
	}

	d.syncMu.Unlock()
	return false, nil
}

// pullAndVerify fetches remote bytes (delta and/or full) and returns data whose
// SHA-256 matches remote.Hash. On delta success with a hash mismatch (e.g. basis
// raced with another commit), retries a full pull once before failing.
func (d *Daemon) pullAndVerify(ctx context.Context, conn net.Conn, remote index.ManifestEntry, useDelta bool) ([]byte, string, error) {
	data, err := d.pullContent(ctx, conn, remote, useDelta)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	if got == remote.Hash {
		return data, got, nil
	}

	if useDelta {
		d.log.Warn("hash mismatch after delta; retrying full pull",
			"path", remote.Path,
			"got", got,
			"want", remote.Hash,
			"use_delta", true,
		)
		data, err = d.pullFull(ctx, conn, remote)
		if err != nil {
			return nil, "", err
		}
		sum = sha256.Sum256(data)
		got = hex.EncodeToString(sum[:])
		if got == remote.Hash {
			return data, got, nil
		}
	}

	d.log.Warn("hash mismatch after pull",
		"path", remote.Path,
		"got", got,
		"want", remote.Hash,
		"use_delta", useDelta,
	)
	return nil, "", peerLogical(fmt.Sprintf("hash mismatch for %s: got %s want %s", remote.Path, got, remote.Hash))
}

// execDeleteLive removes a live local file for a winning remote tombstone.
// Caller must hold syncMu.
func (d *Daemon) execDeleteLive(re index.Entry) (bool, error) {
	if err := d.root.Remove(re.Path); err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("delete %s: %w", re.Path, err)
	}
	if !d.idx.SetIfWins(re) {
		return false, nil
	}
	d.appliesSinceSave++
	d.log.Info("deleted from peer", "path", re.Path)
	d.maybeSaveLocked()
	return true, nil
}

// execMetaOnly adopts remote mode/mtime for same-hash content, with rollback
// on partial failure. Caller must hold syncMu.
func (d *Daemon) execMetaOnly(local, re index.Entry) (bool, error) {
	mode := fileMode(re.Mode)
	prevMode := fileMode(local.Mode)
	prevMT := local.ModTime

	if !re.ModTime.IsZero() {
		if err := d.root.Chtimes(re.Path, re.ModTime, re.ModTime); err != nil {
			return false, fmt.Errorf("chtimes %s: %w", re.Path, err)
		}
	}
	if err := d.root.Chmod(re.Path, mode); err != nil {
		// Best-effort rollback of mtime applied above.
		if !re.ModTime.IsZero() && !prevMT.IsZero() {
			if rerr := d.root.Chtimes(re.Path, prevMT, prevMT); rerr != nil {
				d.log.Warn("rollback mtime after chmod failure", "path", re.Path, "err", rerr)
			}
		}
		if rerr := d.root.Chmod(re.Path, prevMode); rerr != nil {
			d.log.Warn("rollback mode after chmod failure", "path", re.Path, "err", rerr)
		}
		return false, fmt.Errorf("chmod %s: %w", re.Path, err)
	}

	// Store filesystem-observed mtime/mode so scan equality matches disk
	// (some FS truncate timestamps; Chtimes success ≠ Stat equality).
	actualMode, actualMT, err := diskMeta(d.root, re.Path, mode, re.ModTime, local.ModTime)
	if err != nil {
		// Ops succeeded; still commit with best-effort values rather than
		// rolling back a successful metadata write.
		d.log.Warn("stat after metadata adopt", "path", re.Path, "err", err)
	}
	local.UpdatedAt = re.UpdatedAt
	local.ModTime = actualMT
	local.Mode = actualMode
	d.idx.Set(local)
	d.appliesSinceSave++
	return true, nil
}

// commitContent writes verified file bytes and updates the index.
// Caller must hold syncMu. Hash must already match re.Hash.
func (d *Daemon) commitContent(re index.Entry, data []byte, got string) (bool, error) {
	// Final LWW check after transfer — index may have changed during the pull.
	if cur, ok := d.idx.Get(re.Path); ok && !index.Wins(cur, re) {
		return false, nil
	}

	mode := fileMode(re.Mode)
	if err := atomicfile.WriteFileRoot(d.root, re.Path, data, mode); err != nil {
		return false, err
	}
	// Always commit content after a successful write so scan cannot promote the
	// new bytes under a fresh local UpdatedAt (LWW inversion). Chtimes failure
	// is logged; disk mtime is re-Stat'd and same-hash adopt can retry later.
	if !re.ModTime.IsZero() {
		if err := d.root.Chtimes(re.Path, re.ModTime, re.ModTime); err != nil {
			d.log.Warn("chtimes after pull; committing content with disk mtime", "path", re.Path, "err", err)
		}
	}
	fallbackMT := re.ModTime
	if fallbackMT.IsZero() {
		if cur, ok := d.idx.Get(re.Path); ok && !cur.Deleted {
			fallbackMT = cur.ModTime
		}
	}
	actualMode, actualMT, err := diskMeta(d.root, re.Path, mode, re.ModTime, fallbackMT)
	if err != nil {
		d.log.Warn("stat after pull", "path", re.Path, "err", err)
	}

	entry := re
	entry.Hash = got
	entry.Size = int64(len(data))
	entry.Mode = actualMode
	entry.ModTime = actualMT
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = time.Now()
	}
	// Avoid SetIfWins: identical-content Wins false would skip the commit after a write.
	d.idx.Set(entry)
	d.appliesSinceSave++
	d.maybeSaveLocked()
	d.log.Info("pulled file", "path", re.Path, "size", len(data), "hash", got[:min(12, len(got))])
	return true, nil
}

// pullContent fetches remote bytes without holding syncMu.
func (d *Daemon) pullContent(ctx context.Context, conn net.Conn, remote index.ManifestEntry, useDelta bool) ([]byte, error) {
	if useDelta {
		data, err := d.pullDelta(ctx, conn, remote)
		if err != nil {
			if isTransportErr(err) {
				d.log.Debug("delta pull failed, aborting peer apply (transport)", "path", remote.Path, "err", err)
				return nil, err
			}
			d.log.Debug("delta pull failed, falling back to full", "path", remote.Path, "err", err)
			return d.pullFull(ctx, conn, remote)
		}
		return data, nil
	}
	return d.pullFull(ctx, conn, remote)
}

// diskMeta returns mode and mtime actually present on disk after a metadata or
// content apply. When remoteMT is zero, keeps fallbackMT (local/disk) instead of
// committing a zero index mtime. On Stat failure, returns modeWant and a non-zero
// mtime preference of remoteMT then fallbackMT.
func diskMeta(root *os.Root, rel string, modeWant os.FileMode, remoteMT, fallbackMT time.Time) (os.FileMode, time.Time, error) {
	fi, err := root.Stat(rel)
	if err != nil {
		mt := remoteMT
		if mt.IsZero() {
			mt = fallbackMT
		}
		return modeWant, mt, err
	}
	mode := fi.Mode().Perm()
	if mode == 0 {
		mode = modeWant
	}
	mt := fi.ModTime()
	if remoteMT.IsZero() {
		// Do not let a partial peer entry install zero; prefer prior known mtime
		// only when Stat returned zero (exotic); otherwise disk is authoritative.
		if mt.IsZero() && !fallbackMT.IsZero() {
			mt = fallbackMT
		}
	}
	return mode, mt, nil
}

// mtimesDiffer reports whether two mtimes should trigger metadata adopt.
// Equal times, or both non-zero and same Unix second (coarse FS truncation
// after Chtimes), are treated as not differing.
func mtimesDiffer(a, b time.Time) bool {
	if a.Equal(b) {
		return false
	}
	if !a.IsZero() && !b.IsZero() && a.Unix() == b.Unix() {
		return false
	}
	return true
}

// maybeSaveLocked persists the index every N applies to limit crash windows.
// Caller must hold syncMu.
func (d *Daemon) maybeSaveLocked() {
	const every = 8
	if d.appliesSinceSave >= every {
		if err := d.idx.Save(); err != nil {
			d.log.Error("mid-sync index save", "err", err)
			return
		}
		d.appliesSinceSave = 0
	}
}

func (d *Daemon) pullFull(ctx context.Context, conn net.Conn, remote index.ManifestEntry) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := proto.Encode(conn, proto.NewFileReq(remote.Path, remote.Hash)); err != nil {
		return nil, fmt.Errorf("%w: encode file_req: %w", errTransport, err)
	}
	msg, err := proto.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: decode file response: %w", errTransport, err)
	}
	if msg.Header.Type == proto.TypeError {
		return nil, peerLogical(msg.Header.Error)
	}
	if msg.Header.Type != proto.TypeFileData {
		return nil, fmt.Errorf("%w: expected file_data, got %q", errTransport, msg.Header.Type)
	}
	return msg.Payload, nil
}

func (d *Daemon) pullDelta(ctx context.Context, conn net.Conn, remote index.ManifestEntry) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	basis, fi, err := d.readFileLimited(remote.Path)
	if err != nil {
		// Local basis missing/unreadable is a logical failure for this path.
		return nil, peerLogical(err.Error())
	}
	_ = fi
	sig, err := delta.SignBytes(basis, d.cfg.BlockSize)
	if err != nil {
		return nil, peerLogical(err.Error())
	}
	sigRaw, err := delta.MarshalSignature(sig)
	if err != nil {
		return nil, peerLogical(err.Error())
	}
	if err := proto.Encode(conn, proto.NewDeltaReq(remote.Path, remote.Hash, d.cfg.BlockSize, sigRaw)); err != nil {
		return nil, fmt.Errorf("%w: encode delta_req: %w", errTransport, err)
	}
	msg, err := proto.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: decode delta response: %w", errTransport, err)
	}
	if msg.Header.Type == proto.TypeError {
		return nil, peerLogical(msg.Header.Error)
	}
	if msg.Header.Type != proto.TypeDelta {
		return nil, fmt.Errorf("%w: expected delta, got %q", errTransport, msg.Header.Type)
	}
	del, err := delta.UnmarshalDelta(msg.Payload)
	if err != nil {
		return nil, peerLogical(fmt.Sprintf("bad delta: %v", err))
	}
	out, err := delta.Apply(basis, del)
	if err != nil {
		return nil, peerLogical(fmt.Sprintf("apply delta: %v", err))
	}
	return out, nil
}
