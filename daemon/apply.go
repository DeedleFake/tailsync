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
	"syscall"
	"time"

	"deedles.dev/tailsync/internal/atomicfile"
	"deedles.dev/tailsync/internal/delta"
	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/peer"
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

// pullPeers pulls manifests/content from connected peer sessions.
// Content streams are capped by pullSem (PullStreamConcurrency). Disk and
// index commits remain serialized via syncMu. Discovery is separate; this only
// uses already-established sessions. Notify fan-out does not wait on this batch.
func (d *Daemon) pullPeers(ctx context.Context) {
	if d.mesh == nil {
		return
	}
	snap := d.mesh.Snapshot()
	if len(snap) == 0 {
		return
	}
	d.log.Debug("pull peers", "count", len(snap))

	var wg sync.WaitGroup
	for _, info := range snap {
		if err := ctx.Err(); err != nil {
			break
		}
		if !info.Healthy {
			continue
		}
		sess := d.mesh.Session(info.NodeID)
		if sess == nil || !sess.Healthy() {
			continue
		}
		wg.Go(func() {
			if err := d.pullFromSession(ctx, sess); err != nil {
				d.log.Warn("pull peer", "peer", info.NodeID, "addr", info.Addr, "err", err)
			}
		})
	}
	wg.Wait()
}

// pullFromSession requests the peer's manifest on a dedicated stream, then
// applies each entry (opening a stream per file for content). Newly applied
// content is optionally re-notified (infect-and-die).
func (d *Daemon) pullFromSession(ctx context.Context, sess *peer.Session) error {
	if sess == nil || sess.Closed() {
		return net.ErrClosed
	}
	entries, err := d.fetchManifest(ctx, sess)
	if err != nil {
		d.invalidateSessionOnTransport(sess, err)
		return err
	}

	changed := false
	var acquired []index.ManifestEntry
	var transportErr error
	for _, remote := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		did, err := d.applyRemote(ctx, sess, remote)
		if err != nil {
			if isTransportErr(err) {
				transportErr = err
				d.log.Warn("peer transport error, aborting pull", "peer", sess.NodeID(), "err", err)
				break
			}
			d.log.Warn("apply remote entry", "path", remote.Path, "err", err)
			continue
		}
		if did {
			changed = true
			if e, ok := d.idx.Get(remote.Path); ok {
				acquired = append(acquired, e)
			}
		}
	}
	if changed {
		d.syncMu.Lock()
		if err := d.idx.Save(); err != nil {
			d.syncMu.Unlock()
			return fmt.Errorf("save index: %w", err)
		}
		d.appliesSinceSave = 0
		d.syncMu.Unlock()
		if len(acquired) > 0 {
			d.scheduleNotify(ctx, acquired)
		}
	}
	if transportErr != nil {
		d.invalidateSessionOnTransport(sess, transportErr)
		return transportErr
	}
	d.log.Info("pulled peer", "peer", sess.NodeID(), "addr", sess.Addr(), "manifest_entries", len(entries))
	return nil
}

// invalidateSessionOnHardIO closes the session on non-soft I/O failures so
// discovery can replace it. Soft caller cancel/timeout is ignored (mirrors
// OpenStream's IsSoftStreamErr policy). Used by pull and notify paths.
func (d *Daemon) invalidateSessionOnHardIO(sess *peer.Session, err error) {
	if sess == nil || err == nil || peer.IsSoftStreamErr(err) {
		return
	}
	sess.Invalidate()
}

// invalidateSessionOnTransport closes the session on hard framing/connection
// failures (errTransport / EOF). Soft cancel/timeout is ignored.
func (d *Daemon) invalidateSessionOnTransport(sess *peer.Session, err error) {
	if !isTransportErr(err) {
		return
	}
	d.invalidateSessionOnHardIO(sess, err)
}

func (d *Daemon) fetchManifest(ctx context.Context, sess *peer.Session) ([]index.ManifestEntry, error) {
	conn, err := sess.OpenStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("open manifest stream: %w", err)
	}
	defer conn.Close()
	d.setConnDeadline(conn, ctx)
	if err := proto.Encode(conn, proto.NewManifestReq()); err != nil {
		return nil, fmt.Errorf("%w: encode manifest_req: %w", errTransport, err)
	}
	man, err := proto.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: manifest: %w", errTransport, err)
	}
	if man.Header.Type == proto.TypeError {
		return nil, peerLogical(man.Header.Error)
	}
	if man.Header.Type != proto.TypeManifest {
		return nil, fmt.Errorf("%w: expected manifest, got %q", errTransport, man.Header.Type)
	}
	return man.Header.Entries, nil
}

// applyRemote reconciles one remote manifest entry via the peer session.
//
// Pattern: decide under syncMu → release for network transfer → re-lock,
// re-check LWW, then atomic write + index commit. Other peer applies may run
// during the unlocked pull; commits re-take syncMu. Reconcile may run on the
// main loop concurrently with pull (serialized with apply commits via syncMu).
func (d *Daemon) applyRemote(ctx context.Context, sess *peer.Session, remote index.ManifestEntry) (bool, error) {
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

		data, got, err := d.pullAndVerify(ctx, sess, remote, useDelta)
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
func (d *Daemon) pullAndVerify(ctx context.Context, sess *peer.Session, remote index.ManifestEntry, useDelta bool) ([]byte, string, error) {
	data, err := d.pullContent(ctx, sess, remote, useDelta)
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
		data, err = d.pullFull(ctx, sess, remote)
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

// errDial aliases peer.ErrDial for tests and soft-fail classification.
var errDial = peer.ErrDial

// isDialSoftFail reports dial-phase failures that are expected when discovery
// includes nodes not running tailsync (timeout, refused, unreachable).
func isDialSoftFail(err error) bool {
	if err == nil || !errors.Is(err, errDial) {
		return false
	}
	return isSoftDialNetworkErr(err)
}

func isSoftDialNetworkErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return false
}

// pullContent fetches remote bytes without holding syncMu (one stream per file).
func (d *Daemon) pullContent(ctx context.Context, sess *peer.Session, remote index.ManifestEntry, useDelta bool) ([]byte, error) {
	if useDelta {
		data, err := d.pullDelta(ctx, sess, remote)
		if err != nil {
			if isTransportErr(err) {
				d.log.Debug("delta pull failed, aborting peer apply (transport)", "path", remote.Path, "err", err)
				return nil, err
			}
			d.log.Debug("delta pull failed, falling back to full", "path", remote.Path, "err", err)
			return d.pullFull(ctx, sess, remote)
		}
		return data, nil
	}
	return d.pullFull(ctx, sess, remote)
}

// acquirePullSlot blocks until a global pull stream slot is available.
func (d *Daemon) acquirePullSlot(ctx context.Context) error {
	if d.pullSem == nil {
		return nil
	}
	select {
	case d.pullSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Daemon) releasePullSlot() {
	if d.pullSem == nil {
		return
	}
	select {
	case <-d.pullSem:
	default:
	}
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

func (d *Daemon) pullFull(ctx context.Context, sess *peer.Session, remote index.ManifestEntry) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.acquirePullSlot(ctx); err != nil {
		return nil, err
	}
	defer d.releasePullSlot()

	conn, err := sess.OpenStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: open file stream: %w", errTransport, err)
	}
	defer conn.Close()
	d.setConnDeadline(conn, ctx)

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

func (d *Daemon) pullDelta(ctx context.Context, sess *peer.Session, remote index.ManifestEntry) ([]byte, error) {
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

	if err := d.acquirePullSlot(ctx); err != nil {
		return nil, err
	}
	defer d.releasePullSlot()

	conn, err := sess.OpenStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: open delta stream: %w", errTransport, err)
	}
	defer conn.Close()
	d.setConnDeadline(conn, ctx)

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
