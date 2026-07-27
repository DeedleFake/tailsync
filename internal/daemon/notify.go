package daemon

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/proto"
)

// contentKey identifies a path version for notify dedupe (storm prevention).
// Keyed by path + hash + deleted + updated_at unix nano so the same logical
// content is not re-notified in a loop.
//
// Invariant: any meta-only change that should re-notify (mode/mtime adopt)
// must bump UpdatedAt so the key differs. scan stamps and peer LWW already do;
// callers that forge entries must advance UpdatedAt when meta changes.
type contentKey struct {
	path      string
	hash      string
	deleted   bool
	updatedAt int64
}

func contentKeyFrom(e index.Entry) contentKey {
	return contentKey{
		path:      e.Path,
		hash:      e.Hash,
		deleted:   e.Deleted,
		updatedAt: e.UpdatedAt.UnixNano(),
	}
}

// notifyTracker records content keys we successfully advertised so infect-and-die
// and local re-notify do not storm. Keys are claimed in-flight when a fan-out is
// scheduled and marked only after at least one successful notify (or released if
// every dial fails), so peers that appear later can still get a prompt re-notify.
//
// Confirmation semantics: the first successful notify for a claimed batch marks
// the entire claim set as seen — not a full fan-out to every candidate. Peers
// that miss this notify still converge via SyncInterval pull (correctness
// backstop). Empty mesh / all soft-fails release claims for a later retry.
type notifyTracker struct {
	mu       sync.Mutex
	seen     map[contentKey]time.Time
	inflight map[contentKey]struct{}
}

func newNotifyTracker() *notifyTracker {
	return &notifyTracker{
		seen:     make(map[contentKey]time.Time),
		inflight: make(map[contentKey]struct{}),
	}
}

const notifySeenTTL = 10 * time.Minute

// gcLocked drops expired seen entries. Caller must hold t.mu.
func (t *notifyTracker) gcLocked(now time.Time) {
	for k, at := range t.seen {
		if now.Sub(at) > notifySeenTTL {
			delete(t.seen, k)
		}
	}
}

// freeLocked reports whether k is neither successfully seen nor in-flight.
// Caller must hold t.mu (and should have gc'd with now).
func (t *notifyTracker) freeLocked(k contentKey, now time.Time) bool {
	if at, ok := t.seen[k]; ok && now.Sub(at) <= notifySeenTTL {
		return false
	}
	if _, ok := t.inflight[k]; ok {
		return false
	}
	return true
}

// pending is test-only: returns hints not yet confirmed and not currently
// claimed (read-only; does not claim). Production paths use claim only.
func (t *notifyTracker) pending(hints []index.ManifestEntry) []index.ManifestEntry {
	if t == nil {
		return hints
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.gcLocked(now)
	var out []index.ManifestEntry
	for _, h := range hints {
		if t.freeLocked(contentKeyFrom(h), now) {
			out = append(out, h)
		}
	}
	return out
}

// claim returns hints that are neither seen nor already in-flight, and marks
// them in-flight so a concurrent scheduleNotify cannot double-fan-out the same
// keys. Caller must confirm (mark) or release after the batch finishes.
func (t *notifyTracker) claim(hints []index.ManifestEntry) []index.ManifestEntry {
	if t == nil {
		return hints
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.gcLocked(now)
	var out []index.ManifestEntry
	for _, h := range hints {
		k := contentKeyFrom(h)
		if !t.freeLocked(k, now) {
			continue
		}
		t.inflight[k] = struct{}{}
		out = append(out, h)
	}
	return out
}

// mark records keys as successfully notified and clears in-flight claims.
//
// Called after the first successful notify for a claimed batch: marks the whole
// claim set (not per-peer). Remaining candidates may miss this advertisement;
// SyncInterval pull is the correctness backstop.
func (t *notifyTracker) mark(hints []index.ManifestEntry) {
	if t == nil || len(hints) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.gcLocked(now)
	for _, h := range hints {
		k := contentKeyFrom(h)
		t.seen[k] = now
		delete(t.inflight, k)
	}
}

// release drops in-flight claims without marking seen (batch had no success).
func (t *notifyTracker) release(hints []index.ManifestEntry) {
	if t == nil || len(hints) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, h := range hints {
		delete(t.inflight, contentKeyFrom(h))
	}
}

// scheduleNotify fans out best-effort notifies to candidates. Does not block
// the caller on peer dials (fire-and-forget). Hints may be empty (wake-only).
// Returns true when at least one notify goroutine was scheduled.
//
// Content keys are claimed while a fan-out is in flight (prevents concurrent
// double fan-out). The first successful Hello+Notify marks the whole claim set
// as seen (any success ≡ done advertising this content id for this batch — not
// full fan-out). If every dial fails, claims are released so peers that appear
// later can still receive a re-notify. Correctness does not depend on notify
// delivery; SyncInterval pull is the backstop.
func (d *Daemon) scheduleNotify(ctx context.Context, hints []index.ManifestEntry) bool {
	if ctx.Err() != nil {
		return false
	}
	addrs := d.candidateAddrs(ctx)
	if len(addrs) == 0 {
		// Do not claim/mark keys: no peer could have been reached.
		return false
	}

	var toSend []index.ManifestEntry
	if len(hints) > 0 {
		toSend = d.notifySeen.claim(hints)
		// When all keys already confirmed or in-flight, skip to avoid storms.
		if len(toSend) == 0 {
			return false
		}
	}
	// else wake-only (toSend nil)

	d.log.Debug("notify fan-out", "peers", len(addrs), "hints", len(toSend))

	// Cap concurrent notify dials; still no barrier on the main loop.
	sem := make(chan struct{}, maxParallelNotifies)
	var (
		batchWG sync.WaitGroup
		anyOK   atomic.Bool
	)
	// Snapshot for goroutines; do not Wait here — main loop must not stall.
	// notifyWG is joined on shutdown so root/listener stay valid.
	for _, addr := range addrs {
		batchWG.Add(1)
		d.notifyWG.Go(func() {
			defer batchWG.Done()
			if err := ctx.Err(); err != nil {
				return
			}
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			if err := d.notifyPeer(ctx, addr, toSend); err != nil {
				if isDialSoftFail(err) {
					d.log.Debug("notify peer", "addr", addr, "err", err)
					d.peers.softFailAddr(addr)
					return
				}
				d.log.Debug("notify peer", "addr", addr, "err", err)
				return
			}
			// First success marks the whole claim set (not full fan-out).
			anyOK.Store(true)
			if len(toSend) > 0 {
				d.notifySeen.mark(toSend)
			}
		})
	}
	if len(toSend) > 0 {
		// Release unconfirmed claims after the batch so a later schedule can retry.
		d.notifyWG.Go(func() {
			batchWG.Wait()
			if !anyOK.Load() {
				d.notifySeen.release(toSend)
			}
		})
	}
	return true
}

// notifyPeer dials addr, Hellos, sends TypeNotify, then closes. Soft-fail on
// unreachable peers. Learns remote nodeID into the hot set on HelloOK.
func (d *Daemon) notifyPeer(ctx context.Context, addr string, hints []index.ManifestEntry) error {
	// Short deadline for notify probes (not full file transfer).
	conn, remoteNode, err := d.dialHello(ctx, addr, func(c net.Conn) {
		deadline := time.Now().Add(d.cfg.DialTimeout + 5*time.Second)
		if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
			deadline = dl
		}
		_ = c.SetDeadline(deadline)
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := proto.Encode(conn, proto.NewNotify(d.nodeID, d.cfg.Port, hints)); err != nil {
		return err
	}
	// Optional NotifyOK; ignore errors (peer may just close).
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ack, err := proto.Decode(conn)
	if err == nil && ack.Header.Type == proto.TypeNotifyOK {
		d.log.Debug("notify ack", "addr", addr, "remote_node", remoteNode)
	}
	return nil
}

// onNotify handles an inbound TypeNotify: learn membership, content-dedupe,
// schedule a pull. Never commits index state from notify hints.
// remotePort is the peer's advertised listen port from Hello/Notify (0 = use cfg.Port).
func (d *Daemon) onNotify(remoteNode, remoteAddr string, remotePort int, hints []index.ManifestEntry) {
	if remoteNode != "" && remoteNode != d.nodeID {
		if dial := dialBackAddr(remoteAddr, remotePort, d.cfg.Port); dial != "" {
			d.peers.remember(remoteNode, dial)
		}
	}

	// If no hinted version would win LWW (including meta-only UpdatedAt/mode/mtime),
	// ignore (no pull, no re-notify).
	if len(hints) > 0 && d.alreadyHaveAll(hints) {
		d.log.Debug("notify ignored; already have content", "remote_node", remoteNode, "hints", len(hints))
		d.notifySeen.mark(hints)
		return
	}

	d.requestPull()
}

// dialBackAddr builds host:port for re-dialing a peer from an accepted connection.
// advertisedPort is the peer's listen port from Hello (preferred); if zero, uses
// localDefault only when it is the shared mesh port convention.
func dialBackAddr(remoteTCP string, advertisedPort, localDefault int) string {
	host, _, err := net.SplitHostPort(remoteTCP)
	if err != nil || host == "" {
		return ""
	}
	port := advertisedPort
	if port <= 0 {
		port = localDefault
	}
	if port <= 0 {
		return ""
	}
	return net.JoinHostPort(host, fmt.Sprintf("%d", port))
}

// alreadyHaveAll reports whether every hint is fully covered by the local index
// under the same LWW total order used by apply: if any remote hint would win
// (content, tombstone, or meta-only), a pull is still needed.
func (d *Daemon) alreadyHaveAll(hints []index.ManifestEntry) bool {
	for _, h := range hints {
		local, ok := d.idx.Get(h.Path)
		if !ok {
			return false
		}
		// index.Wins(local, remote): true when remote should replace local.
		if index.Wins(local, h) {
			return false
		}
	}
	return true
}

// requestPull coalesces a pull wake-up for the main loop (non-blocking).
func (d *Daemon) requestPull() {
	if d == nil || d.needPull == nil {
		return
	}
	d.needPull.request()
}
