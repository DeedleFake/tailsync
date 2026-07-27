package daemon

import (
	"context"
	"sync"
	"time"
)

// Membership is memory-only: hot set from successful Hello, status Online for
// bootstrap, and optional Config.Peers (test/override). Soft-fail demotes by
// address across all candidate sources with backoff; never a permanent ban —
// backoff expiry and successful Hello reintroduce.

const (
	// peerSoftBackoffBase is the first soft-fail backoff duration.
	peerSoftBackoffBase = 2 * time.Second
	// peerSoftBackoffMax caps exponential soft-fail backoff.
	peerSoftBackoffMax = 30 * time.Second
	// peerIdleTTL evicts hot-set entries not seen for this long (when not in backoff).
	peerIdleTTL = 24 * time.Hour
	// peerMaxEntries caps hot-set size; excess oldest lastSeen are dropped.
	peerMaxEntries = 256
)

// hotPeer is one remembered tailsync peer (nodeID → last good dial addr).
type hotPeer struct {
	addr         string
	lastSeen     time.Time
	backoffUntil time.Time
	failStreak   int
}

type addrBackoff struct {
	until  time.Time
	streak int
}

// peerMem holds the in-memory hot set and per-address soft-fail backoff.
// Guarded by mu.
type peerMem struct {
	mu          sync.Mutex
	byID        map[string]hotPeer     // nodeID → peer
	addrBackoff map[string]addrBackoff // dial addr → backoff (all sources)
}

func newPeerMem() *peerMem {
	return &peerMem{
		byID:        make(map[string]hotPeer),
		addrBackoff: make(map[string]addrBackoff),
	}
}

// remember records or refreshes a peer after a successful Hello. Clears backoff
// for that node and address.
func (p *peerMem) remember(nodeID, addr string) {
	if p == nil || nodeID == "" || addr == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	p.gcLocked(now)
	p.byID[nodeID] = hotPeer{
		addr:     addr,
		lastSeen: now,
	}
	delete(p.addrBackoff, addr)
}

// softFail demotes a peer by nodeID after dial/notify soft-fail. Never deletes.
func (p *peerMem) softFail(nodeID string) {
	if p == nil || nodeID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	h, ok := p.byID[nodeID]
	if !ok {
		return
	}
	h.failStreak++
	d := backoffDuration(h.failStreak)
	h.backoffUntil = time.Now().Add(d)
	p.byID[nodeID] = h
	if h.addr != "" {
		p.setAddrBackoffLocked(h.addr, h.failStreak)
	}
}

// softFailAddr records per-address backoff used when building candidates from
// the hot set and status discovery. Explicit Config.Peers still re-dial every
// batch (respectBackoff=false); if the same addr is also in hot/status it is
// filtered there until backoff expires. Also updates matching hot peers.
// Never permanently bans.
func (p *peerMem) softFailAddr(addr string) {
	if p == nil || addr == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	// Bump address-level streak.
	ab := p.addrBackoff[addr]
	ab.streak++
	ab.until = now.Add(backoffDuration(ab.streak))
	p.addrBackoff[addr] = ab

	for id, h := range p.byID {
		if h.addr != addr {
			continue
		}
		h.failStreak++
		h.backoffUntil = now.Add(backoffDuration(h.failStreak))
		p.byID[id] = h
	}
}

func backoffDuration(streak int) time.Duration {
	if streak < 1 {
		streak = 1
	}
	d := peerSoftBackoffBase
	for i := 1; i < streak && d < peerSoftBackoffMax; i++ {
		d *= 2
	}
	if d > peerSoftBackoffMax {
		d = peerSoftBackoffMax
	}
	return d
}

func (p *peerMem) setAddrBackoffLocked(addr string, streak int) {
	p.addrBackoff[addr] = addrBackoff{
		until:  time.Now().Add(backoffDuration(streak)),
		streak: streak,
	}
}

// inBackoff reports whether addr should be skipped this round.
func (p *peerMem) inBackoff(addr string) bool {
	if p == nil || addr == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	ab, ok := p.addrBackoff[addr]
	if !ok {
		return false
	}
	if ab.until.IsZero() || !time.Now().Before(ab.until) {
		return false
	}
	return true
}

// hotAddrs returns dial addresses from the hot set that are not in backoff.
func (p *peerMem) hotAddrs() []string {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	p.gcLocked(now)
	var out []string
	seen := make(map[string]struct{})
	for _, h := range p.byID {
		if h.addr == "" {
			continue
		}
		if !h.backoffUntil.IsZero() && now.Before(h.backoffUntil) {
			continue
		}
		if ab, ok := p.addrBackoff[h.addr]; ok && !ab.until.IsZero() && now.Before(ab.until) {
			continue
		}
		if _, ok := seen[h.addr]; ok {
			continue
		}
		seen[h.addr] = struct{}{}
		out = append(out, h.addr)
	}
	return out
}

// gcLocked evicts idle hot peers and expired address backoffs. Caller holds mu.
func (p *peerMem) gcLocked(now time.Time) {
	for id, h := range p.byID {
		if !h.backoffUntil.IsZero() && now.Before(h.backoffUntil) {
			continue // keep demoted entries until backoff ends
		}
		if !h.lastSeen.IsZero() && now.Sub(h.lastSeen) > peerIdleTTL {
			delete(p.byID, id)
		}
	}
	for addr, ab := range p.addrBackoff {
		if ab.until.IsZero() || !now.Before(ab.until) {
			// Expired: drop so next fail starts a fresh streak? Keep streak for
			// exponential if we soft-fail again soon — drop entire entry when
			// expired so reintroduce is clean.
			delete(p.addrBackoff, addr)
		}
	}
	// Cap map size by dropping oldest lastSeen (not in active backoff).
	for len(p.byID) > peerMaxEntries {
		var oldestID string
		var oldestTime time.Time
		first := true
		for id, h := range p.byID {
			if !h.backoffUntil.IsZero() && now.Before(h.backoffUntil) {
				continue
			}
			if first || h.lastSeen.Before(oldestTime) {
				first = false
				oldestID = id
				oldestTime = h.lastSeen
			}
		}
		if oldestID == "" {
			break
		}
		delete(p.byID, oldestID)
	}
}

// snapshotIDs returns a copy of known nodeIDs (for tests).
func (p *peerMem) snapshotIDs() []string {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.byID))
	for id := range p.byID {
		out = append(out, id)
	}
	return out
}

// candidateAddrs builds the dial list for notify or pull:
// Config.Peers (if set) ∪ hot set ∪ status Online peers.
// Soft-fail backoff filters hot and status addresses; explicit Config.Peers is a
// test/override pin and is always included (still soft-fails for logging, but
// re-dial every batch so local tests with dead peers stay deterministic).
// Status re-enters after backoff expires (never a permanent ban).
func (d *Daemon) candidateAddrs(ctx context.Context) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(addr string, respectBackoff bool) {
		if addr == "" {
			return
		}
		if _, ok := seen[addr]; ok {
			return
		}
		if respectBackoff && d.peers != nil && d.peers.inBackoff(addr) {
			return
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}

	// Explicit pin: always dial (tests / operator override).
	for _, a := range d.cfg.Peers {
		add(a, false)
	}
	for _, a := range d.peers.hotAddrs() {
		add(a, true)
	}

	// Status bootstrap when Peers empty (keep -peers deterministic for tests).
	if len(d.cfg.Peers) == 0 {
		statusPeers, err := d.listStatusPeers(ctx)
		if err != nil {
			d.log.Debug("list status peers", "err", err)
		} else {
			for _, a := range statusPeers {
				add(a, true)
			}
		}
	}
	return out
}

// listStatusPeers returns online discovery peers from Tailscale status only
// (ignores Config.Peers). Used for membership bootstrap when Peers is empty.
func (d *Daemon) listStatusPeers(ctx context.Context) ([]string, error) {
	if len(d.cfg.Peers) == 0 {
		return d.listPeers(ctx)
	}
	return nil, nil
}
