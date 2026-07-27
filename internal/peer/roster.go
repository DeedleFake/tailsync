package peer

import (
	"fmt"
	"sync"
	"time"
)

// SessionInfo is a snapshot of a connected peer (safe for callers without locks).
type SessionInfo struct {
	NodeID  string
	Addr    string // dial-back host:port when known
	Dialer  bool   // true if we initiated the connection
	Healthy bool
	Since   time.Time
	Remote  string // raw remote UDP address
}

// installResult is the outcome of trying to install a session in the roster.
type installResult int

const (
	installAccepted installResult = iota
	installRejected               // keep existing healthy; discard new
	installReplaced               // new replaced unhealthy or lost-race existing
)

// Roster maps nodeID → at most one live Session. Concurrent dials and accepts
// are coordinated with deterministic node-ID race rules.
type Roster struct {
	mu       sync.Mutex
	sessions map[string]*Session // nodeID → session
}

// NewRoster returns an empty roster.
func NewRoster() *Roster {
	return &Roster{sessions: make(map[string]*Session)}
}

// Snapshot returns a copy of connected session metadata.
func (r *Roster) Snapshot() []SessionInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]SessionInfo, 0, len(r.sessions))
	for _, s := range r.sessions {
		if s == nil {
			continue
		}
		out = append(out, s.info())
	}
	return out
}

// Get returns the live session for nodeID, or nil.
func (r *Roster) Get(nodeID string) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.sessions[nodeID]
	if s == nil || s.Closed() {
		return nil
	}
	return s
}

// Len returns the number of tracked sessions (may include closing).
func (r *Roster) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions)
}

// ConnectedAddrs returns dial addresses of healthy sessions.
func (r *Roster) ConnectedAddrs() map[string]struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]struct{})
	for _, s := range r.sessions {
		if s == nil || s.Closed() || !s.Healthy() {
			continue
		}
		if a := s.Addr(); a != "" {
			out[a] = struct{}{}
		}
	}
	return out
}

// ConnectedNodeIDs returns node IDs with a non-closed session.
func (r *Roster) ConnectedNodeIDs() map[string]struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]struct{}, len(r.sessions))
	for id, s := range r.sessions {
		if s != nil && !s.Closed() {
			out[id] = struct{}{}
		}
	}
	return out
}

// Install attempts to register sess for its NodeID.
//
// Rules:
//   - No existing → accept
//   - Existing unhealthy (or closed) → replace (discard old)
//   - Existing healthy → reject new (AlreadyConnected), unless simultaneous-dial
//     race: keep the connection initiated by the lexicographically smaller node ID
//
// On replace, the previous session is Closed asynchronously by the caller via
// the returned old session pointer.
func (r *Roster) Install(sess *Session) (result installResult, old *Session) {
	if sess == nil || sess.NodeID() == "" {
		return installRejected, nil
	}
	id := sess.NodeID()
	r.mu.Lock()
	defer r.mu.Unlock()

	cur := r.sessions[id]
	if cur == nil || cur.Closed() {
		r.sessions[id] = sess
		if cur != nil {
			return installReplaced, cur
		}
		return installAccepted, nil
	}
	if !cur.Healthy() {
		r.sessions[id] = sess
		return installReplaced, cur
	}

	// Existing healthy: simultaneous-dial race or redundant conn.
	if preferNewSession(sess, cur) {
		r.sessions[id] = sess
		return installReplaced, cur
	}
	return installRejected, nil
}

// preferNewSession reports whether newS should replace cur when both claim the
// same peer. Prefer the connection dialed by the smaller node ID (consistent on
// both sides). If dialer roles match, keep existing.
func preferNewSession(newS, cur *Session) bool {
	// Local and remote IDs are the same pair on both ends.
	local := newS.localID
	remote := newS.NodeID()
	if local == "" || remote == "" {
		return false
	}
	preferredDialer := min(local, remote)
	newDialer := dialerNodeID(newS)
	curDialer := dialerNodeID(cur)
	if newDialer == preferredDialer && curDialer != preferredDialer {
		return true
	}
	return false
}

func dialerNodeID(s *Session) string {
	if s.IsDialer() {
		return s.localID
	}
	return s.NodeID()
}

// Remove drops the session for nodeID if it is still the registered instance.
func (r *Roster) Remove(nodeID string, sess *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.sessions[nodeID]; ok && (sess == nil || cur == sess) {
		delete(r.sessions, nodeID)
	}
}

// RemoveIfCurrent removes sess only if it is still the mapped session.
func (r *Roster) RemoveIfCurrent(sess *Session) bool {
	if sess == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id := sess.NodeID()
	if r.sessions[id] == sess {
		delete(r.sessions, id)
		return true
	}
	return false
}

// CloseAll closes and clears every session.
func (r *Roster) CloseAll() {
	r.mu.Lock()
	all := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		all = append(all, s)
	}
	r.sessions = make(map[string]*Session)
	r.mu.Unlock()
	for _, s := range all {
		if s != nil {
			s.Close()
		}
	}
}

// String for debugging.
func (r *Roster) String() string {
	snap := r.Snapshot()
	return fmt.Sprintf("roster(%d peers)", len(snap))
}
