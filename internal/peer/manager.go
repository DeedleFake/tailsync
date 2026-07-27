// Package peer implements persistent QUIC peer connections: shared UDP
// transports, connection-scoped Hello, roster coordination, and discovery.
package peer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// Manager owns the endpoint, roster, discovery, and connection lifecycle.
type Manager struct {
	cfg      Config
	endpoint *Endpoint
	roster   *Roster
	disc     *Discovery
	log      *slog.Logger

	// ResolveAddr optionally maps host:port to a concrete net.Addr (tsnet).
	// When nil, Endpoint.Dial parses the address directly.
	ResolveAddr func(ctx context.Context, addr string) (net.Addr, error)

	// VerifyRemoteID optionally validates a Hello NodeID against the remote
	// UDP address (e.g. Tailscale WhoIs). When set, mismatched claims are
	// rejected. Plain/local tests leave this nil.
	VerifyRemoteID func(ctx context.Context, remoteAddr, claimedNodeID string) error

	// OnStream is invoked for each inbound application stream after the first
	// message has been decoded (Ping is handled internally).
	OnStream StreamHandler

	// OnPeerUp is called after a session is installed (inbound or outbound).
	OnPeerUp func(s *Session)
	// OnPeerDown is called when a session is removed from the roster.
	OnPeerDown func(nodeID, addr string)

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Config configures the peer manager.
type Config struct {
	NodeID string
	Port   int

	ServerTLS *tls.Config
	ClientTLS *tls.Config
	Logger    *slog.Logger

	DiscoveryConcurrency int
	DiscoveryInterval    time.Duration
	DialTimeout          time.Duration
	HandshakeTimeout     time.Duration
	HeartbeatInterval    time.Duration
	HeartbeatTimeout     time.Duration

	// Candidates returns dial targets (status Online, pins). Required for discovery.
	Candidates func(ctx context.Context) []Candidate
}

// NewManager builds a manager (does not start accept/discovery).
func NewManager(cfg Config) (*Manager, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node id is required")
	}
	if cfg.Port <= 0 {
		return nil, fmt.Errorf("port is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultHandshakeTimeout
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 20 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 10 * time.Second
	}

	ep, err := NewEndpoint(cfg.ServerTLS, cfg.ClientTLS, cfg.Logger)
	if err != nil {
		return nil, err
	}
	r := NewRoster()
	m := &Manager{
		cfg:      cfg,
		endpoint: ep,
		roster:   r,
		log:      cfg.Logger,
	}
	m.disc = newDiscovery(r, DiscoveryConfig{
		Concurrency:      cfg.DiscoveryConcurrency,
		Interval:         cfg.DiscoveryInterval,
		DialTimeout:      cfg.DialTimeout,
		HandshakeTimeout: cfg.HandshakeTimeout,
		Candidates:       cfg.Candidates,
		DialPeer:         m.dialAndInstall,
		Log:              cfg.Logger,
	})
	return m, nil
}

// Endpoint returns the shared transport endpoint.
func (m *Manager) Endpoint() *Endpoint { return m.endpoint }

// Roster returns the session roster.
func (m *Manager) Roster() *Roster { return m.roster }

// Discovery returns the discovery service (for tests / Kick).
func (m *Manager) Discovery() *Discovery { return m.disc }

// AddPacketConn registers a shared UDP socket for listen+dial.
func (m *Manager) AddPacketConn(pc net.PacketConn) error {
	return m.endpoint.AddPacketConn(pc)
}

// Start begins accept and discovery loops. Call after adding PacketConns.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	if m.cancel != nil {
		m.mu.Unlock()
		return
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	runCtx := m.ctx
	m.mu.Unlock()

	m.wg.Go(func() { m.acceptLoop(runCtx) })
	m.wg.Go(func() { m.disc.Run(runCtx) })
}

// KickDiscovery requests a non-blocking discovery pass (coalesced).
func (m *Manager) KickDiscovery() {
	if m.disc != nil {
		m.disc.Kick()
	}
}

// Snapshot returns connected peers.
func (m *Manager) Snapshot() []SessionInfo {
	return m.roster.Snapshot()
}

// Session returns the session for nodeID, if any.
func (m *Manager) Session(nodeID string) *Session {
	return m.roster.Get(nodeID)
}

// Close stops loops, closes sessions, and shuts down the endpoint.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Unlock()
	m.roster.CloseAll()
	err := m.endpoint.Close()
	m.wg.Wait()
	return err
}

func (m *Manager) acceptLoop(ctx context.Context) {
	for {
		qconn, err := m.endpoint.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			m.log.Debug("accept quic", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		m.wg.Go(func() {
			m.handleInbound(ctx, qconn)
		})
	}
}

func (m *Manager) handleInbound(ctx context.Context, qconn *quic.Conn) {
	remote := ""
	if ra := qconn.RemoteAddr(); ra != nil {
		remote = ra.String()
	}

	// decide is advisory (TOCTOU); Install is authoritative for roster races.
	decide := func(remoteID string) bool {
		cur := m.roster.Get(remoteID)
		if cur == nil || cur.Closed() {
			return true
		}
		if !cur.Healthy() {
			return true
		}
		// Healthy existing: allow only if this inbound is preferred (remote is
		// the preferred dialer — they dialed us and remoteID < localID).
		return PreferredDialer(m.cfg.NodeID, remoteID) == remoteID
	}

	verify := func(ctx context.Context, remoteID string) error {
		return m.verifyRemote(ctx, remote, remoteID)
	}
	remoteID, remotePort, accepted, err := inboundHandshake(
		ctx, qconn, m.cfg.NodeID, m.cfg.Port, m.cfg.HandshakeTimeout, decide, verify,
	)
	if err != nil {
		// Identity mismatch and handshake failures both land here (before HelloOK).
		m.log.Debug("inbound handshake", "remote", remote, "err", err)
		_ = qconn.CloseWithError(1, "handshake failed")
		return
	}
	if !accepted {
		m.log.Debug("inbound rejected", "remote", remote, "remote_node", remoteID)
		_ = qconn.CloseWithError(0, "already connected")
		return
	}

	addr := dialBackAddr(remote, remotePort, m.cfg.Port)
	// serveCancel is ready before Install so concurrent Close is safe.
	sess := newSession(SessionConfig{
		NodeID:  remoteID,
		LocalID: m.cfg.NodeID,
		Addr:    addr,
		Dialer:  false,
		Conn:    qconn,
		Log:     m.log,
		OnClose: m.onSessionClose,
	})

	if err := m.installAndActivate(ctx, sess); err != nil {
		return
	}
	m.log.Info("peer connected", "peer", remoteID, "addr", addr, "dir", "inbound")
	if m.OnPeerUp != nil {
		m.OnPeerUp(sess)
	}
}

func (m *Manager) dialAndInstall(ctx context.Context, c Candidate) error {
	// Skip if already connected to this node or addr.
	if c.NodeID != "" {
		if s := m.roster.Get(c.NodeID); s != nil && s.Healthy() {
			return nil
		}
	}
	if addrs := m.roster.ConnectedAddrs(); addrs != nil {
		if _, ok := addrs[c.Addr]; ok {
			return nil
		}
	}

	qconn, err := m.dialQUIC(ctx, c.Addr)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDial, err)
	}
	// ownConn tracks whether we still must close qconn on failure paths.
	// After successful Install, the session owns the connection.
	ownConn := true
	defer func() {
		if ownConn {
			_ = qconn.CloseWithError(1, "dial failed")
		}
	}()

	remoteID, remotePort, err := outboundHandshake(ctx, qconn, m.cfg.NodeID, m.cfg.Port, m.cfg.HandshakeTimeout)
	if err != nil {
		if errors.Is(err, errAlreadyConnected) {
			// Peer already has a session with us; park this addr so we do not thrash.
			m.log.Debug("peer already connected", "addr", c.Addr, "remote_node", remoteID)
			return fmt.Errorf("%w: %w", ErrDial, errAlreadyConnected)
		}
		return err
	}
	if remoteID == m.cfg.NodeID {
		return fmt.Errorf("connected to self")
	}

	// Verify against the address we dialed (host:port or resolved).
	verifyAddr := c.Addr
	if ra := qconn.RemoteAddr(); ra != nil {
		verifyAddr = ra.String()
	}
	if err := m.verifyRemote(ctx, verifyAddr, remoteID); err != nil {
		return fmt.Errorf("outbound identity: %w", err)
	}

	addr := c.Addr
	if remotePort > 0 {
		if host, _, err := net.SplitHostPort(c.Addr); err == nil {
			addr = net.JoinHostPort(host, fmt.Sprintf("%d", remotePort))
		}
	}

	sess := newSession(SessionConfig{
		NodeID:  remoteID,
		LocalID: m.cfg.NodeID,
		Addr:    addr,
		Dialer:  true,
		Conn:    qconn,
		Log:     m.log,
		OnClose: m.onSessionClose,
	})

	// installAndActivate closes sess (and thus qconn) on reject/race; clear ownership.
	ownConn = false
	if err := m.installAndActivate(ctx, sess); err != nil {
		return fmt.Errorf("%w: %w", ErrDial, err)
	}
	m.disc.ClearBackoff(c.Addr)
	m.log.Info("peer connected", "peer", remoteID, "addr", addr, "dir", "outbound")
	if m.OnPeerUp != nil {
		m.OnPeerUp(sess)
	}
	return nil
}

// installAndActivate installs sess into the roster, tears down any replaced
// session, and starts serve/heartbeat only if sess is still the roster entry.
// On reject or lost race, sess is closed without roster onClose.
func (m *Manager) installAndActivate(ctx context.Context, sess *Session) error {
	result, old := m.roster.Install(sess)
	if result == installRejected {
		// Not in roster: tear down without onClose (would remove wrong entry).
		sess.setOnClose(nil)
		sess.Close()
		return errAlreadyConnected
	}
	if old != nil {
		// Install already swapped the map entry; discard previous session.
		old.setOnClose(nil)
		old.Close()
		if m.OnPeerDown != nil {
			m.OnPeerDown(old.NodeID(), old.Addr())
		}
	}
	if !m.activateIfCurrent(ctx, sess) {
		return fmt.Errorf("session replaced during activate")
	}
	return nil
}

func (m *Manager) verifyRemote(ctx context.Context, remoteAddr, claimedNodeID string) error {
	if m.VerifyRemoteID == nil || claimedNodeID == "" {
		return nil
	}
	return m.VerifyRemoteID(ctx, remoteAddr, claimedNodeID)
}

func (m *Manager) dialQUIC(ctx context.Context, addr string) (*quic.Conn, error) {
	dctx, cancel := context.WithTimeout(ctx, m.cfg.DialTimeout)
	defer cancel()
	if m.ResolveAddr != nil {
		raddr, err := m.ResolveAddr(dctx, addr)
		if err != nil {
			return nil, err
		}
		return m.endpoint.DialAddr(dctx, raddr)
	}
	return m.endpoint.Dial(dctx, addr)
}

// activateIfCurrent starts serve/heartbeat only if sess is still the roster
// entry (another install may have won the race after our Install returned).
// Re-checks after starting loops so a concurrent Install that replaced/closed
// sess cannot leave installAndActivate reporting success for a dead session.
func (m *Manager) activateIfCurrent(ctx context.Context, sess *Session) bool {
	if !m.stillCurrent(sess) {
		m.discardSession(sess)
		return false
	}
	m.mu.Lock()
	runCtx := m.ctx
	m.mu.Unlock()
	if runCtx == nil {
		runCtx = ctx
	}
	if runCtx.Err() != nil || sess.Closed() {
		m.discardSession(sess)
		return false
	}
	sess.startServe(runCtx, m.OnStream)
	sess.startHeartbeat(runCtx, m.cfg.HeartbeatInterval, m.cfg.HeartbeatTimeout)
	// Window after startServe: another Install may have cleared onClose, swapped
	// the roster entry, and Closed this session. Do not report success then.
	if !m.stillCurrent(sess) || sess.Closed() {
		m.discardSession(sess)
		return false
	}
	return true
}

// stillCurrent reports whether sess is the live roster entry for its node ID.
func (m *Manager) stillCurrent(sess *Session) bool {
	return m.roster.Get(sess.NodeID()) == sess
}

// discardSession drops sess without firing roster onClose (not the current
// entry, or lost activate race). Removes if still mapped so a closed session
// cannot linger in the roster.
func (m *Manager) discardSession(sess *Session) {
	sess.setOnClose(nil)
	m.roster.RemoveIfCurrent(sess)
	sess.Close()
}

func (m *Manager) onSessionClose(sess *Session) {
	if sess == nil {
		return
	}
	removed := m.roster.RemoveIfCurrent(sess)
	if removed {
		m.log.Info("peer disconnected", "peer", sess.NodeID(), "addr", sess.Addr())
		if m.OnPeerDown != nil {
			m.OnPeerDown(sess.NodeID(), sess.Addr())
		}
		// Re-enter discovery for this address after a short backoff.
		if sess.Addr() != "" {
			m.disc.SoftFailAddr(sess.Addr())
		}
	}
}

// ErrDial marks outbound dial-phase failures (timeouts, refused, already connected, …).
var ErrDial = errors.New("dial")
