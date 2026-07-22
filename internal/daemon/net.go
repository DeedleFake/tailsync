package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
)

// NetMode selects how the daemon attaches to the network.
type NetMode int

const (
	// NetModeHost uses the system tailscaled via LocalAPI (default).
	// Traffic stays on the host Tailscale identity; no extra node is registered.
	NetModeHost NetMode = iota
	// NetModeTSNet runs an embedded tsnet node (registers as a separate machine).
	NetModeTSNet
	// NetModePlain uses plain TCP on ListenHost (tests only).
	NetModePlain
)

func (m NetMode) String() string {
	switch m {
	case NetModeHost:
		return "host"
	case NetModeTSNet:
		return "tsnet"
	case NetModePlain:
		return "plain"
	default:
		return fmt.Sprintf("NetMode(%d)", int(m))
	}
}

// bindAddrsFromTailscaleIPs builds host:port listen addresses for each Tailscale IP.
// Both IPv4 and IPv6 addresses are included so dual-stack peers can connect.
func bindAddrsFromTailscaleIPs(ips []netip.Addr, port int) []string {
	var out []string
	portStr := strconv.Itoa(port)
	for _, ip := range ips {
		if !ip.IsValid() {
			continue
		}
		out = append(out, net.JoinHostPort(ip.String(), portStr))
	}
	return out
}

// nodeIDFromSelf picks a stable protocol node identity from LocalAPI Self.
// Preference: MagicDNS name, then HostName, then StableID.
func nodeIDFromSelf(self *ipnstate.PeerStatus) string {
	if self == nil {
		return ""
	}
	if dns := strings.TrimSuffix(self.DNSName, "."); dns != "" {
		return dns
	}
	if self.HostName != "" {
		return self.HostName
	}
	return string(self.ID)
}

// peersFromStatus returns dial addresses (host:port) for online peers excluding self.
// Prefers the first Tailscale IP for reliable dialing with the host net stack
// (does not depend on MagicDNS); falls back to MagicDNS when no IP is known.
// When serviceName is non-empty, only peers whose HostName or DNSName contains
// that substring (case-insensitive) are included.
//
// Self exclusion uses StableID and MagicDNS equality only (not HostName), so
// distinct nodes that share an OS hostname are still discovered.
func peersFromStatus(st *ipnstate.Status, port int, serviceName string) []string {
	if st == nil {
		return nil
	}
	var (
		selfID  string
		selfDNS string
	)
	if st.Self != nil {
		selfID = string(st.Self.ID)
		selfDNS = strings.TrimSuffix(st.Self.DNSName, ".")
	}
	svc := strings.ToLower(serviceName)
	portStr := strconv.Itoa(port)
	var addrs []string
	for _, p := range st.Peer {
		if p == nil || !p.Online {
			continue
		}
		if selfID != "" && string(p.ID) == selfID {
			continue
		}
		dns := strings.TrimSuffix(p.DNSName, ".")
		// Prefer Tailscale IP so host-mode dial works without MagicDNS.
		host := ""
		if len(p.TailscaleIPs) > 0 {
			host = p.TailscaleIPs[0].String()
		}
		if host == "" {
			host = dns
		}
		if host == "" {
			continue
		}
		// Skip ourselves by MagicDNS when present (StableID is primary).
		if selfDNS != "" && dns != "" {
			if strings.EqualFold(dns, selfDNS) || strings.HasPrefix(strings.ToLower(dns), strings.ToLower(selfDNS)+".") {
				continue
			}
		}
		if svc != "" {
			hn := strings.ToLower(p.HostName)
			dn := strings.ToLower(dns)
			// Filter on hostname/DNS labels, not the dial target (which may be an IP).
			if !strings.Contains(hn, svc) && !strings.Contains(dn, svc) {
				continue
			}
		}
		addrs = append(addrs, net.JoinHostPort(host, portStr))
	}
	return addrs
}

// multiListener accepts connections from multiple underlying listeners.
// A temporary Accept error on one listener is retried; a permanent error stops
// only that listener's loop. Accept returns an error only when the multi-listener
// is closed or every underlying listener has failed permanently.
type multiListener struct {
	lns  []net.Listener
	addr string

	mu     sync.Mutex
	conns  chan acceptResult
	closed bool
	done   chan struct{}
	alive  int
}

type acceptResult struct {
	conn net.Conn
	err  error
}

func newMultiListener(lns []net.Listener) *multiListener {
	parts := make([]string, 0, len(lns))
	for _, ln := range lns {
		parts = append(parts, ln.Addr().String())
	}
	m := &multiListener{
		lns:   lns,
		addr:  strings.Join(parts, ","),
		conns: make(chan acceptResult),
		done:  make(chan struct{}),
		alive: len(lns),
	}
	for _, ln := range lns {
		go m.loop(ln)
	}
	return m
}

func (m *multiListener) loop(ln net.Listener) {
	defer m.listenerExit()

	var tempDelay time.Duration
	for {
		c, err := ln.Accept()
		if err != nil {
			m.mu.Lock()
			closed := m.closed
			m.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return
			}
			// Retry temporary Accept errors (same idea as net/http.Server).
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := time.Second; tempDelay > max {
					tempDelay = max
				}
				select {
				case <-time.After(tempDelay):
					continue
				case <-m.done:
					return
				}
			}
			// Permanent error on this listener only: leave other loops running.
			return
		}
		tempDelay = 0
		select {
		case m.conns <- acceptResult{conn: c}:
		case <-m.done:
			_ = c.Close()
			return
		}
	}
}

// listenerExit decrements the alive count. If the multi-listener is still open
// and no Accept loops remain, signal Accept with an error so the consumer can exit.
func (m *multiListener) listenerExit() {
	m.mu.Lock()
	m.alive--
	alive := m.alive
	closed := m.closed
	m.mu.Unlock()
	if closed || alive > 0 {
		return
	}
	select {
	case m.conns <- acceptResult{err: fmt.Errorf("all listeners failed: %w", net.ErrClosed)}:
	case <-m.done:
	}
}

func (m *multiListener) Accept() (net.Conn, error) {
	select {
	case <-m.done:
		return nil, net.ErrClosed
	case r, ok := <-m.conns:
		if !ok {
			return nil, net.ErrClosed
		}
		if r.err != nil {
			return nil, r.err
		}
		return r.conn, nil
	}
}

func (m *multiListener) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	close(m.done)
	m.mu.Unlock()

	var first error
	for _, ln := range m.lns {
		if err := ln.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *multiListener) Addr() net.Addr {
	if len(m.lns) == 0 {
		return nil
	}
	return multiAddr(m.addr)
}

// multiAddr is a net.Addr that reports all bound addresses.
type multiAddr string

func (a multiAddr) Network() string { return "tcp" }
func (a multiAddr) String() string  { return string(a) }

// listenResult is the outcome of binding a set of addresses.
type listenResult struct {
	Listener net.Listener
	Bound    []string // addresses that bound successfully
	Skipped  []string // addresses that failed to bind (empty if none)
}

// listenAll binds TCP on each address, keeping successful binds even if some fail.
// Errors only when every address fails (or addrs is empty). A single successful
// bind returns that listener directly; multiple binds use multiListener.
func listenAll(addrs []string) (listenResult, error) {
	var res listenResult
	if len(addrs) == 0 {
		return res, fmt.Errorf("no listen addresses")
	}
	var lns []net.Listener
	for _, a := range addrs {
		ln, err := net.Listen("tcp", a)
		if err != nil {
			res.Skipped = append(res.Skipped, a)
			continue
		}
		lns = append(lns, ln)
		res.Bound = append(res.Bound, a)
	}
	if len(lns) == 0 {
		return res, fmt.Errorf("listen failed on all addresses %v", addrs)
	}
	if len(lns) == 1 {
		res.Listener = lns[0]
		return res, nil
	}
	res.Listener = newMultiListener(lns)
	return res, nil
}

func (d *Daemon) listen(ctx context.Context) error {
	switch d.cfg.NetMode {
	case NetModePlain:
		return d.listenPlain()
	case NetModeTSNet:
		return d.listenTSNet(ctx)
	default:
		return d.listenHost(ctx)
	}
}

func (d *Daemon) listenPlain() error {
	host := d.cfg.ListenHost
	ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(d.cfg.Port)))
	if err != nil {
		return fmt.Errorf("listen %s:%d: %w", host, d.cfg.Port, err)
	}
	d.ln = ln
	if d.cfg.Hostname != "" {
		d.nodeID = d.cfg.Hostname
	}
	d.log.Info("listening (plain TCP)", "addr", ln.Addr().String(), "mode", NetModePlain.String())
	return nil
}

func (d *Daemon) listenTSNet(ctx context.Context) error {
	s := &tsnet.Server{
		Dir:      filepath.Join(d.cfg.StateDir, "tsnet"),
		Hostname: d.cfg.Hostname,
		AuthKey:  d.cfg.AuthKey,
		Logf: func(format string, args ...any) {
			d.log.Debug(fmt.Sprintf(format, args...), "component", "tsnet")
		},
	}
	d.server = s

	if _, err := s.Up(ctx); err != nil {
		_ = s.Close()
		d.server = nil
		return fmt.Errorf("tsnet up: %w", err)
	}

	addr := ":" + strconv.Itoa(d.cfg.Port)
	ln, err := s.Listen("tcp", addr)
	if err != nil {
		_ = s.Close()
		d.server = nil
		return fmt.Errorf("tsnet listen %s: %w", addr, err)
	}
	d.ln = ln
	d.nodeID = d.cfg.Hostname
	d.log.Info("listening on tailnet (tsnet)", "addr", ln.Addr().String(), "hostname", d.cfg.Hostname, "mode", NetModeTSNet.String())
	return nil
}

func (d *Daemon) listenHost(ctx context.Context) error {
	lc := &local.Client{}
	d.local = lc

	st, err := lc.Status(ctx)
	if err != nil {
		return fmt.Errorf("local tailscaled status: %w (is tailscaled running? use -tsnet for an embedded node, or -plain for local tests)", err)
	}
	if st.BackendState != "" && st.BackendState != "Running" {
		return fmt.Errorf("tailscaled is not running (state %q); start Tailscale or use -tsnet / -plain", st.BackendState)
	}

	var ips []netip.Addr
	if st.Self != nil && len(st.Self.TailscaleIPs) > 0 {
		ips = st.Self.TailscaleIPs
	} else if len(st.TailscaleIPs) > 0 {
		ips = st.TailscaleIPs
	}
	addrs := bindAddrsFromTailscaleIPs(ips, d.cfg.Port)
	if len(addrs) == 0 {
		return fmt.Errorf("no Tailscale IPs on this node; is tailscaled logged in? use -tsnet or -plain as alternatives")
	}

	// Protocol identity always comes from host Tailscale node (overwrites -hostname).
	id := nodeIDFromSelf(st.Self)
	if id == "" {
		return fmt.Errorf("could not determine host Tailscale identity from LocalAPI")
	}
	d.nodeID = id
	d.cfg.Hostname = id

	res, err := listenAll(addrs)
	if err != nil {
		return fmt.Errorf("listen on Tailscale IPs: %w", err)
	}
	d.ln = res.Listener
	for _, a := range res.Skipped {
		d.log.Warn("could not bind Tailscale address; continuing with others", "addr", a)
	}
	d.log.Info("listening on host tailnet",
		"addrs", res.Bound,
		"skipped", res.Skipped,
		"hostname", d.nodeID,
		"mode", NetModeHost.String(),
		"backend", st.BackendState,
	)
	return nil
}

// closeNetListener closes the TCP listener so Accept unblocks. Idempotent.
// Does not nil d.ln until closeNetworkBackend so a late field read is non-nil
// if any code still observes it; acceptLoop uses a captured listener value.
func (d *Daemon) closeNetListener() {
	if d.ln != nil {
		_ = d.ln.Close()
	}
}

// closeNetworkBackend tears down tsnet/local clients and clears listener state.
// Call only after acceptLoop has exited (so no concurrent Accept on d.ln).
func (d *Daemon) closeNetworkBackend() {
	d.ln = nil
	if d.server != nil {
		_ = d.server.Close()
		d.server = nil
	}
	d.local = nil
}

// closeListener closes the listener and backend. Prefer the split helpers from
// Run so acceptLoop can exit before backend teardown; this remains for tests.
func (d *Daemon) closeListener() {
	d.closeNetListener()
	d.closeNetworkBackend()
}

func (d *Daemon) listPeers(ctx context.Context) ([]string, error) {
	if len(d.cfg.Peers) > 0 {
		return append([]string(nil), d.cfg.Peers...), nil
	}
	switch d.cfg.NetMode {
	case NetModePlain:
		return nil, nil
	case NetModeTSNet:
		if d.server == nil {
			return nil, nil
		}
		lc, err := d.server.LocalClient()
		if err != nil {
			return nil, err
		}
		st, err := lc.Status(ctx)
		if err != nil {
			return nil, err
		}
		// Also skip by configured tsnet hostname (Self may use a different DNS form).
		addrs := peersFromStatus(st, d.cfg.Port, d.cfg.ServiceName)
		return filterSelfHostname(addrs, d.cfg.Hostname), nil
	default: // host
		lc := d.local
		if lc == nil {
			lc = &local.Client{}
		}
		st, err := lc.Status(ctx)
		if err != nil {
			return nil, err
		}
		return peersFromStatus(st, d.cfg.Port, d.cfg.ServiceName), nil
	}
}

// filterSelfHostname drops addresses that clearly refer to the local tsnet hostname.
func filterSelfHostname(addrs []string, hostname string) []string {
	if hostname == "" {
		return addrs
	}
	self := strings.ToLower(hostname)
	var out []string
	for _, a := range addrs {
		host, _, err := net.SplitHostPort(a)
		if err != nil {
			host = a
		}
		h := strings.ToLower(host)
		if h == self || strings.HasPrefix(h, self+".") {
			continue
		}
		out = append(out, a)
	}
	return out
}

func (d *Daemon) dial(ctx context.Context, addr string) (net.Conn, error) {
	switch d.cfg.NetMode {
	case NetModeTSNet:
		if d.server != nil {
			return d.server.Dial(ctx, "tcp", addr)
		}
		fallthrough
	default:
		var nd net.Dialer
		return nd.DialContext(ctx, "tcp", addr)
	}
}
