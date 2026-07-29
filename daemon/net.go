package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"tailscale.com/client/local"
	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"

	"deedles.dev/tailsync/internal/peer"
)

// NetMode selects how the daemon attaches to the network.
type NetMode int

const (
	// NetModeHost uses the system tailscaled via LocalAPI (default).
	// Traffic stays on the host Tailscale identity; no extra node is registered.
	NetModeHost NetMode = iota
	// NetModeTSNet runs an embedded tsnet node (registers as a separate machine).
	NetModeTSNet
	// NetModePlain uses plain QUIC (UDP) on ListenHost (tests only).
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

// mullvadExitNodeTag is the ACL tag Tailscale applies to Mullvad VPN exit-node
// peers. Those peers appear Online on the tailnet but never run tailsync.
const mullvadExitNodeTag = "tag:mullvad-exit-node"

// mullvadDNSSuffix is the MagicDNS suffix of Mullvad exit nodes (with or
// without a trailing dot on the full name).
const mullvadDNSSuffix = "mullvad.ts.net"

// isMullvadDNSName reports whether dns is under the Mullvad MagicDNS domain.
func isMullvadDNSName(dns string) bool {
	dns = strings.ToLower(strings.TrimSuffix(dns, "."))
	return dns == mullvadDNSSuffix || strings.HasSuffix(dns, "."+mullvadDNSSuffix)
}

// isMullvadPeer reports whether p is a Tailscale Mullvad exit-node peer:
// tag:mullvad-exit-node and/or a mullvad.ts.net DNS name. Do not use
// ExitNodeOption alone — user-run exit nodes may still run tailsync.
func isMullvadPeer(p *ipnstate.PeerStatus) bool {
	if p == nil {
		return false
	}
	id := meshIdentityFromPeerStatus(p)
	return isMullvadIdentity(id.tags, id.dns)
}

// peersFromStatus returns dial addresses (host:port) for online mesh peers
// excluding self. Prefers the first Tailscale IP for reliable dialing with the
// host net stack (does not depend on MagicDNS); falls back to MagicDNS when no
// IP is known.
//
// Trust policy (see trustedMeshPeer): untagged Self → same UserID; tagged Self
// → TagMatchMode against peer tags. Sharees, Mullvad, and other users /
// untagged-vs-tagged mismatches are skipped. Fail closed when Self identity is
// unusable (untagged with unknown UserID, or missing Self).
//
// Self exclusion uses StableID and MagicDNS equality only (not HostName), so
// distinct nodes that share an OS hostname are still discovered.
func peersFromStatus(st *ipnstate.Status, port int, mode TagMatchMode) []string {
	if st == nil || st.Self == nil {
		return nil
	}
	selfID := meshIdentityFromPeerStatus(st.Self)
	if !selfID.tagged() && selfID.user == 0 {
		return nil
	}
	self := st.Self
	selfStable := string(self.ID)
	selfDNS := strings.TrimSuffix(self.DNSName, ".")
	portStr := strconv.Itoa(port)
	var addrs []string
	for _, p := range st.Peer {
		if p == nil || !p.Online {
			continue
		}
		if selfStable != "" && string(p.ID) == selfStable {
			continue
		}
		if !trustedMeshPeer(selfID, meshIdentityFromPeerStatus(p), mode) {
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
		addrs = append(addrs, net.JoinHostPort(host, portStr))
	}
	return addrs
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

func (d *Daemon) listen(ctx context.Context) error {
	switch d.cfg.NetMode {
	case NetModePlain:
		return d.listenPlain(ctx)
	case NetModeTSNet:
		return d.listenTSNet(ctx)
	default:
		return d.listenHost(ctx)
	}
}

func (d *Daemon) newMesh() (*peer.Manager, error) {
	m, err := peer.NewManager(peer.Config{
		NodeID:               d.nodeID,
		Port:                 d.cfg.Port,
		ServerTLS:            d.quicTLS,
		ClientTLS:            quicClientTLSConfig(),
		Logger:               d.log,
		DiscoveryConcurrency: d.cfg.DiscoveryConcurrency,
		DialTimeout:          d.cfg.DialTimeout,
		HeartbeatInterval:    d.cfg.HeartbeatInterval,
		Candidates:           d.discoveryCandidates,
	})
	if err != nil {
		return nil, err
	}
	m.OnStream = d.onPeerStream
	m.OnPeerUp = func(s *peer.Session) {
		d.requestPull()
	}
	// Bind Hello NodeID to Tailscale identity for host/tsnet (not plain tests).
	if d.cfg.NetMode != NetModePlain {
		m.VerifyRemoteID = d.verifyPeerNodeID
	}
	return m, nil
}

// verifyPeerNodeID checks that claimed Hello NodeID matches Tailscale WhoIs for
// remoteAddr and that the peer is a trusted mesh peer (same UserID when Self is
// untagged; TagMatchMode when Self is tagged; rejects sharees and Mullvad).
// Prevents roster hijack under skip-verify TLS and blocks wrong-identity nodes
// even when their Hello names match wire identity.
func (d *Daemon) verifyPeerNodeID(ctx context.Context, remoteAddr, claimed string) error {
	lc, err := d.localClient()
	if err != nil {
		return err
	}
	who, err := lc.WhoIs(ctx, remoteAddr)
	if err != nil {
		return fmt.Errorf("whois %s: %w", remoteAddr, err)
	}
	if who == nil || who.Node == nil {
		return fmt.Errorf("whois %s: empty node", remoteAddr)
	}
	if !claimedMatchesWhoIs(claimed, who) {
		return fmt.Errorf("hello node id %q does not match tailscale peer at %s", claimed, remoteAddr)
	}
	// Trust from LocalAPI Self + WhoIs, not Hello alone.
	st, err := lc.StatusWithoutPeers(ctx)
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	if st == nil || st.Self == nil {
		return fmt.Errorf("cannot determine local tailscale identity")
	}
	selfID := meshIdentityFromPeerStatus(st.Self)
	if !selfID.tagged() && selfID.user == 0 {
		return fmt.Errorf("cannot determine local tailscale user")
	}
	if !meshPeerAllowed(st.Self, who, d.cfg.TagMatch) {
		return fmt.Errorf("peer at %s is not a trusted mesh peer", remoteAddr)
	}
	return nil
}

// localClient returns the LocalAPI client for host or tsnet mode.
func (d *Daemon) localClient() (*local.Client, error) {
	switch d.cfg.NetMode {
	case NetModeTSNet:
		if d.server == nil {
			return nil, fmt.Errorf("tsnet not started")
		}
		return d.server.LocalClient()
	case NetModeHost:
		if d.local != nil {
			return d.local, nil
		}
		return &local.Client{}, nil
	default:
		return nil, fmt.Errorf("no local client in %s mode", d.cfg.NetMode)
	}
}

// claimedMatchesWhoIs reports whether claimed Hello NodeID matches a canonical
// identity for the Tailscale peer. Matching is exact on MagicDNS FQDN (no
// trailing dot), StableID, HostName, or ComputedName. A short claim may match
// only as the first label of the peer FQDN (want + "." prefix of c), not the
// reverse (which would allow multi-identity extensions of a short local id).
func claimedMatchesWhoIs(claimed string, who *apitype.WhoIsResponse) bool {
	if claimed == "" || who == nil || who.Node == nil {
		return false
	}
	want := strings.ToLower(strings.TrimSuffix(claimed, "."))
	n := who.Node
	candidates := []string{
		strings.TrimSuffix(n.Name, "."),
		string(n.StableID),
	}
	if n.Hostinfo.Valid() {
		if hn := n.Hostinfo.Hostname(); hn != "" {
			candidates = append(candidates, hn)
		}
	}
	if cn := n.ComputedName; cn != "" {
		candidates = append(candidates, cn)
	}
	for _, c := range candidates {
		c = strings.ToLower(strings.TrimSuffix(c, "."))
		if c == "" {
			continue
		}
		if c == want {
			return true
		}
		// Short claim "peer" matches FQDN "peer.tailnet.ts.net" only.
		if strings.HasPrefix(c, want+".") {
			return true
		}
	}
	return false
}

func (d *Daemon) listenPlain(ctx context.Context) error {
	host := d.cfg.ListenHost
	addr := net.JoinHostPort(host, strconv.Itoa(d.cfg.Port))
	if d.cfg.Hostname != "" {
		d.nodeID = d.cfg.Hostname
	}
	if d.nodeID == "" {
		d.nodeID = d.cfg.Hostname
	}

	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("listen udp %s: %w", addr, err)
	}

	m, err := d.newMesh()
	if err != nil {
		_ = pc.Close()
		return err
	}
	if err := m.AddPacketConn(pc); err != nil {
		_ = m.Close()
		return fmt.Errorf("mesh add packet conn: %w", err)
	}
	m.Start(ctx)
	d.mesh = m
	d.log.Info("listening (plain QUIC)", "addrs", m.Endpoint().LocalAddrs(), "mode", NetModePlain.String())
	return nil
}

func (d *Daemon) listenTSNet(ctx context.Context) error {
	tsnetDir := filepath.Join(d.cfg.StateDir, "tsnet")
	if err := os.MkdirAll(tsnetDir, 0o700); err != nil {
		return fmt.Errorf("create tsnet state dir: %w", err)
	}
	// Android-only: avoid logpolicy.LogsDir panic when default cache/state
	// paths are unusable. Non-Android keeps Tailscale's normal log dirs;
	// existing TS_LOGS_DIR is never overwritten.
	if err := ensureAndroidTSLogsDir(d.cfg.StateDir, d.log); err != nil {
		return err
	}

	s := &tsnet.Server{
		Dir:      tsnetDir,
		Hostname: d.cfg.Hostname,
		AuthKey:  d.cfg.AuthKey,
		Logf: func(format string, args ...any) {
			d.log.Debug(fmt.Sprintf(format, args...), "component", "tsnet")
		},
		// UserLogf surfaces AuthURL and other user-facing tsnet messages
		// (CLI sees login URLs at Info without scraping Debug backend logs).
		UserLogf: func(format string, args ...any) {
			d.log.Info(fmt.Sprintf(format, args...), "component", "tsnet")
		},
	}
	d.server = s

	// Watch for interactive login URLs while Up blocks on Running.
	// Cancel and join before Close/continue so LocalClient use and OnAuthURL
	// cannot race tsnet shutdown or fire after bring-up completes.
	var (
		watchCancel context.CancelFunc
		watchDone   chan struct{}
	)
	stopAuthWatch := func() {
		if watchCancel != nil {
			watchCancel()
		}
		if watchDone != nil {
			<-watchDone
		}
	}
	if d.cfg.OnAuthURL != nil {
		watchCtx, cancel := context.WithCancel(ctx)
		watchCancel = cancel
		watchDone = make(chan struct{})
		tracker := newAuthURLTracker(d.cfg.OnAuthURL)
		go func() {
			defer close(watchDone)
			d.watchTSNetAuthURL(watchCtx, s, tracker)
		}()
	}

	if _, err := s.Up(ctx); err != nil {
		stopAuthWatch()
		_ = s.Close()
		d.server = nil
		return fmt.Errorf("tsnet up: %w", err)
	}
	stopAuthWatch()

	// Enable Android (and other hosts) to inject netmon events after route
	// updates. Capture InjectEvent under netMu; clear before Close so concurrent
	// InjectNetworkChange either runs on a live monitor or no-ops.
	// Catch-up InjectEvent re-reads the host snapshot: mid-Up NotifyNetworkChange
	// is a no-op while injectNetChange is still nil, and NetMon may have polled
	// an older list during Up.
	if mon, ok := s.Sys().NetMon.GetOK(); ok && mon != nil {
		d.setInjectNetChange(mon.InjectEvent)
		mon.InjectEvent()
	}

	// tsnet.ListenPacket requires a concrete IP (not ":port"). Prefer Self IPs.
	lc, err := s.LocalClient()
	if err != nil {
		d.setInjectNetChange(nil)
		_ = s.Close()
		d.server = nil
		return fmt.Errorf("tsnet local client: %w", err)
	}
	st, err := lc.Status(ctx)
	if err != nil {
		d.setInjectNetChange(nil)
		_ = s.Close()
		d.server = nil
		return fmt.Errorf("tsnet status: %w", err)
	}
	var ips []netip.Addr
	if st.Self != nil && len(st.Self.TailscaleIPs) > 0 {
		ips = st.Self.TailscaleIPs
	} else if len(st.TailscaleIPs) > 0 {
		ips = st.TailscaleIPs
	}
	if len(ips) == 0 {
		d.setInjectNetChange(nil)
		_ = s.Close()
		d.server = nil
		return fmt.Errorf("tsnet: no Tailscale IPs after up")
	}

	d.nodeID = d.cfg.Hostname

	m, err := d.newMesh()
	if err != nil {
		d.setInjectNetChange(nil)
		_ = s.Close()
		d.server = nil
		return err
	}
	// Name resolution via LocalAPI (MagicDNS), not system DNS.
	m.ResolveAddr = func(ctx context.Context, addr string) (net.Addr, error) {
		return resolveTSNetUDPAddr(ctx, s, addr)
	}

	addrs := bindAddrsFromTailscaleIPs(ips, d.cfg.Port)
	var bound, skipped []string
	for _, a := range addrs {
		pc, err := s.ListenPacket("udp", a)
		if err != nil {
			skipped = append(skipped, a)
			continue
		}
		if err := m.AddPacketConn(pc); err != nil {
			_ = pc.Close()
			skipped = append(skipped, a)
			continue
		}
		bound = append(bound, a)
	}
	if len(bound) == 0 {
		_ = m.Close()
		d.setInjectNetChange(nil)
		_ = s.Close()
		d.server = nil
		return fmt.Errorf("tsnet quic listen failed on all addresses %v", addrs)
	}
	for _, a := range skipped {
		d.log.Warn("could not bind tsnet address; continuing with others", "addr", a)
	}
	m.Start(ctx)
	d.mesh = m
	d.log.Info("listening on tailnet (tsnet QUIC)",
		"addrs", bound,
		"skipped", skipped,
		"hostname", d.cfg.Hostname,
		"mode", NetModeTSNet.String(),
	)
	return nil
}

// ensureAndroidTSLogsDir points Tailscale logpolicy at a writable directory under
// stateDir on Android only. ipnlocal/sockstatlog calls logpolicy.LogsDir during
// backend start; without a safe path it panics ("no safe place found to store
// log state"). Java Os.setenv is not always visible to the Go runtime, so we
// set the env from Go.
//
// No-op on non-Android platforms so desktop/server tsnet keeps default log
// locations. No-op when TS_LOGS_DIR is already set so hosts can override.
func ensureAndroidTSLogsDir(stateDir string, log *slog.Logger) error {
	if runtime.GOOS != "android" {
		return nil
	}
	if os.Getenv("TS_LOGS_DIR") != "" {
		return nil
	}
	logsDir := filepath.Join(stateDir, "tsnet-logs")
	if err := os.MkdirAll(logsDir, 0o700); err != nil {
		return fmt.Errorf("create tsnet logs dir: %w", err)
	}
	if err := os.Setenv("TS_LOGS_DIR", logsDir); err != nil {
		if log != nil {
			log.Warn("set TS_LOGS_DIR", "err", err, "dir", logsDir)
		}
		return nil
	}
	if log != nil {
		log.Info("tsnet log dir", "dir", logsDir, "component", "tsnet")
	}
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

	m, err := d.newMesh()
	if err != nil {
		return err
	}

	var bound, skipped []string
	for _, a := range addrs {
		pc, err := net.ListenPacket("udp", a)
		if err != nil {
			skipped = append(skipped, a)
			continue
		}
		if err := m.AddPacketConn(pc); err != nil {
			_ = pc.Close()
			skipped = append(skipped, a)
			continue
		}
		bound = append(bound, a)
	}
	if len(bound) == 0 {
		_ = m.Close()
		return fmt.Errorf("listen on Tailscale IPs failed for all addresses %v", addrs)
	}
	for _, a := range skipped {
		d.log.Warn("could not bind Tailscale address; continuing with others", "addr", a)
	}
	m.Start(ctx)
	d.mesh = m
	d.log.Info("listening on host tailnet (QUIC)",
		"addrs", bound,
		"skipped", skipped,
		"hostname", d.nodeID,
		"mode", NetModeHost.String(),
		"backend", st.BackendState,
	)
	return nil
}

// closeMesh stops the peer manager (accept, discovery, sessions). Idempotent.
// Call before waiting on stream/pull/notify wait groups so handlers observe
// closed connections rather than racing root teardown.
func (d *Daemon) closeMesh() {
	if d.mesh != nil {
		_ = d.mesh.Close()
		d.mesh = nil
	}
}

// closeNetworkBackend tears down tsnet/local clients after mesh and workers.
func (d *Daemon) closeNetworkBackend() {
	d.setInjectNetChange(nil)
	// Mesh may already be closed; ensure nil.
	d.closeMesh()
	if d.server != nil {
		_ = d.server.Close()
		d.server = nil
	}
	d.local = nil
}

// listPeers returns online discovery peers from Tailscale status that pass
// mesh trust (see peersFromStatus / trustedMeshPeer).
func (d *Daemon) listPeers(ctx context.Context) ([]string, error) {
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
		addrs := peersFromStatus(st, d.cfg.Port, d.cfg.TagMatch)
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
		return peersFromStatus(st, d.cfg.Port, d.cfg.TagMatch), nil
	}
}
