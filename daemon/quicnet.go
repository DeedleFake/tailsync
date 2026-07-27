package daemon

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
)

// quicALPN is the TLS NextProto for tailsync peer sessions.
// Required by quic-go; not a wire-protocol version (that is proto.Version).
const quicALPN = "tailsync"

// streamAcceptTimeout bounds how long the server waits for the dialer to open
// the single application stream after the QUIC handshake. One session = one
// stream for now (mirrors former one-shot TCP sessions).
const streamAcceptTimeout = 30 * time.Second

// generateQUICTLSConfig builds an ephemeral self-signed server certificate.
// Peer authentication is the tailnet (or localhost in plain tests); the cert
// only satisfies QUIC/TLS requirements. Clients use InsecureSkipVerify.
func generateQUICTLSConfig() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate quic tls key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate cert serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create self-signed cert: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load tls key pair: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{quicALPN},
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func quicClientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, // tailnet (or localhost test) provides peer trust
		NextProtos:         []string{quicALPN},
		MinVersion:         tls.VersionTLS13,
	}
}

func quicConfig() *quic.Config {
	return &quic.Config{
		// Sessions may transfer large files under setConnDeadline (~5m).
		MaxIdleTimeout:  10 * time.Minute,
		KeepAlivePeriod: 30 * time.Second,
		// One application stream per connection for the current one-shot model.
		MaxIncomingStreams:    1,
		MaxIncomingUniStreams: 0,
	}
}

// streamConn presents a single QUIC stream as a net.Conn. Closing the stream
// also closes the underlying QUIC connection (one-shot session model).
type streamConn struct {
	str     *quic.Stream
	qconn   *quic.Conn
	onClose func()

	closeOnce sync.Once
	closeErr  error
}

func newStreamConn(str *quic.Stream, qconn *quic.Conn, onClose func()) *streamConn {
	return &streamConn{str: str, qconn: qconn, onClose: onClose}
}

func (c *streamConn) Read(p []byte) (int, error)  { return c.str.Read(p) }
func (c *streamConn) Write(p []byte) (int, error) { return c.str.Write(p) }

func (c *streamConn) Close() error {
	c.closeOnce.Do(func() {
		var first error
		// Half-close write side, abandon read, then tear down the connection.
		if err := c.str.Close(); err != nil {
			first = err
		}
		c.str.CancelRead(0)
		if err := c.qconn.CloseWithError(0, ""); err != nil && first == nil {
			first = err
		}
		if c.onClose != nil {
			c.onClose()
		}
		c.closeErr = first
	})
	return c.closeErr
}

func (c *streamConn) LocalAddr() net.Addr  { return c.qconn.LocalAddr() }
func (c *streamConn) RemoteAddr() net.Addr { return c.qconn.RemoteAddr() }

func (c *streamConn) SetDeadline(t time.Time) error {
	return c.str.SetDeadline(t)
}
func (c *streamConn) SetReadDeadline(t time.Time) error {
	return c.str.SetReadDeadline(t)
}
func (c *streamConn) SetWriteDeadline(t time.Time) error {
	return c.str.SetWriteDeadline(t)
}

// quicStreamListener adapts a quic.Listener to net.Listener, accepting one
// bidirectional stream per QUIC connection (session = stream).
//
// AcceptStream runs in a per-connection goroutine so a peer that handshakes
// but never opens a stream cannot block Accept of other connections.
//
// ready is never closed: waitStream may send concurrently with acceptLoop exit
// and Close (mirrors multiListener). Shutdown unblocks via ctx cancel only.
type quicStreamListener struct {
	qln     *quic.Listener
	ownedPC net.PacketConn // closed with the listener when non-nil (tsnet UDP)
	log     *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	ready    chan streamAcceptResult
	streamWG sync.WaitGroup // in-flight waitStream goroutines

	closeOnce sync.Once
	closeErr  error
}

type streamAcceptResult struct {
	conn net.Conn
	err  error
}

func newQUICStreamListener(qln *quic.Listener, ownedPC net.PacketConn, log *slog.Logger) *quicStreamListener {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &quicStreamListener{
		qln:     qln,
		ownedPC: ownedPC,
		log:     log,
		ctx:     ctx,
		cancel:  cancel,
		ready:   make(chan streamAcceptResult),
	}
	go l.acceptLoop()
	return l
}

func (l *quicStreamListener) acceptLoop() {
	for {
		qconn, err := l.qln.Accept(l.ctx)
		if err != nil {
			err = mapQUICAcceptErr(l.ctx, err)
			// Deliver one terminal error if a waiter is present; do not close
			// ready (waitStream may still send). Unblock further Accept via cancel.
			select {
			case l.ready <- streamAcceptResult{err: err}:
			case <-l.ctx.Done():
			}
			l.cancel()
			return
		}
		// Do not block the Accept hot path on AcceptStream.
		l.streamWG.Add(1)
		go l.waitStream(qconn)
	}
}

func (l *quicStreamListener) waitStream(qconn *quic.Conn) {
	defer l.streamWG.Done()

	sctx, cancel := context.WithTimeout(l.ctx, streamAcceptTimeout)
	str, err := qconn.AcceptStream(sctx)
	cancel()
	if err != nil {
		remote := ""
		if ra := qconn.RemoteAddr(); ra != nil {
			remote = ra.String()
		}
		l.log.Debug("quic accept stream failed", "remote", remote, "err", err)
		_ = qconn.CloseWithError(1, "no stream")
		return
	}
	conn := newStreamConn(str, qconn, nil)
	// Prefer drop if already shutting down so we do not hand out conns after Close.
	if l.ctx.Err() != nil {
		_ = conn.Close()
		return
	}
	select {
	case l.ready <- streamAcceptResult{conn: conn}:
	case <-l.ctx.Done():
		_ = conn.Close()
	}
}

func mapQUICAcceptErr(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, quic.ErrServerClosed) || errors.Is(err, context.Canceled) {
		return net.ErrClosed
	}
	return err
}

func (l *quicStreamListener) Accept() (net.Conn, error) {
	select {
	case <-l.ctx.Done():
		return nil, net.ErrClosed
	case r := <-l.ready:
		if r.err != nil {
			return nil, r.err
		}
		// Drop late deliveries after Close/cancel raced with a ready send.
		if l.ctx.Err() != nil {
			if r.conn != nil {
				_ = r.conn.Close()
			}
			return nil, net.ErrClosed
		}
		return r.conn, nil
	}
}

func (l *quicStreamListener) Close() error {
	l.closeOnce.Do(func() {
		l.cancel()
		err := l.qln.Close()
		// Join waitStream so Close returns after they drop/send-or-exit; ready
		// stays open for the process lifetime of this listener (no close race).
		l.streamWG.Wait()
		// quic-go does not close an externally supplied PacketConn.
		if l.ownedPC != nil {
			if e := l.ownedPC.Close(); e != nil && err == nil {
				err = e
			}
		}
		l.closeErr = err
	})
	return l.closeErr
}

func (l *quicStreamListener) Addr() net.Addr {
	return l.qln.Addr()
}

// listenQUIC binds UDP addr and returns a net.Listener that yields stream conns.
func listenQUIC(addr string, tlsConf *tls.Config) (net.Listener, error) {
	if tlsConf == nil {
		return nil, fmt.Errorf("quic listen: nil tls config")
	}
	qln, err := quic.ListenAddr(addr, tlsConf, quicConfig())
	if err != nil {
		return nil, err
	}
	// ListenAddr owns the UDP socket; Listener.Close closes it.
	return newQUICStreamListener(qln, nil, nil), nil
}

// listenQUICPacket wraps an existing PacketConn (e.g. tsnet UDP) as a stream
// listener. The returned listener owns pc and closes it on Close.
func listenQUICPacket(pc net.PacketConn, tlsConf *tls.Config) (net.Listener, error) {
	if tlsConf == nil {
		return nil, fmt.Errorf("quic listen: nil tls config")
	}
	qln, err := quic.Listen(pc, tlsConf, quicConfig())
	if err != nil {
		return nil, err
	}
	return newQUICStreamListener(qln, pc, nil), nil
}

// openClientStream dials a QUIC connection (already established) and opens the
// single application stream used for the framed protocol.
func openClientStream(ctx context.Context, qconn *quic.Conn, onClose func()) (net.Conn, error) {
	str, err := qconn.OpenStreamSync(ctx)
	if err != nil {
		_ = qconn.CloseWithError(1, "open stream")
		if onClose != nil {
			onClose()
		}
		return nil, fmt.Errorf("open quic stream: %w", err)
	}
	return newStreamConn(str, qconn, onClose), nil
}

// dialQUICAddr dials addr with the host UDP stack (host + plain modes).
func dialQUICAddr(ctx context.Context, addr string) (net.Conn, error) {
	qconn, err := quic.DialAddr(ctx, addr, quicClientTLSConfig(), quicConfig())
	if err != nil {
		return nil, err
	}
	return openClientStream(ctx, qconn, nil)
}

// dialQUICPacketTo dials over pc to raddr (tsnet mode). pc is closed when the
// session ends (or on dial failure).
func dialQUICPacketTo(ctx context.Context, pc net.PacketConn, raddr net.Addr) (net.Conn, error) {
	qconn, err := quic.Dial(ctx, pc, raddr, quicClientTLSConfig(), quicConfig())
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	return openClientStream(ctx, qconn, func() { _ = pc.Close() })
}

// pickDialHostsByFamily orders local bind IPs so the remote's address family
// is tried first (avoids v4 local + v6 peer bind mismatches on tsnet).
func pickDialHostsByFamily(hosts []string, remote net.IP) []string {
	if len(hosts) == 0 || remote == nil {
		return hosts
	}
	remote4 := remote.To4() != nil
	var same, other []string
	for _, h := range hosts {
		ip := net.ParseIP(h)
		if ip == nil {
			other = append(other, h)
			continue
		}
		if (ip.To4() != nil) == remote4 {
			same = append(same, h)
		} else {
			other = append(other, h)
		}
	}
	return append(same, other...)
}

// peerIPFromStatus resolves a peer host name (MagicDNS or HostName) to a
// Tailscale IP using LocalAPI status. IP literals are not handled here.
func peerIPFromStatus(st *ipnstate.Status, host string) (netip.Addr, bool) {
	if st == nil {
		return netip.Addr{}, false
	}
	want := strings.ToLower(strings.TrimSuffix(host, "."))
	if want == "" {
		return netip.Addr{}, false
	}
	match := func(p *ipnstate.PeerStatus) bool {
		if p == nil || len(p.TailscaleIPs) == 0 {
			return false
		}
		dns := strings.ToLower(strings.TrimSuffix(p.DNSName, "."))
		hn := strings.ToLower(p.HostName)
		if dns == want || hn == want {
			return true
		}
		// Short name matches MagicDNS prefix (e.g. "peer" vs "peer.tailnet.ts.net").
		if dns != "" && (strings.HasPrefix(dns, want+".") || strings.HasPrefix(want, dns+".")) {
			return true
		}
		return false
	}
	for _, p := range st.Peer {
		if match(p) {
			return p.TailscaleIPs[0], true
		}
	}
	if match(st.Self) {
		return st.Self.TailscaleIPs[0], true
	}
	return netip.Addr{}, false
}

// resolveTSNetUDPAddr resolves host:port for tsnet QUIC dials.
// IP literals use the system parse path; hostnames use tsnet LocalAPI status
// (MagicDNS / HostName), not system DNS.
func resolveTSNetUDPAddr(ctx context.Context, s *tsnet.Server, addr string) (*net.UDPAddr, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("split addr %s: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("invalid port %q in %s", portStr, addr)
	}
	if ip := net.ParseIP(host); ip != nil {
		return &net.UDPAddr{IP: ip, Port: port}, nil
	}
	if s == nil {
		return nil, fmt.Errorf("tsnet not started")
	}
	lc, err := s.LocalClient()
	if err != nil {
		return nil, err
	}
	st, err := lc.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("tsnet status for resolve: %w", err)
	}
	tip, ok := peerIPFromStatus(st, host)
	if !ok {
		return nil, fmt.Errorf("resolve %s: no tailscale peer with that name", host)
	}
	ip := net.ParseIP(tip.String())
	if ip == nil {
		return nil, fmt.Errorf("resolve %s: invalid tailscale ip %v", host, tip)
	}
	return &net.UDPAddr{IP: ip, Port: port}, nil
}
