package peer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// ALPN is the TLS NextProto for tailsync peer sessions.
// Required by quic-go; not a wire-protocol version (that is proto.Version).
const ALPN = "tailsync"

// DefaultMaxIncomingStreams allows concurrent op streams (notify, pull, heartbeat).
// Inbound heavy serve work is also capped by the daemon serveSem.
const DefaultMaxIncomingStreams = 32

// Endpoint owns one or more shared UDP PacketConns, each wrapped in a
// quic-go Transport so Listen and Dial share the same socket. Sessions must
// never close the PacketConn; only Endpoint.Close does.
type Endpoint struct {
	serverTLS *tls.Config
	clientTLS *tls.Config
	quicConf  *quic.Config
	log       *slog.Logger

	mu     sync.Mutex
	binds  []bind
	closed bool

	// accept fan-in
	acceptCh chan acceptResult
	acceptWG sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

type bind struct {
	pc        net.PacketConn
	transport *quic.Transport
	listener  *quic.Listener
	localIP   net.IP // may be nil if unspecified
}

type acceptResult struct {
	conn *quic.Conn
	err  error
}

// NewEndpoint builds an endpoint. serverTLS must include certificates; clientTLS
// is used for dials (typically InsecureSkipVerify + same ALPN).
func NewEndpoint(serverTLS, clientTLS *tls.Config, log *slog.Logger) (*Endpoint, error) {
	if serverTLS == nil {
		return nil, fmt.Errorf("nil server tls config")
	}
	if clientTLS == nil {
		clientTLS = &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{ALPN},
			MinVersion:         tls.VersionTLS13,
		}
	}
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Endpoint{
		serverTLS: serverTLS.Clone(),
		clientTLS: clientTLS.Clone(),
		quicConf:  defaultQUICConfig(),
		log:       log,
		acceptCh:  make(chan acceptResult),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func defaultQUICConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:        10 * time.Minute,
		KeepAlivePeriod:       30 * time.Second,
		MaxIncomingStreams:    DefaultMaxIncomingStreams,
		MaxIncomingUniStreams: 0,
	}
}

// AddPacketConn registers pc for shared listen+dial and starts accepting QUIC
// connections. The endpoint owns pc and closes it on Close.
func (e *Endpoint) AddPacketConn(pc net.PacketConn) error {
	if pc == nil {
		return fmt.Errorf("nil packet conn")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		_ = pc.Close()
		return net.ErrClosed
	}

	tr := &quic.Transport{Conn: pc}
	ln, err := tr.Listen(e.serverTLS, e.quicConf)
	if err != nil {
		_ = tr.Close()
		_ = pc.Close()
		return fmt.Errorf("quic listen: %w", err)
	}

	b := bind{
		pc:        pc,
		transport: tr,
		listener:  ln,
		localIP:   packetConnIP(pc),
	}
	e.binds = append(e.binds, b)
	e.acceptWG.Add(1)
	go e.acceptLoop(b)
	return nil
}

func packetConnIP(pc net.PacketConn) net.IP {
	if pc == nil || pc.LocalAddr() == nil {
		return nil
	}
	switch a := pc.LocalAddr().(type) {
	case *net.UDPAddr:
		return a.IP
	default:
		host, _, err := net.SplitHostPort(a.String())
		if err != nil {
			return net.ParseIP(a.String())
		}
		return net.ParseIP(host)
	}
}

func (e *Endpoint) acceptLoop(b bind) {
	defer e.acceptWG.Done()
	for {
		conn, err := b.listener.Accept(e.ctx)
		if err != nil {
			if e.ctx.Err() != nil || errors.Is(err, quic.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case e.acceptCh <- acceptResult{err: err}:
			case <-e.ctx.Done():
				return
			}
			// Permanent listener failure: stop this loop only.
			return
		}
		select {
		case e.acceptCh <- acceptResult{conn: conn}:
		case <-e.ctx.Done():
			_ = conn.CloseWithError(0, "shutdown")
			return
		}
	}
}

// Accept returns the next inbound QUIC connection.
func (e *Endpoint) Accept(ctx context.Context) (*quic.Conn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.ctx.Done():
		return nil, net.ErrClosed
	case r, ok := <-e.acceptCh:
		if !ok {
			return nil, net.ErrClosed
		}
		if r.err != nil {
			return nil, r.err
		}
		if e.ctx.Err() != nil {
			if r.conn != nil {
				_ = r.conn.CloseWithError(0, "shutdown")
			}
			return nil, net.ErrClosed
		}
		return r.conn, nil
	}
}

// Dial opens a QUIC connection to addr using a family-matched shared transport.
// The returned connection does not own the PacketConn.
func (e *Endpoint) Dial(ctx context.Context, addr string) (*quic.Conn, error) {
	raddr, err := resolveUDPAddr(addr)
	if err != nil {
		return nil, err
	}
	return e.DialAddr(ctx, raddr)
}

// DialAddr dials a pre-resolved UDP address (e.g. after tsnet name resolution).
func (e *Endpoint) DialAddr(ctx context.Context, raddr net.Addr) (*quic.Conn, error) {
	if raddr == nil {
		return nil, fmt.Errorf("nil remote addr")
	}
	var ip net.IP
	switch a := raddr.(type) {
	case *net.UDPAddr:
		ip = a.IP
	default:
		host, _, err := net.SplitHostPort(raddr.String())
		if err == nil {
			ip = net.ParseIP(host)
		}
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, net.ErrClosed
	}
	binds := pickBindsByFamily(e.binds, ip)
	e.mu.Unlock()
	if len(binds) == 0 {
		return nil, fmt.Errorf("no local transport for dial %s", raddr)
	}
	var lastErr error
	for _, b := range binds {
		conn, err := b.transport.Dial(ctx, raddr, e.clientTLS, e.quicConf)
		if err != nil {
			lastErr = err
			continue
		}
		return conn, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("dial %s: no transport", raddr)
	}
	return nil, lastErr
}

func pickBindsByFamily(binds []bind, remote net.IP) []bind {
	if len(binds) == 0 {
		return nil
	}
	if remote == nil {
		return binds
	}
	remote4 := remote.To4() != nil
	var same, other []bind
	for _, b := range binds {
		if b.localIP == nil || b.localIP.IsUnspecified() {
			// Unspecified / dual-stack: try after family-matched.
			other = append(other, b)
			continue
		}
		if (b.localIP.To4() != nil) == remote4 {
			same = append(same, b)
		} else {
			other = append(other, b)
		}
	}
	return append(same, other...)
}

func resolveUDPAddr(addr string) (*net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("split addr %s: %w", addr, err)
	}
	// Prefer literal parse (no DNS) for Tailscale IPs and localhost tests.
	if ip := net.ParseIP(host); ip != nil {
		p, err := net.LookupPort("udp", port)
		if err != nil {
			return nil, fmt.Errorf("port %s: %w", port, err)
		}
		return &net.UDPAddr{IP: ip, Port: p}, nil
	}
	return net.ResolveUDPAddr("udp", addr)
}

// LocalAddrs returns bound listen addresses.
func (e *Endpoint) LocalAddrs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.binds))
	for _, b := range e.binds {
		if b.listener != nil {
			out = append(out, b.listener.Addr().String())
		} else if b.pc != nil && b.pc.LocalAddr() != nil {
			out = append(out, b.pc.LocalAddr().String())
		}
	}
	return out
}

// Close stops accepting, closes transports and owned PacketConns.
func (e *Endpoint) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	binds := e.binds
	e.binds = nil
	e.mu.Unlock()

	e.cancel()
	var first error
	for _, b := range binds {
		if b.listener != nil {
			if err := b.listener.Close(); err != nil && first == nil {
				first = err
			}
		}
		// Transport.Close does not close an externally supplied PacketConn.
		if b.transport != nil {
			if err := b.transport.Close(); err != nil && first == nil {
				first = err
			}
		}
		if b.pc != nil {
			if err := b.pc.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	e.acceptWG.Wait()
	return first
}

// AddrsString joins local addresses for logging.
func (e *Endpoint) AddrsString() string {
	return strings.Join(e.LocalAddrs(), ",")
}
