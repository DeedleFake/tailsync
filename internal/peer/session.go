package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"

	"deedles.dev/tailsync/internal/proto"
)

// StreamHandler is invoked for each inbound application stream after the first
// message has been decoded. Ping is handled internally and never reaches the
// handler.
//
// Lifecycle: the peer accept path always closes stream when the handler returns
// (see handleInboundStream). Handlers may Close earlier when finished; Close is
// idempotent (streamConn closeOnce). Do not use stream after the handler returns.
type StreamHandler func(ctx context.Context, s *Session, first proto.Message, stream net.Conn)

// Session is one persistent QUIC connection to a peer after a successful Hello.
type Session struct {
	nodeID  string
	localID string
	addr    string // dial-back host:port
	dialer  bool
	qconn   *quic.Conn
	log     *slog.Logger

	// healthy is true while heartbeats succeed; false after failure (until Close).
	healthy atomic.Bool
	closed  atomic.Bool
	since   time.Time

	// mu serializes Close's closed flag with stream registration so Add never
	// races after Wait: accept path checks closed under mu before Add; Close
	// sets closed under mu before Wait.
	mu          sync.Mutex
	onClose     func(*Session)
	serveCancel context.CancelFunc
	serveCtx    context.Context

	// streamWG tracks in-flight handleInboundStream goroutines; Close waits
	// so Manager.Close does not return while handlers still run.
	// Daemon also has its own streamWG around application handlers: session
	// Wait drains peer-side dispatch; daemon Wait drains after mesh close so
	// Root/index stay live for finishing serve/pull work (intentional dual).
	streamWG sync.WaitGroup

	closeOnce sync.Once
}

// SessionConfig configures a new session after QUIC + Hello succeed.
type SessionConfig struct {
	NodeID  string
	LocalID string
	Addr    string
	Dialer  bool
	Conn    *quic.Conn
	Log     *slog.Logger
	OnClose func(*Session)
}

// SessionForTest builds a healthy session with no QUIC connection for unit tests
// that exercise Healthy/Closed/Invalidate without a live transport.
func SessionForTest(nodeID, localID string) *Session {
	return newSession(SessionConfig{NodeID: nodeID, LocalID: localID, Dialer: true})
}

// newSession builds a session with a serve context ready so Close is always
// safe before startServe (Install/activate races).
func newSession(cfg SessionConfig) *Session {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	// Detached cancel; linked to manager ctx in startServe.
	serveCtx, serveCancel := context.WithCancel(context.Background())
	s := &Session{
		nodeID:      cfg.NodeID,
		localID:     cfg.LocalID,
		addr:        cfg.Addr,
		dialer:      cfg.Dialer,
		qconn:       cfg.Conn,
		log:         cfg.Log,
		since:       time.Now(),
		onClose:     cfg.OnClose,
		serveCtx:    serveCtx,
		serveCancel: serveCancel,
	}
	s.healthy.Store(true)
	return s
}

func (s *Session) NodeID() string   { return s.nodeID }
func (s *Session) LocalID() string  { return s.localID }
func (s *Session) Addr() string     { return s.addr }
func (s *Session) IsDialer() bool   { return s.dialer }
func (s *Session) Healthy() bool    { return s.healthy.Load() && !s.closed.Load() }
func (s *Session) Closed() bool     { return s.closed.Load() }
func (s *Session) Since() time.Time { return s.since }
func (s *Session) Conn() *quic.Conn { return s.qconn }

// setOnClose updates the close callback under lock (clear before discard).
func (s *Session) setOnClose(f func(*Session)) {
	s.mu.Lock()
	s.onClose = f
	s.mu.Unlock()
}

func (s *Session) info() SessionInfo {
	remote := ""
	if s.qconn != nil && s.qconn.RemoteAddr() != nil {
		remote = s.qconn.RemoteAddr().String()
	}
	return SessionInfo{
		NodeID:  s.nodeID,
		Addr:    s.addr,
		Dialer:  s.dialer,
		Healthy: s.Healthy(),
		Since:   s.since,
		Remote:  remote,
	}
}

// OpenStream opens a bidirectional application stream for one op.
// Context cancel/deadline on the open call does not tear down the session
// (short notify timeouts must not kill healthy peers). Connection-level
// failures mark unhealthy and Close so discovery can replace the session.
//
// Heartbeat pings use a separate internal open path (see ping) that does not
// call OpenStream: ping failures always Invalidate the session, while
// OpenStream preserves soft caller cancel/timeout.
func (s *Session) OpenStream(ctx context.Context) (net.Conn, error) {
	if s.Closed() {
		return nil, net.ErrClosed
	}
	str, err := s.qconn.OpenStreamSync(ctx)
	if err != nil {
		if IsSoftStreamErr(err) {
			return nil, fmt.Errorf("open stream: %w", err)
		}
		s.Invalidate()
		return nil, fmt.Errorf("open stream: %w", err)
	}
	return newStreamConn(str, s.qconn), nil
}

// IsSoftStreamErr reports caller-side cancel/timeout that should not tear down
// the peer connection (heartbeat handles soft unreachability).
func IsSoftStreamErr(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, os.ErrDeadlineExceeded)
}

// markUnhealthy flags the session so discovery/roster can replace it.
func (s *Session) markUnhealthy() {
	s.healthy.Store(false)
}

// Invalidate marks the session unhealthy and closes it so discovery can
// replace it. Idempotent. Callers should not use this for soft cancel/timeout
// (see IsSoftStreamErr); use it for mid-stream framing/connection failures.
func (s *Session) Invalidate() {
	s.markUnhealthy()
	s.Close()
}

// Close tears down the QUIC connection (not the shared PacketConn). Idempotent.
// Waits for in-flight stream handlers after canceling accept so callers (Manager.Close)
// do not race handler use of the session.
//
// closed is set under mu before Wait so the accept loop cannot Add after Wait:
// spawnStreamHandler checks closed under the same mutex before Add.
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed.Store(true)
		s.healthy.Store(false)
		cancel := s.serveCancel
		onClose := s.onClose
		s.onClose = nil
		s.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		if s.qconn != nil {
			_ = s.qconn.CloseWithError(0, "")
		}
		// Handlers registered before closed=true will run to completion.
		// Accept path after closed=true discards without Add (see spawnStreamHandler).
		s.streamWG.Wait()
		if onClose != nil {
			onClose(s)
		}
	})
}

// spawnStreamHandler registers and starts an inbound stream handler, or drops
// the stream if Close has already begun. Add is only called while holding mu
// and closed is false, so it cannot race after Close's Wait.
func (s *Session) spawnStreamHandler(conn net.Conn, onStream StreamHandler) {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	s.streamWG.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.streamWG.Done()
		s.handleInboundStream(s.serveCtx, conn, onStream)
	}()
}

// startServe accepts inbound streams and dispatches them. Ping is handled
// locally; other ops go to onStream. Runs until ctx done or connection dies.
// serveCancel is already set in newSession; this links the parent context.
func (s *Session) startServe(ctx context.Context, onStream StreamHandler) {
	if s.Closed() {
		return
	}
	// Cancel serve when parent (manager) is done.
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			cancel := s.serveCancel
			s.mu.Unlock()
			if cancel != nil {
				cancel()
			}
		case <-s.serveCtx.Done():
		}
	}()

	go func() {
		defer s.Close()
		for {
			str, err := s.qconn.AcceptStream(s.serveCtx)
			if err != nil {
				if s.serveCtx.Err() == nil && !s.Closed() {
					s.log.Debug("session accept stream ended", "peer", s.nodeID, "err", err)
				}
				return
			}
			conn := newStreamConn(str, s.qconn)
			s.spawnStreamHandler(conn, onStream)
		}
	}()
}

func (s *Session) handleInboundStream(ctx context.Context, conn net.Conn, onStream StreamHandler) {
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))
	msg, err := proto.Decode(conn)
	if err != nil {
		if err != io.EOF {
			s.log.Debug("inbound stream decode", "peer", s.nodeID, "err", err)
		}
		return
	}
	if msg.Header.Type == proto.TypePing {
		_ = proto.Encode(conn, proto.NewPong())
		return
	}
	if onStream != nil {
		// Pass the already-decoded first message; do not re-encode for a second Decode.
		onStream(ctx, s, msg, conn)
	}
}

// startHeartbeat sends periodic Ping streams; failures mark unhealthy and Close.
func (s *Session) startHeartbeat(ctx context.Context, interval, timeout time.Duration) {
	if interval <= 0 {
		interval = 20 * time.Second
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.serveCtx.Done():
				return
			case <-t.C:
				if s.Closed() {
					return
				}
				if err := s.ping(ctx, timeout); err != nil {
					s.log.Debug("heartbeat failed", "peer", s.nodeID, "err", err)
					s.Invalidate()
					return
				}
			}
		}
	}()
}

// ping opens a stream and exchanges Ping/Pong. Unlike OpenStream, any failure
// (including timeout) is treated as a hard health failure by the heartbeat loop.
func (s *Session) ping(ctx context.Context, timeout time.Duration) error {
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if s.Closed() {
		return net.ErrClosed
	}
	str, err := s.qconn.OpenStreamSync(pctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	conn := newStreamConn(str, s.qconn)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if err := proto.Encode(conn, proto.NewPing()); err != nil {
		return err
	}
	msg, err := proto.Decode(conn)
	if err != nil {
		return err
	}
	if msg.Header.Type != proto.TypePong {
		return fmt.Errorf("expected pong, got %q", msg.Header.Type)
	}
	return nil
}
