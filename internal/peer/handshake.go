package peer

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"

	"deedles.dev/tailsync/internal/proto"
)

// handshakeTimeout bounds Hello exchange on a new connection.
const defaultHandshakeTimeout = 30 * time.Second

// outboundHandshake opens the first stream, sends Hello, and reads HelloOK or
// AlreadyConnected. On success returns remote node ID and port.
func outboundHandshake(ctx context.Context, qconn *quic.Conn, localID string, localPort int, timeout time.Duration) (remoteID string, remotePort int, err error) {
	if timeout <= 0 {
		timeout = defaultHandshakeTimeout
	}
	hctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	str, err := qconn.OpenStreamSync(hctx)
	if err != nil {
		return "", 0, fmt.Errorf("open handshake stream: %w", err)
	}
	// Handshake stream is closed after exchange; connection stays up.
	defer func() {
		_ = str.Close()
		str.CancelRead(0)
	}()

	_ = str.SetDeadline(time.Now().Add(timeout))
	if err := proto.Encode(str, proto.NewHello(localID, localPort)); err != nil {
		return "", 0, fmt.Errorf("encode hello: %w", err)
	}
	msg, err := proto.Decode(str)
	if err != nil {
		return "", 0, fmt.Errorf("hello response: %w", err)
	}
	switch msg.Header.Type {
	case proto.TypeHelloOK:
		if msg.Header.Version != proto.Version {
			return "", 0, fmt.Errorf("unsupported version %d", msg.Header.Version)
		}
		if msg.Header.NodeID == "" {
			return "", 0, fmt.Errorf("empty remote node id")
		}
		if msg.Header.NodeID == localID {
			return "", 0, fmt.Errorf("connected to self")
		}
		return msg.Header.NodeID, msg.Header.Port, nil
	case proto.TypeAlreadyConnected:
		return msg.Header.NodeID, msg.Header.Port, errAlreadyConnected
	case proto.TypeError:
		return "", 0, fmt.Errorf("hello error: %s", msg.Header.Error)
	default:
		return "", 0, fmt.Errorf("unexpected hello response %q", msg.Header.Type)
	}
}

// errAlreadyConnected is returned when the peer rejects a redundant connection.
// Discovery treats it as a soft-fail so we park outbound dials until inbound
// settles or backoff expires.
var errAlreadyConnected = fmt.Errorf("already connected")

// inboundHandshake accepts the first stream and processes Hello.
// Order: decode Hello → decide (roster) → verify (identity) → HelloOK /
// AlreadyConnected / error. Verify runs before HelloOK so a mismatched claim
// never receives a successful handshake.
//
// decide reports whether to accept the connection (roster TOCTOU is resolved
// again at Install). verify, if non-nil, validates remoteID against the peer
// address; on error an error response is written and accepted is false.
func inboundHandshake(
	ctx context.Context,
	qconn *quic.Conn,
	localID string,
	localPort int,
	timeout time.Duration,
	decide func(remoteID string) bool,
	verify func(ctx context.Context, remoteID string) error,
) (remoteID string, remotePort int, accepted bool, err error) {
	if timeout <= 0 {
		timeout = defaultHandshakeTimeout
	}
	hctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	str, err := qconn.AcceptStream(hctx)
	if err != nil {
		return "", 0, false, fmt.Errorf("accept handshake stream: %w", err)
	}
	defer func() {
		_ = str.Close()
		str.CancelRead(0)
	}()

	_ = str.SetDeadline(time.Now().Add(timeout))
	msg, err := proto.Decode(str)
	if err != nil {
		return "", 0, false, fmt.Errorf("decode hello: %w", err)
	}
	if msg.Header.Type != proto.TypeHello {
		_ = proto.Encode(str, proto.NewError("expected hello"))
		return "", 0, false, fmt.Errorf("expected hello, got %q", msg.Header.Type)
	}
	if msg.Header.Version != proto.Version {
		_ = proto.Encode(str, proto.NewError(fmt.Sprintf("unsupported version %d", msg.Header.Version)))
		return "", 0, false, fmt.Errorf("unsupported version %d", msg.Header.Version)
	}
	remoteID = msg.Header.NodeID
	remotePort = msg.Header.Port
	if remoteID == "" {
		_ = proto.Encode(str, proto.NewError("empty node id"))
		return "", 0, false, fmt.Errorf("empty remote node id")
	}
	if remoteID == localID {
		_ = proto.Encode(str, proto.NewError("connected to self"))
		return remoteID, remotePort, false, fmt.Errorf("connected to self")
	}

	if !decide(remoteID) {
		_ = proto.Encode(str, proto.NewAlreadyConnected(localID, localPort))
		return remoteID, remotePort, false, nil
	}
	if verify != nil {
		if verr := verify(hctx, remoteID); verr != nil {
			_ = proto.Encode(str, proto.NewError("identity mismatch"))
			return remoteID, remotePort, false, verr
		}
	}
	if err := proto.Encode(str, proto.NewHelloOK(localID, localPort)); err != nil {
		return remoteID, remotePort, false, fmt.Errorf("encode hello_ok: %w", err)
	}
	return remoteID, remotePort, true, nil
}

// dialBackAddr builds host:port for re-dialing a peer from an accepted connection.
func dialBackAddr(remoteAddr string, advertisedPort, localDefault int) string {
	host, _, err := net.SplitHostPort(remoteAddr)
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
