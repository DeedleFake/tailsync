package peer

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"deedles.dev/tailsync/internal/proto"
)

func TestIsSoftStreamErr(t *testing.T) {
	if !IsSoftStreamErr(context.Canceled) {
		t.Fatal("canceled")
	}
	if !IsSoftStreamErr(context.DeadlineExceeded) {
		t.Fatal("deadline")
	}
	if !IsSoftStreamErr(errors.Join(context.DeadlineExceeded, errors.New("wrap"))) {
		t.Fatal("wrapped deadline")
	}
	if IsSoftStreamErr(errors.New("connection reset")) {
		t.Fatal("hard error must not be soft")
	}
	if IsSoftStreamErr(nil) {
		t.Fatal("nil")
	}
}

func TestOpenStreamOnClosedSession(t *testing.T) {
	s := newSession(SessionConfig{
		NodeID:  "b",
		LocalID: "a",
		Dialer:  true,
	})
	s.closed.Store(true)
	_, err := s.OpenStream(context.Background())
	if err == nil {
		t.Fatal("expected error on closed session")
	}
	if !s.Closed() {
		t.Fatal("already closed")
	}
}

// TestOpenStreamDeadlineDoesNotInvalidate uses a live QUIC session: a caller
// deadline on OpenStream must not tear down the session; a later open succeeds.
func TestOpenStreamDeadlineDoesNotInvalidate(t *testing.T) {
	client, _, cleanup := liveQUICSession(t)
	defer cleanup()

	if !client.Healthy() {
		t.Fatal("want healthy before soft open")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	_, err := client.OpenStream(ctx)
	if err == nil {
		t.Fatal("expected deadline error")
	}
	if !IsSoftStreamErr(err) {
		t.Fatalf("want soft stream err, got %v", err)
	}
	if !client.Healthy() || client.Closed() {
		t.Fatal("soft deadline must not Invalidate session")
	}

	// Session remains usable for a subsequent open.
	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	stream, err := client.OpenStream(sctx)
	if err != nil {
		t.Fatalf("open after soft fail: %v", err)
	}
	_ = stream.Close()
	if !client.Healthy() {
		t.Fatal("want still healthy")
	}
}

// TestOpenStreamHardErrorInvalidates closes the underlying QUIC conn so open
// fails hard; the session must Invalidate (unhealthy + closed).
func TestOpenStreamHardErrorInvalidates(t *testing.T) {
	client, _, cleanup := liveQUICSession(t)
	defer cleanup()

	if client.qconn == nil {
		t.Fatal("nil qconn")
	}
	_ = client.qconn.CloseWithError(1, "test hard fail")

	_, err := client.OpenStream(context.Background())
	if err == nil {
		t.Fatal("expected open error after conn close")
	}
	if IsSoftStreamErr(err) {
		t.Fatalf("hard fail classified soft: %v", err)
	}
	if client.Healthy() {
		t.Fatal("want unhealthy after hard open fail")
	}
	if !client.Closed() {
		t.Fatal("want closed after hard open fail")
	}
}

func TestInvalidateMarksUnhealthyAndCloses(t *testing.T) {
	s := newSession(SessionConfig{
		NodeID:  "b",
		LocalID: "a",
		Dialer:  true,
	})
	if !s.Healthy() {
		t.Fatal("want healthy")
	}
	s.Invalidate()
	if s.Healthy() {
		t.Fatal("want unhealthy after Invalidate")
	}
	if !s.Closed() {
		t.Fatal("want closed after Invalidate")
	}
	// Idempotent.
	s.Invalidate()
}

// TestSpawnStreamHandlerAfterCloseDoesNotAdd ensures Close's Wait cannot race
// an Add from a late Accept: closed under mu blocks registration.
func TestSpawnStreamHandlerAfterCloseDoesNotAdd(t *testing.T) {
	s := newSession(SessionConfig{
		NodeID:  "b",
		LocalID: "a",
		Dialer:  false,
	})
	s.Close()
	if !s.Closed() {
		t.Fatal("want closed")
	}

	c1, c2 := net.Pipe()
	_ = c2.Close()

	var ran atomic.Bool
	s.spawnStreamHandler(c1, func(ctx context.Context, sess *Session, first proto.Message, stream net.Conn) {
		ran.Store(true)
	})
	// Give a late handler a moment to mis-fire if registration was wrong.
	time.Sleep(30 * time.Millisecond)
	if ran.Load() {
		t.Fatal("handler must not run after Close")
	}

	// Second Close must not hang waiting for a phantom Add.
	done := make(chan struct{})
	go func() {
		s.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second Close hung (possible Add after Wait)")
	}
}

// liveQUICSession dials a local QUIC peer and returns client/server Sessions
// bound to the connection. cleanup closes both endpoints.
func liveQUICSession(t *testing.T) (client, server *Session, cleanup func()) {
	t.Helper()
	serverTLS := testTLS(t)
	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{ALPN},
		MinVersion:         tls.VersionTLS13,
	}

	pcSrv, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	epSrv, err := NewEndpoint(serverTLS, clientTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := epSrv.AddPacketConn(pcSrv); err != nil {
		t.Fatal(err)
	}
	addr := epSrv.LocalAddrs()[0]

	pcCli, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	epCli, err := NewEndpoint(serverTLS, clientTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := epCli.AddPacketConn(pcCli); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type acceptResult struct {
		c   *quic.Conn
		err error
	}
	accCh := make(chan acceptResult, 1)
	go func() {
		c, err := epSrv.Accept(ctx)
		accCh <- acceptResult{c, err}
	}()

	qcli, err := epCli.Dial(ctx, addr)
	if err != nil {
		_ = epSrv.Close()
		_ = epCli.Close()
		t.Fatalf("dial: %v", err)
	}

	var qsrv *quic.Conn
	select {
	case r := <-accCh:
		if r.err != nil {
			_ = qcli.CloseWithError(0, "")
			_ = epSrv.Close()
			_ = epCli.Close()
			t.Fatalf("accept: %v", r.err)
		}
		qsrv = r.c
	case <-ctx.Done():
		_ = qcli.CloseWithError(0, "")
		_ = epSrv.Close()
		_ = epCli.Close()
		t.Fatal("accept timeout")
	}

	client = newSession(SessionConfig{
		NodeID:  "server",
		LocalID: "client",
		Addr:    addr,
		Dialer:  true,
		Conn:    qcli,
	})
	server = newSession(SessionConfig{
		NodeID:  "client",
		LocalID: "server",
		Dialer:  false,
		Conn:    qsrv,
	})
	cleanup = func() {
		client.Close()
		server.Close()
		_ = epSrv.Close()
		_ = epCli.Close()
	}
	return client, server, cleanup
}
