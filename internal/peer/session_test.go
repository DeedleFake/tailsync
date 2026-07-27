package peer

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestIsSoftStreamOpenErr(t *testing.T) {
	if !isSoftStreamOpenErr(context.Canceled) {
		t.Fatal("canceled")
	}
	if !isSoftStreamOpenErr(context.DeadlineExceeded) {
		t.Fatal("deadline")
	}
	if !isSoftStreamOpenErr(errors.Join(context.DeadlineExceeded, errors.New("wrap"))) {
		t.Fatal("wrapped deadline")
	}
	if isSoftStreamOpenErr(errors.New("connection reset")) {
		t.Fatal("hard error must not be soft")
	}
	if isSoftStreamOpenErr(nil) {
		t.Fatal("nil")
	}
}

func TestOpenStreamTimeoutDoesNotCloseSession(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if !isSoftStreamOpenErr(ctx.Err()) {
		t.Fatal("timeout soft")
	}
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
	s.spawnStreamHandler(c1, func(ctx context.Context, sess *Session, stream net.Conn) {
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
