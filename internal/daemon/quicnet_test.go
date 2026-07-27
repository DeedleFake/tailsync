package daemon

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/types/key"
)

func TestPickDialHostsByFamily(t *testing.T) {
	hosts := []string{"100.64.0.1", "fd7a:115c:a1e0::1", "100.64.0.2"}
	v4 := net.ParseIP("100.64.0.9")
	v6 := net.ParseIP("fd7a:115c:a1e0::9")

	got := pickDialHostsByFamily(hosts, v4)
	if len(got) != 3 || got[0] != "100.64.0.1" || got[1] != "100.64.0.2" || got[2] != "fd7a:115c:a1e0::1" {
		t.Fatalf("v4 order: %v", got)
	}
	got = pickDialHostsByFamily(hosts, v6)
	if len(got) != 3 || got[0] != "fd7a:115c:a1e0::1" || got[1] != "100.64.0.1" {
		t.Fatalf("v6 order: %v", got)
	}
	if pickDialHostsByFamily(nil, v4) != nil {
		t.Fatal("empty hosts")
	}
}

func TestPeerIPFromStatus(t *testing.T) {
	st := &ipnstate.Status{
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			key.NewNode().Public(): {
				HostName:     "tailsync-a",
				DNSName:      "tailsync-a.tailnet.ts.net.",
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.2")},
			},
			key.NewNode().Public(): {
				HostName:     "other",
				DNSName:      "other.tailnet.ts.net.",
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.3")},
			},
		},
	}
	ip, ok := peerIPFromStatus(st, "tailsync-a.tailnet.ts.net")
	if !ok || ip.String() != "100.64.0.2" {
		t.Fatalf("fqdn: ok=%v ip=%v", ok, ip)
	}
	ip, ok = peerIPFromStatus(st, "tailsync-a")
	if !ok || ip.String() != "100.64.0.2" {
		t.Fatalf("short/host: ok=%v ip=%v", ok, ip)
	}
	if _, ok := peerIPFromStatus(st, "missing"); ok {
		t.Fatal("expected miss")
	}
}

func TestQUICAcceptDoesNotSerializeOnSlowStream(t *testing.T) {
	// A peer that completes the QUIC handshake but never opens a stream must
	// not block Accept of a subsequent full session.
	tlsConf := testQUICTLS(t)
	ln, err := listenQUIC("127.0.0.1:0", tlsConf)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	// Connection 1: handshake only (no application stream).
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel1()
	qconn1, err := quic.DialAddr(ctx1, addr, quicClientTLSConfig(), quicConfig())
	if err != nil {
		t.Fatalf("handshake-only dial: %v", err)
	}
	defer qconn1.CloseWithError(0, "")

	// Connection 2: full session with a stream and a byte.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	type dialRes struct {
		c   net.Conn
		err error
	}
	done := make(chan dialRes, 1)
	go func() {
		c, err := dialQUICAddr(ctx2, addr)
		if err != nil {
			done <- dialRes{err: err}
			return
		}
		_, err = c.Write([]byte("x"))
		done <- dialRes{c: c, err: err}
	}()

	// Accept must return the second session promptly, not wait 30s for the first.
	accDone := make(chan error, 1)
	go func() {
		sc, err := ln.Accept()
		if err != nil {
			accDone <- err
			return
		}
		var buf [1]byte
		_, err = sc.Read(buf[:])
		_ = sc.Close()
		accDone <- err
	}()

	select {
	case err := <-accDone:
		if err != nil {
			t.Fatalf("accept: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept blocked behind stream-less handshake (serialization bug)")
	}
	select {
	case dr := <-done:
		if dr.err != nil {
			t.Fatalf("second dial: %v", dr.err)
		}
		if dr.c != nil {
			_ = dr.c.Close()
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second dial hung")
	}
}

// TestQUICStreamListenerCloseDuringStreamAcceptNoPanic exercises Close while
// waitStream is blocked on AcceptStream (handshake done, no app stream). ready
// must not be closed under concurrent senders — that used to panic on shutdown.
func TestQUICStreamListenerCloseDuringStreamAcceptNoPanic(t *testing.T) {
	const rounds = 40
	for range rounds {
		tlsConf := testQUICTLS(t)
		ln, err := listenQUIC("127.0.0.1:0", tlsConf)
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		qconn, err := quic.DialAddr(ctx, addr, quicClientTLSConfig(), quicConfig())
		cancel()
		if err != nil {
			_ = ln.Close()
			t.Fatalf("handshake dial: %v", err)
		}

		// Optional Accept waiter (may return ErrClosed).
		accDone := make(chan struct{})
		go func() {
			defer close(accDone)
			c, err := ln.Accept()
			if c != nil {
				_ = c.Close()
			}
			_ = err
		}()

		// Give waitStream time to enter AcceptStream.
		time.Sleep(5 * time.Millisecond)
		if err := ln.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		_ = qconn.CloseWithError(0, "")

		select {
		case <-accDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Accept did not return after Close")
		}
	}
}
