package daemon

import (
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

func TestBindAddrsFromTailscaleIPs(t *testing.T) {
	v4 := netip.MustParseAddr("100.64.0.1")
	v6 := netip.MustParseAddr("fd7a:115c:a1e0::1")
	got := bindAddrsFromTailscaleIPs([]netip.Addr{v4, v6}, 5960)
	want := []string{"100.64.0.1:5960", "[fd7a:115c:a1e0::1]:5960"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %q want %q", i, got[i], want[i])
		}
	}
	if got := bindAddrsFromTailscaleIPs(nil, 5960); len(got) != 0 {
		t.Fatalf("empty ips: %v", got)
	}
}

func TestNodeIDFromSelf(t *testing.T) {
	if nodeIDFromSelf(nil) != "" {
		t.Fatal("nil self")
	}
	if got := nodeIDFromSelf(&ipnstate.PeerStatus{DNSName: "host.tailnet.ts.net."}); got != "host.tailnet.ts.net" {
		t.Fatalf("dns: %q", got)
	}
	if got := nodeIDFromSelf(&ipnstate.PeerStatus{HostName: "myhost"}); got != "myhost" {
		t.Fatalf("host: %q", got)
	}
	if got := nodeIDFromSelf(&ipnstate.PeerStatus{ID: tailcfg.StableNodeID("n123")}); got != "n123" {
		t.Fatalf("id: %q", got)
	}
}

func TestPeersFromStatus(t *testing.T) {
	st := &ipnstate.Status{
		Self: &ipnstate.PeerStatus{
			ID:       "self",
			HostName: "me",
			DNSName:  "me.tailnet.ts.net.",
		},
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			key.NewNode().Public(): {
				ID:           "peer1",
				HostName:     "tailsync-a",
				DNSName:      "tailsync-a.tailnet.ts.net.",
				Online:       true,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.2")},
			},
			key.NewNode().Public(): {
				ID:       "peer2",
				HostName: "other",
				DNSName:  "other.tailnet.ts.net.",
				Online:   false, // offline — skip
			},
			key.NewNode().Public(): {
				ID:       "peer3",
				HostName: "laptop",
				DNSName:  "laptop.tailnet.ts.net.",
				Online:   true, // no IP → MagicDNS fallback
			},
			key.NewNode().Public(): {
				ID:       "self", // same StableID as Self — skip
				HostName: "me",
				DNSName:  "me.tailnet.ts.net.",
				Online:   true,
			},
			// Distinct node sharing Self HostName must still be discovered.
			key.NewNode().Public(): {
				ID:           "clone",
				HostName:     "me",
				DNSName:      "clone.tailnet.ts.net.",
				Online:       true,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.9")},
			},
		},
	}

	got := peersFromStatus(st, 5960, "")
	wantSet := map[string]bool{
		"100.64.0.2:5960":            true, // prefers IP over MagicDNS
		"laptop.tailnet.ts.net:5960": true, // DNS fallback when no IP
		"100.64.0.9:5960":            true, // shared HostName still dialed
	}
	if len(got) != len(wantSet) {
		t.Fatalf("got %v", got)
	}
	for _, a := range got {
		if !wantSet[a] {
			t.Errorf("unexpected addr %q", a)
		}
	}

	// Service filter matches HostName/DNS, not dial IP.
	got = peersFromStatus(st, 5960, "tailsync")
	if len(got) != 1 || got[0] != "100.64.0.2:5960" {
		t.Fatalf("service filter: %v", got)
	}
}

func TestFilterSelfHostname(t *testing.T) {
	in := []string{
		"tailsync-a.tailnet.ts.net:5960",
		"other:5960",
		"tailsync-a:5960",
	}
	got := filterSelfHostname(in, "tailsync-a")
	if len(got) != 1 || got[0] != "other:5960" {
		t.Fatalf("got %v", got)
	}
}

func TestListenAllPartialSuccess(t *testing.T) {
	// One bindable localhost address and one that should fail (TEST-NET-3).
	lnProbe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := lnProbe.Addr().(*net.TCPAddr).Port
	_ = lnProbe.Close()

	good := fmt.Sprintf("127.0.0.1:%d", port)
	bad := fmt.Sprintf("203.0.113.1:%d", port) // documentation range; typically not local

	res, err := listenAll([]string{bad, good})
	if err != nil {
		t.Fatalf("listenAll: %v", err)
	}
	defer res.Listener.Close()

	if len(res.Bound) != 1 || res.Bound[0] != good {
		t.Fatalf("bound=%v want [%s]", res.Bound, good)
	}
	if len(res.Skipped) != 1 || res.Skipped[0] != bad {
		t.Fatalf("skipped=%v want [%s]", res.Skipped, bad)
	}

	// Dial the successful bind.
	c, err := net.Dial("tcp", good)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sc, err := res.Listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = sc.Close()
}

func TestListenAllAllFail(t *testing.T) {
	_, err := listenAll([]string{"203.0.113.1:1", "203.0.113.2:1"})
	if err == nil {
		t.Fatal("expected error when all binds fail")
	}
}

func TestListenAllEmpty(t *testing.T) {
	_, err := listenAll(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMultiListenerAcceptAndClose(t *testing.T) {
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr1 := ln1.Addr().String()
	addr2 := ln2.Addr().String()

	ml := newMultiListener([]net.Listener{ln1, ln2})
	defer ml.Close()

	// Concurrent dials to both underlying listeners.
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for _, addr := range []string{addr1, addr2} {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			c, err := net.DialTimeout("tcp", a, 2*time.Second)
			if err != nil {
				errCh <- err
				return
			}
			_, _ = c.Write([]byte("x"))
			_ = c.Close()
		}(addr)
	}

	accepted := 0
	deadline := time.After(3 * time.Second)
	for accepted < 2 {
		type acc struct {
			c   net.Conn
			err error
		}
		ch := make(chan acc, 1)
		go func() {
			c, err := ml.Accept()
			ch <- acc{c, err}
		}()
		select {
		case <-deadline:
			t.Fatal("timeout waiting for accepts")
		case a := <-ch:
			if a.err != nil {
				t.Fatalf("accept: %v", a.err)
			}
			io.Copy(io.Discard, a.c)
			_ = a.c.Close()
			accepted++
		}
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Close should unblock Accept with a closed error.
	done := make(chan error, 1)
	go func() {
		_, err := ml.Accept()
		done <- err
	}()
	// Give Accept a moment to block.
	time.Sleep(20 * time.Millisecond)
	if err := ml.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not return after Close")
	}
}

func TestMultiListenerOneSideClosedStillAccepts(t *testing.T) {
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr2 := ln2.Addr().String()

	ml := newMultiListener([]net.Listener{ln1, ln2})
	defer ml.Close()

	// Close only one underlying listener: multiListener must keep accepting on the other.
	if err := ln1.Close(); err != nil {
		t.Fatal(err)
	}
	// Brief pause so the ln1 Accept loop exits.
	time.Sleep(20 * time.Millisecond)

	c, err := net.DialTimeout("tcp", addr2, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	sc, err := ml.Accept()
	if err != nil {
		t.Fatalf("accept after one side closed: %v", err)
	}
	_ = sc.Close()
}
