package daemon

import (
	"net"
	"net/netip"
	"testing"

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
