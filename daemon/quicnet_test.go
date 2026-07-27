package daemon

import (
	"net/netip"
	"testing"

	"tailscale.com/ipn/ipnstate"
	"tailscale.com/types/key"
)

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
