package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/views"

	"deedles.dev/tailsync/internal/delta"
	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/peer"
)

// test user IDs for PeerStatus / WhoIs ownership fixtures
const (
	testUserSelf  tailcfg.UserID = 1001
	testUserOther tailcfg.UserID = 2002
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
			UserID:   testUserSelf,
		},
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			key.NewNode().Public(): {
				ID:           "peer1",
				HostName:     "tailsync-a",
				DNSName:      "tailsync-a.tailnet.ts.net.",
				Online:       true,
				UserID:       testUserSelf,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.2")},
			},
			key.NewNode().Public(): {
				ID:       "peer2",
				HostName: "other",
				DNSName:  "other.tailnet.ts.net.",
				Online:   false, // offline — skip
				UserID:   testUserSelf,
			},
			key.NewNode().Public(): {
				ID:       "peer3",
				HostName: "laptop",
				DNSName:  "laptop.tailnet.ts.net.",
				Online:   true, // no IP → MagicDNS fallback
				UserID:   testUserSelf,
			},
			key.NewNode().Public(): {
				ID:       "self", // same StableID as Self — skip
				HostName: "me",
				DNSName:  "me.tailnet.ts.net.",
				Online:   true,
				UserID:   testUserSelf,
			},
			// Distinct node sharing Self HostName must still be discovered.
			key.NewNode().Public(): {
				ID:           "clone",
				HostName:     "me",
				DNSName:      "clone.tailnet.ts.net.",
				Online:       true,
				UserID:       testUserSelf,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.9")},
			},
		},
	}

	got := peersFromStatus(st, 5960, TagMatchIntersection)
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
}

func TestPeersFromStatusUntaggedSelf(t *testing.T) {
	// Realistic tagged node uses synthetic tagged-devices UserID, not Self.
	const taggedDevicesUser tailcfg.UserID = 9999
	tagServer := views.SliceOf([]string{"tag:server"})
	mullvadTags := views.SliceOf([]string{mullvadExitNodeTag})
	st := &ipnstate.Status{
		Self: &ipnstate.PeerStatus{
			ID:      "self",
			DNSName: "me.tailnet.ts.net.",
			UserID:  testUserSelf,
		},
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			key.NewNode().Public(): {
				ID:           "mine",
				HostName:     "laptop",
				DNSName:      "laptop.tailnet.ts.net.",
				Online:       true,
				UserID:       testUserSelf,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.2")},
			},
			// Same UserID + tags still allowed for untagged Self (policy is UserID).
			key.NewNode().Public(): {
				ID:           "tagged-same-uid",
				HostName:     "server",
				DNSName:      "server.tailnet.ts.net.",
				Online:       true,
				UserID:       testUserSelf,
				Tags:         &tagServer,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.8")},
			},
			// Typical tagged node: tagged-devices user — not same UserID.
			key.NewNode().Public(): {
				ID:           "tagged-devices",
				HostName:     "nas",
				DNSName:      "nas.tailnet.ts.net.",
				Online:       true,
				UserID:       taggedDevicesUser,
				Tags:         &tagServer,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.80")},
			},
			// Other user's machine on the same multi-user tailnet.
			key.NewNode().Public(): {
				ID:           "coworker",
				HostName:     "coworker-pc",
				DNSName:      "coworker-pc.tailnet.ts.net.",
				Online:       true,
				UserID:       testUserOther,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.3")},
			},
			// Shared-in device (foreign owner).
			key.NewNode().Public(): {
				ID:           "shared-in",
				HostName:     "friend-phone",
				DNSName:      "friend-phone.other.ts.net.",
				Online:       true,
				UserID:       testUserOther,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.4")},
			},
			// Sharee-only netmap entry (recipient of a share we made).
			key.NewNode().Public(): {
				ID:           "sharee",
				HostName:     "sharee-laptop",
				DNSName:      "sharee-laptop.other.ts.net.",
				Online:       true,
				UserID:       testUserOther,
				ShareeNode:   true,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.5")},
			},
			// Same user id but ShareeNode must still be excluded.
			key.NewNode().Public(): {
				ID:           "weird-sharee",
				HostName:     "weird",
				DNSName:      "weird.tailnet.ts.net.",
				Online:       true,
				UserID:       testUserSelf,
				ShareeNode:   true,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.6")},
			},
			// Zero UserID on peer — fail closed.
			key.NewNode().Public(): {
				ID:           "unknown-owner",
				HostName:     "unknown",
				DNSName:      "unknown.tailnet.ts.net.",
				Online:       true,
				UserID:       0,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.7")},
			},
			// User-run exit node owned by self remains a candidate.
			key.NewNode().Public(): {
				ID:             "home-exit",
				HostName:       "home-exit",
				DNSName:        "home-exit.tailnet.ts.net.",
				Online:         true,
				UserID:         testUserSelf,
				ExitNodeOption: true,
				TailscaleIPs:   []netip.Addr{netip.MustParseAddr("100.64.0.60")},
			},
			// Mullvad by tag (even if UserID matched — real shape uses tag).
			key.NewNode().Public(): {
				ID:             "mullvad-tag",
				HostName:       "se-mma-wg-001",
				DNSName:        "se-mma-wg-001.tailnet.ts.net.",
				Online:         true,
				UserID:         testUserSelf,
				ExitNodeOption: true,
				Tags:           &mullvadTags,
				TailscaleIPs:   []netip.Addr{netip.MustParseAddr("100.64.0.50")},
			},
			// Mullvad by DNS suffix alone (without relying on other UserID).
			key.NewNode().Public(): {
				ID:             "mullvad-dns",
				HostName:       "se-mma-wg-002",
				DNSName:        "se-mma-wg-002.mullvad.ts.net.",
				Online:         true,
				UserID:         testUserSelf,
				ExitNodeOption: true,
				TailscaleIPs:   []netip.Addr{netip.MustParseAddr("100.64.0.51")},
			},
		},
	}

	got := peersFromStatus(st, 5960, TagMatchIntersection)
	wantSet := map[string]bool{
		"100.64.0.2:5960":  true,
		"100.64.0.8:5960":  true, // same UserID even if tagged
		"100.64.0.60:5960": true, // user-run exit node
	}
	if len(got) != len(wantSet) {
		t.Fatalf("got %v", got)
	}
	for _, a := range got {
		if !wantSet[a] {
			t.Errorf("unexpected addr %q", a)
		}
	}
}

func TestPeersFromStatusTaggedSelf(t *testing.T) {
	const taggedDevicesUser tailcfg.UserID = 9999
	tagServer := views.SliceOf([]string{"tag:server"})
	tagWeb := views.SliceOf([]string{"tag:web"})
	tagServerWeb := views.SliceOf([]string{"tag:server", "tag:web"})
	tagExact := views.SliceOf([]string{"tag:server"})
	st := &ipnstate.Status{
		Self: &ipnstate.PeerStatus{
			ID:      "self",
			DNSName: "self.tailnet.ts.net.",
			UserID:  taggedDevicesUser,
			Tags:    &tagServer,
		},
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			// Intersecting tag — allow under default intersect.
			key.NewNode().Public(): {
				ID:           "peer-server",
				Online:       true,
				UserID:       taggedDevicesUser,
				Tags:         &tagExact,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.10")},
			},
			// Superset of Self tags — allow under intersect and contains.
			key.NewNode().Public(): {
				ID:           "peer-both",
				Online:       true,
				UserID:       taggedDevicesUser,
				Tags:         &tagServerWeb,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.11")},
			},
			// Disjoint tags — never.
			key.NewNode().Public(): {
				ID:           "peer-web",
				Online:       true,
				UserID:       taggedDevicesUser,
				Tags:         &tagWeb,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.12")},
			},
			// Untagged same synthetic user — tagged Self ignores UserID.
			key.NewNode().Public(): {
				ID:           "untagged",
				Online:       true,
				UserID:       taggedDevicesUser,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.13")},
			},
			// Human user machine — not tagged.
			key.NewNode().Public(): {
				ID:           "laptop",
				Online:       true,
				UserID:       testUserSelf,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.14")},
			},
		},
	}

	got := peersFromStatus(st, 5960, TagMatchIntersection)
	wantIntersect := map[string]bool{
		"100.64.0.10:5960": true,
		"100.64.0.11:5960": true,
	}
	if len(got) != len(wantIntersect) {
		t.Fatalf("intersect: got %v", got)
	}
	for _, a := range got {
		if !wantIntersect[a] {
			t.Errorf("intersect unexpected %q", a)
		}
	}

	got = peersFromStatus(st, 5960, TagMatchEqual)
	wantEqual := map[string]bool{"100.64.0.10:5960": true}
	if len(got) != len(wantEqual) {
		t.Fatalf("equal: got %v", got)
	}
	for _, a := range got {
		if !wantEqual[a] {
			t.Errorf("equal unexpected %q", a)
		}
	}

	got = peersFromStatus(st, 5960, TagMatchContains)
	wantContains := map[string]bool{
		"100.64.0.10:5960": true, // exact set contains all of self
		"100.64.0.11:5960": true, // superset
	}
	if len(got) != len(wantContains) {
		t.Fatalf("contains: got %v", got)
	}
	for _, a := range got {
		if !wantContains[a] {
			t.Errorf("contains unexpected %q", a)
		}
	}
}

func TestPeersFromStatusFailClosedNoSelfUser(t *testing.T) {
	peer := map[key.NodePublic]*ipnstate.PeerStatus{
		key.NewNode().Public(): {
			ID:           "peer1",
			Online:       true,
			UserID:       testUserSelf,
			TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.2")},
		},
	}
	if got := peersFromStatus(nil, 5960, TagMatchIntersection); len(got) != 0 {
		t.Fatalf("nil status: %v", got)
	}
	if got := peersFromStatus(&ipnstate.Status{Peer: peer}, 5960, TagMatchIntersection); len(got) != 0 {
		t.Fatalf("nil self: %v", got)
	}
	if got := peersFromStatus(&ipnstate.Status{
		Self: &ipnstate.PeerStatus{ID: "self", UserID: 0},
		Peer: peer,
	}, 5960, TagMatchIntersection); len(got) != 0 {
		t.Fatalf("zero self UserID: %v", got)
	}
	// Tagged Self does not use UserID; untagged peer is not a candidate.
	tagServer := views.SliceOf([]string{"tag:server"})
	got := peersFromStatus(&ipnstate.Status{
		Self: &ipnstate.PeerStatus{ID: "self", UserID: testUserSelf, Tags: &tagServer},
		Peer: peer,
	}, 5960, TagMatchIntersection)
	if len(got) != 0 {
		t.Fatalf("tagged self must not discover untagged peers via UserID: %v", got)
	}
}

func TestTrustedMeshPeer(t *testing.T) {
	self := meshIdentity{user: testUserSelf}
	if trustedMeshPeer(self, meshIdentity{}, TagMatchIntersection) {
		t.Fatal("zero peer user")
	}
	if trustedMeshPeer(meshIdentity{}, meshIdentity{user: testUserSelf}, TagMatchIntersection) {
		t.Fatal("zero self user")
	}
	if trustedMeshPeer(self, meshIdentity{user: testUserOther}, TagMatchIntersection) {
		t.Fatal("other user")
	}
	if trustedMeshPeer(self, meshIdentity{user: testUserSelf, sharee: true}, TagMatchIntersection) {
		t.Fatal("sharee")
	}
	if !trustedMeshPeer(self, meshIdentity{user: testUserSelf}, TagMatchIntersection) {
		t.Fatal("same user")
	}

	// Untagged self: UserID match even if peer is tagged.
	if !trustedMeshPeer(self, meshIdentity{user: testUserSelf, tags: []string{"tag:server"}}, TagMatchIntersection) {
		t.Fatal("untagged self + tagged peer same user")
	}

	// Tagged self: tags only; UserID ignored.
	taggedSelf := meshIdentity{user: 9999, tags: []string{"tag:server"}}
	if trustedMeshPeer(taggedSelf, meshIdentity{user: 9999}, TagMatchIntersection) {
		t.Fatal("tagged self must not trust untagged peer")
	}
	if !trustedMeshPeer(taggedSelf, meshIdentity{user: 1, tags: []string{"tag:server", "tag:web"}}, TagMatchIntersection) {
		t.Fatal("intersect")
	}
	if trustedMeshPeer(taggedSelf, meshIdentity{tags: []string{"tag:web"}}, TagMatchIntersection) {
		t.Fatal("disjoint tags")
	}
	if !trustedMeshPeer(taggedSelf, meshIdentity{tags: []string{"tag:server"}}, TagMatchEqual) {
		t.Fatal("equal")
	}
	if trustedMeshPeer(taggedSelf, meshIdentity{tags: []string{"tag:server", "tag:web"}}, TagMatchEqual) {
		t.Fatal("equal rejects superset")
	}
	if !trustedMeshPeer(taggedSelf, meshIdentity{tags: []string{"tag:server", "tag:web"}}, TagMatchContains) {
		t.Fatal("contains superset")
	}
	if trustedMeshPeer(meshIdentity{tags: []string{"tag:server", "tag:web"}}, meshIdentity{tags: []string{"tag:server"}}, TagMatchContains) {
		t.Fatal("contains rejects missing tag")
	}

	// Mullvad always denied.
	if trustedMeshPeer(self, meshIdentity{user: testUserSelf, tags: []string{mullvadExitNodeTag}}, TagMatchIntersection) {
		t.Fatal("mullvad tag")
	}
	if trustedMeshPeer(self, meshIdentity{user: testUserSelf, dns: "se.mullvad.ts.net."}, TagMatchIntersection) {
		t.Fatal("mullvad dns")
	}
	if trustedMeshPeer(taggedSelf, meshIdentity{tags: []string{mullvadExitNodeTag, "tag:server"}}, TagMatchIntersection) {
		t.Fatal("mullvad among tags")
	}
}

func TestIsMullvadPeer(t *testing.T) {
	if isMullvadPeer(nil) {
		t.Fatal("nil")
	}
	if isMullvadPeer(&ipnstate.PeerStatus{DNSName: "laptop.tailnet.ts.net."}) {
		t.Fatal("normal peer")
	}
	mullvadTags := views.SliceOf([]string{mullvadExitNodeTag})
	if !isMullvadPeer(&ipnstate.PeerStatus{
		DNSName: "se.tailnet.ts.net.",
		Tags:    &mullvadTags,
	}) {
		t.Fatal("tag")
	}
	if !isMullvadPeer(&ipnstate.PeerStatus{DNSName: "se-mma-wg-001.mullvad.ts.net."}) {
		t.Fatal("dns with trailing dot")
	}
	if !isMullvadPeer(&ipnstate.PeerStatus{DNSName: "se-mma-wg-001.mullvad.ts.net"}) {
		t.Fatal("dns without trailing dot")
	}
	if isMullvadPeer(&ipnstate.PeerStatus{
		DNSName:        "home-exit.tailnet.ts.net.",
		ExitNodeOption: true,
	}) {
		t.Fatal("ExitNodeOption alone must not mark Mullvad")
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

func TestClaimedMatchesWhoIs(t *testing.T) {
	// Minimal WhoIs-shaped node via apitype is awkward without full Node;
	// exercise matching helper with constructed response.
	who := &apitype.WhoIsResponse{
		Node: &tailcfg.Node{
			Name:     "peer.tailnet.ts.net.",
			StableID: "nStable1",
		},
	}
	if !claimedMatchesWhoIs("peer.tailnet.ts.net", who) {
		t.Fatal("magicdns")
	}
	if !claimedMatchesWhoIs("peer", who) {
		t.Fatal("short name first-label of FQDN")
	}
	if !claimedMatchesWhoIs("nStable1", who) {
		t.Fatal("stable id")
	}
	if claimedMatchesWhoIs("other", who) {
		t.Fatal("mismatch should fail")
	}
	if claimedMatchesWhoIs("", who) {
		t.Fatal("empty claim")
	}
	// Reverse prefix must not match (would allow multi-identity extension).
	if claimedMatchesWhoIs("peer.tailnet.ts.net.evil", who) {
		t.Fatal("extended claim must not match via reverse prefix")
	}
	if claimedMatchesWhoIs("peer.extra", who) {
		t.Fatal("claim longer than first label must not match")
	}
}

func TestMeshPeerAllowed(t *testing.T) {
	self := &ipnstate.PeerStatus{UserID: testUserSelf}
	same := &apitype.WhoIsResponse{
		Node: &tailcfg.Node{
			Name: "peer.tailnet.ts.net.",
			User: testUserSelf,
		},
		UserProfile: &tailcfg.UserProfile{ID: testUserSelf},
	}
	if !meshPeerAllowed(self, same, TagMatchIntersection) {
		t.Fatal("same user")
	}

	other := &apitype.WhoIsResponse{
		Node: &tailcfg.Node{
			Name: "other.tailnet.ts.net.",
			User: testUserOther,
		},
		UserProfile: &tailcfg.UserProfile{ID: testUserOther},
	}
	if meshPeerAllowed(self, other, TagMatchIntersection) {
		t.Fatal("other user")
	}

	if meshPeerAllowed(self, &apitype.WhoIsResponse{
		Node: &tailcfg.Node{Name: "peer.tailnet.ts.net.", User: 0},
	}, TagMatchIntersection) {
		t.Fatal("zero peer User must fail closed")
	}
	if meshPeerAllowed(&ipnstate.PeerStatus{UserID: 0}, same, TagMatchIntersection) {
		t.Fatal("zero self UserID must fail closed")
	}
	if meshPeerAllowed(nil, same, TagMatchIntersection) {
		t.Fatal("nil self")
	}
	if meshPeerAllowed(self, nil, TagMatchIntersection) {
		t.Fatal("nil who")
	}

	// UserProfile disagrees with Self — fail closed on untagged path.
	if meshPeerAllowed(self, &apitype.WhoIsResponse{
		Node:        &tailcfg.Node{User: testUserSelf},
		UserProfile: &tailcfg.UserProfile{ID: testUserOther},
	}, TagMatchIntersection) {
		t.Fatal("profile mismatch")
	}

	// ShareeNode must not authenticate.
	hi := (&tailcfg.Hostinfo{ShareeNode: true}).View()
	shareeNode := &tailcfg.Node{User: testUserSelf}
	shareeNode.Hostinfo = hi
	if meshPeerAllowed(self, &apitype.WhoIsResponse{Node: shareeNode}, TagMatchIntersection) {
		t.Fatal("sharee node")
	}

	// Untagged self allows same-user tagged peer.
	if !meshPeerAllowed(self, &apitype.WhoIsResponse{
		Node: &tailcfg.Node{User: testUserSelf, Tags: []string{"tag:server"}},
	}, TagMatchIntersection) {
		t.Fatal("untagged self + tagged peer same user")
	}

	// Tagged self: tag match, not UserID.
	tagServer := views.SliceOf([]string{"tag:server"})
	taggedSelf := &ipnstate.PeerStatus{UserID: 9999, Tags: &tagServer}
	if meshPeerAllowed(taggedSelf, same, TagMatchIntersection) {
		t.Fatal("tagged self must not accept untagged peer via UserID")
	}
	if !meshPeerAllowed(taggedSelf, &apitype.WhoIsResponse{
		Node: &tailcfg.Node{User: 1, Tags: []string{"tag:server"}},
	}, TagMatchIntersection) {
		t.Fatal("tagged self + intersecting peer tags")
	}
	if !meshPeerAllowed(taggedSelf, &apitype.WhoIsResponse{
		Node: &tailcfg.Node{Tags: []string{"tag:server", "tag:web"}},
	}, TagMatchContains) {
		t.Fatal("contains")
	}
	if meshPeerAllowed(taggedSelf, &apitype.WhoIsResponse{
		Node: &tailcfg.Node{Tags: []string{"tag:server", "tag:web"}},
	}, TagMatchEqual) {
		t.Fatal("equal rejects superset")
	}

	// Explicit Mullvad markers on WhoIs path (tag and/or DNS).
	if meshPeerAllowed(self, &apitype.WhoIsResponse{
		Node: &tailcfg.Node{
			Name: "se.tailnet.ts.net.",
			User: testUserSelf,
			Tags: []string{mullvadExitNodeTag},
		},
	}, TagMatchIntersection) {
		t.Fatal("mullvad tag")
	}
	if meshPeerAllowed(self, &apitype.WhoIsResponse{
		Node: &tailcfg.Node{
			Name: "se-mma-wg-001.mullvad.ts.net.",
			User: testUserSelf,
		},
	}, TagMatchIntersection) {
		t.Fatal("mullvad dns")
	}
}

func TestParseTagMatchMode(t *testing.T) {
	m, err := ParseTagMatchMode("")
	if err != nil || m != TagMatchIntersection {
		t.Fatalf("empty: %v %v", m, err)
	}
	m, err = ParseTagMatchMode("intersect")
	if err != nil || m != TagMatchIntersection {
		t.Fatalf("intersect: %v %v", m, err)
	}
	m, err = ParseTagMatchMode("equal")
	if err != nil || m != TagMatchEqual {
		t.Fatalf("equal: %v %v", m, err)
	}
	m, err = ParseTagMatchMode("contains")
	if err != nil || m != TagMatchContains {
		t.Fatalf("contains: %v %v", m, err)
	}
	if _, err := ParseTagMatchMode("nope"); err == nil {
		t.Fatal("want error")
	}
	if TagMatchIntersection.String() != "intersect" || TagMatchEqual.String() != "equal" || TagMatchContains.String() != "contains" {
		t.Fatal("String()")
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := pc.LocalAddr().(*net.UDPAddr).Port
	_ = pc.Close()
	return port
}

func TestEndpointPartialBind(t *testing.T) {
	// One bindable localhost address and one that should fail (TEST-NET-3).
	port := freeUDPPort(t)
	good := fmt.Sprintf("127.0.0.1:%d", port)
	bad := fmt.Sprintf("203.0.113.1:%d", port)

	serverTLS, err := generateQUICTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	ep, err := peer.NewEndpoint(serverTLS, quicClientTLSConfig(), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Close()

	pcGood, err := net.ListenPacket("udp", good)
	if err != nil {
		t.Fatal(err)
	}
	if err := ep.AddPacketConn(pcGood); err != nil {
		t.Fatal(err)
	}
	pcBad, err := net.ListenPacket("udp", bad)
	if err == nil {
		// Rarely bindable; still ok if Add fails or succeeds.
		_ = ep.AddPacketConn(pcBad)
	}
	addrs := ep.LocalAddrs()
	if len(addrs) < 1 {
		t.Fatal("expected at least one bound address")
	}
}

func TestDialTimeoutViaEndpoint(t *testing.T) {
	// Short timeout against a documentation-only address that should not accept.
	serverTLS, err := generateQUICTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep, err := peer.NewEndpoint(serverTLS, quicClientTLSConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Close()
	if err := ep.AddPacketConn(pc); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = ep.Dial(ctx, "192.0.2.1:9")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected dial error")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("dial took %v; expected fail within ~timeout", elapsed)
	}
	// Some platforms fail immediately (EINVAL); others time out. Both are fine
	// as long as dial does not hang. Soft-fail classification is covered separately.
}

func TestDialClosedPortFailsQuickly(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()

	serverTLS, err := generateQUICTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	pc2, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep, err := peer.NewEndpoint(serverTLS, quicClientTLSConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Close()
	if err := ep.AddPacketConn(pc2); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = ep.Dial(ctx, addr)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected dial error")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("closed-port dial took %v", elapsed)
	}
	if !isDialSoftFail(fmt.Errorf("%w: %w", errDial, err)) {
		t.Fatalf("expected soft fail for closed-port dial: %v", err)
	}
}

func TestNewAppliesDefaultDialTimeout(t *testing.T) {
	dir := t.TempDir()
	d, err := New(Config{Dir: dir, NetMode: NetModePlain})
	if err != nil {
		t.Fatal(err)
	}
	if d.cfg.DialTimeout != DefaultDialTimeout {
		t.Fatalf("DialTimeout=%v want %v", d.cfg.DialTimeout, DefaultDialTimeout)
	}
}

func TestNewAppliesDefaultBlockSize(t *testing.T) {
	dir := t.TempDir()
	d, err := New(Config{Dir: dir, NetMode: NetModePlain})
	if err != nil {
		t.Fatal(err)
	}
	if d.cfg.BlockSize != DefaultBlockSize {
		t.Fatalf("BlockSize=%d want %d", d.cfg.BlockSize, DefaultBlockSize)
	}
	if DefaultBlockSize != delta.DefaultBlockSize {
		t.Fatalf("daemon.DefaultBlockSize=%d != delta.DefaultBlockSize=%d", DefaultBlockSize, delta.DefaultBlockSize)
	}
}

func TestNewAppliesDefaultConcurrency(t *testing.T) {
	dir := t.TempDir()
	d, err := New(Config{Dir: dir, NetMode: NetModePlain})
	if err != nil {
		t.Fatal(err)
	}
	if d.cfg.DiscoveryConcurrency != DefaultDiscoveryConcurrency {
		t.Fatalf("DiscoveryConcurrency=%d want %d", d.cfg.DiscoveryConcurrency, DefaultDiscoveryConcurrency)
	}
	if d.cfg.PullStreamConcurrency != DefaultPullStreamConcurrency {
		t.Fatalf("PullStreamConcurrency=%d want %d", d.cfg.PullStreamConcurrency, DefaultPullStreamConcurrency)
	}
	if cap(d.pullSem) != DefaultPullStreamConcurrency {
		t.Fatalf("pullSem cap=%d", cap(d.pullSem))
	}
}

func TestInjectNetworkChangeNilSafeAndConcurrent(t *testing.T) {
	var nilD *Daemon
	nilD.InjectNetworkChange()

	dir := t.TempDir()
	d, err := New(Config{Dir: dir, NetMode: NetModePlain})
	if err != nil {
		t.Fatal(err)
	}
	d.InjectNetworkChange() // no inject registered yet

	var calls atomic.Int64
	d.setInjectNetChange(func() { calls.Add(1) })

	const workers = 8
	const perWorker = 50
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range perWorker {
				d.InjectNetworkChange()
			}
		}()
	}
	go func() {
		time.Sleep(time.Millisecond)
		d.setInjectNetChange(nil)
	}()
	wg.Wait()
	d.InjectNetworkChange()
	if calls.Load() == 0 {
		t.Fatal("expected at least one inject callback before clear")
	}
}

func TestNewAppliesDefaultTombstoneTTL(t *testing.T) {
	dir := t.TempDir()
	d, err := New(Config{Dir: dir, NetMode: NetModePlain})
	if err != nil {
		t.Fatal(err)
	}
	if d.cfg.TombstoneTTL != DefaultTombstoneTTL {
		t.Fatalf("TombstoneTTL=%v want %v", d.cfg.TombstoneTTL, DefaultTombstoneTTL)
	}
	if DefaultTombstoneTTL != index.DefaultTombstoneTTL {
		t.Fatalf("daemon.DefaultTombstoneTTL=%v != index.DefaultTombstoneTTL=%v", DefaultTombstoneTTL, index.DefaultTombstoneTTL)
	}
}

func TestIsDialSoftFail(t *testing.T) {
	dialWrap := func(err error) error {
		return fmt.Errorf("%w: %w", errDial, err)
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "dial timeout", err: dialWrap(context.DeadlineExceeded), want: true},
		{name: "dial os deadline", err: dialWrap(os.ErrDeadlineExceeded), want: true},
		{name: "dial refused", err: dialWrap(syscall.ECONNREFUSED), want: true},
		{name: "dial host unreach", err: dialWrap(syscall.EHOSTUNREACH), want: true},
		{name: "dial net unreach", err: dialWrap(syscall.ENETUNREACH), want: true},
		{name: "dial conn aborted", err: dialWrap(syscall.ECONNABORTED), want: true},
		{name: "dial conn reset", err: dialWrap(syscall.ECONNRESET), want: true},
		{name: "dial canceled", err: dialWrap(context.Canceled), want: false},
		{name: "mid-session deadline", err: fmt.Errorf("hello response: %w", context.DeadlineExceeded), want: false},
		{name: "mid-session refused", err: fmt.Errorf("hello response: %w", syscall.ECONNREFUSED), want: false},
		{name: "plain timeout without dial", err: context.DeadlineExceeded, want: false},
		{name: "plain refused without dial", err: syscall.ECONNREFUSED, want: false},
		{name: "other dial error", err: dialWrap(fmt.Errorf("weird")), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDialSoftFail(tc.err); got != tc.want {
				t.Fatalf("isDialSoftFail(%v)=%v want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestEnsureAndroidTSLogsDirNoopOffAndroid(t *testing.T) {
	if runtime.GOOS == "android" {
		t.Skip("this case covers non-Android builds only")
	}
	prev, had := os.LookupEnv("TS_LOGS_DIR")
	if had {
		t.Cleanup(func() { _ = os.Setenv("TS_LOGS_DIR", prev) })
	} else {
		t.Cleanup(func() { _ = os.Unsetenv("TS_LOGS_DIR") })
	}
	_ = os.Unsetenv("TS_LOGS_DIR")
	dir := t.TempDir()
	if err := ensureAndroidTSLogsDir(dir, slog.Default()); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TS_LOGS_DIR") != "" {
		t.Fatal("should not set TS_LOGS_DIR off Android")
	}
}

func TestEnsureAndroidTSLogsDirPreservesExistingEnv(t *testing.T) {
	prev, had := os.LookupEnv("TS_LOGS_DIR")
	if had {
		t.Cleanup(func() { _ = os.Setenv("TS_LOGS_DIR", prev) })
	} else {
		t.Cleanup(func() { _ = os.Unsetenv("TS_LOGS_DIR") })
	}
	custom := filepath.Join(t.TempDir(), "custom-logs")
	if err := os.Setenv("TS_LOGS_DIR", custom); err != nil {
		t.Fatal(err)
	}
	if err := ensureAndroidTSLogsDir(t.TempDir(), slog.Default()); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("TS_LOGS_DIR"); got != custom {
		t.Fatalf("got %q want %q", got, custom)
	}
}
