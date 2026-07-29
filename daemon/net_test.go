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

	got := peersFromStatus(st, 5960, mustMeshGate(t, st.Self, ""))
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

	got := peersFromStatus(st, 5960, mustMeshGate(t, st.Self, ""))
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
	const mesh = "tag:tailsync"
	tagMesh := views.SliceOf([]string{mesh})
	tagMeshServer := views.SliceOf([]string{mesh, "tag:server"})
	tagServerOnly := views.SliceOf([]string{"tag:server"})
	// Self has mesh + broad tag; only mesh membership should matter.
	tagSelf := views.SliceOf([]string{mesh, "tag:server"})
	st := &ipnstate.Status{
		Self: &ipnstate.PeerStatus{
			ID:      "self",
			DNSName: "self.tailnet.ts.net.",
			UserID:  taggedDevicesUser,
			Tags:    &tagSelf,
		},
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			// Has mesh tag — allow.
			key.NewNode().Public(): {
				ID:           "peer-mesh",
				Online:       true,
				UserID:       taggedDevicesUser,
				Tags:         &tagMesh,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.10")},
			},
			// Mesh + extra tags — allow (extra tags ignored).
			key.NewNode().Public(): {
				ID:           "peer-both",
				Online:       true,
				UserID:       taggedDevicesUser,
				Tags:         &tagMeshServer,
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.11")},
			},
			// Shared broad tag only — deny (would have been allowed under intersect).
			key.NewNode().Public(): {
				ID:           "peer-server-only",
				Online:       true,
				UserID:       taggedDevicesUser,
				Tags:         &tagServerOnly,
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

	got := peersFromStatus(st, 5960, mustMeshGate(t, st.Self, mesh))
	want := map[string]bool{
		"100.64.0.10:5960": true,
		"100.64.0.11:5960": true,
	}
	if len(got) != len(want) {
		t.Fatalf("mesh-tag: got %v", got)
	}
	for _, a := range got {
		if !want[a] {
			t.Errorf("mesh-tag unexpected %q", a)
		}
	}

	// Missing mesh tag on config → gate construction fails (fail closed).
	if _, err := newMeshGate(meshIdentityFromPeerStatus(st.Self), ""); err == nil {
		t.Fatal("empty mesh tag want error")
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
	if got := peersFromStatus(nil, 5960, meshGate{}); len(got) != 0 {
		t.Fatalf("nil status: %v", got)
	}
	if got := peersFromStatus(&ipnstate.Status{Peer: peer}, 5960, meshGate{}); len(got) != 0 {
		t.Fatalf("nil self: %v", got)
	}
	zeroSelf := &ipnstate.PeerStatus{ID: "self", UserID: 0}
	gZero, err := newMeshGate(meshIdentityFromPeerStatus(zeroSelf), "")
	if err != nil {
		t.Fatal(err)
	}
	if got := peersFromStatus(&ipnstate.Status{
		Self: zeroSelf,
		Peer: peer,
	}, 5960, gZero); len(got) != 0 {
		t.Fatalf("zero self UserID: %v", got)
	}
	// Tagged Self does not use UserID; untagged peer is not a candidate.
	tagServer := views.SliceOf([]string{"tag:server"})
	tagSelf := &ipnstate.PeerStatus{ID: "self", UserID: testUserSelf, Tags: &tagServer}
	got := peersFromStatus(&ipnstate.Status{
		Self: tagSelf,
		Peer: peer,
	}, 5960, mustMeshGate(t, tagSelf, "tag:server"))
	if len(got) != 0 {
		t.Fatalf("tagged self must not discover untagged peers via UserID: %v", got)
	}
}

func mustMeshGate(t *testing.T, self *ipnstate.PeerStatus, meshTag string) meshGate {
	t.Helper()
	g, err := newMeshGate(meshIdentityFromPeerStatus(self), meshTag)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestMeshGateAllows(t *testing.T) {
	self := meshIdentity{user: testUserSelf}
	g, err := newMeshGate(self, "")
	if err != nil {
		t.Fatal(err)
	}
	if g.allows(meshIdentity{}) {
		t.Fatal("zero peer user")
	}
	gZero, err := newMeshGate(meshIdentity{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if gZero.allows(meshIdentity{user: testUserSelf}) {
		t.Fatal("zero self user")
	}
	if g.allows(meshIdentity{user: testUserOther}) {
		t.Fatal("other user")
	}
	if g.allows(meshIdentity{user: testUserSelf, sharee: true}) {
		t.Fatal("sharee")
	}
	if !g.allows(meshIdentity{user: testUserSelf}) {
		t.Fatal("same user")
	}

	// Untagged self: UserID match even if peer is tagged.
	if !g.allows(meshIdentity{user: testUserSelf, tags: []string{"tag:server"}}) {
		t.Fatal("untagged self + tagged peer same user")
	}

	// Tagged self: mesh tag only; UserID ignored.
	const mesh = "tag:tailsync"
	taggedSelf := meshIdentity{user: 9999, tags: []string{mesh, "tag:server"}}
	tg, err := newMeshGate(taggedSelf, mesh)
	if err != nil {
		t.Fatal(err)
	}
	if tg.allows(meshIdentity{user: 9999}) {
		t.Fatal("tagged self must not trust untagged peer")
	}
	if !tg.allows(meshIdentity{user: 1, tags: []string{mesh, "tag:web"}}) {
		t.Fatal("peer with mesh tag")
	}
	if tg.allows(meshIdentity{tags: []string{"tag:server"}}) {
		t.Fatal("shared broad tag only must not match")
	}
	if _, err := newMeshGate(taggedSelf, ""); err == nil {
		t.Fatal("empty mesh tag fail closed")
	}
	// Self does not carry configured mesh tag.
	if _, err := newMeshGate(meshIdentity{tags: []string{"tag:server"}}, mesh); err == nil {
		t.Fatal("self missing mesh tag")
	}

	// Mullvad always denied.
	if g.allows(meshIdentity{user: testUserSelf, tags: []string{mullvadExitNodeTag}}) {
		t.Fatal("mullvad tag")
	}
	if g.allows(meshIdentity{user: testUserSelf, dns: "se.mullvad.ts.net."}) {
		t.Fatal("mullvad dns")
	}
	if tg.allows(meshIdentity{tags: []string{mullvadExitNodeTag, mesh}}) {
		t.Fatal("mullvad among tags")
	}
}

func TestCheckSelfMeshTagConfig(t *testing.T) {
	if err := checkSelfMeshTagConfig(meshIdentity{user: testUserSelf}, ""); err != nil {
		t.Fatalf("untagged: %v", err)
	}
	if err := checkSelfMeshTagConfig(meshIdentity{user: testUserSelf}, "tag:tailsync"); err == nil {
		t.Fatal("untagged with mesh tag want error")
	}
	if err := checkSelfMeshTagConfig(meshIdentity{tags: []string{"tag:tailsync"}}, ""); err == nil {
		t.Fatal("tagged without mesh tag want error")
	}
	if err := checkSelfMeshTagConfig(meshIdentity{tags: []string{"tag:server"}}, "tag:tailsync"); err == nil {
		t.Fatal("self missing mesh tag want error")
	}
	if err := checkSelfMeshTagConfig(meshIdentity{tags: []string{"tag:tailsync", "tag:server"}}, "tag:tailsync"); err != nil {
		t.Fatalf("ok: %v", err)
	}
}

func TestParseMeshTag(t *testing.T) {
	got, err := parseMeshTag("")
	if err != nil || got != "" {
		t.Fatalf("empty: got %q, %v", got, err)
	}
	got, err = parseMeshTag("  tag:tailsync  ")
	if err != nil || got != "tag:tailsync" {
		t.Fatalf("trim: got %q, %v", got, err)
	}
	// Case is normalized so it matches control-plane tags at listen.
	got, err = parseMeshTag("tag:TailSync")
	if err != nil || got != "tag:tailsync" {
		t.Fatalf("lower: got %q, %v", got, err)
	}
	if _, err := parseMeshTag("tailsync"); err == nil {
		t.Fatal("want tag: prefix error")
	}
	if _, err := parseMeshTag("tag:"); err == nil {
		t.Fatal("want empty name error")
	}
	if _, err := parseMeshTag("tag:1foo"); err == nil {
		t.Fatal("want leading letter error")
	}
	if _, err := parseMeshTag("tag:foo@bar"); err == nil {
		t.Fatal("want punctuation error")
	}
	if _, err := parseMeshTag("tag:foo bar"); err == nil {
		t.Fatal("want space error")
	}
	got, err = parseMeshTag("tag:lab-1")
	if err != nil || got != "tag:lab-1" {
		t.Fatalf("dash digit: got %q, %v", got, err)
	}
}

func TestIsMullvadIdentity(t *testing.T) {
	if isMullvadIdentity(nil, "") {
		t.Fatal("empty")
	}
	id := meshIdentityFromPeerStatus(&ipnstate.PeerStatus{DNSName: "laptop.tailnet.ts.net."})
	if isMullvadIdentity(id.tags, id.dns) {
		t.Fatal("normal peer")
	}
	mullvadTags := views.SliceOf([]string{mullvadExitNodeTag})
	id = meshIdentityFromPeerStatus(&ipnstate.PeerStatus{
		DNSName: "se.tailnet.ts.net.",
		Tags:    &mullvadTags,
	})
	if !isMullvadIdentity(id.tags, id.dns) {
		t.Fatal("tag")
	}
	if !isMullvadIdentity(nil, "se-mma-wg-001.mullvad.ts.net.") {
		t.Fatal("dns with trailing dot")
	}
	if !isMullvadIdentity(nil, "se-mma-wg-001.mullvad.ts.net") {
		t.Fatal("dns without trailing dot")
	}
	id = meshIdentityFromPeerStatus(&ipnstate.PeerStatus{
		DNSName:        "home-exit.tailnet.ts.net.",
		ExitNodeOption: true,
	})
	if isMullvadIdentity(id.tags, id.dns) {
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

// whoIsNode builds a WhoIs Node with valid Hostinfo (required for fail-closed
// sharee detection). ShareeNode defaults to false unless set on hi.
func whoIsNode(user tailcfg.UserID, name string, tags []string, hi *tailcfg.Hostinfo) *tailcfg.Node {
	if hi == nil {
		hi = &tailcfg.Hostinfo{}
	}
	n := &tailcfg.Node{User: user, Name: name, Tags: tags}
	n.Hostinfo = hi.View()
	return n
}

func TestMeshGateFromWhoIs(t *testing.T) {
	// WhoIs path: compile gate from Self PeerStatus, allow via meshIdentityFromWhoIs.
	self := &ipnstate.PeerStatus{UserID: testUserSelf}
	g := mustMeshGate(t, self, "")
	same := &apitype.WhoIsResponse{
		Node: whoIsNode(testUserSelf, "peer.tailnet.ts.net.", nil, nil),
	}
	pid, ok := meshIdentityFromWhoIs(same)
	if !ok || !g.allows(pid) {
		t.Fatal("same user")
	}

	other := &apitype.WhoIsResponse{
		Node: whoIsNode(testUserOther, "other.tailnet.ts.net.", nil, nil),
	}
	pid, ok = meshIdentityFromWhoIs(other)
	if !ok || g.allows(pid) {
		t.Fatal("other user")
	}

	pid, ok = meshIdentityFromWhoIs(&apitype.WhoIsResponse{
		Node: whoIsNode(0, "peer.tailnet.ts.net.", nil, nil),
	})
	if !ok || g.allows(pid) {
		t.Fatal("zero peer User must fail closed")
	}
	gZero := mustMeshGate(t, &ipnstate.PeerStatus{UserID: 0}, "")
	if gZero.allows(meshIdentity{user: testUserSelf}) {
		t.Fatal("zero self UserID must fail closed")
	}
	if _, ok := meshIdentityFromWhoIs(nil); ok {
		t.Fatal("nil who")
	}

	// Missing Hostinfo: fail closed (unknown sharee must not authenticate).
	if _, ok := meshIdentityFromWhoIs(&apitype.WhoIsResponse{
		Node: &tailcfg.Node{User: testUserSelf, Name: "peer.tailnet.ts.net."},
	}); ok {
		t.Fatal("missing Hostinfo want ok=false")
	}

	// ShareeNode must not authenticate.
	shareeNode := whoIsNode(testUserSelf, "", nil, &tailcfg.Hostinfo{ShareeNode: true})
	pid, ok = meshIdentityFromWhoIs(&apitype.WhoIsResponse{Node: shareeNode})
	if !ok || g.allows(pid) {
		t.Fatal("sharee node")
	}

	// Untagged self allows same-user tagged peer.
	pid, ok = meshIdentityFromWhoIs(&apitype.WhoIsResponse{
		Node: whoIsNode(testUserSelf, "", []string{"tag:server"}, nil),
	})
	if !ok || !g.allows(pid) {
		t.Fatal("untagged self + tagged peer same user")
	}

	// Tagged self: mesh tag, not UserID.
	const mesh = "tag:tailsync"
	tagMesh := views.SliceOf([]string{mesh, "tag:server"})
	taggedSelf := &ipnstate.PeerStatus{UserID: 9999, Tags: &tagMesh}
	tg := mustMeshGate(t, taggedSelf, mesh)
	pid, ok = meshIdentityFromWhoIs(same)
	if !ok || tg.allows(pid) {
		t.Fatal("tagged self must not accept untagged peer via UserID")
	}
	pid, ok = meshIdentityFromWhoIs(&apitype.WhoIsResponse{
		Node: whoIsNode(1, "", []string{mesh}, nil),
	})
	if !ok || !tg.allows(pid) {
		t.Fatal("tagged self + peer with mesh tag")
	}
	pid, ok = meshIdentityFromWhoIs(&apitype.WhoIsResponse{
		Node: whoIsNode(0, "", []string{"tag:server"}, nil),
	})
	if !ok || tg.allows(pid) {
		t.Fatal("broad tag only must not match")
	}

	// Explicit Mullvad markers on WhoIs path (tag and/or DNS).
	pid, ok = meshIdentityFromWhoIs(&apitype.WhoIsResponse{
		Node: whoIsNode(testUserSelf, "se.tailnet.ts.net.", []string{mullvadExitNodeTag}, nil),
	})
	if !ok || g.allows(pid) {
		t.Fatal("mullvad tag")
	}
	pid, ok = meshIdentityFromWhoIs(&apitype.WhoIsResponse{
		Node: whoIsNode(testUserSelf, "se-mma-wg-001.mullvad.ts.net.", nil, nil),
	})
	if !ok || g.allows(pid) {
		t.Fatal("mullvad dns")
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
