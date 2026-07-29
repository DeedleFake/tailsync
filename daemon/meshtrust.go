package daemon

import (
	"fmt"
	"slices"
	"strings"

	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
)

// TagMatchMode selects how two tagged peers are compared when Self is tagged.
// When Self is untagged, mesh trust uses same Tailscale UserID instead and
// TagMatchMode is ignored.
type TagMatchMode int

const (
	// TagMatchIntersection allows a peer when Self and peer share at least one
	// ACL tag (default).
	TagMatchIntersection TagMatchMode = iota
	// TagMatchEqual allows a peer only when Self and peer have the same tag set
	// (order-independent).
	TagMatchEqual
	// TagMatchContains allows a peer when the peer's tag set contains every tag
	// on Self (peer may have extra tags).
	TagMatchContains
)

// String returns the CLI/config name for m.
func (m TagMatchMode) String() string {
	switch m {
	case TagMatchIntersection:
		return "intersect"
	case TagMatchEqual:
		return "equal"
	case TagMatchContains:
		return "contains"
	default:
		return fmt.Sprintf("TagMatchMode(%d)", int(m))
	}
}

// ParseTagMatchMode parses a CLI/config value: intersect|intersection,
// equal|exact, contains|subset (peer has all of self's tags).
func ParseTagMatchMode(s string) (TagMatchMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "intersect", "intersection":
		return TagMatchIntersection, nil
	case "equal", "exact":
		return TagMatchEqual, nil
	case "contains", "subset":
		return TagMatchContains, nil
	default:
		return 0, fmt.Errorf("unknown tag match mode %q (want intersect, equal, or contains)", s)
	}
}

func (m TagMatchMode) valid() bool {
	switch m {
	case TagMatchIntersection, TagMatchEqual, TagMatchContains:
		return true
	default:
		return false
	}
}

// meshIdentity is the control-plane facts used for mesh trust (ownership or
// tags). PeerStatus and WhoIs are adapted into this shape so discovery and
// wire verification share one decision function.
type meshIdentity struct {
	user   tailcfg.UserID
	sharee bool
	tags   []string
	dns    string
}

func (id meshIdentity) tagged() bool {
	return len(id.tags) > 0
}

func meshIdentityFromPeerStatus(p *ipnstate.PeerStatus) meshIdentity {
	if p == nil {
		return meshIdentity{}
	}
	id := meshIdentity{
		user:   p.UserID,
		sharee: p.ShareeNode,
		dns:    p.DNSName,
	}
	if p.Tags != nil && p.Tags.Len() > 0 {
		id.tags = slices.Clone(p.Tags.AsSlice())
	}
	return id
}

// meshIdentityFromWhoIs maps WhoIs identity. ok is false when Node is missing.
// UserProfile vs Node.User consistency is checked in meshPeerAllowed.
func meshIdentityFromWhoIs(who *apitype.WhoIsResponse) (id meshIdentity, ok bool) {
	if who == nil || who.Node == nil {
		return meshIdentity{}, false
	}
	n := who.Node
	id = meshIdentity{
		user: n.User,
		dns:  n.Name,
		tags: slices.Clone(n.Tags),
	}
	if n.Hostinfo.Valid() {
		id.sharee = n.Hostinfo.ShareeNode()
	}
	return id, true
}

// trustedMeshPeer reports whether peer may join the mesh with self under mode.
//
// Policy:
//   - Product skips: sharee-only netmap entries and Mullvad are never trusted.
//   - Untagged self: same Tailscale UserID (fail closed on unknown IDs). Peer
//     tags do not matter; real tagged nodes usually use the synthetic
//     tagged-devices user and therefore do not match.
//   - Tagged self: peer must be tagged and match under TagMatchMode. UserID is
//     ignored (tagged-devices is shared across all tagged nodes).
func trustedMeshPeer(self, peer meshIdentity, mode TagMatchMode) bool {
	if peer.sharee || isMullvadIdentity(peer.tags, peer.dns) {
		return false
	}
	if self.tagged() {
		if !peer.tagged() {
			return false
		}
		return tagsMatch(self.tags, peer.tags, mode)
	}
	if self.user == 0 || peer.user == 0 {
		return false
	}
	return peer.user == self.user
}

// meshPeerAllowed is the WhoIs-path trust check: same policy as trustedMeshPeer,
// plus UserProfile must not disagree with Self when Self is untagged.
func meshPeerAllowed(self *ipnstate.PeerStatus, who *apitype.WhoIsResponse, mode TagMatchMode) bool {
	if self == nil {
		return false
	}
	sid := meshIdentityFromPeerStatus(self)
	pid, ok := meshIdentityFromWhoIs(who)
	if !ok {
		return false
	}
	// Cross-check profile when Self is user-owned (successful WhoIs usually
	// includes UserProfile; nil is tolerated for partial fixtures).
	if !sid.tagged() && who.UserProfile != nil && who.UserProfile.ID != 0 && who.UserProfile.ID != sid.user {
		return false
	}
	return trustedMeshPeer(sid, pid, mode)
}

func isMullvadIdentity(tags []string, dns string) bool {
	if slices.Contains(tags, mullvadExitNodeTag) {
		return true
	}
	return isMullvadDNSName(dns)
}

func tagsMatch(selfTags, peerTags []string, mode TagMatchMode) bool {
	if len(selfTags) == 0 || len(peerTags) == 0 {
		return false
	}
	switch mode {
	case TagMatchEqual:
		return tagSetsEqual(selfTags, peerTags)
	case TagMatchContains:
		return tagSetContainsAll(peerTags, selfTags)
	default:
		return tagSetsIntersect(selfTags, peerTags)
	}
}

func tagSetsIntersect(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, t := range a {
		if t != "" {
			set[t] = struct{}{}
		}
	}
	for _, t := range b {
		if _, ok := set[t]; ok {
			return true
		}
	}
	return false
}

func tagSetsEqual(a, b []string) bool {
	setA := tagSet(a)
	setB := tagSet(b)
	if len(setA) != len(setB) {
		return false
	}
	for t := range setA {
		if _, ok := setB[t]; !ok {
			return false
		}
	}
	return true
}

// tagSetContainsAll reports whether outer contains every tag in need.
func tagSetContainsAll(outer, need []string) bool {
	set := tagSet(outer)
	for _, t := range need {
		if t == "" {
			continue
		}
		if _, ok := set[t]; !ok {
			return false
		}
	}
	// Empty need after filtering would vacuously match; callers require non-empty.
	return len(tagSet(need)) > 0
}

func tagSet(tags []string) map[string]struct{} {
	set := make(map[string]struct{}, len(tags))
	for _, t := range tags {
		if t != "" {
			set[t] = struct{}{}
		}
	}
	return set
}
