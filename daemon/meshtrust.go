package daemon

import (
	"fmt"
	"slices"
	"strings"

	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
)

// mullvadExitNodeTag is the ACL tag Tailscale applies to Mullvad VPN exit-node
// peers. Those peers appear Online on the tailnet but never run tailsync.
const mullvadExitNodeTag = "tag:mullvad-exit-node"

// mullvadDNSSuffix is the MagicDNS suffix of Mullvad exit nodes (with or
// without a trailing dot on the full name).
const mullvadDNSSuffix = "mullvad.ts.net"

// isMullvadDNSName reports whether dns is under the Mullvad MagicDNS domain.
func isMullvadDNSName(dns string) bool {
	dns = strings.ToLower(strings.TrimSuffix(dns, "."))
	return dns == mullvadDNSSuffix || strings.HasSuffix(dns, "."+mullvadDNSSuffix)
}

// isMullvadIdentity reports whether identity facts mark a Mullvad exit node:
// tag:mullvad-exit-node and/or a mullvad.ts.net DNS name. Do not use
// ExitNodeOption alone — user-run exit nodes may still run tailsync.
func isMullvadIdentity(tags []string, dns string) bool {
	if slices.Contains(tags, mullvadExitNodeTag) {
		return true
	}
	return isMullvadDNSName(dns)
}

// normalizeMeshTag trims MeshTag / -mesh-tag values.
func normalizeMeshTag(s string) string {
	return strings.TrimSpace(s)
}

// validateMeshTagFormat checks a non-empty mesh tag looks like a Tailscale ACL
// tag (tag:name). Empty is allowed (untagged Self does not need a mesh tag).
func validateMeshTagFormat(s string) error {
	if s == "" {
		return nil
	}
	if !strings.HasPrefix(s, "tag:") || len(s) <= len("tag:") {
		return fmt.Errorf("mesh tag %q must look like tag:name", s)
	}
	return nil
}

// checkSelfMeshTagConfig fails closed when MeshTag does not match Self:
//   - untagged Self: MeshTag must be empty
//   - tagged Self: MeshTag required and present on Self
//
// Call after LocalAPI status is available (host/tsnet listen).
func checkSelfMeshTagConfig(self meshIdentity, meshTag string) error {
	if !self.tagged() {
		if meshTag != "" {
			return fmt.Errorf("this machine is untagged; omit -mesh-tag (or Config.MeshTag)")
		}
		return nil
	}
	if meshTag == "" {
		return fmt.Errorf("this machine is tagged; set -mesh-tag (or Config.MeshTag) to a tag this node has")
	}
	if !slices.Contains(self.tags, meshTag) {
		return fmt.Errorf("this machine is tagged but does not have mesh tag %q (has %v)", meshTag, self.tags)
	}
	return nil
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

// meshIdentityFromWhoIs maps WhoIs Node fields into meshIdentity.
// ok is false when Node is missing. UserProfile is ignored: LocalAPI WhoIs is
// trusted, and peer ownership/tags come from Node (same facts as status peers).
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

// trustedMeshPeer reports whether peer may join the mesh with self under meshTag.
//
// Policy:
//   - Product skips: sharee-only netmap entries and Mullvad are never trusted.
//   - Untagged self: same Tailscale UserID (fail closed on unknown IDs). Peer
//     tags do not matter; real tagged nodes usually use the synthetic
//     tagged-devices user and therefore do not match.
//   - Tagged self: peer must carry meshTag (Config.MeshTag / -mesh-tag).
//     UserID is ignored (tagged-devices is shared across all tagged nodes).
//     Fail closed if meshTag is empty or not on Self (listen should reject
//     that config before serving).
func trustedMeshPeer(self, peer meshIdentity, meshTag string) bool {
	if peer.sharee || isMullvadIdentity(peer.tags, peer.dns) {
		return false
	}
	if self.tagged() {
		if meshTag == "" || !slices.Contains(self.tags, meshTag) {
			return false
		}
		return slices.Contains(peer.tags, meshTag)
	}
	if self.user == 0 || peer.user == 0 {
		return false
	}
	return peer.user == self.user
}

// meshPeerAllowed is the WhoIs-path adapter: map Self + WhoIs to meshIdentity
// and apply the same policy as discovery (trustedMeshPeer).
func meshPeerAllowed(self *ipnstate.PeerStatus, who *apitype.WhoIsResponse, meshTag string) bool {
	if self == nil {
		return false
	}
	pid, ok := meshIdentityFromWhoIs(who)
	if !ok {
		return false
	}
	return trustedMeshPeer(meshIdentityFromPeerStatus(self), pid, meshTag)
}
