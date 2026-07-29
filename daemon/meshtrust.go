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
// Prefer newMeshGate, which runs this check and compiles the hot-path policy.
// Call after LocalAPI status is available (host/tsnet listen or status refresh).
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

// meshGate is a compiled mesh-trust policy for one Self snapshot.
//
// Build with newMeshGate after LocalAPI Self is known (listen and each status
// refresh). Reuse for every peer in that evaluation; do not re-decide
// user-vs-tag mode per peer. Refresh when Self may have changed (discovery
// status); Hello uses the last good gate so it does not re-fetch Self on every
// handshake.
//
// Policy (after construction succeeds):
//   - requireTag non-empty: peer must carry that ACL tag (tagged Self mode).
//   - requireTag empty: peer must share user (untagged Self mode). User 0 fails
//     closed for all peers.
//   - Sharee-only and Mullvad peers are never allowed.
type meshGate struct {
	// requireTag is set in tagged-Self mode (Config.MeshTag). Empty means
	// same-user mode.
	requireTag string
	// user is Self's Tailscale UserID in same-user mode. Ignored when
	// requireTag is set.
	user tailcfg.UserID
}

// newMeshGate validates MeshTag against Self and compiles the hot-path policy.
// On error the gate must not be used (fail closed).
func newMeshGate(self meshIdentity, meshTag string) (meshGate, error) {
	if err := checkSelfMeshTagConfig(self, meshTag); err != nil {
		return meshGate{}, err
	}
	if self.tagged() {
		// checkSelfMeshTagConfig ensures meshTag is non-empty and on Self.
		return meshGate{requireTag: meshTag}, nil
	}
	// Untagged: same-user mode. user may be 0 (allows nothing until refresh).
	return meshGate{user: self.user}, nil
}

// allows reports whether peer may join the mesh under this gate.
func (g meshGate) allows(peer meshIdentity) bool {
	if peer.sharee || isMullvadIdentity(peer.tags, peer.dns) {
		return false
	}
	if g.requireTag != "" {
		return slices.Contains(peer.tags, g.requireTag)
	}
	if g.user == 0 || peer.user == 0 {
		return false
	}
	return peer.user == g.user
}
