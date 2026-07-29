package daemon

import (
	"context"

	"deedles.dev/tailsync/internal/peer"
)

// discoveryCandidates builds dial targets for the peer discovery service.
// Explicit Config.Peers pins skip status discovery (test determinism / fixed
// dial lists). Pins do not bypass mesh trust: on host/tsnet, Hello still runs
// WhoIs + trustedMeshPeer. When Peers is empty, Online status peers that pass
// mesh trust are returned (untagged Self → same UserID; tagged Self → TagMatch;
// excludes self, sharees, Mullvad). Connected sessions are filtered out by
// discovery itself.
func (d *Daemon) discoveryCandidates(ctx context.Context) []peer.Candidate {
	if len(d.cfg.Peers) > 0 {
		out := make([]peer.Candidate, 0, len(d.cfg.Peers))
		for _, a := range d.cfg.Peers {
			if a != "" {
				out = append(out, peer.Candidate{Addr: a})
			}
		}
		return out
	}

	addrs, err := d.listPeers(ctx)
	if err != nil {
		d.log.Debug("list status peers", "err", err)
		return nil
	}

	out := make([]peer.Candidate, 0, len(addrs))
	for _, a := range addrs {
		if a != "" {
			out = append(out, peer.Candidate{Addr: a})
		}
	}
	return out
}
