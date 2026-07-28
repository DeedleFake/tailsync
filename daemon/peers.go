package daemon

import (
	"context"

	"deedles.dev/tailsync/internal/peer"
)

// discoveryCandidates builds dial targets for the peer discovery service.
// Explicit Config.Peers pins skip status discovery (test determinism).
// Otherwise status Online peers owned by the current Tailscale user (untagged,
// same UserID; excluding self, shared-in, sharee, tagged, and other users'
// machines) are returned. Connected sessions are filtered out by discovery itself.
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
