package daemon

import (
	"context"

	"deedles.dev/tailsync/internal/peer"
)

// maxUnfilteredDiscoveryPeers is the hard cap on status-discovered dial targets
// when ServiceName is empty. Above this, discovery returns no status candidates
// (fail-closed) so a large tailnet cannot self-DoS; operator must set -service
// or -peers. Pins (Config.Peers) are never capped.
const maxUnfilteredDiscoveryPeers = 64

// discoveryCandidates builds dial targets for the peer discovery service.
// Explicit Config.Peers pins skip status discovery (test determinism).
// Otherwise status Online peers (filtered by ServiceName, excluding Mullvad)
// are returned. Connected sessions are filtered out by discovery itself.
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

	unfiltered := d.cfg.ServiceName == ""
	// Fail-closed on huge unfiltered sets: refuse status discovery until filtered.
	if unfiltered && len(addrs) > maxUnfilteredDiscoveryPeers {
		if d.refuseWarned.CompareAndSwap(false, true) {
			d.log.Warn("refusing unfiltered discovery: too many online peers",
				"count", len(addrs),
				"max", maxUnfilteredDiscoveryPeers,
				"hint", "set -service <name-substring> to limit candidates",
			)
		}
		return nil
	}
	if unfiltered && len(addrs) > manyPeersWarnThreshold {
		if d.manyPeersWarned.CompareAndSwap(false, true) {
			d.log.Warn("discovered many peers without -service; dialing all online nodes wastes dial attempts on hosts not running tailsync",
				"count", len(addrs),
				"hint", "set -service <name-substring> (or -peers for tests/overrides)",
			)
		}
	}

	out := make([]peer.Candidate, 0, len(addrs))
	for _, a := range addrs {
		if a != "" {
			out = append(out, peer.Candidate{Addr: a})
		}
	}
	return out
}
