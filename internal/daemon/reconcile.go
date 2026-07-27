package daemon

import (
	"context"
	"fmt"
	"time"

	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/scan"
)

// reconcile scans the sync tree, applies peer-visible local index updates, and
// GC's expired tombstones. changed is true when file/tombstone index content
// peers care about was updated (scan apply wins). Pure tombstone GC or a Save
// driven only by remote applies does not set changed (avoids notify thrash after
// remote apply writes files that fire FS events). hints lists applied entries
// for soft notify (path/hash/updated_at); may be empty when unchanged.
func (d *Daemon) reconcile(ctx context.Context) (changed bool, hints []index.ManifestEntry, err error) {
	d.syncMu.Lock()
	defer d.syncMu.Unlock()

	res, err := scan.Scan(ctx, d.root, d.idx, nil)
	if err != nil {
		return false, nil, err
	}
	applied := 0
	if len(res.Changes) > 0 {
		for _, c := range res.Changes {
			d.log.Info("local change", "kind", c.Kind.String(), "path", c.Path)
		}
		// Collect hints for entries that actually win into the index.
		for _, c := range res.Changes {
			if d.idx.SetIfWins(c.Entry) {
				applied++
				hints = append(hints, c.Entry)
			}
		}
	}

	gc := 0
	if n := d.idx.GCTombstones(time.Now(), d.cfg.TombstoneTTL); n > 0 {
		d.log.Info("gc tombstones", "removed", n)
		gc = n
	}

	if applied > 0 || gc > 0 || d.appliesSinceSave > 0 {
		if err := d.idx.Save(); err != nil {
			return false, nil, fmt.Errorf("save index: %w", err)
		}
		d.appliesSinceSave = 0
	}
	return applied > 0, hints, nil
}
