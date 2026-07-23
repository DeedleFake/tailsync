package daemon

import (
	"context"
	"fmt"
	"time"

	"deedles.dev/tailsync/internal/scan"
)

func (d *Daemon) reconcile(ctx context.Context) error {
	d.syncMu.Lock()
	defer d.syncMu.Unlock()

	res, err := scan.Scan(ctx, d.root, d.idx, nil)
	if err != nil {
		return err
	}
	applied := 0
	if len(res.Changes) > 0 {
		for _, c := range res.Changes {
			d.log.Info("local change", "kind", c.Kind.String(), "path", c.Path)
		}
		applied = scan.Apply(d.idx, res)
	}

	if n := d.idx.GCTombstones(time.Now(), d.cfg.TombstoneTTL); n > 0 {
		d.log.Info("gc tombstones", "removed", n)
		applied += n
	}

	if applied > 0 || d.appliesSinceSave > 0 {
		if err := d.idx.Save(); err != nil {
			return fmt.Errorf("save index: %w", err)
		}
		d.appliesSinceSave = 0
	}
	return nil
}
