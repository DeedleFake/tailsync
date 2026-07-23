package daemon

import (
	"fmt"
	"path/filepath"

	"deedles.dev/tailsync/internal/pathutil"
)

// relPath validates a relative sync path and returns its cleaned slash form for
// [os.Root] I/O and index keys. Uses [pathutil.CleanRel] for shared reserved-path
// and locality checks, then confirms the path cannot escape cfg.Dir.
//
// String validation is defense in depth; actual tree I/O is confined by d.root.
func (d *Daemon) relPath(rel string) (string, error) {
	cleaned, err := pathutil.CleanRel(rel)
	if err != nil {
		return "", err
	}
	fromSlash := filepath.FromSlash(cleaned)
	abs := filepath.Join(d.cfg.Dir, fromSlash)
	relCheck, err := filepath.Rel(d.cfg.Dir, abs)
	if err != nil || !filepath.IsLocal(relCheck) {
		return "", fmt.Errorf("path escapes root: %q", rel)
	}
	return cleaned, nil
}
