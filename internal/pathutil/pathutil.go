// Package pathutil provides shared relative-path validation for the sync tree.
package pathutil

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
)

// IsReservedComponent reports whether a single path component is reserved for
// tailsync state. Scan skips these directories; peer path validation rejects
// any path that contains them so StateDir (Dir/.tailsync) cannot be targeted.
func IsReservedComponent(name string) bool {
	return name == ".tailsync" || strings.HasPrefix(name, ".tailsync-")
}

// CleanRel validates and normalizes a slash-separated relative path for use as
// an index key and [os.Root] name. It rejects empty, absolute, non-local, and
// "." paths, and any reserved path component.
//
// CleanRel does not bind to a root directory; callers that need escape checks
// against a concrete Dir should join and re-validate with filepath.Rel.
func CleanRel(rel string) (string, error) {
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	if rel == "" || strings.HasPrefix(rel, "/") {
		return "", fmt.Errorf("invalid path %q", rel)
	}
	cleaned := filepath.ToSlash(filepath.Clean(filepath.FromSlash(rel)))
	if cleaned == "." || cleaned == "" || !fs.ValidPath(cleaned) {
		return "", fmt.Errorf("invalid path %q", rel)
	}
	for part := range strings.SplitSeq(cleaned, "/") {
		if IsReservedComponent(part) {
			return "", fmt.Errorf("reserved path %q", rel)
		}
	}
	fromSlash := filepath.FromSlash(cleaned)
	if !filepath.IsLocal(fromSlash) {
		return "", fmt.Errorf("invalid path %q", rel)
	}
	return cleaned, nil
}
