// Package tagset provides set operations over string tag lists (e.g. Tailscale
// ACL tags). Empty strings are ignored. Comparisons are order-independent.
//
// Implementations use linear scans: tag lists are typically tiny (a handful of
// tags per node), so maps would cost more than they save.
package tagset

import "slices"

// Intersect reports whether a and b share at least one non-empty tag.
func Intersect(a, b []string) bool {
	for _, x := range a {
		if x == "" {
			continue
		}
		if slices.Contains(b, x) {
			return true
		}
	}
	return false
}

// Equal reports whether a and b contain the same non-empty tags
// (order-independent, duplicates collapsed).
func Equal(a, b []string) bool {
	for _, t := range a {
		if t == "" {
			continue
		}
		if !slices.Contains(b, t) {
			return false
		}
	}
	for _, t := range b {
		if t == "" {
			continue
		}
		if !slices.Contains(a, t) {
			return false
		}
	}
	return true
}

// ContainsAll reports whether outer contains every non-empty tag in need.
// Returns false if need has no non-empty tags (callers that require a match
// should ensure need is non-empty first).
func ContainsAll(outer, need []string) bool {
	var saw bool
	for _, t := range need {
		if t == "" {
			continue
		}
		saw = true
		if !slices.Contains(outer, t) {
			return false
		}
	}
	return saw
}
