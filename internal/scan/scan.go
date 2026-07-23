// Package scan walks a sync directory and reconciles it with the local index.
package scan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/pathutil"
)

// ChangeKind classifies a local filesystem change relative to the index.
type ChangeKind int

const (
	// Added is a new file not previously in the index (or resurrected from tombstone).
	Added ChangeKind = iota
	// Modified is an existing live file whose content or metadata changed.
	Modified
	// Deleted is a file present in the index as live but missing on disk.
	Deleted
)

func (k ChangeKind) String() string {
	switch k {
	case Added:
		return "added"
	case Modified:
		return "modified"
	case Deleted:
		return "deleted"
	default:
		return fmt.Sprintf("ChangeKind(%d)", int(k))
	}
}

// Change is one path-level difference between disk and index.
type Change struct {
	Kind  ChangeKind
	Path  string // relative, forward slashes
	Entry index.Entry
}

// Result holds the full set of changes and the updated view of disk state.
type Result struct {
	Changes []Change
	// Disk is the set of live files observed on disk (path → Entry).
	Disk map[string]index.Entry
}

// Options control scanning behavior.
type Options struct {
	// Now is used for UpdatedAt/DeletedAt; defaults to time.Now.
	Now func() time.Time
	// Hash is the content hasher; defaults to SHA-256 hex of file contents via root.Open(rel).
	// rel is the slash-separated path relative to the scan root.
	Hash func(ctx context.Context, rel string) (string, error)
	// ForceRehash skips the size+mtime fast path and always hashes content.
	ForceRehash bool
}

func (o *Options) now() time.Time {
	if o != nil && o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

func (o *Options) hash(ctx context.Context, root *os.Root, rel string) (string, error) {
	if o != nil && o.Hash != nil {
		return o.Hash(ctx, rel)
	}
	return HashFile(ctx, root, rel)
}

func (o *Options) forceRehash() bool {
	return o != nil && o.ForceRehash
}

// HashFile returns the hex SHA-256 of the file at rel under root.
func HashFile(ctx context.Context, root *os.Root, rel string) (string, error) {
	if root == nil {
		return "", fmt.Errorf("nil sync root")
	}
	f, err := root.Open(rel)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	buf := make([]byte, 256*1024)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, readErr := f.Read(buf)
		if n > 0 {
			if _, err := h.Write(buf[:n]); err != nil {
				return "", err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// stampUpdatedAt chooses an LWW clock for a local change:
// max(now, file mtime, previous UpdatedAt + 1ns) so local edits always advance
// past the prior entry even under mild clock skew.
func stampUpdatedAt(now, modTime, prevUpdated time.Time) time.Time {
	t := now
	if modTime.After(t) {
		t = modTime
	}
	if !prevUpdated.IsZero() {
		minNext := prevUpdated.Add(time.Nanosecond)
		if t.Before(minNext) {
			t = minNext
		}
	}
	return t
}

// Scan walks root, compares against idx, and returns changes without mutating idx.
// root confines all filesystem I/O to the sync directory tree (see [os.Root]).
// Relative paths in the result use forward slashes.
//
// Only regular files are considered. Empty directories, symlinks, and special
// files are not synchronized (v1 limitation). Parent dirs are created when a
// peer file is written.
//
// Content hash is reused when size and mtime match a live index entry unless
// ForceRehash is set. Filesystems with coarse mtime (or tools that preserve
// mtime while rewriting content) can miss silent content changes until another
// metadata field changes.
func Scan(ctx context.Context, root *os.Root, idx *index.Index, opts *Options) (*Result, error) {
	if root == nil {
		return nil, fmt.Errorf("nil sync root")
	}

	disk := make(map[string]index.Entry)
	now := opts.now()
	fsys := root.FS()

	err := fs.WalkDir(fsys, ".", func(rel string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		// fs.WalkDir paths are slash-separated and relative to the FS root.
		rel = filepath.ToSlash(rel)

		// Skip reserved state directories anywhere in the sync tree.
		if d.IsDir() {
			if pathutil.IsReservedComponent(filepath.Base(rel)) {
				return fs.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}

		fi, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", rel, err)
		}

		// Fast path: reuse hash when size and mtime match a live index entry.
		// Intentional: mtime-only (touch) changes miss this path and rehash.
		// Size alone is not enough — same-size content rewrites always bump
		// mtime, and skipping the hash there would permanently miss the edit.
		// Mode-only chmod preserves mtime on common OS filesystems, so those
		// still hit the fast path; pure metadata after a rehash reuses hash
		// below when size is unchanged.
		hash := ""
		prev, hasPrev := idx.Get(rel)
		if !opts.forceRehash() && hasPrev && !prev.Deleted &&
			prev.Size == fi.Size() && prev.ModTime.Equal(fi.ModTime()) && prev.Hash != "" {
			hash = prev.Hash
		} else {
			hash, err = opts.hash(ctx, root, rel)
			if err != nil {
				return fmt.Errorf("hash %s: %w", rel, err)
			}
		}

		var prevUpdated time.Time
		if hasPrev {
			prevUpdated = prev.UpdatedAt
		}
		disk[rel] = index.Entry{
			Path:      rel,
			Size:      fi.Size(),
			ModTime:   fi.ModTime(),
			Mode:      fi.Mode().Perm(),
			Hash:      hash,
			Deleted:   false,
			UpdatedAt: stampUpdatedAt(now, fi.ModTime(), prevUpdated),
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk sync root: %w", err)
	}

	var changes []Change
	known := idx.All()

	// Detect adds and modifies.
	for path, de := range disk {
		prev, ok := known[path]
		if !ok || prev.Deleted {
			entry := de
			entry.UpdatedAt = stampUpdatedAt(now, de.ModTime, prev.UpdatedAt)
			changes = append(changes, Change{Kind: Added, Path: path, Entry: entry})
			continue
		}
		// Content or metadata (mode / mtime) change.
		if prev.Hash != de.Hash || prev.Size != de.Size || prev.Mode != de.Mode || !prev.ModTime.Equal(de.ModTime) {
			entry := de
			entry.UpdatedAt = stampUpdatedAt(now, de.ModTime, prev.UpdatedAt)
			// Metadata-only (mode and/or mtime): keep existing hash/size.
			if prev.Hash == de.Hash && prev.Size == de.Size {
				entry.Hash = prev.Hash
				entry.Size = prev.Size
			}
			changes = append(changes, Change{Kind: Modified, Path: path, Entry: entry})
		}
	}

	// Detect offline (and online) deletions: live index entry missing on disk.
	for path, prev := range known {
		if prev.Deleted {
			continue
		}
		if _, ok := disk[path]; !ok {
			tomb := prev
			tomb.Deleted = true
			tomb.DeletedAt = now
			tomb.UpdatedAt = stampUpdatedAt(now, time.Time{}, prev.UpdatedAt)
			tomb.Size = 0
			tomb.Hash = ""
			changes = append(changes, Change{Kind: Deleted, Path: path, Entry: tomb})
		}
	}

	return &Result{Changes: changes, Disk: disk}, nil
}

// Apply writes Scan changes into the index (adds, modifies, tombstones).
// Each change is applied only if it still wins against the current index entry
// (avoids clobbering a peer update that landed after Scan started).
func Apply(idx *index.Index, res *Result) int {
	n := 0
	for _, c := range res.Changes {
		if idx.SetIfWins(c.Entry) {
			n++
		}
	}
	return n
}
