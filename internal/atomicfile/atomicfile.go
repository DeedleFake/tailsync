// Package atomicfile writes files atomically via temp file + renames under an [os.Root].
package atomicfile

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
)

// WriteFileRoot writes data to a path relative to root using a temporary file
// in the same directory then renaming into place. Parent directories are created
// under root as needed. mode is applied to the final file (default 0o644 if zero).
//
// name must be relative to root (slash-separated is preferred). All I/O goes
// through root so path components and symlinks cannot escape the root tree.
// name is validated with [fs.ValidPath] and [filepath.IsLocal] before any I/O.
func WriteFileRoot(root *os.Root, name string, data []byte, mode os.FileMode) error {
	if root == nil {
		return fmt.Errorf("nil root")
	}
	if mode == 0 {
		mode = 0o644
	}
	name = filepath.ToSlash(name)
	cleaned := path.Clean(name)
	if cleaned == "." || path.IsAbs(cleaned) || !fs.ValidPath(cleaned) {
		return fmt.Errorf("invalid path %q", name)
	}
	if !filepath.IsLocal(filepath.FromSlash(cleaned)) {
		return fmt.Errorf("invalid path %q", name)
	}
	name = cleaned

	dir := path.Dir(name)
	if dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}

	tmpName, tmp, err := createTempRoot(root, dir, ".tailsync-write-")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = root.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}
	if err := root.Rename(tmpName, name); err != nil {
		return fmt.Errorf("rename temp to %s: %w", name, err)
	}
	cleanup = false
	return nil
}

// createTempRoot creates an exclusive temp file under dir (relative to root).
// dir may be "." for the root directory itself. Returns the relative temp name.
func createTempRoot(root *os.Root, dir, prefix string) (string, *os.File, error) {
	for range 10000 {
		base := prefix + randomSuffix()
		var name string
		if dir == "." || dir == "" {
			name = base
		} else {
			name = path.Join(dir, base)
		}
		f, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		return name, f, nil
	}
	return "", nil, fmt.Errorf("too many collisions")
}

func randomSuffix() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Extremely unlikely; fall back to a non-crypto value.
		return fmt.Sprintf("%d", os.Getpid())
	}
	return hex.EncodeToString(b[:])
}
