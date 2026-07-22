// Package index provides a persistent file index used for offline deletion
// detection and last-writer-wins conflict resolution.
package index

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Version is the on-disk schema version.
const Version = 1

// DefaultTombstoneTTL is how long deletion tombstones are retained before GC.
const DefaultTombstoneTTL = 30 * 24 * time.Hour

// Entry describes a known file (or a deletion tombstone).
type Entry struct {
	// Path is relative to the sync root, using forward slashes.
	Path string `json:"path"`
	// Size is the file size in bytes (0 for tombstones).
	Size int64 `json:"size"`
	// ModTime is the file's modification time as observed locally.
	ModTime time.Time `json:"mod_time"`
	// Mode is the file permission bits.
	Mode os.FileMode `json:"mode"`
	// Hash is the hex-encoded SHA-256 of the file contents (empty for tombstones).
	Hash string `json:"hash,omitempty"`
	// Deleted is true when this entry is a deletion tombstone.
	Deleted bool `json:"deleted,omitempty"`
	// DeletedAt is when the deletion was recorded (zero if not deleted).
	DeletedAt time.Time `json:"deleted_at"`
	// UpdatedAt is when this entry last changed; used for LWW among peers.
	// Callers should keep wall clocks roughly in sync (NTP); ties use a stable
	// total order (see Wins).
	UpdatedAt time.Time `json:"updated_at"`
}

// Index is a thread-safe map of relative path → Entry, backed by a JSON file.
type Index struct {
	mu      sync.RWMutex
	version int
	files   map[string]Entry
	path    string // on-disk path; empty if not yet bound
}

// diskFormat is the JSON serialization shape.
type diskFormat struct {
	Version int              `json:"version"`
	Files   map[string]Entry `json:"files"`
}

// New returns an empty in-memory index.
func New() *Index {
	return &Index{
		version: Version,
		files:   make(map[string]Entry),
	}
}

// Load reads an index from path. Missing files yield an empty index bound to path.
func Load(path string) (*Index, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			idx := New()
			idx.path = path
			return idx, nil
		}
		return nil, fmt.Errorf("read index %s: %w", path, err)
	}

	var df diskFormat
	if err := json.Unmarshal(data, &df); err != nil {
		return nil, fmt.Errorf("decode index %s: %w", path, err)
	}
	if df.Version == 0 {
		df.Version = Version
	}
	if df.Files == nil {
		df.Files = make(map[string]Entry)
	}
	// Ensure Path field is populated from the map key.
	for p, e := range df.Files {
		if e.Path == "" {
			e.Path = p
			df.Files[p] = e
		}
	}
	return &Index{
		version: df.Version,
		files:   df.Files,
		path:    path,
	}, nil
}

// Path returns the on-disk path for this index (may be empty).
func (idx *Index) Path() string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.path
}

// SetPath binds the index to an on-disk location for Save.
func (idx *Index) SetPath(path string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.path = path
}

// Save writes the index atomically (temp file + rename).
// The mutex is held only while snapshotting; disk I/O runs unlocked.
func (idx *Index) Save() error {
	idx.mu.RLock()
	if idx.path == "" {
		idx.mu.RUnlock()
		return fmt.Errorf("index has no path")
	}
	path := idx.path
	version := idx.version
	files := make(map[string]Entry, len(idx.files))
	maps.Copy(files, idx.files)
	idx.mu.RUnlock()
	return writeIndexFile(path, version, files)
}

func writeIndexFile(path string, version int, files map[string]Entry) error {
	df := diskFormat{
		Version: version,
		Files:   files,
	}
	data, err := json.MarshalIndent(df, "", "  ")
	if err != nil {
		return fmt.Errorf("encode index: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".tailsync-index-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp index: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp index: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp index: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp index: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename index: %w", err)
	}
	cleanup = false
	return nil
}

// Get returns a copy of the entry for path, if any.
func (idx *Index) Get(path string) (Entry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	e, ok := idx.files[path]
	return e, ok
}

// Set stores or replaces an entry and returns the previous value if any.
func (idx *Index) Set(e Entry) (prev Entry, had bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	prev, had = idx.files[e.Path]
	idx.files[e.Path] = e
	return prev, had
}

// SetIfWins stores remote only if it wins LWW against the current entry
// (or the path is unknown). Returns whether the index was updated.
func (idx *Index) SetIfWins(remote Entry) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	local, ok := idx.files[remote.Path]
	if ok && !Wins(local, remote) {
		return false
	}
	idx.files[remote.Path] = remote
	return true
}

// Delete removes an entry entirely (not a tombstone). Prefer MarkDeleted.
func (idx *Index) Delete(path string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.files, path)
}

// MarkDeleted records a tombstone for path.
func (idx *Index) MarkDeleted(path string, at time.Time) Entry {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	e, ok := idx.files[path]
	if !ok {
		e = Entry{Path: path}
	}
	e.Deleted = true
	e.DeletedAt = at
	e.UpdatedAt = at
	e.Size = 0
	e.Hash = ""
	idx.files[path] = e
	return e
}

// All returns a snapshot of all entries.
func (idx *Index) All() map[string]Entry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make(map[string]Entry, len(idx.files))
	maps.Copy(out, idx.files)
	return out
}

// Live returns non-deleted entries only.
func (idx *Index) Live() map[string]Entry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make(map[string]Entry)
	for k, v := range idx.files {
		if !v.Deleted {
			out[k] = v
		}
	}
	return out
}

// Len returns the number of entries including tombstones.
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.files)
}

// Clone returns a deep copy of the index (no path binding).
func (idx *Index) Clone() *Index {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	c := New()
	c.version = idx.version
	maps.Copy(c.files, idx.files)
	return c
}

// GCTombstones removes deleted entries whose DeletedAt is older than ttl
// (relative to now). Returns the number of entries removed.
// Tombstones with zero DeletedAt use UpdatedAt as the age basis.
//
// Resurrection risk: once a tombstone is dropped, a lagging peer that still
// has a live file for that path can re-introduce it as a plain add. Operators
// should keep the TTL longer than the maximum expected peer offline window,
// or use -peers with all members regularly online. Peer-ack GC is not implemented.
func (idx *Index) GCTombstones(now time.Time, ttl time.Duration) int {
	if ttl <= 0 {
		return 0
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	cutoff := now.Add(-ttl)
	n := 0
	for path, e := range idx.files {
		if !e.Deleted {
			continue
		}
		t := e.DeletedAt
		if t.IsZero() {
			t = e.UpdatedAt
		}
		if !t.IsZero() && t.Before(cutoff) {
			delete(idx.files, path)
			n++
		}
	}
	return n
}

// ManifestEntry is a compact view exchanged with peers.
type ManifestEntry struct {
	Path      string      `json:"path"`
	Size      int64       `json:"size"`
	ModTime   time.Time   `json:"mod_time"`
	Mode      os.FileMode `json:"mode"`
	Hash      string      `json:"hash,omitempty"`
	Deleted   bool        `json:"deleted,omitempty"`
	DeletedAt time.Time   `json:"deleted_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// Manifest returns all entries as a list suitable for peer exchange.
func (idx *Index) Manifest() []ManifestEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make([]ManifestEntry, 0, len(idx.files))
	for _, e := range idx.files {
		out = append(out, ManifestEntry(e))
	}
	return out
}

// Wins reports whether remote should replace local under last-writer-wins.
//
// Ordering:
//  1. Higher UpdatedAt wins.
//  2. On equal UpdatedAt, prefer remote only when the entries differ, using a
//     stable total order: Deleted (true > false), then Hash (string compare),
//     then Mode, then ModTime (later mtime wins). Preferring remote always
//     would flip-flop under concurrent mutual sync; a stable order guarantees
//     both sides converge to the same winner.
//
// Entries that differ only in ModTime are not treated as identical, so a
// touch-only peer update still participates in LWW (via UpdatedAt first, or
// the ModTime tie-break when clocks match).
func Wins(local, remote Entry) bool {
	if remote.UpdatedAt.After(local.UpdatedAt) {
		return true
	}
	if local.UpdatedAt.After(remote.UpdatedAt) {
		return false
	}
	// Equal timestamps: identical content/deletion/metadata → no change.
	if local.Deleted == remote.Deleted && local.Hash == remote.Hash &&
		local.Mode == remote.Mode && local.ModTime.Equal(remote.ModTime) {
		return false
	}
	// Stable total order for ties.
	return compareEntries(remote, local) > 0
}

// compareEntries returns cmp-style ordering for equal-timestamp ties.
// Deleted sorts above live; then higher Hash string; then higher Mode;
// then later ModTime.
func compareEntries(a, b Entry) int {
	if a.Deleted != b.Deleted {
		if a.Deleted {
			return 1
		}
		return -1
	}
	if c := cmp.Compare(a.Hash, b.Hash); c != 0 {
		return c
	}
	if c := cmp.Compare(uint32(a.Mode), uint32(b.Mode)); c != 0 {
		return c
	}
	// Later ModTime sorts higher so mtime-only peers converge without flip-flop.
	if a.ModTime.After(b.ModTime) {
		return 1
	}
	if b.ModTime.After(a.ModTime) {
		return -1
	}
	return 0
}

// EntryFromManifest converts a ManifestEntry to Entry.
func EntryFromManifest(m ManifestEntry) Entry {
	return Entry(m)
}
