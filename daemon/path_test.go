package daemon

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRelPath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	d := &Daemon{cfg: Config{Dir: root}}

	ok := []string{"a.txt", "foo/bar.txt", "foo..bar.txt", "dir/file..txt"}
	for _, p := range ok {
		rel, err := d.relPath(p)
		if err != nil {
			t.Fatalf("relPath(%q): %v", p, err)
		}
		if rel == "" {
			t.Fatalf("empty rel for %q", p)
		}
		if filepath.IsAbs(rel) || rel[0] == '/' {
			t.Fatalf("relPath(%q) = %q; want relative", p, rel)
		}
	}

	bad := []string{
		"", "/etc/passwd", "..", "../x", "a/../../etc/passwd", "a/../..", ".", "foo/../../../x",
		// Reserved state trees (default StateDir lives under Dir/.tailsync).
		".tailsync", ".tailsync/index.json", "foo/.tailsync/x", ".tailsync-tmp/x",
	}
	for _, p := range bad {
		if _, err := d.relPath(p); err == nil {
			t.Fatalf("relPath(%q) should fail", p)
		}
	}
}
