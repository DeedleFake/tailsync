package daemon

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAbsPath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	d := &Daemon{cfg: Config{Dir: root}}

	ok := []string{"a.txt", "foo/bar.txt", "foo..bar.txt", "dir/file..txt"}
	for _, p := range ok {
		abs, err := d.absPath(p)
		if err != nil {
			t.Fatalf("absPath(%q): %v", p, err)
		}
		if abs == "" {
			t.Fatalf("empty abs for %q", p)
		}
	}

	bad := []string{"", "/etc/passwd", "..", "../x", "a/../../etc/passwd", "a/../..", ".", "foo/../../../x"}
	for _, p := range bad {
		if _, err := d.absPath(p); err == nil {
			t.Fatalf("absPath(%q) should fail", p)
		}
	}
}
