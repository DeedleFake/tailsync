package daemon

import (
	"os"
	"path/filepath"
	"testing"

	"deedles.dev/tailsync/internal/atomicfile"
)

// TestRootConfinesSymlinkEscape verifies that a symlink under the sync tree
// pointing outside cannot be used to read or write outside via Root I/O
// (defense-in-depth beyond relPath string checks).
func TestRootConfinesSymlinkEscape(t *testing.T) {
	base := t.TempDir()
	syncDir := filepath.Join(base, "sync")
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(syncDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	secretPath := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secretPath, []byte("top-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Symlink inside sync tree → outside secret.
	link := filepath.Join(syncDir, "escape")
	if err := os.Symlink(secretPath, link); err != nil {
		t.Fatal(err)
	}

	root, err := os.OpenRoot(syncDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })

	// Read through the escaping symlink must fail (Root refuses out-of-tree targets).
	if _, err := root.ReadFile("escape"); err == nil {
		t.Fatal("root.ReadFile through outside symlink should fail")
	}
	if _, err := root.Open("escape"); err == nil {
		t.Fatal("root.Open through outside symlink should fail")
	}
	if _, err := root.Stat("escape"); err == nil {
		t.Fatal("root.Stat through outside symlink should fail")
	}

	// Write via atomicfile must not replace the outside target through the link.
	// On Linux, Root Rename replaces the symlink with a regular file in-tree.
	err = atomicfile.WriteFileRoot(root, "escape", []byte("pwned"), 0o644)
	data, readErr := os.ReadFile(secretPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(data) != "top-secret" {
		t.Fatalf("outside file was modified: %q", data)
	}
	if err != nil {
		t.Logf("WriteFileRoot on symlink path: %v", err)
	} else {
		// Success path: symlink must be replaced by a regular file with new content.
		fi, lerr := os.Lstat(link)
		if lerr != nil {
			t.Fatal(lerr)
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			t.Fatal("expected symlink to be replaced by regular file")
		}
		if !fi.Mode().IsRegular() {
			t.Fatalf("want regular file, got mode %v", fi.Mode())
		}
		got, rerr := root.ReadFile("escape")
		if rerr != nil {
			t.Fatal(rerr)
		}
		if string(got) != "pwned" {
			t.Fatalf("in-tree content %q, want pwned", got)
		}
	}

	// relPath still rejects ".." escapes at the string layer.
	d := &Daemon{cfg: Config{Dir: syncDir}, root: root}
	if _, err := d.relPath("../outside/secret.txt"); err == nil {
		t.Fatal("relPath should reject parent escape")
	}
}

// TestRootConfinesDirSymlinkEscape covers an intermediate directory symlink
// (subdir → outside) used as a path prefix for Open / WriteFileRoot.
func TestRootConfinesDirSymlinkEscape(t *testing.T) {
	base := t.TempDir()
	syncDir := filepath.Join(base, "sync")
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(syncDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(outside, "planted.txt")
	if err := os.WriteFile(marker, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(syncDir, "subdir")); err != nil {
		t.Fatal(err)
	}

	root, err := os.OpenRoot(syncDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })

	if _, err := root.Open("subdir/planted.txt"); err == nil {
		t.Fatal("root.Open via dir symlink should fail")
	}
	if _, err := root.Stat("subdir/x"); err == nil {
		t.Fatal("root.Stat via dir symlink should fail")
	}
	if err := atomicfile.WriteFileRoot(root, "subdir/x", []byte("pwned"), 0o644); err == nil {
		t.Fatal("WriteFileRoot via dir symlink should fail")
	}
	// Outside tree must be unchanged (no new file, marker intact).
	if _, err := os.Stat(filepath.Join(outside, "x")); !os.IsNotExist(err) {
		t.Fatalf("outside/x should not exist, err=%v", err)
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "outside" {
		t.Fatalf("marker changed: %q", data)
	}
}

func TestWriteFileRootCreatesNested(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })

	if err := atomicfile.WriteFileRoot(root, "a/b/c.txt", []byte("hi"), 0o640); err != nil {
		t.Fatal(err)
	}
	got, err := root.ReadFile("a/b/c.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hi" {
		t.Fatalf("got %q", got)
	}
	fi, err := root.Stat("a/b/c.txt")
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o640 {
		t.Fatalf("mode=%o want 640", perm)
	}
	// Temp files must not remain under the tree.
	ents, err := os.ReadDir(filepath.Join(dir, "a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if e.Name() != "c.txt" {
			t.Fatalf("unexpected entry left behind: %s", e.Name())
		}
	}
}

func TestWriteFileRootRejectsBadPaths(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })

	for _, p := range []string{"", ".", "..", "/etc/passwd", "a/../../x", "../x"} {
		if err := atomicfile.WriteFileRoot(root, p, []byte("x"), 0o644); err == nil {
			t.Fatalf("WriteFileRoot(%q) should fail", p)
		}
	}
}
