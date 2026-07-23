package pathutil_test

import (
	"testing"

	"deedles.dev/tailsync/internal/pathutil"
)

func TestIsReservedComponent(t *testing.T) {
	if !pathutil.IsReservedComponent(".tailsync") {
		t.Fatal("want .tailsync reserved")
	}
	if !pathutil.IsReservedComponent(".tailsync-tmp") {
		t.Fatal("want .tailsync- prefix reserved")
	}
	if pathutil.IsReservedComponent("tailsync") {
		t.Fatal("tailsync alone is not reserved")
	}
	if pathutil.IsReservedComponent("foo") {
		t.Fatal("foo is not reserved")
	}
}

func TestCleanRel(t *testing.T) {
	ok := []string{"a.txt", "foo/bar.txt", "foo..bar.txt", "dir/file..txt"}
	for _, p := range ok {
		got, err := pathutil.CleanRel(p)
		if err != nil {
			t.Fatalf("CleanRel(%q): %v", p, err)
		}
		if got == "" {
			t.Fatalf("empty for %q", p)
		}
	}

	bad := []string{
		"", "/etc/passwd", "..", "../x", "a/../../etc/passwd", "a/../..", ".", "foo/../../../x",
		".tailsync", ".tailsync/index.json", "foo/.tailsync/x", ".tailsync-tmp/x",
	}
	for _, p := range bad {
		if _, err := pathutil.CleanRel(p); err == nil {
			t.Fatalf("CleanRel(%q) should fail", p)
		}
	}
}
