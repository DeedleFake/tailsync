package tagset_test

import (
	"testing"

	"deedles.dev/tailsync/internal/tagset"
)

func TestIntersect(t *testing.T) {
	if !tagset.Intersect([]string{"a", "b"}, []string{"b", "c"}) {
		t.Fatal("share b")
	}
	if tagset.Intersect([]string{"a"}, []string{"b"}) {
		t.Fatal("disjoint")
	}
	if tagset.Intersect(nil, []string{"a"}) || tagset.Intersect([]string{"a"}, nil) {
		t.Fatal("empty")
	}
	if tagset.Intersect([]string{""}, []string{""}) {
		t.Fatal("only empty strings")
	}
}

func TestEqual(t *testing.T) {
	if !tagset.Equal([]string{"a", "b"}, []string{"b", "a"}) {
		t.Fatal("order independent")
	}
	if !tagset.Equal([]string{"a", "a"}, []string{"a"}) {
		t.Fatal("duplicates collapsed")
	}
	if tagset.Equal([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("superset")
	}
	if !tagset.Equal(nil, nil) {
		t.Fatal("both empty")
	}
}

func TestContainsAll(t *testing.T) {
	if !tagset.ContainsAll([]string{"a", "b", "c"}, []string{"a", "c"}) {
		t.Fatal("subset")
	}
	if !tagset.ContainsAll([]string{"a"}, []string{"a"}) {
		t.Fatal("exact")
	}
	if tagset.ContainsAll([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("missing b")
	}
	if tagset.ContainsAll([]string{"a"}, nil) {
		t.Fatal("empty need")
	}
	if tagset.ContainsAll([]string{"a"}, []string{""}) {
		t.Fatal("only empty need")
	}
}
