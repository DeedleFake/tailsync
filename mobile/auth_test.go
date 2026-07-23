package mobile

import (
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
)

func TestNoteAuthURLEmitsEventAndStatus(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	n, err := NewNode(&Config{Dir: dir, NetMode: "plain"})
	if err != nil {
		t.Fatal(err)
	}
	lis := &captureListener{}
	n.SetListener(lis)
	n.acceptAuthURL = true // simulate claimStart auth window

	// No auth needed initially.
	s, err := n.StatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(s, "auth_url") || strings.Contains(s, `"needs_login":true`) {
		t.Fatalf("unexpected auth fields when idle: %s", s)
	}

	const url = "https://login.tailscale.com/a/test-auth"
	n.noteAuthURL(url)
	n.noteAuthURL(url) // dedup — one event

	evs := lis.snapshot()
	if len(evs) != 1 {
		t.Fatalf("want 1 auth event, got %d: %v", len(evs), evs)
	}
	var ev map[string]any
	if err := json.Unmarshal([]byte(evs[0]), &ev); err != nil {
		t.Fatal(err)
	}
	if ev["type"] != "auth" {
		t.Fatalf("type: %v", ev["type"])
	}
	if ev["url"] != url {
		t.Fatalf("url: %v", ev["url"])
	}

	s, err = n.StatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	if err := json.Unmarshal([]byte(s), &st); err != nil {
		t.Fatal(err)
	}
	if st["needs_login"] != true {
		t.Fatalf("needs_login: %v", st["needs_login"])
	}
	if st["auth_url"] != url {
		t.Fatalf("auth_url: %v", st["auth_url"])
	}
	if strings.Contains(s, "tskey") || strings.Contains(s, "AuthKey") {
		t.Fatal("auth key-like material in StatusJSON")
	}

	// Distinct URL emits again.
	const url2 = "https://login.tailscale.com/a/other"
	n.noteAuthURL(url2)
	if len(lis.snapshot()) != 2 {
		t.Fatalf("want 2 auth events, got %d", len(lis.snapshot()))
	}

	n.clearAuthState()
	s, err = n.StatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(s, "auth_url") || strings.Contains(s, `"needs_login":true`) {
		t.Fatalf("auth fields should clear: %s", s)
	}
}

func TestNoteAuthURLNoEventWhenEmpty(t *testing.T) {
	t.Parallel()
	n := &Node{acceptAuthURL: true}
	lis := &captureListener{}
	n.SetListener(lis)
	n.noteAuthURL("")
	if len(lis.snapshot()) != 0 {
		t.Fatalf("unexpected events: %v", lis.snapshot())
	}
	if n.needsLogin {
		t.Fatal("needsLogin should stay false")
	}
}

func TestNoteAuthURLNoListener(t *testing.T) {
	t.Parallel()
	n := &Node{acceptAuthURL: true}
	// Still updates status fields without a listener.
	n.noteAuthURL("https://login.tailscale.com/a/x")
	if !n.needsLogin || n.authURL == "" {
		t.Fatal("expected auth state without listener")
	}
}

func TestNoteAuthURLIgnoredWithoutAccept(t *testing.T) {
	t.Parallel()
	n := &Node{} // acceptAuthURL false (idle)
	lis := &captureListener{}
	n.SetListener(lis)
	n.noteAuthURL("https://login.tailscale.com/a/late")
	if len(lis.snapshot()) != 0 {
		t.Fatalf("want no events when not accepting: %v", lis.snapshot())
	}
	if n.needsLogin || n.authURL != "" {
		t.Fatal("auth state should stay empty without acceptAuthURL")
	}
}

func TestNoteAuthURLIgnoredAfterClear(t *testing.T) {
	t.Parallel()
	n := &Node{acceptAuthURL: true}
	lis := &captureListener{}
	n.SetListener(lis)

	const url = "https://login.tailscale.com/a/during-start"
	n.noteAuthURL(url)
	if len(lis.snapshot()) != 1 {
		t.Fatalf("want 1 event during accept window, got %d", len(lis.snapshot()))
	}

	// Simulate onReady / finish.
	n.clearAuthState()
	if n.acceptAuthURL {
		t.Fatal("clearAuthState should drop acceptAuthURL")
	}

	// Late watcher callback must not re-arm needs_login or emit again.
	n.noteAuthURL("https://login.tailscale.com/a/late-after-ready")
	n.noteAuthURL(url)
	if len(lis.snapshot()) != 1 {
		t.Fatalf("want still 1 event after clear, got %d: %v", len(lis.snapshot()), lis.snapshot())
	}
	s, err := n.StatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(s, "auth_url") || strings.Contains(s, `"needs_login":true`) {
		t.Fatalf("StatusJSON must not show auth after clear+late callback: %s", s)
	}
	if n.needsLogin || n.authURL != "" {
		t.Fatal("residual auth state after clear+late noteAuthURL")
	}
}

func TestAuthEventNeverIncludesAuthKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	n, err := NewNode(&Config{
		Dir:     dir,
		NetMode: "plain",
		AuthKey: "tskey-should-not-appear-in-events",
	})
	if err != nil {
		t.Fatal(err)
	}
	lis := &captureListener{}
	n.SetListener(lis)
	n.acceptAuthURL = true
	n.noteAuthURL("https://login.tailscale.com/a/safe")

	for _, e := range lis.snapshot() {
		if strings.Contains(e, "tskey-should-not-appear") {
			t.Fatalf("auth key leaked in event: %s", e)
		}
	}
	s, err := n.StatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(s, "tskey-should-not-appear") {
		t.Fatal("auth key leaked in StatusJSON")
	}
}

func TestNoteAuthURLConcurrentDedup(t *testing.T) {
	t.Parallel()
	n := &Node{acceptAuthURL: true}
	lis := &captureListener{}
	n.SetListener(lis)

	const url = "https://login.tailscale.com/a/concurrent"
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for range 10 {
				n.noteAuthURL(url)
			}
		})
	}
	wg.Wait()
	if n := len(lis.snapshot()); n != 1 {
		t.Fatalf("want 1 event under concurrent noteAuthURL, got %d", n)
	}
}

func TestPlainStartDoesNotEmitAuth(t *testing.T) {
	// Plain mode never needs browser auth; ensure no spurious auth events.
	dir := t.TempDir()
	state := t.TempDir()
	port := mustFreePortLocal(t)

	lis := &captureListener{}
	n, err := NewNode(&Config{
		Dir:      dir,
		StateDir: state,
		Hostname: "no-auth",
		Port:     port,
		NetMode:  "plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	n.SetListener(lis)
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Stop() }()

	for _, e := range lis.snapshot() {
		var m map[string]any
		if err := json.Unmarshal([]byte(e), &m); err != nil {
			t.Fatal(err)
		}
		if m["type"] == "auth" {
			t.Fatalf("unexpected auth event in plain mode: %s", e)
		}
	}
	s, err := n.StatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(s, `"needs_login":true`) {
		t.Fatalf("needs_login in plain running status: %s", s)
	}
}

// mustFreePortLocal reserves an ephemeral port for plain-mode tests.
func mustFreePortLocal(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}
