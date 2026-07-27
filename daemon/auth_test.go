package daemon

import (
	"sync"
	"testing"

	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
)

func TestAuthURLTrackerDedup(t *testing.T) {
	t.Parallel()
	var (
		mu  sync.Mutex
		got []string
	)
	tr := newAuthURLTracker(func(url string) {
		mu.Lock()
		got = append(got, url)
		mu.Unlock()
	})

	tr.observe("")
	tr.observe("https://login.tailscale.com/a/one")
	tr.observe("https://login.tailscale.com/a/one") // same — skip
	tr.observe("https://login.tailscale.com/a/two")
	tr.observe("https://login.tailscale.com/a/two")
	tr.observe("")

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("got %v want 2 distinct URLs", got)
	}
	if got[0] != "https://login.tailscale.com/a/one" || got[1] != "https://login.tailscale.com/a/two" {
		t.Fatalf("got %v", got)
	}
	if tr.last() != "https://login.tailscale.com/a/two" {
		t.Fatalf("last: %q", tr.last())
	}
}

func TestAuthURLTrackerNilSafe(t *testing.T) {
	t.Parallel()
	var tr *authURLTracker
	tr.observe("https://example.com") // no panic
	if tr.last() != "" {
		t.Fatal("nil tracker last")
	}
	tr = newAuthURLTracker(nil)
	tr.observe("https://example.com") // no panic, records last
	if tr.last() != "https://example.com" {
		t.Fatalf("last: %q", tr.last())
	}
}

func TestAuthURLFromNotify(t *testing.T) {
	t.Parallel()
	browse := "https://login.tailscale.com/a/browse"
	if got := authURLFromNotify(ipn.Notify{}); got != "" {
		t.Fatalf("empty notify: %q", got)
	}
	if got := authURLFromNotify(ipn.Notify{BrowseToURL: &browse}); got != browse {
		t.Fatalf("browse: %q", got)
	}
	// Empty BrowseToURL falls through to InitialStatus.
	empty := ""
	st := &ipnstate.Status{AuthURL: "https://login.tailscale.com/a/status"}
	if got := authURLFromNotify(ipn.Notify{BrowseToURL: &empty, InitialStatus: st}); got != st.AuthURL {
		t.Fatalf("status fallback: %q", got)
	}
	// Non-empty BrowseToURL wins over InitialStatus.
	if got := authURLFromNotify(ipn.Notify{BrowseToURL: &browse, InitialStatus: st}); got != browse {
		t.Fatalf("browse preferred: %q", got)
	}
}

func TestAuthURLFromStatus(t *testing.T) {
	t.Parallel()
	if got := authURLFromStatus(nil); got != "" {
		t.Fatalf("nil: %q", got)
	}
	if got := authURLFromStatus(&ipnstate.Status{}); got != "" {
		t.Fatalf("empty: %q", got)
	}
	if got := authURLFromStatus(&ipnstate.Status{AuthURL: "https://login.example/x"}); got != "https://login.example/x" {
		t.Fatalf("got %q", got)
	}
}

func TestBackendRunning(t *testing.T) {
	t.Parallel()
	if backendRunning(nil) {
		t.Fatal("nil")
	}
	if backendRunning(&ipnstate.Status{BackendState: "NeedsLogin"}) {
		t.Fatal("NeedsLogin")
	}
	if !backendRunning(&ipnstate.Status{BackendState: "Running"}) {
		t.Fatal("Running")
	}
}

func TestAuthURLToObserveSkipsRunning(t *testing.T) {
	t.Parallel()
	if got := authURLToObserve(nil); got != "" {
		t.Fatalf("nil: %q", got)
	}
	// Running with leftover AuthURL must not be observed.
	st := &ipnstate.Status{
		BackendState: "Running",
		AuthURL:      "https://login.tailscale.com/a/stale",
	}
	if got := authURLToObserve(st); got != "" {
		t.Fatalf("Running should skip AuthURL, got %q", got)
	}
	// NeedsLogin with AuthURL is observed.
	st.BackendState = "NeedsLogin"
	if got := authURLToObserve(st); got != st.AuthURL {
		t.Fatalf("NeedsLogin: %q", got)
	}
}

func TestNotifyBackendRunning(t *testing.T) {
	t.Parallel()
	if notifyBackendRunning(ipn.Notify{}) {
		t.Fatal("empty")
	}
	running := ipn.Running
	if !notifyBackendRunning(ipn.Notify{State: &running}) {
		t.Fatal("State Running")
	}
	needs := ipn.NeedsLogin
	if notifyBackendRunning(ipn.Notify{State: &needs}) {
		t.Fatal("NeedsLogin")
	}
	// InitialStatus Running (even with AuthURL) counts as running.
	if !notifyBackendRunning(ipn.Notify{
		InitialStatus: &ipnstate.Status{
			BackendState: "Running",
			AuthURL:      "https://login.tailscale.com/a/x",
		},
	}) {
		t.Fatal("InitialStatus Running")
	}
}

func TestAuthURLTrackerConcurrent(t *testing.T) {
	t.Parallel()
	var count int
	var mu sync.Mutex
	tr := newAuthURLTracker(func(url string) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 50 {
				tr.observe("https://login.tailscale.com/a/same")
			}
		})
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("want 1 emit under concurrent same-URL observes, got %d", count)
	}
}
