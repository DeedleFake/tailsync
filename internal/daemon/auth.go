package daemon

import (
	"context"
	"sync"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
)

// authURLTracker deduplicates AuthURL deliveries so status re-observations
// do not spam the same login URL.
type authURLTracker struct {
	mu      sync.Mutex
	lastURL string
	emit    func(url string)
}

func newAuthURLTracker(emit func(url string)) *authURLTracker {
	return &authURLTracker{emit: emit}
}

// observe delivers url to the callback when non-empty and distinct from the
// last delivered URL. Safe for concurrent use.
func (t *authURLTracker) observe(url string) {
	if t == nil || url == "" {
		return
	}
	t.mu.Lock()
	if url == t.lastURL {
		t.mu.Unlock()
		return
	}
	t.lastURL = url
	cb := t.emit
	t.mu.Unlock()
	if cb != nil {
		cb(url)
	}
}

// last returns the most recently observed URL (for tests).
func (t *authURLTracker) last() string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastURL
}

// authURLFromNotify extracts a browser login URL from an IPN bus notification.
// Prefers BrowseToURL (structured “open browser now”), then InitialStatus.AuthURL.
func authURLFromNotify(n ipn.Notify) string {
	if n.BrowseToURL != nil {
		if u := *n.BrowseToURL; u != "" {
			return u
		}
	}
	if n.InitialStatus != nil {
		if u := n.InitialStatus.AuthURL; u != "" {
			return u
		}
	}
	return ""
}

// authURLFromStatus returns Status.AuthURL when set.
func authURLFromStatus(st *ipnstate.Status) string {
	if st == nil {
		return ""
	}
	return st.AuthURL
}

// backendRunning reports whether status indicates the node is Running.
func backendRunning(st *ipnstate.Status) bool {
	return st != nil && st.BackendState == "Running"
}

// notifyBackendRunning reports whether an IPN notify indicates Running.
func notifyBackendRunning(n ipn.Notify) bool {
	if n.State != nil && *n.State == ipn.Running {
		return true
	}
	return n.InitialStatus != nil && backendRunning(n.InitialStatus)
}

// authURLToObserve returns a login URL from status only when the backend is
// not already Running (avoids post-success AuthURL leftovers).
func authURLToObserve(st *ipnstate.Status) string {
	if st == nil || backendRunning(st) {
		return ""
	}
	return st.AuthURL
}

// watchTSNetAuthURL watches LocalAPI / IPN status during tsnet bring-up and
// invokes the tracker when an AuthURL appears. Stops when ctx is canceled or
// the backend reaches Running. Prefer structured bus notifications; poll
// Status as a backup (same source tsnet’s printAuthURLLoop uses).
//
// On return (cancel or Running), nested bus watching has been joined so the
// caller may Close the tsnet server without racing LocalClient use, and
// OnAuthURL will not be invoked after this function returns.
func (d *Daemon) watchTSNetAuthURL(ctx context.Context, s *tsnet.Server, tracker *authURLTracker) {
	if tracker == nil {
		return
	}
	lc, err := waitLocalClient(ctx, s)
	if err != nil {
		return
	}

	// Primary path: IPN bus (BrowseToURL / initial status).
	// Local cancel joins the bus goroutine on every exit (Running, parent cancel).
	busCtx, busCancel := context.WithCancel(ctx)
	defer busCancel()
	busDone := make(chan struct{})
	go func() {
		defer close(busDone)
		d.watchIPNAuthBus(busCtx, lc, tracker)
	}()
	defer func() {
		busCancel()
		<-busDone
	}()

	// Backup: periodic Status.AuthURL (covers delayed URL assignment).
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	if d.pollAuthStatus(ctx, lc, tracker) {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-busDone:
			// Bus ended (error or Running); keep polling until ctx cancel
			// or Running so a late AuthURL still surfaces before Up returns.
			for {
				if d.pollAuthStatus(ctx, lc, tracker) {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		case <-ticker.C:
			if d.pollAuthStatus(ctx, lc, tracker) {
				return
			}
		}
	}
}

func waitLocalClient(ctx context.Context, s *tsnet.Server) (*local.Client, error) {
	// LocalClient becomes available once tsnet.Start has finished init.
	// Up and this watcher race Start; retry until ready or ctx cancel.
	if lc, err := s.LocalClient(); err == nil {
		return lc, nil
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if lc, err := s.LocalClient(); err == nil {
				return lc, nil
			}
		}
	}
}

func (d *Daemon) watchIPNAuthBus(ctx context.Context, lc *local.Client, tracker *authURLTracker) {
	watcher, err := lc.WatchIPNBus(ctx, ipn.NotifyInitialState|ipn.NotifyInitialStatus)
	if err != nil {
		if d != nil && d.log != nil {
			d.log.Debug("tsnet auth bus watch unavailable", "err", err)
		}
		return
	}
	defer watcher.Close()

	for {
		n, err := watcher.Next()
		if err != nil {
			return
		}
		// Running first: never observe AuthURL after successful login.
		if notifyBackendRunning(n) {
			return
		}
		if u := authURLFromNotify(n); u != "" {
			tracker.observe(u)
		}
		// When NeedsLogin, also fetch Status for AuthURL (may lag BrowseToURL).
		if n.State != nil && *n.State == ipn.NeedsLogin {
			if d.pollAuthStatus(ctx, lc, tracker) {
				return
			}
		}
	}
}

// pollAuthStatus observes AuthURL from Status when not Running.
// Returns true when Running (caller should stop watching).
func (d *Daemon) pollAuthStatus(ctx context.Context, lc *local.Client, tracker *authURLTracker) bool {
	st, err := lc.StatusWithoutPeers(ctx)
	if err != nil {
		return false
	}
	if backendRunning(st) {
		return true
	}
	if u := authURLToObserve(st); u != "" {
		tracker.observe(u)
	}
	return false
}
