package peer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Default discovery tuning.
const (
	DefaultDiscoveryConcurrency = 32
	DefaultDiscoveryInterval    = 10 * time.Second
	DefaultDialTimeout          = 30 * time.Second
	discoveryBackoffBase        = 2 * time.Second
	discoveryBackoffMax         = 5 * time.Minute
	// After this many consecutive failures, park at max backoff (never ban).
	discoveryBackoffParkAfter = 5
)

// Candidate is a dial target from status, pins, or config.
type Candidate struct {
	// Addr is host:port.
	Addr string
	// NodeID is optional identity when already known.
	NodeID string
}

// Discovery continuously dials candidates that are not yet connected.
// The in-flight semaphore is released before backoff sleep so other peers can
// dial while a failed address waits. Ticks are single-flighted; Kick is
// non-blocking.
type Discovery struct {
	endpoint *Endpoint
	roster   *Roster
	log      *slog.Logger

	concurrency int
	interval    time.Duration
	dialTimeout time.Duration
	handshake   time.Duration

	// candidates returns current dial targets (status Online, pins, …).
	candidates func(ctx context.Context) []Candidate

	// dialPeer performs dial+handshake and installs the session.
	// Provided by Manager so handshake stays in one place.
	dialPeer func(ctx context.Context, c Candidate) error

	// sem is a lifetime in-flight dial semaphore (not recreated per tick).
	sem chan struct{}
	// kick requests a discovery pass (buffered 1; coalesce).
	kick chan struct{}

	mu       sync.Mutex
	backoff  map[string]addrBackoff // addr → backoff
	inflight map[string]struct{}    // addr currently dialing

	// ticking prevents concurrent tick Wait; pending coalesces Kick during tick.
	ticking atomic.Bool
	pending atomic.Bool
}

type addrBackoff struct {
	until  time.Time
	streak int
}

// DiscoveryConfig tunes discovery.
type DiscoveryConfig struct {
	Concurrency      int
	Interval         time.Duration
	DialTimeout      time.Duration
	HandshakeTimeout time.Duration
	Candidates       func(ctx context.Context) []Candidate
	DialPeer         func(ctx context.Context, c Candidate) error
	Log              *slog.Logger
}

func newDiscovery(endpoint *Endpoint, roster *Roster, cfg DiscoveryConfig) *Discovery {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultDiscoveryConcurrency
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultDiscoveryInterval
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = DefaultDialTimeout
	}
	return &Discovery{
		endpoint:    endpoint,
		roster:      roster,
		log:         cfg.Log,
		concurrency: cfg.Concurrency,
		interval:    cfg.Interval,
		dialTimeout: cfg.DialTimeout,
		handshake:   cfg.HandshakeTimeout,
		candidates:  cfg.Candidates,
		dialPeer:    cfg.DialPeer,
		sem:         make(chan struct{}, cfg.Concurrency),
		kick:        make(chan struct{}, 1),
		backoff:     make(map[string]addrBackoff),
		inflight:    make(map[string]struct{}),
	}
}

// Run watches candidates until ctx is cancelled. Single tick loop so Kick and
// the interval cannot stack concurrent ticks beyond one in-flight + one pending.
func (d *Discovery) Run(ctx context.Context) {
	if d.candidates == nil || d.dialPeer == nil {
		return
	}
	// Immediate pass, then interval / kick.
	d.tick(ctx)
	t := time.NewTicker(d.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.tick(ctx)
		case <-d.kick:
			d.tick(ctx)
		}
	}
}

// Kick requests an immediate discovery pass without blocking the caller.
// Coalesces with an in-progress or already-queued tick.
func (d *Discovery) Kick() {
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

func (d *Discovery) tick(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	// Single-flight: if a tick is running, mark pending and return.
	if !d.ticking.CompareAndSwap(false, true) {
		d.pending.Store(true)
		return
	}
	defer func() {
		d.ticking.Store(false)
		if d.pending.Swap(false) {
			// Another kick/tick arrived during this pass.
			d.Kick()
		}
	}()

	cands := d.candidates(ctx)
	if len(cands) == 0 {
		return
	}
	connected := d.roster.ConnectedAddrs()
	connectedIDs := d.roster.ConnectedNodeIDs()

	var wg sync.WaitGroup
	for _, c := range cands {
		if c.Addr == "" {
			continue
		}
		if _, ok := connected[c.Addr]; ok {
			continue
		}
		if c.NodeID != "" {
			if _, ok := connectedIDs[c.NodeID]; ok {
				continue
			}
		}
		if d.shouldSkip(c.Addr) {
			continue
		}
		if !d.tryBeginDial(c.Addr) {
			continue
		}
		wg.Go(func() {
			defer d.endDial(c.Addr)

			select {
			case d.sem <- struct{}{}:
				// Hold semaphore only for dial/handshake, not backoff sleep.
			case <-ctx.Done():
				return
			}
			err := d.dialWithTimeout(ctx, c)
			<-d.sem // release before any backoff accounting (backoff is in softFail)

			if err != nil {
				d.log.Debug("discovery dial", "addr", c.Addr, "err", err)
				d.softFail(c.Addr)
				return
			}
			d.clearBackoff(c.Addr)
		})
	}
	wg.Wait()
}

func (d *Discovery) dialWithTimeout(ctx context.Context, c Candidate) error {
	timeout := d.dialTimeout + d.handshake
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if d.dialPeer == nil {
		return fmt.Errorf("no dial function")
	}
	return d.dialPeer(dctx, c)
}

func (d *Discovery) shouldSkip(addr string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.inflight[addr]; ok {
		return true
	}
	ab, ok := d.backoff[addr]
	if !ok || ab.until.IsZero() {
		return false
	}
	return time.Now().Before(ab.until)
}

func (d *Discovery) tryBeginDial(addr string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.inflight[addr]; ok {
		return false
	}
	if ab, ok := d.backoff[addr]; ok && !ab.until.IsZero() && time.Now().Before(ab.until) {
		return false
	}
	d.inflight[addr] = struct{}{}
	return true
}

func (d *Discovery) endDial(addr string) {
	d.mu.Lock()
	delete(d.inflight, addr)
	d.mu.Unlock()
}

func (d *Discovery) softFail(addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	ab := d.backoff[addr]
	ab.streak++
	ab.until = time.Now().Add(discoveryBackoffDuration(ab.streak))
	d.backoff[addr] = ab
}

func (d *Discovery) clearBackoff(addr string) {
	d.mu.Lock()
	delete(d.backoff, addr)
	d.mu.Unlock()
}

// SoftFailAddr records a failure from outside discovery (e.g. session drop).
func (d *Discovery) SoftFailAddr(addr string) {
	if addr == "" {
		return
	}
	d.softFail(addr)
}

// ClearBackoff clears backoff for addr (e.g. after successful session).
func (d *Discovery) ClearBackoff(addr string) {
	d.clearBackoff(addr)
}

// InBackoff reports whether addr is currently backed off (tests).
func (d *Discovery) InBackoff(addr string) bool {
	return d.shouldSkip(addr) && !d.isInflight(addr)
}

func (d *Discovery) isInflight(addr string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.inflight[addr]
	return ok
}

// BackoffStreak returns the failure streak for addr (tests).
func (d *Discovery) BackoffStreak(addr string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.backoff[addr].streak
}

func discoveryBackoffDuration(streak int) time.Duration {
	if streak < 1 {
		streak = 1
	}
	// Cap exponential growth after discoveryBackoffParkAfter attempts.
	n := min(streak, discoveryBackoffParkAfter)
	d := discoveryBackoffBase
	for i := 1; i < n && d < discoveryBackoffMax; i++ {
		d *= 2
	}
	return min(d, discoveryBackoffMax)
}
