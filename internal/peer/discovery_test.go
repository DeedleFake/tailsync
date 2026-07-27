package peer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiscoveryBackoffDuration(t *testing.T) {
	if d := discoveryBackoffDuration(1); d != discoveryBackoffBase {
		t.Fatalf("streak1: %v", d)
	}
	if d := discoveryBackoffDuration(2); d != 2*discoveryBackoffBase {
		t.Fatalf("streak2: %v", d)
	}
	d5 := discoveryBackoffDuration(5)
	d10 := discoveryBackoffDuration(10)
	if d5 != d10 {
		t.Fatalf("park after %d: %v vs %v", discoveryBackoffParkAfter, d5, d10)
	}
	if d5 > discoveryBackoffMax {
		t.Fatalf("exceeded max: %v", d5)
	}
}

func TestDiscoverySemaphoreReleasedBeforeBackoff(t *testing.T) {
	// Ensure concurrent dials for different addrs proceed while one is in backoff.
	roster := NewRoster()
	var dials atomic.Int32
	block := make(chan struct{})
	firstStarted := make(chan struct{})

	d := newDiscovery(nil, roster, DiscoveryConfig{
		Concurrency: 1, // single in-flight slot
		Interval:    time.Hour,
		DialTimeout: time.Second,
		Candidates: func(ctx context.Context) []Candidate {
			return []Candidate{{Addr: "a:1"}, {Addr: "b:2"}}
		},
		DialPeer: func(ctx context.Context, c Candidate) error {
			dials.Add(1)
			if c.Addr == "a:1" {
				close(firstStarted)
				<-block
				return context.DeadlineExceeded
			}
			return context.DeadlineExceeded
		},
	})

	ctx := t.Context()

	done := make(chan struct{})
	go func() {
		d.tick(ctx)
		close(done)
	}()

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first dial did not start")
	}
	// With concurrency 1, second waits on semaphore until first releases.
	// Release first dial; second should run (semaphore released before backoff sleep).
	close(block)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("tick did not finish")
	}
	if n := dials.Load(); n != 2 {
		t.Fatalf("expected both dials, got %d", n)
	}
	if !d.InBackoff("a:1") || !d.InBackoff("b:2") {
		t.Fatal("expected backoff on both")
	}
}

func TestDiscoverySkipsConnected(t *testing.T) {
	roster := NewRoster()
	s := &Session{nodeID: "p", localID: "me", addr: "127.0.0.1:9"}
	s.healthy.Store(true)
	roster.Install(s)

	var dials atomic.Int32
	d := newDiscovery(nil, roster, DiscoveryConfig{
		Concurrency: 4,
		Candidates: func(ctx context.Context) []Candidate {
			return []Candidate{{Addr: "127.0.0.1:9"}, {Addr: "127.0.0.1:10"}}
		},
		DialPeer: func(ctx context.Context, c Candidate) error {
			dials.Add(1)
			return nil
		},
	})
	d.tick(context.Background())
	if dials.Load() != 1 {
		t.Fatalf("should dial only unconnected, got %d", dials.Load())
	}
}

func TestDiscoveryNeverPermanentBan(t *testing.T) {
	d := newDiscovery(nil, NewRoster(), DiscoveryConfig{})
	for range 20 {
		d.softFail("x:1")
	}
	// Parked but still has finite until; force expiry.
	d.mu.Lock()
	ab := d.backoff["x:1"]
	ab.until = time.Now().Add(-time.Second)
	d.backoff["x:1"] = ab
	d.mu.Unlock()
	if d.shouldSkip("x:1") {
		t.Fatal("expired backoff must allow re-dial")
	}
}

func TestDiscoveryKickNonBlocking(t *testing.T) {
	d := newDiscovery(nil, NewRoster(), DiscoveryConfig{
		Candidates: func(ctx context.Context) []Candidate { return nil },
		DialPeer:   func(ctx context.Context, c Candidate) error { return nil },
	})
	// Kick must not block even when Run is not consuming.
	done := make(chan struct{})
	go func() {
		d.Kick()
		d.Kick()
		d.Kick()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Kick blocked")
	}
}

func TestDiscoveryTickSingleFlight(t *testing.T) {
	var concurrent atomic.Int32
	var max atomic.Int32
	block := make(chan struct{})
	started := make(chan struct{})

	d := newDiscovery(nil, NewRoster(), DiscoveryConfig{
		Concurrency: 4,
		Candidates: func(ctx context.Context) []Candidate {
			return []Candidate{{Addr: "a:1"}}
		},
		DialPeer: func(ctx context.Context, c Candidate) error {
			n := concurrent.Add(1)
			for {
				old := max.Load()
				if n <= old || max.CompareAndSwap(old, n) {
					break
				}
			}
			select {
			case <-started:
			default:
				close(started)
			}
			<-block
			concurrent.Add(-1)
			return context.DeadlineExceeded
		},
	})

	ctx := t.Context()
	go d.tick(ctx)
	<-started
	// Concurrent tick should not start another dial while first is running.
	go d.tick(ctx)
	time.Sleep(50 * time.Millisecond)
	close(block)
	// Drain pending kick from single-flight defer.
	time.Sleep(50 * time.Millisecond)
	if max.Load() > 1 {
		t.Fatalf("concurrent dials from overlapping ticks: max=%d", max.Load())
	}
}
