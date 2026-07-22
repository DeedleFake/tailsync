package mobile

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentStartWithLeftoverFinished simulates spontaneous exit leaving a
// closed finished channel while phase is idle, then hammers concurrent Start.
// Only one Start must succeed (drains leftover under mu without double-claim).
func TestConcurrentStartWithLeftoverFinished(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := mustFreePortLifecycle(t)

	n, err := NewNode(&Config{
		Dir:            dir,
		StateDir:       state,
		Hostname:       "leftover-finished",
		Port:           port,
		NetMode:        "plain",
		ScanIntervalMs: 500,
		SyncIntervalMs: 500,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate post-run leftover: idle + closed finished (old bug shape).
	// finish() now clears finished; this defends the drain path.
	stale := make(chan struct{})
	close(stale)
	n.mu.Lock()
	n.phase = phaseIdle
	n.finished = stale
	n.cancel = nil
	n.ctx = nil
	n.mu.Unlock()

	const workers = 8
	var (
		wg      sync.WaitGroup
		okCount atomic.Int32
		errCnt  atomic.Int32
	)
	startGate := make(chan struct{})
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-startGate
			if err := n.Start(); err != nil {
				errCnt.Add(1)
				return
			}
			okCount.Add(1)
		}()
	}
	close(startGate)
	wg.Wait()

	if okCount.Load() != 1 {
		_ = n.Stop()
		t.Fatalf("want exactly 1 successful Start, got %d (errors=%d)", okCount.Load(), errCnt.Load())
	}
	if !n.IsRunning() {
		t.Fatal("node should be running")
	}
	if err := n.Stop(); err != nil {
		t.Fatal(err)
	}
	if n.IsRunning() {
		t.Fatal("should be idle after Stop")
	}
}

// TestStopDuringStartBeforeRun covers Stop while phaseStarting after claim but
// before daemon.Run (setup window). Stop must cancel; Start must return
// start aborted; no orphan daemon; second Start must work.
func TestStopDuringStartBeforeRun(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := mustFreePortLifecycle(t)

	n, err := NewNode(&Config{
		Dir:            dir,
		StateDir:       state,
		Hostname:       "stop-mid-start",
		Port:           port,
		NetMode:        "plain",
		ScanIntervalMs: 500,
		SyncIntervalMs: 500,
	})
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var claimOnce sync.Once
	afterStartClaim = func() {
		// Only intercept the first Start; clear hook so restart is unimpeded.
		claimOnce.Do(func() {
			close(entered)
			<-release
			afterStartClaim = nil
		})
	}
	defer func() { afterStartClaim = nil }()

	stopErrCh := make(chan error, 1)
	go func() {
		<-entered
		// Stop while Start is blocked mid-claim (cancel/finished installed).
		go func() { stopErrCh <- n.Stop() }()
		// Let Stop transition to phaseStopping + cancel before Start continues.
		time.Sleep(20 * time.Millisecond)
		close(release)
	}()

	startErr := n.Start()
	stopErr := <-stopErrCh

	if stopErr != nil {
		t.Fatalf("Stop during Start: %v", stopErr)
	}
	if !errors.Is(startErr, errStartAborted) {
		t.Fatalf("Start want errStartAborted, got %v", startErr)
	}
	if n.IsRunning() {
		t.Fatal("should be idle after Stop during Start")
	}

	// Node must be usable again.
	if err := n.Start(); err != nil {
		t.Fatalf("restart after mid-start Stop: %v", err)
	}
	if !n.IsRunning() {
		t.Fatal("expected running after restart")
	}
	if err := n.Stop(); err != nil {
		t.Fatal(err)
	}
}

// TestStartStopStartRace hammers Start∥Stop under the race detector.
func TestStartStopStartRace(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := mustFreePortLifecycle(t)

	n, err := NewNode(&Config{
		Dir:            dir,
		StateDir:       state,
		Hostname:       "race-lifecycle",
		Port:           port,
		NetMode:        "plain",
		ScanIntervalMs: 200,
		SyncIntervalMs: 200,
	})
	if err != nil {
		t.Fatal(err)
	}

	const rounds = 20
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_ = n.Start()
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_ = n.Stop()
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	_ = n.Stop()
	if n.IsRunning() {
		// Allow brief wind-down after last Stop.
		deadline := time.Now().Add(2 * time.Second)
		for n.IsRunning() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if n.IsRunning() {
			t.Fatal("still running after final Stop")
		}
	}
}

func mustFreePortLifecycle(t *testing.T) int {
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
