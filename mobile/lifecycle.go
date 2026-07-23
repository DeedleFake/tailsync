package mobile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"deedles.dev/tailsync/internal/daemon"
)

// stopTimeout is how long Stop waits for the daemon goroutine to exit.
const stopTimeout = 30 * time.Second

// afterStartClaim is an optional test hook invoked after Start claims exclusive
// ownership (phaseStarting + cancel/finished installed) and before daemon
// construction. Production code leaves this nil.
var afterStartClaim func()

// errStartAborted is used when Stop (or ownership loss) aborts Start before ready.
var errStartAborted = errors.New("start aborted")

// nodePhase is the lifecycle state of a Node.
type nodePhase int

const (
	phaseIdle nodePhase = iota
	phaseStarting
	phaseRunning
	phaseStopping
)

func (p nodePhase) String() string {
	switch p {
	case phaseIdle:
		return "idle"
	case phaseStarting:
		return "starting"
	case phaseRunning:
		return "running"
	case phaseStopping:
		return "stopping"
	default:
		return fmt.Sprintf("phase(%d)", int(p))
	}
}

// Lifecycle invariants (generation + phase machine):
//
//   - gen increments on every successful claimStart. finished, workerDone, and
//     cancel belong to that generation; finish() only clears ownership when
//     finished still matches (stale finish from a previous run is a no-op).
//   - When phase is starting, running, or stopping, cancel and finished are
//     non-nil for the active generation. finished is closed exactly once when
//     ownership ends; workerDone is closed once when the Run worker (or the
//     Start abort path that never launched Run) has fully finished.
//   - There is never a phaseIdle window with a live daemon that Stop cannot
//     cancel: claimStart installs cancel/finished/workerDone under the same
//     lock as phase→starting.
//   - workerDone is per-generation (not a shared WaitGroup). Waiters hold the
//     channel value captured under mu; the next Start does not proceed until
//     the prior generation’s workerDone is closed (no Add/Wait races).

// Start starts the daemon and blocks until the node is listening or startup
// fails. Concurrent or double Start returns an error. After a successful Stop
// (or daemon exit), Start may be called again.
//
// For NetMode "tsnet", this may take a while (tailnet bring-up / auth). Call
// off the main thread on Android.
func (n *Node) Start() (err error) {
	if n == nil {
		return errors.New("nil node")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("start panic: %v", r)
		}
	}()

	ctx, finished, workerDone, gen, cfg, err := n.claimStart()
	if err != nil {
		return err
	}

	// closeWorker signals that this generation’s Run worker (or abort path)
	// is fully done. Safe to call from abort or from the Run goroutine.
	closeWorker := sync.OnceFunc(func() {
		close(workerDone)
		n.mu.Lock()
		if n.workerDone == workerDone {
			n.workerDone = nil
		}
		n.mu.Unlock()
	})

	// finish ends this run exactly once: clears ownership under mu and closes
	// finished so Stop/waiters unblock. Only acts if still the active gen.
	// Caller must still closeWorker (Run defer or abort path).
	var finishOnce sync.Once
	finish := func(runErr error, wasReady bool) {
		finishOnce.Do(func() {
			n.mu.Lock()
			var injectGen uint64
			if n.finished == finished && n.gen == gen {
				n.runErr = runErr
				n.ctx = nil
				n.cancel = nil
				n.finished = nil
				n.d = nil
				injectGen = n.injectGen
				n.injectGen = 0
				// workerDone stays until closeWorker so claimStart can drain it.
				n.phase = phaseIdle
				n.clearAuthStateLocked()
			}
			n.mu.Unlock()
			if injectGen != 0 {
				androidNet.clearInject(injectGen)
			}
			close(finished)

			if wasReady {
				if runErr != nil && !errors.Is(runErr, context.Canceled) {
					n.emitEvent(map[string]any{
						"type":  "error",
						"msg":   runErr.Error(),
						"phase": "run",
					})
				}
				n.emitEvent(map[string]any{
					"type":    "status",
					"running": false,
					"msg":     "stopped",
				})
			}
		})
	}

	if afterStartClaim != nil {
		afterStartClaim()
	}

	if !n.ownsStart(finished, gen) {
		finish(errStartAborted, false)
		closeWorker()
		return errStartAborted
	}

	log := slog.New(newEventHandler(n))
	ready := make(chan struct{})
	var readyOnce sync.Once
	var reachedReady atomic.Bool
	onReady := func() {
		readyOnce.Do(func() {
			// Login completed (or was never needed). Drop acceptAuthURL so
			// any in-flight watcher callback cannot re-arm needs_login.
			n.clearAuthState()
			reachedReady.Store(true)
			close(ready)
		})
	}
	// Only tsnet can produce interactive login URLs.
	var onAuthURL func(string)
	if effectiveNetMode(cfg.NetMode) == "tsnet" {
		onAuthURL = func(url string) {
			n.noteAuthURL(url)
		}
	}

	dc, err := toDaemonConfig(&cfg, log, onReady, onAuthURL)
	if err != nil {
		finish(err, false)
		closeWorker()
		return err
	}
	d, err := daemon.New(dc)
	if err != nil {
		finish(err, false)
		closeWorker()
		return err
	}

	if !n.ownsStart(finished, gen) {
		finish(errStartAborted, false)
		closeWorker()
		return errStartAborted
	}

	// Publish daemon for NotifyNetworkChange and register package-level inject
	// before Run. During tsnet.Up the daemon inject is still nil (no-op); after
	// Up the daemon installs NetMon.InjectEvent and fires a catch-up so host
	// snapshot updates made while Up blocked are re-read. injectGen is cleared
	// in finish so a concurrent Stop cannot leave a stale package callback.
	n.mu.Lock()
	if n.finished == finished && n.gen == gen && n.phase == phaseStarting {
		n.d = d
		n.injectGen = androidNet.setInject(d.InjectNetworkChange)
	}
	n.mu.Unlock()

	// Always launch while we own finished so Stop's wait is satisfied.
	go func() {
		var runErr error
		defer func() {
			if r := recover(); r != nil {
				runErr = fmt.Errorf("daemon panic: %v", r)
			}
			finish(runErr, reachedReady.Load())
			closeWorker()
		}()
		if err := ctx.Err(); err != nil {
			runErr = err
			return
		}
		runErr = d.Run(ctx)
	}()

	select {
	case <-ready:
		n.mu.Lock()
		select {
		case <-finished:
			err := n.runErr
			n.mu.Unlock()
			if err == nil {
				err = errors.New("daemon exited immediately after ready")
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, errStartAborted) {
				return errStartAborted
			}
			return err
		default:
		}
		if n.finished != finished || n.gen != gen {
			n.mu.Unlock()
			return errStartAborted
		}
		// Stop already requested: do not report success or emit running:true.
		if n.phase != phaseStarting {
			n.mu.Unlock()
			return errStartAborted
		}
		n.phase = phaseRunning
		// Defense in depth: auth should already be cleared in onReady.
		n.clearAuthStateLocked()
		n.mu.Unlock()
		n.emitEvent(map[string]any{
			"type":    "status",
			"running": true,
			"msg":     "started",
		})
		return nil
	case <-finished:
		n.mu.Lock()
		err := n.runErr
		n.mu.Unlock()
		if err == nil {
			err = errors.New("daemon exited before ready")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, errStartAborted) {
			n.emitEvent(map[string]any{
				"type":  "error",
				"msg":   errStartAborted.Error(),
				"phase": "start",
			})
			return errStartAborted
		}
		n.emitEvent(map[string]any{
			"type":  "error",
			"msg":   err.Error(),
			"phase": "start",
		})
		return err
	}
}

// claimStart acquires exclusive starting ownership under mu: sets phaseStarting
// and installs ctx/cancel/finished/workerDone in the same critical section so
// Stop can always cancel and drain. Concurrent Starts never both succeed.
// Returns the generation token and per-run channels for this Start.
func (n *Node) claimStart() (context.Context, chan struct{}, chan struct{}, uint64, Config, error) {
	for {
		n.mu.Lock()
		switch n.phase {
		case phaseStarting, phaseRunning:
			n.mu.Unlock()
			return nil, nil, nil, 0, Config{}, errors.New("already running")

		case phaseStopping:
			finished := n.finished
			workerDone := n.workerDone
			n.mu.Unlock()
			if err := n.waitFinished(finished, workerDone); err != nil {
				return nil, nil, nil, 0, Config{}, errors.New("previous run still stopping")
			}

		case phaseIdle:
			// Drain a closed leftover finished without releasing mu so two
			// concurrent Starts cannot both pass this path and double-launch.
			if n.finished != nil {
				select {
				case <-n.finished:
					n.finished = nil
					n.cancel = nil
					n.ctx = nil
				default:
					// Open finished while idle is unexpected; wait outside.
					finished := n.finished
					workerDone := n.workerDone
					n.mu.Unlock()
					if err := n.waitFinished(finished, workerDone); err != nil {
						return nil, nil, nil, 0, Config{}, errors.New("previous run still stopping")
					}
					continue
				}
			}
			// Prior generation may still be closing workerDone after finish
			// set phaseIdle (finish closes finished before workerDone).
			if n.workerDone != nil {
				workerDone := n.workerDone
				n.mu.Unlock()
				if err := n.waitFinished(nil, workerDone); err != nil {
					return nil, nil, nil, 0, Config{}, errors.New("previous run still stopping")
				}
				continue
			}

			ctx, cancel := context.WithCancel(context.Background())
			finished := make(chan struct{})
			workerDone := make(chan struct{})
			n.gen++
			gen := n.gen
			n.phase = phaseStarting
			// Fresh auth window for this Start; drop any residual UI fields.
			n.clearAuthStateLocked()
			n.acceptAuthURL = true
			n.ctx = ctx
			n.cancel = cancel
			n.finished = finished
			n.workerDone = workerDone
			n.runErr = nil
			cfg := n.cfg
			n.mu.Unlock()
			return ctx, finished, workerDone, gen, cfg, nil

		default:
			n.mu.Unlock()
			return nil, nil, nil, 0, Config{}, fmt.Errorf("invalid phase %v", n.phase)
		}
	}
}

// ownsStart reports whether finished/gen is still the active run and Start may
// proceed with setup/launch. False when Stop moved phase to stopping (or the
// run slot was cleared) so Start should finish(aborted) without starting Run.
func (n *Node) ownsStart(finished chan struct{}, gen uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.finished == finished && n.gen == gen && n.phase == phaseStarting
}

// NotifyNetworkChange tells this node's running tsnet netmon that host
// connectivity changed. Prefer updating SetNetworkInterfaces* /
// SetDefaultRoute* first, then call this. No-op if the node is not running,
// not in tsnet mode, or NetMon is not yet available (during tsnet.Up; after
// Up a catch-up InjectEvent applies the latest snapshot). Prefer this method
// over package-level NotifyNetworkChange when more than one Node may run in
// the process (package-level targets only the most recently started node).
//
// Safe concurrent with Stop: copies the daemon pointer under mu, then
// InjectNetworkChange copies its inject func under a separate lock.
func (n *Node) NotifyNetworkChange() {
	if n == nil {
		return
	}
	n.mu.Lock()
	d := n.d
	n.mu.Unlock()
	if d != nil {
		d.InjectNetworkChange()
	}
}

// Stop cancels the daemon and waits for it to exit (with a timeout).
// Stop when not running (idle, or already exited) is a no-op and returns nil.
// A timed-out Stop leaves the node in "stopping" until the goroutine exits;
// IsRunning remains true and a later Start waits for wind-down.
func (n *Node) Stop() (err error) {
	if n == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stop panic: %v", r)
		}
	}()

	n.mu.Lock()
	phase := n.phase
	finished := n.finished
	workerDone := n.workerDone
	n.mu.Unlock()

	if phase == phaseIdle {
		// May still need to drain a lingering workerDone after finish→idle.
		if workerDone != nil {
			return n.waitFinished(nil, workerDone)
		}
		return nil
	}

	// Already stopping: wait for the in-flight stop / exit.
	if phase == phaseStopping {
		return n.waitFinished(finished, workerDone)
	}

	// phaseStarting or phaseRunning: request cancel and wait.
	// cancel/finished are always installed with phaseStarting (claimStart),
	// so Stop can always cancel even mid-setup.
	n.mu.Lock()
	if n.phase == phaseStarting || n.phase == phaseRunning {
		n.phase = phaseStopping
	}
	cancel := n.cancel
	finished = n.finished
	workerDone = n.workerDone
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if finished == nil && workerDone == nil {
		// Should not happen under the ownership invariant.
		n.mu.Lock()
		if n.phase == phaseStopping {
			n.phase = phaseIdle
		}
		n.mu.Unlock()
		return nil
	}
	return n.waitFinished(finished, workerDone)
}

// waitFinished waits for the generation’s finished channel (ownership clear)
// and then workerDone (Run worker fully exited). Both are per-generation;
// waiters use channel values captured under mu, never a shared WaitGroup.
func (n *Node) waitFinished(finished, workerDone chan struct{}) error {
	deadline := time.Now().Add(stopTimeout)

	waitOne := func(ch chan struct{}) error {
		if ch == nil {
			return nil
		}
		// Already closed?
		select {
		case <-ch:
			return nil
		default:
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("stop timed out after %s (daemon may still be running; IsRunning stays true until exit)", stopTimeout)
		}
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case <-ch:
			return nil
		case <-timer.C:
			return fmt.Errorf("stop timed out after %s (daemon may still be running; IsRunning stays true until exit)", stopTimeout)
		}
	}

	if err := waitOne(finished); err != nil {
		n.mu.Lock()
		if n.phase != phaseIdle {
			n.phase = phaseStopping
		}
		n.mu.Unlock()
		return err
	}
	if err := waitOne(workerDone); err != nil {
		n.mu.Lock()
		if n.phase != phaseIdle {
			n.phase = phaseStopping
		}
		n.mu.Unlock()
		return err
	}
	return nil
}
