package daemon_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"deedles.dev/tailsync/daemon"
)

// meshNode is one plain-mode daemon in a mesh fixture.
type meshNode struct {
	Dir, State string
	Port       int
	D          *daemon.Daemon
	Err        chan error
}

// startMesh launches n fully-meshed plain QUIC daemons on 127.0.0.1 and waits
// until all OnReady callbacks fire. configure may adjust intervals/hooks per
// node (i = 0..n-1); Port/Peers/ListenHost/NetMode/OnReady are set after
// configure so wiring stays consistent. Uses freePorts and waitFile helpers
// from daemon_test.go.
func startMesh(t *testing.T, ctx context.Context, n int, configure func(i int, cfg *daemon.Config)) []meshNode {
	t.Helper()
	if n < 1 {
		t.Fatalf("startMesh: n=%d", n)
	}
	ports := freePorts(t, n)
	nodes := make([]meshNode, n)
	for i := range n {
		nodes[i] = meshNode{
			Dir:   t.TempDir(),
			State: t.TempDir(),
			Port:  ports[i],
			Err:   make(chan error, 1),
		}
	}

	ready := make(chan struct{}, n)
	for i := range n {
		var peers []string
		for j := range n {
			if j == i {
				continue
			}
			peers = append(peers, "127.0.0.1:"+strconv.Itoa(nodes[j].Port))
		}
		cfg := daemon.Config{
			Dir:           nodes[i].Dir,
			StateDir:      nodes[i].State,
			Hostname:      "mesh-" + strconv.Itoa(i),
			Port:          nodes[i].Port,
			NetMode:       daemon.NetModePlain,
			ListenHost:    "127.0.0.1",
			Peers:         peers,
			ScanInterval:  time.Hour,
			SyncInterval:  time.Hour,
			WatchDebounce: 40 * time.Millisecond,
		}
		if configure != nil {
			configure(i, &cfg)
		}
		// Re-assert mesh wiring so configure cannot break peer topology.
		cfg.Dir = nodes[i].Dir
		cfg.StateDir = nodes[i].State
		cfg.Port = nodes[i].Port
		cfg.NetMode = daemon.NetModePlain
		cfg.ListenHost = "127.0.0.1"
		cfg.Peers = peers
		prevReady := cfg.OnReady
		cfg.OnReady = func() {
			if prevReady != nil {
				prevReady()
			}
			ready <- struct{}{}
		}
		d, err := daemon.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i].D = d
	}

	for i := range nodes {
		go func(i int) { nodes[i].Err <- nodes[i].D.Run(ctx) }(i)
	}
	waitMeshReady(t, n, ready, nodes)
	return nodes
}

// pairDaemons is startMesh for two mutual peers.
func pairDaemons(t *testing.T, ctx context.Context, configure func(i int, cfg *daemon.Config)) (a, b meshNode) {
	t.Helper()
	nodes := startMesh(t, ctx, 2, configure)
	return nodes[0], nodes[1]
}

func waitMeshReady(t *testing.T, n int, ready <-chan struct{}, nodes []meshNode) {
	t.Helper()
	// Fan-in daemon exits so a failed Run fails immediately (same as pre-refactor
	// select on Err). Restore each buffered Err after reading so stopMesh can join.
	type exit struct {
		i   int
		err error
	}
	exited := make(chan exit, 1)
	done := make(chan struct{})
	defer close(done)
	for i, node := range nodes {
		go func(i int, errCh chan error) {
			select {
			case err := <-errCh:
				errCh <- err // restore for stopMesh
				select {
				case exited <- exit{i: i, err: err}:
				case <-done:
				}
			case <-done:
			}
		}(i, node.Err)
	}
	for range n {
		select {
		case <-ready:
		case e := <-exited:
			t.Fatalf("daemon %d exited during ready: %v", e.i, e.err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for ready")
		}
	}
}

func meshErrs(nodes ...meshNode) []<-chan error {
	out := make([]<-chan error, len(nodes))
	for i, n := range nodes {
		out[i] = n.Err
	}
	return out
}

func stopMesh(cancel context.CancelFunc, nodes ...meshNode) {
	cancel()
	for _, n := range nodes {
		<-n.Err
	}
}

// TestNotifyPullDeliversFile: A writes → notify → B pulls (long SyncInterval).
func TestNotifyPullDeliversFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, b := pairDaemons(t, ctx, func(i int, cfg *daemon.Config) {
		cfg.Hostname = []string{"notify-a", "notify-b"}[i]
	})
	defer stopMesh(cancel, a, b)

	if err := os.WriteFile(filepath.Join(a.Dir, "n.txt"), []byte("from-a"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(b.Dir, "n.txt"), "from-a", 8*time.Second, meshErrs(a, b)...)
}

// TestWriterNotBlockedByDeadPeers: reconcile finishes quickly even when a
// configured peer is unreachable (discovery dials are background; notify needs
// connected sessions and is a no-op without them).
func TestWriterNotBlockedByDeadPeers(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	ports := freePorts(t, 2)
	port, dead := ports[0], ports[1]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	var readyOnce sync.Once
	reconciled := make(chan struct{}, 8)

	d, err := daemon.New(daemon.Config{
		Dir:           dir,
		StateDir:      state,
		Hostname:      "writer",
		Port:          port,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         []string{"127.0.0.1:" + strconv.Itoa(dead)},
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		DialTimeout:   200 * time.Millisecond,
		WatchDebounce: 30 * time.Millisecond,
		OnReady: func() {
			readyOnce.Do(func() { close(ready) })
		},
		AfterReconcile: func(changed bool) {
			if changed {
				select {
				case reconciled <- struct{}{}:
				default:
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()

	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("Run exited: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout ready")
	}

	start := time.Now()
	if err := os.WriteFile(filepath.Join(dir, "w.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Reconcile must complete without waiting for dead peer discovery dials.
	select {
	case <-reconciled:
	case err := <-errCh:
		t.Fatalf("Run exited: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for AfterReconcile (writer blocked?)")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("reconcile path took %v; writer should not wait on dead peers", elapsed)
	}

	cancel()
	<-errCh
}

// TestThreeNodeNotifyDedupe: A writes; B and C both end with the same content
// without requiring reverse-pull. Long intervals so delivery is notify+pull.
func TestThreeNodeNotifyDedupe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := startMesh(t, ctx, 3, func(i int, cfg *daemon.Config) {
		cfg.Hostname = []string{"mesh-a", "mesh-b", "mesh-c"}[i]
	})
	defer stopMesh(cancel, nodes...)

	// Let bootstrap pulls settle so hot sets form.
	time.Sleep(200 * time.Millisecond)

	if err := os.WriteFile(filepath.Join(nodes[0].Dir, "mesh.txt"), []byte("mesh-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	errs := meshErrs(nodes...)
	waitFile(t, filepath.Join(nodes[1].Dir, "mesh.txt"), "mesh-content", 10*time.Second, errs...)
	waitFile(t, filepath.Join(nodes[2].Dir, "mesh.txt"), "mesh-content", 10*time.Second, errs...)
}

// TestStaleNotifyThenNewerFile: B eventually has the latest content after A
// updates again (pull uses current manifest, not notify hints).
func TestStaleNotifyThenNewerFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, b := pairDaemons(t, ctx, func(i int, cfg *daemon.Config) {
		cfg.Hostname = []string{"stale-a", "stale-b"}[i]
		cfg.WatchDebounce = 30 * time.Millisecond
	})
	defer stopMesh(cancel, a, b)

	pathA := filepath.Join(a.Dir, "v.txt")
	if err := os.WriteFile(pathA, []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(b.Dir, "v.txt"), "v1", 8*time.Second, meshErrs(a, b)...)

	if err := os.WriteFile(pathA, []byte("v2-final"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFile(t, filepath.Join(b.Dir, "v.txt"), "v2-final", 8*time.Second, meshErrs(a, b)...)
}

// TestLateJoinerPullCatchUp: B starts after A already has content; interval or
// bootstrap pull converges without permanent ban after soft-fails.
func TestLateJoinerPullCatchUp(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()
	ports := freePorts(t, 2)
	portA, portB := ports[0], ports[1]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := os.WriteFile(filepath.Join(dirA, "late.txt"), []byte("late-join"), 0o644); err != nil {
		t.Fatal(err)
	}

	readyA := make(chan struct{})
	var onceA sync.Once
	da, err := daemon.New(daemon.Config{
		Dir:          dirA,
		StateDir:     stateA,
		Hostname:     "late-a",
		Port:         portA,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portB)},
		ScanInterval: 200 * time.Millisecond,
		SyncInterval: 200 * time.Millisecond,
		DialTimeout:  300 * time.Millisecond,
		DisableWatch: true,
		OnReady:      func() { onceA.Do(func() { close(readyA) }) },
	})
	if err != nil {
		t.Fatal(err)
	}
	errA := make(chan error, 1)
	go func() { errA <- da.Run(ctx) }()

	select {
	case <-readyA:
	case err := <-errA:
		t.Fatalf("A: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("A ready timeout")
	}

	// Soft-fails while B is down must not permanently ban; B joins later.
	time.Sleep(500 * time.Millisecond)

	db, err := daemon.New(daemon.Config{
		Dir:          dirB,
		StateDir:     stateB,
		Hostname:     "late-b",
		Port:         portB,
		NetMode:      daemon.NetModePlain,
		ListenHost:   "127.0.0.1",
		Peers:        []string{"127.0.0.1:" + strconv.Itoa(portA)},
		ScanInterval: 200 * time.Millisecond,
		SyncInterval: 200 * time.Millisecond,
		DisableWatch: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	errB := make(chan error, 1)
	go func() { errB <- db.Run(ctx) }()

	waitFile(t, filepath.Join(dirB, "late.txt"), "late-join", 10*time.Second, errA, errB)

	cancel()
	<-errA
	<-errB
}

// TestNotifyDoesNotStorm: many rapid local writes still complete; no hang from
// notify/pull loops (content-id dedupe + single-flight pull).
func TestNotifyDoesNotStorm(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, b := pairDaemons(t, ctx, func(i int, cfg *daemon.Config) {
		cfg.Hostname = []string{"storm-a", "storm-b"}[i]
	})
	defer stopMesh(cancel, a, b)

	for i := range 15 {
		name := filepath.Join(a.Dir, "s"+strconv.Itoa(i)+".txt")
		if err := os.WriteFile(name, []byte("x"+strconv.Itoa(i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	waitFile(t, filepath.Join(b.Dir, "s14.txt"), "x14", 15*time.Second, meshErrs(a, b)...)

	// Daemons must still be healthy (not stuck in notify storm).
	select {
	case err := <-a.Err:
		t.Fatalf("A exited early: %v", err)
	case err := <-b.Err:
		t.Fatalf("B exited early: %v", err)
	default:
	}
}

// TestMetaOnlyNotifyPull: touch (mtime-only) on A reaches B via notify→pull
// with huge SyncIntervals (would fail if alreadyHaveAll ignored meta LWW).
func TestMetaOnlyNotifyPull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	content := []byte("touch-meta")
	a, b := pairDaemons(t, ctx, func(i int, cfg *daemon.Config) {
		cfg.Hostname = []string{"meta-a", "meta-b"}[i]
	})
	defer stopMesh(cancel, a, b)

	pathA := filepath.Join(a.Dir, "meta.txt")
	if err := os.WriteFile(pathA, content, 0o644); err != nil {
		t.Fatal(err)
	}
	pathB := filepath.Join(b.Dir, "meta.txt")
	waitFile(t, pathB, string(content), 8*time.Second, meshErrs(a, b)...)

	fiB, err := os.Stat(pathB)
	if err != nil {
		t.Fatal(err)
	}
	mtimeBefore := fiB.ModTime()

	newMT := time.Now().Add(3 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(pathA, newMT, newMT); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(8 * time.Second)
	for {
		if time.Now().After(deadline) {
			fiB, _ = os.Stat(pathB)
			t.Fatalf("timeout waiting for mtime via notify+pull: B=%v want≈%v", fiB.ModTime(), newMT)
		}
		for _, ch := range meshErrs(a, b) {
			select {
			case err := <-ch:
				if err != nil {
					t.Fatalf("daemon exited: %v", err)
				}
			default:
			}
		}
		fiB, err = os.Stat(pathB)
		if err != nil {
			time.Sleep(40 * time.Millisecond)
			continue
		}
		mtimeOK := fiB.ModTime().Equal(newMT) ||
			(fiB.ModTime().Truncate(time.Second).Equal(newMT.Truncate(time.Second)) &&
				!fiB.ModTime().Equal(mtimeBefore))
		if !mtimeOK {
			time.Sleep(40 * time.Millisecond)
			continue
		}
		data, err := os.ReadFile(pathB)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != string(content) {
			t.Fatalf("content changed: %q", data)
		}
		break
	}
}

// TestNotifyDuringSlowPull: local write finishes reconcile quickly while
// discovery dials dead peers in the background — main loop must not wait.
func TestNotifyDuringSlowPull(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	ports := freePorts(t, 7)
	port := ports[0]
	var dead []string
	for _, p := range ports[1:] {
		dead = append(dead, "127.0.0.1:"+strconv.Itoa(p))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	var readyOnce sync.Once
	reconciled := make(chan struct{}, 4)

	d, err := daemon.New(daemon.Config{
		Dir:           dir,
		StateDir:      state,
		Hostname:      "slow-pull",
		Port:          port,
		NetMode:       daemon.NetModePlain,
		ListenHost:    "127.0.0.1",
		Peers:         dead,
		ScanInterval:  time.Hour,
		SyncInterval:  time.Hour,
		DialTimeout:   400 * time.Millisecond,
		WatchDebounce: 30 * time.Millisecond,
		OnReady: func() {
			readyOnce.Do(func() { close(ready) })
		},
		AfterReconcile: func(changed bool) {
			if changed {
				select {
				case reconciled <- struct{}{}:
				default:
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()

	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("Run: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ready")
	}

	start := time.Now()
	if err := os.WriteFile(filepath.Join(dir, "fast.txt"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reconciled:
	case err := <-errCh:
		t.Fatalf("Run: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("AfterReconcile delayed behind discovery/pull")
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("reconcile path took %v; discovery should not block main loop", elapsed)
	}

	cancel()
	<-errCh
}
