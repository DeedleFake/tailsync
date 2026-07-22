package mobile_test

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"deedles.dev/tailsync/mobile"
)

func TestVersion(t *testing.T) {
	v := mobile.Version()
	if v == "" {
		t.Fatal("Version() returned empty string")
	}
}

func TestNewNodeValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	_, err := mobile.NewNode(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}

	_, err = mobile.NewNode(&mobile.Config{})
	if err == nil {
		t.Fatal("expected error for empty Dir")
	}

	_, err = mobile.NewNode(&mobile.Config{Dir: "relative/path"})
	if err == nil {
		t.Fatal("expected error for relative Dir")
	}

	_, err = mobile.NewNode(&mobile.Config{
		Dir:      dir,
		StateDir: "relative-state",
	})
	if err == nil {
		t.Fatal("expected error for relative StateDir")
	}

	_, err = mobile.NewNode(&mobile.Config{
		Dir:     dir,
		NetMode: "bogus",
	})
	if err == nil {
		t.Fatal("expected error for invalid NetMode")
	}

	_, err = mobile.NewNode(&mobile.Config{
		Dir:  dir,
		Port: 99999,
	})
	if err == nil {
		t.Fatal("expected error for invalid Port")
	}

	// Default net mode is tsnet; validation should accept empty NetMode.
	n, err := mobile.NewNode(&mobile.Config{Dir: dir})
	if err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if n.IsRunning() {
		t.Fatal("new node should not be running")
	}

	// Explicit plain for tests.
	if _, err := mobile.NewNode(&mobile.Config{
		Dir:     dir,
		NetMode: "plain",
		Port:    0,
	}); err != nil {
		t.Fatalf("plain config: %v", err)
	}
}

func TestDoubleStartAndStopIdempotent(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := mustFreePort(t)

	n, err := mobile.NewNode(&mobile.Config{
		Dir:            dir,
		StateDir:       state,
		Hostname:       "mobile-test",
		Port:           port,
		NetMode:        "plain",
		ScanIntervalMs: 200,
		SyncIntervalMs: 200,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Stop(); err != nil {
		t.Fatalf("Stop when not running: %v", err)
	}

	if err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !n.IsRunning() {
		t.Fatal("expected running after Start")
	}

	if err := n.Start(); err == nil {
		t.Fatal("expected double Start error")
	}

	if err := n.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if n.IsRunning() {
		t.Fatal("expected not running after Stop")
	}

	// Stop again is OK.
	if err := n.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	// Restart after Stop.
	if err := n.Start(); err != nil {
		t.Fatalf("restart Start: %v", err)
	}
	if !n.IsRunning() {
		t.Fatal("expected running after restart")
	}
	if err := n.Stop(); err != nil {
		t.Fatalf("final Stop: %v", err)
	}
}

func TestConcurrentStartOnlyOneSucceeds(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := mustFreePort(t)

	n, err := mobile.NewNode(&mobile.Config{
		Dir:            dir,
		StateDir:       state,
		Hostname:       "mobile-concurrent",
		Port:           port,
		NetMode:        "plain",
		ScanIntervalMs: 500,
		SyncIntervalMs: 500,
	})
	if err != nil {
		t.Fatal(err)
	}

	const workers = 8
	var (
		wg       sync.WaitGroup
		okCount  atomic.Int32
		errCount atomic.Int32
	)
	start := make(chan struct{})
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			if err := n.Start(); err != nil {
				errCount.Add(1)
				return
			}
			okCount.Add(1)
		}()
	}
	close(start)
	wg.Wait()

	if okCount.Load() != 1 {
		t.Fatalf("want exactly 1 successful Start, got %d (errors=%d)", okCount.Load(), errCount.Load())
	}
	if errCount.Load() != workers-1 {
		t.Fatalf("want %d Start errors, got %d", workers-1, errCount.Load())
	}
	if !n.IsRunning() {
		t.Fatal("node should be running after concurrent Start")
	}
	if err := n.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestStatusJSON(t *testing.T) {
	dir := t.TempDir()
	n, err := mobile.NewNode(&mobile.Config{
		Dir:     dir,
		NetMode: "plain",
		// Port/intervals/block zero → effective defaults in StatusJSON.
		AuthKey: "tskey-should-not-appear",
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := n.StatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	if m["type"] != "status" {
		t.Fatalf("type: %v", m["type"])
	}
	if m["running"] != false {
		t.Fatalf("running: %v", m["running"])
	}
	if m["phase"] != "idle" {
		t.Fatalf("phase: %v", m["phase"])
	}
	if m["net_mode"] != "plain" {
		t.Fatalf("net_mode: %v", m["net_mode"])
	}
	// Effective defaults (zeros not echoed).
	if int(m["port"].(float64)) != 5960 {
		t.Fatalf("port: %v want 5960", m["port"])
	}
	if int(m["scan_interval_ms"].(float64)) != 30_000 {
		t.Fatalf("scan_interval_ms: %v", m["scan_interval_ms"])
	}
	if int(m["sync_interval_ms"].(float64)) != 45_000 {
		t.Fatalf("sync_interval_ms: %v", m["sync_interval_ms"])
	}
	if int(m["block_size"].(float64)) != 4096 {
		t.Fatalf("block_size: %v", m["block_size"])
	}
	if strings.Contains(s, "tskey-should-not-appear") {
		t.Fatal("auth key leaked in StatusJSON")
	}
}

func TestDefaultNetModeTSNet(t *testing.T) {
	dir := t.TempDir()
	n, err := mobile.NewNode(&mobile.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	s, err := n.StatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	if m["net_mode"] != "tsnet" {
		t.Fatalf("default net_mode want tsnet, got %v", m["net_mode"])
	}
}

type recordingListener struct {
	mu     sync.Mutex
	events []string
}

func (r *recordingListener) OnEvent(eventJSON string) {
	r.mu.Lock()
	r.events = append(r.events, eventJSON)
	r.mu.Unlock()
}

func (r *recordingListener) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	copy(out, r.events)
	return out
}

func TestFailedStartSingleErrorEvent(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	lis := &recordingListener{}
	n, err := mobile.NewNode(&mobile.Config{
		Dir:      dir,
		StateDir: state,
		Hostname: "fail-listen",
		Port:     port,
		NetMode:  "plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	n.SetListener(lis)

	err = n.Start()
	if err == nil {
		_ = n.Stop()
		t.Fatal("expected Start to fail when port is in use")
	}
	if n.IsRunning() {
		t.Fatal("should not be running after failed Start")
	}

	// Wait briefly for any late async events.
	time.Sleep(50 * time.Millisecond)
	evs := lis.snapshot()
	var startErrs, runErrs, stopped int
	for _, e := range evs {
		var m map[string]any
		if err := json.Unmarshal([]byte(e), &m); err != nil {
			t.Fatalf("event JSON: %v: %s", err, e)
		}
		switch m["type"] {
		case "error":
			switch m["phase"] {
			case "start":
				startErrs++
			case "run":
				runErrs++
			}
		case "status":
			if m["msg"] == "stopped" {
				stopped++
			}
		}
	}
	if startErrs != 1 {
		t.Fatalf("want 1 phase=start error, got %d events=%v", startErrs, evs)
	}
	if runErrs != 0 {
		t.Fatalf("want 0 phase=run errors on failed Start, got %d events=%v", runErrs, evs)
	}
	if stopped != 0 {
		t.Fatalf("want 0 stopped status on failed Start, got %d events=%v", stopped, evs)
	}

	// Stop after failed Start is a pure no-op.
	if err := n.Stop(); err != nil {
		t.Fatalf("Stop after failed Start: %v", err)
	}
}

func TestStartStopEventsAndPlainSync(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	stateA := t.TempDir()
	stateB := t.TempDir()

	if err := os.WriteFile(filepath.Join(dirA, "hello.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	portA := mustFreePort(t)
	portB := mustFreePort(t)

	lis := &recordingListener{}

	nodeA, err := mobile.NewNode(&mobile.Config{
		Dir:            dirA,
		StateDir:       stateA,
		Hostname:       "mobile-a",
		Port:           portA,
		NetMode:        "plain",
		Peers:          "127.0.0.1:" + strconv.Itoa(portB),
		ScanIntervalMs: 150,
		SyncIntervalMs: 150,
	})
	if err != nil {
		t.Fatal(err)
	}
	nodeA.SetListener(lis)

	nodeB, err := mobile.NewNode(&mobile.Config{
		Dir:            dirB,
		StateDir:       stateB,
		Hostname:       "mobile-b",
		Port:           portB,
		NetMode:        "plain",
		Peers:          "127.0.0.1:" + strconv.Itoa(portA),
		ScanIntervalMs: 150,
		SyncIntervalMs: 150,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := nodeA.Start(); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer func() { _ = nodeA.Stop() }()
	if err := nodeB.Start(); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer func() { _ = nodeB.Stop() }()

	if !nodeA.IsRunning() || !nodeB.IsRunning() {
		t.Fatal("both nodes should be running")
	}

	// Wait for file to sync A → B.
	wantPath := filepath.Join(dirB, "hello.txt")
	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for sync to %s", wantPath)
		}
		data, err := os.ReadFile(wantPath)
		if err == nil && string(data) == "hello" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := nodeA.Stop(); err != nil {
		t.Fatalf("stop A: %v", err)
	}
	if nodeA.IsRunning() {
		t.Fatal("A should not be running")
	}
	// Extra Stop after graceful stop is no-op.
	if err := nodeA.Stop(); err != nil {
		t.Fatalf("Stop after Stop: %v", err)
	}

	// Listener should have seen status and/or log events.
	evs := lis.snapshot()
	if len(evs) == 0 {
		t.Fatal("expected events on listener")
	}
	sawOK := false
	for _, e := range evs {
		var m map[string]any
		if err := json.Unmarshal([]byte(e), &m); err != nil {
			t.Fatalf("event JSON: %v: %s", err, e)
		}
		if m["type"] == "status" || m["type"] == "log" {
			sawOK = true
		}
		if strings.Contains(e, "tskey-should-not-appear") {
			t.Fatalf("possible secret in event: %s", e)
		}
	}
	if !sawOK {
		t.Fatalf("expected status/log events, got %v", evs)
	}
}

func TestStartFailsWhenPortInUse(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	n, err := mobile.NewNode(&mobile.Config{
		Dir:      dir,
		StateDir: state,
		Hostname: "fail-listen",
		Port:     port,
		NetMode:  "plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = n.Start()
	if err == nil {
		_ = n.Stop()
		t.Fatal("expected Start to fail when port is in use")
	}
	if n.IsRunning() {
		t.Fatal("should not be running after failed Start")
	}
}

// mustFreePort reserves an ephemeral port, closes the listener, and returns
// the port number. A small race remains until Start binds it.
func mustFreePort(t *testing.T) int {
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
