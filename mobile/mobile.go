// Package mobile provides a gomobile-bindable API for running tailsync from
// Android (and other gomobile targets).
//
// Build an Android AAR for a separate Kotlin app:
//
//	gomobile bind -target=android -o tailsync.aar deedles.dev/tailsync/mobile
//
// Mobile apps should use NetMode "tsnet" (the default): Android does not use
// the host LocalAPI the same way as desktop. "host" is desktop-oriented;
// "plain" is for local tests only.
//
// Lifecycle is Start/Stop; the app (typically a foreground service) owns when
// the node runs. Paths must be absolute and writable by the process.
//
// Start may block for a long time (tsnet bring-up). Call it off the Android
// main thread. EventListener.OnEvent is invoked from daemon/start goroutines
// and must return quickly (non-blocking); hop to the main thread only for UI.
//
// # Event JSON format
//
// EventListener.OnEvent receives one JSON object per call:
//
//	{"type":"log","level":"INFO","msg":"...","time":"...","attrs":{...}}
//	{"type":"status","running":true,"msg":"started"}
//	{"type":"error","msg":"...","phase":"start"|"run"}
//
// Secrets such as AuthKey are never included. Log attribute keys named like
// authkey/token are redacted; free-text log messages are not scrubbed.
package mobile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"deedles.dev/tailsync/internal/daemon"
	"deedles.dev/tailsync/internal/delta"
)

// stopTimeout is how long Stop waits for the daemon goroutine to exit.
const stopTimeout = 30 * time.Second

// Daemon defaults mirrored for StatusJSON when config fields are zero.
// Keep in sync with internal/daemon.New defaults.
const (
	defaultPort           = 5960
	defaultScanIntervalMs = 30_000
	defaultSyncIntervalMs = 45_000
)

// afterStartClaim is an optional test hook invoked after Start claims exclusive
// ownership (phaseStarting + cancel/finished installed) and before daemon
// construction. Production code leaves this nil.
var afterStartClaim func()

// errStartAborted is used when Stop (or ownership loss) aborts Start before ready.
var errStartAborted = errors.New("start aborted")

// Config holds bindable configuration. All exported fields use types gomobile
// can bind (string, bool, int, int64).
//
// Dir is required and must be an absolute path. StateDir, when set, must also
// be absolute; otherwise it defaults to Dir/.tailsync (resolved by the daemon).
//
// Zero values for Port, ScanIntervalMs, SyncIntervalMs, and BlockSize mean
// “use daemon defaults” (5960, 30s, 45s, delta default block size). StatusJSON
// reports those effective defaults.
type Config struct {
	// Dir is the absolute path to the sync root (required).
	Dir string
	// StateDir is optional; default is under Dir (.tailsync).
	StateDir string
	// Hostname is the tsnet hostname when NetMode is "tsnet".
	Hostname string
	// AuthKey is a Tailscale auth key for tsnet registration.
	AuthKey string
	// Port is the TCP listen/dial port (0 = daemon default 5960).
	Port int
	// Peers is a comma-separated list of host:port peers (optional; empty = discovery).
	Peers string
	// ServiceName filters discovered peers by hostname/DNS substring.
	ServiceName string
	// ScanIntervalMs is the local rescan period in milliseconds (0 = default 30000).
	ScanIntervalMs int64
	// SyncIntervalMs is the peer sync period in milliseconds (0 = default 45000).
	SyncIntervalMs int64
	// BlockSize is the delta block size (0 = default).
	BlockSize int
	// NetMode selects networking: "tsnet" (default for mobile), "host", or "plain".
	// Android apps should use "tsnet". "plain" is for testing only (localhost TCP).
	// "host" requires a system tailscaled (not typical on Android).
	NetMode string
}

// EventListener is implemented in Kotlin (or other gomobile hosts) for status
// and log callbacks. Events are JSON objects (see package docs).
//
// OnEvent must return quickly: it is called synchronously from daemon and
// start/stop paths. Blocking here stalls logging and can delay Start. Do not
// update Android UI directly; post to the main looper/dispatcher first.
//
// AuthKey and other secrets are never included in event payloads.
type EventListener interface {
	// OnEvent receives a single JSON event object as a string.
	OnEvent(eventJSON string)
}

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

// Node is a long-lived sync instance. Construct with NewNode, then Start/Stop
// from the app lifecycle (e.g. a foreground service).
//
// Ownership invariant: when phase is starting, running, or stopping, cancel,
// ctx, and finished are non-nil for that run. finished is closed exactly once
// when the run ends; cancel/finished/ctx are cleared under mu in the same
// critical section as phase→idle. There is never a phaseIdle window with a
// live daemon that Stop cannot cancel.
type Node struct {
	cfg Config

	mu       sync.Mutex
	phase    nodePhase
	listener EventListener

	ctx      context.Context
	cancel   context.CancelFunc
	finished chan struct{} // closed when the active run ends
	runErr   error         // set before finished is closed
}

// Version returns the module version from build info when available.
func Version() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		if bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			return bi.Main.Version
		}
		for _, m := range bi.Deps {
			if m.Path == "deedles.dev/tailsync" && m.Version != "" {
				return m.Version
			}
		}
	}
	return "devel"
}

// NewNode validates cfg and returns a stopped Node.
func NewNode(cfg *Config) (*Node, error) {
	if cfg == nil {
		return nil, errors.New("config is required")
	}
	c := *cfg
	if err := validateConfig(&c); err != nil {
		return nil, err
	}
	// Ensure daemon accepts the mapped config (defaults, abs paths, etc.).
	dc, err := toDaemonConfig(&c, slog.Default(), nil)
	if err != nil {
		return nil, err
	}
	if _, err := daemon.New(dc); err != nil {
		return nil, err
	}
	return &Node{cfg: c}, nil
}

// SetListener sets the callback for log/status events. May be called at any
// time; nil clears the listener. Safe for concurrent use with Start/Stop.
//
// The listener’s OnEvent must be non-blocking (see EventListener docs).
func (n *Node) SetListener(l EventListener) {
	if n == nil {
		return
	}
	n.mu.Lock()
	n.listener = l
	n.mu.Unlock()
}

// IsRunning reports whether the node holds an active lifecycle (starting,
// running, or stopping). False only when fully idle. After a timed-out Stop,
// this remains true until the daemon goroutine exits (resources may still be
// held). StatusJSON.running is true only when serving after a successful Start.
func (n *Node) IsRunning() bool {
	if n == nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.phase != phaseIdle
}

// StatusJSON returns a small JSON snapshot for UI (config + lifecycle).
// AuthKey is never included. Zero config fields are reported as effective
// daemon defaults (see Config). Includes "phase": idle|starting|running|stopping.
func (n *Node) StatusJSON() (string, error) {
	if n == nil {
		return "", errors.New("nil node")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	port, scanMs, syncMs, block := effectiveDisplay(n.cfg)
	st := statusSnapshot{
		Type:      "status",
		Running:   n.phase == phaseRunning,
		Phase:     n.phase.String(),
		Dir:       n.cfg.Dir,
		StateDir:  n.cfg.StateDir,
		Hostname:  n.cfg.Hostname,
		Port:      port,
		NetMode:   effectiveNetMode(n.cfg.NetMode),
		Service:   n.cfg.ServiceName,
		Peers:     n.cfg.Peers,
		ScanMs:    scanMs,
		SyncMs:    syncMs,
		BlockSize: block,
		Version:   Version(),
	}
	b, err := json.Marshal(st)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

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

	ctx, finished, cfg, err := n.claimStart()
	if err != nil {
		return err
	}

	// finish ends this run exactly once: clears ownership under mu and closes
	// finished so Stop/waiters unblock.
	var finishOnce sync.Once
	finish := func(runErr error, wasReady bool) {
		finishOnce.Do(func() {
			n.mu.Lock()
			if n.finished == finished {
				n.runErr = runErr
				n.ctx = nil
				n.cancel = nil
				n.finished = nil
				n.phase = phaseIdle
			}
			n.mu.Unlock()
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

	if !n.ownsStart(finished) {
		finish(errStartAborted, false)
		return errStartAborted
	}

	log := slog.New(newEventHandler(n))
	ready := make(chan struct{})
	var readyOnce sync.Once
	var reachedReady atomic.Bool
	onReady := func() {
		readyOnce.Do(func() {
			reachedReady.Store(true)
			close(ready)
		})
	}

	dc, err := toDaemonConfig(&cfg, log, onReady)
	if err != nil {
		finish(err, false)
		return err
	}
	d, err := daemon.New(dc)
	if err != nil {
		finish(err, false)
		return err
	}

	if !n.ownsStart(finished) {
		finish(errStartAborted, false)
		return errStartAborted
	}

	// Always launch while we own finished so Stop's wait is satisfied.
	go func() {
		var runErr error
		defer func() {
			if r := recover(); r != nil {
				runErr = fmt.Errorf("daemon panic: %v", r)
			}
			finish(runErr, reachedReady.Load())
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
		if n.finished != finished {
			n.mu.Unlock()
			return errStartAborted
		}
		// Promote starting→running only; leave stopping as-is.
		if n.phase == phaseStarting {
			n.phase = phaseRunning
		}
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
// and installs ctx/cancel/finished in the same critical section so Stop can
// always cancel. Concurrent Starts never both succeed.
func (n *Node) claimStart() (context.Context, chan struct{}, Config, error) {
	for {
		n.mu.Lock()
		switch n.phase {
		case phaseStarting, phaseRunning:
			n.mu.Unlock()
			return nil, nil, Config{}, errors.New("already running")

		case phaseStopping:
			finished := n.finished
			n.mu.Unlock()
			if finished == nil {
				time.Sleep(time.Millisecond)
				continue
			}
			timer := time.NewTimer(stopTimeout)
			select {
			case <-finished:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			case <-timer.C:
				return nil, nil, Config{}, errors.New("previous run still stopping")
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
					n.mu.Unlock()
					timer := time.NewTimer(stopTimeout)
					select {
					case <-finished:
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
					case <-timer.C:
						return nil, nil, Config{}, errors.New("previous run still stopping")
					}
					continue
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			finished := make(chan struct{})
			n.phase = phaseStarting
			n.ctx = ctx
			n.cancel = cancel
			n.finished = finished
			n.runErr = nil
			cfg := n.cfg
			n.mu.Unlock()
			return ctx, finished, cfg, nil

		default:
			n.mu.Unlock()
			return nil, nil, Config{}, fmt.Errorf("invalid phase %v", n.phase)
		}
	}
}

// ownsStart reports whether finished is still the active run and Start may
// proceed with setup/launch. False when Stop moved phase to stopping (or the
// run slot was cleared) so Start should finish(aborted) without starting Run.
func (n *Node) ownsStart(finished chan struct{}) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.finished == finished && n.phase == phaseStarting
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
	cancel := n.cancel
	finished := n.finished
	n.mu.Unlock()

	if phase == phaseIdle {
		return nil
	}

	// Already stopping: wait for the in-flight stop / exit.
	if phase == phaseStopping {
		return n.waitFinished(finished)
	}

	// phaseStarting or phaseRunning: request cancel and wait.
	// cancel/finished are always installed with phaseStarting (claimStart),
	// so Stop can always cancel even mid-setup.
	n.mu.Lock()
	if n.phase == phaseStarting || n.phase == phaseRunning {
		n.phase = phaseStopping
	}
	cancel = n.cancel
	finished = n.finished
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if finished == nil {
		// Should not happen under the ownership invariant.
		n.mu.Lock()
		if n.phase == phaseStopping {
			n.phase = phaseIdle
		}
		n.mu.Unlock()
		return nil
	}
	return n.waitFinished(finished)
}

func (n *Node) waitFinished(finished chan struct{}) error {
	if finished == nil {
		return nil
	}
	timer := time.NewTimer(stopTimeout)
	defer timer.Stop()
	select {
	case <-finished:
		// finish() already cleared ownership and set phaseIdle.
		return nil
	case <-timer.C:
		n.mu.Lock()
		if n.phase != phaseIdle {
			n.phase = phaseStopping
		}
		n.mu.Unlock()
		return fmt.Errorf("stop timed out after %s (daemon may still be running; IsRunning stays true until exit)", stopTimeout)
	}
}

func validateConfig(cfg *Config) error {
	if cfg.Dir == "" {
		return errors.New("Dir is required")
	}
	if !filepath.IsAbs(cfg.Dir) {
		return fmt.Errorf("Dir must be an absolute path: %q", cfg.Dir)
	}
	if cfg.StateDir != "" && !filepath.IsAbs(cfg.StateDir) {
		return fmt.Errorf("StateDir must be an absolute path: %q", cfg.StateDir)
	}
	mode := effectiveNetMode(cfg.NetMode)
	switch mode {
	case "tsnet", "host", "plain":
	default:
		return fmt.Errorf("invalid NetMode %q (want tsnet, host, or plain)", cfg.NetMode)
	}
	if cfg.Port < 0 || cfg.Port > 65535 {
		return fmt.Errorf("invalid Port %d", cfg.Port)
	}
	if cfg.ScanIntervalMs < 0 {
		return errors.New("ScanIntervalMs must be >= 0")
	}
	if cfg.SyncIntervalMs < 0 {
		return errors.New("SyncIntervalMs must be >= 0")
	}
	if cfg.BlockSize < 0 {
		return errors.New("BlockSize must be >= 0")
	}
	return nil
}

func effectiveNetMode(mode string) string {
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		return "tsnet"
	}
	return mode
}

func effectiveDisplay(cfg Config) (port int, scanMs, syncMs int64, block int) {
	port = cfg.Port
	if port == 0 {
		port = defaultPort
	}
	scanMs = cfg.ScanIntervalMs
	if scanMs == 0 {
		scanMs = defaultScanIntervalMs
	}
	syncMs = cfg.SyncIntervalMs
	if syncMs == 0 {
		syncMs = defaultSyncIntervalMs
	}
	block = cfg.BlockSize
	if block == 0 {
		block = delta.DefaultBlockSize
	}
	return port, scanMs, syncMs, block
}

func toDaemonConfig(cfg *Config, log *slog.Logger, onReady func()) (daemon.Config, error) {
	mode := effectiveNetMode(cfg.NetMode)
	var netMode daemon.NetMode
	switch mode {
	case "tsnet":
		netMode = daemon.NetModeTSNet
	case "host":
		netMode = daemon.NetModeHost
	case "plain":
		netMode = daemon.NetModePlain
	default:
		return daemon.Config{}, fmt.Errorf("invalid NetMode %q", cfg.NetMode)
	}

	var peers []string
	if cfg.Peers != "" {
		for _, p := range strings.Split(cfg.Peers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peers = append(peers, p)
			}
		}
	}

	var scanEvery, syncEvery time.Duration
	if cfg.ScanIntervalMs > 0 {
		scanEvery = time.Duration(cfg.ScanIntervalMs) * time.Millisecond
	}
	if cfg.SyncIntervalMs > 0 {
		syncEvery = time.Duration(cfg.SyncIntervalMs) * time.Millisecond
	}

	return daemon.Config{
		Dir:          cfg.Dir,
		StateDir:     cfg.StateDir,
		Hostname:     cfg.Hostname,
		ServiceName:  cfg.ServiceName,
		Port:         cfg.Port,
		AuthKey:      cfg.AuthKey,
		ScanInterval: scanEvery,
		SyncInterval: syncEvery,
		BlockSize:    cfg.BlockSize,
		Logger:       log,
		NetMode:      netMode,
		Peers:        peers,
		OnReady:      onReady,
	}, nil
}

type statusSnapshot struct {
	Type      string `json:"type"`
	Running   bool   `json:"running"`
	Phase     string `json:"phase"`
	Dir       string `json:"dir"`
	StateDir  string `json:"state_dir,omitempty"`
	Hostname  string `json:"hostname,omitempty"`
	Port      int    `json:"port"`
	NetMode   string `json:"net_mode"`
	Service   string `json:"service,omitempty"`
	Peers     string `json:"peers,omitempty"`
	ScanMs    int64  `json:"scan_interval_ms"`
	SyncMs    int64  `json:"sync_interval_ms"`
	BlockSize int    `json:"block_size"`
	Version   string `json:"version"`
}

func (n *Node) emitEvent(ev map[string]any) {
	if n == nil {
		return
	}
	n.mu.Lock()
	l := n.listener
	n.mu.Unlock()
	if l == nil {
		return
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	defer func() { _ = recover() }() // do not panic into Java/Kotlin
	l.OnEvent(string(b))
}

// eventHandler is a slog.Handler that forwards records as JSON OnEvent calls.
// Sensitive attribute keys are redacted.
type eventHandler struct {
	node  *Node
	level slog.Level
	group string
	attrs []slog.Attr
}

func newEventHandler(n *Node) *eventHandler {
	return &eventHandler{node: n, level: slog.LevelInfo}
}

func (h *eventHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *eventHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, len(h.attrs)+r.NumAttrs()+2)
	for _, a := range h.attrs {
		appendAttr(attrs, h.group, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		appendAttr(attrs, h.group, a)
		return true
	})
	ev := map[string]any{
		"type":  "log",
		"level": r.Level.String(),
		"msg":   r.Message,
		"time":  r.Time.UTC().Format(time.RFC3339Nano),
	}
	if len(attrs) > 0 {
		ev["attrs"] = attrs
	}
	h.node.emitEvent(ev)
	return nil
}

func (h *eventHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := *h
	c.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &c
}

func (h *eventHandler) WithGroup(name string) slog.Handler {
	c := *h
	if h.group != "" {
		c.group = h.group + "." + name
	} else {
		c.group = name
	}
	return &c
}

func appendAttr(dst map[string]any, group string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	key := a.Key
	if group != "" {
		key = group + "." + key
	}
	if isSecretKey(key) {
		dst[key] = "[redacted]"
		return
	}
	switch a.Value.Kind() {
	case slog.KindGroup:
		for _, ga := range a.Value.Group() {
			appendAttr(dst, key, ga)
		}
	default:
		dst[key] = a.Value.Any()
	}
}

func isSecretKey(key string) bool {
	k := strings.ToLower(key)
	// Strip group prefixes for matching the leaf name.
	if i := strings.LastIndex(k, "."); i >= 0 {
		k = k[i+1:]
	}
	switch k {
	case "authkey", "auth_key", "ts_authkey", "password", "secret", "token":
		return true
	default:
		return false
	}
}
