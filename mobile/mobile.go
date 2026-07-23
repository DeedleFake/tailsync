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
//	{"type":"auth","url":"https://login.tailscale.com/..."}
//
// The "auth" event is emitted during Start (while still blocking) when the
// embedded tsnet node needs interactive browser login and an AuthURL is
// available. It is not emitted when AuthKey works, or when existing tsnet
// state under StateDir already enrolls the node. Open the URL in a browser
// or Custom Tab; after login, Start completes when the node is Running.
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
	"time"

	"deedles.dev/tailsync/internal/daemon"
	"deedles.dev/tailsync/internal/delta"
)

// Config holds bindable configuration. All exported fields use types gomobile
// can bind (string, bool, int, int64).
//
// Dir is required and must be an absolute path. StateDir, when set, must also
// be absolute; otherwise it defaults to Dir/.tailsync (resolved by the daemon).
//
// Zero values for Port, ScanIntervalMs, SyncIntervalMs, and BlockSize mean
// “use daemon defaults”. StatusJSON reports those effective defaults.
type Config struct {
	// Dir is the absolute path to the sync root (required).
	Dir string
	// StateDir is optional; default is under Dir (.tailsync).
	StateDir string
	// Hostname is the tsnet hostname when NetMode is "tsnet".
	Hostname string
	// AuthKey is an optional Tailscale auth key for tsnet registration.
	// When empty, first run may require browser login (see "auth" events);
	// subsequent runs reuse enrolled state under StateDir without re-prompting.
	AuthKey string
	// Port is the TCP listen/dial port (0 = daemon default).
	Port int
	// Peers is a comma-separated list of host:port peers (optional; empty = discovery).
	Peers string
	// ServiceName filters discovered peers by hostname/DNS substring.
	ServiceName string
	// ScanIntervalMs is the local rescan period in milliseconds (0 = default).
	ScanIntervalMs int64
	// SyncIntervalMs is the peer sync period in milliseconds (0 = default).
	SyncIntervalMs int64
	// BlockSize is the delta block size (0 = default).
	BlockSize int
	// NetMode selects networking: "tsnet" (default for mobile), "host", or "plain".
	// Android apps should use "tsnet". "plain" is for testing only (localhost TCP).
	// "host" requires a system tailscaled (not typical on Android).
	NetMode string
}

// EventListener is implemented in Kotlin (or other gomobile hosts) for status,
// log, and auth callbacks. Events are JSON objects (see package docs).
//
// OnEvent must return quickly: it is called synchronously from daemon and
// start/stop paths. Blocking here stalls logging and can delay Start. Do not
// update Android UI directly; post to the main looper/dispatcher first.
// For "auth" events, post opening a Custom Tab / browser; do not block OnEvent
// on the login completing (Start remains blocked until login finishes).
//
// AuthKey and other secrets are never included in event payloads.
type EventListener interface {
	// OnEvent receives a single JSON event object as a string.
	OnEvent(eventJSON string)
}

// Node is a long-lived sync instance. Construct with NewNode, then Start/Stop
// from the app lifecycle (e.g. a foreground service).
//
// See lifecycle.go for ownership invariants (generation counter + phase machine).
type Node struct {
	cfg Config

	mu       sync.Mutex
	phase    nodePhase
	listener EventListener

	// authURL / needsLogin track interactive tsnet login during Start.
	// Updated when OnAuthURL fires; cleared when ready or the run ends.
	// acceptAuthURL gates noteAuthURL so late callbacks after ready/finish
	// cannot re-arm needs_login or emit another "auth" event.
	authURL       string
	needsLogin    bool
	acceptAuthURL bool

	// gen is incremented on each claimStart; finish only clears matching gen.
	gen        uint64
	ctx        context.Context
	cancel     context.CancelFunc
	finished   chan struct{} // closed when ownership ends (per generation)
	workerDone chan struct{} // closed when Run/abort fully done (per generation)
	runErr     error         // set before finished is closed
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
	dc, err := toDaemonConfig(&c, slog.Default(), nil, nil)
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
// When interactive login is in progress: "needs_login" and "auth_url" (if known).
func (n *Node) StatusJSON() (string, error) {
	if n == nil {
		return "", errors.New("nil node")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	port, scanMs, syncMs, block := effectiveDisplay(n.cfg)
	st := statusSnapshot{
		Type:       "status",
		Running:    n.phase == phaseRunning,
		Phase:      n.phase.String(),
		Dir:        n.cfg.Dir,
		StateDir:   n.cfg.StateDir,
		Hostname:   n.cfg.Hostname,
		Port:       port,
		NetMode:    effectiveNetMode(n.cfg.NetMode),
		Service:    n.cfg.ServiceName,
		Peers:      n.cfg.Peers,
		ScanMs:     scanMs,
		SyncMs:     syncMs,
		BlockSize:  block,
		Version:    Version(),
		NeedsLogin: n.needsLogin,
		AuthURL:    n.authURL,
	}
	b, err := json.Marshal(st)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func validateConfig(cfg *Config) error {
	if cfg.Dir == "" {
		return errors.New("dir is required")
	}
	if !filepath.IsAbs(cfg.Dir) {
		return fmt.Errorf("dir must be an absolute path: %q", cfg.Dir)
	}
	if cfg.StateDir != "" && !filepath.IsAbs(cfg.StateDir) {
		return fmt.Errorf("state dir must be an absolute path: %q", cfg.StateDir)
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
		port = daemon.DefaultPort
	}
	scanMs = cfg.ScanIntervalMs
	if scanMs == 0 {
		scanMs = daemon.DefaultScanInterval.Milliseconds()
	}
	syncMs = cfg.SyncIntervalMs
	if syncMs == 0 {
		syncMs = daemon.DefaultSyncInterval.Milliseconds()
	}
	block = cfg.BlockSize
	if block == 0 {
		block = delta.DefaultBlockSize
	}
	return port, scanMs, syncMs, block
}

func toDaemonConfig(cfg *Config, log *slog.Logger, onReady func(), onAuthURL func(string)) (daemon.Config, error) {
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
		for p := range strings.SplitSeq(cfg.Peers, ",") {
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
		OnAuthURL:    onAuthURL,
	}, nil
}

type statusSnapshot struct {
	Type       string `json:"type"`
	Running    bool   `json:"running"`
	Phase      string `json:"phase"`
	Dir        string `json:"dir"`
	StateDir   string `json:"state_dir,omitempty"`
	Hostname   string `json:"hostname,omitempty"`
	Port       int    `json:"port"`
	NetMode    string `json:"net_mode"`
	Service    string `json:"service,omitempty"`
	Peers      string `json:"peers,omitempty"`
	ScanMs     int64  `json:"scan_interval_ms"`
	SyncMs     int64  `json:"sync_interval_ms"`
	BlockSize  int    `json:"block_size"`
	Version    string `json:"version"`
	NeedsLogin bool   `json:"needs_login,omitempty"`
	AuthURL    string `json:"auth_url,omitempty"`
}

// noteAuthURL records interactive login state and emits a single "auth" event
// per distinct URL while Start is accepting auth (phaseStarting and before
// ready/finish). Late callbacks after clearAuthState are ignored.
// Safe for concurrent use; returns quickly.
func (n *Node) noteAuthURL(url string) {
	if n == nil || url == "" {
		return
	}
	n.mu.Lock()
	if !n.acceptAuthURL {
		n.mu.Unlock()
		return
	}
	if n.authURL == url && n.needsLogin {
		n.mu.Unlock()
		return
	}
	n.authURL = url
	n.needsLogin = true
	n.mu.Unlock()
	n.emitEvent(map[string]any{
		"type": "auth",
		"url":  url,
	})
}

// clearAuthState clears interactive login fields and stops accepting further
// auth URLs (late OnAuthURL callbacks become no-ops).
func (n *Node) clearAuthState() {
	if n == nil {
		return
	}
	n.mu.Lock()
	n.clearAuthStateLocked()
	n.mu.Unlock()
}

// clearAuthStateLocked clears auth URL state and acceptAuthURL. Caller holds mu.
func (n *Node) clearAuthStateLocked() {
	n.authURL = ""
	n.needsLogin = false
	n.acceptAuthURL = false
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
