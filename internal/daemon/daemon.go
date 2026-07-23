// Package daemon runs the tailsync synchronization service.
package daemon

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/tsnet"

	"deedles.dev/tailsync/internal/delta"
	"deedles.dev/tailsync/internal/index"
)

// Default configuration values applied by [New] when the corresponding
// Config field is zero. CLI flags and mobile StatusJSON should reference
// these rather than re-hardcoding.
const (
	DefaultPort         = 5960
	DefaultScanInterval = 30 * time.Second
	DefaultSyncInterval = 45 * time.Second
	// DefaultWatchDebounce is how long to wait after an FS event before
	// reconciling when filesystem watching is active.
	DefaultWatchDebounce = 300 * time.Millisecond
	// DefaultMaxFileBytes is the max single-file size loaded into memory for
	// transfer/delta (v1 keeps whole-file buffers). Wire framing is also capped
	// by proto.MaxMessageSize.
	DefaultMaxFileBytes = 64 << 20 // 64 MiB
	// DefaultDialTimeout is how long an outbound peer dial may block before
	// failing. Without this, dials to online nodes that are not running tailsync
	// (or are unreachable) can hang for a long time and stall syncPeers.
	DefaultDialTimeout = 5 * time.Second
	// maxParallelPeerSyncs caps concurrent peer dial/sync workers so N online
	// peers cannot each hold up to MaxFileBytes in memory at once.
	maxParallelPeerSyncs = 4
	// manyPeersWarnThreshold is how many discovered peers trigger a one-time
	// recommendation for -peers or -service when discovery is unfiltered.
	// Independent of maxParallelPeerSyncs (concurrency/memory cap).
	manyPeersWarnThreshold = 8
)

// Config holds daemon configuration.
type Config struct {
	// Dir is the directory to synchronize (required).
	Dir string
	// StateDir holds the index and, when NetMode is NetModeTSNet, tsnet state.
	// Defaults to Dir/.tailsync.
	StateDir string
	// Hostname is the protocol / tsnet node name.
	//   - NetModeTSNet: advertised tsnet hostname (default tailsync-<os-hostname>).
	//   - NetModeHost: always overwritten from LocalAPI Self (MagicDNS → HostName →
	//     StableID); any configured value is ignored for protocol identity.
	//   - NetModePlain: wire-protocol node id when set.
	Hostname string
	// ServiceName, when non-empty, filters discovered peers to those whose
	// HostName or DNSName contains this substring (not a path prefix).
	// Empty means dial all online tailnet peers except self. On large tailnets
	// that dials phones, TVs, and other nodes not running tailsync; prefer
	// setting this or Peers so outbound sync does not waste time on them.
	// Ignored when Peers is set.
	ServiceName string
	// Port is the TCP port to listen on over the tailnet (or localhost in plain mode).
	Port int
	// AuthKey is an optional Tailscale auth key for NetModeTSNet
	// (else interactive login / existing tsnet state). Unused in host mode.
	AuthKey string
	// ScanInterval is the safety-net full rescan period. When filesystem
	// watching is active, local edits are reconciled via debounced FS events;
	// this interval still walks the tree to catch missed events.
	ScanInterval time.Duration
	// SyncInterval is the backup peer sync period for catch-up (offline peers,
	// missed notifications). Local index changes also request an immediate
	// coalesced bidirectional peer session (both sides pull on one connection)
	// without waiting for this ticker.
	SyncInterval time.Duration
	// WatchDebounce is how long to wait after an FS event before reconciling
	// (0 = DefaultWatchDebounce). Ignored when DisableWatch is set or watch
	// fails to start.
	WatchDebounce time.Duration
	// DisableWatch skips filesystem watching and relies on ScanInterval only.
	// Useful in tests and on platforms where watching is unavailable.
	DisableWatch bool
	// BlockSize for rsync-style signatures.
	BlockSize int
	// MaxFileBytes rejects local files larger than this for serve/pull (0 = default).
	MaxFileBytes int64
	// DialTimeout is the max wait for an outbound peer TCP dial (0 = DefaultDialTimeout).
	// Caps hangs against online nodes that are not listening for tailsync.
	DialTimeout time.Duration
	// TombstoneTTL drops old deletion tombstones from the index (0 = default 30d).
	TombstoneTTL time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// NetMode selects networking: host (default), tsnet, or plain. See NetMode constants.
	NetMode NetMode
	// ListenHost is used when NetMode is NetModePlain (default 127.0.0.1).
	ListenHost string
	// Peers is an optional explicit list of peer addresses (host:port). When empty,
	// peers are discovered from Tailscale status (all online nodes other than self).
	// Discovery will dial nodes that are not running tailsync; use Peers or
	// ServiceName on multi-device tailnets to avoid slow outbound batches.
	Peers []string
	// OnReady, if non-nil, is called once after the daemon is listening and before
	// the main loop. Used by library wrappers (e.g. mobile) so Start can wait for
	// listen success or a fast failure. Must not block indefinitely.
	OnReady func()
	// OnAuthURL, if non-nil, is called when interactive Tailscale login is needed
	// during NetModeTSNet bring-up and an auth/login URL is available (for example
	// browser login when AuthKey is empty and no enrolled tsnet state exists).
	// Invoked from a background goroutine while Up is still waiting. Called at
	// most once per distinct URL per listen attempt. Must return quickly.
	// Not used for host or plain modes. Never receives AuthKey material.
	OnAuthURL func(url string)
	// AfterReconcile, if non-nil, is called after each successful reconcile with
	// whether peer-visible local index content changed. For tests. Must return
	// quickly and must not call back into the daemon.
	AfterReconcile func(changed bool)
	// AfterSyncPeers, if non-nil, is called after each syncPeers batch completes
	// (including empty peer lists). For tests. Must return quickly and must not
	// call back into the daemon.
	AfterSyncPeers func()
}

// Daemon is the synchronization service.
//
// Locking:
//
//   - syncMu serializes multi-step local reconcile and remote apply (decide →
//     optional network → re-check LWW → disk/index commit). Concurrent peer
//     applies may run network I/O in parallel while unlocked; only decide and
//     commit hold syncMu. The main Run loop does not start reconcile until
//     syncPeers returns, so reconcile signals cannot interleave with an
//     in-flight peer sync batch.
//   - index.Index has its own RWMutex for map access. Holding syncMu does not
//     replace index locks; index methods still lock internally. Callers that
//     need a stable multi-step view of the index relative to disk must hold
//     syncMu around the whole operation (see reconcile, applyRemote).
type Daemon struct {
	cfg    Config
	log    *slog.Logger
	idx    *index.Index
	server *tsnet.Server // NetModeTSNet only
	local  *local.Client // NetModeHost
	ln     net.Listener
	// root confines sync-tree filesystem I/O to cfg.Dir (opened for Run).
	root *os.Root

	// syncMu serializes reconcile and remote apply commits (see package comment).
	syncMu sync.Mutex

	// netMu guards injectNetChange so mobile NotifyNetworkChange can race Stop
	// without observing a half-torn-down callback.
	netMu           sync.Mutex
	injectNetChange func() // NetModeTSNet: mon.InjectEvent after Up; nil otherwise

	// connWG tracks in-flight handleConn goroutines so Run can drain them
	// before closing root (avoids nil/use-after-close races on d.root).
	connWG sync.WaitGroup

	// nodeID is the protocol identity. Set during listen before accept/sync
	// goroutines run; treated as immutable for the rest of Run.
	nodeID string

	// appliesSinceSave counts successful index mutations since last Save.
	// Touched only while holding syncMu.
	appliesSinceSave int

	// manyPeersWarned is set after the one-time unfiltered-discovery Warn so
	// syncPeers does not spam every batch on multi-device tailnets.
	manyPeersWarned atomic.Bool
}

// InjectNetworkChange signals tsnet's netmon that host connectivity changed
// (Android ConnectivityManager updates). No-op when not in tsnet mode, before
// Up succeeds, or after closeNetworkBackend. Safe concurrent with Run/Stop:
// copies the inject func under netMu, then invokes it outside the lock.
func (d *Daemon) InjectNetworkChange() {
	if d == nil {
		return
	}
	d.netMu.Lock()
	f := d.injectNetChange
	d.netMu.Unlock()
	if f != nil {
		f()
	}
}

func (d *Daemon) setInjectNetChange(f func()) {
	d.netMu.Lock()
	d.injectNetChange = f
	d.netMu.Unlock()
}

// New constructs a Daemon from cfg (does not start it).
func New(cfg Config) (*Daemon, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("sync directory is required")
	}
	abs, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("resolve dir: %w", err)
	}
	cfg.Dir = abs

	if cfg.StateDir == "" {
		cfg.StateDir = filepath.Join(cfg.Dir, ".tailsync")
	} else {
		cfg.StateDir, err = filepath.Abs(cfg.StateDir)
		if err != nil {
			return nil, fmt.Errorf("resolve state dir: %w", err)
		}
	}
	// Hostname defaults for tsnet (advertised name) and plain (protocol id).
	// Host mode fills identity from LocalAPI during listen when empty.
	if cfg.Hostname == "" && cfg.NetMode != NetModeHost {
		host, _ := os.Hostname()
		if host == "" {
			host = "tailsync"
		}
		if cfg.NetMode == NetModeTSNet {
			cfg.Hostname = "tailsync-" + host
		} else {
			cfg.Hostname = host
		}
	}
	// ServiceName stays empty by default: discover all online peers.
	if cfg.Port == 0 {
		cfg.Port = DefaultPort
	}
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = DefaultScanInterval
	}
	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = DefaultSyncInterval
	}
	if cfg.WatchDebounce <= 0 {
		cfg.WatchDebounce = DefaultWatchDebounce
	}
	if cfg.BlockSize <= 0 {
		cfg.BlockSize = delta.DefaultBlockSize
	}
	if cfg.MaxFileBytes <= 0 {
		cfg.MaxFileBytes = DefaultMaxFileBytes
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.TombstoneTTL <= 0 {
		cfg.TombstoneTTL = index.DefaultTombstoneTTL
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.ListenHost == "" {
		cfg.ListenHost = "127.0.0.1"
	}

	return &Daemon{
		cfg: cfg,
		log: log,
	}, nil
}

// fileMode returns mode, or 0o644 when mode is zero (peer omitted permissions).
func fileMode(mode os.FileMode) os.FileMode {
	if mode == 0 {
		return 0o644
	}
	return mode
}
