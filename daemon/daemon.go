// Package daemon is the public library API for running the tailsync
// synchronization service. Embedders (CLI, Android app wrappers, other hosts)
// construct a [Daemon] with [New] and call [Daemon.Run].
//
// The CLI lives in cmd/tailsync. Android/gomobile bind packages should live in
// the app repository and depend on this package rather than a bind surface here.
//
// # Unstable hooks
//
// [Config.AfterReconcile], [Config.AfterSyncPeers], and [Config.AfterNotify]
// exist for in-module tests. They are not a supported production embedder API
// and may change or be removed without notice.
package daemon

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/tsnet"

	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/peer"
)

// Default configuration values applied by [New] when the corresponding
// Config field is zero. CLI flags and embedder UIs should reference these
// rather than re-hardcoding.
const (
	DefaultPort         = 5960
	DefaultScanInterval = 30 * time.Second
	DefaultSyncInterval = 45 * time.Second
	// DefaultWatchDebounce is how long to wait after an FS event before
	// reconciling when filesystem watching is active.
	DefaultWatchDebounce = time.Second
	// DefaultBlockSize is the rsync-style delta block size when Config.BlockSize
	// is zero (4096 bytes).
	DefaultBlockSize = 4096
	// DefaultMaxFileBytes is the max single-file size loaded into memory for
	// transfer/delta (v1 keeps whole-file buffers). Wire framing is capped
	// separately so peer messages cannot allocate unboundedly.
	DefaultMaxFileBytes = 64 << 20 // 64 MiB
	// DefaultDialTimeout is how long an outbound peer dial/handshake may block
	// before failing during discovery.
	DefaultDialTimeout = 30 * time.Second
	// DefaultTombstoneTTL is how long deletion tombstones are retained before
	// garbage collection when Config.TombstoneTTL is zero (30 days).
	DefaultTombstoneTTL = 30 * 24 * time.Hour
	// DefaultDiscoveryConcurrency caps concurrent discovery dials (in-flight only).
	DefaultDiscoveryConcurrency = 32
	// DefaultPullStreamConcurrency caps concurrent pull streams across peers
	// (each may buffer up to MaxFileBytes).
	DefaultPullStreamConcurrency = 8
	// DefaultHeartbeatInterval is the app-level ping interval on each session.
	DefaultHeartbeatInterval = 20 * time.Second
	// maxParallelNotifies caps concurrent notify streams so a large connected
	// set cannot open unbounded streams per local change. Fan-out is still
	// fire-and-forget w.r.t. the main loop (no batch Wait).
	maxParallelNotifies = 16
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
	// Port is the UDP port for QUIC peer sessions over the tailnet (or localhost in plain mode).
	Port int
	// AuthKey is an optional Tailscale auth key for NetModeTSNet
	// (else interactive login / existing tsnet state). Unused in host mode.
	AuthKey string
	// ScanInterval is the safety-net full rescan period. When filesystem
	// watching is active, local edits are reconciled via debounced FS events;
	// this interval still walks the tree to catch missed events.
	ScanInterval time.Duration
	// SyncInterval is the backup peer pull period for catch-up (offline peers,
	// missed notifies). Local index changes fan out best-effort notifies; peers
	// pull on their own. Correctness does not depend on notify delivery.
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
	// DialTimeout is the max wait for an outbound peer QUIC dial+handshake
	// during discovery (0 = DefaultDialTimeout).
	DialTimeout time.Duration
	// DiscoveryConcurrency caps concurrent discovery dials (0 = default 32).
	// The semaphore is in-flight only (released before backoff sleep).
	DiscoveryConcurrency int
	// PullStreamConcurrency caps concurrent content pull streams across all
	// peers (0 = default 8). Separate from discovery concurrency.
	PullStreamConcurrency int
	// HeartbeatInterval is the app-level ping period on each peer session
	// (0 = DefaultHeartbeatInterval). QUIC also has its own keep-alive.
	HeartbeatInterval time.Duration
	// TombstoneTTL drops old deletion tombstones from the index (0 = DefaultTombstoneTTL).
	TombstoneTTL time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// NetMode selects networking: host (default), tsnet, or plain. See NetMode constants.
	NetMode NetMode
	// ListenHost is used when NetMode is NetModePlain (default 127.0.0.1).
	ListenHost string
	// Peers is an optional explicit list of peer addresses (host:port) for tests
	// and overrides. When empty, discovery uses mesh trust on Online status
	// peers: untagged Self → same UserID; tagged Self → peer must carry MeshTag
	// (excludes self, sharees, Mullvad, and identity mismatches). When set,
	// status discovery is skipped (test determinism), but host/tsnet handshakes
	// still enforce WhoIs + the same trust policy. Soft dial failures and
	// backoff handle nodes that are Online but not running tailsync.
	Peers []string
	// MeshTag is the ACL tag peers must share when this machine is tagged
	// (for example "tag:tailsync"). Required at listen when Self is tagged;
	// ignored when Self is untagged. Must look like tag:name when non-empty.
	MeshTag string
	// OnReady, if non-nil, is called once after the daemon is listening and before
	// the main loop. Used by library hosts so Start/lifecycle wrappers can wait
	// for listen success or a fast failure. Must not block indefinitely.
	OnReady func()
	// OnAuthURL, if non-nil, is called when interactive Tailscale login is needed
	// during NetModeTSNet bring-up and an auth/login URL is available (for example
	// browser login when AuthKey is empty and no enrolled tsnet state exists).
	// Invoked from a background goroutine while Up is still waiting. Called at
	// most once per distinct URL per listen attempt. Must return quickly.
	// Not used for host or plain modes. Never receives AuthKey material.
	OnAuthURL func(url string)
	// AfterReconcile, if non-nil, is called after each successful reconcile with
	// whether peer-visible local index content changed.
	//
	// Unstable / test-only: not a supported production API; may change or be
	// removed without notice. Must return quickly and must not call back into
	// the daemon.
	AfterReconcile func(changed bool)
	// AfterSyncPeers, if non-nil, is called after each pullPeers batch completes
	// (including empty peer lists).
	//
	// Unstable / test-only: not a supported production API; may change or be
	// removed without notice. Must return quickly and must not call back into
	// the daemon.
	AfterSyncPeers func()
	// AfterNotify, if non-nil, is called when scheduleNotify actually schedules
	// at least one notify goroutine (not when deduped or no candidates; not
	// after each peer dial finishes).
	//
	// Unstable / test-only: not a supported production API; may change or be
	// removed without notice. Must return quickly and must not call back into
	// the daemon.
	AfterNotify func()
}

// Daemon is the synchronization service.
//
// Locking:
//
//   - syncMu serializes multi-step local reconcile and remote apply (decide →
//     optional network → re-check LWW → disk/index commit). Concurrent peer
//     applies may run network I/O in parallel while unlocked; only decide and
//     commit hold syncMu. The main Run loop keeps reconcile→notify responsive;
//     pull batches run on a single-flight worker (pullWG) and do not block
//     notify scheduling. Notify fan-out is fire-and-forget and does not hold
//     syncMu.
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
	// mesh owns shared QUIC transports, roster, and discovery.
	mesh *peer.Manager
	// quicTLS is the server TLS config (ephemeral self-signed cert) for QUIC.
	quicTLS *tls.Config
	// root confines sync-tree filesystem I/O to cfg.Dir (opened for Run).
	root *os.Root

	// syncMu serializes reconcile and remote apply commits (see package comment).
	syncMu sync.Mutex

	// netMu guards injectNetChange so embedder InjectNetworkChange can race
	// shutdown without observing a half-torn-down callback.
	netMu           sync.Mutex
	injectNetChange func() // NetModeTSNet: mon.InjectEvent after Up; nil otherwise

	// streamWG tracks in-flight handleStream goroutines so Run can drain them
	// before closing root (avoids nil/use-after-close races on d.root).
	streamWG sync.WaitGroup
	// notifyWG tracks fire-and-forget notify streams; joined on shutdown before
	// root/network teardown so handlers never see nil shared resources.
	notifyWG sync.WaitGroup
	// pullWG tracks the single-flight pull worker; joined on shutdown with
	// notifyWG so pull streams cannot use root after close.
	pullWG sync.WaitGroup

	// nodeID is the protocol identity. Set during listen before accept/sync
	// goroutines run; treated as immutable for the rest of Run.
	nodeID string

	// appliesSinceSave counts successful index mutations since last Save.
	// Touched only while holding syncMu.
	appliesSinceSave int

	// pullSem limits concurrent content pull streams across peers.
	pullSem chan struct{}
	// serveSem limits concurrent inbound serve ops (manifest/file/delta).
	serveSem chan struct{}

	// notifySeen dedupes content keys successfully advertised (storm prevention).
	notifySeen *notifyTracker
	// needPull is allocated in New and never niled; requestPull coalesces
	// pull wake-ups safely concurrent with onNotify and Run shutdown.
	needPull signal

	// pullFlight single-flights background pull batches (see schedulePull).
	pullFlight pullFlight
}

// InjectNetworkChange signals tsnet's netmon that host connectivity changed
// (Android ConnectivityManager updates). No-op when not in tsnet mode, before
// Up succeeds, or after network backend teardown. Safe concurrent with [Daemon.Run]
// and Run shutdown (context cancel): copies the inject func under netMu, then
// invokes it outside the lock.
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
		cfg.BlockSize = DefaultBlockSize
	}
	if cfg.MaxFileBytes <= 0 {
		cfg.MaxFileBytes = DefaultMaxFileBytes
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.DiscoveryConcurrency <= 0 {
		cfg.DiscoveryConcurrency = DefaultDiscoveryConcurrency
	}
	if cfg.PullStreamConcurrency <= 0 {
		cfg.PullStreamConcurrency = DefaultPullStreamConcurrency
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.TombstoneTTL <= 0 {
		cfg.TombstoneTTL = DefaultTombstoneTTL
	}
	cfg.MeshTag = normalizeMeshTag(cfg.MeshTag)
	if err := validateMeshTagFormat(cfg.MeshTag); err != nil {
		return nil, err
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.ListenHost == "" {
		cfg.ListenHost = "127.0.0.1"
	}

	quicTLS, err := generateQUICTLSConfig()
	if err != nil {
		return nil, err
	}

	return &Daemon{
		cfg:        cfg,
		log:        log,
		quicTLS:    quicTLS,
		pullSem:    make(chan struct{}, cfg.PullStreamConcurrency),
		serveSem:   make(chan struct{}, cfg.PullStreamConcurrency),
		notifySeen: newNotifyTracker(),
		needPull:   newSignal(),
	}, nil
}

// fileMode returns mode, or 0o644 when mode is zero (peer omitted permissions).
func fileMode(mode os.FileMode) os.FileMode {
	if mode == 0 {
		return 0o644
	}
	return mode
}

// pullFlight single-flights background pull batches so the main loop can keep
// reconcile→notify responsive while a pull is in flight. A request that arrives
// during a running batch sets pending and runs once more after the current batch.
type pullFlight struct {
	mu      sync.Mutex
	running bool
	pending bool
}

// tryStart returns true if the caller should run a pull batch. If a batch is
// already running, marks pending and returns false.
func (f *pullFlight) tryStart() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.running {
		f.pending = true
		return false
	}
	f.running = true
	return true
}

// finish clears running, or returns true if another batch should run immediately
// because work was requested while the previous batch was in flight.
func (f *pullFlight) finish() (runAgain bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pending {
		f.pending = false
		// keep running true for the follow-up batch
		return true
	}
	f.running = false
	return false
}
