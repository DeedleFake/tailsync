// Package daemon runs the tailsync synchronization service.
package daemon

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
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
	// DefaultMaxFileBytes is the max single-file size loaded into memory for
	// transfer/delta (v1 keeps whole-file buffers). Wire framing is also capped
	// by proto.MaxMessageSize.
	DefaultMaxFileBytes = 64 << 20 // 64 MiB
	// maxParallelPeerSyncs caps concurrent peer dial/sync workers so N online
	// peers cannot each hold up to MaxFileBytes in memory at once.
	maxParallelPeerSyncs = 4
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
	// Empty means dial all online tailnet peers except self (on large tailnets
	// prefer setting this or -peers). Ignored when Peers is set.
	ServiceName string
	// Port is the TCP port to listen on over the tailnet (or localhost in plain mode).
	Port int
	// AuthKey is an optional Tailscale auth key for NetModeTSNet
	// (else interactive login / existing tsnet state). Unused in host mode.
	AuthKey string
	// ScanInterval is how often to rescan the local directory.
	ScanInterval time.Duration
	// SyncInterval is how often to reconnect/sync with peers.
	SyncInterval time.Duration
	// BlockSize for rsync-style signatures.
	BlockSize int
	// MaxFileBytes rejects local files larger than this for serve/pull (0 = default).
	MaxFileBytes int64
	// TombstoneTTL drops old deletion tombstones from the index (0 = default 30d).
	TombstoneTTL time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// NetMode selects networking: host (default), tsnet, or plain. See NetMode constants.
	NetMode NetMode
	// ListenHost is used when NetMode is NetModePlain (default 127.0.0.1).
	ListenHost string
	// Peers is an optional explicit list of peer addresses (host:port). When empty,
	// peers are discovered from Tailscale status (online nodes other than self).
	Peers []string
	// OnReady, if non-nil, is called once after the daemon is listening and before
	// the main loop. Used by library wrappers (e.g. mobile) so Start can wait for
	// listen success or a fast failure. Must not block indefinitely.
	OnReady func()
}

// Daemon is the synchronization service.
//
// Locking:
//
//   - syncMu serializes multi-step local reconcile and remote apply (decide →
//     optional network → re-check LWW → disk/index commit). Concurrent peer
//     applies may run network I/O in parallel while unlocked; only decide and
//     commit hold syncMu. The main Run loop does not start reconcile until
//     syncPeers returns, so scanTick cannot interleave with an in-flight peer
//     sync batch.
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

	// connWG tracks in-flight handleConn goroutines so Run can drain them
	// before closing root (avoids nil/use-after-close races on d.root).
	connWG sync.WaitGroup

	// nodeID is the protocol identity. Set during listen before accept/sync
	// goroutines run; treated as immutable for the rest of Run.
	nodeID string

	// appliesSinceSave counts successful index mutations since last Save.
	// Touched only while holding syncMu.
	appliesSinceSave int
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
	if cfg.BlockSize <= 0 {
		cfg.BlockSize = delta.DefaultBlockSize
	}
	if cfg.MaxFileBytes <= 0 {
		cfg.MaxFileBytes = DefaultMaxFileBytes
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
