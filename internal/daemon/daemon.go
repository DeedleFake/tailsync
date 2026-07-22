// Package daemon runs the tailsync synchronization service.
package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/tsnet"

	"deedles.dev/tailsync/internal/atomicfile"
	"deedles.dev/tailsync/internal/delta"
	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/proto"
	"deedles.dev/tailsync/internal/scan"
)

// DefaultMaxFileBytes is the max single-file size loaded into memory for
// transfer/delta (v1 keeps whole-file buffers). Wire framing is also capped
// by proto.MaxMessageSize.
const DefaultMaxFileBytes = 64 << 20 // 64 MiB

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
type Daemon struct {
	cfg    Config
	log    *slog.Logger
	idx    *index.Index
	server *tsnet.Server // NetModeTSNet only
	local  *local.Client // NetModeHost
	ln     net.Listener

	// syncMu serializes local reconcile and remote apply so scan→apply and
	// peer mutations cannot interleave on the same path/index snapshot.
	// Held across network pulls in applyRemote (v1 correctness over throughput):
	// one slow peer or large file can block other applies and local scans until
	// the connection deadline (up to ~5m). Future work: per-path locks or
	// release-during-I/O with re-validation before commit.
	syncMu sync.Mutex

	mu       sync.Mutex
	nodeID   string
	peerSeen map[string]time.Time // last successful sync

	// appliesSinceSave counts successful index mutations since last Save.
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
		cfg.Port = 5960
	}
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = 30 * time.Second
	}
	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = 45 * time.Second
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
		cfg:      cfg,
		log:      log,
		peerSeen: make(map[string]time.Time),
	}, nil
}

// Run starts the daemon until ctx is cancelled.
func (d *Daemon) Run(ctx context.Context) error {
	if err := os.MkdirAll(d.cfg.StateDir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	if err := os.MkdirAll(d.cfg.Dir, 0o755); err != nil {
		return fmt.Errorf("create sync dir: %w", err)
	}

	indexPath := filepath.Join(d.cfg.StateDir, "index.json")
	idx, err := index.Load(indexPath)
	if err != nil {
		return fmt.Errorf("load index: %w", err)
	}
	d.idx = idx
	// nodeID may be refined during listen (host mode uses LocalAPI Self).
	d.nodeID = d.cfg.Hostname

	// Initial reconcile: detect offline deletions and local changes.
	if err := d.reconcile(ctx); err != nil {
		return fmt.Errorf("initial reconcile: %w", err)
	}

	if err := d.listen(ctx); err != nil {
		return err
	}

	// Capture listener for acceptLoop so Close/nil of d.ln cannot race Accept.
	ln := d.ln
	acceptDone := make(chan struct{})
	var acceptErr error
	go func() {
		defer close(acceptDone)
		acceptErr = d.acceptLoop(ctx, ln)
	}()

	// On every exit path: close the listener to unblock Accept, wait for the
	// accept goroutine to finish, then tear down tsnet/local client state.
	// Never nil d.ln while acceptLoop may still call Accept on a shared field.
	defer func() {
		d.closeNetListener()
		<-acceptDone
		d.closeNetworkBackend()
	}()

	d.log.Info("tailsync started",
		"dir", d.cfg.Dir,
		"state", d.cfg.StateDir,
		"hostname", d.nodeID,
		"port", d.cfg.Port,
		"net_mode", d.cfg.NetMode.String(),
		"index_entries", d.idx.Len(),
		"max_file_bytes", d.cfg.MaxFileBytes,
	)
	if d.cfg.OnReady != nil {
		d.cfg.OnReady()
	}

	scanTick := time.NewTicker(d.cfg.ScanInterval)
	defer scanTick.Stop()
	syncTick := time.NewTicker(d.cfg.SyncInterval)
	defer syncTick.Stop()

	// Immediate peer sync attempt.
	d.syncPeers(ctx)

	for {
		select {
		case <-ctx.Done():
			d.log.Info("shutting down")
			d.syncMu.Lock()
			if err := d.idx.Save(); err != nil {
				d.log.Error("save index on shutdown", "err", err)
			}
			d.syncMu.Unlock()
			return nil
		case <-acceptDone:
			if acceptErr != nil && ctx.Err() == nil {
				return acceptErr
			}
			return nil
		case <-scanTick.C:
			if err := d.reconcile(ctx); err != nil {
				d.log.Error("reconcile", "err", err)
			}
		case <-syncTick.C:
			d.syncPeers(ctx)
		}
	}
}

// acceptLoop accepts connections on ln until ln is closed or a fatal Accept
// error occurs. ln must be non-nil and remain valid for the duration (caller
// closes it to unblock, then waits for this function to return).
func (d *Daemon) acceptLoop(ctx context.Context, ln net.Listener) error {
	if ln == nil {
		return nil
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Closed listener and/or cancelled context are normal shutdown.
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return nil
			}
			// Some platforms wrap closed-listener errors without net.ErrClosed.
			if isListenerClosedErr(err) {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		go d.handleConn(ctx, conn)
	}
}

func isListenerClosedErr(err error) bool {
	if err == nil {
		return false
	}
	// net.ErrClosed and "use of closed network connection" variants.
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "listener closed") ||
		strings.Contains(msg, "server closed")
}

func (d *Daemon) setConnDeadline(conn net.Conn, ctx context.Context) {
	deadline := time.Now().Add(5 * time.Minute)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)
}

func (d *Daemon) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	d.setConnDeadline(conn, ctx)
	remote := conn.RemoteAddr().String()
	d.log.Debug("inbound connection", "remote", remote)

	// Expect Hello first.
	msg, err := proto.Decode(conn)
	if err != nil {
		d.log.Debug("decode hello", "remote", remote, "err", err)
		return
	}
	if msg.Header.Type != proto.TypeHello {
		_ = proto.Encode(conn, proto.NewError("expected hello"))
		return
	}
	if msg.Header.Version != 0 && msg.Header.Version != proto.Version {
		_ = proto.Encode(conn, proto.NewError(fmt.Sprintf("unsupported version %d", msg.Header.Version)))
		return
	}
	if err := proto.Encode(conn, proto.NewHelloOK(d.nodeID)); err != nil {
		return
	}

	// Serve requests until EOF or fatal error. Recoverable per-request errors
	// send TypeError and keep the session open so the client can continue.
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		d.setConnDeadline(conn, ctx)
		msg, err := proto.Decode(conn)
		if err != nil {
			if err != io.EOF {
				d.log.Debug("session ended", "remote", remote, "err", err)
			}
			return
		}
		if err := d.serveMsg(ctx, conn, msg); err != nil {
			d.log.Debug("serve message", "remote", remote, "type", msg.Header.Type, "err", err)
			if encodeErr := proto.Encode(conn, proto.NewError(err.Error())); encodeErr != nil {
				return
			}
			// Keep session open for not-found / validation errors; close on
			// unexpected protocol types so a confused peer does not loop.
			if msg.Header.Type == "" || isFatalServeErr(err) {
				return
			}
		}
	}
}

func isFatalServeErr(err error) bool {
	// Unexpected message types are protocol-level; everything else is per-request.
	return err != nil && strings.HasPrefix(err.Error(), "unexpected message type")
}

func (d *Daemon) serveMsg(ctx context.Context, conn net.Conn, msg proto.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch msg.Header.Type {
	case proto.TypePing:
		return proto.Encode(conn, proto.Message{Header: proto.Header{Type: proto.TypePong}})
	case proto.TypeManifestReq:
		return proto.Encode(conn, proto.NewManifest(d.idx.Manifest()))
	case proto.TypeFileReq:
		return d.serveFile(ctx, conn, msg.Header.Path)
	case proto.TypeSigReq:
		return d.serveSig(ctx, conn, msg.Header.Path, msg.Header.BlockSize)
	case proto.TypeDeltaReq:
		return d.serveDelta(ctx, conn, msg.Header.Path, msg.Header.Hash, msg.Header.BlockSize, msg.Payload)
	default:
		return fmt.Errorf("unexpected message type %q", msg.Header.Type)
	}
}

// absPath validates a relative sync path and returns its absolute location under Dir.
// Uses filepath.IsLocal so names like "foo..bar" are allowed while ".." segments are not.
func (d *Daemon) absPath(rel string) (string, error) {
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	if rel == "" || strings.HasPrefix(rel, "/") {
		return "", fmt.Errorf("invalid path %q", rel)
	}
	// Normalize and reject ".." segments without banning ".." as a substring of a name.
	cleaned := pathCleanSlash(rel)
	if cleaned == "." || cleaned == "" || !fs.ValidPath(cleaned) {
		return "", fmt.Errorf("invalid path %q", rel)
	}
	// filepath.IsLocal rejects "..", absolute, and volume paths on all platforms.
	fromSlash := filepath.FromSlash(cleaned)
	if !filepath.IsLocal(fromSlash) {
		return "", fmt.Errorf("invalid path %q", rel)
	}
	abs := filepath.Join(d.cfg.Dir, fromSlash)
	relCheck, err := filepath.Rel(d.cfg.Dir, abs)
	if err != nil || !filepath.IsLocal(relCheck) {
		return "", fmt.Errorf("path escapes root: %q", rel)
	}
	return abs, nil
}

// pathCleanSlash is path.Clean for slash-separated relative paths.
func pathCleanSlash(p string) string {
	if p == "" {
		return ""
	}
	// Use filepath.Clean on FromSlash then back, so "a/../b" becomes "b".
	return filepath.ToSlash(filepath.Clean(filepath.FromSlash(p)))
}

func (d *Daemon) checkFileSize(path string, size int64) error {
	if size > d.cfg.MaxFileBytes {
		return fmt.Errorf("file %s too large: %d > max %d", path, size, d.cfg.MaxFileBytes)
	}
	return nil
}

// readFileLimited reads a file after checking size against MaxFileBytes.
func (d *Daemon) readFileLimited(abs, rel string) ([]byte, os.FileInfo, error) {
	fi, err := os.Stat(abs)
	if err != nil {
		return nil, nil, err
	}
	if err := d.checkFileSize(rel, fi.Size()); err != nil {
		return nil, nil, err
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return nil, nil, err
	}
	return data, fi, nil
}

// serveEntry returns a live index entry after validating path. It refuses the
// serve (without rehashing) if size/mtime have drifted from the index since the
// last scan, so clients fail fast instead of transferring stale bytes.
func (d *Daemon) serveEntry(ctx context.Context, rel string) (index.Entry, string, error) {
	if _, err := d.absPath(rel); err != nil {
		return index.Entry{}, "", err
	}
	// Normalize path key to cleaned slash form for index lookup.
	rel = pathCleanSlash(filepath.ToSlash(rel))
	e, ok := d.idx.Get(rel)
	if !ok || e.Deleted {
		return index.Entry{}, "", fmt.Errorf("file not found: %s", rel)
	}
	abs, err := d.absPath(rel)
	if err != nil {
		return index.Entry{}, "", err
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return index.Entry{}, "", fmt.Errorf("stat %s: %w", rel, err)
	}
	if err := d.checkFileSize(rel, fi.Size()); err != nil {
		return index.Entry{}, "", err
	}
	if fi.Size() != e.Size || !fi.ModTime().Equal(e.ModTime) {
		// Disk drifted since last scan; refuse stale serve so clients do not
		// transfer bytes that fail hash verification.
		return index.Entry{}, "", fmt.Errorf("file %s changed since last scan; try again after reconcile", rel)
	}
	if err := ctx.Err(); err != nil {
		return index.Entry{}, "", err
	}
	return e, abs, nil
}

func (d *Daemon) serveFile(ctx context.Context, conn net.Conn, rel string) error {
	e, abs, err := d.serveEntry(ctx, rel)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return fmt.Errorf("read %s: %w", rel, err)
	}
	return proto.Encode(conn, proto.NewFileData(pathCleanSlash(filepath.ToSlash(rel)), e, data))
}

func (d *Daemon) serveSig(ctx context.Context, conn net.Conn, rel string, blockSize int) error {
	if blockSize <= 0 {
		blockSize = d.cfg.BlockSize
	}
	e, abs, err := d.serveEntry(ctx, rel)
	if err != nil {
		return err
	}
	_ = e
	f, err := os.Open(abs)
	if err != nil {
		return err
	}
	defer f.Close()
	sig, err := delta.Sign(f, blockSize)
	if err != nil {
		return err
	}
	raw, err := delta.MarshalSignature(sig)
	if err != nil {
		return err
	}
	return proto.Encode(conn, proto.NewSig(pathCleanSlash(filepath.ToSlash(rel)), blockSize, raw))
}

func (d *Daemon) serveDelta(ctx context.Context, conn net.Conn, rel, wantHash string, blockSize int, sigRaw []byte) error {
	e, abs, err := d.serveEntry(ctx, rel)
	if err != nil {
		return err
	}
	if wantHash != "" && e.Hash != wantHash {
		d.log.Debug("delta hash mismatch", "path", rel, "want", wantHash, "have", e.Hash)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return err
	}
	var sig *delta.Signature
	if len(sigRaw) > 0 {
		sig, err = delta.UnmarshalSignature(sigRaw)
		if err != nil {
			return fmt.Errorf("bad signature: %w", err)
		}
		// Prefer the signature's embedded block size; error if header disagrees.
		if blockSize > 0 && sig.BlockSize > 0 && blockSize != sig.BlockSize {
			return fmt.Errorf("block size mismatch: header %d signature %d", blockSize, sig.BlockSize)
		}
	}
	del, err := delta.EncodeBytes(data, sig)
	if err != nil {
		return err
	}
	raw, err := delta.MarshalDelta(del)
	if err != nil {
		return err
	}
	return proto.Encode(conn, proto.NewDelta(pathCleanSlash(filepath.ToSlash(rel)), e, raw))
}

func (d *Daemon) reconcile(ctx context.Context) error {
	d.syncMu.Lock()
	defer d.syncMu.Unlock()

	res, err := scan.Scan(ctx, d.cfg.Dir, d.idx, nil)
	if err != nil {
		return err
	}
	applied := 0
	if len(res.Changes) > 0 {
		for _, c := range res.Changes {
			d.log.Info("local change", "kind", c.Kind.String(), "path", c.Path)
		}
		applied = scan.Apply(d.idx, res)
	}

	if n := d.idx.GCTombstones(time.Now(), d.cfg.TombstoneTTL); n > 0 {
		d.log.Info("gc tombstones", "removed", n)
		applied += n
	}

	if applied > 0 || d.appliesSinceSave > 0 {
		if err := d.idx.Save(); err != nil {
			return fmt.Errorf("save index: %w", err)
		}
		d.appliesSinceSave = 0
	}
	return nil
}

func (d *Daemon) syncPeers(ctx context.Context) {
	peers, err := d.listPeers(ctx)
	if err != nil {
		d.log.Debug("list peers", "err", err)
		return
	}
	for _, addr := range peers {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := d.syncWith(ctx, addr); err != nil {
			d.log.Debug("sync peer", "addr", addr, "err", err)
			continue
		}
		d.mu.Lock()
		d.peerSeen[addr] = time.Now()
		d.mu.Unlock()
	}
}

func (d *Daemon) syncWith(ctx context.Context, addr string) error {
	conn, err := d.dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	d.setConnDeadline(conn, ctx)

	if err := proto.Encode(conn, proto.NewHello(d.nodeID)); err != nil {
		return err
	}
	resp, err := proto.Decode(conn)
	if err != nil {
		return fmt.Errorf("hello response: %w", err)
	}
	if resp.Header.Type == proto.TypeError {
		return fmt.Errorf("hello error: %s", resp.Header.Error)
	}
	if resp.Header.Type != proto.TypeHelloOK {
		return fmt.Errorf("unexpected hello response %q", resp.Header.Type)
	}
	if resp.Header.NodeID == d.nodeID {
		return fmt.Errorf("connected to self")
	}

	if err := proto.Encode(conn, proto.NewManifestReq()); err != nil {
		return err
	}
	man, err := proto.Decode(conn)
	if err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if man.Header.Type == proto.TypeError {
		return fmt.Errorf("manifest error: %s", man.Header.Error)
	}
	if man.Header.Type != proto.TypeManifest {
		return fmt.Errorf("expected manifest, got %q", man.Header.Type)
	}

	changed := false
	var transportErr error
	for _, remote := range man.Header.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		d.setConnDeadline(conn, ctx)
		did, err := d.applyRemote(ctx, conn, remote)
		if err != nil {
			// Per-entry logical errors: continue. Transport/decode failures abort.
			if isTransportErr(err) {
				transportErr = err
				d.log.Warn("peer transport error, aborting peer sync", "addr", addr, "err", err)
				break
			}
			d.log.Warn("apply remote entry", "path", remote.Path, "err", err)
			continue
		}
		if did {
			changed = true
		}
	}
	if changed {
		d.syncMu.Lock()
		if err := d.idx.Save(); err != nil {
			d.syncMu.Unlock()
			return fmt.Errorf("save index: %w", err)
		}
		d.appliesSinceSave = 0
		d.syncMu.Unlock()
	}
	if transportErr != nil {
		return transportErr
	}
	d.log.Info("synced peer", "addr", addr, "remote_node", resp.Header.NodeID, "entries", len(man.Header.Entries))
	return nil
}

// errTransport marks framing / connection failures that invalidate the session.
// errPeerLogical marks TypeError responses and other per-entry logical failures
// that should not abort the rest of a peer sync.
var (
	errTransport   = errors.New("transport error")
	errPeerLogical = errors.New("peer logical error")
)

func isTransportErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errTransport) || errors.Is(err, io.EOF)
}

func peerLogical(msg string) error {
	return fmt.Errorf("%w: %s", errPeerLogical, msg)
}

// applyRemote reconciles one remote manifest entry. Returns true if local state changed.
func (d *Daemon) applyRemote(ctx context.Context, conn net.Conn, remote index.ManifestEntry) (bool, error) {
	// Validate path before any index mutation.
	if _, err := d.absPath(remote.Path); err != nil {
		return false, fmt.Errorf("reject path: %w", err)
	}
	remote.Path = pathCleanSlash(filepath.ToSlash(remote.Path))

	d.syncMu.Lock()
	defer d.syncMu.Unlock()

	// Re-check cancellation under lock so we do not start a long pull after cancel.
	if err := ctx.Err(); err != nil {
		return false, err
	}

	local, hasLocal := d.idx.Get(remote.Path)
	re := index.EntryFromManifest(remote)

	if remote.Deleted {
		if !hasLocal {
			if d.idx.SetIfWins(re) {
				d.appliesSinceSave++
				return true, nil
			}
			return false, nil
		}
		if local.Deleted {
			if d.idx.SetIfWins(re) {
				d.appliesSinceSave++
				return true, nil
			}
			return false, nil
		}
		// Remote deleted, we have live file: delete if remote wins LWW.
		if !index.Wins(local, re) {
			return false, nil
		}
		abs, err := d.absPath(remote.Path)
		if err != nil {
			return false, err
		}
		if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
			return false, fmt.Errorf("delete %s: %w", remote.Path, err)
		}
		// Re-check LWW before commit in case another apply raced (we hold syncMu).
		if !d.idx.SetIfWins(re) {
			return false, nil
		}
		d.appliesSinceSave++
		d.log.Info("deleted from peer", "path", remote.Path)
		d.maybeSaveLocked()
		return true, nil
	}

	// Remote has live file — require a content hash for integrity.
	if remote.Hash == "" {
		return false, peerLogical(fmt.Sprintf("live entry %q missing content hash", remote.Path))
	}

	if hasLocal && !local.Deleted && local.Hash == remote.Hash {
		// Same content hash — adopt remote metadata (mode/mtime) when remote wins LWW
		// and metadata actually differs. Higher UpdatedAt always qualifies; equal
		// UpdatedAt uses Wins total order (mode, then ModTime).
		// Zero remote ModTime is ignored for "differs" (keep local/disk mtime).
		// Same-second mtimes are treated as equal so coarse FS truncation after
		// Chtimes does not re-adopt every sync.
		mtimeDiffers := !remote.ModTime.IsZero() && mtimesDiffer(remote.ModTime, local.ModTime)
		metaDiffers := remote.Mode != local.Mode || mtimeDiffers ||
			remote.UpdatedAt.After(local.UpdatedAt)
		needMeta := metaDiffers && index.Wins(local, re)
		if !needMeta {
			return false, nil
		}
		metaAbs, err := d.absPath(remote.Path)
		if err != nil {
			return false, err
		}
		mode := remote.Mode
		if mode == 0 {
			mode = 0o644
		}
		// If the file is missing, fall through to a content pull instead of committing metadata-only.
		if _, err := os.Stat(metaAbs); err != nil {
			if !os.IsNotExist(err) {
				return false, fmt.Errorf("stat %s: %w", remote.Path, err)
			}
			d.log.Debug("metadata adopt: local file missing, pulling content", "path", remote.Path)
			// Fall through to content pull below.
		} else {
			// Transactional metadata adopt: on partial failure, restore prior mode/mtime
			// so disk stays aligned with the index (avoids scan inventing a local LWW win).
			prevMode := local.Mode
			if prevMode == 0 {
				prevMode = 0o644
			}
			prevMT := local.ModTime

			if !remote.ModTime.IsZero() {
				if err := os.Chtimes(metaAbs, remote.ModTime, remote.ModTime); err != nil {
					return false, fmt.Errorf("chtimes %s: %w", remote.Path, err)
				}
			}
			if err := os.Chmod(metaAbs, mode); err != nil {
				// Best-effort rollback of mtime applied above.
				if !remote.ModTime.IsZero() && !prevMT.IsZero() {
					if rerr := os.Chtimes(metaAbs, prevMT, prevMT); rerr != nil {
						d.log.Warn("rollback mtime after chmod failure", "path", remote.Path, "err", rerr)
					}
				}
				if rerr := os.Chmod(metaAbs, prevMode); rerr != nil {
					d.log.Warn("rollback mode after chmod failure", "path", remote.Path, "err", rerr)
				}
				return false, fmt.Errorf("chmod %s: %w", remote.Path, err)
			}

			// Store filesystem-observed mtime/mode so scan equality matches disk
			// (some FS truncate timestamps; Chtimes success ≠ Stat equality).
			actualMode, actualMT, err := diskMeta(metaAbs, mode, remote.ModTime, local.ModTime)
			if err != nil {
				// Ops succeeded; still commit with best-effort values rather than
				// rolling back a successful metadata write.
				d.log.Warn("stat after metadata adopt", "path", remote.Path, "err", err)
			}
			local.UpdatedAt = remote.UpdatedAt
			local.ModTime = actualMT
			local.Mode = actualMode
			d.idx.Set(local)
			d.appliesSinceSave++
			return true, nil
		}
	} else if hasLocal && !index.Wins(local, re) {
		return false, nil
	}

	abs, err := d.absPath(remote.Path)
	if err != nil {
		return false, err
	}

	// Network I/O while holding syncMu (see field comment): serializes applies.
	var data []byte
	if hasLocal && !local.Deleted {
		data, err = d.pullDelta(ctx, conn, remote, abs)
		if err != nil {
			if isTransportErr(err) {
				d.log.Debug("delta pull failed, aborting peer apply (transport)", "path", remote.Path, "err", err)
				return false, err
			}
			d.log.Debug("delta pull failed, falling back to full", "path", remote.Path, "err", err)
			data, err = d.pullFull(ctx, conn, remote)
		}
	} else {
		data, err = d.pullFull(ctx, conn, remote)
	}
	if err != nil {
		return false, err
	}

	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	if got != remote.Hash {
		return false, peerLogical(fmt.Sprintf("hash mismatch for %s: got %s want %s", remote.Path, got, remote.Hash))
	}
	if err := d.checkFileSize(remote.Path, int64(len(data))); err != nil {
		return false, err
	}

	// Final LWW check after transfer under syncMu — index cannot change under us.
	if cur, ok := d.idx.Get(remote.Path); ok && !index.Wins(cur, re) {
		return false, nil
	}

	mode := remote.Mode
	if mode == 0 {
		mode = 0o644
	}
	if err := atomicfile.WriteFile(abs, data, mode); err != nil {
		return false, err
	}
	// Always commit content after a successful write so scan cannot promote the
	// new bytes under a fresh local UpdatedAt (LWW inversion). Chtimes failure
	// is logged; disk mtime is re-Stat'd and same-hash adopt can retry later.
	if !remote.ModTime.IsZero() {
		if err := os.Chtimes(abs, remote.ModTime, remote.ModTime); err != nil {
			d.log.Warn("chtimes after pull; committing content with disk mtime", "path", remote.Path, "err", err)
		}
	}
	// Prefer filesystem-observed mtime; never install a zero ModTime from a partial peer entry.
	fallbackMT := remote.ModTime
	if fallbackMT.IsZero() && hasLocal && !local.Deleted {
		fallbackMT = local.ModTime
	}
	actualMode, actualMT, err := diskMeta(abs, mode, remote.ModTime, fallbackMT)
	if err != nil {
		d.log.Warn("stat after pull", "path", remote.Path, "err", err)
	}

	entry := re
	entry.Hash = got
	entry.Size = int64(len(data))
	entry.Mode = actualMode
	entry.ModTime = actualMT
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = time.Now()
	}
	// Under syncMu we already re-validated LWW; Set cannot lose a concurrent race.
	// Avoid SetIfWins here: identical-content Wins false would skip the commit after a write.
	d.idx.Set(entry)
	d.appliesSinceSave++
	d.maybeSaveLocked()
	d.log.Info("pulled file", "path", remote.Path, "size", len(data), "hash", got[:min(12, len(got))])
	return true, nil
}

// diskMeta returns mode and mtime actually present on disk after a metadata or
// content apply. When remoteMT is zero, keeps fallbackMT (local/disk) instead of
// committing a zero index mtime. On Stat failure, returns modeWant and a non-zero
// mtime preference of remoteMT then fallbackMT.
func diskMeta(abs string, modeWant os.FileMode, remoteMT, fallbackMT time.Time) (os.FileMode, time.Time, error) {
	fi, err := os.Stat(abs)
	if err != nil {
		mt := remoteMT
		if mt.IsZero() {
			mt = fallbackMT
		}
		return modeWant, mt, err
	}
	mode := fi.Mode().Perm()
	if mode == 0 {
		mode = modeWant
	}
	mt := fi.ModTime()
	if remoteMT.IsZero() {
		// Do not let a partial peer entry install zero; prefer prior known mtime
		// only when Stat returned zero (exotic); otherwise disk is authoritative.
		if mt.IsZero() && !fallbackMT.IsZero() {
			mt = fallbackMT
		}
	}
	return mode, mt, nil
}

// mtimesDiffer reports whether two mtimes should trigger metadata adopt.
// Equal times, or both non-zero and same Unix second (coarse FS truncation
// after Chtimes), are treated as not differing.
func mtimesDiffer(a, b time.Time) bool {
	if a.Equal(b) {
		return false
	}
	if !a.IsZero() && !b.IsZero() && a.Unix() == b.Unix() {
		return false
	}
	return true
}

// maybeSaveLocked persists the index every N applies to limit crash windows.
// Caller must hold syncMu.
func (d *Daemon) maybeSaveLocked() {
	const every = 8
	if d.appliesSinceSave >= every {
		if err := d.idx.Save(); err != nil {
			d.log.Error("mid-sync index save", "err", err)
			return
		}
		d.appliesSinceSave = 0
	}
}

func (d *Daemon) pullFull(ctx context.Context, conn net.Conn, remote index.ManifestEntry) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := proto.Encode(conn, proto.NewFileReq(remote.Path, remote.Hash)); err != nil {
		return nil, fmt.Errorf("%w: encode file_req: %w", errTransport, err)
	}
	msg, err := proto.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: decode file response: %w", errTransport, err)
	}
	if msg.Header.Type == proto.TypeError {
		return nil, peerLogical(msg.Header.Error)
	}
	if msg.Header.Type != proto.TypeFileData {
		return nil, fmt.Errorf("%w: expected file_data, got %q", errTransport, msg.Header.Type)
	}
	return msg.Payload, nil
}

func (d *Daemon) pullDelta(ctx context.Context, conn net.Conn, remote index.ManifestEntry, abs string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	basis, fi, err := d.readFileLimited(abs, remote.Path)
	if err != nil {
		// Local basis missing/unreadable is a logical failure for this path.
		return nil, peerLogical(err.Error())
	}
	_ = fi
	sig, err := delta.SignBytes(basis, d.cfg.BlockSize)
	if err != nil {
		return nil, peerLogical(err.Error())
	}
	sigRaw, err := delta.MarshalSignature(sig)
	if err != nil {
		return nil, peerLogical(err.Error())
	}
	if err := proto.Encode(conn, proto.NewDeltaReq(remote.Path, remote.Hash, d.cfg.BlockSize, sigRaw)); err != nil {
		return nil, fmt.Errorf("%w: encode delta_req: %w", errTransport, err)
	}
	msg, err := proto.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: decode delta response: %w", errTransport, err)
	}
	if msg.Header.Type == proto.TypeError {
		return nil, peerLogical(msg.Header.Error)
	}
	if msg.Header.Type != proto.TypeDelta {
		return nil, fmt.Errorf("%w: expected delta, got %q", errTransport, msg.Header.Type)
	}
	del, err := delta.UnmarshalDelta(msg.Payload)
	if err != nil {
		return nil, peerLogical(fmt.Sprintf("bad delta: %v", err))
	}
	out, err := delta.Apply(basis, del)
	if err != nil {
		return nil, peerLogical(fmt.Sprintf("apply delta: %v", err))
	}
	return out, nil
}
