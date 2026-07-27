package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"deedles.dev/tailsync/internal/delta"
	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/peer"
	"deedles.dev/tailsync/internal/proto"
)

// errUnexpectedMsgType is returned for protocol message types this node does not
// handle. It is fatal for the stream (confused peer).
var errUnexpectedMsgType = errors.New("unexpected message type")

func (d *Daemon) setConnDeadline(conn net.Conn, ctx context.Context) {
	deadline := time.Now().Add(5 * time.Minute)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)
}

// onPeerStream is the peer.Manager callback for inbound application streams.
// Hello is connection-scoped (handled by peer package); first is already decoded
// (Ping is handled in the peer package and never reaches here).
// Invoked from a per-stream goroutine in the peer session.
//
// streamWG here is intentional and distinct from Session.streamWG: the session
// Wait drains dispatch; this Wait (after mesh close) keeps Root/index live for
// finishing serve work.
func (d *Daemon) onPeerStream(ctx context.Context, s *peer.Session, first proto.Message, stream net.Conn) {
	d.streamWG.Add(1)
	defer d.streamWG.Done()
	d.handleStream(ctx, s, first, stream)
}

// handleStream serves one inbound op stream on an established peer session.
//
// first defines the op: Notify, ManifestReq, FileReq, DeltaReq (and optionally
// Ping if a peer package path is bypassed in tests).
//
// Closes conn on return. When invoked via OnStream, the peer package also
// closes after the handler returns; double-close is safe (idempotent).
func (d *Daemon) handleStream(ctx context.Context, s *peer.Session, first proto.Message, conn net.Conn) {
	defer conn.Close()
	d.setConnDeadline(conn, ctx)
	// Trust session identity only (Hello NodeID), never message header overrides.
	remote := ""
	if s != nil {
		remote = s.NodeID()
	}

	switch first.Header.Type {
	case proto.TypeNotify:
		d.onNotify(remote, first.Header.Entries)
		_ = proto.Encode(conn, proto.NewNotifyOK(d.nodeID, d.cfg.Port))
		d.log.Debug("inbound notify", "peer", remote, "hints", len(first.Header.Entries))
		return

	case proto.TypePing:
		_ = proto.Encode(conn, proto.NewPong())
		return

	case proto.TypeManifestReq, proto.TypeFileReq, proto.TypeDeltaReq:
		// Cap concurrent heavy serve ops (same budget as pull streams).
		if err := d.acquireServeSlot(ctx); err != nil {
			_ = proto.Encode(conn, proto.NewError("server busy"))
			return
		}
		defer d.releaseServeSlot()
		if err := d.serveMsg(ctx, conn, first); err != nil {
			d.log.Debug("serve message", "peer", remote, "type", first.Header.Type, "err", err)
			_ = proto.Encode(conn, proto.NewError(err.Error()))
		}
		return

	default:
		d.log.Debug("unexpected stream op", "peer", remote, "type", first.Header.Type)
		_ = proto.Encode(conn, proto.NewError(fmt.Sprintf("unexpected message type %q", first.Header.Type)))
	}
}

// acquireServeSlot limits concurrent inbound serve work (manifest/file/delta).
func (d *Daemon) acquireServeSlot(ctx context.Context) error {
	if d.serveSem == nil {
		return nil
	}
	select {
	case d.serveSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Daemon) releaseServeSlot() {
	if d.serveSem == nil {
		return
	}
	select {
	case <-d.serveSem:
	default:
	}
}

func (d *Daemon) serveMsg(ctx context.Context, conn net.Conn, msg proto.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch msg.Header.Type {
	case proto.TypePing:
		return proto.Encode(conn, proto.NewPong())
	case proto.TypeManifestReq:
		return proto.Encode(conn, proto.NewManifest(d.idx.Manifest()))
	case proto.TypeFileReq:
		return d.serveFile(ctx, conn, msg.Header.Path)
	case proto.TypeDeltaReq:
		return d.serveDelta(ctx, conn, msg.Header.Path, msg.Header.Hash, msg.Header.BlockSize, msg.Payload)
	default:
		return fmt.Errorf("%w %q", errUnexpectedMsgType, msg.Header.Type)
	}
}

func (d *Daemon) checkFileSize(path string, size int64) error {
	if size > d.cfg.MaxFileBytes {
		return fmt.Errorf("file %s too large: %d > max %d", path, size, d.cfg.MaxFileBytes)
	}
	return nil
}

// readFileLimited reads a file under the sync root after checking size against MaxFileBytes.
func (d *Daemon) readFileLimited(rel string) ([]byte, os.FileInfo, error) {
	fi, err := d.root.Stat(rel)
	if err != nil {
		return nil, nil, err
	}
	if err := d.checkFileSize(rel, fi.Size()); err != nil {
		return nil, nil, err
	}
	data, err := d.root.ReadFile(rel)
	if err != nil {
		return nil, nil, err
	}
	return data, fi, nil
}

// serveEntry returns a live index entry after validating path. It refuses the
// serve (without rehashing) if size/mtime have drifted from the index since the
// last scan, so clients fail fast instead of transferring stale bytes.
// The returned path is the cleaned relative form used for Root I/O.
func (d *Daemon) serveEntry(ctx context.Context, rel string) (index.Entry, string, error) {
	rel, err := d.relPath(rel)
	if err != nil {
		return index.Entry{}, "", err
	}
	e, ok := d.idx.Get(rel)
	if !ok || e.Deleted {
		return index.Entry{}, "", fmt.Errorf("file not found: %s", rel)
	}
	fi, err := d.root.Stat(rel)
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
	return e, rel, nil
}

func (d *Daemon) serveFile(ctx context.Context, conn net.Conn, rel string) error {
	e, rel, err := d.serveEntry(ctx, rel)
	if err != nil {
		return err
	}
	data, err := d.root.ReadFile(rel)
	if err != nil {
		return fmt.Errorf("read %s: %w", rel, err)
	}
	return proto.Encode(conn, proto.NewFileData(rel, e, data))
}

// Block size bounds for untrusted DeltaReq headers (CPU/memory DoS).
const (
	minDeltaBlockSize = 256
	maxDeltaBlockSize = 1 << 20 // 1 MiB
)

func (d *Daemon) serveDelta(ctx context.Context, conn net.Conn, rel string, wantHash string, blockSize int, sigRaw []byte) error {
	e, rel, err := d.serveEntry(ctx, rel)
	if err != nil {
		return err
	}
	if wantHash != "" && e.Hash != wantHash {
		d.log.Debug("delta hash mismatch", "path", rel, "want", wantHash, "have", e.Hash)
	}
	data, err := d.root.ReadFile(rel)
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
		if err := validateBlockSize(sig.BlockSize); err != nil {
			return err
		}
	} else if blockSize > 0 {
		if err := validateBlockSize(blockSize); err != nil {
			return err
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
	return proto.Encode(conn, proto.NewDelta(rel, e, raw))
}

func validateBlockSize(bs int) error {
	if bs < minDeltaBlockSize || bs > maxDeltaBlockSize {
		return fmt.Errorf("block size %d out of range [%d, %d]", bs, minDeltaBlockSize, maxDeltaBlockSize)
	}
	return nil
}
