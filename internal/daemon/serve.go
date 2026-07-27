package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"deedles.dev/tailsync/internal/delta"
	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/proto"
)

// errUnexpectedMsgType is returned for protocol message types this node does not
// handle. It is fatal for the session (confused peer).
var errUnexpectedMsgType = errors.New("unexpected message type")

func (d *Daemon) setConnDeadline(conn net.Conn, ctx context.Context) {
	deadline := time.Now().Add(5 * time.Minute)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)
}

// handleConn serves an inbound session (listener side):
//
//  1. Hello / HelloOK (learn remote into hot set)
//     2a. TypeNotify → schedule pull, NotifyOK, close
//     2b. ManifestReq / FileReq / DeltaReq until SyncDone (serve pull), close
//
// Sessions are pull- or notify-oriented; reverse-pull after SyncDone is not used.
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
	remoteNode := msg.Header.NodeID
	remotePort := msg.Header.Port
	if err := proto.Encode(conn, proto.NewHelloOK(d.nodeID, d.cfg.Port)); err != nil {
		return
	}
	// Learn dial-back using peer's advertised listen port (not the ephemeral TCP port).
	if remoteNode != "" && remoteNode != d.nodeID {
		if dial := dialBackAddr(remote, remotePort, d.cfg.Port); dial != "" {
			d.peers.remember(remoteNode, dial)
		}
	}

	// Serve until notify, SyncDone, or EOF.
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
		if msg.Header.Type == proto.TypeSyncDone {
			d.log.Info("inbound pull served", "remote", remote, "remote_node", remoteNode)
			return
		}
		if msg.Header.Type == proto.TypeNotify {
			// Prefer node id from hello; header may also carry it.
			n := remoteNode
			if msg.Header.NodeID != "" {
				n = msg.Header.NodeID
			}
			port := remotePort
			if msg.Header.Port > 0 {
				port = msg.Header.Port
			}
			d.onNotify(n, remote, port, msg.Header.Entries)
			_ = proto.Encode(conn, proto.NewNotifyOK(d.nodeID, d.cfg.Port))
			d.log.Debug("inbound notify", "remote", remote, "remote_node", n, "hints", len(msg.Header.Entries))
			return
		}
		if err := d.serveMsg(ctx, conn, msg); err != nil {
			d.log.Debug("serve message", "remote", remote, "type", msg.Header.Type, "err", err)
			if encodeErr := proto.Encode(conn, proto.NewError(err.Error())); encodeErr != nil {
				return
			}
			// Keep session open for not-found / validation errors; close on
			// unexpected protocol types so a confused peer does not loop.
			if msg.Header.Type == "" || errors.Is(err, errUnexpectedMsgType) {
				return
			}
		}
	}
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
