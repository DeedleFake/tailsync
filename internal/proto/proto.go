// Package proto defines the tailsync peer wire protocol (length-prefixed JSON
// messages with optional binary payloads).
package proto

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"deedles.dev/tailsync/internal/index"
)

// Protocol version negotiated in Hello (connection-scoped only).
// v2: connection-scoped Hello + AlreadyConnected; one-off op streams without
// per-stream Hello; Ping handled on the session accept path. Wire-incompatible
// with v1 (session model and stream framing assumptions differ).
// v1: early stream-per-op Hello experiments (no longer supported).
const Version = 2

// MaxMessageSize caps a single framed message (headers + payload).
const MaxMessageSize = 64 << 20 // 64 MiB

// Type identifies a message.
type Type string

const (
	// Connection handshake (first stream only; not repeated on op streams).
	TypeHello   Type = "hello"
	TypeHelloOK Type = "hello_ok"
	// TypeAlreadyConnected rejects a redundant connection when a healthy
	// session with this peer already exists (or lost a simultaneous-dial race).
	TypeAlreadyConnected Type = "already_connected"
	TypeManifest         Type = "manifest"
	TypeManifestReq      Type = "manifest_req"
	TypeFileReq          Type = "file_req"
	TypeFileData         Type = "file_data"
	// Delta path: client sends its local block signature; server returns a delta.
	// (Older TypeSigReq/TypeSig half of the protocol was removed; clients never
	// requested the peer's signature — only TypeDeltaReq is used.)
	TypeDeltaReq Type = "delta_req"
	TypeDelta    Type = "delta"
	// TypeSyncDone is retained for compatibility but unused with one-off streams
	// (each op stream closes when done).
	TypeSyncDone Type = "sync_done"
	// TypeNotify is a soft wake-up: the sender has peer-visible index changes.
	// Payload is optional path/hash/updated_at hints (Header.Entries); never
	// authoritative bytes. Receiver should schedule a pull; correctness does
	// not depend on notify delivery.
	TypeNotify Type = "notify"
	// TypeNotifyOK acknowledges a notify (optional; sender may close without it).
	TypeNotifyOK Type = "notify_ok"
	TypeError    Type = "error"
	TypePing     Type = "ping"
	TypePong     Type = "pong"
)

// Header is the JSON envelope for every message. Binary payload follows when
// PayloadLen > 0 (FileData, Delta).
type Header struct {
	Type       Type `json:"type"`
	PayloadLen int  `json:"payload_len,omitempty"`
	// Common fields (set per type as needed).
	NodeID  string `json:"node_id,omitempty"`
	Version int    `json:"version,omitempty"`
	// Port is the sender's tailsync listen port (Hello/HelloOK), used by peers
	// to build a dial-back address when the QUIC remote port is ephemeral.
	Port  int    `json:"port,omitempty"`
	Path  string `json:"path,omitempty"`
	Hash  string `json:"hash,omitempty"`
	Size  int64  `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	// File metadata for transfers.
	ModTime   time.Time `json:"mod_time"`
	Mode      uint32    `json:"mode,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
	Deleted   bool      `json:"deleted,omitempty"`
	// Manifest entries (for TypeManifest).
	Entries []index.ManifestEntry `json:"entries,omitempty"`
	// BlockSize for signatures / deltas.
	BlockSize int `json:"block_size,omitempty"`
}

// Message is a decoded header plus optional binary payload.
type Message struct {
	Header  Header
	Payload []byte
}

// Encode writes a framed message to w:
//
//	u32le headerLen | headerJSON | payload
func Encode(w io.Writer, msg Message) error {
	h := msg.Header
	h.PayloadLen = len(msg.Payload)
	jb, err := json.Marshal(h)
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	}
	total := 4 + len(jb) + len(msg.Payload)
	if total > MaxMessageSize {
		return fmt.Errorf("message too large: %d", total)
	}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(jb)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write header len: %w", err)
	}
	if _, err := w.Write(jb); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if len(msg.Payload) > 0 {
		if _, err := w.Write(msg.Payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}
	return nil
}

// Decode reads one framed message from r.
// Total framed size (4 + header + payload) must not exceed MaxMessageSize,
// matching Encode.
func Decode(r io.Reader) (Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Message{}, err
	}
	hLen := binary.LittleEndian.Uint32(lenBuf[:])
	if hLen == 0 || int(hLen) > MaxMessageSize-4 {
		return Message{}, fmt.Errorf("invalid header length %d", hLen)
	}
	hb := make([]byte, hLen)
	if _, err := io.ReadFull(r, hb); err != nil {
		return Message{}, fmt.Errorf("read header: %w", err)
	}
	var h Header
	if err := json.Unmarshal(hb, &h); err != nil {
		return Message{}, fmt.Errorf("unmarshal header: %w", err)
	}
	if h.PayloadLen < 0 {
		return Message{}, fmt.Errorf("negative payload length")
	}
	total := 4 + int(hLen) + h.PayloadLen
	if total > MaxMessageSize {
		return Message{}, fmt.Errorf("message too large: %d", total)
	}
	var payload []byte
	if h.PayloadLen > 0 {
		payload = make([]byte, h.PayloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return Message{}, fmt.Errorf("read payload: %w", err)
		}
	}
	return Message{Header: h, Payload: payload}, nil
}

// NewHello builds a hello message. port is the sender's listen port for dial-back.
func NewHello(nodeID string, port int) Message {
	return Message{Header: Header{
		Type:    TypeHello,
		NodeID:  nodeID,
		Version: Version,
		Port:    port,
	}}
}

// NewHelloOK builds a hello response. port is the responder's listen port.
func NewHelloOK(nodeID string, port int) Message {
	return Message{Header: Header{
		Type:    TypeHelloOK,
		NodeID:  nodeID,
		Version: Version,
		Port:    port,
	}}
}

// NewAlreadyConnected rejects a redundant peer connection during handshake.
func NewAlreadyConnected(nodeID string, port int) Message {
	return Message{Header: Header{
		Type:    TypeAlreadyConnected,
		NodeID:  nodeID,
		Version: Version,
		Port:    port,
	}}
}

// NewManifest builds a manifest message from index entries.
func NewManifest(entries []index.ManifestEntry) Message {
	return Message{Header: Header{
		Type:    TypeManifest,
		Entries: entries,
	}}
}

// NewManifestReq requests the peer's manifest.
func NewManifestReq() Message {
	return Message{Header: Header{Type: TypeManifestReq}}
}

// NewFileReq requests a full file by path (expected hash optional).
func NewFileReq(path, hash string) Message {
	return Message{Header: Header{
		Type: TypeFileReq,
		Path: path,
		Hash: hash,
	}}
}

// NewFileData sends full file contents.
func NewFileData(path string, e index.Entry, data []byte) Message {
	return Message{
		Header: Header{
			Type:      TypeFileData,
			Path:      path,
			Hash:      e.Hash,
			Size:      e.Size,
			ModTime:   e.ModTime,
			Mode:      uint32(e.Mode),
			UpdatedAt: e.UpdatedAt,
		},
		Payload: data,
	}
}

// NewDeltaReq asks the peer to encode a delta for path using our signature.
func NewDeltaReq(path, wantHash string, blockSize int, sig []byte) Message {
	return Message{
		Header: Header{
			Type:      TypeDeltaReq,
			Path:      path,
			Hash:      wantHash,
			BlockSize: blockSize,
		},
		Payload: sig,
	}
}

// NewDelta sends a delta payload for path.
func NewDelta(path string, e index.Entry, delta []byte) Message {
	return Message{
		Header: Header{
			Type:      TypeDelta,
			Path:      path,
			Hash:      e.Hash,
			Size:      e.Size,
			ModTime:   e.ModTime,
			Mode:      uint32(e.Mode),
			UpdatedAt: e.UpdatedAt,
		},
		Payload: delta,
	}
}

// NewError builds an error message.
func NewError(err string) Message {
	return Message{Header: Header{Type: TypeError, Error: err}}
}

// NewSyncDone marks the end of a pull phase.
func NewSyncDone() Message {
	return Message{Header: Header{Type: TypeSyncDone}}
}

// NewNotify builds a soft wake-up with optional content hints (path/hash/updated_at).
// Hints are not authoritative; the receiver must pull a manifest to apply state.
// On a persistent session stream, nodeID/port/version are optional (connection
// Hello already identified the peer); they may still be set for logging.
func NewNotify(nodeID string, port int, hints []index.ManifestEntry) Message {
	return Message{Header: Header{
		Type:    TypeNotify,
		NodeID:  nodeID,
		Version: Version,
		Port:    port,
		Entries: hints,
	}}
}

// NewNotifyOK acknowledges a notify.
func NewNotifyOK(nodeID string, port int) Message {
	return Message{Header: Header{
		Type:    TypeNotifyOK,
		NodeID:  nodeID,
		Version: Version,
		Port:    port,
	}}
}

// NewPing builds an application-level heartbeat request.
func NewPing() Message {
	return Message{Header: Header{Type: TypePing}}
}

// NewPong builds an application-level heartbeat response.
func NewPong() Message {
	return Message{Header: Header{Type: TypePong}}
}
