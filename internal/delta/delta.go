// Package delta implements rsync-style rolling checksums and block matching
// for efficient partial file transfers.
package delta

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
)

// DefaultBlockSize is used when a caller does not specify a block size.
const DefaultBlockSize = 4096

// Signature describes the block layout of a basis file (the version the
// receiver already has).
type Signature struct {
	BlockSize int             `json:"block_size"`
	Blocks    []BlockChecksum `json:"blocks"`
}

// BlockChecksum holds weak (rolling) and strong checksums for one block.
type BlockChecksum struct {
	// Index is the block number in the basis file.
	Index int `json:"index"`
	// Weak is the Adler-32-like rolling checksum.
	Weak uint32 `json:"weak"`
	// Strong is MD5 of the block for match-only candidate confirmation (rsync-like).
	// MD5 is not a security boundary: after apply, the full file is verified with
	// SHA-256 against the manifest hash before commit.
	Strong [md5.Size]byte `json:"strong"`
}

// MaxDecodeOps caps ops/blocks allocated when unmarshalling untrusted wire data.
// Bounded well below MaxMessageSize-scale payloads (each op is at least 1 byte).
const MaxDecodeOps = 16 << 20 // 16M

// MaxOutputSize caps reconstructed file size from a delta (matches typical
// MaxFileBytes / proto framing). Rejects malicious OutputSize before allocate.
const MaxOutputSize = 64 << 20 // 64 MiB

// OpKind is the kind of a delta operation.
type OpKind byte

const (
	// OpLiteral copies raw bytes from the delta stream.
	OpLiteral OpKind = iota
	// OpCopy copies a block from the basis file by block index.
	OpCopy
)

// Op is one instruction in a delta: either a literal run or a basis block copy.
type Op struct {
	Kind OpKind
	// Data is set for OpLiteral.
	Data []byte
	// BlockIndex is set for OpCopy (index into the basis signature).
	BlockIndex int
}

// Delta is a sequence of ops that reconstruct a target file from a basis.
type Delta struct {
	BlockSize  int
	Ops        []Op
	OutputSize int64
}

// weakChecksum computes an Adler-32-style sum over b (a and b 16-bit halves).
// Compatible with rolling updates via Roll.
func weakChecksum(b []byte) uint32 {
	var a, bb uint32
	for _, c := range b {
		a += uint32(c)
		bb += a
	}
	return (bb << 16) | (a & 0xffff)
}

// roll updates a weak checksum when the window slides by one byte:
// remove old byte at left, add new byte at right; n is window length.
func roll(sum uint32, out, in byte, n int) uint32 {
	a := sum & 0xffff
	b := sum >> 16
	a = (a - uint32(out) + uint32(in)) & 0xffff
	b = (b - uint32(n)*uint32(out) + a) & 0xffff
	return (b << 16) | a
}

// Sign reads r and produces a block signature for use as a basis.
func Sign(r io.Reader, blockSize int) (*Signature, error) {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}
	sig := &Signature{BlockSize: blockSize}
	buf := make([]byte, blockSize)
	idx := 0
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			block := buf[:n]
			var strong [md5.Size]byte
			sum := md5.Sum(block)
			strong = sum
			// For a short final block, weak still covers only the real bytes.
			sig.Blocks = append(sig.Blocks, BlockChecksum{
				Index:  idx,
				Weak:   weakChecksum(block),
				Strong: strong,
			})
			idx++
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read for signature: %w", err)
		}
	}
	return sig, nil
}

// SignBytes is Sign over an in-memory buffer.
func SignBytes(data []byte, blockSize int) (*Signature, error) {
	return Sign(bytes.NewReader(data), blockSize)
}

// Encode computes a delta from basis signature to the full target contents in r.
func Encode(r io.Reader, sig *Signature) (*Delta, error) {
	if sig == nil || sig.BlockSize <= 0 {
		// No basis: whole file as one literal.
		data, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		return &Delta{
			BlockSize:  DefaultBlockSize,
			Ops:        []Op{{Kind: OpLiteral, Data: data}},
			OutputSize: int64(len(data)),
		}, nil
	}

	// Index weak → list of block indices for candidate matches.
	weakMap := make(map[uint32][]int, len(sig.Blocks))
	for i, b := range sig.Blocks {
		weakMap[b.Weak] = append(weakMap[b.Weak], i)
	}

	// Read entire target into memory for rolling window (v1 simplicity).
	// Callers should enforce MaxFileBytes before Encode on network paths.
	target, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read target for delta: %w", err)
	}

	bs := sig.BlockSize
	d := &Delta{BlockSize: bs, OutputSize: int64(len(target))}
	var lit []byte

	flushLit := func() {
		if len(lit) > 0 {
			// Copy to avoid retaining large underlying buffer slices long-term.
			cp := make([]byte, len(lit))
			copy(cp, lit)
			d.Ops = append(d.Ops, Op{Kind: OpLiteral, Data: cp})
			lit = lit[:0]
		}
	}

	i := 0
	for i < len(target) {
		// Not enough bytes for a full block: remainder is literal (unless short final basis block).
		if i+bs > len(target) {
			// Try matching a short final block if signature has a short last block.
			matched := false
			if rem := len(target) - i; rem > 0 && len(sig.Blocks) > 0 {
				// Strong-hash the short remainder against any block with matching weak sum
				// (the final basis block is often shorter than BlockSize).
				sum := md5.Sum(target[i:])
				for _, bi := range weakMap[weakChecksum(target[i:])] {
					if sig.Blocks[bi].Strong == sum {
						flushLit()
						d.Ops = append(d.Ops, Op{Kind: OpCopy, BlockIndex: bi})
						i = len(target)
						matched = true
						break
					}
				}
			}
			if !matched {
				lit = append(lit, target[i:]...)
			}
			break
		}

		block := target[i : i+bs]
		weak := weakChecksum(block)
		candidates := weakMap[weak]
		matched := false
		if len(candidates) > 0 {
			strong := md5.Sum(block)
			for _, bi := range candidates {
				if sig.Blocks[bi].Strong == strong {
					flushLit()
					d.Ops = append(d.Ops, Op{Kind: OpCopy, BlockIndex: bi})
					i += bs
					matched = true
					break
				}
			}
		}
		if matched {
			continue
		}

		// No match: emit one byte as literal and slide (classic rsync).
		// Use rolling checksum optimization for subsequent windows.
		lit = append(lit, target[i])
		i++

		// Fast-path rolling search while unmatched.
		if i+bs <= len(target) {
			sum := weakChecksum(target[i : i+bs])
			for i+bs <= len(target) {
				cands := weakMap[sum]
				if len(cands) > 0 {
					strong := md5.Sum(target[i : i+bs])
					hit := false
					for _, bi := range cands {
						if sig.Blocks[bi].Strong == strong {
							flushLit()
							d.Ops = append(d.Ops, Op{Kind: OpCopy, BlockIndex: bi})
							i += bs
							hit = true
							break
						}
					}
					if hit {
						break
					}
				}
				if i+bs >= len(target) {
					break
				}
				out := target[i]
				in := target[i+bs]
				lit = append(lit, out)
				sum = roll(sum, out, in, bs)
				i++
			}
		}
	}
	flushLit()
	return d, nil
}

// EncodeBytes is Encode over an in-memory buffer.
func EncodeBytes(target []byte, sig *Signature) (*Delta, error) {
	return Encode(bytes.NewReader(target), sig)
}

// Apply reconstructs the target file by applying d against basis.
// basis must be the full original file contents that produced the signature.
func Apply(basis []byte, d *Delta) ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("nil delta")
	}
	if d.OutputSize < 0 || d.OutputSize > MaxOutputSize {
		return nil, fmt.Errorf("delta output size %d out of range (max %d)", d.OutputSize, MaxOutputSize)
	}
	bs := d.BlockSize
	if bs <= 0 {
		bs = DefaultBlockSize
	}
	// Cap capacity to declared size only after the range check above.
	out := make([]byte, 0, int(d.OutputSize))
	for _, op := range d.Ops {
		switch op.Kind {
		case OpLiteral:
			out = append(out, op.Data...)
		case OpCopy:
			start := op.BlockIndex * bs
			if start > len(basis) {
				return nil, fmt.Errorf("copy block %d out of range", op.BlockIndex)
			}
			end := min(start+bs, len(basis))
			out = append(out, basis[start:end]...)
		default:
			return nil, fmt.Errorf("unknown op kind %d", op.Kind)
		}
	}
	if d.OutputSize > 0 && int64(len(out)) != d.OutputSize {
		return nil, fmt.Errorf("delta output size mismatch: got %d want %d", len(out), d.OutputSize)
	}
	return out, nil
}

// MarshalDelta serializes a Delta to a compact binary form.
// Format: magic(4) blockSize(u32) outputSize(u64) nOps(u32) then ops.
// OpLiteral: kind(u8) len(u32) data
// OpCopy: kind(u8) blockIndex(u32)
func MarshalDelta(d *Delta) ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("nil delta")
	}
	var buf bytes.Buffer
	buf.Write([]byte{'T', 'S', 'D', '1'})
	_ = binary.Write(&buf, binary.LittleEndian, uint32(d.BlockSize))
	_ = binary.Write(&buf, binary.LittleEndian, uint64(d.OutputSize))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(d.Ops)))
	for _, op := range d.Ops {
		buf.WriteByte(byte(op.Kind))
		switch op.Kind {
		case OpLiteral:
			_ = binary.Write(&buf, binary.LittleEndian, uint32(len(op.Data)))
			buf.Write(op.Data)
		case OpCopy:
			_ = binary.Write(&buf, binary.LittleEndian, uint32(op.BlockIndex))
		default:
			return nil, fmt.Errorf("unknown op kind %d", op.Kind)
		}
	}
	return buf.Bytes(), nil
}

// UnmarshalDelta parses a binary delta produced by MarshalDelta.
func UnmarshalDelta(data []byte) (*Delta, error) {
	if len(data) < 4+4+8+4 {
		return nil, fmt.Errorf("delta too short")
	}
	if !bytes.Equal(data[:4], []byte{'T', 'S', 'D', '1'}) {
		return nil, fmt.Errorf("bad delta magic")
	}
	r := bytes.NewReader(data[4:])
	var blockSize uint32
	var outputSize uint64
	var nOps uint32
	if err := binary.Read(r, binary.LittleEndian, &blockSize); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &outputSize); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &nOps); err != nil {
		return nil, err
	}
	remaining := r.Len()
	// Each op is at least 1 byte (kind); reject absurd counts before allocate.
	if int(nOps) > MaxDecodeOps || int(nOps) > remaining {
		return nil, fmt.Errorf("delta nOps %d exceeds limit (remaining %d)", nOps, remaining)
	}
	if outputSize > MaxOutputSize {
		return nil, fmt.Errorf("delta output size %d exceeds max %d", outputSize, MaxOutputSize)
	}
	d := &Delta{
		BlockSize:  int(blockSize),
		OutputSize: int64(outputSize),
		Ops:        make([]Op, 0, nOps),
	}
	for i := uint32(0); i < nOps; i++ {
		kind, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read op kind: %w", err)
		}
		switch OpKind(kind) {
		case OpLiteral:
			var n uint32
			if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
				return nil, err
			}
			if int(n) > r.Len() {
				return nil, fmt.Errorf("literal length %d exceeds remaining %d", n, r.Len())
			}
			payload := make([]byte, n)
			if _, err := io.ReadFull(r, payload); err != nil {
				return nil, err
			}
			d.Ops = append(d.Ops, Op{Kind: OpLiteral, Data: payload})
		case OpCopy:
			var bi uint32
			if err := binary.Read(r, binary.LittleEndian, &bi); err != nil {
				return nil, err
			}
			d.Ops = append(d.Ops, Op{Kind: OpCopy, BlockIndex: int(bi)})
		default:
			return nil, fmt.Errorf("unknown op kind %d", kind)
		}
	}
	return d, nil
}

// MarshalSignature serializes a Signature for the wire protocol.
func MarshalSignature(sig *Signature) ([]byte, error) {
	if sig == nil {
		return nil, fmt.Errorf("nil signature")
	}
	var buf bytes.Buffer
	buf.Write([]byte{'T', 'S', 'S', '1'})
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sig.BlockSize))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(sig.Blocks)))
	for _, b := range sig.Blocks {
		_ = binary.Write(&buf, binary.LittleEndian, uint32(b.Index))
		_ = binary.Write(&buf, binary.LittleEndian, b.Weak)
		buf.Write(b.Strong[:])
	}
	return buf.Bytes(), nil
}

// UnmarshalSignature parses a signature from MarshalSignature.
func UnmarshalSignature(data []byte) (*Signature, error) {
	if len(data) < 4+4+4 {
		return nil, fmt.Errorf("signature too short")
	}
	if !bytes.Equal(data[:4], []byte{'T', 'S', 'S', '1'}) {
		return nil, fmt.Errorf("bad signature magic")
	}
	r := bytes.NewReader(data[4:])
	var blockSize, nBlocks uint32
	if err := binary.Read(r, binary.LittleEndian, &blockSize); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &nBlocks); err != nil {
		return nil, err
	}
	remaining := r.Len()
	// Each block record is 4+4+16 = 24 bytes.
	const blockRecSize = 4 + 4 + md5.Size
	if int(nBlocks) > MaxDecodeOps || int(nBlocks)*blockRecSize > remaining {
		return nil, fmt.Errorf("signature nBlocks %d exceeds limit (remaining %d)", nBlocks, remaining)
	}
	sig := &Signature{
		BlockSize: int(blockSize),
		Blocks:    make([]BlockChecksum, 0, nBlocks),
	}
	for i := uint32(0); i < nBlocks; i++ {
		var idx, weak uint32
		var strong [md5.Size]byte
		if err := binary.Read(r, binary.LittleEndian, &idx); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.LittleEndian, &weak); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(r, strong[:]); err != nil {
			return nil, err
		}
		sig.Blocks = append(sig.Blocks, BlockChecksum{
			Index:  int(idx),
			Weak:   weak,
			Strong: strong,
		})
	}
	return sig, nil
}
