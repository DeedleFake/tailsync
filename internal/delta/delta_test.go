package delta_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"deedles.dev/tailsync/internal/delta"
)

func TestIdenticalFileEmptyDeltaCopies(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefghij"), 500) // 5000 bytes
	sig, err := delta.SignBytes(data, 64)
	if err != nil {
		t.Fatal(err)
	}
	d, err := delta.EncodeBytes(data, sig)
	if err != nil {
		t.Fatal(err)
	}
	out, err := delta.Apply(data, d)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("reconstructed file mismatch")
	}
	// Should be mostly OpCopy.
	var copies, lits int
	for _, op := range d.Ops {
		switch op.Kind {
		case delta.OpCopy:
			copies++
		case delta.OpLiteral:
			lits++
		}
	}
	if copies == 0 {
		t.Fatal("expected copy ops for identical file")
	}
	t.Logf("ops: copies=%d literals=%d totalOps=%d", copies, lits, len(d.Ops))
}

func TestPartialChange(t *testing.T) {
	basis := bytes.Repeat([]byte("A"), 4096)
	basis = append(basis, bytes.Repeat([]byte("B"), 4096)...)
	basis = append(basis, bytes.Repeat([]byte("C"), 4096)...)

	// Change middle block region.
	target := make([]byte, len(basis))
	copy(target, basis)
	copy(target[4096:8192], bytes.Repeat([]byte("X"), 4096))

	sig, err := delta.SignBytes(basis, 1024)
	if err != nil {
		t.Fatal(err)
	}
	d, err := delta.EncodeBytes(target, sig)
	if err != nil {
		t.Fatal(err)
	}
	out, err := delta.Apply(basis, d)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, target) {
		t.Fatal("reconstruct mismatch after partial change")
	}

	// Delta binary should be smaller than full target for this case.
	raw, err := delta.MarshalDelta(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) >= len(target) {
		t.Logf("warning: delta size %d >= target %d (still correct)", len(raw), len(target))
	} else {
		t.Logf("delta %d bytes vs full %d", len(raw), len(target))
	}
}

func TestNoBasisFullLiteral(t *testing.T) {
	data := []byte("hello world")
	d, err := delta.EncodeBytes(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := delta.Apply(nil, d)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("mismatch")
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	basis := make([]byte, 10000)
	if _, err := rand.Read(basis); err != nil {
		t.Fatal(err)
	}
	target := make([]byte, len(basis))
	copy(target, basis)
	// Mutate a slice.
	for i := 100; i < 500; i++ {
		target[i] ^= 0xff
	}

	sig, err := delta.SignBytes(basis, 256)
	if err != nil {
		t.Fatal(err)
	}
	sigRaw, err := delta.MarshalSignature(sig)
	if err != nil {
		t.Fatal(err)
	}
	sig2, err := delta.UnmarshalSignature(sigRaw)
	if err != nil {
		t.Fatal(err)
	}
	if sig2.BlockSize != sig.BlockSize || len(sig2.Blocks) != len(sig.Blocks) {
		t.Fatalf("sig roundtrip: %+v vs %+v", sig2, sig)
	}

	d, err := delta.EncodeBytes(target, sig2)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := delta.MarshalDelta(d)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := delta.UnmarshalDelta(raw)
	if err != nil {
		t.Fatal(err)
	}
	out, err := delta.Apply(basis, d2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, target) {
		t.Fatal("marshal roundtrip reconstruct failed")
	}
}

func TestAppendOnly(t *testing.T) {
	basis := bytes.Repeat([]byte("line\n"), 200)
	target := append(append([]byte{}, basis...), []byte("extra line\n")...)
	sig, err := delta.SignBytes(basis, 32)
	if err != nil {
		t.Fatal(err)
	}
	d, err := delta.EncodeBytes(target, sig)
	if err != nil {
		t.Fatal(err)
	}
	out, err := delta.Apply(basis, d)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, target) {
		t.Fatal("append reconstruct failed")
	}
}

func TestWeakRollConsistency(t *testing.T) {
	// Encode/Apply is the public API; rolling is internal. Spot-check via many sizes.
	for _, bs := range []int{16, 64, 512, 4096} {
		basis := bytes.Repeat([]byte{1, 2, 3, 4, 5}, 2000)
		target := append([]byte{9, 9, 9}, basis...)
		sig, err := delta.SignBytes(basis, bs)
		if err != nil {
			t.Fatal(err)
		}
		d, err := delta.EncodeBytes(target, sig)
		if err != nil {
			t.Fatal(err)
		}
		out, err := delta.Apply(basis, d)
		if err != nil {
			t.Fatalf("bs=%d apply: %v", bs, err)
		}
		if !bytes.Equal(out, target) {
			t.Fatalf("bs=%d mismatch", bs)
		}
	}
}

func TestUnmarshalDeltaHugeNOps(t *testing.T) {
	// magic + blockSize + outputSize + nOps=0xFFFFFFFF with no op bodies.
	buf := []byte{'T', 'S', 'D', '1'}
	buf = append(buf, 0, 0, 0, 0)             // blockSize
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0) // outputSize
	buf = append(buf, 0xff, 0xff, 0xff, 0xff) // nOps
	_, err := delta.UnmarshalDelta(buf)
	if err == nil {
		t.Fatal("expected error for huge nOps")
	}
}

func TestUnmarshalSignatureHugeNBlocks(t *testing.T) {
	// blockSize u32 LE = 64, nBlocks = 0xFFFFFFFF
	buf := []byte{'T', 'S', 'S', '1', 64, 0, 0, 0}
	buf = append(buf, 0xff, 0xff, 0xff, 0xff)
	_, err := delta.UnmarshalSignature(buf)
	if err == nil {
		t.Fatal("expected error for huge nBlocks")
	}
}

func TestUnmarshalDeltaHugeOutputSize(t *testing.T) {
	// magic + blockSize=64 + outputSize=max uint64 + nOps=0
	buf := []byte{'T', 'S', 'D', '1'}
	buf = append(buf, 64, 0, 0, 0)
	buf = append(buf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
	buf = append(buf, 0, 0, 0, 0) // nOps = 0
	_, err := delta.UnmarshalDelta(buf)
	if err == nil {
		t.Fatal("expected error for huge OutputSize")
	}
}

func TestApplyRejectsHugeOutputSize(t *testing.T) {
	_, err := delta.Apply(nil, &delta.Delta{OutputSize: delta.MaxOutputSize + 1})
	if err == nil {
		t.Fatal("expected error")
	}
}
