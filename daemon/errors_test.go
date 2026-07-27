package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"deedles.dev/tailsync/internal/peer"
)

func TestIsTransportErr(t *testing.T) {
	if isTransportErr(nil) {
		t.Fatal("nil")
	}
	if !isTransportErr(fmt.Errorf("%w: decode", errTransport)) {
		t.Fatal("want transport")
	}
	if !isTransportErr(io.EOF) {
		t.Fatal("EOF is transport")
	}
	// Server TypeError text that historically false-positived on "read ".
	logical := peerLogical("read foo.txt: permission denied")
	if isTransportErr(logical) {
		t.Fatalf("logical TypeError must not abort peer sync: %v", logical)
	}
	if !errors.Is(logical, errPeerLogical) {
		t.Fatal("want errPeerLogical")
	}
	// Substring alone must not classify.
	if isTransportErr(errors.New("read foo.txt: permission denied")) {
		t.Fatal("plain string with read must not be transport")
	}
}

func TestUnexpectedMsgType(t *testing.T) {
	err := fmt.Errorf("%w %q", errUnexpectedMsgType, "sig_req")
	if !errors.Is(err, errUnexpectedMsgType) {
		t.Fatal("want errors.Is unexpected message type")
	}
}

func TestInvalidateSessionOnTransport(t *testing.T) {
	d := &Daemon{}

	// Hard transport → Invalidate.
	sHard := peer.SessionForTest("peer", "local")
	hard := fmt.Errorf("%w: decode", errTransport)
	d.invalidateSessionOnTransport(sHard, hard)
	if sHard.Healthy() || !sHard.Closed() {
		t.Fatal("hard transport must Invalidate")
	}

	// Soft deadline wrapped as transport → leave session up.
	sSoft := peer.SessionForTest("peer", "local")
	soft := fmt.Errorf("%w: open: %w", errTransport, context.DeadlineExceeded)
	d.invalidateSessionOnTransport(sSoft, soft)
	if !sSoft.Healthy() || sSoft.Closed() {
		t.Fatal("soft transport must not Invalidate")
	}

	// Logical peer error → no Invalidate.
	sLogical := peer.SessionForTest("peer", "local")
	d.invalidateSessionOnTransport(sLogical, peerLogical("nope"))
	if !sLogical.Healthy() || sLogical.Closed() {
		t.Fatal("logical must not Invalidate")
	}

	// EOF is transport (hard).
	sEOF := peer.SessionForTest("peer", "local")
	d.invalidateSessionOnTransport(sEOF, io.EOF)
	if sEOF.Healthy() || !sEOF.Closed() {
		t.Fatal("EOF must Invalidate")
	}

	// Hard I/O helper (notify Encode path) without errTransport wrapper.
	sIO := peer.SessionForTest("peer", "local")
	d.invalidateSessionOnHardIO(sIO, errors.New("connection reset"))
	if sIO.Healthy() || !sIO.Closed() {
		t.Fatal("hard I/O must Invalidate")
	}
	sIOSoft := peer.SessionForTest("peer", "local")
	d.invalidateSessionOnHardIO(sIOSoft, context.Canceled)
	if !sIOSoft.Healthy() || sIOSoft.Closed() {
		t.Fatal("soft I/O must not Invalidate")
	}
}
