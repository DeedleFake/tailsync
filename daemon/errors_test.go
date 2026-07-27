package daemon

import (
	"errors"
	"fmt"
	"io"
	"testing"
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
