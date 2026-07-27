package peer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"deedles.dev/tailsync/internal/proto"
)

func managerTLS(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{ALPN},
		MinVersion:   tls.VersionTLS13,
	}
}

func TestManagerPersistentSessionAndStream(t *testing.T) {
	serverTLS := managerTLS(t)
	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{ALPN},
		MinVersion:         tls.VersionTLS13,
	}

	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	portA := pcA.LocalAddr().(*net.UDPAddr).Port
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	portB := pcB.LocalAddr().(*net.UDPAddr).Port

	addrA := net.JoinHostPort("127.0.0.1", itoa(portA))
	addrB := net.JoinHostPort("127.0.0.1", itoa(portB))

	ma, err := NewManager(Config{
		NodeID:            "node-a",
		Port:              portA,
		ServerTLS:         serverTLS,
		ClientTLS:         clientTLS,
		DialTimeout:       2 * time.Second,
		HandshakeTimeout:  2 * time.Second,
		HeartbeatInterval: time.Hour, // disable for test noise
		Candidates: func(ctx context.Context) []Candidate {
			return []Candidate{{Addr: addrB}}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ma.OnStream = func(ctx context.Context, s *Session, first proto.Message, stream net.Conn) {
		defer stream.Close()
		if first.Header.Type == proto.TypeNotify {
			_ = proto.Encode(stream, proto.NewNotifyOK("node-a", portA))
		}
	}
	if err := ma.AddPacketConn(pcA); err != nil {
		t.Fatal(err)
	}

	mb, err := NewManager(Config{
		NodeID:            "node-b",
		Port:              portB,
		ServerTLS:         serverTLS,
		ClientTLS:         clientTLS,
		DialTimeout:       2 * time.Second,
		HandshakeTimeout:  2 * time.Second,
		HeartbeatInterval: time.Hour,
		Candidates: func(ctx context.Context) []Candidate {
			return []Candidate{{Addr: addrA}}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mb.OnStream = func(ctx context.Context, s *Session, first proto.Message, stream net.Conn) {
		defer stream.Close()
		if first.Header.Type == proto.TypeNotify {
			_ = proto.Encode(stream, proto.NewNotifyOK("node-b", portB))
		}
	}
	if err := mb.AddPacketConn(pcB); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	ma.Start(ctx)
	mb.Start(ctx)
	defer ma.Close()
	defer mb.Close()

	// Wait for connection.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(ma.Snapshot()) > 0 && len(mb.Snapshot()) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(ma.Snapshot()) == 0 {
		t.Fatal("a has no peers")
	}
	if len(mb.Snapshot()) == 0 {
		t.Fatal("b has no peers")
	}

	// Notify stream on persistent session.
	sess := ma.Session("node-b")
	if sess == nil {
		// Might be connected under reverse if b dialed first and a's session is inbound.
		// Snapshot tells us the peer id.
		for _, info := range ma.Snapshot() {
			sess = ma.Session(info.NodeID)
			break
		}
	}
	if sess == nil {
		t.Fatal("no session on a")
	}
	sctx, scancel := context.WithTimeout(ctx, 3*time.Second)
	defer scancel()
	stream, err := sess.OpenStream(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Encode(stream, proto.NewNotify("node-a", portA, nil)); err != nil {
		t.Fatal(err)
	}
	msg, err := proto.Decode(stream)
	_ = stream.Close()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Header.Type != proto.TypeNotifyOK {
		t.Fatalf("got %q", msg.Header.Type)
	}

	// Second stream on same session (no re-Hello).
	stream2, err := sess.OpenStream(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Encode(stream2, proto.NewNotify("node-a", portA, nil)); err != nil {
		t.Fatal(err)
	}
	_, _ = proto.Decode(stream2)
	_ = stream2.Close()
}

// TestActivateIfCurrentRejectsNonCurrent covers the post-Install race path:
// activate must not report success for a session that is no longer roster current.
func TestActivateIfCurrentRejectsNonCurrent(t *testing.T) {
	r := NewRoster()
	m := &Manager{
		roster: r,
		log:    nil,
		cfg: Config{
			HeartbeatInterval: time.Hour,
			HeartbeatTimeout:  time.Second,
		},
		ctx: context.Background(),
	}
	s1 := newSession(SessionConfig{NodeID: "b", LocalID: "a", Dialer: true})
	s2 := newSession(SessionConfig{NodeID: "b", LocalID: "a", Dialer: true})
	if res, _ := r.Install(s1); res != installAccepted {
		t.Fatal("install s1")
	}
	s1.markUnhealthy()
	res, old := r.Install(s2)
	if res != installReplaced || old != s1 {
		t.Fatalf("replace: res=%v old=%v", res, old)
	}
	if m.activateIfCurrent(context.Background(), s1) {
		t.Fatal("must reject non-current session")
	}
	if !s1.Closed() {
		t.Fatal("discard should close non-current")
	}
	// Winner remains current and not closed by discard of loser.
	if r.Get("b") != s2 {
		t.Fatal("s2 should remain current")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
