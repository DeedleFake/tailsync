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

	"github.com/quic-go/quic-go"
)

func testTLS(t *testing.T) *tls.Config {
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

func TestSharedTransportDialAndAccept(t *testing.T) {
	serverTLS := testTLS(t)
	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{ALPN},
		MinVersion:         tls.VersionTLS13,
	}

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep, err := NewEndpoint(serverTLS, clientTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Close()
	if err := ep.AddPacketConn(pc); err != nil {
		t.Fatal(err)
	}
	addr := ep.LocalAddrs()[0]

	// Dial from a second shared endpoint.
	pc2, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep2, err := NewEndpoint(serverTLS, clientTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ep2.Close()
	if err := ep2.AddPacketConn(pc2); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		c   *quic.Conn
		err error
	}
	acc := make(chan result, 1)
	go func() {
		c, err := ep.Accept(ctx)
		acc <- result{c, err}
	}()

	conn, err := ep2.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseWithError(0, "")

	select {
	case r := <-acc:
		if r.err != nil {
			t.Fatalf("accept: %v", r.err)
		}
		defer r.c.CloseWithError(0, "")
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	// Open stream both ways to prove connection is live; PacketConns still owned by endpoints.
	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := str.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	_ = str.Close()
}

func TestPickBindsByFamily(t *testing.T) {
	b4 := bind{localIP: net.ParseIP("127.0.0.1")}
	b6 := bind{localIP: net.ParseIP("::1")}
	bu := bind{localIP: nil}
	got := pickBindsByFamily([]bind{b6, b4, bu}, net.ParseIP("127.0.0.2"))
	if len(got) != 3 || got[0].localIP.To4() == nil {
		t.Fatalf("v4 first: %+v", got)
	}
	got = pickBindsByFamily([]bind{b4, b6}, net.ParseIP("::2"))
	if len(got) != 2 || got[0].localIP.To4() != nil {
		t.Fatalf("v6 first: %+v", got)
	}
}

func TestSessionDoesNotClosePacketConn(t *testing.T) {
	// Closing a session must leave the endpoint PacketConn usable for another dial.
	serverTLS := testTLS(t)
	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{ALPN},
		MinVersion:         tls.VersionTLS13,
	}
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep, err := NewEndpoint(serverTLS, clientTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Close()
	if err := ep.AddPacketConn(pc); err != nil {
		t.Fatal(err)
	}
	addr := ep.LocalAddrs()[0]

	pc2, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep2, err := NewEndpoint(serverTLS, clientTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ep2.Close()
	if err := ep2.AddPacketConn(pc2); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		c, err := ep.Accept(ctx)
		if err != nil {
			return
		}
		// Accept one stream then close conn only.
		sctx, scancel := context.WithTimeout(ctx, 2*time.Second)
		str, err := c.AcceptStream(sctx)
		scancel()
		if err == nil {
			str.CancelRead(0)
			_ = str.Close()
		}
		_ = c.CloseWithError(0, "done")
	}()

	conn, err := ep2.Dial(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sc := newStreamConn(str, conn)
	_ = sc.Close()
	_ = conn.CloseWithError(0, "")

	// Dial again on same shared transports.
	go func() {
		c, err := ep.Accept(ctx)
		if err != nil {
			return
		}
		_ = c.CloseWithError(0, "")
	}()
	conn2, err := ep2.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("second dial after session close: %v", err)
	}
	_ = conn2.CloseWithError(0, "")
}
