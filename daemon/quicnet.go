package daemon

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"

	"deedles.dev/tailsync/internal/peer"
)

// generateQUICTLSConfig builds an ephemeral self-signed server certificate.
// Peer authentication is the tailnet (or localhost in plain tests); the cert
// only satisfies QUIC/TLS requirements. Clients use InsecureSkipVerify.
// NextProtos must match peer.ALPN (owned by the peer package).
func generateQUICTLSConfig() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate quic tls key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate cert serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create self-signed cert: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load tls key pair: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{peer.ALPN},
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func quicClientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, // tailnet (or localhost test) provides peer trust
		NextProtos:         []string{peer.ALPN},
		MinVersion:         tls.VersionTLS13,
	}
}

// peerIPFromStatus resolves a peer host name (MagicDNS or HostName) to a
// Tailscale IP using LocalAPI status. IP literals are not handled here.
func peerIPFromStatus(st *ipnstate.Status, host string) (netip.Addr, bool) {
	if st == nil {
		return netip.Addr{}, false
	}
	want := strings.ToLower(strings.TrimSuffix(host, "."))
	if want == "" {
		return netip.Addr{}, false
	}
	match := func(p *ipnstate.PeerStatus) bool {
		if p == nil || len(p.TailscaleIPs) == 0 {
			return false
		}
		dns := strings.ToLower(strings.TrimSuffix(p.DNSName, "."))
		hn := strings.ToLower(p.HostName)
		if dns == want || hn == want {
			return true
		}
		// Short name matches MagicDNS prefix (e.g. "peer" vs "peer.tailnet.ts.net").
		if dns != "" && (strings.HasPrefix(dns, want+".") || strings.HasPrefix(want, dns+".")) {
			return true
		}
		return false
	}
	for _, p := range st.Peer {
		if match(p) {
			return p.TailscaleIPs[0], true
		}
	}
	if match(st.Self) {
		return st.Self.TailscaleIPs[0], true
	}
	return netip.Addr{}, false
}

// resolveTSNetUDPAddr resolves host:port for tsnet QUIC dials.
// IP literals use the system parse path; hostnames use tsnet LocalAPI status
// (MagicDNS / HostName), not system DNS.
func resolveTSNetUDPAddr(ctx context.Context, s *tsnet.Server, addr string) (*net.UDPAddr, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("split addr %s: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("invalid port %q in %s", portStr, addr)
	}
	if ip := net.ParseIP(host); ip != nil {
		return &net.UDPAddr{IP: ip, Port: port}, nil
	}
	if s == nil {
		return nil, fmt.Errorf("tsnet not started")
	}
	lc, err := s.LocalClient()
	if err != nil {
		return nil, err
	}
	st, err := lc.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("tsnet status for resolve: %w", err)
	}
	tip, ok := peerIPFromStatus(st, host)
	if !ok {
		return nil, fmt.Errorf("resolve %s: no tailscale peer with that name", host)
	}
	ip := net.ParseIP(tip.String())
	if ip == nil {
		return nil, fmt.Errorf("resolve %s: invalid tailscale ip %v", host, tip)
	}
	return &net.UDPAddr{IP: ip, Port: port}, nil
}
