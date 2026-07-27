package peer

import (
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// streamConn presents a single QUIC stream as a net.Conn without closing the
// underlying peer connection (streams are one-off ops on a long-lived session).
type streamConn struct {
	str   *quic.Stream
	qconn *quic.Conn

	closeOnce sync.Once
	closeErr  error
}

func newStreamConn(str *quic.Stream, qconn *quic.Conn) *streamConn {
	return &streamConn{str: str, qconn: qconn}
}

func (c *streamConn) Read(p []byte) (int, error)  { return c.str.Read(p) }
func (c *streamConn) Write(p []byte) (int, error) { return c.str.Write(p) }

func (c *streamConn) Close() error {
	c.closeOnce.Do(func() {
		var first error
		if err := c.str.Close(); err != nil {
			first = err
		}
		c.str.CancelRead(0)
		c.closeErr = first
	})
	return c.closeErr
}

func (c *streamConn) LocalAddr() net.Addr  { return c.qconn.LocalAddr() }
func (c *streamConn) RemoteAddr() net.Addr { return c.qconn.RemoteAddr() }

func (c *streamConn) SetDeadline(t time.Time) error {
	return c.str.SetDeadline(t)
}
func (c *streamConn) SetReadDeadline(t time.Time) error {
	return c.str.SetReadDeadline(t)
}
func (c *streamConn) SetWriteDeadline(t time.Time) error {
	return c.str.SetWriteDeadline(t)
}
