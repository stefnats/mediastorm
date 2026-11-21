//go:generate go tool mockgen -source=./nntp.go -destination=./nntp_mock.go -package=nntpcli Client
package nntpcli

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// ContextDialer is a dialer that supports context.
// Use this for proxy connections or custom dialing behavior.
type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type DialConfig struct {
	KeepAliveTime time.Duration
	DialTimeout   time.Duration
	// Dialer is an optional custom dialer. If nil, net.Dialer is used.
	// Use this for proxy connections.
	Dialer ContextDialer
}

type Client interface {
	Dial(
		ctx context.Context,
		host string,
		port int,
		config ...DialConfig,
	) (Connection, error)
	DialTLS(
		ctx context.Context,
		host string,
		port int,
		insecureSSL bool,
		config ...DialConfig,
	) (Connection, error)
}

type client struct {
	config Config
}

// New creates a new NNTP client
//
// If no config is provided, the default config will be used
func New(
	c ...Config,
) Client {
	config := mergeWithDefault(c...)

	return &client{
		config: config,
	}
}

// setupTCPConn configures a TCP connection with keep-alive and other options.
// If cfg.Dialer is set, it will be used for dialing (e.g., for proxy connections).
func (c *client) setupTCPConn(ctx context.Context, host string, port int, cfg DialConfig) (net.Conn, time.Duration, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, 0, errors.New("NNTP context unavailable")
	}
	var dialer ContextDialer

	if cfg.Dialer != nil {
		dialer = cfg.Dialer
	} else {
		d := &net.Dialer{}
		if cfg.DialTimeout != 0 {
			d.Timeout = cfg.DialTimeout
		}
		dialer = d
	}

	conn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, 0, err
	}

	keep := false
	defer func() {
		if !keep {
			_ = conn.Close()
		}
	}()
	keepAlive := c.config.KeepAliveTime
	if cfg.KeepAliveTime != 0 {
		keepAlive = cfg.KeepAliveTime
	}

	// Configure TCP options only for direct TCP connections (not proxied)
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err = tcpConn.SetKeepAlive(true); err != nil {
			return nil, 0, err
		}

		if err = tcpConn.SetKeepAlivePeriod(keepAlive); err != nil {
			return nil, 0, err
		}

		if err = tcpConn.SetNoDelay(true); err != nil {
			return nil, 0, err
		}
	}

	keep = true
	return conn, keepAlive, nil
}

// watchSocketCancellation only interrupts the actual owned socket. The caller
// must join this signal callback before transferring or releasing the connection.
func watchSocketCancellation(ctx context.Context, conn net.Conn) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); _ = conn.Close() })
	var once sync.Once
	return func() {
		once.Do(func() {
			if !stop() {
				<-done
			}
		})
	}
}

// BindConnectionCancellation links an operation to a genuine original NNTP
// socket. It is lifetime plumbing and supplies no provider/source permission.
// InterruptConnection interrupts only an initialized concrete socket. A later
// pool cleanup still has to join actual borrowers, constructors and destructors.
func InterruptConnection(value Connection) error {
	actual, ok := value.(*connection)
	if !ok || actual == nil || actual.netconn == nil {
		return errors.New("NNTP cancellation unsupported")
	}
	return actual.netconn.Close()
}

func BindConnectionCancellation(ctx context.Context, value Connection) (func(), error) {
	actual, ok := value.(*connection)
	if !ok || actual == nil || actual.netconn == nil || ctx == nil {
		return nil, errors.New("NNTP cancellation unsupported")
	}
	return watchSocketCancellation(ctx, actual.netconn), nil
}

func (c *client) finishSetup(ctx context.Context, raw net.Conn, maxAge time.Time) (Connection, error) {
	stop := watchSocketCancellation(ctx, raw)
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		if err := raw.SetDeadline(deadline); err != nil {
			_ = raw.Close()
			return nil, err
		}
	}
	result, err := newConnection(raw, maxAge, c.config.OperationTimeout)
	stop()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = raw.SetDeadline(time.Time{})
	}
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return result, nil
}

// Dial connects to an NNTP server using a plain TCP connection.
//
// Parameters:
//   - ctx: Context for controlling the connection lifecycle
//   - host: The hostname or IP address of the NNTP server
//   - port: The port number of the NNTP server
//   - keepAliveTime: Optional duration to override the default keep-alive time
//   - dialTimeout: Optional timeout duration for the initial connection
//
// Returns:
//   - Connection: An NNTP connection interface if successful
//   - error: Any error encountered during connection
func (c *client) Dial(
	ctx context.Context,
	host string,
	port int,
	config ...DialConfig,
) (Connection, error) {
	var cfg DialConfig
	if len(config) > 0 {
		cfg = config[0]
	}

	conn, keepAlive, err := c.setupTCPConn(ctx, host, port, cfg)
	if err != nil {
		return nil, err
	}

	maxAgeTime := time.Now().Add(keepAlive)

	return c.finishSetup(ctx, conn, maxAgeTime)
}

// DialTLS connects to an NNTP server using a TLS-encrypted connection.
//
// Parameters:
//   - ctx: Context for controlling the connection lifecycle
//   - host: The hostname or IP address of the NNTP server
//   - port: The port number of the NNTP server
//   - insecureSSL: If true, skips verification of the server's certificate chain and host name
//   - keepAliveTime: Optional duration to override the default keep-alive time
//   - dialTimeout: Optional timeout duration for the initial connection
//
// Returns:
//   - Connection: An NNTP connection interface if successful
//   - error: Any error encountered during connection
func (c *client) DialTLS(
	ctx context.Context,
	host string,
	port int,
	insecureSSL bool,
	config ...DialConfig,
) (Connection, error) {
	var cfg DialConfig
	if len(config) > 0 {
		cfg = config[0]
	}

	conn, keepAlive, err := c.setupTCPConn(ctx, host, port, cfg)
	if err != nil {
		return nil, err
	}

	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: insecureSSL,
	})

	stopHandshake := watchSocketCancellation(ctx, conn)
	err = tlsConn.HandshakeContext(ctx)
	stopHandshake()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	maxAgeTime := time.Now().Add(keepAlive)

	return c.finishSetup(ctx, tlsConn, maxAgeTime)
}
