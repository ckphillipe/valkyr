package valkyr

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const maxFrameBytes = 1024 * 1024

type transport struct {
	conn      net.Conn
	reader    *bufio.Reader
	mu        sync.Mutex
	closeOnce sync.Once
	closed    chan struct{}
}

func openTransport(ctx context.Context, address string, opts options) (*transport, error) {
	host, port, err := parseAddress(address)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{Timeout: opts.ConnectTimeout}
	var c net.Conn
	if opts.TLS != nil {
		tc, e := tlsConfig(opts.TLS)
		if e != nil {
			return nil, e
		}
		c, e = d.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
		if e != nil {
			return nil, fmt.Errorf("%w: %v", ErrConnection, e)
		}
		tlsConn := tlsClient(c, tc, host)
		if err = tlsHandshake(ctx, tlsConn, opts.ConnectTimeout); err != nil {
			c.Close()
			return nil, err
		}
		c = tlsConn
	} else {
		c, err = d.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrConnection, err)
		}
	}
	return &transport{conn: c, reader: bufio.NewReaderSize(c, 64*1024), closed: make(chan struct{})}, nil
}

func tlsClient(conn net.Conn, config *tls.Config, host string) net.Conn {
	if config.ServerName == "" && net.ParseIP(host) == nil {
		config = config.Clone()
		config.ServerName = host
	}
	return tls.Client(conn, config)
}
func tlsHandshake(ctx context.Context, conn net.Conn, timeout time.Duration) error {
	t := conn.(*tls.Conn)
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := t.SetDeadline(deadline); err != nil {
		return err
	}
	if err := t.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("%w: TLS handshake failed: %v", ErrConnection, err)
	}
	if err := t.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("%w: clearing TLS handshake deadline: %v", ErrConnection, err)
	}
	return nil
}

func parseAddress(address string) (string, string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", "", fmt.Errorf("%w: address is required", ErrRoute)
	}
	if h, p, e := net.SplitHostPort(address); e == nil {
		if h == "" || p == "" {
			return "", "", fmt.Errorf("%w: invalid address %q", ErrRoute, address)
		}
		return h, p, nil
	}
	if len(address) > 0 && address[0] == '[' {
		if end := strings.IndexByte(address, ']'); end >= 0 && end == len(address)-1 {
			host := address[1:end]
			if host == "" || net.ParseIP(host) == nil {
				return "", "", fmt.Errorf("%w: invalid address %q", ErrRoute, address)
			}
			return host, "8081", nil
		}
		return "", "", fmt.Errorf("%w: invalid address %q", ErrRoute, address)
	}
	if strings.Count(address, ":") > 1 {
		if net.ParseIP(address) == nil {
			return "", "", fmt.Errorf("%w: invalid IPv6 address %q", ErrRoute, address)
		}
		return address, "8081", nil
	}
	if strings.Contains(address, ":") {
		return "", "", fmt.Errorf("%w: invalid address %q", ErrRoute, address)
	}
	return address, "8081", nil
}

func (t *transport) close() { t.closeOnce.Do(func() { close(t.closed); _ = t.conn.Close() }) }
func (t *transport) isClosed() bool {
	select {
	case <-t.closed:
		return true
	default:
		return false
	}
}
func (t *transport) ioDeadline(ctx context.Context, read bool) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	setDeadline := t.conn.SetReadDeadline
	if !read {
		setDeadline = t.conn.SetWriteDeadline
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := setDeadline(deadline); err != nil {
			return nil, err
		}
	} else if err := setDeadline(time.Time{}); err != nil {
		return nil, err
	}
	if ctx.Done() == nil {
		return func() {}, nil
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = setDeadline(time.Now())
		case <-stop:
		}
	}()
	return func() { close(stop) }, nil
}
func (t *transport) readFrame(ctx context.Context) ([]byte, error) {
	if t.isClosed() {
		return nil, fmt.Errorf("%w: transport is closed", ErrConnection)
	}
	clearDeadline, err := t.ioDeadline(ctx, true)
	if err != nil {
		return nil, err
	}
	defer clearDeadline()
	var line []byte
	for {
		part, readErr := t.reader.ReadSlice('\n')
		line = append(line, part...)
		if len(line) > maxFrameBytes+1 {
			t.close()
			return nil, protocol("frame exceeds 1 MiB")
		}
		if readErr == nil {
			break
		}
		if readErr != bufio.ErrBufferFull {
			err = readErr
			break
		}
	}
	if err != nil {
		t.close()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w: %v", ErrTimeout, ctx.Err())
		}
		return nil, fmt.Errorf("%w: %v", ErrConnection, err)
	}
	if len(line) > maxFrameBytes+1 {
		t.close()
		return nil, protocol("frame exceeds 1 MiB")
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		t.close()
		return nil, protocol("empty frame")
	}
	return line, nil
}
func (t *transport) writeFrame(ctx context.Context, frame []byte) error {
	if len(frame) > maxFrameBytes {
		t.close()
		return protocol("frame exceeds 1 MiB")
	}
	if t.isClosed() {
		return fmt.Errorf("%w: transport is closed", ErrConnection)
	}
	clearDeadline, err := t.ioDeadline(ctx, false)
	if err != nil {
		return err
	}
	defer clearDeadline()
	data := make([]byte, len(frame)+1)
	copy(data, frame)
	data[len(frame)] = '\n'
	if n, err := t.conn.Write(data); err != nil {
		t.close()
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %v", ErrTimeout, ctx.Err())
		}
		return fmt.Errorf("%w: %v", ErrConnection, err)
	} else if n != len(data) {
		t.close()
		return fmt.Errorf("%w: %v", ErrConnection, io.ErrShortWrite)
	}
	return nil
}
func (t *transport) request(ctx context.Context, cmd wireCommand) (wireResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	encoded, err := textCommand(cmd)
	if err != nil {
		return wireResponse{}, err
	}
	if err := t.writeFrame(ctx, encoded); err != nil {
		return wireResponse{}, err
	}
	line, err := t.readFrame(ctx)
	if err != nil {
		t.close()
		return wireResponse{}, err
	}
	r, err := parseTextResponse(line, cmd)
	if err != nil {
		t.close()
		return r, err
	}
	return r, nil
}
func (t *transport) writeRaw(ctx context.Context, b []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writeFrame(ctx, b)
}
func (t *transport) readCommand(ctx context.Context) (wireServerCommand, error) {
	b, e := t.readFrame(ctx)
	if e != nil {
		return wireServerCommand{}, e
	}
	return parseTextServerCommand(b)
}
