//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	valkyr "github.com/ckphillipe/valkyr/valkyr-go"
)

var (
	serverBuildOnce sync.Once
	serverBuildErr  error
)

type runningServer struct {
	root          string
	config        string
	nativeAddress string
	tlsAddress    string
	bootstrapKey  string
	tlsCA         string
	process       *exec.Cmd
}

func repositoryRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func serverBinary(t *testing.T) string {
	t.Helper()
	root := repositoryRoot()
	path := filepath.Join(root, "target", "debug", "valkyr-server")
	serverBuildOnce.Do(func() {
		if _, err := os.Stat(path); err == nil {
			return
		}
		cmd := exec.Command("cargo", "build", "-p", "valkyr-server")
		cmd.Dir = root
		cmd.Stdout = io.Discard
		cmd.Stderr = os.Stderr
		serverBuildErr = cmd.Run()
	})
	if serverBuildErr != nil {
		t.Fatalf("build valkyr-server: %v", serverBuildErr)
	}
	return path
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForPort(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("tcp", address, 250*time.Millisecond)
		if err == nil {
			connection.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server did not listen on %s", address)
}

func startServer(t *testing.T, tlsEnabled bool) *runningServer {
	t.Helper()
	nativePort := freePort(t)
	tlsPort := freePort(t)
	tmp := t.TempDir()
	bootstrapKey := "integration-bootstrap-key"
	keyFile := filepath.Join(tmp, "bootstrap-api-key")
	if err := os.WriteFile(keyFile, []byte(bootstrapKey), 0o600); err != nil {
		t.Fatal(err)
	}

	tlsConfig := ""
	tlsCA := ""
	if tlsEnabled {
		tlsCA = filepath.Join(tmp, "tls.crt")
		keyPath := filepath.Join(tmp, "tls.key")
		cmd := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-keyout", keyPath, "-out", tlsCA, "-days", "1", "-nodes", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1")
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if err := cmd.Run(); err != nil {
			t.Skipf("TLS integration requires openssl with subjectAltName support: %v", err)
		}
		tlsConfig = fmt.Sprintf("tls:\n  listen: 127.0.0.1:%d\n  certificate_file: %s\n  private_key_file: %s\n", tlsPort, tlsCA, keyPath)
	}

	config := filepath.Join(tmp, "server.yml")
	contents := fmt.Sprintf("native_listen: 127.0.0.1:%d\nhttp_listen: 127.0.0.1:%d\nmetrics_listen: 127.0.0.1:%d\nlog_filter: error\n%sauth:\n  bootstrap_api_key_file: %s\n  session_ttl_seconds: 3600\n", nativePort, freePort(t), freePort(t), tlsConfig, keyFile)
	if tlsConfig == "" {
		contents = fmt.Sprintf("native_listen: 127.0.0.1:%d\nhttp_listen: 127.0.0.1:%d\nmetrics_listen: 127.0.0.1:%d\nlog_filter: error\nauth:\n  bootstrap_api_key_file: %s\n  session_ttl_seconds: 3600\n", nativePort, freePort(t), freePort(t), keyFile)
	}
	if err := os.WriteFile(config, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	server := &runningServer{root: repositoryRoot(), config: config, nativeAddress: fmt.Sprintf("127.0.0.1:%d", nativePort), tlsAddress: fmt.Sprintf("127.0.0.1:%d", tlsPort), bootstrapKey: bootstrapKey, tlsCA: tlsCA}
	server.start(t)
	t.Cleanup(func() { server.stop() })
	return server
}

func (s *runningServer) start(t *testing.T) {
	t.Helper()
	cmd := exec.Command(serverBinary(t), "--config", s.config)
	cmd.Dir = s.root
	cmd.Stdout = io.Discard
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s.process = cmd
	waitForPort(t, s.nativeAddress)
	if s.tlsCA != "" {
		waitForPort(t, s.tlsAddress)
	}
}

func (s *runningServer) stop() {
	if s.process == nil || s.process.Process == nil {
		return
	}
	_ = s.process.Process.Kill()
	_, _ = s.process.Process.Wait()
	s.process = nil
}

func (s *runningServer) restart(t *testing.T) {
	t.Helper()
	s.stop()
	s.start(t)
}

func waitUntil(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func closeAdapter(t *testing.T, adapter *valkyr.AdapterClient, serveDone <-chan error) {
	t.Helper()
	adapter.Close()
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("adapter Serve did not stop")
	}
}

func contextWithTimeout(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 10*time.Second)
}
