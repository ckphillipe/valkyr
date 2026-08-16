package valkyr

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CAPEM      []byte
	CAFile     string
	ServerName string
}

func tlsConfig(c *TLSConfig) (*tls.Config, error) {
	if c == nil {
		return nil, nil
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if len(c.CAPEM) > 0 && !roots.AppendCertsFromPEM(c.CAPEM) {
		return nil, fmt.Errorf("%w: invalid CA PEM", ErrProtocol)
	}
	if c.CAFile != "" {
		b, e := os.ReadFile(c.CAFile)
		if e != nil {
			return nil, e
		}
		if !roots.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("%w: invalid CA file", ErrProtocol)
		}
	}
	return &tls.Config{RootCAs: roots, ServerName: c.ServerName, MinVersion: tls.VersionTLS12}, nil
}
