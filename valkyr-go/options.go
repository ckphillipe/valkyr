package valkyr

import "time"

type options struct {
	APIKey         string
	ConnectTimeout time.Duration
	RequestTimeout time.Duration
	AuthTimeout    time.Duration
	TLS            *TLSConfig
}
type Option func(*options) error

func WithAPIKey(key string) Option { return func(o *options) error { o.APIKey = key; return nil } }
func WithConnectTimeout(v time.Duration) Option {
	return func(o *options) error {
		if v <= 0 {
			return protocol("connect timeout must be positive")
		}
		o.ConnectTimeout = v
		return nil
	}
}
func WithRequestTimeout(v time.Duration) Option {
	return func(o *options) error {
		if v <= 0 {
			return protocol("request timeout must be positive")
		}
		o.RequestTimeout = v
		return nil
	}
}
func WithAuthTimeout(v time.Duration) Option {
	return func(o *options) error {
		if v <= 0 {
			return protocol("auth timeout must be positive")
		}
		o.AuthTimeout = v
		return nil
	}
}
func WithTLS(config TLSConfig) Option { return func(o *options) error { o.TLS = &config; return nil } }
func defaultOptions() options {
	return options{ConnectTimeout: 10 * time.Second, RequestTimeout: 30 * time.Second, AuthTimeout: 5 * time.Second}
}
