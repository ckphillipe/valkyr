package valkyr

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type ProvideOptions struct {
	MaxRate *uint64
	Timeout time.Duration
	MissTTL time.Duration
}

type ProvideRoute struct {
	NamespacePattern string
	KeyPattern       string
	Provider         Provider
	MaxRate          *uint64
	Options          ProvideOptions
	namespaceTokens  []patternToken
	keyTokens        []patternToken
}
type StoreRoute struct {
	NamespacePattern string
	KeyPattern       string
	Store            Store
	namespaceTokens  []patternToken
	keyTokens        []patternToken
}
type Adapter struct {
	mu        sync.RWMutex
	instance  string
	providers []ProvideRoute
	stores    []StoreRoute
}

func NewAdapter() (*Adapter, error) {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		return nil, e
	}
	return &Adapter{instance: fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[:4]), hex.EncodeToString(b[4:6]), hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:]))}, nil
}
func (a *Adapter) AdapterInstance() string { a.mu.RLock(); defer a.mu.RUnlock(); return a.instance }
func (a *Adapter) SetAdapterInstance(instance string) error {
	if _, e := uuidValue(instance, true); e != nil {
		return e
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.instance = instance
	return nil
}
func (a *Adapter) Provide(namespacePattern, keyPattern string, provider Provider, maxRate ...uint64) error {
	var options ProvideOptions
	if len(maxRate) > 1 {
		return fmt.Errorf("%w: only one max rate is allowed", ErrRoute)
	}
	if len(maxRate) == 1 {
		options.MaxRate = &maxRate[0]
	}
	return a.ProvideWithOptions(namespacePattern, keyPattern, provider, options)
}

func (a *Adapter) ProvideWithOptions(namespacePattern, keyPattern string, provider Provider, options ProvideOptions) error {
	if provider == nil || namespacePattern == "" || keyPattern == "" {
		return fmt.Errorf("%w: provider registration is incomplete", ErrRoute)
	}
	if options.Timeout < 0 || options.Timeout%time.Millisecond != 0 {
		return fmt.Errorf("%w: provider timeout must be a non-negative whole number of milliseconds", ErrRoute)
	}
	if options.MissTTL < 0 || options.MissTTL%time.Second != 0 {
		return fmt.Errorf("%w: provider miss TTL must be a non-negative whole number of seconds", ErrRoute)
	}
	if options.MaxRate != nil && (*options.MaxRate == 0 || *options.MaxRate > uint64(^uint32(0))) {
		return fmt.Errorf("%w: max rate must be a non-zero u32", ErrRoute)
	}
	var maxRate *uint64
	if options.MaxRate != nil {
		ownedMaxRate := *options.MaxRate
		maxRate = &ownedMaxRate
	}
	ownedOptions := options
	ownedOptions.MaxRate = maxRate
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, old := range a.providers {
		if overlap(old.NamespacePattern, namespacePattern) && overlap(old.KeyPattern, keyPattern) {
			return fmt.Errorf("%w: overlapping provider registrations are ambiguous", ErrRoute)
		}
	}
	a.providers = append(a.providers, ProvideRoute{NamespacePattern: namespacePattern, KeyPattern: keyPattern, Provider: provider, MaxRate: maxRate, Options: ownedOptions, namespaceTokens: tokenize(namespacePattern), keyTokens: tokenize(keyPattern)})
	return nil
}
func (a *Adapter) Store(namespacePattern, keyPattern string, store Store) error {
	if store == nil || namespacePattern == "" || keyPattern == "" {
		return fmt.Errorf("%w: store registration is incomplete", ErrRoute)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stores = append(a.stores, StoreRoute{namespacePattern, keyPattern, store, tokenize(namespacePattern), tokenize(keyPattern)})
	return nil
}
func (a *Adapter) snapshot() ([]ProvideRoute, []StoreRoute, string) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	p := append([]ProvideRoute(nil), a.providers...)
	for i := range p {
		if p[i].MaxRate != nil {
			maxRate := *p[i].MaxRate
			p[i].MaxRate = &maxRate
		}
		if p[i].Options.MaxRate != nil {
			maxRate := *p[i].Options.MaxRate
			p[i].Options.MaxRate = &maxRate
		}
	}
	s := append([]StoreRoute(nil), a.stores...)
	return p, s, a.instance
}
