package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"time"

	valkyr "github.com/ckphillipe/valkyr/valkyr-go"
)

type provider struct{}

func (provider) Get(context.Context, string, string) (any, error) {
	ttl := 5 * time.Minute
	return valkyr.ProviderValue{
		Value: map[string]string{"source": "Go adapter"},
		TTL:   &ttl,
	}, nil
}

type store struct{}

func (store) Set(context.Context, string, string, json.RawMessage, *time.Duration) error { return nil }
func (store) SetMany(context.Context, string, []valkyr.Entry, *time.Duration) error      { return nil }
func (store) Delete(context.Context, string, *string) error                              { return nil }
func (store) Move(context.Context, string, string) error                                 { return nil }

func main() {
	adapter, err := valkyr.NewAdapter()
	if err != nil {
		panic(err)
	}
	if err = adapter.Provide("/examples", "*", provider{}); err != nil {
		panic(err)
	}
	if err = adapter.Store("/examples", "*", store{}); err != nil {
		panic(err)
	}
	endpoint := os.Getenv("VALKYR_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:8081"
	}
	client, err := valkyr.NewAdapterClient([]string{endpoint}, os.Getenv("VALKYR_API_KEY"), adapter)
	if err != nil {
		panic(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := client.Serve(ctx); err != nil && ctx.Err() == nil {
		panic(err)
	}
}
