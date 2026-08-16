# Valkyr Go SDK

`github.com/ckphillipe/valkyr/valkyr-go` is the standalone Go 1.25 SDK for the
Valkyr native TCP/TLS human-readable text protocol v1. It supports application reads and
writes plus provider/store adapters. It targets Valkyr server `0.1.x`.

## Install

```sh
go get github.com/ckphillipe/valkyr/valkyr-go@v0.1.0
```

The module is released from this repository with subdirectory tags such as
`valkyr-go/v0.1.0`; the tag prefix is part of the module release convention.

## Application client

```go
client, err := valkyr.Dial(ctx, "127.0.0.1:8081",
    valkyr.WithAPIKey("app-key"),
)
if err != nil {
    return err
}
defer client.Close()

route := client.Namespace("/users").Key("42")
if err := route.Set(ctx, User{Name: "Ada"}, 5*time.Minute); err != nil {
    return err
}
result, err := route.GetWithRetry(ctx)
if err != nil {
    return err
}
switch result := result.(type) {
case valkyr.Value:
    var user User
    if err := result.Decode(&user); err != nil {
        return err
    }
case valkyr.Miss:
    // The provider refresh is still pending after the bounded retry.
case valkyr.Unknown:
    // No value and no matching provider.
}
```

`Set`, `SetMany`, `Delete`, `Move`, `Ping`, and `Stats` accept a context. A
`Move` source must be a `namespace::context` route and must preserve the base
namespace. TTLs are whole, non-negative seconds. `Value` preserves the JSON
payload and decodes it only when `Decode` is called. A provider `nil` result is
the protocol's provider miss and cannot represent a stored JSON null.

## Provider/store adapter

Implement `Provider.Get` and/or the `Store` methods, register routes with
`NewAdapter`, and run one supervised connection per configured endpoint:

```go
adapter, err := valkyr.NewAdapter()
if err != nil { return err }
if err := adapter.ProvideWithOptions("/weather", "*", weatherProvider{}, valkyr.ProvideOptions{
    Timeout: 250 * time.Millisecond, MissTTL: 30 * time.Second,
}); err != nil { return err }
if err := adapter.Store("/durable", "*", durableStore{}); err != nil { return err }

client, err := valkyr.NewAdapterClient(
    []string{"127.0.0.1:8081"}, "adapter-key", adapter,
    valkyr.AdapterWithMaxConcurrency(16),
)
if err != nil { return err }
return client.Serve(ctx)
```

Callback work is bounded and has a timeout. Overload is returned as a
correlated callback error. Durable writes are acknowledged only after the
store method succeeds. A provider miss returns `value: null` without an
error. Adapter identity is generated once, reused across reconnects, and
registrations are restored independently for every endpoint.

Provider methods may return a raw JSON-compatible value for an unbounded
cache result, `nil` for a miss, or `ProviderValue{Value: value, TTL: &ttl}` for
a successful result with a whole-second TTL. A `ProviderValue` with a nil
`Value` is still a miss. Provider errors, callback timeouts, cancellation, and
overload errors never carry a value TTL. The provider TTL controls Valkyr's
cache lifetime after a successful result; it does not refresh the upstream
source. Registration `MissTTL` and `Timeout` have separate meanings.

## TLS and errors

TLS uses system roots by default. Add private roots with `TLSConfig.CAPEM` or
`TLSConfig.CAFile`, and set `ServerName` when the certificate name differs from
the dial address:

```go
tls := valkyr.TLSConfig{CAFile: "./ca.pem", ServerName: "valkyr.example"}
client, err := valkyr.Dial(ctx, "127.0.0.1:8443",
    valkyr.WithAPIKey("app-key"), valkyr.WithTLS(tls))
```

Use `errors.Is` with `ErrProtocol`, `ErrConnection`, `ErrTimeout`, `ErrAuth`,
`ErrServer`, `ErrOverload`, and `ErrRoute`. Ordered ordinary requests are
serialized; a timeout, cancellation after a write, malformed frame, or unknown
response poisons that connection, so reconnect before retrying an ambiguous
operation.

## Development and integration tests

From this directory:

```sh
GOCACHE=/tmp/valkyr-go-cache gofmt -w .
GOCACHE=/tmp/valkyr-go-cache go test ./...
GOCACHE=/tmp/valkyr-go-cache go vet ./...
GOCACHE=/tmp/valkyr-go-cache go test -race ./...
GOCACHE=/tmp/valkyr-go-cache go test -tags=integration -race ./tests/integration -v
GOCACHE=/tmp/valkyr-go-cache go build ./examples/...
```

The integration suite builds `valkyr-server` when `target/debug/valkyr-server`
is absent, starts isolated loopback servers, and generates temporary TLS
material with `openssl`. The normal unit suite does not require Rust, a
running server, or external credentials.

## Release validation

Release the nested module with a tag named `valkyr-go/v<version>`, for example
`valkyr-go/v0.1.0`. Before publishing the first release, verify that the tag
resolves through the Go module proxy:

```sh
GOPROXY=https://proxy.golang.org \
  go list -m -json github.com/ckphillipe/valkyr/valkyr-go@v0.1.0
```

There is no separate Go artifact-publishing workflow; consumers install the
module directly from the repository tag.

See [`docs/feature_map.md`](docs/feature_map.md) for ownership, flows,
invariants, and test coverage. Runnable examples are in `examples/`.
