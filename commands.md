# Valkyr human-readable line protocol

This document defines the human-readable protocol that replaces the current
JSON wire messages. It is the target protocol for native TCP and WebSocket
clients and adapters.

The Valkyr server accepts this syntax as protocol v1. There is no JSON
envelope or compatibility negotiation; JSON remains available only inside
structured value literals.

## Framing and syntax

- Native TCP uses one UTF-8 command or response per line.
- WebSocket uses one complete command or response per text message. A message
  must not contain a second command.
- Commands use uppercase keywords and space-separated arguments.
- Arguments containing spaces, tabs, quotes, or backslashes use double quotes.
- Inside a quoted argument, `\\`, `\"`, `\n`, `\r`, and `\t` represent a
  backslash, quote, newline, carriage return, and tab.
- A response message is either a single line or a quoted message argument, as
  shown below. Clients must not split a quoted value on whitespace.
- A code block wrapped with `\\` for readability still represents one
  physical protocol line. The backslash and formatting newline are not sent.

For example:

```text
AUTH my-app-key
GET /weather/loc::Paris temperature
SET /weather/loc::Paris temperature 22 EX 300
```

Namespaces, contexts, keys, and patterns are UTF-8 strings. A namespace may
include a context after `::`; a context move must keep the same base
namespace.

## Values

The command protocol has no JSON command envelope. Values retain the current
Valkyr value model and use these human-readable literals:

| Literal | Value |
| --- | --- |
| `null` | Null |
| `true`, `false` | Boolean |
| `22`, `-4`, `3.14` | Number |
| `"Ada Lovelace"` | String |
| `["red", "green"]` | Array |
| `{ "name": "Ada", "active": true }` | Object |

Strings and object keys are quoted. Arrays and objects may contain nested
values. A value literal is one argument even when it contains spaces.

The unquoted word `EX` is reserved as the final TTL option in `SET` and
`SET_BATCH`. Quote a key named `EX`.

## Request and answer reference

| Request | Possible answer |
| --- | --- |
| `AUTH <api-key>`<br>`AUTH <api-key> ADAPTER <uuid>` | `OK <client-id> TTL <seconds>`<br>`MISS <retry-after-ms>`<br>`KO <message>` |
| `GET <namespace> <key>` | `SET <namespace> <key> <value> [EX <seconds>]`<br>`MISS <retry-after-ms>`<br>`UNKNOWN`<br>`KO <message>` |
| `SET <namespace> <key> <value> [EX <seconds>]` | `OK`<br>`KO <message>` |
| `SET_BATCH <namespace> <key> <value> ... [EX <seconds>]` | `OK`<br>`KO <message>` |
| `DELETE <namespace> [<key-pattern>]` | `OK`<br>`KO <message>` |
| `MOVE <source> <destination>` | `OK`<br>`KO <message>` |
| `PROVIDE <namespace-pattern> <key-pattern> [MAX_RATE <n>] [TIMEOUT <ms>] [MISS_TTL <seconds>]` | `OK`<br>`KO <message>` |
| `STORE <namespace-pattern> <key-pattern>` | `OK`<br>`KO <message>` |
| `PING` | `PONG` |
| `STATS` | `STATS REQUESTS <n> HITS <n> MISSES <n> VALUES <n>`<br>`KO <message>` |

`DELETE` without a key deletes the whole namespace or context. A key pattern
ending in `*` deletes matching keys. `SET_BATCH` contains repeated key/value
pairs and applies one optional TTL to the whole batch.

## Authentication

Authenticate once after connecting:

```text
AUTH <api-key>
```

Example:

```text
AUTH my-app-key
```

Adapters include their instance identity when registering provider or store
routes:

```text
AUTH my-adapter-key ADAPTER 550e8400-e29b-41d4-a716-446655440000
```

The server responds with `OK`, `MISS`, or `KO`. A client must authenticate
before sending protected commands.

## Read a value

```text
GET <namespace>[::<context>] <key>
```

Example:

```text
GET /weather/loc::Paris temperature
```

Possible answers:

```text
SET /weather/loc::Paris temperature 22 EX 300
MISS 25
UNKNOWN
KO "provider unavailable"
```

`MISS` carries a retry delay in milliseconds. A successful `SET` answer
includes `EX` when the value has a remaining lifetime; the number is in
seconds.

## Set or replace a value

```text
SET <namespace>[::<context>] <key> <value> [EX <seconds>]
```

Examples:

```text
SET /weather/loc::Paris temperature 22
SET /weather/loc::Paris forecast "sunny and warm" EX 300
SET /users 42 { "name": "Ada", "active": true }
```

The server returns:

```text
OK
```

`EX` sets the value lifetime in whole seconds. Omit it for no command TTL.

## Set or read an encrypted value

Surround an encrypted key with `~` markers:

```text
SET /weather/loc::Paris ~api-token~ "secret-value"
GET /weather/loc::Paris ~api-token~
```

The markers are part of the key syntax. Encrypted operations require the
appropriate `write_encrypted` or `read_encrypted` permission and a registered
`/__secrets` provider.

## Set several values

```text
SET_BATCH <namespace>[::<context>] <key> <value> \
  [<key> <value> ...] [EX <seconds>]
```

Example:

```text
SET_BATCH /weather/loc::Paris \
  temperature 22 \
  humidity 65 \
  conditions "partly cloudy" \
  EX 60
```

All entries are persisted and committed as one operation. `SET_BATCH` returns
`OK` or `KO`.

## Delete values

Delete one key:

```text
DELETE <namespace>[::<context>] <key>
```

Delete keys matching a prefix:

```text
DELETE <namespace>[::<context>] <prefix>*
```

Delete every key in a namespace or context:

```text
DELETE <namespace>[::<context>]
```

Examples:

```text
DELETE /weather/loc::Paris temperature
DELETE /weather/loc::Paris forecast-*
DELETE /weather/loc::Paris
```

The server returns `OK` or `KO`. Delete operations on `/__auth` and
`/__secrets` are rejected.

## Move a context

Move all values between contexts of the same base namespace:

```text
MOVE <namespace>::<source-context> <namespace>::<destination-context>
```

Example:

```text
MOVE /weather/loc::Paris /weather/loc::London
```

The server returns `OK` or `KO`.

## Register a provider

A provider loads missing values on demand:

```text
PROVIDE <namespace-pattern> <key-pattern> \
  [MAX_RATE <requests-per-second>] \
  [TIMEOUT <milliseconds>] \
  [MISS_TTL <seconds>]
```

Example:

```text
PROVIDE /weather/* * MAX_RATE 100 TIMEOUT 250 MISS_TTL 30
```

All options are optional and default to zero or unset as follows:

- `MAX_RATE` limits admitted provider refreshes.
- `TIMEOUT` lets a normal `GET` wait for the shared provider refresh. Zero
  returns `MISS` immediately.
- `MISS_TTL` temporarily caches only a confirmed provider miss.

The server returns `OK` or `KO`.

## Register durable storage

A store persists matching values:

```text
STORE <namespace-pattern> <key-pattern>
```

Example:

```text
STORE /weather/* *
```

The server returns `OK` or `KO`.

## Check connectivity

```text
PING
```

Response:

```text
PONG
```

## Read server statistics

```text
STATS
```

Response:

```text
STATS REQUESTS 10 HITS 5 MISSES 3 VALUES 2
```

The counters are unsigned integers and are connection-independent.

## Client answers

| Answer | Meaning |
| --- | --- |
| `OK` | Mutation or registration succeeded. |
| `SET <namespace> <key> <value> [EX <seconds>]` | A value was found. |
| `MISS <retry-after-ms>` | A provider refresh is pending. |
| `UNKNOWN` | No value or provider result exists. |
| `OK <client-id> TTL <seconds>` | Authentication succeeded. |
| `MISS <retry-after-ms>` | Authentication data is warming. |
| `PONG` | Liveness response. |
| `STATS ...` | Server counters. |
| `KO <message>` | The command failed without closing the connection. |

Normal commands are ordered: the response corresponds to the next command
sent on the same connection. A malformed command receives `KO` and does
not change the authenticated session.

## Adapter callbacks

Provider and store adapters receive server-initiated lines with a request ID.
The adapter must return a result containing the same request ID.

### Server to adapter

```text
QUERY <request-id> <namespace> <key>
PERSIST_SET <request-id> <namespace> <key> <value> [EX <seconds>]
PERSIST_SET_BATCH <request-id> <namespace> <key> <value> [<key> <value> ...] [EX <seconds>]
PERSIST_DELETE <request-id> <namespace> [<key-pattern>]
PERSIST_MOVE <request-id> <source> <destination>
```

### Adapter to server

Successful durable operation:

```text
OPERATION <request-id> OK
```

Failed durable operation:

```text
OPERATION <request-id> KO "database unavailable"
```

Provider value:

```text
QUERY_RESULT <request-id> SET /users 42 { "name": "Ada" } EX 300
```

Provider miss:

```text
QUERY_RESULT <request-id> MISS
```

Provider failure:

```text
QUERY_RESULT <request-id> KO "upstream unavailable"
```

Only `QUERY_RESULT ... MISS` represents a cacheable provider miss. Adapter
callbacks may complete out of order; request IDs correlate them.

## REST comparison

The REST API remains available on the HTTP listener:

| Operation | Native line protocol | REST |
| --- | --- | --- |
| Authenticate | `AUTH <api-key>` | `Authorization: Bearer <api-key>` |
| Read | `GET <namespace> <key>` | `GET /<namespace>?<key>` |
| Set | `SET ...` | `PUT /<namespace>?<key>` |
| Set TTL | `EX <seconds>` | `Valkyr-Ttl: <seconds>` |
| Delete | `DELETE ...` | `DELETE /<namespace>?<key>` |
| Move | `MOVE <source> <destination>` | `PUT` + `Destination` header |

Native TCP defaults to `127.0.0.1:8081`; REST and WebSocket default to
`127.0.0.1:8080`.
