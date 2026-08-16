# Valkyr native text protocol v1

This is the language-neutral contract for native TCP/TLS and WebSocket
transports. TCP carries one UTF-8 command or answer per line; WebSocket carries
one complete protocol message per text frame. JSON objects are not protocol
frames. JSON syntax is used only for typed value literals.

Arguments are separated by ASCII whitespace. Quoted arguments use JSON-style
double-quoted escapes. Structured arrays and objects are one balanced value
argument. Frames are bounded to one MiB and reject empty input, trailing
arguments, malformed escapes, overflowed unsigned integers, non-canonical
UUIDs, duplicate or out-of-order options, and unknown keywords. Ordinary
malformed commands receive `KO <message>` and keep the connection usable.
Ambiguous or uncorrelated adapter results close the adapter connection.

## Ordinary commands and answers

Commands are `AUTH`, `GET`, `SET`, `SET_BATCH`, `DELETE`, `MOVE`, `PROVIDE`,
`STORE`, `PING`, and `STATS`; their exact grammar and answer vocabulary are in
[`docs/commands.md`](commands.md). `GET` values answer as `SET` and echo the
originating namespace and key. `MISS` is interpreted using the originating
command: authentication warm-up for `AUTH` and provider refresh for `GET`.

Normal requests are ordered and have no request ID. A timeout, invalid answer,
or route-mismatched `SET` poisons the ordinary connection.

## Adapter callbacks

Server-to-adapter commands carry a mandatory canonical UUID:

```text
QUERY <id> <namespace> <key>
PERSIST_SET <id> <namespace> <key> <value> [EX <seconds>]
PERSIST_SET_BATCH <id> <namespace> <key> <value> ... [EX <seconds>]
PERSIST_DELETE <id> <namespace> [<key-pattern>]
PERSIST_MOVE <id> <source> <destination>
```

Results must echo the ID and callback family:

```text
OPERATION <id> OK
OPERATION <id> KO <message>
QUERY_RESULT <id> SET <namespace> <key> <value> [EX <seconds>]
QUERY_RESULT <id> MISS
QUERY_RESULT <id> KO <message>
```

The server validates the pending callback kind and, for query values, the
echoed route before completing the callback. Callback results may complete out
of order.