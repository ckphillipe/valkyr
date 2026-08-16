"""Wire models and codecs for Valkyr protocol v1.

The native wire format is human-readable text, with JSON retained for
structured value literals and the SDK's in-memory/public dict models.
"""

import json
import math
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from ._errors import ProtocolError

MAX_U64 = 2**64 - 1


def _ensure_mapping(data: Any, context: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ProtocolError(f"{context} must be a JSON object")
    return data


def _required(data: dict[str, Any], name: str) -> Any:
    if name not in data:
        raise ProtocolError(f"missing required field: {name}")
    return data[name]


def _required_string(data: dict[str, Any], name: str) -> str:
    value = _required(data, name)
    if not isinstance(value, str):
        raise ProtocolError(f"field {name} must be a string")
    return value


def _optional_string(data: dict[str, Any], name: str) -> str | None:
    value = data.get(name)
    if value is not None and not isinstance(value, str):
        raise ProtocolError(f"field {name} must be a string or null")
    return value


def _required_non_negative_int(data: dict[str, Any], name: str) -> int:
    value = _required(data, name)
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_U64:
        raise ProtocolError(f"field {name} must be a u64 integer")
    return value


def _optional_non_negative_int(data: dict[str, Any], name: str) -> int | None:
    value = data.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_U64:
        raise ProtocolError(f"field {name} must be a u64 integer or null")
    return value


def _required_uuid(data: dict[str, Any], name: str) -> str:
    value = _required_string(data, name)
    try:
        UUID(value)
    except ValueError as exc:
        raise ProtocolError(f"field {name} must be a UUID string") from exc
    if str(UUID(value)) != value:
        raise ProtocolError(f"field {name} must use canonical UUID encoding")
    return value


def _optional_uuid(data: dict[str, Any], name: str) -> str | None:
    value = _optional_string(data, name)
    if value is None:
        return None
    try:
        UUID(value)
    except ValueError as exc:
        raise ProtocolError(f"field {name} must be a UUID string or null") from exc
    if str(UUID(value)) != value:
        raise ProtocolError(f"field {name} must use canonical UUID encoding")
    return value


def _required_list(data: dict[str, Any], name: str) -> list[Any]:
    value = _required(data, name)
    if not isinstance(value, list):
        raise ProtocolError(f"field {name} must be a list")
    return value


@dataclass(frozen=True)
class SetEntry:
    key: str
    value: Any

    def to_dict(self) -> dict:
        return {"key": self.key, "value": self.value}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SetEntry":
        data = _ensure_mapping(data, "set entry")
        return cls(key=_required_string(data, "key"), value=_required(data, "value"))


@dataclass(frozen=True)
class Stats:
    requests: int
    hits: int
    misses: int
    values: int

    def to_dict(self) -> dict:
        return {
            "type": "stats",
            "requests": self.requests,
            "hits": self.hits,
            "misses": self.misses,
            "values": self.values,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Stats":
        data = _ensure_mapping(data, "stats")
        return cls(
            requests=_required_non_negative_int(data, "requests"),
            hits=_required_non_negative_int(data, "hits"),
            misses=_required_non_negative_int(data, "misses"),
            values=_required_non_negative_int(data, "values"),
        )


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Auth:
    api_key: str
    adapter_instance: str | None = None
    type: str = field(default="auth", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "api_key": self.api_key,
            "adapter_instance": self.adapter_instance,
        }


@dataclass(frozen=True)
class Get:
    namespace: str
    key: str
    type: str = field(default="get", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type, "namespace": self.namespace, "key": self.key}


@dataclass(frozen=True)
class Set:
    namespace: str
    key: str
    value: Any
    ttl_seconds: int | None = None
    type: str = field(default="set", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "namespace": self.namespace,
            "key": self.key,
            "value": self.value,
            "ttl_seconds": self.ttl_seconds,
        }


@dataclass(frozen=True)
class SetBatch:
    namespace: str
    entries: list[SetEntry]
    ttl_seconds: int | None = None
    type: str = field(default="set_batch", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "namespace": self.namespace,
            "entries": [entry.to_dict() for entry in self.entries],
            "ttl_seconds": self.ttl_seconds,
        }


@dataclass(frozen=True)
class Delete:
    namespace: str
    key_pattern: str | None = None
    type: str = field(default="delete", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "namespace": self.namespace,
            "key_pattern": self.key_pattern,
        }


@dataclass(frozen=True)
class Move:
    source: str
    destination: str
    type: str = field(default="move", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "source": self.source,
            "destination": self.destination,
        }


@dataclass(frozen=True)
class Provide:
    namespace_pattern: str
    key_pattern: str
    max_rate: int | None = None
    timeout: int | None = None
    miss_ttl: int | None = None
    type: str = field(default="provide", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "namespace_pattern": self.namespace_pattern,
            "key_pattern": self.key_pattern,
            "max_rate": self.max_rate,
            "timeout": self.timeout,
            "miss_ttl": self.miss_ttl,
        }


@dataclass(frozen=True)
class Store:
    namespace_pattern: str
    key_pattern: str
    type: str = field(default="store", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "namespace_pattern": self.namespace_pattern,
            "key_pattern": self.key_pattern,
        }


@dataclass(frozen=True)
class Ping:
    type: str = field(default="ping", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type}


@dataclass(frozen=True)
class StatsCommand:
    type: str = field(default="stats", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type}


Command = Auth | Get | Set | SetBatch | Delete | Move | Provide | Store | Ping | StatsCommand


_COMMAND_TYPES: dict[str, type] = {
    "auth": Auth,
    "get": Get,
    "set": Set,
    "set_batch": SetBatch,
    "delete": Delete,
    "move": Move,
    "provide": Provide,
    "store": Store,
    "ping": Ping,
    "stats": StatsCommand,
}


def command_from_dict(data: dict[str, Any]) -> Command:
    try:
        type_name = _type_name(data)
        cls = _COMMAND_TYPES.get(type_name)
        if cls is None:
            raise ProtocolError(f"unknown command type: {type_name}")
        if cls is Auth:
            return Auth(
                api_key=_required_string(data, "api_key"),
                adapter_instance=_optional_uuid(data, "adapter_instance"),
            )
        if cls is Get:
            return Get(
                namespace=_required_string(data, "namespace"),
                key=_required_string(data, "key"),
            )
        if cls is Set:
            return Set(
                namespace=_required_string(data, "namespace"),
                key=_required_string(data, "key"),
                value=_required(data, "value"),
                ttl_seconds=_optional_non_negative_int(data, "ttl_seconds"),
            )
        if cls is SetBatch:
            entries = [SetEntry.from_dict(entry) for entry in _required_list(data, "entries")]
            return SetBatch(
                namespace=_required_string(data, "namespace"),
                entries=entries,
                ttl_seconds=_optional_non_negative_int(data, "ttl_seconds"),
            )
        if cls is Delete:
            return Delete(
                namespace=_required_string(data, "namespace"),
                key_pattern=_optional_string(data, "key_pattern"),
            )
        if cls is Move:
            return Move(
                source=_required_string(data, "source"),
                destination=_required_string(data, "destination"),
            )
        if cls is Provide:
            return Provide(
                namespace_pattern=_required_string(data, "namespace_pattern"),
                key_pattern=_required_string(data, "key_pattern"),
                max_rate=_optional_non_negative_int(data, "max_rate"),
                timeout=_optional_non_negative_int(data, "timeout"),
                miss_ttl=_optional_non_negative_int(data, "miss_ttl"),
            )
        if cls is Store:
            return Store(
                namespace_pattern=_required_string(data, "namespace_pattern"),
                key_pattern=_required_string(data, "key_pattern"),
            )
        if cls in (Ping, StatsCommand):
            return cls()
        raise ProtocolError(f"unknown command type: {type_name}")
    except ProtocolError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ProtocolError(f"malformed command frame: {exc}") from exc


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Ok:
    type: str = field(default="ok", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type}


@dataclass(frozen=True)
class Value:
    value: Any
    ttl_seconds: int | None = None
    type: str = field(default="value", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "value": self.value,
            "ttl_seconds": self.ttl_seconds,
        }


@dataclass(frozen=True)
class Miss:
    retry_after_ms: int
    type: str = field(default="miss", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type, "retry_after_ms": self.retry_after_ms}


@dataclass(frozen=True)
class Unknown:
    type: str = field(default="unknown", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type}


@dataclass(frozen=True)
class AuthSuccess:
    client_id: str
    session_ttl_seconds: int
    type: str = field(default="auth_success", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "client_id": self.client_id,
            "session_ttl_seconds": self.session_ttl_seconds,
        }


@dataclass(frozen=True)
class AuthPending:
    retry_after_ms: int
    type: str = field(default="auth_pending", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type, "retry_after_ms": self.retry_after_ms}


@dataclass(frozen=True)
class AuthFailure:
    message: str
    type: str = field(default="auth_failure", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type, "message": self.message}


@dataclass(frozen=True)
class Pong:
    type: str = field(default="pong", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type}


@dataclass(frozen=True)
class StatsResponse:
    stats: Stats
    type: str = field(default="stats", init=False)

    def to_dict(self) -> dict:
        return self.stats.to_dict()


@dataclass(frozen=True)
class Error:
    message: str
    type: str = field(default="error", init=False)

    def to_dict(self) -> dict:
        return {"type": self.type, "message": self.message}


Response = (
    Ok
    | Value
    | Miss
    | Unknown
    | AuthSuccess
    | AuthPending
    | AuthFailure
    | Pong
    | StatsResponse
    | Error
)

_RESPONSE_TYPES: dict[str, type] = {
    "ok": Ok,
    "value": Value,
    "miss": Miss,
    "unknown": Unknown,
    "auth_success": AuthSuccess,
    "auth_pending": AuthPending,
    "auth_failure": AuthFailure,
    "pong": Pong,
    "stats": StatsResponse,
    "error": Error,
}


def response_from_dict(data: dict[str, Any]) -> Response:
    try:
        type_name = _type_name(data)
        cls = _RESPONSE_TYPES.get(type_name)
        if cls is None:
            raise ProtocolError(f"unknown response type: {type_name}")
        if cls is Ok:
            return Ok()
        if cls is Value:
            return Value(
                value=_required(data, "value"),
                ttl_seconds=_optional_non_negative_int(data, "ttl_seconds"),
            )
        if cls is Miss:
            return Miss(retry_after_ms=_required_non_negative_int(data, "retry_after_ms"))
        if cls is Unknown:
            return Unknown()
        if cls is AuthSuccess:
            return AuthSuccess(
                client_id=_required_string(data, "client_id"),
                session_ttl_seconds=_required_non_negative_int(data, "session_ttl_seconds"),
            )
        if cls is AuthPending:
            return AuthPending(retry_after_ms=_required_non_negative_int(data, "retry_after_ms"))
        if cls is AuthFailure:
            return AuthFailure(message=_required_string(data, "message"))
        if cls is Pong:
            return Pong()
        if cls is StatsResponse:
            return StatsResponse(stats=Stats.from_dict(data))
        if cls is Error:
            return Error(message=_required_string(data, "message"))
        raise ProtocolError(f"unknown response type: {type_name}")
    except ProtocolError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ProtocolError(f"malformed response frame: {exc}") from exc


# ---------------------------------------------------------------------------
# Server callbacks
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Query:
    request_id: str
    namespace: str
    key: str
    type: str = field(default="query", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "request_id": self.request_id,
            "namespace": self.namespace,
            "key": self.key,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Query":
        data = _ensure_mapping(data, "query command")
        return cls(
            request_id=_required_uuid(data, "request_id"),
            namespace=_required_string(data, "namespace"),
            key=_required_string(data, "key"),
        )


@dataclass(frozen=True)
class PersistSet:
    request_id: str
    namespace: str
    key: str
    value: Any
    ttl_seconds: int | None
    type: str = field(default="persist_set", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "request_id": self.request_id,
            "namespace": self.namespace,
            "key": self.key,
            "value": self.value,
            "ttl_seconds": self.ttl_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PersistSet":
        data = _ensure_mapping(data, "persist set command")
        return cls(
            request_id=_required_uuid(data, "request_id"),
            namespace=_required_string(data, "namespace"),
            key=_required_string(data, "key"),
            value=_required(data, "value"),
            ttl_seconds=_optional_non_negative_int(data, "ttl_seconds"),
        )


@dataclass(frozen=True)
class PersistSetBatch:
    request_id: str
    namespace: str
    entries: list[SetEntry]
    ttl_seconds: int | None
    type: str = field(default="persist_set_batch", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "request_id": self.request_id,
            "namespace": self.namespace,
            "entries": [entry.to_dict() for entry in self.entries],
            "ttl_seconds": self.ttl_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PersistSetBatch":
        data = _ensure_mapping(data, "persist set batch command")
        return cls(
            request_id=_required_uuid(data, "request_id"),
            namespace=_required_string(data, "namespace"),
            entries=[SetEntry.from_dict(entry) for entry in _required_list(data, "entries")],
            ttl_seconds=_optional_non_negative_int(data, "ttl_seconds"),
        )


@dataclass(frozen=True)
class PersistDelete:
    request_id: str
    namespace: str
    key_pattern: str | None
    type: str = field(default="persist_delete", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "request_id": self.request_id,
            "namespace": self.namespace,
            "key_pattern": self.key_pattern,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PersistDelete":
        data = _ensure_mapping(data, "persist delete command")
        return cls(
            request_id=_required_uuid(data, "request_id"),
            namespace=_required_string(data, "namespace"),
            key_pattern=_optional_string(data, "key_pattern"),
        )


@dataclass(frozen=True)
class PersistMove:
    request_id: str
    source: str
    destination: str
    type: str = field(default="persist_move", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "request_id": self.request_id,
            "source": self.source,
            "destination": self.destination,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PersistMove":
        data = _ensure_mapping(data, "persist move command")
        return cls(
            request_id=_required_uuid(data, "request_id"),
            source=_required_string(data, "source"),
            destination=_required_string(data, "destination"),
        )


ServerCommand = Query | PersistSet | PersistSetBatch | PersistDelete | PersistMove

_SERVER_COMMAND_TYPES: dict[str, Any] = {
    "query": Query,
    "persist_set": PersistSet,
    "persist_set_batch": PersistSetBatch,
    "persist_delete": PersistDelete,
    "persist_move": PersistMove,
}


def server_command_from_dict(data: dict[str, Any]) -> ServerCommand:
    try:
        type_name = _type_name(data)
        cls = _SERVER_COMMAND_TYPES.get(type_name)
        if cls is None:
            raise ProtocolError(f"unknown server command type: {type_name}")
        return cls.from_dict(data)  # type: ignore[no-any-return]
    except ProtocolError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ProtocolError(f"malformed server command frame: {exc}") from exc


def _type_name(data: dict[str, Any]) -> str:
    if not isinstance(data, dict):
        raise ProtocolError("frame must be a JSON object")
    type_name = data.get("type")
    if not isinstance(type_name, str):
        raise ProtocolError("frame type must be a string")
    return type_name


# ---------------------------------------------------------------------------
# Server results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OperationResult:
    request_id: str
    error: str | None = None
    type: str = field(default="operation", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "request_id": self.request_id,
            "error": self.error,
        }


@dataclass(frozen=True)
class QueryResult:
    request_id: str
    value: Any | None = None
    error: str | None = None
    ttl_seconds: int | None = None
    type: str = field(default="query", init=False)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "request_id": self.request_id,
            "value": self.value,
            "error": self.error,
            "ttl_seconds": self.ttl_seconds,
        }


ServerResult = OperationResult | QueryResult


SERVER_RESULT_TYPES: dict[str, type] = {
    "operation": OperationResult,
    "query": QueryResult,
}


def server_result_from_dict(data: dict[str, Any]) -> ServerResult:
    try:
        type_name = _type_name(data)
        if type_name == "operation":
            return OperationResult(
                request_id=_required_uuid(data, "request_id"),
                error=_optional_string(data, "error"),
            )
        if type_name == "query":
            return QueryResult(
                request_id=_required_uuid(data, "request_id"),
                value=data.get("value"),
                error=_optional_string(data, "error"),
                ttl_seconds=_optional_non_negative_int(data, "ttl_seconds"),
            )
        raise ProtocolError(f"unknown server result type: {type_name}")
    except ProtocolError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ProtocolError(f"malformed server result frame: {exc}") from exc


def server_result_to_dict(result: ServerResult) -> dict:
    return result.to_dict()


# ---------------------------------------------------------------------------
# Human-readable text protocol
# ---------------------------------------------------------------------------


class _Token(str):
    quoted: bool

    def __new__(cls, value: str, *, quoted: bool = False) -> "_Token":
        token = super().__new__(cls, value)
        token.quoted = quoted
        return token


def _is_ascii_whitespace(char: str) -> bool:
    return char in " \t\n\v\f\r"


def _tokens(line: str) -> list[_Token]:
    if not line or len(line.encode("utf-8")) > 1024 * 1024:
        raise ProtocolError("empty or oversized frame")
    if "\n" in line or "\r" in line:
        raise ProtocolError("frame contains a line break")
    tokens: list[_Token] = []
    index = 0
    while index < len(line):
        while index < len(line) and _is_ascii_whitespace(line[index]):
            index += 1
        if index == len(line):
            break
        start = index
        if line[index] == '"':
            try:
                value, end = json.JSONDecoder().raw_decode(line[index:])
            except json.JSONDecodeError as exc:
                raise ProtocolError("invalid quoted token") from exc
            if not isinstance(value, str):
                raise ProtocolError("quoted token must be a string")
            index += end
            tokens.append(_Token(value, quoted=True))
            continue
        if line[index] in "[{":
            stack = [line[index]]
            index += 1
            quoted = False
            escaped = False
            while index < len(line):
                char = line[index]
                index += 1
                if quoted:
                    if escaped:
                        escaped = False
                    elif char == "\\":
                        escaped = True
                    elif char == '"':
                        quoted = False
                    continue
                if char == '"':
                    quoted = True
                elif char in "[{":
                    stack.append(char)
                elif char in "]}":
                    expected = "[" if char == "]" else "{"
                    if not stack or stack.pop() != expected:
                        raise ProtocolError("unbalanced structured value")
                    if not stack:
                        raw = line[start:index]
                        try:
                            json.loads(raw)
                        except json.JSONDecodeError as exc:
                            raise ProtocolError("invalid structured value") from exc
                        tokens.append(_Token(raw))
                        break
            else:
                raise ProtocolError("incomplete structured value")
            continue
        while index < len(line) and not _is_ascii_whitespace(line[index]):
            index += 1
        tokens.append(_Token(line[start:index]))
    if not tokens:
        raise ProtocolError("empty frame")
    return tokens


def _is_unquoted(tokens: list[_Token], index: int, value: str) -> bool:
    return index < len(tokens) and not tokens[index].quoted and tokens[index] == value


def _quote(value: str) -> str:
    if value and all(char.isascii() and (char.isalnum() or char in "_./:*~$-") for char in value):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _batch_key(value: str) -> str:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        if value == "EX"
        else _quote(value)
    )


def _line_value(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ProtocolError("invalid value literal") from exc


def _json_value(token: str) -> Any:
    if isinstance(token, _Token) and token.quoted:
        return str(token)

    def parse_finite_float(value: str) -> float:
        result = float(value)
        if not math.isfinite(result):
            raise ValueError(value)
        return result

    try:
        return json.loads(
            token,
            parse_float=parse_finite_float,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise ProtocolError("invalid value literal") from exc


def command_to_line(command: Command) -> str:
    if isinstance(command, Auth):
        line = f"AUTH {_quote(command.api_key)}"
        if command.adapter_instance is not None:
            line += f" ADAPTER {command.adapter_instance}"
        return line
    if isinstance(command, Get):
        return f"GET {_quote(command.namespace)} {_quote(command.key)}"
    if isinstance(command, Set):
        line = f"SET {_quote(command.namespace)} {_quote(command.key)} {_line_value(command.value)}"
        return line + (f" EX {command.ttl_seconds}" if command.ttl_seconds is not None else "")
    if isinstance(command, SetBatch):
        if not command.entries:
            raise ProtocolError("SET_BATCH requires an entry")
        line = f"SET_BATCH {_quote(command.namespace)} " + " ".join(
            f"{_batch_key(entry.key)} {_line_value(entry.value)}" for entry in command.entries
        )
        return line + (f" EX {command.ttl_seconds}" if command.ttl_seconds is not None else "")
    if isinstance(command, Delete):
        return f"DELETE {_quote(command.namespace)}" + (
            f" {_quote(command.key_pattern)}" if command.key_pattern is not None else ""
        )
    if isinstance(command, Move):
        return f"MOVE {_quote(command.source)} {_quote(command.destination)}"
    if isinstance(command, Provide):
        line = f"PROVIDE {_quote(command.namespace_pattern)} {_quote(command.key_pattern)}"
        for name, value in (
            ("MAX_RATE", command.max_rate),
            ("TIMEOUT", command.timeout),
            ("MISS_TTL", command.miss_ttl),
        ):
            if value is not None:
                line += f" {name} {value}"
        return line
    if isinstance(command, Store):
        return f"STORE {_quote(command.namespace_pattern)} {_quote(command.key_pattern)}"
    if isinstance(command, Ping):
        return "PING"
    return "STATS"


def _u64(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise ProtocolError("invalid unsigned integer") from exc
    if result < 0 or result > MAX_U64:
        raise ProtocolError("unsigned integer overflow")
    return result


def _canonical_uuid(value: str) -> str:
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ProtocolError("invalid canonical UUID") from exc
    if str(parsed) != value:
        raise ProtocolError("UUID must use canonical lowercase encoding")
    return value


def command_from_line(line: str) -> Command:
    t = _tokens(line)
    keyword = "" if t[0].quoted else t[0]
    try:
        if keyword == "AUTH":
            if len(t) not in (2, 4) or len(t) == 4 and not _is_unquoted(t, 2, "ADAPTER"):
                raise ProtocolError("invalid AUTH arguments")
            return Auth(t[1], _canonical_uuid(t[3]) if len(t) == 4 else None)
        if keyword == "GET" and len(t) == 3:
            return Get(t[1], t[2])
        if keyword == "SET" and len(t) in (4, 6):
            if len(t) == 6 and not _is_unquoted(t, 4, "EX"):
                raise ProtocolError("invalid SET option")
            return Set(t[1], t[2], _json_value(t[3]), _u64(t[5]) if len(t) == 6 else None)
        if keyword == "SET_BATCH" and len(t) >= 4:
            end = len(t) - 2 if len(t) >= 4 and not t[-2].quoted and t[-2] == "EX" else len(t)
            if (end - 2) % 2:
                raise ProtocolError("SET_BATCH requires key/value pairs")
            entries = [SetEntry(t[index], _json_value(t[index + 1])) for index in range(2, end, 2)]
            if not entries:
                raise ProtocolError("SET_BATCH requires an entry")
            return SetBatch(t[1], entries, _u64(t[-1]) if end != len(t) else None)
        if keyword == "DELETE" and len(t) in (2, 3):
            return Delete(t[1], t[2] if len(t) == 3 else None)
        if keyword == "MOVE" and len(t) == 3:
            return Move(t[1], t[2])
        if keyword == "PROVIDE" and len(t) >= 3:
            values: dict[str, int | None] = {"MAX_RATE": None, "TIMEOUT": None, "MISS_TTL": None}
            option_order = {"MAX_RATE": 0, "TIMEOUT": 1, "MISS_TTL": 2}
            index = 3
            last = -1
            while index < len(t):
                name = t[index]
                if (
                    name not in option_order
                    or t[index].quoted
                    or option_order[name] <= last
                    or index + 1 == len(t)
                ):
                    raise ProtocolError("invalid PROVIDE options")
                values[name] = _u64(t[index + 1])
                last = option_order[name]
                index += 2
            if index != len(t):
                raise ProtocolError("invalid PROVIDE options")
            max_rate = values["MAX_RATE"]
            if max_rate is not None and max_rate > 2**32 - 1:
                raise ProtocolError("MAX_RATE overflow")
            return Provide(t[1], t[2], max_rate, values["TIMEOUT"], values["MISS_TTL"])
        if keyword == "STORE" and len(t) == 3:
            return Store(t[1], t[2])
        if keyword == "PING" and len(t) == 1:
            return Ping()
        if keyword == "STATS" and len(t) == 1:
            return StatsCommand()
    except (ValueError, json.JSONDecodeError) as exc:
        raise ProtocolError("invalid value literal") from exc
    raise ProtocolError("invalid command frame")


def response_to_line(command: Command, response: Response) -> str:
    if isinstance(response, Ok):
        if not isinstance(command, (Set, SetBatch, Delete, Move, Provide, Store)):
            raise ProtocolError("OK answer has invalid context")
        return "OK"
    if isinstance(response, Value):
        if not isinstance(command, Get):
            raise ProtocolError("value answer requires GET context")
        line = (
            f"SET {_quote(command.namespace)} {_quote(command.key)} {_line_value(response.value)}"
        )
        return line + (f" EX {response.ttl_seconds}" if response.ttl_seconds is not None else "")
    if isinstance(response, Miss):
        if not isinstance(command, Get):
            raise ProtocolError("MISS answer has invalid context")
        return f"MISS {response.retry_after_ms}"
    if isinstance(response, AuthPending):
        if not isinstance(command, Auth):
            raise ProtocolError("MISS answer has invalid context")
        return f"MISS {response.retry_after_ms}"
    if isinstance(response, Unknown):
        if not isinstance(command, Get):
            raise ProtocolError("UNKNOWN answer has invalid context")
        return "UNKNOWN"
    if isinstance(response, AuthSuccess):
        if not isinstance(command, Auth):
            raise ProtocolError("authentication success requires AUTH context")
        return f"OK {_quote(response.client_id)} TTL {response.session_ttl_seconds}"
    if isinstance(response, AuthFailure):
        if not isinstance(command, Auth):
            raise ProtocolError("authentication failure requires AUTH context")
        return f"KO {_quote(response.message)}"
    if isinstance(response, Error):
        if isinstance(command, Auth):
            raise ProtocolError("error answer has invalid AUTH context")
        return f"KO {_quote(response.message)}"
    if isinstance(response, Pong):
        if not isinstance(command, Ping):
            raise ProtocolError("PONG answer has invalid context")
        return "PONG"
    stats = response.stats
    if not isinstance(command, StatsCommand):
        raise ProtocolError("STATS answer has invalid context")
    return (
        f"STATS REQUESTS {stats.requests} HITS {stats.hits} "
        f"MISSES {stats.misses} VALUES {stats.values}"
    )


def response_from_line(command: Command, line: str) -> Response:
    t = _tokens(line)
    if _is_unquoted(t, 0, "OK") and isinstance(command, Auth):
        if len(t) != 4 or not _is_unquoted(t, 2, "TTL"):
            raise ProtocolError("invalid authentication answer")
        return AuthSuccess(t[1], _u64(t[3]))
    if (
        _is_unquoted(t, 0, "OK")
        and len(t) == 1
        and isinstance(command, (Set, SetBatch, Delete, Move, Provide, Store))
    ):
        return Ok()
    if _is_unquoted(t, 0, "SET") and isinstance(command, Get) and len(t) in (4, 6):
        if (
            t[1] != command.namespace
            or t[2] != command.key
            or len(t) == 6
            and not _is_unquoted(t, 4, "EX")
        ):
            raise ProtocolError("SET answer route mismatch")
        return Value(_json_value(t[3]), _u64(t[5]) if len(t) == 6 else None)
    if _is_unquoted(t, 0, "MISS") and len(t) == 2:
        if isinstance(command, Auth):
            return AuthPending(_u64(t[1]))
        if isinstance(command, Get):
            return Miss(_u64(t[1]))
        raise ProtocolError("MISS answer has invalid context")
    if _is_unquoted(t, 0, "UNKNOWN") and len(t) == 1 and isinstance(command, Get):
        return Unknown()
    if _is_unquoted(t, 0, "PONG") and len(t) == 1 and isinstance(command, Ping):
        return Pong()
    if (
        _is_unquoted(t, 0, "STATS")
        and isinstance(command, StatsCommand)
        and len(t) == 9
        and all(
            _is_unquoted(t, index, label)
            for index, label in zip(
                (1, 3, 5, 7), ("REQUESTS", "HITS", "MISSES", "VALUES"), strict=True
            )
        )
    ):
        return StatsResponse(Stats(_u64(t[2]), _u64(t[4]), _u64(t[6]), _u64(t[8])))
    if _is_unquoted(t, 0, "KO") and len(t) == 2:
        return AuthFailure(t[1]) if isinstance(command, Auth) else Error(t[1])
    raise ProtocolError("answer does not match command")


def server_command_to_line(command: ServerCommand) -> str:
    if isinstance(command, Query):
        return f"QUERY {command.request_id} {_quote(command.namespace)} {_quote(command.key)}"
    if isinstance(command, PersistSet):
        line = (
            f"PERSIST_SET {command.request_id} {_quote(command.namespace)} "
            f"{_quote(command.key)} {_line_value(command.value)}"
        )
        return line + (f" EX {command.ttl_seconds}" if command.ttl_seconds is not None else "")
    if isinstance(command, PersistSetBatch):
        if not command.entries:
            raise ProtocolError("PERSIST_SET_BATCH requires an entry")
        line = f"PERSIST_SET_BATCH {command.request_id} {_quote(command.namespace)} " + " ".join(
            f"{_batch_key(e.key)} {_line_value(e.value)}" for e in command.entries
        )
        return line + (f" EX {command.ttl_seconds}" if command.ttl_seconds is not None else "")
    if isinstance(command, PersistDelete):
        return f"PERSIST_DELETE {command.request_id} {_quote(command.namespace)}" + (
            f" {_quote(command.key_pattern)}" if command.key_pattern is not None else ""
        )
    return (
        f"PERSIST_MOVE {command.request_id} {_quote(command.source)} {_quote(command.destination)}"
    )


def server_command_from_line(line: str) -> ServerCommand:
    t = _tokens(line)
    if len(t) < 2:
        raise ProtocolError("invalid server command frame")
    request_id = _canonical_uuid(t[1])
    if _is_unquoted(t, 0, "QUERY") and len(t) == 4:
        return Query(request_id, t[2], t[3])
    if _is_unquoted(t, 0, "PERSIST_SET") and len(t) in (5, 7):
        if len(t) == 7 and not _is_unquoted(t, 5, "EX"):
            raise ProtocolError("invalid PERSIST_SET option")
        return PersistSet(
            request_id, t[2], t[3], _json_value(t[4]), _u64(t[6]) if len(t) == 7 else None
        )
    if _is_unquoted(t, 0, "PERSIST_SET_BATCH") and len(t) >= 5:
        end = len(t) - 2 if len(t) >= 5 and not t[-2].quoted and t[-2] == "EX" else len(t)
        if (end - 3) % 2:
            raise ProtocolError("invalid callback batch")
        entries = [SetEntry(t[index], _json_value(t[index + 1])) for index in range(3, end, 2)]
        if not entries:
            raise ProtocolError("PERSIST_SET_BATCH requires an entry")
        return PersistSetBatch(request_id, t[2], entries, _u64(t[-1]) if end != len(t) else None)
    if _is_unquoted(t, 0, "PERSIST_DELETE") and len(t) in (3, 4):
        return PersistDelete(request_id, t[2], t[3] if len(t) == 4 else None)
    if _is_unquoted(t, 0, "PERSIST_MOVE") and len(t) == 4:
        return PersistMove(request_id, t[2], t[3])
    raise ProtocolError("invalid server command frame")


def server_result_to_line(command: ServerCommand, result: ServerResult) -> str:
    if (
        isinstance(command, Query)
        and isinstance(result, QueryResult)
        and command.request_id == result.request_id
    ):
        if result.error is not None:
            return f"QUERY_RESULT {result.request_id} KO {_quote(result.error)}"
        if result.value is None:
            return f"QUERY_RESULT {result.request_id} MISS"
        line = (
            f"QUERY_RESULT {result.request_id} SET {_quote(command.namespace)} "
            f"{_quote(command.key)} {_line_value(result.value)}"
        )
        return line + (f" EX {result.ttl_seconds}" if result.ttl_seconds is not None else "")
    if (
        not isinstance(command, Query)
        and isinstance(result, OperationResult)
        and result.request_id == command.request_id
    ):
        return f"OPERATION {result.request_id} " + (
            f"KO {_quote(result.error)}" if result.error else "OK"
        )
    raise ProtocolError("callback result does not match command")


def server_result_from_line(command: ServerCommand, line: str) -> ServerResult:
    t = _tokens(line)
    if len(t) < 3 or _canonical_uuid(t[1]) != command.request_id:
        raise ProtocolError("callback correlation mismatch")
    if isinstance(command, Query):
        if not _is_unquoted(t, 0, "QUERY_RESULT"):
            raise ProtocolError("callback result kind mismatch")
        if _is_unquoted(t, 2, "MISS") and len(t) == 3:
            return QueryResult(t[1])
        if _is_unquoted(t, 2, "KO") and len(t) == 4:
            return QueryResult(t[1], error=t[3])
        if _is_unquoted(t, 2, "SET") and len(t) in (6, 8):
            if t[3] != command.namespace or t[4] != command.key:
                raise ProtocolError("callback route mismatch")
            if len(t) == 8 and not _is_unquoted(t, 6, "EX"):
                raise ProtocolError("invalid query result option")
            return QueryResult(
                t[1], _json_value(t[5]), ttl_seconds=_u64(t[7]) if len(t) == 8 else None
            )
        raise ProtocolError("invalid query result")
    if not _is_unquoted(t, 0, "OPERATION"):
        raise ProtocolError("callback result kind mismatch")
    if _is_unquoted(t, 2, "OK") and len(t) == 3:
        return OperationResult(t[1])
    if _is_unquoted(t, 2, "KO") and len(t) == 4:
        return OperationResult(t[1], error=t[3])
    raise ProtocolError("invalid operation result")
