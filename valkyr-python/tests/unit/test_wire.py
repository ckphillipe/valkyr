"""Unit tests for Valkyr wire models."""

from pathlib import Path

import pytest

from valkyr._errors import ProtocolError
from valkyr._wire import (
    Auth,
    AuthFailure,
    AuthPending,
    AuthSuccess,
    Delete,
    Error,
    Get,
    Miss,
    Move,
    Ok,
    Ping,
    Pong,
    Provide,
    Set,
    SetBatch,
    SetEntry,
    Stats,
    StatsCommand,
    StatsResponse,
    Store,
    Unknown,
    Value,
    command_from_dict,
    command_from_line,
    command_to_line,
    response_from_dict,
    response_from_line,
    response_to_line,
    server_command_from_dict,
    server_command_from_line,
    server_command_to_line,
    server_result_from_line,
    server_result_to_line,
)


def _fixture_path(name: str) -> Path:
    return Path(__file__).parents[3] / "docs" / "protocol" / "fixtures" / name


@pytest.fixture
def command_fixtures():
    return _fixture_path("commands.txt").read_text().splitlines()


@pytest.fixture
def response_fixtures():
    return _fixture_path("responses.txt").read_text().splitlines()


@pytest.fixture
def server_command_fixtures():
    return _fixture_path("server_commands.txt").read_text().splitlines()


@pytest.fixture
def server_result_fixtures():
    return _fixture_path("server_results.txt").read_text().splitlines()


class TestCommandSerialization:
    def test_auth(self):
        cmd = Auth(api_key="app-key", adapter_instance=None)
        assert cmd.to_dict() == {
            "type": "auth",
            "api_key": "app-key",
            "adapter_instance": None,
        }

    def test_get(self):
        cmd = Get(namespace="/users", key="42")
        assert cmd.to_dict() == {"type": "get", "namespace": "/users", "key": "42"}

    def test_set(self):
        cmd = Set(namespace="/users", key="42", value={"name": "Ada"}, ttl_seconds=300)
        assert cmd.to_dict() == {
            "type": "set",
            "namespace": "/users",
            "key": "42",
            "value": {"name": "Ada"},
            "ttl_seconds": 300,
        }

    def test_set_batch(self):
        cmd = SetBatch(
            namespace="/users",
            entries=[SetEntry(key="1", value={"name": "Ada"}), SetEntry(key="2", value=None)],
            ttl_seconds=None,
        )
        assert cmd.to_dict() == {
            "type": "set_batch",
            "namespace": "/users",
            "entries": [{"key": "1", "value": {"name": "Ada"}}, {"key": "2", "value": None}],
            "ttl_seconds": None,
        }

    def test_delete(self):
        cmd = Delete(namespace="/users", key_pattern="42*")
        assert cmd.to_dict() == {
            "type": "delete",
            "namespace": "/users",
            "key_pattern": "42*",
        }

    def test_move(self):
        cmd = Move(source="/users::draft", destination="/users::published")
        assert cmd.to_dict() == {
            "type": "move",
            "source": "/users::draft",
            "destination": "/users::published",
        }

    def test_provide(self):
        cmd = Provide(namespace_pattern="/users", key_pattern="*", max_rate=100)
        assert cmd.to_dict() == {
            "type": "provide",
            "namespace_pattern": "/users",
            "key_pattern": "*",
            "max_rate": 100,
            "timeout": None,
            "miss_ttl": None,
        }

    def test_provide_options_round_trip_and_reject_invalid_integers(self):
        data = {
            "type": "provide",
            "namespace_pattern": "/users",
            "key_pattern": "*",
            "max_rate": None,
            "timeout": 250,
            "miss_ttl": 30,
        }
        command = command_from_dict(data)
        assert isinstance(command, Provide)
        assert command.to_dict() == data
        for name, value in (("timeout", True), ("miss_ttl", -1), ("timeout", 2**64)):
            invalid = {**data, name: value}
            with pytest.raises(ProtocolError):
                command_from_dict(invalid)

    def test_store(self):
        cmd = Store(namespace_pattern="/users", key_pattern="*")
        assert cmd.to_dict() == {
            "type": "store",
            "namespace_pattern": "/users",
            "key_pattern": "*",
        }

    def test_ping(self):
        cmd = Ping()
        assert cmd.to_dict() == {"type": "ping"}

    def test_stats_command(self):
        cmd = StatsCommand()
        assert cmd.to_dict() == {"type": "stats"}


class TestCommandRoundTrip:
    def test_commands_match_fixtures(self, command_fixtures):
        for line in command_fixtures:
            assert command_to_line(command_from_line(line)) == line


class TestResponseSerialization:
    def test_ok(self):
        assert Ok().to_dict() == {"type": "ok"}

    def test_value(self):
        assert Value(value={"name": "Ada"}, ttl_seconds=300).to_dict() == {
            "type": "value",
            "value": {"name": "Ada"},
            "ttl_seconds": 300,
        }

    def test_miss(self):
        assert Miss(retry_after_ms=25).to_dict() == {
            "type": "miss",
            "retry_after_ms": 25,
        }

    def test_unknown(self):
        assert Unknown().to_dict() == {"type": "unknown"}

    def test_auth_success(self):
        assert AuthSuccess(client_id="client-1", session_ttl_seconds=3600).to_dict() == {
            "type": "auth_success",
            "client_id": "client-1",
            "session_ttl_seconds": 3600,
        }

    def test_auth_pending(self):
        assert AuthPending(retry_after_ms=10).to_dict() == {
            "type": "auth_pending",
            "retry_after_ms": 10,
        }

    def test_auth_failure(self):
        assert AuthFailure(message="invalid API key").to_dict() == {
            "type": "auth_failure",
            "message": "invalid API key",
        }

    def test_pong(self):
        assert Pong().to_dict() == {"type": "pong"}

    def test_stats(self):
        stats = Stats(requests=10, hits=5, misses=3, values=2)
        assert StatsResponse(stats=stats).to_dict() == {
            "type": "stats",
            "requests": 10,
            "hits": 5,
            "misses": 3,
            "values": 2,
        }

    def test_error(self):
        assert Error(message="provider unavailable").to_dict() == {
            "type": "error",
            "message": "provider unavailable",
        }


class TestResponseRoundTrip:
    def test_responses_match_fixtures(self, response_fixtures):
        for line in response_fixtures:
            command = Get("/users", "42")
            if line in {"OK client-1 TTL 3600", "MISS 10", 'KO "invalid API key"'}:
                command = Auth("app-key")
            elif line == "PONG":
                command = Ping()
            elif line.startswith("STATS "):
                command = StatsCommand()
            elif line == "OK" or line == 'KO "provider unavailable"':
                command = Set("/users", "42", {})
            assert response_to_line(command, response_from_line(command, line)) == line


class TestServerCommandRoundTrip:
    def test_server_commands_match_fixtures(self, server_command_fixtures):
        for line in server_command_fixtures:
            assert server_command_to_line(server_command_from_line(line)) == line


class TestServerResultRoundTrip:
    def test_server_results_match_fixtures(self, server_result_fixtures):
        for line in server_result_fixtures:
            command = (
                server_command_from_line(
                    "PERSIST_SET 00000000-0000-0000-0000-000000000002 /users 42 "
                    '{"name":"Ada"} EX 300'
                )
                if line.startswith("OPERATION")
                else server_command_from_line(
                    "QUERY 00000000-0000-0000-0000-000000000001 /users 42"
                )
            )
            assert server_result_to_line(command, server_result_from_line(command, line)) == line


class TestMalformedAndUnknown:
    @pytest.mark.parametrize("separator", [" ", "\t", "\v", "\f"])
    def test_ascii_whitespace_separates_tokens(self, separator):
        assert command_from_line(f"GET{separator}/x{separator}k") == Get("/x", "k")

    @pytest.mark.parametrize("separator", ["\u00a0", "\u2003"])
    def test_unicode_whitespace_is_not_a_separator(self, separator):
        with pytest.raises(ProtocolError):
            command_from_line(f"GET{separator}/x{separator}k")

    def test_unknown_command_type_rejected(self):
        with pytest.raises(ProtocolError):
            command_from_dict({"type": "future_command"})

    def test_unknown_response_type_rejected(self):
        from valkyr._errors import ProtocolError

        with pytest.raises(ProtocolError):
            response_from_dict({"type": "future_response"})

    def test_unknown_server_command_type_rejected(self):
        from valkyr._errors import ProtocolError

        with pytest.raises(ProtocolError):
            server_command_from_dict({"type": "future_callback"})

    def test_additive_fields_ignored(self):
        data = {
            "type": "value",
            "value": {"name": "Ada"},
            "ttl_seconds": 300,
            "extra": "ignored",
        }
        response = response_from_dict(data)
        assert response.value == {"name": "Ada"}

    def test_missing_required_field_raises(self):
        from valkyr._errors import ProtocolError

        with pytest.raises(ProtocolError):
            response_from_dict({"type": "value"})

    def test_u64_and_uuid_encodings_are_canonical(self):
        from valkyr._errors import ProtocolError

        with pytest.raises(ProtocolError):
            response_from_dict({"type": "miss", "retry_after_ms": 2**64})
        with pytest.raises(ProtocolError):
            server_command_from_dict(
                {
                    "type": "query",
                    "request_id": "00000000000000000000000000000001",
                    "namespace": "/users",
                    "key": "42",
                }
            )

    def test_batch_entries_are_required(self):
        from valkyr._errors import ProtocolError

        with pytest.raises(ProtocolError):
            command_from_dict({"type": "set_batch", "namespace": "/users"})

    def test_text_invalid_frames_are_rejected(self):
        command_cases = [
            "SET /x k 1 BAD 2",
            "PROVIDE /x * MISS_TTL 2 TIMEOUT 1",
            "AUTH key ADAPTER 00000000000000000000000000000001",
            "SET /x k 1 EX 18446744073709551616",
            "SET /x k bare-word",
            "SET /x k NaN",
            "SET /x k Infinity",
            "SET /x k 1e999",
            "SET /x k -1e999",
            'SET /x k {"nested":1e999}',
        ]
        for line in command_cases:
            with pytest.raises(ProtocolError):
                command_from_line(line)
        with pytest.raises(ProtocolError):
            response_from_line(Ping(), "MISS 1")
        with pytest.raises(ProtocolError):
            server_command_from_line(
                "PERSIST_SET 00000000-0000-0000-0000-000000000001 /x k 1 BAD 2"
            )
        query = server_command_from_line("QUERY 00000000-0000-0000-0000-000000000001 /x k")
        with pytest.raises(ProtocolError):
            server_result_from_line(query, "QUERY_RESULT nope SET /x k 1")

    def test_text_answer_contexts_and_quoted_ex_batches(self):
        with pytest.raises(ProtocolError):
            response_from_line(Get("/x", "k"), "OK")
        with pytest.raises(ProtocolError):
            response_from_line(Set("/x", "k", 1), "PONG")
        with pytest.raises(ProtocolError):
            response_from_line(Ping(), "UNKNOWN")
        with pytest.raises(ProtocolError):
            response_from_line(StatsCommand(), "OK")

        command = command_from_line('SET_BATCH /x "EX" 1 EX 5')
        assert command_to_line(command) == 'SET_BATCH /x "EX" 1 EX 5'
        with pytest.raises(ProtocolError):
            command_from_line("SET_BATCH /x EX 5")

        callback = server_command_from_line(
            'PERSIST_SET_BATCH 00000000-0000-0000-0000-000000000003 /x "EX" 1'
        )
        assert server_command_to_line(callback) == (
            'PERSIST_SET_BATCH 00000000-0000-0000-0000-000000000003 /x "EX" 1'
        )
        with pytest.raises(ProtocolError):
            server_command_from_line(
                "PERSIST_SET_BATCH 00000000-0000-0000-0000-000000000003 /x EX 5"
            )

    def test_finite_exponents_are_preserved(self):
        scalar = command_from_line("SET /x k 1e+100")
        nested = command_from_line('SET /x k {"nested":-1e-100}')

        assert scalar.value == 1e100
        assert nested.value == {"nested": -1e-100}

    def test_reserved_keywords_must_be_unquoted(self):
        command_cases = [
            'AUTH key "ADAPTER" 00000000-0000-0000-0000-000000000001',
            'SET /x k 1 "EX" 5',
            'PROVIDE /x * "MAX_RATE" 1',
            'PROVIDE /x * TIMEOUT 1 "MISS_TTL" 2',
        ]
        for line in command_cases:
            with pytest.raises(ProtocolError):
                command_from_line(line)

        auth = Auth("key")
        get = Get("/x", "k")
        stats = StatsCommand()
        for command, line in [
            (auth, 'OK client "TTL" 1'),
            (get, 'SET /x k 1 "EX" 5'),
            (stats, 'STATS "REQUESTS" 1 HITS 0 MISSES 0 VALUES 0'),
        ]:
            with pytest.raises(ProtocolError):
                response_from_line(command, line)

        with pytest.raises(ProtocolError):
            server_command_from_line(
                'PERSIST_SET 00000000-0000-0000-0000-000000000001 /x k 1 "EX" 5'
            )
        query = server_command_from_line("QUERY 00000000-0000-0000-0000-000000000001 /x k")
        with pytest.raises(ProtocolError):
            server_result_from_line(
                query,
                'QUERY_RESULT 00000000-0000-0000-0000-000000000001 "SET" /x k 1',
            )
        persist = server_command_from_line(
            "PERSIST_SET 00000000-0000-0000-0000-000000000002 /x k 1"
        )
        with pytest.raises(ProtocolError):
            server_result_from_line(
                persist,
                'OPERATION 00000000-0000-0000-0000-000000000002 "OK"',
            )
