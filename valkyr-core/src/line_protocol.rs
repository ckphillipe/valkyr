//! Canonical human-readable protocol v1 codec.
//!
//! Frames are deliberately kept independent from transports. TCP callers add
//! a newline and WebSocket callers send the returned string as one text frame.

use crate::{
    Command, Error, Key, KeyPattern, NamespaceContext, NamespacePattern, Response, Result,
    ServerCommand, ServerResult, SetEntry, Stats,
};
use serde_json::Value;
use uuid::Uuid;

const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
struct Scanner<'a> {
    input: &'a str,
    index: usize,
}

#[derive(Clone, Debug)]
struct Token {
    text: String,
    quoted: bool,
}

impl<'a> Scanner<'a> {
    fn new(input: &'a str) -> Result<Self> {
        if input.is_empty() || input.len() > MAX_FRAME_BYTES {
            return Err(Error::Protocol("frame is empty or too large".into()));
        }
        if input.contains(['\r', '\n']) {
            return Err(Error::Protocol("frame contains a line break".into()));
        }
        Ok(Self { input, index: 0 })
    }

    fn next(&mut self) -> Result<Option<Token>> {
        while self
            .input
            .as_bytes()
            .get(self.index)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.index += 1;
        }
        if self.index == self.input.len() {
            return Ok(None);
        }
        let start = self.index;
        let first = self.input.as_bytes()[self.index];
        if first == b'"' {
            self.index += 1;
            let mut escaped = false;
            while let Some(&byte) = self.input.as_bytes().get(self.index) {
                self.index += 1;
                if escaped {
                    if !matches!(byte, b'"' | b'\\' | b'n' | b'r' | b't') {
                        return Err(Error::Protocol("invalid quoted escape".into()));
                    }
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    let raw = &self.input[start..self.index];
                    let value: String = serde_json::from_str(raw)
                        .map_err(|_| Error::Protocol("invalid quoted token".into()))?;
                    return Ok(Some(Token {
                        text: value,
                        quoted: true,
                    }));
                }
            }
            return Err(Error::Protocol("unterminated quoted token".into()));
        }
        if first == b'[' || first == b'{' {
            self.index += 1;
            let mut stack = vec![first];
            let mut depth = 1usize;
            let mut quoted = false;
            let mut escaped = false;
            while let Some(&byte) = self.input.as_bytes().get(self.index) {
                self.index += 1;
                if quoted {
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        quoted = false;
                    }
                    continue;
                }
                match byte {
                    b'"' => quoted = true,
                    b'[' | b'{' => {
                        stack.push(byte);
                        depth += 1;
                    }
                    b']' | b'}' => {
                        let expected = if byte == b']' { b'[' } else { b'{' };
                        if stack.pop() != Some(expected) {
                            return Err(Error::Protocol("unbalanced structured value".into()));
                        }
                        depth = depth
                            .checked_sub(1)
                            .ok_or_else(|| Error::Protocol("unbalanced structured value".into()))?;
                        if depth == 0 {
                            let token = &self.input[start..self.index];
                            serde_json::from_str::<Value>(token)
                                .map_err(|_| Error::Protocol("invalid structured value".into()))?;
                            return Ok(Some(Token {
                                text: token.to_owned(),
                                quoted: false,
                            }));
                        }
                    }
                    _ => {}
                }
            }
            return Err(Error::Protocol("incomplete structured value".into()));
        }
        while self
            .input
            .as_bytes()
            .get(self.index)
            .is_some_and(|byte| !byte.is_ascii_whitespace())
        {
            self.index += 1;
        }
        Ok(Some(Token {
            text: self.input[start..self.index].to_owned(),
            quoted: false,
        }))
    }

    fn all(mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        while let Some(token) = self.next()? {
            tokens.push(token);
        }
        if tokens.is_empty() {
            return Err(Error::Protocol("empty frame".into()));
        }
        Ok(tokens)
    }
}

fn tokens(frame: &str) -> Result<Vec<Token>> {
    Scanner::new(frame)?.all()
}

fn exact(tokens: &[Token], expected: usize) -> Result<()> {
    if tokens.len() != expected {
        return Err(Error::Protocol("unexpected number of arguments".into()));
    }
    Ok(())
}

fn word(tokens: &[Token], index: usize) -> Result<&str> {
    tokens
        .get(index)
        .map(|token| token.text.as_str())
        .ok_or_else(|| Error::Protocol("missing argument".into()))
}

fn keyword(tokens: &[Token], index: usize, expected: &str) -> Result<()> {
    let token = tokens
        .get(index)
        .ok_or_else(|| Error::Protocol("missing argument".into()))?;
    if token.quoted || token.text != expected {
        return Err(Error::Protocol(format!("expected {expected}")));
    }
    Ok(())
}

fn number(tokens: &[Token], index: usize) -> Result<u64> {
    word(tokens, index)?
        .parse::<u64>()
        .map_err(|_| Error::Protocol("invalid unsigned integer".into()))
}

fn uuid(tokens: &[Token], index: usize) -> Result<Uuid> {
    let value = Uuid::parse_str(word(tokens, index)?)
        .map_err(|_| Error::Protocol("invalid UUID".into()))?;
    if value.to_string() != word(tokens, index)? {
        return Err(Error::Protocol(
            "UUID must use canonical lowercase encoding".into(),
        ));
    }
    Ok(value)
}

fn namespace(value: &str) -> Result<NamespaceContext> {
    NamespaceContext::new(value.to_owned())
}
fn key(value: &str) -> Result<Key> {
    Key::new(value.to_owned())
}
fn key_pattern(value: &str) -> Result<KeyPattern> {
    KeyPattern::new(value.to_owned())
}
fn namespace_pattern(value: &str) -> Result<NamespacePattern> {
    NamespacePattern::new(value.to_owned())
}

fn value(token: &Token) -> Result<Value> {
    if token.quoted {
        return Ok(Value::String(token.text.clone()));
    }
    serde_json::from_str(&token.text).map_err(|_| Error::Protocol("invalid value literal".into()))
}

fn value_text(value: &Value) -> Result<String> {
    serde_json::to_string(value).map_err(|error| Error::Protocol(error.to_string()))
}

fn quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_./:*~$-".contains(&byte))
    {
        value.to_owned()
    } else {
        serde_json::to_string(value).expect("a Rust string is serializable")
    }
}

fn option(tokens: &[Token], index: usize, name: &str) -> Result<Option<u64>> {
    if index == tokens.len() {
        return Ok(None);
    }
    keyword(tokens, index, name)?;
    Ok(Some(number(tokens, index + 1)?))
}

fn is_unquoted(tokens: &[Token], index: usize, value: &str) -> bool {
    tokens
        .get(index)
        .is_some_and(|token| !token.quoted && token.text == value)
}

fn batch_key(value: &str) -> String {
    if value == "EX" {
        serde_json::to_string(value).expect("a Rust string is serializable")
    } else {
        quote(value)
    }
}

fn answer_allowed(command: &Command, response: &Response) -> bool {
    matches!(
        (command, response),
        (
            Command::Auth { .. },
            Response::AuthSuccess { .. }
                | Response::AuthPending { .. }
                | Response::AuthFailure { .. },
        ) | (
            Command::Get { .. },
            Response::Value { .. }
                | Response::Miss { .. }
                | Response::Unknown
                | Response::Error { .. },
        ) | (
            Command::Set { .. }
                | Command::SetBatch { .. }
                | Command::Delete { .. }
                | Command::Move { .. }
                | Command::Provide { .. }
                | Command::Store { .. },
            Response::Ok | Response::Error { .. },
        ) | (Command::Ping, Response::Pong | Response::Error { .. })
            | (Command::Stats, Response::Stats(_) | Response::Error { .. })
    )
}

pub fn encode_command(command: &Command) -> Result<String> {
    let mut output = String::new();
    match command {
        Command::Auth {
            api_key,
            adapter_instance,
        } => {
            output.push_str("AUTH ");
            output.push_str(&quote(api_key));
            if let Some(id) = adapter_instance {
                output.push_str(" ADAPTER ");
                output.push_str(&id.to_string());
            }
        }
        Command::Get { namespace, key } => {
            output = format!("GET {} {}", quote(namespace.as_str()), quote(key.as_str()))
        }
        Command::Set {
            namespace,
            key,
            value,
            ttl_seconds,
        } => {
            output = format!(
                "SET {} {} {}",
                quote(namespace.as_str()),
                quote(key.as_str()),
                value_text(value)?
            );
            if let Some(ttl) = ttl_seconds {
                output.push_str(&format!(" EX {ttl}"));
            }
        }
        Command::SetBatch {
            namespace,
            entries,
            ttl_seconds,
        } => {
            if entries.is_empty() {
                return Err(Error::Protocol("SET_BATCH requires an entry".into()));
            }
            output = format!("SET_BATCH {}", quote(namespace.as_str()));
            for entry in entries {
                output.push(' ');
                output.push_str(&batch_key(entry.key.as_str()));
                output.push(' ');
                output.push_str(&value_text(&entry.value)?);
            }
            if let Some(ttl) = ttl_seconds {
                output.push_str(&format!(" EX {ttl}"));
            }
        }
        Command::Delete {
            namespace,
            key_pattern,
        } => {
            output = format!("DELETE {}", quote(namespace.as_str()));
            if let Some(pattern) = key_pattern {
                output.push(' ');
                output.push_str(&quote(pattern.as_str()));
            }
        }
        Command::Move {
            source,
            destination,
        } => {
            output = format!(
                "MOVE {} {}",
                quote(source.as_str()),
                quote(destination.as_str())
            )
        }
        Command::Provide {
            namespace_pattern,
            key_pattern,
            max_rate,
            timeout,
            miss_ttl,
        } => {
            output = format!(
                "PROVIDE {} {}",
                quote(namespace_pattern.as_str()),
                quote(key_pattern.as_str())
            );
            if let Some(value) = max_rate {
                output.push_str(&format!(" MAX_RATE {value}"));
            }
            if let Some(value) = timeout {
                output.push_str(&format!(" TIMEOUT {value}"));
            }
            if let Some(value) = miss_ttl {
                output.push_str(&format!(" MISS_TTL {value}"));
            }
        }
        Command::Store {
            namespace_pattern,
            key_pattern,
        } => {
            output = format!(
                "STORE {} {}",
                quote(namespace_pattern.as_str()),
                quote(key_pattern.as_str())
            )
        }
        Command::Ping => output.push_str("PING"),
        Command::Stats => output.push_str("STATS"),
    }
    Ok(output)
}

pub fn decode_command(frame: &str) -> Result<Command> {
    let t = tokens(frame)?;
    if t[0].quoted {
        return Err(Error::Protocol("command keyword must be unquoted".into()));
    }
    match word(&t, 0)? {
        "AUTH" => {
            if !(t.len() == 2 || t.len() == 4 && is_unquoted(&t, 2, "ADAPTER")) {
                return Err(Error::Protocol("invalid AUTH arguments".into()));
            }
            Ok(Command::Auth {
                api_key: word(&t, 1)?.to_owned(),
                adapter_instance: (t.len() == 4).then(|| uuid(&t, 3)).transpose()?,
            })
        }
        "GET" => {
            exact(&t, 3)?;
            Ok(Command::Get {
                namespace: namespace(word(&t, 1)?)?,
                key: key(word(&t, 2)?)?,
            })
        }
        "SET" => {
            if !(t.len() == 4 || t.len() == 6) {
                return Err(Error::Protocol("invalid SET arguments".into()));
            }
            let ttl = option(&t, 4, "EX")?;
            Ok(Command::Set {
                namespace: namespace(word(&t, 1)?)?,
                key: key(word(&t, 2)?)?,
                value: value(&t[3])?,
                ttl_seconds: ttl,
            })
        }
        "SET_BATCH" => {
            if t.len() < 4 {
                return Err(Error::Protocol("SET_BATCH requires entries".into()));
            }
            let end = if t.len() >= 3 && is_unquoted(&t, t.len() - 2, "EX") {
                t.len() - 2
            } else {
                t.len()
            };
            if end <= 2 || (end - 2) % 2 != 0 {
                return Err(Error::Protocol("SET_BATCH requires key/value pairs".into()));
            }
            let mut entries = Vec::new();
            let mut i = 2;
            while i < end {
                entries.push(SetEntry {
                    key: key(word(&t, i)?)?,
                    value: value(&t[i + 1])?,
                });
                i += 2;
            }
            Ok(Command::SetBatch {
                namespace: namespace(word(&t, 1)?)?,
                entries,
                ttl_seconds: if end < t.len() {
                    Some(number(&t, t.len() - 1)?)
                } else {
                    None
                },
            })
        }
        "DELETE" => {
            if !(t.len() == 2 || t.len() == 3) {
                return Err(Error::Protocol("invalid DELETE arguments".into()));
            }
            Ok(Command::Delete {
                namespace: namespace(word(&t, 1)?)?,
                key_pattern: t.get(2).map(|v| key_pattern(&v.text)).transpose()?,
            })
        }
        "MOVE" => {
            exact(&t, 3)?;
            Ok(Command::Move {
                source: namespace(word(&t, 1)?)?,
                destination: namespace(word(&t, 2)?)?,
            })
        }
        "PROVIDE" => {
            if t.len() < 3 {
                return Err(Error::Protocol("invalid PROVIDE arguments".into()));
            }
            let mut i = 3;
            let max_rate = if i < t.len() && is_unquoted(&t, i, "MAX_RATE") {
                let value = number(&t, i + 1)?
                    .try_into()
                    .map_err(|_| Error::Protocol("MAX_RATE overflows u32".into()))?;
                i += 2;
                Some(value)
            } else {
                None
            };
            let timeout = if i < t.len() && is_unquoted(&t, i, "TIMEOUT") {
                let value = number(&t, i + 1)?;
                i += 2;
                Some(value)
            } else {
                None
            };
            let miss_ttl = if i < t.len() && is_unquoted(&t, i, "MISS_TTL") {
                let value = number(&t, i + 1)?;
                i += 2;
                Some(value)
            } else {
                None
            };
            if i != t.len() {
                return Err(Error::Protocol("invalid PROVIDE options".into()));
            }
            Ok(Command::Provide {
                namespace_pattern: namespace_pattern(word(&t, 1)?)?,
                key_pattern: key_pattern(word(&t, 2)?)?,
                max_rate,
                timeout,
                miss_ttl,
            })
        }
        "STORE" => {
            exact(&t, 3)?;
            Ok(Command::Store {
                namespace_pattern: namespace_pattern(word(&t, 1)?)?,
                key_pattern: key_pattern(word(&t, 2)?)?,
            })
        }
        "PING" => {
            exact(&t, 1)?;
            Ok(Command::Ping)
        }
        "STATS" => {
            exact(&t, 1)?;
            Ok(Command::Stats)
        }
        _ => Err(Error::Protocol("unknown command".into())),
    }
}

pub fn encode_response(command: &Command, response: &Response) -> Result<String> {
    if !answer_allowed(command, response) {
        return Err(Error::Protocol("answer does not match command".into()));
    }
    Ok(match response {
        Response::Ok => "OK".into(),
        Response::Value { value, ttl_seconds } => {
            let Command::Get { namespace, key } = command else {
                return Err(Error::Protocol("VALUE answer requires GET context".into()));
            };
            let mut s = format!(
                "SET {} {} {}",
                quote(namespace.as_str()),
                quote(key.as_str()),
                value_text(value)?
            );
            if let Some(ttl) = ttl_seconds {
                s.push_str(&format!(" EX {ttl}"));
            }
            s
        }
        Response::Miss { retry_after_ms } | Response::AuthPending { retry_after_ms } => {
            format!("MISS {retry_after_ms}")
        }
        Response::Unknown => "UNKNOWN".into(),
        Response::AuthSuccess {
            client_id,
            session_ttl_seconds,
        } => format!("OK {} TTL {session_ttl_seconds}", quote(client_id)),
        Response::AuthFailure { message } | Response::Error { message } => {
            format!("KO {}", quote(message))
        }
        Response::Pong => "PONG".into(),
        Response::Stats(Stats {
            requests,
            hits,
            misses,
            values,
        }) => format!("STATS REQUESTS {requests} HITS {hits} MISSES {misses} VALUES {values}"),
    })
}

pub fn decode_response(command: &Command, frame: &str) -> Result<Response> {
    let t = tokens(frame)?;
    if t[0].quoted {
        return Err(Error::Protocol("answer keyword must be unquoted".into()));
    }
    match word(&t, 0)? {
        "OK" if matches!(command, Command::Auth { .. }) => {
            exact(&t, 4)?;
            keyword(&t, 2, "TTL")?;
            Ok(Response::AuthSuccess {
                client_id: word(&t, 1)?.to_owned(),
                session_ttl_seconds: number(&t, 3)?,
            })
        }
        "OK" if matches!(
            command,
            Command::Set { .. }
                | Command::SetBatch { .. }
                | Command::Delete { .. }
                | Command::Move { .. }
                | Command::Provide { .. }
                | Command::Store { .. }
        ) =>
        {
            exact(&t, 1)?;
            Ok(Response::Ok)
        }
        "SET" => {
            if !(t.len() == 4 || t.len() == 6) {
                return Err(Error::Protocol("invalid SET answer".into()));
            }
            let Command::Get {
                namespace: expected_ns,
                key: expected_key,
            } = command
            else {
                return Err(Error::Protocol("SET answer requires GET context".into()));
            };
            if word(&t, 1)? != expected_ns.as_str() || word(&t, 2)? != expected_key.as_str() {
                return Err(Error::Protocol("SET answer route mismatch".into()));
            }
            Ok(Response::Value {
                value: value(&t[3])?,
                ttl_seconds: option(&t, 4, "EX")?,
            })
        }
        "MISS" => {
            exact(&t, 2)?;
            let retry = number(&t, 1)?;
            if matches!(command, Command::Auth { .. }) {
                Ok(Response::AuthPending {
                    retry_after_ms: retry,
                })
            } else if matches!(command, Command::Get { .. }) {
                Ok(Response::Miss {
                    retry_after_ms: retry,
                })
            } else {
                Err(Error::Protocol("MISS answer has invalid context".into()))
            }
        }
        "UNKNOWN" => {
            exact(&t, 1)?;
            if !matches!(command, Command::Get { .. }) {
                return Err(Error::Protocol("UNKNOWN answer has invalid context".into()));
            }
            Ok(Response::Unknown)
        }
        "PONG" => {
            exact(&t, 1)?;
            if !matches!(command, Command::Ping) {
                return Err(Error::Protocol("PONG answer has invalid context".into()));
            }
            Ok(Response::Pong)
        }
        "STATS" => {
            exact(&t, 9)?;
            if !matches!(command, Command::Stats) {
                return Err(Error::Protocol("STATS answer has invalid context".into()));
            }
            keyword(&t, 1, "REQUESTS")?;
            keyword(&t, 3, "HITS")?;
            keyword(&t, 5, "MISSES")?;
            keyword(&t, 7, "VALUES")?;
            Ok(Response::Stats(Stats {
                requests: number(&t, 2)?,
                hits: number(&t, 4)?,
                misses: number(&t, 6)?,
                values: number(&t, 8)?,
            }))
        }
        "KO" => {
            if t.len() != 2 {
                return Err(Error::Protocol("invalid KO answer".into()));
            }
            let message = word(&t, 1)?.to_owned();
            if matches!(command, Command::Auth { .. }) {
                Ok(Response::AuthFailure { message })
            } else {
                Ok(Response::Error { message })
            }
        }
        _ => Err(Error::Protocol("answer does not match command".into())),
    }
}

pub fn encode_server_command(command: &ServerCommand) -> Result<String> {
    Ok(match command {
        ServerCommand::Query {
            request_id,
            namespace,
            key,
        } => format!(
            "QUERY {request_id} {} {}",
            quote(namespace.as_str()),
            quote(key.as_str())
        ),
        ServerCommand::PersistSet {
            request_id,
            namespace,
            key,
            value,
            ttl_seconds,
        } => {
            let mut s = format!(
                "PERSIST_SET {request_id} {} {} {}",
                quote(namespace.as_str()),
                quote(key.as_str()),
                value_text(value)?
            );
            if let Some(ttl) = ttl_seconds {
                s.push_str(&format!(" EX {ttl}"));
            }
            s
        }
        ServerCommand::PersistSetBatch {
            request_id,
            namespace,
            entries,
            ttl_seconds,
        } => {
            let mut s = format!(
                "PERSIST_SET_BATCH {request_id} {}",
                quote(namespace.as_str())
            );
            for e in entries {
                s.push(' ');
                s.push_str(&batch_key(e.key.as_str()));
                s.push(' ');
                s.push_str(&value_text(&e.value)?);
            }
            if let Some(ttl) = ttl_seconds {
                s.push_str(&format!(" EX {ttl}"));
            }
            s
        }
        ServerCommand::PersistDelete {
            request_id,
            namespace,
            key_pattern,
        } => format!(
            "PERSIST_DELETE {request_id} {}{}",
            quote(namespace.as_str()),
            key_pattern
                .as_ref()
                .map(|p| format!(" {}", quote(p.as_str())))
                .unwrap_or_default()
        ),
        ServerCommand::PersistMove {
            request_id,
            source,
            destination,
        } => format!(
            "PERSIST_MOVE {request_id} {} {}",
            quote(source.as_str()),
            quote(destination.as_str())
        ),
    })
}

pub fn decode_server_command(frame: &str) -> Result<ServerCommand> {
    let t = tokens(frame)?;
    if t[0].quoted {
        return Err(Error::Protocol("callback keyword must be unquoted".into()));
    }
    let id = uuid(&t, 1)?;
    match word(&t, 0)? {
        "QUERY" => {
            exact(&t, 4)?;
            Ok(ServerCommand::Query {
                request_id: id,
                namespace: namespace(word(&t, 2)?)?,
                key: key(word(&t, 3)?)?,
            })
        }
        "PERSIST_SET" => {
            if !(t.len() == 5 || t.len() == 7) {
                return Err(Error::Protocol("invalid PERSIST_SET".into()));
            }
            Ok(ServerCommand::PersistSet {
                request_id: id,
                namespace: namespace(word(&t, 2)?)?,
                key: key(word(&t, 3)?)?,
                value: value(&t[4])?,
                ttl_seconds: option(&t, 5, "EX")?,
            })
        }
        "PERSIST_SET_BATCH" => {
            if t.len() < 5 {
                return Err(Error::Protocol("invalid PERSIST_SET_BATCH".into()));
            }
            let end = if t.len() >= 4 && is_unquoted(&t, t.len() - 2, "EX") {
                t.len() - 2
            } else {
                t.len()
            };
            if end <= 3 || (end - 3) % 2 != 0 {
                return Err(Error::Protocol("invalid batch entries".into()));
            }
            let mut entries = Vec::new();
            let mut i = 3;
            while i < end {
                entries.push(SetEntry {
                    key: key(word(&t, i)?)?,
                    value: value(&t[i + 1])?,
                });
                i += 2;
            }
            Ok(ServerCommand::PersistSetBatch {
                request_id: id,
                namespace: namespace(word(&t, 2)?)?,
                entries,
                ttl_seconds: if end < t.len() {
                    Some(number(&t, t.len() - 1)?)
                } else {
                    None
                },
            })
        }
        "PERSIST_DELETE" => {
            if !(t.len() == 3 || t.len() == 4) {
                return Err(Error::Protocol("invalid PERSIST_DELETE".into()));
            }
            Ok(ServerCommand::PersistDelete {
                request_id: id,
                namespace: namespace(word(&t, 2)?)?,
                key_pattern: t.get(3).map(|v| key_pattern(&v.text)).transpose()?,
            })
        }
        "PERSIST_MOVE" => {
            exact(&t, 4)?;
            Ok(ServerCommand::PersistMove {
                request_id: id,
                source: namespace(word(&t, 2)?)?,
                destination: namespace(word(&t, 3)?)?,
            })
        }
        _ => Err(Error::Protocol("unknown callback command".into())),
    }
}

pub fn encode_server_result(command: &ServerCommand, result: &ServerResult) -> Result<String> {
    let expected = match command {
        ServerCommand::Query {
            request_id,
            namespace,
            key,
        } => match result {
            ServerResult::Query {
                request_id: actual,
                value,
                error,
                ttl_seconds,
            } if actual == request_id => {
                let mut s = format!("QUERY_RESULT {request_id} ");
                if let Some(message) = error {
                    s.push_str("KO ");
                    s.push_str(&quote(message));
                } else if let Some(value) = value {
                    s.push_str("SET ");
                    s.push_str(&format!(
                        "{} {} {}",
                        quote(namespace.as_str()),
                        quote(key.as_str()),
                        value_text(value)?
                    ));
                    if let Some(ttl) = ttl_seconds {
                        s.push_str(&format!(" EX {ttl}"));
                    }
                } else {
                    s.push_str("MISS");
                }
                s
            }
            _ => return Err(Error::Protocol("callback result kind mismatch".into())),
        },
        ServerCommand::PersistSet { request_id, .. }
        | ServerCommand::PersistSetBatch { request_id, .. }
        | ServerCommand::PersistDelete { request_id, .. }
        | ServerCommand::PersistMove { request_id, .. } => match result {
            ServerResult::Operation {
                request_id: actual,
                error,
            } if actual == request_id => format!(
                "OPERATION {request_id} {}",
                error
                    .as_ref()
                    .map(|e| format!("KO {}", quote(e)))
                    .unwrap_or_else(|| "OK".into())
            ),
            _ => return Err(Error::Protocol("callback result kind mismatch".into())),
        },
    };
    Ok(expected)
}

pub fn decode_server_result(command: &ServerCommand, frame: &str) -> Result<ServerResult> {
    let t = tokens(frame)?;
    if t[0].quoted {
        return Err(Error::Protocol(
            "callback result keyword must be unquoted".into(),
        ));
    }
    let id = uuid(&t, 1)?;
    let expected_id = match command {
        ServerCommand::Query { request_id, .. }
        | ServerCommand::PersistSet { request_id, .. }
        | ServerCommand::PersistSetBatch { request_id, .. }
        | ServerCommand::PersistDelete { request_id, .. }
        | ServerCommand::PersistMove { request_id, .. } => request_id,
    };
    if id != *expected_id {
        return Err(Error::Protocol("callback correlation mismatch".into()));
    }
    match command {
        ServerCommand::Query { namespace, key, .. } => {
            keyword(&t, 0, "QUERY_RESULT")?;
            match word(&t, 2)? {
                "MISS" if is_unquoted(&t, 2, "MISS") => {
                    exact(&t, 3)?;
                    Ok(ServerResult::Query {
                        request_id: id,
                        value: None,
                        error: None,
                        ttl_seconds: None,
                    })
                }
                "KO" if is_unquoted(&t, 2, "KO") => {
                    exact(&t, 4)?;
                    Ok(ServerResult::Query {
                        request_id: id,
                        value: None,
                        error: Some(word(&t, 3)?.to_owned()),
                        ttl_seconds: None,
                    })
                }
                "SET" if is_unquoted(&t, 2, "SET") => {
                    if !(t.len() == 6 || t.len() == 8)
                        || word(&t, 3)? != namespace.as_str()
                        || word(&t, 4)? != key.as_str()
                    {
                        return Err(Error::Protocol("callback route mismatch".into()));
                    }
                    Ok(ServerResult::Query {
                        request_id: id,
                        value: Some(value(&t[5])?),
                        error: None,
                        ttl_seconds: option(&t, 6, "EX")?,
                    })
                }
                _ => Err(Error::Protocol("invalid query result".into())),
            }
        }
        _ => {
            keyword(&t, 0, "OPERATION")?;
            match word(&t, 2)? {
                "OK" if is_unquoted(&t, 2, "OK") => {
                    exact(&t, 3)?;
                    Ok(ServerResult::Operation {
                        request_id: id,
                        error: None,
                    })
                }
                "KO" if is_unquoted(&t, 2, "KO") => {
                    exact(&t, 4)?;
                    Ok(ServerResult::Operation {
                        request_id: id,
                        error: Some(word(&t, 3)?.to_owned()),
                    })
                }
                _ => Err(Error::Protocol("invalid operation result".into())),
            }
        }
    }
}

pub fn callback_request_id(frame: &str) -> Result<Uuid> {
    let t = tokens(frame)?;
    if t[0].quoted || !matches!(word(&t, 0)?, "OPERATION" | "QUERY_RESULT") {
        return Err(Error::Protocol("not a callback result".into()));
    }
    uuid(&t, 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Vec<&'static str> {
        match name {
            "commands" => include_str!("../../docs/protocol/fixtures/commands.txt"),
            "responses" => include_str!("../../docs/protocol/fixtures/responses.txt"),
            "server_commands" => include_str!("../../docs/protocol/fixtures/server_commands.txt"),
            "server_results" => include_str!("../../docs/protocol/fixtures/server_results.txt"),
            _ => unreachable!(),
        }
        .lines()
        .collect()
    }
    #[test]
    fn command_round_trips() {
        let c = Command::Set {
            namespace: namespace("/users").unwrap(),
            key: key("name").unwrap(),
            value: serde_json::json!({"name":"Ada"}),
            ttl_seconds: Some(60),
        };
        let decoded = decode_command(&encode_command(&c).unwrap()).unwrap();
        assert_eq!(
            encode_command(&decoded).unwrap(),
            encode_command(&c).unwrap()
        );
    }
    #[test]
    fn answers_use_context() {
        let c = Command::Get {
            namespace: namespace("/users").unwrap(),
            key: key("name").unwrap(),
        };
        let r = Response::Value {
            value: serde_json::json!("Ada"),
            ttl_seconds: None,
        };
        let line = encode_response(&c, &r).unwrap();
        assert_eq!(line, "SET /users name \"Ada\"");
        assert!(decode_response(&c, &line).is_ok());
    }
    #[test]
    fn malformed_frames_are_rejected() {
        for line in [
            "",
            "GET /x",
            "SET /x k 1 EXTRA",
            "AUTH key ADAPTER nope",
            "SET /x k {",
        ] {
            assert!(decode_command(line).is_err(), "{line}");
        }
    }

    #[test]
    fn canonical_fixtures_round_trip_exactly() {
        for line in fixture("commands") {
            let command = decode_command(line).unwrap();
            assert_eq!(encode_command(&command).unwrap(), line);
        }

        for line in fixture("responses") {
            let command = match line {
                "OK client-1 TTL 3600" | "MISS 10" | "KO \"invalid API key\"" => Command::Auth {
                    api_key: "app-key".into(),
                    adapter_instance: None,
                },
                "PONG" => Command::Ping,
                line if line.starts_with("STATS ") => Command::Stats,
                "OK" | "KO \"provider unavailable\"" => Command::Set {
                    namespace: namespace("/users").unwrap(),
                    key: key("42").unwrap(),
                    value: serde_json::json!({}),
                    ttl_seconds: None,
                },
                _ => Command::Get {
                    namespace: namespace("/users").unwrap(),
                    key: key("42").unwrap(),
                },
            };
            let response = decode_response(&command, line).unwrap();
            assert_eq!(encode_response(&command, &response).unwrap(), line);
        }

        for line in fixture("server_commands") {
            let command = decode_server_command(line).unwrap();
            assert_eq!(encode_server_command(&command).unwrap(), line);
        }

        for line in fixture("server_results") {
            let command = if line.starts_with("OPERATION") {
                decode_server_command("PERSIST_SET 00000000-0000-0000-0000-000000000002 /users 42 {\"name\":\"Ada\"} EX 300").unwrap()
            } else {
                decode_server_command("QUERY 00000000-0000-0000-0000-000000000001 /users 42")
                    .unwrap()
            };
            let result = decode_server_result(&command, line).unwrap();
            assert_eq!(encode_server_result(&command, &result).unwrap(), line);
        }
    }

    #[test]
    fn shared_invalid_text_cases_are_rejected() {
        for line in [
            "SET /x k 1 BAD 2",
            "PROVIDE /x * MISS_TTL 2 TIMEOUT 1",
            "AUTH key ADAPTER 00000000000000000000000000000001",
            "SET /x k 1 EX 18446744073709551616",
        ] {
            assert!(decode_command(line).is_err(), "{line}");
        }
        let ping = Command::Ping;
        assert!(decode_response(&ping, "MISS 1").is_err());
        assert!(
            decode_server_command("PERSIST_SET 00000000-0000-0000-0000-000000000001 /x k 1 BAD 2")
                .is_err()
        );
        let query =
            decode_server_command("QUERY 00000000-0000-0000-0000-000000000001 /x k").unwrap();
        assert!(decode_server_result(&query, "QUERY_RESULT nope SET /x k 1").is_err());
    }

    #[test]
    fn answer_matrix_rejects_mismatched_families() {
        let get = Command::Get {
            namespace: namespace("/x").unwrap(),
            key: key("k").unwrap(),
        };
        let set = Command::Set {
            namespace: namespace("/x").unwrap(),
            key: key("k").unwrap(),
            value: serde_json::json!(1),
            ttl_seconds: None,
        };
        let ping = Command::Ping;
        let stats = Command::Stats;
        for (command, frame) in [
            (&get, "OK"),
            (&set, "PONG"),
            (&ping, "UNKNOWN"),
            (&stats, "OK"),
        ] {
            assert!(decode_response(command, frame).is_err(), "accepted {frame}");
        }
        assert!(encode_response(&get, &Response::Ok).is_err());
        assert!(encode_response(&ping, &Response::Unknown).is_err());
    }

    #[test]
    fn strict_values_and_quoted_ex_batch_keys() {
        for line in ["SET /x k bare", "SET /x k NaN", "SET /x k Infinity"] {
            assert!(decode_command(line).is_err(), "accepted {line}");
        }
        let command = Command::SetBatch {
            namespace: namespace("/x").unwrap(),
            entries: vec![SetEntry {
                key: key("EX").unwrap(),
                value: serde_json::json!(1),
            }],
            ttl_seconds: Some(5),
        };
        let line = encode_command(&command).unwrap();
        assert_eq!(line, "SET_BATCH /x \"EX\" 1 EX 5");
        assert_eq!(
            encode_command(&decode_command(&line).unwrap()).unwrap(),
            line
        );
        assert!(decode_command("SET_BATCH /x EX 5").is_err());

        let callback = ServerCommand::PersistSetBatch {
            request_id: Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap(),
            namespace: namespace("/x").unwrap(),
            entries: vec![SetEntry {
                key: key("EX").unwrap(),
                value: serde_json::json!(1),
            }],
            ttl_seconds: None,
        };
        let callback_line = encode_server_command(&callback).unwrap();
        assert_eq!(
            callback_line,
            "PERSIST_SET_BATCH 00000000-0000-0000-0000-000000000003 /x \"EX\" 1"
        );
        assert_eq!(
            encode_server_command(&decode_server_command(&callback_line).unwrap()).unwrap(),
            callback_line
        );
        assert!(
            decode_server_command("PERSIST_SET_BATCH 00000000-0000-0000-0000-000000000003 /x EX 5")
                .is_err()
        );
    }

    #[test]
    fn reserved_keywords_must_be_unquoted() {
        let command_cases = [
            "AUTH key \"ADAPTER\" 00000000-0000-0000-0000-000000000001",
            "SET /x k 1 \"EX\" 5",
            "PROVIDE /x * \"MAX_RATE\" 1",
            "PROVIDE /x * TIMEOUT 1 \"MISS_TTL\" 2",
        ];
        for line in command_cases {
            assert!(decode_command(line).is_err(), "accepted {line}");
        }

        let auth = Command::Auth {
            api_key: "key".into(),
            adapter_instance: None,
        };
        let get = Command::Get {
            namespace: namespace("/x").unwrap(),
            key: key("k").unwrap(),
        };
        let stats = Command::Stats;
        for (command, line) in [
            (&auth, "OK client \"TTL\" 1"),
            (&get, "SET /x k 1 \"EX\" 5"),
            (&stats, "STATS \"REQUESTS\" 1 HITS 0 MISSES 0 VALUES 0"),
        ] {
            assert!(decode_response(command, line).is_err(), "accepted {line}");
        }

        assert!(
            decode_server_command(
                "PERSIST_SET 00000000-0000-0000-0000-000000000001 /x k 1 \"EX\" 5"
            )
            .is_err()
        );
        let query =
            decode_server_command("QUERY 00000000-0000-0000-0000-000000000001 /x k").unwrap();
        assert!(
            decode_server_result(
                &query,
                "QUERY_RESULT 00000000-0000-0000-0000-000000000001 \"SET\" /x k 1"
            )
            .is_err()
        );
        let persist =
            decode_server_command("PERSIST_SET 00000000-0000-0000-0000-000000000002 /x k 1")
                .unwrap();
        assert!(
            decode_server_result(
                &persist,
                "OPERATION 00000000-0000-0000-0000-000000000002 \"OK\""
            )
            .is_err()
        );
    }
}
