use crate::{Key, KeyPattern, NamespaceContext, NamespacePattern};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SetEntry {
    pub key: Key,
    pub value: Value,
}

/// A native protocol request. Native transports frame the human-readable text
/// representation one command per line or text frame; structured values keep
/// their JSON literal syntax.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    Auth {
        api_key: String,
        adapter_instance: Option<Uuid>,
    },
    Get {
        namespace: NamespaceContext,
        key: Key,
    },
    Set {
        namespace: NamespaceContext,
        key: Key,
        value: Value,
        ttl_seconds: Option<u64>,
    },
    SetBatch {
        namespace: NamespaceContext,
        entries: Vec<SetEntry>,
        ttl_seconds: Option<u64>,
    },
    Delete {
        namespace: NamespaceContext,
        key_pattern: Option<KeyPattern>,
    },
    Move {
        source: NamespaceContext,
        destination: NamespaceContext,
    },
    Provide {
        namespace_pattern: NamespacePattern,
        key_pattern: KeyPattern,
        max_rate: Option<u32>,
        timeout: Option<u64>,
        miss_ttl: Option<u64>,
    },
    Store {
        namespace_pattern: NamespacePattern,
        key_pattern: KeyPattern,
    },
    Ping,
    Stats,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Stats {
    pub requests: u64,
    pub hits: u64,
    pub misses: u64,
    pub values: u64,
}

/// A native protocol response. Errors are data so a connection remains usable.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Ok,
    Value {
        value: Value,
        ttl_seconds: Option<u64>,
    },
    Miss {
        retry_after_ms: u64,
    },
    Unknown,
    AuthSuccess {
        client_id: String,
        session_ttl_seconds: u64,
    },
    AuthPending {
        retry_after_ms: u64,
    },
    AuthFailure {
        message: String,
    },
    Pong,
    Stats(Stats),
    Error {
        message: String,
    },
}

/// A server-initiated request sent only to a connected provider or store adapter.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerCommand {
    Query {
        request_id: Uuid,
        namespace: NamespaceContext,
        key: Key,
    },
    PersistSet {
        request_id: Uuid,
        namespace: NamespaceContext,
        key: Key,
        value: Value,
        ttl_seconds: Option<u64>,
    },
    PersistSetBatch {
        request_id: Uuid,
        namespace: NamespaceContext,
        entries: Vec<SetEntry>,
        ttl_seconds: Option<u64>,
    },
    PersistDelete {
        request_id: Uuid,
        namespace: NamespaceContext,
        key_pattern: Option<KeyPattern>,
    },
    PersistMove {
        request_id: Uuid,
        source: NamespaceContext,
        destination: NamespaceContext,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerResult {
    Operation {
        request_id: Uuid,
        error: Option<String>,
    },
    Query {
        request_id: Uuid,
        value: Option<Value>,
        error: Option<String>,
        ttl_seconds: Option<u64>,
    },
}

impl Response {
    pub fn error(error: impl ToString) -> Self {
        Self::Error {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_tagged_variants_are_rejected() {
        let error = serde_json::from_str::<Response>(r#"{"type":"future_response"}"#)
            .expect_err("unknown tagged variants must fail closed");
        assert!(error.to_string().contains("unknown variant"));
    }

    #[test]
    fn provide_additive_fields_accept_legacy_null_zero_and_positive_values() {
        let legacy: Command = serde_json::from_str(
            r#"{"type":"provide","namespace_pattern":"/values","key_pattern":"*","max_rate":null}"#,
        )
        .unwrap();
        assert!(matches!(
            legacy,
            Command::Provide {
                timeout: None,
                miss_ttl: None,
                ..
            }
        ));
        let explicit: Command = serde_json::from_str(
            r#"{"type":"provide","namespace_pattern":"/values","key_pattern":"*","max_rate":null,"timeout":0,"miss_ttl":12}"#,
        )
        .unwrap();
        assert!(matches!(
            explicit,
            Command::Provide {
                timeout: Some(0),
                miss_ttl: Some(12),
                ..
            }
        ));
    }
}
