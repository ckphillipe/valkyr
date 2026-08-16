use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::Duration;
use valkyr_core::{Key, NamespaceContext};

use crate::{AdapterError, Result};

#[derive(Clone, Debug)]
pub struct OpenBaoMapping {
    prefix: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecretLocation {
    Root {
        base: String,
        path: String,
    },
    Context {
        base: String,
        context: String,
        path: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuthDocument {
    pub key: String,
    pub value: Value,
    pub ttl: Option<Duration>,
}

impl OpenBaoMapping {
    pub fn new(prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into().trim_matches('/').to_owned();
        if prefix.is_empty() {
            return Err(AdapterError::Configuration(
                "openbao.prefix is required".into(),
            ));
        }
        Ok(Self { prefix })
    }
    pub fn index_path(&self, base: &str) -> String {
        format!("{}/indexes/{}", self.prefix, encode(base))
    }
    pub fn root_collection_path(&self, namespace: &NamespaceContext) -> String {
        format!("{}/values/{}/root", self.prefix, encode(namespace.as_str()))
    }
    pub fn auth_key_parts(&self, key: &Key) -> [String; 4] {
        let digest = Sha256::digest(key.as_str().as_bytes());
        let hex = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        [
            hex[0..16].to_owned(),
            hex[16..32].to_owned(),
            hex[32..48].to_owned(),
            hex[48..64].to_owned(),
        ]
    }
    pub fn auth_path(&self, key: &Key) -> String {
        let parts = self.auth_key_parts(key);
        format!(
            "{}/{}/{}/{}/{}",
            self.auth_collection_path(),
            parts[0],
            parts[1],
            parts[2],
            parts[3]
        )
    }
    pub fn auth_collection_path(&self) -> String {
        format!("{}/values/{}/root", self.prefix, encode("/__auth"))
    }
    pub fn is_auth_namespace(namespace: &NamespaceContext) -> bool {
        namespace.as_str() == "/__auth" && namespace.ctx().is_none()
    }
    pub(crate) fn encode_auth_document(key: &Key, value: Value, ttl: Option<Duration>) -> Value {
        serde_json::json!({
            "key": key.as_str(),
            "value": value,
            "ttl_seconds": ttl.map(|duration| duration.as_secs()),
        })
    }
    pub(crate) fn decode_auth_document(document: &Value) -> Result<AuthDocument> {
        let key = document
            .get("key")
            .and_then(Value::as_str)
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                AdapterError::Configuration("OpenBao auth document had no valid key".into())
            })?
            .to_owned();
        let value = document.get("value").cloned().ok_or_else(|| {
            AdapterError::Configuration("OpenBao auth document had no value".into())
        })?;
        let ttl = match document.get("ttl_seconds") {
            None | Some(Value::Null) => None,
            Some(value) => Some(Duration::from_secs(value.as_u64().ok_or_else(|| {
                AdapterError::Configuration(
                    "OpenBao auth document had an invalid ttl_seconds".into(),
                )
            })?)),
        };
        Ok(AuthDocument { key, value, ttl })
    }
    pub fn context_collection_path(&self, base: &str, collection: &str) -> String {
        format!(
            "{}/values/{}/contexts/{collection}",
            self.prefix,
            encode(base)
        )
    }
    pub fn locate(
        &self,
        namespace: &NamespaceContext,
        key: &Key,
        collection: Option<&str>,
    ) -> Result<SecretLocation> {
        match namespace.ctx() {
            None => Ok(SecretLocation::Root {
                base: namespace.as_str().into(),
                path: if Self::is_auth_namespace(namespace) {
                    self.auth_path(key)
                } else {
                    format!(
                        "{}/{}",
                        self.root_collection_path(namespace),
                        encode(key.as_str())
                    )
                },
            }),
            Some(context) => {
                let collection = collection.ok_or_else(|| {
                    AdapterError::Configuration("context collection is required".into())
                })?;
                Ok(SecretLocation::Context {
                    base: namespace.ns().into(),
                    context: context.into(),
                    path: format!(
                        "{}/{}",
                        self.context_collection_path(namespace.ns(), collection),
                        encode(key.as_str())
                    ),
                })
            }
        }
    }
}
impl SecretLocation {
    pub fn path(&self) -> &str {
        match self {
            Self::Root { path, .. } | Self::Context { path, .. } => path,
        }
    }
}
pub fn encode(value: &str) -> String {
    match value {
        "." => return "%2E".into(),
        ".." => return "%2E%2E".into(),
        _ => {}
    }

    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

pub fn decode(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        let high = bytes.get(index + 1).and_then(hex_value).ok_or_else(|| {
            AdapterError::Configuration("OpenBao provider key had an invalid percent escape".into())
        })?;
        let low = bytes.get(index + 2).and_then(hex_value).ok_or_else(|| {
            AdapterError::Configuration("OpenBao provider key had an invalid percent escape".into())
        })?;
        decoded.push((high << 4) | low);
        index += 3;
    }

    String::from_utf8(decoded).map_err(|error| {
        AdapterError::Configuration(format!("OpenBao provider key was not UTF-8: {error}"))
    })
}

fn hex_value(byte: &u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mapping_is_injective_and_path_safe() {
        assert_eq!(encode("/orders"), "%2Forders");
        assert!(!encode("a/b..%é").contains('/'));
        assert_eq!(encode("."), "%2E");
        assert_eq!(encode(".."), "%2E%2E");
        assert_ne!(encode("/"), encode("//"));
    }

    #[test]
    fn percent_encoding_round_trips_utf8() {
        let value = "/orders/a b%\u{00e9}";
        assert_eq!(decode(&encode(value)).unwrap(), value);
    }

    #[test]
    fn percent_decoding_rejects_invalid_escapes_and_utf8() {
        assert!(decode("a%2").is_err());
        assert!(decode("a%XZ").is_err());
        assert!(decode("%FF").is_err());
    }
    #[test]
    fn maps_roots_and_contexts() {
        let m = OpenBaoMapping::new("cache").unwrap();
        let key = Key::new("a/b").unwrap();
        assert_eq!(
            m.locate(&NamespaceContext::new("/orders").unwrap(), &key, None)
                .unwrap()
                .path(),
            "cache/values/%2Forders/root/a%2Fb"
        );
        assert_eq!(
            m.locate(
                &NamespaceContext::new("/orders::draft").unwrap(),
                &key,
                Some("id")
            )
            .unwrap()
            .path(),
            "cache/values/%2Forders/contexts/id/a%2Fb"
        );
    }

    #[test]
    fn auth_paths_use_four_fixed_sha256_segments() {
        let mapping = OpenBaoMapping::new("cache").unwrap();
        assert_eq!(
            mapping.auth_key_parts(&Key::new("abc").unwrap()),
            [
                "ba7816bf8f01cfea".to_owned(),
                "414140de5dae2223".to_owned(),
                "b00361a396177a9c".to_owned(),
                "b410ff61f20015ad".to_owned(),
            ]
        );
        let key = Key::new("api/key\u{00e9}").unwrap();
        let parts = mapping.auth_key_parts(&key);
        assert_eq!(
            parts.iter().map(String::len).collect::<Vec<_>>(),
            vec![16; 4]
        );
        assert!(parts.iter().all(|part| {
            part.bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        }));
        assert_eq!(
            mapping.auth_path(&key),
            format!("cache/values/%2F__auth/root/{}", parts.join("/"))
        );
        assert!(!mapping.auth_path(&key).contains(key.as_str()));
    }

    #[test]
    fn auth_documents_round_trip_and_reject_malformed_fields() {
        let key = Key::new("a/b\u{00e9}").unwrap();
        let document = OpenBaoMapping::encode_auth_document(
            &key,
            serde_json::json!({"role": "reader"}),
            Some(Duration::from_secs(30)),
        );
        assert_eq!(
            OpenBaoMapping::decode_auth_document(&document).unwrap(),
            AuthDocument {
                key: key.as_str().into(),
                value: serde_json::json!({"role": "reader"}),
                ttl: Some(Duration::from_secs(30)),
            }
        );
        for malformed in [
            serde_json::json!({"value": {}}),
            serde_json::json!({"key": "", "value": {}}),
            serde_json::json!({"key": "key", "ttl_seconds": "soon", "value": {}}),
        ] {
            assert!(OpenBaoMapping::decode_auth_document(&malformed).is_err());
        }
    }
}
