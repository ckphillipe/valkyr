use crate::{Error, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// An owned logical value collection with optional context decomposition.
///
/// The complete wire value is stored once. `ctx_start` is the byte offset
/// immediately after the first valid `::` delimiter, when one exists.
#[derive(Clone, Debug)]
pub struct NamespaceContext {
    value: Arc<str>,
    ctx_start: Option<NonZeroUsize>,
}

impl NamespaceContext {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(Error::EmptyIdentifier { field: "namespace" });
        }
        let ctx_start = value.split_once("::").and_then(|(namespace, context)| {
            if namespace.is_empty() || context.is_empty() {
                return None;
            }
            NonZeroUsize::new(namespace.len() + 2)
        });
        Ok(Self {
            value: Arc::from(value),
            ctx_start,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn ns(&self) -> &str {
        self.ctx_start
            .map_or(self.as_str(), |start| &self.value[..start.get() - 2])
    }

    pub fn ctx(&self) -> Option<&str> {
        self.ctx_start.map(|start| &self.value[start.get()..])
    }
}

impl PartialEq for NamespaceContext {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for NamespaceContext {}

impl Hash for NamespaceContext {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Serialize for NamespaceContext {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NamespaceContext {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Rejects moves that would change a base namespace.
pub fn validate_context_move(
    source: &NamespaceContext,
    destination: &NamespaceContext,
) -> Result<()> {
    source
        .ctx()
        .ok_or_else(|| Error::Protocol("MOVE source must be a namespace::context route".into()))?;
    destination.ctx().ok_or_else(|| {
        Error::Protocol("MOVE destination must be a namespace::context route".into())
    })?;
    if source.ns() != destination.ns() {
        return Err(Error::Protocol(
            "MOVE source and destination must share a base namespace".into(),
        ));
    }
    Ok(())
}

impl TryFrom<String> for NamespaceContext {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl Display for NamespaceContext {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_accessors_preserve_the_first_delimiter_and_text() {
        let qualified = NamespaceContext::new("/people::user::region").unwrap();
        assert_eq!(qualified.as_str(), "/people::user::region");
        assert_eq!(qualified.ns(), "/people");
        assert_eq!(qualified.ctx(), Some("user::region"));
        assert_eq!(NamespaceContext::new("/people").unwrap().ctx(), None);
        let malformed = NamespaceContext::new("/people::").unwrap();
        assert_eq!(malformed.as_str(), "/people::");
        assert_eq!(malformed.ns(), "/people::");
        assert_eq!(malformed.ctx(), None);
    }

    #[test]
    fn empty_namespaces_are_rejected() {
        assert!(NamespaceContext::new("").is_err());
    }

    #[test]
    fn context_moves_require_one_shared_base_namespace() {
        let source = NamespaceContext::new("/people::draft").unwrap();
        let destination = NamespaceContext::new("/people::published").unwrap();
        assert!(validate_context_move(&source, &destination).is_ok());
        assert!(
            validate_context_move(
                &source,
                &NamespaceContext::new("/archive::published").unwrap()
            )
            .is_err()
        );
        assert!(
            validate_context_move(&source, &NamespaceContext::new("/archive").unwrap()).is_err()
        );
    }
}

/// A value identifier inside a [`NamespaceContext`].
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Key(String);

impl Key {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(Error::EmptyIdentifier { field: "key" });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Key {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl Display for Key {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A deletion pattern. A trailing `*` means a textual key prefix; all other
/// patterns are exact key matches.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyPattern(String);

impl KeyPattern {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(Error::EmptyIdentifier {
                field: "key pattern",
            });
        }
        Ok(Self(value))
    }

    pub fn matches(&self, key: &Key) -> bool {
        self.0
            .strip_suffix('*')
            .map_or(self.0 == key.as_str(), |prefix| {
                key.as_str().starts_with(prefix)
            })
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for KeyPattern {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A namespace routing expression. It is intentionally distinct from a
/// concrete namespace so registrations cannot be passed to store operations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NamespacePattern(String);

impl NamespacePattern {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(Error::EmptyIdentifier {
                field: "namespace pattern",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for NamespacePattern {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The fully-qualified address of a stored JSON value.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Route {
    pub namespace: NamespaceContext,
    pub key: Key,
}

impl Route {
    pub fn new(namespace: NamespaceContext, key: Key) -> Self {
        Self { namespace, key }
    }
}

impl Display for Route {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.namespace, self.key)
    }
}
