use crate::{Command, Error, Response, Result, Stats, Store};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

/// Executes protocol commands against one [`Store`].
pub struct Router<S> {
    store: S,
    requests: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<S: Store> Router<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            requests: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub async fn execute(&self, command: Command) -> Result<Response> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        match command {
            Command::Get { namespace, key } => match self.store.get(&namespace, &key).await? {
                Some(stored) => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    Ok(Response::Value {
                        value: stored.value,
                        ttl_seconds: stored.remaining_ttl.map(duration_to_seconds),
                    })
                }
                None => {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    Err(Error::NotFound(crate::Route::new(namespace, key)))
                }
            },
            Command::Set {
                namespace,
                key,
                value,
                ttl_seconds,
            } => {
                self.store
                    .set(namespace, key, value, ttl_seconds.map(Duration::from_secs))
                    .await?;
                Ok(Response::Ok)
            }
            Command::Delete {
                namespace,
                key_pattern,
            } => {
                self.store.delete(&namespace, key_pattern.as_ref()).await?;
                Ok(Response::Ok)
            }
            Command::Move {
                source,
                destination,
            } => {
                crate::validate_context_move(&source, &destination)?;
                self.store.move_namespace(&source, destination).await?;
                Ok(Response::Ok)
            }
            Command::SetBatch {
                namespace,
                entries,
                ttl_seconds,
            } => {
                self.store
                    .set_batch(
                        namespace,
                        entries
                            .into_iter()
                            .map(|entry| (entry.key, entry.value))
                            .collect(),
                        ttl_seconds.map(Duration::from_secs),
                    )
                    .await?;
                Ok(Response::Ok)
            }
            Command::Auth { .. } | Command::Provide { .. } | Command::Store { .. } => {
                Err(Error::Protocol("this command requires the broker".into()))
            }
            Command::Ping => Ok(Response::Pong),
            Command::Stats => Ok(Response::Stats(self.stats().await?)),
        }
    }

    pub async fn stats(&self) -> Result<Stats> {
        Ok(Stats {
            requests: self.requests.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            values: self.store.len().await?,
        })
    }
}

fn duration_to_seconds(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(!duration.subsec_nanos().eq(&0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Key, MemoryStore, NamespaceContext};
    use serde_json::json;

    fn namespace() -> NamespaceContext {
        NamespaceContext::new("/users").unwrap()
    }
    fn key() -> Key {
        Key::new("42").unwrap()
    }

    #[tokio::test]
    async fn routes_a_value_and_reports_stats() {
        let router = Router::new(MemoryStore::new());
        router
            .execute(Command::Set {
                namespace: namespace(),
                key: key(),
                value: json!({"name": "Ada"}),
                ttl_seconds: None,
            })
            .await
            .unwrap();
        let value = router
            .execute(Command::Get {
                namespace: namespace(),
                key: key(),
            })
            .await
            .unwrap();
        assert!(matches!(value, Response::Value { value, .. } if value == json!({"name": "Ada"})));
        assert_eq!(
            router.stats().await.unwrap(),
            Stats {
                requests: 2,
                hits: 1,
                misses: 0,
                values: 1
            }
        );
    }

    #[tokio::test]
    async fn expiry_is_not_returned() {
        let router = Router::new(MemoryStore::new());
        router
            .execute(Command::Set {
                namespace: namespace(),
                key: key(),
                value: json!(1),
                ttl_seconds: Some(0),
            })
            .await
            .unwrap();
        assert!(matches!(
            router
                .execute(Command::Get {
                    namespace: namespace(),
                    key: key()
                })
                .await,
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn moves_only_contexts_in_one_namespace() {
        let router = Router::new(MemoryStore::new());
        let result = router
            .execute(Command::Move {
                source: NamespaceContext::new("/users").unwrap(),
                destination: NamespaceContext::new("/archive").unwrap(),
            })
            .await;
        assert!(matches!(result, Err(Error::Protocol(_))));
    }
}
