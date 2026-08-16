use crate::{AdapterError, Result};
use reqwest::{Client, Method, StatusCode, Url};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct OpenBaoClient {
    http: Client,
    address: Url,
    mount: String,
    auth: AppRole,
    token: Arc<RwLock<Option<String>>>,
}
#[derive(Clone)]
pub struct AppRole {
    pub role_id: String,
    pub secret_id: String,
}
#[derive(Clone, Debug)]
pub struct Versioned<T> {
    pub value: T,
    pub version: u64,
}

impl OpenBaoClient {
    pub fn new(
        address: &str,
        mount: String,
        timeout: Duration,
        role: AppRole,
        ca_certificate: Option<&[u8]>,
    ) -> Result<Self> {
        let address = Url::parse(address)
            .map_err(|e| AdapterError::Configuration(format!("invalid openbao.address: {e}")))?;
        let mut builder = Client::builder().timeout(timeout);
        if let Some(certificate) = ca_certificate {
            builder =
                builder.add_root_certificate(reqwest::Certificate::from_pem(certificate).map_err(
                    |e| AdapterError::Configuration(format!("invalid OpenBao CA certificate: {e}")),
                )?);
        }
        Ok(Self {
            http: builder.build()?,
            address,
            mount: mount.trim_matches('/').into(),
            auth: role,
            token: Arc::new(RwLock::new(None)),
        })
    }
    fn endpoint(&self, suffix: &str) -> Result<Url> {
        let suffix = suffix.replace('%', "%25");
        self.address
            .join(&format!("v1/{suffix}"))
            .map_err(|e| AdapterError::Configuration(e.to_string()))
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        authenticated: bool,
    ) -> Result<reqwest::Response> {
        if authenticated && self.token.read().await.is_none() {
            self.login().await?;
        }
        let mut request = self.http.request(method, self.endpoint(path)?);
        if authenticated {
            if let Some(token) = self.token.read().await.as_deref() {
                request = request.header("X-Vault-Token", token);
            }
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        Ok(request.send().await?)
    }
    pub async fn login(&self) -> Result<()> {
        let response = self
            .http
            .request(Method::POST, self.endpoint("auth/approle/login")?)
            .json(&json!({"role_id": self.auth.role_id, "secret_id": self.auth.secret_id}))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(AdapterError::OpenBao {
                status: response.status(),
            });
        }
        let value: Value = response.json().await?;
        let token = value
            .pointer("/auth/client_token")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AdapterError::Configuration("OpenBao login response had no client token".into())
            })?;
        *self.token.write().await = Some(token.into());
        Ok(())
    }
    pub async fn renew(&self) -> Result<()> {
        let response = self
            .request(Method::POST, "auth/token/renew-self", Some(json!({})), true)
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            *self.token.write().await = None;
            self.login().await
        }
    }
    pub async fn read(&self, path: &str) -> Result<Option<Versioned<Value>>> {
        let response = self
            .request(
                Method::GET,
                &format!("{}/data/{path}", self.mount),
                None,
                true,
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(AdapterError::OpenBao {
                status: response.status(),
            });
        }
        let value: Value = response.json().await?;
        let data = value
            .pointer("/data/data")
            .cloned()
            .ok_or_else(|| AdapterError::Configuration("OpenBao KV response had no data".into()))?;
        let version = value
            .pointer("/data/metadata/version")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        Ok(Some(Versioned {
            value: data,
            version,
        }))
    }
    /// List immediate KV v2 metadata children below a path.
    pub async fn list(&self, path: &str) -> Result<Vec<String>> {
        let response = self
            .request(
                Method::from_bytes(b"LIST").expect("LIST is a valid HTTP method"),
                &format!("{}/metadata/{path}", self.mount),
                None,
                true,
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        if !response.status().is_success() {
            return Err(AdapterError::OpenBao {
                status: response.status(),
            });
        }
        let value: Value = response.json().await?;
        Ok(value
            .pointer("/data/keys")
            .and_then(Value::as_array)
            .ok_or_else(|| AdapterError::Configuration("OpenBao list response had no keys".into()))?
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                AdapterError::Configuration("OpenBao list response had a non-string key".into())
            })?
            .into_iter()
            .map(str::to_owned)
            .collect())
    }
    pub async fn write(&self, path: &str, data: Value, cas: Option<u64>) -> Result<bool> {
        let mut payload = json!({"data": data});
        if let Some(cas) = cas {
            payload["options"] = json!({"cas": cas});
        }
        let response = self
            .request(
                Method::POST,
                &format!("{}/data/{path}", self.mount),
                Some(payload),
                true,
            )
            .await?;
        if response.status() == StatusCode::BAD_REQUEST && cas.is_some() {
            return Ok(false);
        }
        if !response.status().is_success() {
            return Err(AdapterError::OpenBao {
                status: response.status(),
            });
        }
        Ok(true)
    }
    pub async fn delete(&self, path: &str) -> Result<()> {
        let response = self
            .request(
                Method::DELETE,
                &format!("{}/data/{path}", self.mount),
                None,
                true,
            )
            .await?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(AdapterError::OpenBao {
                status: response.status(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_preserves_percent_encoded_storage_segments() {
        let client = OpenBaoClient::new(
            "https://openbao.example",
            "secret".into(),
            Duration::from_secs(1),
            AppRole {
                role_id: "role".into(),
                secret_id: "secret".into(),
            },
            None,
        )
        .unwrap();

        assert_eq!(
            client
                .endpoint("secret/data/cache/values/%2Forders/root/a%2Fb")
                .unwrap()
                .as_str(),
            "https://openbao.example/v1/secret/data/cache/values/%252Forders/root/a%252Fb"
        );
    }
}
