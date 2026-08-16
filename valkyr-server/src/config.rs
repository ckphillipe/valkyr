use std::{
    fs,
    io::{self, ErrorKind},
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::Deserialize;

const DEFAULT_NATIVE_LISTEN: &str = "127.0.0.1:8081";
const DEFAULT_HTTP_LISTEN: &str = "127.0.0.1:8080";
const DEFAULT_METRICS_LISTEN: &str = "127.0.0.1:8090";
const DEFAULT_LOG_FILTER: &str = "info";
const DEFAULT_SESSION_TTL_SECONDS: u64 = 3_600;
const MOKA_MAX_TIME_TO_IDLE_SECONDS: u64 = 1_000 * 365 * 24 * 3_600;

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ServerConfig {
    pub(crate) native_listen: SocketAddr,
    pub(crate) http_listen: SocketAddr,
    pub(crate) metrics_listen: SocketAddr,
    pub(crate) log_filter: String,
    pub(crate) tls: Option<TlsConfig>,
    pub(crate) auth: Option<AuthConfig>,
    pub(crate) cache: CacheConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            native_listen: DEFAULT_NATIVE_LISTEN.parse().expect("valid native address"),
            http_listen: DEFAULT_HTTP_LISTEN.parse().expect("valid HTTP address"),
            metrics_listen: DEFAULT_METRICS_LISTEN
                .parse()
                .expect("valid metrics address"),
            log_filter: DEFAULT_LOG_FILTER.to_owned(),
            tls: None,
            auth: None,
            cache: CacheConfig::default(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheConfig {
    pub(crate) max_capacity: Option<u64>,
    pub(crate) time_to_idle_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TlsConfig {
    pub(crate) listen: SocketAddr,
    pub(crate) certificate_file: PathBuf,
    pub(crate) private_key_file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthConfig {
    pub(crate) bootstrap_api_key_file: PathBuf,
    #[serde(default = "default_session_ttl_seconds")]
    pub(crate) session_ttl_seconds: u64,
}

pub(crate) fn load_config(path: Option<&Path>) -> Result<ServerConfig, io::Error> {
    let config = match path {
        Some(path) => {
            let source = fs::read_to_string(path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "failed to read configuration file {}: {error}",
                        path.display()
                    ),
                )
            })?;
            let config = serde_yaml::from_str(&source).map_err(|error| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid configuration file {}: {error}", path.display()),
                )
            })?;
            let config_directory = path.parent().unwrap_or_else(|| Path::new("."));
            resolve_file_paths(config, config_directory)
        }
        None => ServerConfig::default(),
    };
    validate_config(&config)?;
    Ok(config)
}

pub(crate) fn bootstrap_api_key(config: &AuthConfig) -> Result<String, io::Error> {
    let value = fs::read_to_string(&config.bootstrap_api_key_file).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to read bootstrap API key file {}: {error}",
                config.bootstrap_api_key_file.display()
            ),
        )
    })?;
    let key = value.trim();
    if key.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "bootstrap API key file {} is empty",
                config.bootstrap_api_key_file.display()
            ),
        ));
    }
    Ok(key.to_owned())
}

fn validate_config(config: &ServerConfig) -> Result<(), io::Error> {
    if config.log_filter.trim().is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "log_filter must not be empty",
        ));
    }
    let auth = config.auth.as_ref().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "auth.bootstrap_api_key_file is required",
        )
    })?;
    if auth.session_ttl_seconds < 2 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "auth.session_ttl_seconds must be at least 2",
        ));
    }
    if config.cache.max_capacity == Some(0) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "cache.max_capacity must be greater than 0",
        ));
    }
    if config.cache.time_to_idle_seconds == Some(0) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "cache.time_to_idle_seconds must be greater than 0",
        ));
    }
    if let Some(seconds) = config.cache.time_to_idle_seconds {
        if seconds > MOKA_MAX_TIME_TO_IDLE_SECONDS {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "cache.time_to_idle_seconds must be at most {MOKA_MAX_TIME_TO_IDLE_SECONDS} (Moka's 1000-year limit)"
                ),
            ));
        }
    }
    Ok(())
}

fn resolve_file_paths(mut config: ServerConfig, config_directory: &Path) -> ServerConfig {
    if let Some(tls) = &mut config.tls {
        resolve_relative_path(&mut tls.certificate_file, config_directory);
        resolve_relative_path(&mut tls.private_key_file, config_directory);
    }
    if let Some(auth) = &mut config.auth {
        resolve_relative_path(&mut auth.bootstrap_api_key_file, config_directory);
    }
    config
}

fn resolve_relative_path(path: &mut PathBuf, config_directory: &Path) {
    if path.is_relative() {
        *path = config_directory.join(&path);
    }
}

const fn default_session_ttl_seconds() -> u64 {
    DEFAULT_SESSION_TTL_SECONDS
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;

    static TEMPORARY_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temporary_file(contents: &str) -> PathBuf {
        let name = format!(
            "valkyr-server-config-{}-{}.yml",
            std::process::id(),
            TEMPORARY_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let path = std::env::temp_dir().join(name);
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn default_listener_addresses_are_loopback() {
        let config = ServerConfig::default();

        assert_eq!(config.native_listen, "127.0.0.1:8081".parse().unwrap());
        assert_eq!(config.http_listen, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.metrics_listen, "127.0.0.1:8090".parse().unwrap());
        assert!(config.tls.is_none());
        assert!(config.auth.is_none());
        assert!(config.cache.max_capacity.is_none());
        assert!(config.cache.time_to_idle_seconds.is_none());
    }

    #[test]
    fn rejects_an_absent_configuration_file_without_authentication() {
        let error = load_config(None).unwrap_err();

        assert_eq!(error.to_string(), "auth.bootstrap_api_key_file is required");
    }

    #[test]
    fn reports_missing_configuration_file_path() {
        let path = std::env::temp_dir().join(format!(
            "valkyr-server-missing-config-{}",
            TEMPORARY_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));

        let error = load_config(Some(&path)).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert!(
            error
                .to_string()
                .contains("failed to read configuration file")
        );
        assert!(error.to_string().contains(path.to_string_lossy().as_ref()));
    }

    #[test]
    fn rejects_unknown_fields() {
        let path = temporary_file("unknown: value\n");

        let error = load_config(Some(&path)).unwrap_err();

        fs::remove_file(path).unwrap();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn rejects_missing_authentication_configuration() {
        let path = temporary_file("log_filter: info\n");

        let error = load_config(Some(&path)).unwrap_err();

        fs::remove_file(path).unwrap();
        assert_eq!(error.to_string(), "auth.bootstrap_api_key_file is required");
    }

    #[test]
    fn loads_complete_configuration() {
        let path = temporary_file(
            r#"native_listen: 0.0.0.0:9001
http_listen: 0.0.0.0:9002
metrics_listen: 0.0.0.0:9003
log_filter: valkyr_server=debug
tls:
  listen: 0.0.0.0:9004
  certificate_file: tls.crt
  private_key_file: tls.key
auth:
  bootstrap_api_key_file: bootstrap
  session_ttl_seconds: 120
cache:
  max_capacity: 5000
  time_to_idle_seconds: 600
"#,
        );

        let config = load_config(Some(&path)).unwrap();

        let config_directory = path.parent().unwrap().to_path_buf();
        fs::remove_file(&path).unwrap();
        assert_eq!(config.native_listen, "0.0.0.0:9001".parse().unwrap());
        assert_eq!(config.http_listen, "0.0.0.0:9002".parse().unwrap());
        assert_eq!(config.metrics_listen, "0.0.0.0:9003".parse().unwrap());
        assert_eq!(config.log_filter, "valkyr_server=debug");
        let tls = config.tls.unwrap();
        assert_eq!(tls.listen, "0.0.0.0:9004".parse().unwrap());
        assert_eq!(tls.certificate_file, config_directory.join("tls.crt"));
        assert_eq!(tls.private_key_file, config_directory.join("tls.key"));
        assert_eq!(
            config.auth.unwrap().bootstrap_api_key_file,
            config_directory.join("bootstrap")
        );
        assert_eq!(config.cache.max_capacity, Some(5000));
        assert_eq!(config.cache.time_to_idle_seconds, Some(600));
    }

    #[test]
    fn rejects_short_auth_session_ttl() {
        let path = temporary_file(
            "auth:\n  bootstrap_api_key_file: /run/valkyr/bootstrap\n  session_ttl_seconds: 1\n",
        );

        let error = load_config(Some(&path)).unwrap_err();

        fs::remove_file(path).unwrap();
        assert_eq!(
            error.to_string(),
            "auth.session_ttl_seconds must be at least 2"
        );
    }

    #[test]
    fn rejects_zero_cache_policies() {
        for (field, expected) in [
            ("max_capacity", "cache.max_capacity must be greater than 0"),
            (
                "time_to_idle_seconds",
                "cache.time_to_idle_seconds must be greater than 0",
            ),
        ] {
            let path = temporary_file(&format!(
                "auth:\n  bootstrap_api_key_file: /run/valkyr/bootstrap\ncache:\n  {field}: 0\n"
            ));
            let error = load_config(Some(&path)).unwrap_err();
            fs::remove_file(path).unwrap();
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn accepts_moka_maximum_idle_duration() {
        let path = temporary_file(&format!(
            "auth:\n  bootstrap_api_key_file: /run/valkyr/bootstrap\ncache:\n  time_to_idle_seconds: {MOKA_MAX_TIME_TO_IDLE_SECONDS}\n"
        ));

        let config = load_config(Some(&path)).unwrap();

        fs::remove_file(path).unwrap();
        assert_eq!(
            config.cache.time_to_idle_seconds,
            Some(MOKA_MAX_TIME_TO_IDLE_SECONDS)
        );
    }

    #[test]
    fn rejects_idle_duration_above_moka_maximum() {
        let path = temporary_file(&format!(
            "auth:\n  bootstrap_api_key_file: /run/valkyr/bootstrap\ncache:\n  time_to_idle_seconds: {}\n",
            MOKA_MAX_TIME_TO_IDLE_SECONDS + 1
        ));

        let error = load_config(Some(&path)).unwrap_err();

        fs::remove_file(path).unwrap();
        assert_eq!(
            error.to_string(),
            format!(
                "cache.time_to_idle_seconds must be at most {MOKA_MAX_TIME_TO_IDLE_SECONDS} (Moka's 1000-year limit)"
            )
        );
    }

    #[test]
    fn trims_bootstrap_api_key_file_contents() {
        let key_path = temporary_file("  secret-key\n");
        let config = AuthConfig {
            bootstrap_api_key_file: key_path.clone(),
            session_ttl_seconds: DEFAULT_SESSION_TTL_SECONDS,
        };

        let key = bootstrap_api_key(&config).unwrap();

        fs::remove_file(key_path).unwrap();
        assert_eq!(key, "secret-key");
    }

    #[test]
    fn rejects_an_empty_bootstrap_api_key_file() {
        let key_path = temporary_file(" \n");
        let config = AuthConfig {
            bootstrap_api_key_file: key_path.clone(),
            session_ttl_seconds: DEFAULT_SESSION_TTL_SECONDS,
        };

        let error = bootstrap_api_key(&config).unwrap_err();

        fs::remove_file(key_path).unwrap();
        assert!(error.to_string().contains("is empty"));
    }

    #[test]
    fn reports_missing_bootstrap_api_key_file_path() {
        let key_path = std::env::temp_dir().join(format!(
            "valkyr-server-missing-bootstrap-{}",
            TEMPORARY_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let config = AuthConfig {
            bootstrap_api_key_file: key_path.clone(),
            session_ttl_seconds: DEFAULT_SESSION_TTL_SECONDS,
        };

        let error = bootstrap_api_key(&config).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert!(
            error
                .to_string()
                .contains("failed to read bootstrap API key file")
        );
        assert!(
            error
                .to_string()
                .contains(key_path.to_string_lossy().as_ref())
        );
    }
}
