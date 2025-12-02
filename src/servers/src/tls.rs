// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::path::Path;
use std::sync::Arc;

use common_grpc::reloadable_tls::{ReloadableTlsConfig, TlsConfigLoader};
use common_telemetry::error;
use rustls::ServerConfig;
use rustls_pemfile::{Item, certs, read_one};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use strum::EnumString;

use crate::error::{InternalIoSnafu, Result};

/// TlsMode is used for Mysql and Postgres server start up.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq, EnumString)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    #[default]
    #[strum(to_string = "disable")]
    Disable,

    #[strum(to_string = "prefer")]
    Prefer,

    #[strum(to_string = "require")]
    Require,

    // TODO(SSebo): Implement the following 2 TSL mode described in
    // ["34.19.3. Protection Provided in Different Modes"](https://www.postgresql.org/docs/current/libpq-ssl.html)
    #[strum(to_string = "verify-ca")]
    VerifyCa,

    #[strum(to_string = "verify-full")]
    VerifyFull,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TlsOption {
    pub mode: TlsMode,
    #[serde(default)]
    pub cert_path: String,
    #[serde(default)]
    pub key_path: String,
    #[serde(default)]
    pub ca_cert_path: String,
    #[serde(default)]
    pub watch: bool,
}

impl TlsOption {
    pub fn new(
        mode: Option<TlsMode>,
        cert_path: Option<String>,
        key_path: Option<String>,
        watch: bool,
    ) -> Self {
        let mut tls_option = TlsOption::default();

        if let Some(mode) = mode {
            tls_option.mode = mode
        };

        if let Some(cert_path) = cert_path {
            tls_option.cert_path = cert_path
        };

        if let Some(key_path) = key_path {
            tls_option.key_path = key_path
        };

        tls_option.watch = watch;

        tls_option
    }

    pub fn setup(&self) -> Result<Option<ServerConfig>> {
        if let TlsMode::Disable = self.mode {
            return Ok(None);
        }
        let cert = certs(&mut BufReader::new(
            File::open(&self.cert_path)
                .inspect_err(|e| error!(e; "Failed to open {}", self.cert_path))
                .context(InternalIoSnafu)?,
        ))
        .collect::<std::result::Result<Vec<CertificateDer>, IoError>>()
        .context(InternalIoSnafu)?;

        let mut key_reader = BufReader::new(
            File::open(&self.key_path)
                .inspect_err(|e| error!(e; "Failed to open {}", self.key_path))
                .context(InternalIoSnafu)?,
        );
        let key = match read_one(&mut key_reader)
            .inspect_err(|e| error!(e; "Failed to read {}", self.key_path))
            .context(InternalIoSnafu)?
        {
            Some(Item::Pkcs1Key(key)) => PrivateKeyDer::from(key),
            Some(Item::Pkcs8Key(key)) => PrivateKeyDer::from(key),
            Some(Item::Sec1Key(key)) => PrivateKeyDer::from(key),
            _ => {
                return Err(IoError::new(ErrorKind::InvalidInput, "invalid key"))
                    .context(InternalIoSnafu);
            }
        };

        // TODO(SSebo): with_client_cert_verifier if TlsMode is Required.
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert, key)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidInput, err))?;

        Ok(Some(config))
    }

    pub fn should_force_tls(&self) -> bool {
        !matches!(self.mode, TlsMode::Disable | TlsMode::Prefer)
    }

    pub fn cert_path(&self) -> &Path {
        Path::new(&self.cert_path)
    }

    pub fn key_path(&self) -> &Path {
        Path::new(&self.key_path)
    }

    pub fn watch_enabled(&self) -> bool {
        self.mode != TlsMode::Disable && self.watch
    }
}

impl TlsConfigLoader<Arc<ServerConfig>> for TlsOption {
    type Error = crate::error::Error;

    fn load(&self) -> Result<Option<Arc<ServerConfig>>> {
        Ok(self.setup()?.map(Arc::new))
    }

    fn watch_paths(&self) -> Vec<&Path> {
        vec![self.cert_path(), self.key_path()]
    }

    fn watch_enabled(&self) -> bool {
        self.mode != TlsMode::Disable && self.watch
    }
}

/// Type alias for server-side reloadable TLS config
pub type ReloadableTlsServerConfig = ReloadableTlsConfig<Arc<ServerConfig>, TlsOption>;

/// Convenience function for watching server TLS configuration
pub fn maybe_watch_server_tls_config(
    tls_server_config: Arc<ReloadableTlsServerConfig>,
) -> Result<()> {
    common_grpc::reloadable_tls::maybe_watch_tls_config(tls_server_config, || {}).map_err(|e| {
        crate::error::Error::Internal {
            err_msg: format!("Failed to watch TLS config: {}", e),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::install_ring_crypto_provider;
    use crate::tls::TlsMode::Disable;

    #[test]
    fn test_new_tls_option() {
        assert_eq!(
            TlsOption::default(),
            TlsOption::new(None, None, None, false)
        );
        assert_eq!(
            TlsOption {
                mode: Disable,
                ..Default::default()
            },
            TlsOption::new(Some(Disable), None, None, false)
        );
        assert_eq!(
            TlsOption {
                mode: Disable,
                cert_path: "/path/to/cert_path".to_string(),
                key_path: "/path/to/key_path".to_string(),
                ca_cert_path: String::new(),
                watch: false
            },
            TlsOption::new(
                Some(Disable),
                Some("/path/to/cert_path".to_string()),
                Some("/path/to/key_path".to_string()),
                false
            )
        );
    }

    #[test]
    fn test_tls_option_disable() {
        let s = r#"
        {
            "mode": "disable"
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();

        assert!(!t.should_force_tls());

        assert!(matches!(t.mode, TlsMode::Disable));
        assert!(t.key_path.is_empty());
        assert!(t.cert_path.is_empty());
        assert!(!t.watch_enabled());

        let setup = t.setup();
        let setup = setup.unwrap();
        assert!(setup.is_none());
    }

    #[test]
    fn test_tls_option_prefer() {
        let s = r#"
        {
            "mode": "prefer",
            "cert_path": "/some_dir/some.crt",
            "key_path": "/some_dir/some.key"
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();

        assert!(!t.should_force_tls());

        assert!(matches!(t.mode, TlsMode::Prefer));
        assert!(!t.key_path.is_empty());
        assert!(!t.cert_path.is_empty());
        assert!(!t.watch_enabled());
    }

    #[test]
    fn test_tls_option_require() {
        let s = r#"
        {
            "mode": "require",
            "cert_path": "/some_dir/some.crt",
            "key_path": "/some_dir/some.key"
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();

        assert!(t.should_force_tls());

        assert!(matches!(t.mode, TlsMode::Require));
        assert!(!t.key_path.is_empty());
        assert!(!t.cert_path.is_empty());
        assert!(!t.watch_enabled());
    }

    #[test]
    fn test_tls_option_verify_ca() {
        let s = r#"
        {
            "mode": "verify_ca",
            "cert_path": "/some_dir/some.crt",
            "key_path": "/some_dir/some.key"
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();

        assert!(t.should_force_tls());

        assert!(matches!(t.mode, TlsMode::VerifyCa));
        assert!(!t.key_path.is_empty());
        assert!(!t.cert_path.is_empty());
        assert!(!t.watch_enabled());
    }

    #[test]
    fn test_tls_option_verify_full() {
        let s = r#"
        {
            "mode": "verify_full",
            "cert_path": "/some_dir/some.crt",
            "key_path": "/some_dir/some.key"
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();

        assert!(t.should_force_tls());

        assert!(matches!(t.mode, TlsMode::VerifyFull));
        assert!(!t.key_path.is_empty());
        assert!(!t.cert_path.is_empty());
        assert!(!t.watch_enabled());
    }

    #[test]
    fn test_tls_option_watch_enabled() {
        let s = r#"
        {
            "mode": "verify_full",
            "cert_path": "/some_dir/some.crt",
            "key_path": "/some_dir/some.key",
            "watch": true
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();

        assert!(t.should_force_tls());

        assert!(matches!(t.mode, TlsMode::VerifyFull));
        assert!(!t.key_path.is_empty());
        assert!(!t.cert_path.is_empty());
        assert!(t.watch_enabled());
    }

    #[test]
    fn test_tls_file_change_watch() {
        common_telemetry::init_default_ut_logging();
        let _ = install_ring_crypto_provider();

        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");

        std::fs::copy("tests/ssl/server.crt", &cert_path).expect("failed to copy cert to tmpdir");
        std::fs::copy("tests/ssl/server-rsa.key", &key_path).expect("failed to copy key to tmpdir");

        assert!(std::fs::exists(&cert_path).unwrap());
        assert!(std::fs::exists(&key_path).unwrap());

        let server_tls = TlsOption {
            mode: TlsMode::Require,
            cert_path: cert_path
                .clone()
                .into_os_string()
                .into_string()
                .expect("failed to convert path to string"),
            key_path: key_path
                .clone()
                .into_os_string()
                .into_string()
                .expect("failed to convert path to string"),
            ca_cert_path: String::new(),
            watch: true,
        };

        let server_config = Arc::new(
            ReloadableTlsServerConfig::try_new(server_tls).expect("failed to create server config"),
        );
        maybe_watch_server_tls_config(server_config.clone())
            .expect("failed to watch server config");

        assert_eq!(0, server_config.get_version());
        assert!(server_config.get_config().is_some());

        let tmp_file = key_path.with_extension("tmp");
        std::fs::copy("tests/ssl/server-pkcs8.key", &tmp_file)
            .expect("Failed to copy temp key file");
        std::fs::rename(&tmp_file, &key_path).expect("Failed to rename temp key file");

        const MAX_RETRIES: usize = 30;
        let mut retries = 0;
        let mut version_updated = false;

        while retries < MAX_RETRIES {
            if server_config.get_version() > 0 {
                version_updated = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            retries += 1;
        }

        assert!(version_updated, "TLS config did not reload in time");
        assert!(server_config.get_version() > 0);
        assert!(server_config.get_config().is_some());
    }
}
