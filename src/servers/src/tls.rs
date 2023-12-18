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
use std::io::{BufReader, Error, ErrorKind};

use rustls::ServerConfig;
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use strum::EnumString;

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
}

impl TlsOption {
    pub fn new(mode: Option<TlsMode>, cert_path: Option<String>, key_path: Option<String>) -> Self {
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

        tls_option
    }

    pub fn setup(&self) -> Result<Option<ServerConfig>, Error> {
        if let TlsMode::Disable = self.mode {
            return Ok(None);
        }
        let cert = certs(&mut BufReader::new(File::open(&self.cert_path)?))
            .collect::<Result<Vec<CertificateDer>, Error>>()?;

        let key = {
            let mut pkcs8 = pkcs8_private_keys(&mut BufReader::new(File::open(&self.key_path)?))
                .map(|key| key.map(PrivateKeyDer::from))
                .collect::<Result<Vec<PrivateKeyDer>, Error>>()?;
            if !pkcs8.is_empty() {
                pkcs8.remove(0)
            } else {
                let mut rsa = rsa_private_keys(&mut BufReader::new(File::open(&self.key_path)?))
                    .map(|key| key.map(PrivateKeyDer::from))
                    .collect::<Result<Vec<PrivateKeyDer>, Error>>()?;
                if !rsa.is_empty() {
                    rsa.remove(0)
                } else {
                    return Err(Error::new(ErrorKind::InvalidInput, "invalid key"));
                }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::TlsMode::Disable;

    #[test]
    fn test_new_tls_option() {
        assert_eq!(TlsOption::default(), TlsOption::new(None, None, None));
        assert_eq!(
            TlsOption {
                mode: Disable,
                ..Default::default()
            },
            TlsOption::new(Some(Disable), None, None)
        );
        assert_eq!(
            TlsOption {
                mode: Disable,
                cert_path: "/path/to/cert_path".to_string(),
                key_path: "/path/to/key_path".to_string(),
            },
            TlsOption::new(
                Some(Disable),
                Some("/path/to/cert_path".to_string()),
                Some("/path/to/key_path".to_string())
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
    }
}
