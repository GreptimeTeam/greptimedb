// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::File;
use std::io::{BufReader, Error, ErrorKind};

use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};
use serde::{Deserialize, Serialize};

/// TlsMode is used for Mysql and Postgres server start up.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    #[default]
    Disable,
    Prefer,
    Require,
    // TODO(SSebo): Implement the following 2 TSL mode described in
    // ["34.19.3. Protection Provided in Different Modes"](https://www.postgresql.org/docs/current/libpq-ssl.html)
    VerifyCa,
    VerifyFull,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct TlsOption {
    pub mode: TlsMode,
    #[serde(default)]
    pub cert_path: String,
    #[serde(default)]
    pub key_path: String,
}

impl TlsOption {
    pub fn setup(&self) -> Result<Option<ServerConfig>, Error> {
        if let TlsMode::Disable = self.mode {
            return Ok(None);
        }
        let cert = certs(&mut BufReader::new(File::open(&self.cert_path)?))
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid cert"))
            .map(|mut certs| certs.drain(..).map(Certificate).collect())?;
        let key = pkcs8_private_keys(&mut BufReader::new(File::open(&self.key_path)?))
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid key"))
            .map(|mut keys| keys.drain(..).map(PrivateKey).next())?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "invalid key"))?;

        // TODO(SSebo): with_client_cert_verifier if TlsMode is Required.
        let config = ServerConfig::builder()
            .with_safe_defaults()
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
        assert!(setup.is_ok());
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
    fn test_tls_option_verifiy_ca() {
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
    fn test_tls_option_verifiy_full() {
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
