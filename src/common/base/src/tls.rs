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
use std::io::{BufReader, ErrorKind};

use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    #[default]
    Disabled,
    Prefered,
    Required,
    Verified,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TlsOption {
    pub mode: TlsMode,
    #[serde(default)]
    pub cert_path: String,
    #[serde(default)]
    pub key_path: String,
}

impl TlsOption {
    pub fn setup(&self) -> Result<Option<ServerConfig>, std::io::Error> {
        if let TlsMode::Disabled = self.mode {
            return Ok(None);
        }
        let cert = certs(&mut BufReader::new(File::open(&self.key_path)?))
            .map_err(|_| std::io::Error::new(ErrorKind::InvalidInput, "invalid cert"))
            .map(|mut certs| certs.drain(..).map(Certificate).collect())?;
        let key = pkcs8_private_keys(&mut BufReader::new(File::open(&self.key_path)?))
            .map_err(|_| std::io::Error::new(ErrorKind::InvalidInput, "invalid key"))
            .map(|mut keys| keys.drain(..).map(PrivateKey).next().unwrap())?;

        let config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(cert, key)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidInput, err))?;

        Ok(Some(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_option_disable() {
        let s = r#"
        {
            "mode": "disabled"
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();
        assert!(matches!(t.mode, TlsMode::Disabled));
        assert!(t.key_path.is_empty());
        assert!(t.cert_path.is_empty());

        let setup = t.setup();
        assert!(setup.is_ok());
        let setup = setup.unwrap();
        assert!(setup.is_none());
    }

    #[test]
    fn test_tls_option_prefered() {
        let s = r#"
        {
            "mode": "prefered",
            "cert_path": "/some_dir/some.crt",
            "key_path": "/some_dir/some.key"
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();
        assert!(matches!(t.mode, TlsMode::Prefered));
        assert!(!t.key_path.is_empty());
        assert!(!t.cert_path.is_empty());
    }

    #[test]
    fn test_tls_option_required() {
        let s = r#"
        {
            "mode": "required",
            "cert_path": "/some_dir/some.crt",
            "key_path": "/some_dir/some.key"
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();
        assert!(matches!(t.mode, TlsMode::Required));
        assert!(!t.key_path.is_empty());
        assert!(!t.cert_path.is_empty());
    }

    #[test]
    fn test_tls_option_verified() {
        let s = r#"
        {
            "mode": "required",
            "cert_path": "/some_dir/some.crt",
            "key_path": "/some_dir/some.key"
        }
        "#;

        let t: TlsOption = serde_json::from_str(s).unwrap();
        assert!(matches!(t.mode, TlsMode::Required));
        assert!(!t.key_path.is_empty());
        assert!(!t.cert_path.is_empty());
    }
}
