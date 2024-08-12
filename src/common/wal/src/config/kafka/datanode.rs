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

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use rskafka::client::{Credentials, SaslConfig};
use rustls::{ClientConfig, RootCertStore};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::config::kafka::common::{backoff_prefix, BackoffConfig, KafkaTopicConfig};
use crate::error::{self, Result};
use crate::BROKER_ENDPOINT;

/// Kafka wal configurations for datanode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct DatanodeKafkaConfig {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
    /// TODO(weny): Remove the alias once we release v0.9.
    /// The max size of a single producer batch.
    #[serde(alias = "max_batch_size")]
    pub max_batch_bytes: ReadableSize,
    /// The consumer wait timeout.
    #[serde(with = "humantime_serde")]
    pub consumer_wait_timeout: Duration,
    /// The backoff config.
    #[serde(flatten, with = "backoff_prefix")]
    pub backoff: BackoffConfig,
    /// The kafka topic config.
    #[serde(flatten)]
    pub kafka_topic: KafkaTopicConfig,
    /// Client SASL.
    pub sasl: Option<KafkaClientSasl>,
    /// Client TLS config
    pub tls: Option<KafkaClientTls>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaClientSasl {
    #[serde(flatten)]
    pub config: KafkaClientSaslConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "SCREAMING-KEBAB-CASE")]
pub enum KafkaClientSaslConfig {
    Plain {
        username: String,
        password: String,
    },
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256 {
        username: String,
        password: String,
    },
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512 {
        username: String,
        password: String,
    },
}

impl KafkaClientSaslConfig {
    /// Converts to [`SaslConfig`].
    pub fn into_sasl_config(self) -> SaslConfig {
        match self {
            KafkaClientSaslConfig::Plain { username, password } => {
                SaslConfig::Plain(Credentials::new(username, password))
            }
            KafkaClientSaslConfig::ScramSha256 { username, password } => {
                SaslConfig::ScramSha256(Credentials::new(username, password))
            }
            KafkaClientSaslConfig::ScramSha512 { username, password } => {
                SaslConfig::ScramSha512(Credentials::new(username, password))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaClientTls {
    pub server_ca_cert_path: String,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
}

impl KafkaClientTls {
    /// Builds the [`ClientConfig`].
    pub fn to_tsl_config(&self) -> Result<Arc<ClientConfig>> {
        let builder = ClientConfig::builder();
        let mut roots = RootCertStore::empty();

        let root_cert_bytes =
            std::fs::read(&self.server_ca_cert_path).context(error::ReadFileSnafu {
                path: &self.server_ca_cert_path,
            })?;
        let mut cursor = Cursor::new(root_cert_bytes);
        for cert in rustls_pemfile::certs(&mut cursor)
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(error::ReadCertsSnafu {
                path: &self.server_ca_cert_path,
            })?
        {
            roots.add(cert).context(error::AddCertSnafu)?;
        }
        let builder = builder.with_root_certificates(roots);

        let config = if let (Some(cert_path), Some(key_path)) =
            (&self.client_cert_path, &self.client_key_path)
        {
            let cert_bytes =
                std::fs::read(cert_path).context(error::ReadFileSnafu { path: cert_path })?;
            let client_certs = rustls_pemfile::certs(&mut Cursor::new(cert_bytes))
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(error::ReadCertsSnafu { path: cert_path })?;

            let key_bytes = std::fs::read(key_path).unwrap();
            let client_key = rustls_pemfile::private_key(&mut Cursor::new(key_bytes))
                .context(error::ReadKeySnafu { path: key_path })?
                .context(error::KeyNotFoundSnafu { path: key_path })?;

            builder
                .with_client_auth_cert(client_certs, client_key)
                .context(error::SetClientAuthCertSnafu)?
        } else {
            builder.with_no_client_auth()
        };

        Ok(Arc::new(config))
    }
}

impl Default for DatanodeKafkaConfig {
    fn default() -> Self {
        Self {
            broker_endpoints: vec![BROKER_ENDPOINT.to_string()],
            // Warning: Kafka has a default limit of 1MB per message in a topic.
            max_batch_bytes: ReadableSize::mb(1),
            consumer_wait_timeout: Duration::from_millis(100),
            backoff: BackoffConfig::default(),
            kafka_topic: KafkaTopicConfig::default(),
            sasl: None,
            tls: None,
        }
    }
}
