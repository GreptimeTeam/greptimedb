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

use rskafka::client::{Credentials, SaslConfig};
use rustls::{ClientConfig, RootCertStore};
use serde::{Deserialize, Serialize};
use serde_with::with_prefix;
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::{TopicSelectorType, BROKER_ENDPOINT, TOPIC_NAME_PREFIX};

with_prefix!(pub backoff_prefix "backoff_");

/// Backoff configurations for kafka client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BackoffConfig {
    /// The initial backoff delay.
    #[serde(with = "humantime_serde")]
    pub init: Duration,
    /// The maximum backoff delay.
    #[serde(with = "humantime_serde")]
    pub max: Duration,
    /// The exponential backoff rate, i.e. next backoff = base * current backoff.
    pub base: u32,
    /// The deadline of retries. `None` stands for no deadline.
    #[serde(with = "humantime_serde")]
    pub deadline: Option<Duration>,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            init: Duration::from_millis(500),
            max: Duration::from_secs(10),
            base: 2,
            deadline: Some(Duration::from_secs(60 * 5)), // 5 mins
        }
    }
}

/// The SASL configurations for kafka client.
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

/// The TLS configurations for kafka client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaClientTls {
    pub server_ca_cert_path: String,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
}

impl KafkaClientTls {
    /// Builds the [`ClientConfig`].
    pub async fn to_tls_config(&self) -> Result<Arc<ClientConfig>> {
        let builder = ClientConfig::builder();
        let mut roots = RootCertStore::empty();

        let root_cert_bytes =
            tokio::fs::read(&self.server_ca_cert_path)
                .await
                .context(error::ReadFileSnafu {
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
            let cert_bytes = tokio::fs::read(cert_path)
                .await
                .context(error::ReadFileSnafu { path: cert_path })?;
            let client_certs = rustls_pemfile::certs(&mut Cursor::new(cert_bytes))
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(error::ReadCertsSnafu { path: cert_path })?;

            let key_bytes = tokio::fs::read(key_path)
                .await
                .context(error::ReadFileSnafu { path: key_path })?;
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

/// The connection configurations for kafka clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaConnectionConfig {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
    /// Client SASL.
    pub sasl: Option<KafkaClientSasl>,
    /// Client TLS config
    pub tls: Option<KafkaClientTls>,
}

impl Default for KafkaConnectionConfig {
    fn default() -> Self {
        Self {
            broker_endpoints: vec![BROKER_ENDPOINT.to_string()],
            sasl: None,
            tls: None,
        }
    }
}

/// Topic configurations for kafka clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaTopicConfig {
    /// Number of topics to be created upon start.
    pub num_topics: usize,
    /// Number of partitions per topic.
    pub num_partitions: i32,
    /// The type of the topic selector with which to select a topic for a region.
    pub selector_type: TopicSelectorType,
    /// The replication factor of each topic.
    pub replication_factor: i16,
    /// The timeout of topic creation.
    #[serde(with = "humantime_serde")]
    pub create_topic_timeout: Duration,
    /// Topic name prefix.
    pub topic_name_prefix: String,
}

impl Default for KafkaTopicConfig {
    fn default() -> Self {
        Self {
            num_topics: 64,
            num_partitions: 1,
            selector_type: TopicSelectorType::RoundRobin,
            replication_factor: 1,
            create_topic_timeout: Duration::from_secs(30),
            topic_name_prefix: TOPIC_NAME_PREFIX.to_string(),
        }
    }
}
