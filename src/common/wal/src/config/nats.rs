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

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use serde::{Deserialize, Serialize};

/// Default NATS server URL.
pub const DEFAULT_NATS_SERVER: &str = "nats://127.0.0.1:4222";
/// Default cluster name used to form the JetStream stream name.
pub const DEFAULT_CLUSTER_NAME: &str = "greptimedb";
/// Default JetStream stream name prefix. The full stream name is
/// `"{prefix}-{cluster_name}"`.
pub const DEFAULT_STREAM_NAME_PREFIX: &str = "greptimedb-wal";
/// Default subject prefix. Each region's WAL subject is `"{prefix}.{n}"`.
pub const DEFAULT_SUBJECT_PREFIX: &str = "greptimedb_wal_subject";
/// Default number of pre-generated subjects in the pool (analogous to Kafka topic count).
pub const DEFAULT_NUM_SUBJECTS: u32 = 64;
/// Default JetStream stream replica count (1 = no replication).
pub const DEFAULT_NUM_REPLICAS: usize = 1;
/// Default max batch bytes per publish.
pub const DEFAULT_MAX_BATCH_BYTES: ReadableSize = ReadableSize::mb(1);
/// Default timeout waiting for a publish acknowledgement.
pub const DEFAULT_PUBLISH_ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// Default consumer fetch wait timeout.
pub const DEFAULT_CONSUMER_WAIT_TIMEOUT: Duration = Duration::from_millis(500);
/// Default background purge interval.
pub const DEFAULT_PURGE_INTERVAL: Duration = Duration::from_secs(30);
/// Default sequence-cache refresh interval.
pub const DEFAULT_SEQ_CACHE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// TLS configuration for connecting to NATS.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NatsTlsConfig {
    /// Path to a PEM-encoded CA certificate file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_cert_path: Option<String>,
    /// Path to the client certificate file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_cert_path: Option<String>,
    /// Path to the client key file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_key_path: Option<String>,
}

/// WAL configuration for a NATS JetStream-backed datanode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct DatanodeNatsConfig {
    /// NATS server URLs.
    ///
    /// Example: `["nats://nats.monitoring.svc.cluster.local:4222"]`
    pub servers: Vec<String>,

    /// Prefix used to name the JetStream stream.
    ///
    /// The actual stream is named `"{stream_name_prefix}-{cluster_name}"`.
    pub stream_name_prefix: String,

    /// Prefix used to name WAL subjects.
    ///
    /// Subjects in the pool are named `"{subject_prefix}.{n}"` for
    /// `n` in `0..num_subjects`.  Multiple regions may share a subject
    /// (round-robin assignment, analogous to Kafka topics).
    pub subject_prefix: String,

    /// Number of subjects in the pool.  Analogous to `num_topics` in the
    /// Kafka WAL configuration.
    pub num_subjects: u32,

    /// Number of JetStream stream replicas.
    ///
    /// Set to 3 for high-availability clusters; 1 for single-node deployments.
    pub num_replicas: usize,

    /// Maximum number of bytes allowed per publish batch.
    pub max_batch_bytes: ReadableSize,

    /// Timeout when waiting for a publish acknowledgement from the NATS server.
    #[serde(with = "humantime_serde")]
    pub publish_ack_timeout: Duration,

    /// Maximum wait time when fetching messages from a pull consumer.
    #[serde(with = "humantime_serde")]
    pub consumer_wait_timeout: Duration,

    /// How often the background purge worker runs to remove obsolete WAL entries.
    #[serde(with = "humantime_serde")]
    pub purge_interval: Duration,

    /// How often the background task refreshes the cached latest sequence number
    /// for each subject from the NATS server.
    #[serde(with = "humantime_serde")]
    pub seq_cache_refresh_interval: Duration,

    /// Optional NKey seed for NATS authentication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nkey_seed: Option<String>,

    /// Optional path to a NATS credentials file (`.creds`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_file: Option<String>,

    /// Optional plaintext username for NATS user/password authentication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// Optional plaintext password for NATS user/password authentication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    /// Cluster name used to form the JetStream stream name:
    /// `"{stream_name_prefix}-{cluster_name}"`.
    ///
    /// All datanodes in the same cluster must use the same value.
    pub cluster_name: String,

    /// Optional TLS configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<NatsTlsConfig>,
}

impl Default for DatanodeNatsConfig {
    fn default() -> Self {
        Self {
            servers: vec![DEFAULT_NATS_SERVER.to_string()],
            stream_name_prefix: DEFAULT_STREAM_NAME_PREFIX.to_string(),
            subject_prefix: DEFAULT_SUBJECT_PREFIX.to_string(),
            num_subjects: DEFAULT_NUM_SUBJECTS,
            num_replicas: DEFAULT_NUM_REPLICAS,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            publish_ack_timeout: DEFAULT_PUBLISH_ACK_TIMEOUT,
            consumer_wait_timeout: DEFAULT_CONSUMER_WAIT_TIMEOUT,
            purge_interval: DEFAULT_PURGE_INTERVAL,
            seq_cache_refresh_interval: DEFAULT_SEQ_CACHE_REFRESH_INTERVAL,
            nkey_seed: None,
            credentials_file: None,
            username: None,
            password: None,
            cluster_name: DEFAULT_CLUSTER_NAME.to_string(),
            tls: None,
        }
    }
}

/// WAL configuration for metasrv when using NATS JetStream as the remote WAL backend.
///
/// Metasrv uses this config to derive WAL subject names and allocate them to regions.
/// Unlike the Kafka provider, no NATS calls are made at allocation time — subjects are
/// assigned deterministically from the pool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct MetasrvNatsConfig {
    /// NATS server URLs (used to validate connectivity on startup).
    pub servers: Vec<String>,

    /// Must match the `stream_name_prefix` configured on the datanodes.
    pub stream_name_prefix: String,

    /// Must match the `subject_prefix` configured on the datanodes.
    ///
    /// Subjects are derived as `"{subject_prefix}.{n}"` for `n` in `0..num_subjects`.
    pub subject_prefix: String,

    /// Must match the `num_subjects` configured on the datanodes.
    pub num_subjects: u32,

    /// Number of JetStream stream replicas (informational; datanodes create the stream).
    pub num_replicas: usize,

    /// Optional NKey seed for NATS authentication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nkey_seed: Option<String>,

    /// Optional path to a NATS credentials file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_file: Option<String>,

    /// Optional plaintext username.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// Optional plaintext password.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    /// Optional TLS configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<NatsTlsConfig>,
}

impl Default for MetasrvNatsConfig {
    fn default() -> Self {
        Self {
            servers: vec![DEFAULT_NATS_SERVER.to_string()],
            stream_name_prefix: DEFAULT_STREAM_NAME_PREFIX.to_string(),
            subject_prefix: DEFAULT_SUBJECT_PREFIX.to_string(),
            num_subjects: DEFAULT_NUM_SUBJECTS,
            num_replicas: DEFAULT_NUM_REPLICAS,
            nkey_seed: None,
            credentials_file: None,
            username: None,
            password: None,
            tls: None,
        }
    }
}

impl From<DatanodeNatsConfig> for MetasrvNatsConfig {
    fn from(config: DatanodeNatsConfig) -> Self {
        Self {
            servers: config.servers,
            stream_name_prefix: config.stream_name_prefix,
            subject_prefix: config.subject_prefix,
            num_subjects: config.num_subjects,
            num_replicas: config.num_replicas,
            nkey_seed: config.nkey_seed,
            credentials_file: config.credentials_file,
            username: config.username,
            password: config.password,
            tls: config.tls,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datanode_nats_config_defaults() {
        let config = DatanodeNatsConfig::default();
        assert_eq!(config.servers, vec![DEFAULT_NATS_SERVER]);
        assert_eq!(config.num_subjects, DEFAULT_NUM_SUBJECTS);
        assert_eq!(config.num_replicas, DEFAULT_NUM_REPLICAS);
        assert_eq!(config.max_batch_bytes, DEFAULT_MAX_BATCH_BYTES);
    }

    #[test]
    fn test_toml_nats_config_roundtrip() {
        let toml_str = r#"
            provider = "nats_jetstream"
            servers = ["nats://nats.monitoring.svc.cluster.local:4222"]
            num_subjects = 32
            num_replicas = 3
            subject_prefix = "my_wal"
        "#;
        let config: crate::config::DatanodeWalConfig = toml::from_str(toml_str).unwrap();
        if let crate::config::DatanodeWalConfig::NatsJetstream(nats) = config {
            assert_eq!(
                nats.servers,
                vec!["nats://nats.monitoring.svc.cluster.local:4222"]
            );
            assert_eq!(nats.num_subjects, 32);
            assert_eq!(nats.num_replicas, 3);
        } else {
            panic!("expected NatsJetstream variant");
        }
    }
}
