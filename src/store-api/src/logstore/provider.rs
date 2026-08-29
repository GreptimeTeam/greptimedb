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

use std::fmt::Display;
use std::sync::Arc;

use crate::logstore::LogStore;
use crate::storage::RegionId;

// The Provider of kafka log store
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaProvider {
    pub topic: String,
}

impl KafkaProvider {
    pub fn new(topic: String) -> Self {
        Self { topic }
    }

    /// Returns the type name.
    pub fn type_name() -> &'static str {
        "KafkaProvider"
    }
}

impl Display for KafkaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.topic)
    }
}

/// Opaque identity for a log store implemented outside the built-in backends.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExternalProvider {
    backend: String,
    namespace: String,
}

impl ExternalProvider {
    pub fn new(backend: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            namespace: namespace.into(),
        }
    }

    /// Returns the stable backend identity.
    pub fn backend(&self) -> &str {
        &self.backend
    }

    /// Returns the backend-defined namespace identity.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the type name.
    pub fn type_name() -> &'static str {
        "ExternalProvider"
    }
}

impl Display for ExternalProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.backend, self.namespace)
    }
}

// The Provider of raft engine log store
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RaftEngineProvider {
    pub id: u64,
}

impl RaftEngineProvider {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    /// Returns the type name.
    pub fn type_name() -> &'static str {
        "RaftEngineProvider"
    }
}

/// The Provider of LogStore
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Provider {
    RaftEngine(RaftEngineProvider),
    Kafka(Arc<KafkaProvider>),
    External(Arc<ExternalProvider>),
    Noop,
}

impl Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Provider::RaftEngine(provider) => {
                write!(f, "RaftEngine(region={})", RegionId::from_u64(provider.id))
            }
            Provider::Kafka(provider) => write!(f, "Kafka(topic={})", provider.topic),
            Provider::External(provider) => write!(f, "External({provider})"),
            Provider::Noop => write!(f, "Noop"),
        }
    }
}

impl Provider {
    /// Returns the initial flushed entry id of the provider.
    /// This is used to initialize the flushed entry id of the region when creating the region from scratch.
    ///
    /// Currently only used for remote WAL.
    /// For local WAL, the initial flushed entry id is 0.
    pub fn initial_flushed_entry_id<S: LogStore>(&self, wal: &S) -> u64 {
        if matches!(self, Provider::Kafka(_)) {
            return wal.latest_entry_id(self).unwrap_or(0);
        }
        0
    }

    pub fn raft_engine_provider(id: u64) -> Provider {
        Provider::RaftEngine(RaftEngineProvider { id })
    }

    pub fn kafka_provider(topic: String) -> Provider {
        Provider::Kafka(Arc::new(KafkaProvider { topic }))
    }

    pub fn external(provider: ExternalProvider) -> Provider {
        Provider::External(Arc::new(provider))
    }

    pub fn external_provider(backend: impl Into<String>, namespace: impl Into<String>) -> Provider {
        Provider::external(ExternalProvider::new(backend, namespace))
    }

    pub fn noop_provider() -> Provider {
        Provider::Noop
    }

    /// Returns true if it's remote WAL.
    pub fn is_remote_wal(&self) -> bool {
        matches!(self, Provider::Kafka(_))
    }

    /// Returns the type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            Provider::RaftEngine(_) => RaftEngineProvider::type_name(),
            Provider::Kafka(_) => KafkaProvider::type_name(),
            Provider::External(_) => ExternalProvider::type_name(),
            Provider::Noop => "Noop",
        }
    }

    /// Returns the reference of [`RaftEngineProvider`] if it's the type of [`LogStoreProvider::RaftEngine`].
    pub fn as_raft_engine_provider(&self) -> Option<&RaftEngineProvider> {
        if let Provider::RaftEngine(ns) = self {
            return Some(ns);
        }
        None
    }

    /// Returns the reference of [`KafkaProvider`] if it's the type of [`LogStoreProvider::Kafka`].
    pub fn as_kafka_provider(&self) -> Option<&Arc<KafkaProvider>> {
        if let Provider::Kafka(ns) = self {
            return Some(ns);
        }
        None
    }

    /// Returns the reference of [`ExternalProvider`] if present.
    pub fn as_external_provider(&self) -> Option<&ExternalProvider> {
        if let Provider::External(provider) = self {
            return Some(provider);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_external_provider_identity() {
        let provider = Provider::external_provider("test-backend", "node-1/region-2");

        let external = provider.as_external_provider().unwrap();
        assert_eq!("test-backend", external.backend());
        assert_eq!("node-1/region-2", external.namespace());
        assert_eq!(
            "External(test-backend(node-1/region-2))",
            provider.to_string()
        );
        assert!(!provider.is_remote_wal());
    }
}
