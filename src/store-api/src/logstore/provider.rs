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

/// The Provider of NATS JetStream log store.
///
/// The `topic` field holds the NATS subject string for this region's WAL
/// (e.g. `"greptimedb_wal_subject.42"`).  Multiple regions may share a
/// subject, analogous to the Kafka topic pool.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NatsProvider {
    pub topic: String,
}

impl NatsProvider {
    pub fn new(topic: String) -> Self {
        Self { topic }
    }

    /// Returns the type name.
    pub fn type_name() -> &'static str {
        "NatsProvider"
    }
}

impl Display for NatsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.topic)
    }
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
    /// NATS JetStream remote WAL.
    Nats(Arc<NatsProvider>),
    Noop,
}

impl Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Provider::RaftEngine(provider) => {
                write!(f, "RaftEngine(region={})", RegionId::from_u64(provider.id))
            }
            Provider::Kafka(provider) => write!(f, "Kafka(topic={})", provider.topic),
            Provider::Nats(provider) => write!(f, "Nats(topic={})", provider.topic),
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
        if matches!(self, Provider::Kafka(_) | Provider::Nats(_)) {
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

    pub fn nats_provider(topic: String) -> Provider {
        Provider::Nats(Arc::new(NatsProvider { topic }))
    }

    pub fn noop_provider() -> Provider {
        Provider::Noop
    }

    /// Returns true if it's remote WAL.
    pub fn is_remote_wal(&self) -> bool {
        matches!(self, Provider::Kafka(_) | Provider::Nats(_))
    }

    /// Returns the type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            Provider::RaftEngine(_) => RaftEngineProvider::type_name(),
            Provider::Kafka(_) => KafkaProvider::type_name(),
            Provider::Nats(_) => NatsProvider::type_name(),
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

    /// Returns the reference of [`NatsProvider`] if it's the type of [`Provider::Nats`].
    pub fn as_nats_provider(&self) -> Option<&Arc<NatsProvider>> {
        if let Provider::Nats(ns) = self {
            return Some(ns);
        }
        None
    }
}
