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

// The Provider of raft engine log store
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Provider {
    RaftEngine(RaftEngineProvider),
    Kafka(Arc<KafkaProvider>),
}

impl Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Provider::RaftEngine(provider) => {
                write!(f, "region: {}", RegionId::from_u64(provider.id))
            }
            Provider::Kafka(provider) => write!(f, "topic: {}", provider.topic),
        }
    }
}

impl Provider {
    pub fn raft_engine_provider(id: u64) -> Provider {
        Provider::RaftEngine(RaftEngineProvider { id })
    }

    pub fn kafka_provider(topic: String) -> Provider {
        Provider::Kafka(Arc::new(KafkaProvider { topic }))
    }

    /// Returns the type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            Provider::RaftEngine(_) => RaftEngineProvider::type_name(),
            Provider::Kafka(_) => KafkaProvider::type_name(),
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
}
