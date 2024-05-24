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
use std::hash::Hash;
use std::sync::Arc;

/// The namespace id.
/// Usually the namespace id is identical with the region id.
pub type Id = u64;

pub trait Namespace: Send + Sync + Clone + std::fmt::Debug + Hash + PartialEq + Eq {
    /// Returns the namespace id.
    fn id(&self) -> Id;
}

// The namespace of kafka log store
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaNamespace {
    pub topic: String,
}

impl KafkaNamespace {
    pub fn new(topic: String) -> Self {
        Self { topic }
    }

    /// Returns the type name.
    pub fn type_name() -> &'static str {
        "KafkaNamespace"
    }
}

impl Display for KafkaNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.topic)
    }
}

// The namespace of raft engine log store
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftEngineNamespace {
    pub id: u64,
}

impl RaftEngineNamespace {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    /// Returns the type name.
    pub fn type_name() -> &'static str {
        "RaftEngineNamespace"
    }
}

/// The namespace of LogStore
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogStoreNamespace {
    RaftEngine(RaftEngineNamespace),
    Kafka(Arc<KafkaNamespace>),
}

impl LogStoreNamespace {
    pub fn raft_engine_namespace(id: u64) -> LogStoreNamespace {
        LogStoreNamespace::RaftEngine(RaftEngineNamespace { id })
    }

    pub fn kafka_namespace(topic: String) -> LogStoreNamespace {
        LogStoreNamespace::Kafka(Arc::new(KafkaNamespace { topic }))
    }

    /// Returns the type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            LogStoreNamespace::RaftEngine(_) => RaftEngineNamespace::type_name(),
            LogStoreNamespace::Kafka(_) => KafkaNamespace::type_name(),
        }
    }

    /// Returns the reference of [`RaftEngineNamespace`] if it's the type of [`LogStoreNamespace::RaftEngine`].
    pub fn as_raft_engine_namespace(&self) -> Option<&RaftEngineNamespace> {
        if let LogStoreNamespace::RaftEngine(ns) = self {
            return Some(ns);
        }
        None
    }

    /// Returns the reference of [`KafkaNamespace`] if it's the type of [`LogStoreNamespace::Kafka`].
    pub fn as_kafka_namespace(&self) -> Option<&KafkaNamespace> {
        if let LogStoreNamespace::Kafka(ns) = self {
            return Some(ns);
        }
        None
    }
}
