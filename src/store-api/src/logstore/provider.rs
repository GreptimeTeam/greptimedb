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

/// The provider of the object store log store.
///
/// Objects under one prefix hold entries of many regions, so reads and obsoletes
/// are scoped by `region_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectStoreProvider {
    pub region_id: RegionId,
    pub prefix: String,
}

impl ObjectStoreProvider {
    pub fn new(region_id: RegionId, prefix: String) -> Self {
        Self { region_id, prefix }
    }

    /// Returns the type name.
    pub fn type_name() -> &'static str {
        "ObjectStoreProvider"
    }
}

impl Display for ObjectStoreProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.prefix, self.region_id)
    }
}

/// The Provider of LogStore
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Provider {
    RaftEngine(RaftEngineProvider),
    Kafka(Arc<KafkaProvider>),
    ObjectStore(Arc<ObjectStoreProvider>),
    Noop,
}

impl Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Provider::RaftEngine(provider) => {
                write!(f, "RaftEngine(region={})", RegionId::from_u64(provider.id))
            }
            Provider::Kafka(provider) => write!(f, "Kafka(topic={})", provider.topic),
            Provider::ObjectStore(provider) => write!(
                f,
                "ObjectStore(prefix={}, region={})",
                provider.prefix, provider.region_id
            ),
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
        if self.is_remote_wal() {
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

    pub fn object_store_provider(region_id: RegionId, prefix: String) -> Provider {
        Provider::ObjectStore(Arc::new(ObjectStoreProvider { region_id, prefix }))
    }

    pub fn noop_provider() -> Provider {
        Provider::Noop
    }

    /// Returns true if it's remote WAL.
    ///
    /// Remote WAL entries are shared, immutable objects that are read and obsoleted per
    /// region, so replay starts from the latest entry id of the WAL instead of 0.
    pub fn is_remote_wal(&self) -> bool {
        matches!(self, Provider::Kafka(_) | Provider::ObjectStore(_))
    }

    /// Returns the type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            Provider::RaftEngine(_) => RaftEngineProvider::type_name(),
            Provider::Kafka(_) => KafkaProvider::type_name(),
            Provider::ObjectStore(_) => ObjectStoreProvider::type_name(),
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

    /// Returns the reference of [`ObjectStoreProvider`] if it's the type of [`LogStoreProvider::ObjectStore`].
    pub fn as_object_store_provider(&self) -> Option<&Arc<ObjectStoreProvider>> {
        if let Provider::ObjectStore(ns) = self {
            return Some(ns);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;

    use super::*;
    use crate::logstore::entry::Entry;
    use crate::logstore::{AppendBatchResponse, EntryId, SendableEntryStream, WalIndex};

    /// A log store whose latest entry id is fixed.
    #[derive(Debug)]
    struct LatestEntryIdLogStore(EntryId);

    #[async_trait::async_trait]
    impl LogStore for LatestEntryIdLogStore {
        type Error = MockError;

        async fn stop(&self) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn append_batch(
            &self,
            _entries: Vec<Entry>,
        ) -> Result<AppendBatchResponse, Self::Error> {
            unreachable!()
        }

        async fn read(
            &self,
            _provider: &Provider,
            _id: EntryId,
            _index: Option<WalIndex>,
        ) -> Result<SendableEntryStream<'static, Entry, Self::Error>, Self::Error> {
            unreachable!()
        }

        async fn create_namespace(&self, _ns: &Provider) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn delete_namespace(&self, _ns: &Provider) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn list_namespaces(&self) -> Result<Vec<Provider>, Self::Error> {
            unreachable!()
        }

        async fn obsolete(
            &self,
            _provider: &Provider,
            _region_id: RegionId,
            _entry_id: EntryId,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn obsolete_all(
            &self,
            _provider: &Provider,
            _region_id: RegionId,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn entry(
            &self,
            _data: Vec<u8>,
            _entry_id: EntryId,
            _region_id: RegionId,
            _provider: &Provider,
        ) -> Result<Entry, Self::Error> {
            unreachable!()
        }

        fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId, Self::Error> {
            if provider.is_remote_wal() {
                Ok(self.0)
            } else {
                Err(MockError::new(StatusCode::Unexpected))
            }
        }
    }

    #[test]
    fn test_object_store_provider() {
        let region_id = RegionId::new(1, 2);
        let provider = Provider::object_store_provider(region_id, "wal".to_string());

        let object_store = provider.as_object_store_provider().unwrap();
        assert_eq!(region_id, object_store.region_id);
        assert_eq!("wal", object_store.prefix);
        assert_eq!(ObjectStoreProvider::type_name(), provider.type_name());
        assert_eq!(
            format!("ObjectStore(prefix=wal, region={region_id})"),
            provider.to_string()
        );
        assert!(provider.is_remote_wal());
        assert!(provider.as_kafka_provider().is_none());
        assert!(provider.as_raft_engine_provider().is_none());
    }

    #[test]
    fn test_initial_flushed_entry_id_follows_remote_wal() {
        let region_id = RegionId::new(1, 2);
        let store = LatestEntryIdLogStore(42);

        let object_store = Provider::object_store_provider(region_id, "wal".to_string());
        assert_eq!(42, object_store.initial_flushed_entry_id(&store));

        let kafka = Provider::kafka_provider("topic".to_string());
        assert_eq!(42, kafka.initial_flushed_entry_id(&store));

        let raft_engine = Provider::raft_engine_provider(region_id.as_u64());
        assert_eq!(0, raft_engine.initial_flushed_entry_id(&store));
        assert_eq!(
            0,
            Provider::noop_provider().initial_flushed_entry_id(&store)
        );
    }
}
