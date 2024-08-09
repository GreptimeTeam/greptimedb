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

use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::buf::Writer;
use bytes::{BufMut, Bytes, BytesMut};
use common_telemetry::tracing::error;
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::logstore::provider::KafkaProvider;
use store_api::logstore::EntryId;
use store_api::storage::RegionId;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex as TokioMutex;

use crate::error::{self, Result};
use crate::kafka::worker::{DumpIndexRequest, WorkerRequest};

pub trait IndexEncoder: Send + Sync {
    fn encode(&self, provider: &KafkaProvider, region_index: &RegionIndexes);

    fn finish(&self) -> Result<Vec<u8>>;
}

/// The [`IndexCollector`] trait defines the operations for managing and collecting index entries.
pub trait IndexCollector: Send + Sync {
    /// Appends an [`EntryId`] for a specific region.
    fn append(&mut self, region_id: RegionId, entry_id: EntryId);

    /// Truncates the index for a specific region up to a given [`EntryId`].
    ///
    /// It removes all [`EntryId`]s smaller than `entry_id`.
    fn truncate(&mut self, region_id: RegionId, entry_id: EntryId);

    /// Sets the latest [`EntryId`].
    fn set_latest_entry_id(&mut self, entry_id: EntryId);

    /// Dumps the index.
    fn dump(&mut self, encoder: &dyn IndexEncoder);
}

/// The [`GlobalIndexCollector`] struct is responsible for managing index entries
/// across multiple providers.
#[derive(Debug, Clone, Default)]
pub struct GlobalIndexCollector {
    providers: Arc<TokioMutex<HashMap<Arc<KafkaProvider>, Sender<WorkerRequest>>>>,
}

impl GlobalIndexCollector {
    /// Creates a new [`ProviderLevelIndexCollector`] for a specified provider.
    pub fn provider_level_index_collector(
        &self,
        provider: Arc<KafkaProvider>,
        sender: Sender<WorkerRequest>,
    ) -> Box<dyn IndexCollector> {
        Box::new(ProviderLevelIndexCollector {
            indexes: Default::default(),
            provider,
        })
    }
}

/// The [`RegionIndexes`] struct maintains indexes for a collection of regions.
/// Each region is identified by a `RegionId` and maps to a set of [`EntryId`]s,
/// representing the entries within that region. It also keeps track of the
/// latest [`EntryId`] across all regions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegionIndexes {
    regions: HashMap<RegionId, BTreeSet<EntryId>>,
    latest_entry_id: EntryId,
}

impl RegionIndexes {
    fn append(&mut self, region_id: RegionId, entry_id: EntryId) {
        self.regions.entry(region_id).or_default().insert(entry_id);
        self.latest_entry_id = self.latest_entry_id.max(entry_id);
    }

    fn truncate(&mut self, region_id: RegionId, entry_id: EntryId) {
        if let Some(entry_ids) = self.regions.get_mut(&region_id) {
            *entry_ids = entry_ids.split_off(&entry_id);
            // The `RegionIndexes` may be, keeps to track the latest entry id.
            self.latest_entry_id = self.latest_entry_id.max(entry_id);
        }
    }

    fn set_latest_entry_id(&mut self, entry_id: EntryId) {
        self.latest_entry_id = entry_id;
    }
}

/// The [`ProviderLevelIndexCollector`] struct is responsible for managing index entries
/// specific to a particular provider.
#[derive(Debug, Clone)]
pub struct ProviderLevelIndexCollector {
    indexes: RegionIndexes,
    provider: Arc<KafkaProvider>,
}

impl IndexCollector for ProviderLevelIndexCollector {
    fn append(&mut self, region_id: RegionId, entry_id: EntryId) {
        self.indexes.append(region_id, entry_id)
    }

    fn truncate(&mut self, region_id: RegionId, entry_id: EntryId) {
        self.indexes.truncate(region_id, entry_id)
    }

    fn set_latest_entry_id(&mut self, entry_id: EntryId) {
        self.indexes.set_latest_entry_id(entry_id);
    }

    fn dump(&mut self, encoder: &dyn IndexEncoder) {
        encoder.encode(&self.provider, &self.indexes)
    }
}

/// The [`NoopCollector`] struct implements the [`IndexCollector`] trait with no-op methods.
///
/// This collector effectively ignores all operations, making it suitable for cases
/// where index collection is not required or should be disabled.
pub struct NoopCollector;

impl IndexCollector for NoopCollector {
    fn append(&mut self, _region_id: RegionId, _entry_id: EntryId) {}

    fn truncate(&mut self, _region_id: RegionId, _entry_id: EntryId) {}

    fn set_latest_entry_id(&mut self, _entry_id: EntryId) {}

    fn dump(&mut self, encoder: &dyn IndexEncoder) {}
}
