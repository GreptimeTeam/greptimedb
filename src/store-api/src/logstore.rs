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

//! LogStore APIs.

pub mod entry;
pub mod provider;

use std::collections::HashMap;
use std::pin::Pin;

use common_error::ext::ErrorExt;
use entry::Entry;
use futures::Stream;

pub type SendableEntryStream<'a, I, E> = Pin<Box<dyn Stream<Item = Result<Vec<I>, E>> + Send + 'a>>;

pub use crate::logstore::entry::Id as EntryId;
use crate::logstore::provider::Provider;
use crate::storage::RegionId;

// The information used to locate WAL index for the specified region.
#[derive(Debug, Clone, Copy)]
pub struct WalIndex {
    pub region_id: RegionId,
    pub location_id: u64,
}

impl WalIndex {
    pub fn new(region_id: RegionId, location_id: u64) -> Self {
        Self {
            region_id,
            location_id,
        }
    }
}

/// `LogStore` serves as a Write-Ahead-Log for storage engine.
#[async_trait::async_trait]
pub trait LogStore: Send + Sync + 'static + std::fmt::Debug {
    type Error: ErrorExt + Send + Sync + 'static;

    /// Stops components of the logstore.
    async fn stop(&self) -> Result<(), Self::Error>;

    /// Appends a batch of entries and returns a response containing a map where the key is a region id
    /// while the value is the id of the last successfully written entry of the region.
    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse, Self::Error>;

    /// Creates a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`.
    async fn read(
        &self,
        provider: &Provider,
        id: EntryId,
        index: Option<WalIndex>,
    ) -> Result<SendableEntryStream<'static, Entry, Self::Error>, Self::Error>;

    /// Creates a new `Namespace` from the given ref.
    async fn create_namespace(&self, ns: &Provider) -> Result<(), Self::Error>;

    /// Deletes an existing `Namespace` specified by the given ref.
    async fn delete_namespace(&self, ns: &Provider) -> Result<(), Self::Error>;

    /// Lists all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Provider>, Self::Error>;

    /// Marks all entries with ids `<=entry_id` of the given `namespace` as obsolete,
    /// so that the log store can safely delete those entries. This method does not guarantee
    /// that the obsolete entries are deleted immediately.
    async fn obsolete(
        &self,
        provider: &Provider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<(), Self::Error>;

    /// Makes an entry instance of the associated Entry type
    fn entry(
        &self,
        data: &mut Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry, Self::Error>;

    /// Returns the highest existing entry id in the log store.
    fn high_watermark(&self, provider: &Provider) -> Result<EntryId, Self::Error>;
}

/// The response of an `append` operation.
#[derive(Debug, Default)]
pub struct AppendResponse {
    /// The id of the entry appended to the log store.
    pub last_entry_id: EntryId,
}

/// The response of an `append_batch` operation.
#[derive(Debug, Default)]
pub struct AppendBatchResponse {
    /// Key: region id (as u64). Value: the id of the last successfully written entry of the region.
    pub last_entry_ids: HashMap<RegionId, EntryId>,
}
