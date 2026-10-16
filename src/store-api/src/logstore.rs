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

    /// Marks all entries of a region as obsolete and removes its dedicated namespace when
    /// supported by the backend.
    async fn obsolete_all(
        &self,
        provider: &Provider,
        region_id: RegionId,
    ) -> Result<(), Self::Error>;

    /// Makes an entry instance of the associated Entry type
    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry, Self::Error>;

    /// Returns the latest entry id in the log store.
    fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId, Self::Error>;

    /// Waits until every entry of the provider's region with an id at or below
    /// `entry_id` has the persistence this log store guarantees for an
    /// acknowledged append, so that a caller can persist `entry_id` as a
    /// replay watermark.
    ///
    /// The default returns at once and keeps each log store's own
    /// acknowledgement guarantee, whatever it is: Raft Engine with
    /// `sync_write = false`, for example, acknowledges an append before its
    /// periodic sync. A log store that acknowledges an append before its
    /// entries have that persistence overrides this method.
    async fn wait_durable(
        &self,
        _provider: &Provider,
        _entry_id: EntryId,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Returns whether the log store currently fails to give appended entries
    /// that persistence, so that a caller about to give up a wait in
    /// [`wait_durable`](Self::wait_durable), such as a flush of a stopping
    /// engine, need not wait any longer.
    ///
    /// The default returns false.
    fn is_failing(&self) -> bool {
        false
    }

    /// Asks the log store to give the entries it acknowledged, and those it
    /// acknowledges from now on until it stops, that persistence as soon as it
    /// can rather than on its usual schedule, because its caller, such as an
    /// engine that begins to stop, will soon wait for them in
    /// [`wait_durable`](Self::wait_durable) and not for long.
    ///
    /// The default does nothing.
    fn begin_drain(&self) {}

    /// Releases every entry of a region the caller dropped, after `obsolete`
    /// marked them obsolete, so that the log store keeps none of them even
    /// across a restart, which never opens the region again to mark its
    /// entries obsolete.
    ///
    /// The default does nothing and keeps each log store's existing handling
    /// of a dropped region. A log store that keeps obsolete marks only in
    /// memory overrides this method and returns once the release is durable.
    async fn release_region(
        &self,
        _provider: &Provider,
        _region_id: RegionId,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Checks, before an existing region is replayed from its persisted
    /// state, that the log store holds the region's WAL history through
    /// `entry_id`, the largest replay watermark the region persisted, so that
    /// an open against a WAL that lost that history fails instead of
    /// silently replaying nothing.
    ///
    /// The default accepts, which keeps the behaviour of every log store that
    /// does not override it.
    async fn check_region_history(
        &self,
        _provider: &Provider,
        _entry_id: EntryId,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Records that the WAL history of the provider's region starts in this
    /// log store, as it does for a region created on it, so that
    /// [`check_region_history`](Self::check_region_history) accepts a later
    /// open of the region by the same log store.
    ///
    /// The default does nothing.
    fn start_region_history(&self, _provider: &Provider) -> Result<(), Self::Error> {
        Ok(())
    }
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
