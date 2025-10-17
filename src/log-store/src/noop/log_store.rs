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

use std::collections::HashMap;

use futures::stream;
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::Provider;
use store_api::logstore::{AppendBatchResponse, EntryId, LogStore, SendableEntryStream, WalIndex};
use store_api::storage::RegionId;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy)]
pub struct NoopLogStore;

#[async_trait::async_trait]
impl LogStore for NoopLogStore {
    type Error = Error;

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse> {
        let last_entry_ids = entries
            .iter()
            .map(|entry| (entry.region_id(), 0))
            .collect::<HashMap<RegionId, EntryId>>();
        Ok(AppendBatchResponse { last_entry_ids })
    }

    async fn read(
        &self,
        _provider: &Provider,
        _entry_id: EntryId,
        _index: Option<WalIndex>,
    ) -> Result<SendableEntryStream<'static, Entry, Self::Error>> {
        Ok(Box::pin(stream::empty()))
    }

    async fn create_namespace(&self, _ns: &Provider) -> Result<()> {
        Ok(())
    }

    async fn delete_namespace(&self, _ns: &Provider) -> Result<()> {
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<Provider>> {
        Ok(vec![])
    }

    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry> {
        Ok(Entry::Naive(NaiveEntry {
            provider: provider.clone(),
            region_id,
            entry_id,
            data,
        }))
    }

    async fn obsolete(
        &self,
        _provider: &Provider,
        _region_id: RegionId,
        _entry_id: EntryId,
    ) -> Result<()> {
        Ok(())
    }

    fn latest_entry_id(&self, _provider: &Provider) -> Result<EntryId> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_append_batch() {
        let log_store = NoopLogStore;
        let entries = vec![Entry::Naive(NaiveEntry {
            provider: Provider::noop_provider(),
            region_id: RegionId::new(1, 1),
            entry_id: 1,
            data: vec![1],
        })];

        let last_entry_ids = log_store
            .append_batch(entries)
            .await
            .unwrap()
            .last_entry_ids;
        assert_eq!(last_entry_ids.len(), 1);
        assert_eq!(last_entry_ids[&(RegionId::new(1, 1))], 0);
    }
}
