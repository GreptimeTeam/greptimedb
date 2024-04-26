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
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use api::v1::Rows;
use common_telemetry::info;
use common_wal::options::WalOptions;
use futures::{Stream, StreamExt};
use prost::Message;

use crate::logstore::entry::Entry;
use crate::logstore::namespace::Namespace;
use crate::logstore::{EntryId, LogStore};
use crate::storage::RegionId;

// TODO(niebayes): add docs.
pub struct WalEntry {
    pub region_id: RegionId,
    pub entry_id: EntryId,
    pub rows: Rows,
}

pub type WalEntryStream<'a> = Pin<Box<dyn Stream<Item = Result<Vec<WalEntry>>> + Send + 'a>>;

#[derive(Debug)]
pub struct RegionReplayInfo {
    start_entry_id: EntryId,
    wal_options: WalOptions,
}

#[derive(Debug, Default, Clone)]
pub struct WalReader {
    region_replay_infos: Arc<Mutex<HashMap<RegionId, RegionReplayInfo>>>,
}

// FIXME(niebayes): do not implement WalReader in store-api. Only define the WalReader in store-api.
impl WalReader {
    pub fn register_region(
        &self,
        region_id: RegionId,
        start_entry_id: EntryId,
        wal_options: WalOptions,
    ) {
        info!("Register a region to wal reader. region id = {}, start entry id = {}, wal options = {:?}", region_id, start_entry_id, wal_options);
        self.region_replay_infos.lock().unwrap().insert(
            region_id,
            RegionReplayInfo {
                start_entry_id,
                wal_options,
            },
        );
    }

    pub async fn read<S: LogStore>(&self, store: Arc<S>) -> Result<WalEntryStream> {
        // TODO(niebayes): simplify the following codes.
        let region_replay_infos = self.region_replay_infos.lock().unwrap();
        let mut namespaces = Vec::with_capacity(region_replay_infos.len());
        let mut ns_to_id = HashMap::with_capacity(region_replay_infos.len());
        for (region_id, info) in region_replay_infos.iter() {
            let ns = store.namespace(region_id.as_u64(), &info.wal_options);
            namespaces.push(ns.clone());
            ns_to_id.insert(ns, info.start_entry_id);
        }

        // TODO(niebayes): adds a test to ensure the kafka wal works as expected when there's only one topic.
        // FIXME(niebayes): there's an error when there's only topic.
        let ns_groups = store.group_by_namespaces(&namespaces);
        let ns_with_id = ns_groups
            .into_iter()
            .map(|group| {
                let min_start_entry_id = group.iter().map(|ns| ns_to_id[ns]).min().unwrap();
                (group[0].clone(), min_start_entry_id)
            })
            .collect::<Vec<_>>();

        // TODO(niebayes): return all streams to the caller so the caller is able to consume streams in parallel.
        let stream = async_stream::try_stream!({
            for (ns, start_entry_id) in ns_with_id {
                info!(
                    "Start replaying a stream. ns = {:?}, start entry id = {}",
                    ns, start_entry_id
                );
                let mut stream = store.read(&ns, start_entry_id, false).await.unwrap();
                while let Some(result) = stream.next().await {
                    let entries = result.unwrap();
                    for entry in entries {
                        yield Self::decode_entry(entry).unwrap();
                    }
                }
            }
        });
        Ok(stream.boxed())
    }

    fn decode_entry<E: Entry>(entry: E) -> Result<Vec<WalEntry>> {
        let region_id = entry.namespace().id();
        let entry_id = entry.id();
        // FIXME(niebayes): handle decode error.
        let wal_entries = api::v1::WalEntry::decode(entry.data())
            .unwrap()
            .mutations
            .into_iter()
            .filter_map(|m| m.rows)
            .map(|rows| WalEntry {
                region_id: RegionId::from_u64(region_id),
                entry_id,
                rows,
            })
            .collect();
        Ok(wal_entries)
    }
}

// FIXME(niebayes): remove Error since we will move the implementations to another crate.
use common_macro::stack_trace_debug;
use snafu::Snafu;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Placeholder"))]
    Placeholder {},
}

pub type Result<T> = std::result::Result<T, Error>;
