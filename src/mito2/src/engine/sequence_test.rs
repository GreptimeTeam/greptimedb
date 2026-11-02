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

//! Mito sequence recovery tests using a memory WAL with store-assigned entry ids.
//! Uses ObjectStore providers; Kafka producers are not tested.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use common_wal::options::{ObjectStoreWalOptions, WAL_OPTIONS_KEY, WalOptions};
use futures::stream;
use log_store::error::{Error, IllegalStateSnafu, Result};
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::Provider;
use store_api::logstore::{AppendBatchResponse, EntryId, LogStore, SendableEntryStream};
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{PathType, RegionOpenRequest, RegionPutRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::oneshot;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows_for_key, flush_region, rows_schema,
};

#[derive(Debug)]
enum AppendFault {
    Persist,
    Partial(RegionId),
    Deferred {
        release: oneshot::Receiver<()>,
        saved: oneshot::Sender<()>,
    },
}

#[derive(Debug, Default)]
struct LogState {
    entries: HashMap<Provider, Vec<Entry>>,
    fault: Option<AppendFault>,
    append_calls: usize,
}

impl LogState {
    fn save(&mut self, entries: Vec<Entry>) -> AppendBatchResponse {
        let mut last_entry_ids = HashMap::new();
        for mut entry in entries {
            let log = self.entries.entry(entry.provider().clone()).or_default();
            // Entry ids start from 1 (zero means no data).
            let offset = log.len() as u64 + 1;
            entry.set_entry_id(offset);
            last_entry_ids.insert(entry.region_id(), offset);
            log.push(entry);
        }
        AppendBatchResponse { last_entry_ids }
    }
}

#[derive(Debug, Default)]
struct MemoryLogStore {
    state: Arc<Mutex<LogState>>,
}

impl MemoryLogStore {
    fn fail_next_append(&self, fault: AppendFault) {
        let mut state = self.state.lock().unwrap();
        assert!(state.fault.replace(fault).is_none());
    }
}

#[async_trait::async_trait]
impl LogStore for MemoryLogStore {
    type Error = Error;

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse> {
        let mut state = self.state.lock().unwrap();
        state.append_calls += 1;
        match state.fault.take() {
            None => return Ok(state.save(entries)),
            Some(AppendFault::Persist) => {
                state.save(entries);
            }
            Some(AppendFault::Partial(region_id)) => {
                state.save(
                    entries
                        .into_iter()
                        .filter(|e| e.region_id() == region_id)
                        .collect(),
                );
            }
            Some(AppendFault::Deferred { release, saved }) => {
                let state = self.state.clone();
                common_runtime::spawn_global(async move {
                    if release.await.is_ok() {
                        state.lock().unwrap().save(entries);
                        let _ = saved.send(());
                    }
                });
            }
        }
        IllegalStateSnafu {}.fail()
    }

    async fn read(
        &self,
        provider: &Provider,
        id: EntryId,
    ) -> Result<SendableEntryStream<'static, Entry, Error>> {
        // Snapshot the log for recovery.
        let entries = self
            .state
            .lock()
            .unwrap()
            .entries
            .get(provider)
            .cloned()
            .unwrap_or_default();
        Ok(Box::pin(stream::iter([Ok(entries
            .into_iter()
            .filter(|e| e.entry_id() >= id)
            .collect())])))
    }

    async fn create_namespace(&self, _provider: &Provider) -> Result<()> {
        Ok(())
    }
    async fn delete_namespace(&self, _provider: &Provider) -> Result<()> {
        Ok(())
    }
    async fn list_namespaces(&self) -> Result<Vec<Provider>> {
        Ok(vec![])
    }
    async fn obsolete(
        &self,
        _provider: &Provider,
        _region_id: RegionId,
        _id: EntryId,
    ) -> Result<()> {
        Ok(())
    }
    async fn obsolete_all(&self, _provider: &Provider, _region_id: RegionId) -> Result<()> {
        Ok(())
    }

    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry> {
        Ok(Entry::Naive(NaiveEntry {
            data,
            entry_id,
            region_id,
            provider: provider.clone(),
        }))
    }

    fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .entries
            .get(provider)
            .and_then(|log| log.last())
            .map_or(0, Entry::entry_id))
    }
}

fn wal_options() -> HashMap<String, String> {
    HashMap::from([(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&WalOptions::ObjectStore(ObjectStoreWalOptions::new(
            "sequence-test/".to_string(),
        )))
        .unwrap(),
    )])
}

async fn create_region(
    engine: &MitoEngine,
    region_id: RegionId,
) -> (String, Vec<api::v1::ColumnSchema>) {
    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build();
    let table_dir = request.table_dir.clone();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    (table_dir, schema)
}

async fn open_region(engine: &MitoEngine, region_id: RegionId, table_dir: String) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: wal_options(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
}

fn put_request(schema: &[api::v1::ColumnSchema], keys: &[(&str, usize)]) -> RegionPutRequest {
    RegionPutRequest {
        rows: Rows {
            schema: schema.to_vec(),
            rows: keys
                .iter()
                .flat_map(|(key, value)| build_rows_for_key(key, 0, 1, *value))
                .collect(),
        },
        skip_wal: false,
        hint: None,
        partition_expr_version: None,
    }
}

async fn scan_rows(engine: &MitoEngine, region_id: RegionId) -> String {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    RecordBatches::try_collect(stream)
        .await
        .unwrap()
        .pretty_print()
        .unwrap()
}

#[tokio::test]
async fn test_persisted_wal_error_does_not_overwrite_success() {
    for flat_format in [false, true] {
        for (deferred, flush) in [(false, false), (true, false), (true, true)] {
            let mut env = TestEnv::new().await;
            let store = Arc::new(MemoryLogStore::default());
            let config = MitoConfig {
                default_flat_format: flat_format,
                num_workers: 1,
                ..Default::default()
            };
            let engine = env
                .create_engine_with_log_store(config.clone(), store.clone())
                .await;
            let region_id = RegionId::new(1, 1);
            let (table_dir, schema) = create_region(&engine, region_id).await;
            let (release, released) = oneshot::channel();
            let (saved, saved_rx) = oneshot::channel();
            store.fail_next_append(if deferred {
                AppendFault::Deferred {
                    release: released,
                    saved,
                }
            } else {
                AppendFault::Persist
            });
            assert!(
                engine
                    .handle_request(
                        region_id,
                        RegionRequest::Put(put_request(&schema, &[("a", 1), ("b", 1)]))
                    )
                    .await
                    .is_err()
            );
            let failed = engine
                .get_region(region_id)
                .unwrap()
                .version_control
                .current();
            assert_eq!(failed.committed_sequence, 0);
            assert_eq!(failed.last_sequence, 2);
            assert_eq!(failed.last_entry_id, 0);
            assert_eq!(failed.version.flushed_sequence, 0);
            engine
                .handle_request(
                    region_id,
                    RegionRequest::Put(put_request(&schema, &[("b", 2)])),
                )
                .await
                .unwrap();
            if flush {
                flush_region(&engine, region_id, None).await;
            }
            if deferred {
                release.send(()).unwrap();
                saved_rx.await.unwrap();
            }
            assert!(
                scan_rows(&engine, region_id)
                    .await
                    .contains("| b     | 2.0")
            );
            engine.stop().await.unwrap();
            drop(engine);
            let engine = env.create_engine_with_log_store(config, store).await;
            open_region(&engine, region_id, table_dir).await;
            let recovered = scan_rows(&engine, region_id).await;
            assert!(
                recovered.contains("| b     | 2.0"),
                "deferred={deferred}, flush={flush}, flat={flat_format}: {recovered}"
            );
            let current = engine
                .get_region(region_id)
                .unwrap()
                .version_control
                .current();
            assert_eq!(current.committed_sequence, 3);
            assert_eq!(current.last_sequence, 3);
            engine.stop().await.unwrap();
        }
    }
}

#[tokio::test]
async fn test_partial_region_append_preserves_all_allocations() {
    let mut env = TestEnv::new().await;
    let store = Arc::new(MemoryLogStore::default());
    let engine = env
        .create_engine_with_log_store(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            store.clone(),
        )
        .await;
    let regions = [RegionId::new(1, 1), RegionId::new(1, 2)];
    let mut schemas = Vec::new();
    let mut table_dirs = Vec::new();
    for id in regions {
        let (dir, schema) = create_region(&engine, id).await;
        table_dirs.push(dir);
        schemas.push(schema);
    }
    store.fail_next_append(AppendFault::Partial(regions[0]));
    let requests = regions
        .into_iter()
        .zip(&schemas)
        .map(|(id, schema)| (id, put_request(schema, &[("a", 1), ("b", 1)])))
        .collect();
    for (_, response) in Arc::new(engine.clone()).handle_batch_put_requests(requests) {
        assert!(response.await.is_err());
    }
    assert_eq!(store.state.lock().unwrap().append_calls, 1);
    for (id, schema) in regions.into_iter().zip(&schemas) {
        let region = engine.get_region(id).unwrap();
        let current = region.version_control.current();
        assert_eq!(
            (
                current.last_sequence,
                current.committed_sequence,
                current.last_entry_id
            ),
            (2, 0, 0)
        );
        assert!(current.version.memtables.is_empty());
        assert_eq!(
            store.latest_entry_id(&region.provider).unwrap(),
            u64::from(id == regions[0])
        );
        engine
            .handle_request(id, RegionRequest::Put(put_request(schema, &[("b", 2)])))
            .await
            .unwrap();
        assert_eq!(region.version_control.current().committed_sequence, 3);
    }
    engine.stop().await.unwrap();
    drop(engine);
    let engine = env
        .create_engine_with_log_store(MitoConfig::default(), store)
        .await;
    for (id, dir) in regions.into_iter().zip(table_dirs) {
        open_region(&engine, id, dir).await;
        assert!(scan_rows(&engine, id).await.contains("| b     | 2.0"));
    }
    engine.stop().await.unwrap();
}
