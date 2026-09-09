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

//! Behavioral coverage for series-index reconciliation.

use std::sync::Arc;
use std::time::Duration;

use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Rows, SemanticType, WriteHint};
use object_store::ObjectStore;
use object_store::layers::mock::{self, MockLayerBuilder, oio};
use object_store::services::Memory;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::PRIMARY_KEY_ENCODING;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionPutRequest, RegionRequest};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use store_api::storage::{FileId, RegionId};

use super::catalog::{
    delete_catalogs, load_version_control, range_catalog_path, range_index_path,
    series_catalog_path, series_index_path,
};
use super::maintenance::reconcile_series_indexes;
use super::purger::series_index_channel;
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::region::{MitoRegionRef, RegionMap};
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use crate::test_util::{CreateRequestBuilder, TestEnv, flush_region, rows_schema};
use crate::time_provider::StdTimeProvider;

/// Builds real sparse SSTs; background maintenance is disabled so tests control publication.
pub(super) async fn prepare_region(env: &mut TestEnv) -> (MitoEngine, MitoRegionRef) {
    prepare_region_with_timestamps(env, &[1000, 2000, 3000, 4000]).await
}

async fn prepare_region_with_timestamps(
    env: &mut TestEnv,
    timestamps: &[i64],
) -> (MitoEngine, MitoRegionRef) {
    let engine = env.create_engine(MitoConfig::default()).await;
    let metadata = Arc::new(sst_region_metadata_with_encoding(
        PrimaryKeyEncoding::Sparse,
    ));
    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    request.column_metadatas = metadata.column_metadatas.clone();
    request.primary_key = metadata.primary_key.clone();
    request
        .options
        .insert(PRIMARY_KEY_ENCODING.to_string(), "sparse".to_string());
    request
        .options
        .insert("memtable.type".to_string(), "bulk".to_string());
    request
        .options
        .insert("sst_format".to_string(), "flat".to_string());
    request
        .options
        .insert("compaction.type".to_string(), "twcs".to_string());
    request.options.insert(
        "compaction.twcs.time_window".to_string(),
        "100s".to_string(),
    );
    // Keep the source SSTs stable while tests reconcile multiple buckets.
    for window in ["active_window", "inactive_window"] {
        request.options.insert(
            format!("compaction.twcs.{window}.trigger_file_num"),
            "100".to_string(),
        );
    }
    let full_schema = rows_schema(&request);
    let mut pk_column = full_schema[0].clone();
    pk_column.column_name = PRIMARY_KEY_COLUMN_NAME.to_string();
    pk_column.datatype = ColumnDataType::Binary.into();
    pk_column.semantic_type = SemanticType::Tag.into();
    let schema = vec![pk_column, full_schema[5].clone(), full_schema[4].clone()];
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    for &ts in timestamps {
        engine
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    skip_wal: false,
                    rows: Rows {
                        schema: schema.clone(),
                        rows: vec![row(vec![
                            ValueData::BinaryValue(new_sparse_primary_key(
                                &["a", "x"],
                                &metadata,
                                10,
                                0,
                            )),
                            ValueData::TimestampMillisecondValue(ts),
                            ValueData::U64Value(1),
                        ])],
                    },
                    hint: Some(WriteHint {
                        primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
                    }),
                    partition_expr_version: None,
                }),
            )
            .await
            .unwrap();
        flush_region(&engine, region_id, None).await;
    }
    let region = engine.get_region(region_id).unwrap();
    (engine, region)
}

struct IndexTest {
    region: MitoRegionRef,
    store: ObjectStore,
    purger: super::purger::IndexFilePurger,
}

impl IndexTest {
    fn new(
        region: MitoRegionRef,
    ) -> (
        Self,
        tokio::sync::mpsc::UnboundedReceiver<super::purger::PurgeRequest>,
    ) {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, receiver) = series_index_channel(store.clone());
        (
            Self {
                region,
                store,
                purger,
            },
            receiver,
        )
    }

    async fn reconcile(
        &self,
        store: ObjectStore,
    ) -> crate::error::Result<super::maintenance::ReconcileStats> {
        reconcile_series_indexes(
            0,
            store,
            self.region.clone(),
            Duration::from_secs(100),
            0,
            self.purger.clone(),
        )
        .await
    }
}

#[tokio::test]
async fn test_reconcile_restores_and_reuses_indexes() {
    let mut env = TestEnv::with_prefix("series-reconcile").await;
    let (engine, region) = prepare_region(&mut env).await;
    let (test, _receiver) = IndexTest::new(region.clone());
    let store = &test.store;
    let purger = &test.purger;
    let stats = test.reconcile(store.clone()).await.unwrap();
    assert_eq!((4, 1), (stats.built_range, stats.built_series));
    let first = region.series_index_version();
    let first_id = *first.series_indexes.keys().next().unwrap();

    let missing_paths = [
        range_index_path(
            region.region_id,
            *first.range_indexes.iter().next().unwrap(),
        ),
        series_index_path(region.region_id, first_id),
    ];
    for path in &missing_paths {
        store.delete(path).await.unwrap();
    }

    // Restore catalog entries even when their index files are missing.
    let restored = load_version_control(store, region.region_id, purger)
        .await
        .current();
    assert_eq!(first.range_indexes, restored.range_indexes);
    assert_eq!(first.index_buckets, restored.index_buckets);
    assert_eq!(
        first.series_indexes[&first_id].entry(),
        restored.series_indexes[&first_id].entry()
    );

    // Catalog changes are not reloaded, and missing index files are not repaired.
    for path in [
        range_catalog_path(region.region_id),
        series_catalog_path(region.region_id),
    ] {
        store.write(&path, "{}").await.unwrap();
    }
    let stats = test.reconcile(store.clone()).await.unwrap();
    assert_eq!((0, 0), (stats.built_range, stats.built_series));
    assert!(Arc::ptr_eq(&first, &region.series_index_version()));
    for path in [
        range_catalog_path(region.region_id),
        series_catalog_path(region.region_id),
    ] {
        assert_eq!(
            b"{}".as_slice(),
            store.read(&path).await.unwrap().to_bytes().as_ref()
        );
    }
    for path in &missing_paths {
        assert!(!store.exists(path).await.unwrap());
    }
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_reconcile_range_only_build_and_removal() {
    let mut env = TestEnv::with_prefix("series-range-lifecycle").await;
    let (engine, region) = prepare_region_with_timestamps(&mut env, &[1000]).await;
    let (test, _receiver) = IndexTest::new(region.clone());
    let initial = region.series_index_version();
    let stats = test.reconcile(test.store.clone()).await.unwrap();
    assert_eq!((1, 0), (stats.built_range, stats.built_series));
    let built = region.series_index_version();
    assert!(!Arc::ptr_eq(&initial, &built));
    let files = region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .map(|file| file.meta_ref().clone())
        .collect();
    region.version_control.apply_edit(
        Some(crate::manifest::action::RegionEdit {
            files_to_remove: files,
            files_to_add: Vec::new(),
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        }),
        &[],
        crate::test_util::new_noop_file_purger(),
    );
    let stats = test.reconcile(test.store.clone()).await.unwrap();
    assert_eq!(
        (0, 0, 1),
        (stats.built_range, stats.built_series, stats.removed_range)
    );
    let removed = region.series_index_version();
    assert!(removed.range_indexes.is_empty());
    assert!(!Arc::ptr_eq(&built, &removed));
    let restored = load_version_control(&test.store, region.region_id, &test.purger).await;
    assert!(restored.current().range_indexes.is_empty());
    test.reconcile(test.store.clone()).await.unwrap();
    assert!(Arc::ptr_eq(&removed, &region.series_index_version()));
    engine.stop().await.unwrap();
}

struct FailingSeriesWriter {
    inner: oio::Writer,
    fail: bool,
}

#[rstest::rstest]
#[case::series("series")]
#[case::range_catalog("range-index.json")]
#[case::series_catalog("series-index.json")]
#[tokio::test]
async fn test_reconcile_replaces_whole_bucket_after_failure(#[case] failure: &str) {
    let mut env = TestEnv::with_prefix("series-replacement").await;
    let (engine, region) =
        prepare_region_with_timestamps(&mut env, &[1000, 2000, 3000, 4000, 5000]).await;
    // Retain all source handles while hiding the newest SST for the initial build.
    let sources = region.version();
    let newest = sources
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .max_by_key(|file| file.meta_ref().sequence)
        .unwrap()
        .meta_ref()
        .clone();
    let edit = |add| crate::manifest::action::RegionEdit {
        files_to_add: if add { vec![newest.clone()] } else { vec![] },
        files_to_remove: if add { vec![] } else { vec![newest.clone()] },
        timestamp_ms: None,
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: None,
    };
    region.version_control.apply_edit(
        Some(edit(false)),
        &[],
        crate::test_util::new_noop_file_purger(),
    );
    let (test, mut receiver) = IndexTest::new(region.clone());
    test.reconcile(test.store.clone()).await.unwrap();
    let previous = region.series_index_version();
    let old_id = *previous.series_indexes.keys().next().unwrap();
    assert_eq!(
        4,
        previous.series_indexes[&old_id]
            .entry()
            .source_file_ids
            .len()
    );
    region.version_control.apply_edit(
        Some(edit(true)),
        &[],
        crate::test_util::new_noop_file_purger(),
    );

    let failure = failure.to_string();
    let layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _, inner| {
            Box::new(FailingSeriesWriter {
                inner,
                fail: if failure == "series" {
                    path.contains("/series/")
                } else {
                    path.ends_with(&failure)
                },
            })
        }))
        .build()
        .unwrap();
    assert!(
        test.reconcile(test.store.clone().layer(layer))
            .await
            .is_err()
    );
    assert!(Arc::ptr_eq(&previous, &region.series_index_version()));
    // Failed catalog publication may retire a completed replacement, never its predecessor.
    while let Ok(request) = receiver.try_recv() {
        assert_ne!(old_id, request.file_id.file_id());
    }

    let stats = test.reconcile(test.store.clone()).await.unwrap();
    assert_eq!((1, 1), (stats.built_series, stats.removed_series));
    let replacement = region.series_index_version();
    assert_eq!(1, replacement.series_indexes.len());
    assert!(!replacement.series_indexes.contains_key(&old_id));
    let entry = replacement.series_indexes.values().next().unwrap().entry();
    assert_eq!(5, entry.source_file_ids.len());
    assert!(entry.source_file_ids.contains(&newest.file_id));
    let restored = load_version_control(&test.store, region.region_id, &test.purger).await;
    assert_eq!(replacement.index_buckets, restored.current().index_buckets);
    assert_eq!(1, restored.current().series_indexes.len());
    test.reconcile(test.store.clone()).await.unwrap();
    assert!(Arc::ptr_eq(&replacement, &region.series_index_version()));

    // Publication retires the old file only after the last reader releases its snapshot.
    let old_path = series_index_path(region.region_id, old_id);
    assert!(test.store.exists(&old_path).await.unwrap());
    assert!(receiver.try_recv().is_err());
    drop(previous);
    assert_eq!(old_id, receiver.try_recv().unwrap().file_id.file_id());
    assert!(receiver.try_recv().is_err());
    engine.stop().await.unwrap();
}

impl mock::Write for FailingSeriesWriter {
    async fn write(&mut self, buffer: mock::Buffer) -> mock::Result<()> {
        self.inner.write(buffer).await
    }

    async fn close(&mut self) -> mock::Result<mock::Metadata> {
        if self.fail {
            return Err(mock::Error::new(
                mock::ErrorKind::Unexpected,
                "injected series write failure",
            ));
        }
        self.inner.close().await
    }

    async fn abort(&mut self) -> mock::Result<()> {
        self.inner.abort().await
    }
}

#[tokio::test]
async fn test_failed_series_build_keeps_completed_sst_range_indexes() {
    let mut env = TestEnv::with_prefix("series-build-failure").await;
    let (engine, region) = prepare_region(&mut env).await;
    let (test, mut receiver) = IndexTest::new(region.clone());
    let store = &test.store;
    let layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(|path, _, inner| {
            Box::new(FailingSeriesWriter {
                inner,
                fail: path.contains("/series/"),
            })
        }))
        .build()
        .unwrap();
    let failing_store = store.clone().layer(layer);
    assert!(test.reconcile(failing_store).await.is_err());
    assert!(region.series_index_version().range_indexes.is_empty());
    assert!(receiver.try_recv().is_err());
    for file in region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
    {
        let path = range_index_path(region.region_id, file.file_id().file_id());
        assert!(store.exists(&path).await.unwrap());
    }
    // A retry can rebuild and publish the complete snapshot.
    let stats = test.reconcile(store.clone()).await.unwrap();
    assert_eq!((4, 1), (stats.built_range, stats.built_series));
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_reconcile_publishes_after_region_version_changes() {
    for during_catalog_write in [false, true] {
        let mut env = TestEnv::with_prefix("series-version-change").await;
        let (engine, region) = prepare_region(&mut env).await;
        let initial_version = region.version();
        let (test, mut receiver) = IndexTest::new(region.clone());
        let store = &test.store;
        let purger = &test.purger;
        let target_region = region.clone();
        let catalog_path = range_catalog_path(region.region_id);
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |path, _, inner| {
                let should_advance = if during_catalog_write {
                    path == catalog_path
                } else {
                    path.contains("/series/")
                };
                if should_advance {
                    // Replace the version while reconciliation is writing its captured snapshot.
                    target_region
                        .version_control
                        .alter_options(target_region.version().options.clone());
                }
                inner
            }))
            .build()
            .unwrap();
        let stats = test.reconcile(store.clone().layer(layer)).await.unwrap();

        assert!(!Arc::ptr_eq(&initial_version, &region.version()));
        assert_eq!((4, 1), (stats.built_range, stats.built_series));
        let published = region.series_index_version();
        assert_eq!(4, published.range_indexes.len());
        assert_eq!(1, published.series_indexes.len());
        let restored = load_version_control(store, region.region_id, purger).await;
        assert_eq!(published.range_indexes, restored.current().range_indexes);
        let handle = published.series_indexes.values().next().unwrap();
        assert_eq!(
            handle.entry(),
            restored.current().series_indexes[&handle.entry().index_uuid].entry()
        );
        assert!(receiver.try_recv().is_err());
        engine.stop().await.unwrap();
    }
}

#[tokio::test]
async fn test_failed_reconcile_retires_only_unpublished_series() {
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    use super::bucket::{group_files_into_series_buckets, plan_series_indexes};
    use super::builder::{build_range_index, build_series_index};
    use super::purger::run_index_purge_task;
    use super::version::SeriesIndexVersion;

    for failure in ["series", "range", "range-index.json", "series-index.json"] {
        let mut env = TestEnv::with_prefix("series-unpublished-cleanup").await;
        let mut timestamps = vec![
            -99000, -98000, -97000, -96000, 1000, 2000, 3000, 4000, 101000,
        ];
        if failure != "range" {
            timestamps.extend([102000, 103000, 104000]);
        }
        let (engine, region) = prepare_region_with_timestamps(&mut env, &timestamps).await;
        let (test, mut receiver) = IndexTest::new(region.clone());
        let store = &test.store;
        let purger = &test.purger;

        // Publish the first bucket so failed attempts must preserve a reused handle.
        let version = region.version();
        let files = version
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files())
            .cloned()
            .collect::<Vec<_>>();
        let plan = plan_series_indexes(
            group_files_into_series_buckets(
                &files,
                100,
                version.compaction_time_window.unwrap().as_secs() as i64,
            ),
            Default::default(),
            None,
            0,
        );
        assert_eq!(if failure == "range" { 2 } else { 3 }, plan.builds.len());
        let (bucket, entry) = &plan.builds[0];
        let mut ranges = HashSet::new();
        for file in &bucket.files {
            let id = build_range_index(store, &region, &version, file.clone())
                .await
                .unwrap()
                .unwrap();
            ranges.insert(id);
        }
        let handle = build_series_index(store, &region, &version, bucket, entry, purger)
            .await
            .unwrap();
        let reused_path = series_index_path(region.region_id, entry.index_uuid);
        region
            .series_index_version_control
            .publish(Arc::new(SeriesIndexVersion::new(
                ranges,
                HashMap::from([(entry.index_uuid, handle)]),
            )));
        let previous = region.series_index_version();
        let standalone_path = files
            .iter()
            .find(|file| file.time_range().0 == common_time::Timestamp::new_millisecond(101000))
            .map(|file| range_index_path(region.region_id, file.file_id().file_id()))
            .unwrap();

        // Every completed unpublished output must be retired.
        let attempted = Arc::new(Mutex::new(Vec::<String>::new()));
        let recorded = attempted.clone();
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |path, _, inner| {
                let mut paths = recorded.lock().unwrap();
                if path.contains("/series/") {
                    paths.push(path.to_string());
                }
                let fail = match failure {
                    "series" => path.contains("/series/") && paths.len() == 2,
                    "range" => path == standalone_path,
                    catalog => path.ends_with(catalog),
                };
                Box::new(FailingSeriesWriter { inner, fail })
            }))
            .build()
            .unwrap();
        let result = test.reconcile(store.clone().layer(layer)).await;
        assert!(
            result.is_err(),
            "expected {failure} failure, got {result:?}; attempted series paths: {:?}",
            attempted.lock().unwrap()
        );
        assert!(Arc::ptr_eq(&previous, &region.series_index_version()));

        let completed = attempted
            .lock()
            .unwrap()
            .iter()
            .take(if failure.ends_with(".json") { 2 } else { 1 })
            .cloned()
            .collect::<HashSet<_>>();
        assert_eq!(
            if failure.ends_with(".json") { 2 } else { 1 },
            completed.len()
        );
        let mut retired = HashSet::new();
        // Drain through the real purge task using a finite channel.
        let (drain_purger, drain_receiver) = series_index_channel(store.clone());
        while let Ok(request) = receiver.try_recv() {
            let path = series_index_path(request.file_id.region_id(), request.file_id.file_id());
            assert!(store.exists(&path).await.unwrap());
            assert!(retired.insert(path));
            drain_purger.purge(request);
        }
        assert_eq!(completed, retired);
        drop(drain_purger);
        run_index_purge_task(0, store.clone(), drain_receiver).await;
        let attempted_paths = attempted.lock().unwrap().clone();
        for path in attempted_paths {
            assert!(!store.exists(&path).await.unwrap());
        }
        assert!(store.exists(&reused_path).await.unwrap());

        let stats = test.reconcile(store.clone()).await.unwrap();
        assert_eq!(if failure == "range" { 1 } else { 2 }, stats.built_series);
        let published = region.series_index_version();
        for handle in published.series_indexes.values() {
            assert!(
                store
                    .exists(&series_index_path(
                        region.region_id,
                        handle.entry().index_uuid
                    ))
                    .await
                    .unwrap()
            );
        }
        // Release all snapshots without retiring them: neither reused nor published files
        // should have been marked deleted by the cleanup guard.
        region.series_index_version_control.publish(Arc::default());
        drop(published);
        drop(previous);
        assert!(receiver.try_recv().is_err());
        engine.stop().await.unwrap();
    }
}

/// Waits for a background state change without relying on exact scheduling delays.
async fn wait_for(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_maintenance_wakeup_and_timer() {
    let mut env = TestEnv::with_prefix("series-task").await;
    let (engine, region) = prepare_region(&mut env).await;
    let mut options = region.version().options.clone();
    options.ttl = Some(common_time::TimeToLive::Duration(Duration::from_secs(100)));
    region.version_control.alter_options(options);
    let store = ObjectStore::new(Memory::default()).unwrap();
    let (purger, receiver) = series_index_channel(store.clone());
    let state = Arc::new(super::task::SeriesIndexTaskState::new());
    let clock = Arc::new(crate::time_provider::mock::MockTimeProvider::new(0));
    // A notification issued before the task starts must also trigger maintenance.
    state.wake();
    let regions = Arc::new(RegionMap::default());
    regions.insert_region(region.clone());
    let task = super::task::spawn_series_index_tasks(
        0,
        store,
        regions,
        state.clone(),
        Duration::from_secs(100),
        purger,
        receiver,
        Duration::from_secs(3600),
        clock.clone(),
    );
    wait_for(|| !region.series_index_version().series_indexes.is_empty()).await;
    // Advance event time; periodic maintenance must expire coverage without a wakeup.
    clock.set_now(201_000);
    wait_for(|| region.series_index_version().series_indexes.is_empty()).await;
    state.stop();
    task.await.unwrap();
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_drop_catalogs_and_retained_snapshot_after_task_stop() {
    use std::collections::BTreeMap;

    use common_time::Timestamp;

    use super::catalog::{SeriesIndexCatalog, SeriesIndexEntry, WindowSequence, store_catalog};

    let store = ObjectStore::new(Memory::default()).unwrap();
    let region_id = RegionId::new(1, 1);
    let entry = SeriesIndexEntry {
        index_uuid: FileId::random(),
        bucket_start: Timestamp::new_second(0),
        bucket_end: Timestamp::new_second(100),
        source_file_ids: vec![FileId::random()],
        min_file_sequence: 1,
        max_file_sequence: 2,
        compaction_window_secs: 10,
        window_sequences: BTreeMap::from([(
            0,
            WindowSequence {
                start: 0,
                end: 100,
                max_sequence: 2,
            },
        )]),
    };
    let path = series_index_path(region_id, entry.index_uuid);
    store.write(&path, "index").await.unwrap();
    store_catalog(
        &store,
        &series_catalog_path(region_id),
        &SeriesIndexCatalog {
            indexes: vec![entry.clone()],
        },
    )
    .await
    .unwrap();
    store_catalog(
        &store,
        &range_catalog_path(region_id),
        &super::catalog::RangeIndexCatalog::default(),
    )
    .await
    .unwrap();
    let (purger, receiver) = series_index_channel(store.clone());
    let control = load_version_control(&store, region_id, &purger).await;
    let snapshot = control.current();
    assert_eq!(&entry, snapshot.series_indexes[&entry.index_uuid].entry());
    let state = Arc::new(super::task::SeriesIndexTaskState::new());
    state.stop();
    super::task::spawn_series_index_tasks(
        0,
        store.clone(),
        Arc::new(RegionMap::default()),
        state,
        Duration::from_secs(100),
        purger,
        receiver,
        Duration::from_secs(3600),
        Arc::new(StdTimeProvider),
    )
    .await
    .unwrap();
    delete_catalogs(&store, region_id).await;
    // Removing already absent catalogs is harmless.
    delete_catalogs(&store, region_id).await;
    assert!(!store.exists(&range_catalog_path(region_id)).await.unwrap());
    assert!(!store.exists(&series_catalog_path(region_id)).await.unwrap());
    control.mark_dropped();
    assert!(store.exists(&path).await.unwrap());
    drop(snapshot);
    tokio::time::timeout(Duration::from_secs(10), async {
        while store.exists(&path).await.unwrap() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
