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

//! Artifact lifecycle tests; fake formats have no codec.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datatypes::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngine;
use store_api::storage::FileId;

use crate::config::MitoConfig;
use crate::error::{OpenDalSnafu, Result, UnexpectedSnafu};
use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata, ArtifactObject};
use crate::series_index::catalog::SeriesIndexEntry;
use crate::series_index::format::{FormatWriter, SamplesFormat, SeriesFormats, SeriesIndexFormat};
use crate::series_index::maintenance::reconcile_series_indexes;
use crate::series_index::purger::series_index_channel;
use crate::series_index::tests::prepare_region_with_config;
use crate::test_util::TestEnv;

#[derive(Debug, Default)]
struct FakeSamples {
    fail: Arc<AtomicBool>,
}

struct FakeWriter {
    store: ObjectStore,
    artifact: ArtifactMetadata,
    fail: Arc<AtomicBool>,
}

#[async_trait]
impl FormatWriter for FakeWriter {
    async fn write(&mut self, _batch: &RecordBatch) -> Result<()> {
        if self.fail.load(Ordering::Relaxed) {
            return UnexpectedSnafu {
                reason: "injected samples write failure",
            }
            .fail();
        }
        Ok(())
    }
    async fn finish(self: Box<Self>) -> Result<ArtifactMetadata> {
        Ok(self.artifact)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        for object in &self.artifact.objects {
            self.store
                .delete(&object.path)
                .await
                .context(OpenDalSnafu)?;
        }
        Ok(())
    }
}

#[async_trait]
impl SamplesFormat for FakeSamples {
    fn format(&self) -> ArtifactFormat {
        ArtifactFormat::FakeSamples
    }
    async fn writer(
        &self,
        _metadata: RegionMetadataRef,
        store: ObjectStore,
        prefix: &str,
        _source_file_id: FileId,
        generation: FileId,
    ) -> Result<Box<dyn FormatWriter>> {
        let objects = vec![format!("{prefix}data"), format!("{prefix}directory")];
        for path in &objects {
            store.write(path, "fake").await.context(OpenDalSnafu)?;
        }
        Ok(Box::new(FakeWriter {
            store,
            artifact: ArtifactMetadata {
                format: self.format(),
                generation,
                objects: objects
                    .into_iter()
                    .map(|path| ArtifactObject { path, file_size: 4 })
                    .collect(),
                file_size: 8,
            },
            fail: self.fail.clone(),
        }))
    }
}

#[tokio::test]
async fn daily_samples_abort_removes_all_objects() {
    let mut env = TestEnv::with_prefix("format-abort").await;
    let (engine, region) = crate::series_index::tests::prepare_region(&mut env).await;
    let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
    let (purger, _receiver) = series_index_channel(store.clone());
    let version = region.version();
    let file = version
        .ssts
        .levels()
        .iter()
        .flat_map(|l| l.files())
        .next()
        .unwrap()
        .clone();
    let fake = FakeSamples::default();
    fake.fail.store(true, Ordering::Relaxed);
    assert!(
        crate::series_index::builder::build_sample_windows(
            &store,
            &region,
            &version.metadata,
            &[file],
            &fake,
            &purger,
        )
        .await
        .is_err()
    );
    assert!(
        store
            .list_with(&format!("{}/samples/", region.region_id.as_u64()))
            .recursive(true)
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.metadata().is_dir())
    );
    assert!(region.series_index_version().samples.is_empty());
    engine.stop().await.unwrap();
}

/// Samples are only written in the daily columnar layout, so a backend naming another format
/// must fail instead of publishing nothing: the published version stays as it was.
#[tokio::test]
async fn unsupported_samples_format_is_refused() {
    let mut env = TestEnv::with_prefix("format-unsupported-samples").await;
    let (engine, region) = prepare_region_with_config(
        &mut env,
        &[1000, 2000],
        MitoConfig {
            experimental_enable_series_index: true,
            experimental_series_index_maintenance_interval: Duration::from_secs(3600),
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        },
    )
    .await;
    *region.series_index_version_control.formats.write().unwrap() = Arc::new(SeriesFormats {
        samples: Some(Arc::new(FakeSamples::default())),
        ..Default::default()
    });
    let store = region.series_index_store.clone().unwrap();
    let (purger, _purges) = series_index_channel(store.clone());
    let error = reconcile_series_indexes(
        0,
        store.clone(),
        region.clone(),
        Duration::from_secs(100),
        0,
        purger.clone(),
        false,
        true,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, crate::error::Error::UnsupportedOperation { .. }),
        "{error:?}"
    );
    assert!(region.series_index_version().samples.is_empty());
    assert!(
        store
            .list_with(&format!("{}/samples/", region.region_id.as_u64()))
            .recursive(true)
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.metadata().is_dir())
    );
    engine.stop().await.unwrap();
}

#[derive(Debug, Default)]
struct FakeIndex;

#[async_trait]
impl SeriesIndexFormat for FakeIndex {
    fn format(&self) -> ArtifactFormat {
        ArtifactFormat::FakeIndex
    }

    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        entry: &SeriesIndexEntry,
    ) -> Result<Box<dyn FormatWriter>> {
        let path = format!(
            "{}/series/{}.fake",
            metadata.region_id.as_u64(),
            entry.index_uuid
        );
        store.write(&path, "fake").await.context(OpenDalSnafu)?;
        Ok(Box::new(FakeWriter {
            store,
            artifact: ArtifactMetadata {
                format: self.format(),
                generation: entry.index_uuid,
                objects: vec![ArtifactObject { path, file_size: 4 }],
                file_size: 4,
            },
            fail: Arc::new(AtomicBool::new(false)),
        }))
    }
}

#[tokio::test]
async fn format_index_rebuild_is_scoped_and_scans_fall_back_to_ssts() {
    let mut env = TestEnv::with_prefix("format-index").await;
    // Index builds require four SSTs per bucket.
    let timestamps = [1000, 2000, 3000, 4000, 101000, 102000, 103000, 104000];
    let (engine, region) = prepare_region_with_config(
        &mut env,
        &timestamps,
        MitoConfig {
            experimental_enable_series_index: true,
            experimental_series_scan_v2: true,
            experimental_series_index_maintenance_interval: Duration::from_secs(3600),
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        },
    )
    .await;
    *region.series_index_version_control.formats.write().unwrap() = Arc::new(SeriesFormats {
        index: Some(Arc::new(FakeIndex)),
        ..Default::default()
    });
    let store = region.series_index_store.clone().unwrap();
    let (purger, mut purges) = series_index_channel(store.clone());
    let reconcile = || {
        reconcile_series_indexes(
            0,
            store.clone(),
            region.clone(),
            Duration::from_secs(100),
            0,
            purger.clone(),
            false,
            true,
        )
    };
    assert_eq!(2, reconcile().await.unwrap().built_series);
    let before = region.series_index_version();
    assert!(
        before
            .series_indexes
            .values()
            .all(|h| h.entry().format() == ArtifactFormat::FakeIndex)
    );
    assert_eq!(0, reconcile().await.unwrap().built_series);
    let scanner = engine
        .scanner(
            region.region_id,
            store_api::storage::ScanRequest {
                distribution: Some(store_api::storage::TimeSeriesDistribution::PerSeries),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = scanner
        .scan()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        timestamps.len(),
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
    drop(scanner);

    let missing = before.series_indexes.values().next().unwrap();
    store
        .delete(&missing.entry().artifact.as_ref().unwrap().objects[0].path)
        .await
        .unwrap();
    assert_eq!(1, reconcile().await.unwrap().built_series);
    let after = region.series_index_version();
    assert_eq!(
        1,
        after
            .series_indexes
            .keys()
            .filter(|id| before.series_indexes.contains_key(id))
            .count()
    );
    assert!(purges.try_recv().is_err());
    drop(before);
    assert!(purges.try_recv().is_ok());

    // Disabling the injected writer restores ordinary Parquet artifacts without a read adapter.
    *region.series_index_version_control.formats.write().unwrap() =
        Arc::new(SeriesFormats::default());
    assert_eq!(2, reconcile().await.unwrap().built_series);
    assert!(
        region
            .series_index_version()
            .series_indexes
            .values()
            .all(|h| h.entry().artifact.is_none())
    );
    assert_eq!(0, reconcile().await.unwrap().built_series);
    engine.stop().await.unwrap();
}
