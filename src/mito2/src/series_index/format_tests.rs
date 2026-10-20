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

//! Small end-to-end wiring tests. The fake samples format deliberately has no codec.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::{IntoError, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    BoundTag, QueryScanContext, RawLabelSource, RegionEngine, RegionScanner,
};
use store_api::storage::{FileId, ScanRequest, TimeSeriesDistribution};

use crate::config::MitoConfig;
use crate::error::{OpenDalSnafu, Result, UnexpectedSnafu};
use crate::read::BoxedRecordBatchStream;
use crate::read::scan_region::Scanner;
use crate::series_index::artifact::ArtifactMetadata;
use crate::series_index::catalog::load_version_control;
use crate::series_index::format::{FormatWriter, SamplesFormat, SamplesReadRequest, SeriesFormats};
use crate::series_index::maintenance::reconcile_series_indexes;
use crate::series_index::purger::series_index_channel;
use crate::series_index::tests::prepare_region_with_config;
use crate::test_util::TestEnv;

#[derive(Debug, Default)]
struct FakeSamples {
    batches: Arc<Mutex<HashMap<FileId, Vec<RecordBatch>>>>,
    reads: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}

struct FakeWriter {
    batches: Arc<Mutex<HashMap<FileId, Vec<RecordBatch>>>>,
    buffered: Vec<RecordBatch>,
    store: ObjectStore,
    artifact: ArtifactMetadata,
    fail: Arc<AtomicBool>,
}

#[async_trait]
impl FormatWriter for FakeWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.fail.load(Ordering::Relaxed) {
            return UnexpectedSnafu {
                reason: "injected samples write failure",
            }
            .fail();
        }
        self.buffered.push(batch.clone());
        Ok(())
    }
    async fn finish(self: Box<Self>) -> Result<ArtifactMetadata> {
        self.batches
            .lock()
            .unwrap()
            .insert(self.artifact.generation, self.buffered);
        Ok(self.artifact)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        for path in &self.artifact.objects {
            self.store.delete(path).await.context(OpenDalSnafu)?;
        }
        Ok(())
    }
}

#[async_trait]
impl SamplesFormat for FakeSamples {
    fn name(&self) -> &str {
        "fake-samples"
    }
    async fn writer(
        &self,
        _metadata: RegionMetadataRef,
        store: ObjectStore,
        prefix: &str,
        generation: FileId,
    ) -> Result<Box<dyn FormatWriter>> {
        let objects = vec![format!("{prefix}data"), format!("{prefix}directory")];
        for path in &objects {
            store.write(path, "fake").await.context(OpenDalSnafu)?;
        }
        Ok(Box::new(FakeWriter {
            batches: self.batches.clone(),
            buffered: Vec::new(),
            store,
            artifact: ArtifactMetadata {
                format: self.name().into(),
                generation,
                objects,
                file_size: 8,
            },
            fail: self.fail.clone(),
        }))
    }
    fn supports(&self, request: &SamplesReadRequest) -> bool {
        // This fake only admits unbounded scans. Real codecs implement time selection.
        request
            .time_range
            .is_none_or(|range| range.start().is_none() && range.end().is_none())
    }
    async fn read(
        &self,
        _store: ObjectStore,
        artifact: &ArtifactMetadata,
        request: SamplesReadRequest,
    ) -> Result<BoxedRecordBatchStream> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        assert!(!request.series.is_empty());
        let batches = self.batches.lock().unwrap()[&artifact.generation].clone();
        let schema = request.mapper.input_arrow_schema(false);
        let projected = batches
            .into_iter()
            .map(|batch| {
                let columns = schema
                    .fields()
                    .iter()
                    .map(|field| batch.column_by_name(field.name()).unwrap().clone())
                    .collect();
                RecordBatch::try_new(schema.clone(), columns)
                    .map_err(|error| crate::error::NewRecordBatchSnafu.into_error(error))
            })
            .collect::<Vec<_>>();
        Ok(Box::pin(futures::stream::iter(projected)))
    }
}

#[tokio::test]
async fn format_skeleton_build_reopen_and_prepare() {
    let mut env = TestEnv::with_prefix("format-skeleton").await;
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
    let fake = Arc::new(FakeSamples::default());
    *region.series_index_version_control.formats.write().unwrap() = Arc::new(SeriesFormats {
        samples: Some(fake.clone()),
        ..Default::default()
    });
    let store = region.series_index_store.clone().unwrap();
    let (purger, mut purges) = series_index_channel(store.clone());
    let stats = reconcile_series_indexes(
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
    .unwrap();
    assert_eq!(2, stats.built_samples);
    let snapshot = region.series_index_version();
    assert_eq!(2, snapshot.samples.len());
    let reopened = load_version_control(&store, region.region_id, &purger).await;
    assert_eq!(2, reopened.current().samples.len());
    assert!(snapshot.disk_usage() >= 16);
    let stats = reconcile_series_indexes(
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
    .unwrap();
    assert_eq!(0, stats.built_samples);

    let Scanner::Series(scanner) = engine
        .scanner(
            region.region_id,
            ScanRequest {
                distribution: Some(TimeSeriesDistribution::PerSeries),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("expected series scanner")
    };
    let metadata = scanner.input().region_metadata().clone();
    let labels = RawLabelSource::PrimaryKey {
        metric_name: "metric".into(),
        physical_metadata: metadata.clone(),
        logical_tags: metadata
            .column_metadatas
            .iter()
            .filter(|c| c.column_schema.name.starts_with("tag_"))
            .map(|c| BoundTag {
                column_id: c.column_id,
                name: c.column_schema.name.clone().into(),
            })
            .collect::<Vec<_>>()
            .into(),
    };
    let source = scanner
        .prepare_series_partition(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            0,
            &labels,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(1, source.num_series());
    assert_eq!(0, fake.reads.load(Ordering::Relaxed));
    let key = source.series_info(0).unwrap().primary_key;
    assert!(key.len() > 22, "prepared identity retains labels");
    assert!(source.series_info(1).is_err());
    assert!(source.read_series(0..2).is_err());
    assert!(source.read_series(1..0).is_err());
    assert!(
        source
            .read_series(0..0)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    let left = source.read_series(0..1).unwrap();
    let right = source.read_series(0..1).unwrap();
    assert_eq!(
        0,
        fake.reads.load(Ordering::Relaxed),
        "reader construction must stay lazy"
    );
    // A prepared source must retain the old view across both flush and subsequent writes.
    put_points(&engine, &region, &[(2500, 5)]).await;
    crate::test_util::flush_region(&engine, region.region_id, None).await;
    put_points(&engine, &region, &[(2000, 7), (3000, 9)]).await;
    drop(source);
    drop(scanner);
    let left = left.try_collect::<Vec<_>>().await.unwrap();
    let right = right.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        vec![1000, 2000],
        left.iter()
            .flat_map(|c| c.time.as_buffer().iter().copied())
            .collect::<Vec<_>>()
    );
    assert_eq!(2, right.iter().map(|c| c.time.len()).sum::<usize>());
    assert!(left.iter().all(|c| c.primary_key == key));
    assert!(left.last().unwrap().is_last);
    assert!(
        fake.reads.load(Ordering::Relaxed) > 0,
        "exercise sidecar dispatch, not only fallback"
    );

    // Rebuilding a missing sidecar replaces its generation but retains old readers' files.
    let old = snapshot.samples.values().next().unwrap().clone();
    store
        .delete(&old.entry().artifact.objects[0])
        .await
        .unwrap();
    let Scanner::Series(mixed_scanner) = engine
        .scanner(
            region.region_id,
            ScanRequest {
                distribution: Some(TimeSeriesDistribution::PerSeries),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("expected series scanner")
    };
    let mixed = mixed_scanner
        .prepare_series_partition(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            0,
            &labels,
        )
        .await
        .unwrap()
        .unwrap();
    let output = mixed
        .read_series(0..mixed.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut points = Vec::new();
    for chunk in output {
        for i in 0..chunk.time.len() {
            let datatypes::series::samples::SampleRef::Float(value) =
                chunk.fields.get(0).unwrap().sample(i)
            else {
                panic!("expected float")
            };
            points.push((chunk.time.timestamp(i).unwrap(), value));
        }
    }
    assert_eq!(
        vec![(1000, 1.0), (2000, 7.0), (2500, 5.0), (3000, 9.0)],
        points
    );
    drop(mixed);
    drop(mixed_scanner);
    let stats = reconcile_series_indexes(
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
    .unwrap();
    assert_eq!(2, stats.built_samples);
    assert!(purges.try_recv().is_err());
    drop(snapshot);
    assert!(purges.try_recv().is_err());
    drop(old);
    let request = purges.try_recv().unwrap();
    assert_eq!(2, request.objects.unwrap().len());
    engine.stop().await.unwrap();
}

async fn put_points(
    engine: &crate::engine::MitoEngine,
    region: &crate::region::MitoRegionRef,
    points: &[(i64, u64)],
) {
    use api::v1::helper::row;
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Rows, SemanticType, WriteHint};
    use store_api::region_request::{RegionPutRequest, RegionRequest};
    let metadata = region.metadata();
    let mut request = crate::test_util::CreateRequestBuilder::new().build();
    request.column_metadatas = metadata.column_metadatas.clone();
    let full = crate::test_util::rows_schema(&request);
    let mut pk = full[0].clone();
    pk.column_name = "__primary_key".into();
    pk.datatype = ColumnDataType::Binary.into();
    pk.semantic_type = SemanticType::Tag.into();
    engine
        .handle_request(
            region.region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: false,
                rows: Rows {
                    schema: vec![pk, full[5].clone(), full[4].clone()],
                    rows: points
                        .iter()
                        .map(|(time, value)| {
                            row(vec![
                                ValueData::BinaryValue(
                                    crate::test_util::sst_util::new_sparse_primary_key(
                                        &["a", "x"],
                                        &metadata,
                                        10,
                                        0,
                                    ),
                                ),
                                ValueData::TimestampMillisecondValue(*time),
                                ValueData::U64Value(*value),
                            ])
                        })
                        .collect(),
                },
                hint: Some(WriteHint {
                    primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
                }),
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn format_skeleton_abort_removes_all_objects() {
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
        crate::series_index::builder::build_samples(
            &store, &region, &version, file, &fake, &purger
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
