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

use std::sync::Mutex;
use std::time::Duration;

use api::v1::helper::row;
use api::v1::region::build_index_request;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Rows};
use common_error::ext::ErrorExt;
use common_query::series_frame::unpack_raw_series;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, metrics::ExecutionPlanMetricsSet};
use datafusion_expr::{col, lit};
use datatypes::prelude::ConcreteDataType;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SampleRef;
use object_store::ObjectStore;
use object_store::layers::mock::{self, MockLayerBuilder, oio};
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_engine::{
    BoundTag, PrepareRequest, QueryScanContext, RegionEngine, RegionScanner,
};
use store_api::region_request::{
    RegionBuildIndexRequest, RegionDeleteRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{ScanRequest, TimeSeriesDistribution};
use table::table::scan::RegionScanExec;

use super::*;
use crate::config::{MitoConfig, SeriesIndexFormat as SelectedFormat};
use crate::engine::MitoEngine;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::scan_region::{PredicateGroup, ScanInput};
use crate::read::series_scan::SeriesScan;
use crate::region::MitoRegionRef;
use crate::series_index::SeriesIndexReadContext;
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use crate::test_util::{TestEnv, flush_region};

async fn fixture() -> (TestEnv, MitoEngine, MitoRegionRef) {
    let mut env = TestEnv::with_prefix("prepared-columnar").await;
    let mut metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    metadata
        .column_metadatas
        .iter_mut()
        .find(|c| c.column_id == 2)
        .unwrap()
        .column_schema
        .data_type = ConcreteDataType::float64_datatype();
    let (engine, region) = crate::series_index::prepare_region_with_metadata(
        &mut env,
        &[1000, 1000, 2000],
        MitoConfig {
            experimental_enable_series_index: true,
            experimental_series_index_format: SelectedFormat::Columnar,
            experimental_series_index_maintenance_interval: Duration::from_secs(3600),
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        },
        Arc::new(metadata),
    )
    .await;
    (env, engine, region)
}

fn labels(metadata: store_api::metadata::RegionMetadataRef) -> RawLabelSource {
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from("fixture"),
        physical_metadata: metadata,
        logical_tags: vec![
            BoundTag {
                column_id: 0,
                name: Arc::from("tag_0"),
            },
            BoundTag {
                column_id: 1,
                name: Arc::from("tag_1"),
            },
        ]
        .into(),
    }
}

async fn build(engine: &MitoEngine, region: &MitoRegionRef) {
    engine
        .handle_request(
            region.region_id,
            RegionRequest::BuildIndex(RegionBuildIndexRequest {
                options: Some(build_index_request::Options::SeriesIndex(Default::default())),
            }),
        )
        .await
        .unwrap();
}

async fn write(
    engine: &MitoEngine,
    region: &MitoRegionRef,
    tsid: u64,
    timestamp: i64,
    value: Option<f64>,
) {
    let metadata = region.version().metadata.clone();
    let mut schema = vec![
        ColumnSchema {
            column_name: "__primary_key".into(),
            datatype: ColumnDataType::Binary as i32,
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        },
        ColumnSchema {
            column_name: "ts".into(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
            ..Default::default()
        },
    ];
    let mut values = vec![
        ValueData::BinaryValue(new_sparse_primary_key(&["a", "x"], &metadata, 10, tsid)),
        ValueData::TimestampMillisecondValue(timestamp),
    ];
    let request = if let Some(value) = value {
        schema.push(ColumnSchema {
            column_name: "field_0".into(),
            datatype: ColumnDataType::Float64 as i32,
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        });
        values.push(ValueData::F64Value(value));
        RegionRequest::Put(RegionPutRequest {
            rows: Rows {
                schema,
                rows: vec![row(values)],
            },
            skip_wal: false,
            hint: Some(api::v1::WriteHint {
                primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse as i32,
            }),
            partition_expr_version: None,
        })
    } else {
        RegionRequest::Delete(RegionDeleteRequest {
            rows: Rows {
                schema,
                rows: vec![row(values)],
            },
            hint: Some(api::v1::WriteHint {
                primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse as i32,
            }),
            partition_expr_version: None,
        })
    };
    engine
        .handle_request(region.region_id, request)
        .await
        .unwrap();
}

struct ReadRecorder {
    inner: oio::Reader,
    path: String,
    reads: Arc<Mutex<Vec<String>>>,
}
impl mock::Read for ReadRecorder {
    async fn read(
        &self,
        range: mock::BytesRange,
    ) -> object_store::Result<(mock::RpRead, object_store::Buffer)> {
        self.reads.lock().unwrap().push(self.path.clone());
        self.inner.read(range).await
    }

    async fn open(
        &self,
        range: mock::BytesRange,
    ) -> object_store::Result<(mock::RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.reads.lock().unwrap().push(self.path.clone());
        self.inner.open(range).await
    }
}

fn scanner(region: &MitoRegionRef, store: ObjectStore) -> SeriesScan {
    scanner_with_format(region, store, SelectedFormat::Columnar)
}

fn scanner_with_format(
    region: &MitoRegionRef,
    store: ObjectStore,
    format: SelectedFormat,
) -> SeriesScan {
    let version = region.version();
    let mapper = FlatProjectionMapper::all(&version.metadata).unwrap();
    let input = ScanInput::builder(region.access_layer.clone(), mapper)
        .with_files(
            version
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files())
                .cloned()
                .collect(),
        )
        .with_series_index(Some(SeriesIndexReadContext {
            format,
            store,
            version: region.series_index_version(),
        }))
        .with_predicate(
            PredicateGroup::new(
                &version.metadata,
                &[col("tag_0").eq(lit("a")), col("field_0").gt(lit(0.0))],
            )
            .unwrap(),
        )
        .build();
    SeriesScan::new(input, false)
}

fn rows(chunks: &[RawSeriesChunk]) -> Vec<(Vec<u8>, i64, f64)> {
    chunks
        .iter()
        .flat_map(|chunk| {
            (0..chunk.time.len()).map(move |i| {
                let SampleRef::Float(value) = chunk.fields.get(0).unwrap().sample(i) else {
                    panic!("expected float");
                };
                (
                    chunk.primary_key.to_vec(),
                    chunk.time.timestamp(i).unwrap(),
                    value,
                )
            })
        })
        .collect()
}

#[tokio::test]
async fn prepared_sources_share_discovery_and_read_independently() {
    let (_env, engine, region) = fixture().await;
    // A second series and updates/deletes across SSTs exercise the merge adapter.
    write(&engine, &region, 1, 1500, Some(2.0)).await;
    write(&engine, &region, 0, 1000, Some(3.0)).await;
    write(&engine, &region, 0, 2000, None).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    let reads = Arc::new(Mutex::new(Vec::new()));
    let observed = reads.clone();
    let store = region.series_index_store.clone().unwrap().layer(
        MockLayerBuilder::default()
            .reader_factory(Arc::new(move |path, _, inner| {
                Box::new(ReadRecorder {
                    inner,
                    path: path.to_owned(),
                    reads: observed.clone(),
                })
            }))
            .build()
            .unwrap(),
    );
    let legacy = scanner_with_format(&region, store.clone(), SelectedFormat::Legacy);
    let legacy_binding = labels(legacy.metadata());
    let oracle = legacy
        .scan_series_partition(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            0,
            &legacy_binding,
        )
        .unwrap()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    reads.lock().unwrap().clear();
    let mut scanner = scanner(&region, store);
    let binding = labels(scanner.metadata());
    let ranges = scanner.properties().partitions[0].clone();
    scanner
        .prepare(PrepareRequest::default().with_ranges(vec![ranges, vec![], vec![]]))
        .unwrap();
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let (a, b) = tokio::join!(
        scanner.prepare_series(&ctx, &metrics, &binding),
        scanner.prepare_series(&ctx, &metrics, &binding)
    );
    let source = a.unwrap().unwrap();
    assert!(Arc::ptr_eq(&source, &b.unwrap().unwrap()));
    assert_eq!(2, source.num_series());
    assert!(
        !reads
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.ends_with(".samples"))
    );
    let left = source.read_series(0..2).unwrap();
    let right = source.read_series(0..2).unwrap();
    assert!(
        !reads
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.ends_with(".samples"))
    );
    let (left, right) = tokio::join!(left.try_collect::<Vec<_>>(), right.try_collect::<Vec<_>>());
    let expected = rows(&left.unwrap());
    assert_eq!(rows(&oracle), expected);
    assert_eq!(expected, rows(&right.unwrap()));
    assert_eq!(
        vec![(1000, 3.0), (1500, 2.0)],
        expected
            .iter()
            .map(|(_, t, v)| (*t, *v))
            .collect::<Vec<_>>()
    );
    let before = reads
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.contains("/series/"))
        .count();
    let mut partition_rows = Vec::new();
    for p in 0..3 {
        let chunks = scanner
            .scan_series_partition(&ctx, &metrics, p, &binding)
            .unwrap()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        partition_rows.extend(rows(&chunks));
    }
    assert_eq!(expected, partition_rows);
    assert_eq!(
        before,
        reads
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.contains("/series/"))
            .count()
    );
    let survivor = source.read_series(0..2).unwrap();
    drop(source);
    drop(scanner);
    assert_eq!(
        expected,
        rows(&survivor.try_collect::<Vec<_>>().await.unwrap())
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn native_scan_reports_incomplete_coverage_and_uses_prepared_source() {
    let (_env, engine, region) = fixture().await;
    let request = ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        ..Default::default()
    };
    // Three files are below the automatic index threshold.
    let raw_scanner = engine
        .handle_query(region.region_id, request.clone())
        .await
        .unwrap();
    let binding = labels(raw_scanner.metadata());
    assert!(
        raw_scanner
            .prepare_series(
                &QueryScanContext::default(),
                &ExecutionPlanMetricsSet::new(),
                &binding
            )
            .await
            .is_err()
    );
    build(&engine, &region).await;
    let raw_scanner = engine
        .handle_query(region.region_id, request.clone())
        .await
        .unwrap();
    let binding = labels(raw_scanner.metadata());
    let plan = Arc::new(RegionScanExec::new(raw_scanner, request.clone(), None).unwrap())
        .try_to_series_scan(binding.clone())
        .unwrap()
        .unwrap();
    let frame = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    let chunks = unpack_raw_series(frame, Some(plan.series_schema().clone()))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        vec![1000, 2000],
        rows(&chunks).iter().map(|(_, t, _)| *t).collect::<Vec<_>>()
    );
    write(&engine, &region, 0, 3000, Some(4.0)).await;
    let raw_scanner = engine
        .handle_query(region.region_id, request)
        .await
        .unwrap();
    let binding = labels(raw_scanner.metadata());
    assert!(
        raw_scanner
            .prepare_series(
                &QueryScanContext::default(),
                &ExecutionPlanMetricsSet::new(),
                &binding
            )
            .await
            .err()
            .unwrap()
            .output_msg()
            .contains("memtables")
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn manual_build_covers_unknown_sequences_without_a_compaction_window() {
    use crate::region::options::CompactionOptions;
    use crate::region::version::VersionBuilder;
    use crate::series_index::reconcile_series_indexes_with_mode;
    use crate::series_index::series_index_channel;

    let (_env, engine, region) = fixture().await;
    let original = region.version();
    let mut options = original.options.clone();
    let CompactionOptions::Twcs(twcs) = &mut options.compaction;
    twcs.time_window = None;
    // Keep the original version pinned while replacing handles to the same SSTs.
    let originals: Vec<_> = original
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .map(|f| f.meta_ref().clone())
        .collect();
    let mut unknown = originals.clone();
    for file in &mut unknown {
        file.sequence = None;
    }
    let version = VersionBuilder::from_version(original.clone())
        .options(options)
        .compaction_time_window(None)
        .remove_files(originals.into_iter())
        .add_files(region.file_purger.clone(), unknown.into_iter())
        .build();
    region.version_control.overwrite_current(Arc::new(version));
    let store = region.series_index_store.clone().unwrap();
    let (purger, _receiver) = series_index_channel(store.clone());
    let reconcile = |manual, capacity| {
        reconcile_series_indexes_with_mode(
            0,
            store.clone(),
            region.clone(),
            Duration::from_secs(100),
            0,
            purger.clone(),
            false,
            capacity,
            manual,
        )
    };
    assert_eq!(0, reconcile(false, true).await.unwrap().built_series);
    assert!(
        reconcile(true, false)
            .await
            .unwrap_err()
            .to_string()
            .contains("capacity")
    );
    assert_eq!(1, reconcile(true, true).await.unwrap().built_series);
    assert_eq!(0, reconcile(true, true).await.unwrap().built_series);
    let scanner = scanner(&region, store.clone());
    let binding = labels(scanner.metadata());
    let source = scanner
        .prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &binding,
        )
        .await
        .unwrap()
        .unwrap();
    let chunks = source
        .read_series(0..source.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        vec![1000, 2000],
        rows(&chunks).iter().map(|(_, t, _)| *t).collect::<Vec<_>>()
    );
    // Missing sample objects must fail even when capacity would otherwise make this a no-op.
    let missing = region
        .series_index_version()
        .samples
        .values()
        .next()
        .unwrap()
        .entry()
        .artifact
        .objects[0]
        .path
        .clone();
    store.delete(&missing).await.unwrap();
    assert!(
        reconcile(true, false)
            .await
            .unwrap_err()
            .to_string()
            .contains("capacity")
    );
    engine.stop().await.unwrap();
}
