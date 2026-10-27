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
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SampleRef;
use object_store::ObjectStore;
use object_store::layers::mock::{self, MockLayerBuilder, oio};
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_engine::{
    BoundTag, PartitionRange, PrepareRequest, QueryScanContext, RegionEngine, RegionScanner,
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
    fixture_with(false).await
}

async fn fixture_with(experimental_series_scan_v2: bool) -> (TestEnv, MitoEngine, MitoRegionRef) {
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
            experimental_series_scan_v2,
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
    reads: Arc<Mutex<Vec<(String, mock::BytesRange)>>>,
}
impl mock::Read for ReadRecorder {
    async fn read(
        &self,
        range: mock::BytesRange,
    ) -> object_store::Result<(mock::RpRead, object_store::Buffer)> {
        self.reads.lock().unwrap().push((self.path.clone(), range));
        self.inner.read(range).await
    }

    async fn open(
        &self,
        range: mock::BytesRange,
    ) -> object_store::Result<(mock::RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.reads.lock().unwrap().push((self.path.clone(), range));
        self.inner.open(range).await
    }
}

/// Fails reads of the selected artifact paths so tests can prove that object-store failures
/// surface as errors instead of being classified as not-ready fallbacks.
struct FailingReader {
    inner: oio::Reader,
    fail: bool,
}
impl mock::Read for FailingReader {
    async fn read(
        &self,
        range: mock::BytesRange,
    ) -> object_store::Result<(mock::RpRead, object_store::Buffer)> {
        if self.fail {
            return Err(injected_failure());
        }
        self.inner.read(range).await
    }

    async fn open(
        &self,
        range: mock::BytesRange,
    ) -> object_store::Result<(mock::RpRead, Box<dyn oio::ReadStreamDyn>)> {
        if self.fail {
            return Err(injected_failure());
        }
        self.inner.open(range).await
    }
}

fn injected_failure() -> mock::Error {
    mock::Error::new(
        mock::ErrorKind::Unexpected,
        "injected series manifest read failure",
    )
}

fn scanner(region: &MitoRegionRef, store: ObjectStore) -> SeriesScan {
    scanner_with_format(region, store, SelectedFormat::Columnar)
}

fn scanner_with_format(
    region: &MitoRegionRef,
    store: ObjectStore,
    format: SelectedFormat,
) -> SeriesScan {
    scanner_with_projection(
        region,
        store,
        format,
        (0..region.version().metadata.column_metadatas.len()).collect(),
    )
}

fn scanner_with_projection(
    region: &MitoRegionRef,
    store: ObjectStore,
    format: SelectedFormat,
    projection: Vec<usize>,
) -> SeriesScan {
    SeriesScan::new(scan_input(region, store, format, projection), false)
}

fn scan_input(
    region: &MitoRegionRef,
    store: ObjectStore,
    format: SelectedFormat,
    projection: Vec<usize>,
) -> ScanInput {
    let version = region.version();
    let mapper = FlatProjectionMapper::new(&version.metadata, projection).unwrap();
    ScanInput::builder(region.access_layer.clone(), mapper)
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
        .build()
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

fn samples(chunks: &[RawSeriesChunk]) -> Vec<(i64, f64)> {
    rows(chunks)
        .into_iter()
        .map(|(_, time, value)| (time, value))
        .collect()
}

/// Reads the raw path of every partition, checks each stream declares `ordering`, and returns the
/// samples ordered by time so a check does not depend on how range grouping spread a memtable.
async fn all_raw_samples(
    scanner: &dyn RegionScanner,
    ctx: &QueryScanContext,
    metrics: &ExecutionPlanMetricsSet,
    binding: &RawLabelSource,
    ordering: SeriesOrdering,
) -> Vec<(i64, f64)> {
    let mut collected = Vec::new();
    for partition in 0..scanner.properties().num_partitions().max(1) {
        let stream = scanner
            .scan_series_partition(ctx, metrics, partition, binding)
            .unwrap()
            .unwrap();
        assert_eq!(ordering, stream.ordering());
        collected.extend(samples(&stream.try_collect::<Vec<_>>().await.unwrap()));
    }
    collected.sort_unstable_by_key(|(time, _)| *time);
    collected
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
            .any(|(p, _)| p.ends_with(".samples"))
    );
    let left = source.read_series(0..2).unwrap();
    let right = source.read_series(0..2).unwrap();
    assert!(
        !reads
            .lock()
            .unwrap()
            .iter()
            .any(|(p, _)| p.ends_with(".samples"))
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
        .filter(|(p, _)| p.contains("/series/"))
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
            .filter(|(p, _)| p.contains("/series/"))
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

#[rstest::rstest]
#[case::legacy(false)]
#[case::two_phase(true)]
#[tokio::test]
async fn native_scan_falls_back_to_raw_sources_until_columnar_coverage(#[case] v2: bool) {
    let (_env, engine, region) = fixture_with(v2).await;
    let request = ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        ..Default::default()
    };
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    // The three flushed SSTs only have columnar artifacts after `build`.
    let fallback_ordering = if v2 {
        SeriesOrdering::Contiguous
    } else {
        SeriesOrdering::KeyThenTime
    };
    let raw_scanner = engine
        .handle_query(region.region_id, request.clone())
        .await
        .unwrap();
    let binding = labels(raw_scanner.metadata());
    assert_eq!(
        fallback_ordering,
        raw_scanner.raw_series_ordering().unwrap()
    );
    assert!(raw_scanner.raw_series_schema(&binding).unwrap().is_some());
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0)],
        all_raw_samples(
            raw_scanner.as_ref(),
            &ctx,
            &metrics,
            &binding,
            fallback_ordering
        )
        .await
    );
    // Known unready coverage selects the ordinary scan instead of a prepared source.
    assert!(
        raw_scanner
            .prepare_series(&ctx, &metrics, &binding)
            .await
            .unwrap()
            .is_none()
    );

    build(&engine, &region).await;
    let mut raw_scanner = engine
        .handle_query(region.region_id, request.clone())
        .await
        .unwrap();
    // Move every assigned range into a new last partition: readiness, the ordering guarantee and
    // the reader must all follow the new assignment, not the pre-prepare partition list.
    let assigned = raw_scanner
        .properties()
        .partitions
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    raw_scanner
        .prepare(PrepareRequest::default().with_ranges(vec![vec![], assigned]))
        .unwrap();
    assert_eq!(2, raw_scanner.properties().num_partitions());
    let binding = labels(raw_scanner.metadata());
    assert_eq!(
        SeriesOrdering::KeyThenTime,
        raw_scanner.raw_series_ordering().unwrap()
    );
    let source = raw_scanner
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(1, source.num_series());
    // The first partition owns no range and the second owns all of them.
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0)],
        all_raw_samples(
            raw_scanner.as_ref(),
            &ctx,
            &metrics,
            &binding,
            SeriesOrdering::KeyThenTime
        )
        .await
    );
    let plan = Arc::new(RegionScanExec::new(raw_scanner, request.clone(), None).unwrap())
        .try_to_series_scan(binding.clone())
        .unwrap()
        .unwrap();
    assert_eq!(SeriesOrdering::KeyThenTime, plan.raw_series_ordering());
    let frame = plan.execute(1, Arc::new(TaskContext::default())).unwrap();
    let unpacked = unpack_raw_series(frame, Some(plan.series_schema().clone())).unwrap();
    // The packed frame declares the same guarantee the plan advertises.
    assert_eq!(plan.raw_series_ordering(), unpacked.ordering());
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0)],
        samples(&unpacked.try_collect::<Vec<_>>().await.unwrap())
    );

    // An assigned memtable is known unready, so the raw path keeps the ordinary scan and must
    // still return the new row.
    write(&engine, &region, 0, 3000, Some(4.0)).await;
    let raw_scanner = engine
        .handle_query(region.region_id, request)
        .await
        .unwrap();
    let binding = labels(raw_scanner.metadata());
    assert_eq!(
        fallback_ordering,
        raw_scanner.raw_series_ordering().unwrap()
    );
    assert!(raw_scanner.raw_series_schema(&binding).unwrap().is_some());
    assert!(
        raw_scanner
            .prepare_series(&ctx, &metrics, &binding)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0), (3000, 4.0)],
        all_raw_samples(
            raw_scanner.as_ref(),
            &ctx,
            &metrics,
            &binding,
            fallback_ordering
        )
        .await
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

#[rstest::rstest]
#[case::field(SemanticType::Field)]
#[case::tag(SemanticType::Tag)]
#[tokio::test]
async fn manual_build_repairs_schema_changes(#[case] semantic_type: SemanticType) {
    use store_api::metadata::ColumnMetadata;
    use store_api::region_request::{AddColumn, AlterKind, RegionAlterRequest};

    use crate::series_index::{reconcile_series_indexes_with_mode, series_index_channel};

    let (_env, engine, region) = fixture().await;
    let store = region.series_index_store.clone().unwrap();
    build(&engine, &region).await;
    let original = region.series_index_version();
    let projection = (0..region.version().metadata.column_metadatas.len()).collect::<Vec<_>>();
    let query = || {
        scanner_with_projection(
            &region,
            store.clone(),
            SelectedFormat::Columnar,
            projection.clone(),
        )
    };
    let scan = query();
    let source = scan
        .prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &labels(scan.metadata()),
        )
        .await
        .unwrap()
        .unwrap();
    let expected = rows(
        &source
            .read_series(0..source.num_series())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
    );
    let data_type = if semantic_type == SemanticType::Field {
        ConcreteDataType::float64_datatype()
    } else {
        ConcreteDataType::string_datatype()
    };
    engine
        .handle_request(
            region.region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::AddColumns {
                    columns: vec![AddColumn {
                        column_metadata: ColumnMetadata {
                            column_schema: datatypes::schema::ColumnSchema::new(
                                "added", data_type, true,
                            ),
                            semantic_type,
                            column_id: 4,
                        },
                        location: None,
                    }],
                },
            }),
        )
        .await
        .unwrap();
    // Even a projection containing only the original fields must reject stale sidecars.
    let scan = query();
    let error = scan
        .prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &labels(scan.metadata()),
        )
        .await
        .err()
        .unwrap();
    assert!(
        error
            .output_msg()
            .contains(if semantic_type == SemanticType::Field {
                "missing scan fields"
            } else {
                "missing scan tags"
            }),
        "{error}"
    );
    let (purger, _receiver) = series_index_channel(store.clone());
    let error = reconcile_series_indexes_with_mode(
        0,
        store.clone(),
        region.clone(),
        Duration::from_secs(100),
        0,
        purger,
        false,
        false,
        true,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("capacity"));
    assert!(Arc::ptr_eq(&original, &region.series_index_version()));

    build(&engine, &region).await;
    let rebuilt = region.series_index_version();
    for (id, old) in &original.samples {
        let new = &rebuilt.samples[id];
        assert_eq!(
            semantic_type == SemanticType::Tag,
            old.entry().artifact.generation == new.entry().artifact.generation
        );
        if semantic_type == SemanticType::Field {
            let reader = ColumnarSamplesFormat::default()
                .reader(store.clone(), new.clone())
                .await
                .unwrap();
            let batches = reader
                .read(crate::series_index::format::SamplesReadRequest {
                    projection: vec!["added".to_string()],
                    ..Default::default()
                })
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert!(!batches.is_empty());
            for batch in batches {
                assert_eq!(batch.num_rows(), batch.column(0).null_count());
            }
        }
    }
    assert_eq!(
        semantic_type == SemanticType::Field,
        original.series_indexes.keys().collect::<HashSet<_>>()
            == rebuilt.series_indexes.keys().collect::<HashSet<_>>()
    );
    let scan = query();
    let source = scan
        .prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &labels(scan.metadata()),
        )
        .await
        .unwrap()
        .unwrap();
    let actual = source
        .read_series(0..source.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(expected, rows(&actual));

    build(&engine, &region).await;
    let unchanged = region.series_index_version();
    assert!(
        rebuilt.series_indexes.keys().collect::<HashSet<_>>()
            == unchanged.series_indexes.keys().collect::<HashSet<_>>()
    );
    for (id, old) in &rebuilt.samples {
        assert_eq!(old.entry(), unchanged.samples[id].entry());
    }
    engine.stop().await.unwrap();
}

struct SamplesReadRecorder {
    inner: Arc<dyn SamplesFormatReader>,
    requests: Arc<Mutex<Vec<SamplesReadRequest>>>,
}

impl SamplesFormatReader for SamplesReadRecorder {
    fn schema(&self) -> datatypes::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn read(
        &self,
        request: SamplesReadRequest,
    ) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
        self.requests.lock().unwrap().push(request.clone());
        self.inner.read(request)
    }
}

#[tokio::test]
async fn ordinal_range_batches_sample_requests_and_pushes_time_bounds() {
    use common_time::{Timestamp, range::TimestampRange};

    let (_env, engine, region) = fixture().await;
    write(&engine, &region, 1, 1500, Some(2.0)).await;
    write(&engine, &region, 2, 1600, Some(3.0)).await;
    write(&engine, &region, 0, 1000, Some(4.0)).await;
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
    let make_scan = |format| {
        let mut input = scan_input(
            &region,
            store.clone(),
            format,
            (0..region.version().metadata.column_metadatas.len()).collect(),
        );
        // Bounds in a finer unit must round up for both inclusive start and exclusive end.
        input.time_range = TimestampRange::new(
            Timestamp::new_microsecond(1_000_001),
            Timestamp::new_microsecond(1_600_001),
        );
        SeriesScan::new(input, false)
    };
    let legacy = make_scan(SelectedFormat::Legacy);
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let oracle = legacy
        .scan_series_partition(&ctx, &metrics, 0, &labels(legacy.metadata()))
        .unwrap()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let scan = make_scan(SelectedFormat::Columnar);
    scan.prepare_series(&ctx, &metrics, &labels(scan.metadata()))
        .await
        .unwrap()
        .unwrap();
    let source = scan.prepared.source.get().unwrap().as_ref().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let source = ColumnarPreparedSource {
        context: source.context.clone(),
        labels: source.labels.clone(),
        raw: source.raw.clone(),
        candidates: source.candidates.clone(),
        metrics: source.metrics.clone(),
        reservation: source.reservation.clone(),
        samples: Arc::new(
            source
                .samples
                .iter()
                .map(|inner| {
                    Arc::new(SamplesReadRecorder {
                        inner: inner.clone(),
                        requests: requests.clone(),
                    }) as Arc<dyn SamplesFormatReader>
                })
                .collect(),
        ),
    };
    let stream = source.read_series(0..source.num_series()).unwrap();
    assert!(requests.lock().unwrap().is_empty());
    let result = stream.try_collect::<Vec<_>>().await.unwrap();
    // SST time-range pruning is inexact; apply the exact residual bounds to the oracle.
    let expected = rows(&oracle)
        .into_iter()
        .filter(|(_, time, _)| (1001..1601).contains(time))
        .collect::<Vec<_>>();
    assert_eq!(expected, rows(&result));
    assert_eq!(
        vec![1500, 1600],
        rows(&result).iter().map(|(_, t, _)| *t).collect::<Vec<_>>()
    );
    {
        let requests = requests.lock().unwrap();
        assert_eq!(source.samples.len(), requests.len());
        let ids = source
            .candidates
            .iter()
            .map(|c| c.id)
            .collect::<BTreeSet<_>>();
        for request in requests.iter() {
            assert_eq!(Some(&ids), request.ids.as_ref());
            assert_eq!(Some(1001), request.start);
            assert_eq!(Some(1601), request.end);
        }
    }
    // Each selected samples part opens its header once, even when it has multiple series.
    let mut headers = HashMap::<String, usize>::new();
    for (path, range) in reads.lock().unwrap().iter() {
        if path.ends_with(".samples") && range.offset() == 0 {
            *headers.entry(path.clone()).or_default() += 1;
        }
    }
    assert!(!headers.is_empty());
    assert!(headers.values().all(|count| *count == 1));
    requests.lock().unwrap().clear();
    assert!(
        source
            .read_series(1..1)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(requests.lock().unwrap().is_empty());
    assert!(source.read_series(0..source.num_series() + 1).is_err());
    let chunks = source
        .read_series(1..3)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows(&result), rows(&chunks));
    assert!(
        requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.ids.as_ref().unwrap().len() == 2)
    );
    engine.stop().await.unwrap();
}

#[rstest::rstest]
#[case(common_time::timestamp::TimeUnit::Second)]
#[case(common_time::timestamp::TimeUnit::Millisecond)]
#[case(common_time::timestamp::TimeUnit::Microsecond)]
#[case(common_time::timestamp::TimeUnit::Nanosecond)]
#[tokio::test]
async fn visibility_uses_named_columns_and_region_time_unit(
    #[case] unit: common_time::timestamp::TimeUnit,
) {
    use common_time::{Timestamp, range::TimestampRange};
    use datatypes::arrow::array::Int64Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use store_api::storage::SequenceRange;

    let (_env, engine, region) = fixture().await;
    let mut metadata = (*region.version().metadata).clone();
    let time_name = metadata.time_index_column().column_schema.name.clone();
    metadata
        .column_metadatas
        .iter_mut()
        .find(|c| c.column_schema.name == time_name)
        .unwrap()
        .column_schema
        .data_type = ConcreteDataType::timestamp_datatype(unit);
    let mapper = FlatProjectionMapper::new(&Arc::new(metadata), vec![]).unwrap();
    let input = ScanInput::builder(region.access_layer.clone(), mapper)
        .with_snapshot_sequence(Some(4))
        .with_sequence_range(SequenceRange::new(Some(1), Some(5)))
        .with_time_range(TimestampRange::new(
            Timestamp::new(-1, unit),
            Timestamp::new(2, unit),
        ))
        .build();
    let context = StreamContext::seq_scan_ctx(input);
    let time_type = DataType::Timestamp(unit.into(), None);
    let values: ArrayRef = Arc::new(Int64Array::from(vec![-2, -1, 0, 1, 2]));
    let time = cast(&values, &time_type).unwrap();
    // Neither internal column is at its normal flat-format offset.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(&time_name, time_type, false),
            Field::new("other", DataType::Int64, false),
            Field::new(SEQUENCE_COLUMN_NAME, DataType::UInt64, false),
        ])),
        vec![
            time,
            values,
            Arc::new(UInt64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )
    .unwrap();
    let filtered = filter_visibility(batch.clone(), &context).unwrap();
    assert_eq!(3, filtered.num_rows());
    assert_eq!(
        &[2, 3, 4],
        filtered
            .column_by_name(SEQUENCE_COLUMN_NAME)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .as_ref()
    );
    assert!(
        filter_visibility(batch.project(&[0, 1]).unwrap(), &context)
            .unwrap_err()
            .to_string()
            .contains("missing sequence")
    );
    assert!(
        filter_visibility(batch.project(&[1, 2]).unwrap(), &context)
            .unwrap_err()
            .to_string()
            .contains("missing time index")
    );
    let wrong_time = RecordBatch::try_from_iter(vec![
        (time_name.as_str(), batch.column(1).clone()),
        (SEQUENCE_COLUMN_NAME, batch.column(2).clone()),
    ])
    .unwrap();
    assert!(
        filter_visibility(wrong_time, &context)
            .unwrap_err()
            .to_string()
            .contains("must be a timestamp")
    );
    let wrong_unit = if unit == common_time::timestamp::TimeUnit::Second {
        common_time::timestamp::TimeUnit::Millisecond
    } else {
        common_time::timestamp::TimeUnit::Second
    };
    let wrong_time = RecordBatch::try_from_iter(vec![
        (
            time_name.as_str(),
            cast(
                batch.column(1),
                &DataType::Timestamp(wrong_unit.into(), None),
            )
            .unwrap(),
        ),
        (SEQUENCE_COLUMN_NAME, batch.column(2).clone()),
    ])
    .unwrap();
    assert!(
        filter_visibility(wrong_time, &context)
            .unwrap_err()
            .to_string()
            .contains("expected")
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn preflight_rejects_unsupported_assignments_without_artifact_io() {
    use crate::read::range::RowGroupIndex;
    use crate::series_index::SeriesIndexVersion;
    use crate::sst::file::FileHandle;
    use crate::sst::file_purger::NoopFilePurger;

    let (_env, engine, region) = fixture().await;
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
    let make_context = || {
        StreamContext::seq_scan_ctx(scan_input(
            &region,
            store.clone(),
            SelectedFormat::Columnar,
            (0..region.version().metadata.column_metadatas.len()).collect(),
        ))
    };
    let mut context = make_context();
    let ranges = context.partition_ranges();
    // Complete columnar coverage is the only ready answer.
    assert!(validate_inputs(&context, &ranges).unwrap());
    context.ranges[ranges[0].identifier].row_group_indices = vec![RowGroupIndex {
        index: context.input.num_files(),
        row_group_index: -1,
    }]
    .into();
    assert!(
        validate_inputs(&context, &ranges)
            .unwrap_err()
            .to_string()
            .contains("extension ranges")
    );

    let mut context = make_context();
    context.ranges[ranges[0].identifier].row_group_indices = vec![RowGroupIndex {
        index: 0,
        row_group_index: i64::MAX,
    }]
    .into();
    assert!(
        validate_inputs(&context, &ranges[..1])
            .unwrap_err()
            .to_string()
            .contains("complete SST")
    );

    let mut context = make_context();
    let mut meta = context.input.files[0].meta_ref().clone();
    meta.region_id = store_api::storage::RegionId::new(123, 456);
    context.input.files[0] = FileHandle::new(meta, Arc::new(NoopFilePurger));
    assert!(
        validate_inputs(&context, &ranges)
            .unwrap_err()
            .to_string()
            .contains("foreign SST")
    );

    // Missing columnar samples are a not-ready answer, not an error.
    let mut context = make_context();
    let index = context.input.series_index.as_mut().unwrap();
    index.version = Arc::new(SeriesIndexVersion {
        series_indexes: index.version.series_indexes.clone(),
        ..Default::default()
    });
    assert!(!validate_inputs(&context, &ranges).unwrap());
    // Invalid assignments remain errors even when sample coverage is incomplete.
    let mut invalid = ranges.clone();
    invalid.push(PartitionRange {
        identifier: ranges[0].identifier + ranges.len(),
        ..ranges[0]
    });
    assert!(
        validate_inputs(&context, &invalid)
            .unwrap_err()
            .to_string()
            .contains("invalid prepared scan range")
    );

    // The public ordering API must propagate malformed scanner assignments, rather than
    // silently describing them as an ordinary scan.
    let mut scan = SeriesScan::new(make_context().input, false);
    scan.prepare(PrepareRequest::default()).unwrap();
    let malformed = PartitionRange {
        identifier: ranges[0].identifier + ranges.len(),
        ..ranges[0]
    };
    // Inject after valid preparation: prepare() also checks scan limits by indexing ranges,
    // whereas this test targets the ordering API's prepared-input validation.
    scan.properties.partitions = vec![vec![malformed]];
    assert!(
        scan.raw_series_ordering()
            .unwrap_err()
            .to_string()
            .contains("invalid prepared scan range")
    );

    let scan = SeriesScan::new(context.input, false);
    let binding = labels(scan.metadata());
    // The raw path stays available for the ordinary scan and preparation is skipped.
    assert!(scan.raw_series_schema(&binding).unwrap().is_some());
    assert!(
        scan.prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &binding
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(reads.lock().unwrap().is_empty());
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn series_artifact_read_failure_propagates() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    // The published catalog is complete; the injected manifest read failure must still propagate.
    let store = region.series_index_store.clone().unwrap().layer(
        MockLayerBuilder::default()
            .reader_factory(Arc::new(|path, _, inner| {
                Box::new(FailingReader {
                    inner,
                    fail: path.contains("/series/") && path.ends_with("/manifest.json"),
                })
            }))
            .build()
            .unwrap(),
    );
    let context = StreamContext::seq_scan_ctx(scan_input(
        &region,
        store,
        SelectedFormat::Columnar,
        (0..region.version().metadata.column_metadatas.len()).collect(),
    ));
    let ranges = context.partition_ranges();
    assert!(validate_inputs(&context, &ranges).unwrap());
    let scan = SeriesScan::new(context.input, false);
    let binding = labels(scan.metadata());
    assert!(scan.raw_series_schema(&binding).unwrap().is_some());
    let error = scan
        .prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &binding,
        )
        .await
        .err()
        .expect("a failed artifact read must not report a not-ready source");
    assert!(error.output_msg().contains("injected"), "{error}");
    engine.stop().await.unwrap();
}

#[test]
fn series_pushdown_preserves_predicate_semantics() {
    use datafusion_expr::{Expr, Operator};
    use datatypes::arrow::datatypes::{DataType, Field};
    let schema = Arc::new(Schema::new(vec![
        Field::new(DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DataType::UInt32, false),
        Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
        Field::new(ROW_COUNT_COLUMN, DataType::UInt64, false),
        Field::new("tag", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![10, 10, 20, 20])),
            Arc::new(UInt64Array::from(vec![1, 2, 1, 2])),
            Arc::new(UInt64Array::from(vec![1, 1, 1, 1])),
            Arc::new(StringArray::from(vec![
                None,
                Some(""),
                Some("alpha"),
                Some("beta"),
            ])),
        ],
    )
    .unwrap();
    let table = col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME);
    let cases = vec![
        table.clone().eq(lit(10u32)).and(col("tag").is_null()),
        lit(20u32).eq(table.clone()).and(col("tag").not_eq(lit(""))),
        col("tag").is_null().or(col("tag").eq(lit(""))),
        datafusion_expr::binary_expr(col("tag"), Operator::RegexMatch, lit("^a.*")),
        datafusion_expr::cast(col("tag"), DataType::Utf8).eq(lit("alpha")),
        table
            .clone()
            .eq(lit(10u32))
            .and(table.clone().eq(lit(20u32))),
    ];
    for expr in cases {
        let expected = common_recordbatch::filter::batch_filter(
            &batch,
            &Predicate::to_physical_expr(&expr, &schema).unwrap(),
        )
        .unwrap();
        let request = series_read_request(&schema, Some(&Predicate::new(vec![expr])));
        assert!(
            !request
                .projection
                .iter()
                .any(|name| name == ROW_COUNT_COLUMN)
        );
        let mut actual = batch.clone();
        for filter in request.filters {
            let mask = filter
                .evaluate_array(actual.column_by_name(filter.column_name()).unwrap())
                .unwrap();
            actual = filter_record_batch(&actual, &BooleanArray::new(mask, None)).unwrap();
        }
        assert_eq!(expected, actual);
    }
    // A field-dependent OR cannot contribute either branch as an index restriction.
    let mixed = table.clone().eq(lit(10u32)).or(col("field_0").gt(lit(0.0)));
    let request = series_read_request(&schema, Some(&Predicate::new(vec![mixed])));
    assert!(request.filters.is_empty());
    assert_eq!(None, request.table_id);
    let unsupported = Expr::Not(Box::new(col("tag").eq(lit("alpha"))));
    assert!(
        series_read_request(&schema, Some(&Predicate::new(vec![unsupported])))
            .filters
            .is_empty()
    );
    let request = series_read_request(&schema, Some(&Predicate::new(vec![lit(20u32).eq(table)])));
    assert_eq!(Some(20), request.table_id);
}

#[tokio::test]
async fn series_pushdown_skips_unrelated_table_payloads() {
    let (_env, engine, region) = fixture().await;
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
    let version = region.series_index_version();
    let handle = version.series_indexes.values().next().unwrap().clone();
    let reader = ColumnarSeriesFormat::default()
        .reader(store, handle)
        .await
        .unwrap();
    let schema = reader.schema();
    reads.lock().unwrap().clear();
    let unprojected = reader
        .read(SeriesReadRequest {
            projection: schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
            ..Default::default()
        })
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let reads_with_count = reads.lock().unwrap().len();
    reads.lock().unwrap().clear();
    let all = reader
        .read(series_read_request(&schema, None))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(all.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
    let full_reads = reads.lock().unwrap().len();
    assert!(full_reads < reads_with_count);
    assert_eq!(
        all.iter().map(RecordBatch::num_rows).sum::<usize>(),
        unprojected.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    reads.lock().unwrap().clear();
    let request = series_read_request(
        &schema,
        Some(&Predicate::new(vec![
            col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME).eq(lit(u32::MAX)),
        ])),
    );
    let selected = reader
        .read(request)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(selected.is_empty());
    assert!(reads.lock().unwrap().len() < full_reads);
    engine.stop().await.unwrap();
}
