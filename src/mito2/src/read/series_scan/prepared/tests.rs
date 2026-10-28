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

use std::collections::{BTreeMap, BTreeSet};
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
use crate::read::series_scan::partition_balance;
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

/// Returns one partition's series, in the order its reader presented them, with their row counts.
///
/// The stream contract is checked while the order is still there to check: a series is read in one
/// run of chunks rather than interleaved with another, its timestamps increase across those chunks,
/// and `is_last` marks the final chunk of the series and nothing else. Sorting the rows first, as a
/// series-by-series comparison does, would hide both a reordering and a wrongly closed series.
fn partition_series(chunks: &[RawSeriesChunk]) -> Vec<(Vec<u8>, usize)> {
    let mut runs = Vec::<(Vec<u8>, Vec<&RawSeriesChunk>)>::new();
    for chunk in chunks {
        let key = chunk.primary_key.to_vec();
        match runs.last_mut() {
            Some((last, run)) if *last == key => run.push(chunk),
            _ => runs.push((key, vec![chunk])),
        }
    }
    runs.into_iter()
        .map(|(key, run)| {
            let mut rows = 0;
            let mut previous = None::<i64>;
            for (index, chunk) in run.iter().enumerate() {
                assert_eq!(
                    index + 1 == run.len(),
                    chunk.is_last,
                    "only the final chunk of a series is marked last"
                );
                assert!(!chunk.time.is_empty(), "a chunk carries no samples");
                for sample in 0..chunk.time.len() {
                    let timestamp = chunk
                        .time
                        .timestamp(sample)
                        .expect("a chunk sample carries a timestamp");
                    assert!(
                        previous.is_none_or(|previous| previous < timestamp),
                        "the timestamps of a series must increase"
                    );
                    previous = Some(timestamp);
                }
                rows += chunk.time.len();
            }
            (key, rows)
        })
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
        boundaries: source.boundaries.clone(),
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

/// Partitions the skewed split test divides its series over.
const SKEW_PARTITIONS: usize = 4;
/// Long series in the skewed source, each with [`SKEW_LONG_ROWS`] rows.
const SKEW_LONG_SERIES: usize = 8;
/// Rows in a long series: enough that its rows, and not the per-series overhead the split charges
/// every series, decide where it goes.
const SKEW_LONG_ROWS: usize = 10_000;
/// Short series in the skewed source, each with [`SKEW_SHORT_ROWS`] rows.
const SKEW_SHORT_SERIES: usize = 20;
const SKEW_SHORT_ROWS: usize = 10;

/// Creates an empty region with the columnar format enabled.
///
/// Unlike [`fixture`], no data is written while creating it, so every series' index row count is
/// exactly the rows the skewed split test writes and flushes into one SST.
async fn skew_fixture() -> (TestEnv, MitoEngine, MitoRegionRef) {
    let mut env = TestEnv::with_prefix("prepared-skew").await;
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
        &[],
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

/// Writes every point of one series in a single request.
///
/// One request per series keeps the flushed SST's index row count per series equal to the points
/// written here, which is the cost the split divides.
async fn write_series(
    engine: &MitoEngine,
    region: &MitoRegionRef,
    tag_1: &str,
    tsid: u64,
    points: &[(i64, f64)],
) {
    let metadata = region.version().metadata.clone();
    let schema = vec![
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
        ColumnSchema {
            column_name: "field_0".into(),
            datatype: ColumnDataType::Float64 as i32,
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        },
    ];
    let rows = points
        .iter()
        .map(|(timestamp, value)| {
            row(vec![
                // `tag_0` stays "a" so the fixture predicate keeps every series.
                ValueData::BinaryValue(new_sparse_primary_key(&["a", tag_1], &metadata, 10, tsid)),
                ValueData::TimestampMillisecondValue(*timestamp),
                ValueData::F64Value(*value),
            ])
        })
        .collect();
    engine
        .handle_request(
            region.region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: Rows { schema, rows },
                skip_wal: false,
                hint: Some(api::v1::WriteHint {
                    primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse as i32,
                }),
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
}

/// Groups rows by series so two reads compare series by series: the label set, and every timestamp
/// with its value's bits.
fn by_series(rows: Vec<(Vec<u8>, i64, f64)>) -> BTreeMap<Vec<u8>, Vec<(i64, u64)>> {
    let mut series = BTreeMap::<Vec<u8>, Vec<(i64, u64)>>::new();
    for (key, timestamp, value) in rows {
        series
            .entry(key)
            .or_default()
            .push((timestamp, value.to_bits()));
    }
    for samples in series.values_mut() {
        samples.sort_unstable();
    }
    series
}

/// Returns the `assigned_series` counter of a partition.
///
/// `ScanMetricsSet` exposes the counter only through its verbose `Debug` output, which is what
/// `EXPLAIN VERBOSE` prints and what reports a partition's share of a source.
fn assigned_series(metrics: &PartitionMetrics) -> usize {
    let debug = format!("{metrics:?}");
    match debug.split("\"assigned_series\":").nth(1) {
        Some(rest) => rest
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse()
            .unwrap(),
        // The counter is omitted when it is zero, which is a partition reading no series.
        None => 0,
    }
}

/// The prepared path must divide a skewed source by work rather than by series count, and still
/// read exactly the rows the legacy path reads.
///
/// The source holds [`SKEW_LONG_SERIES`] long series followed by [`SKEW_SHORT_SERIES`] short ones.
/// They sit in that ordinal order because the fixture writes the long ones under the lowest TSIDs,
/// and a series is never divided across partitions, so a contiguous split can only hand the eight
/// long series out whole and in place: this is a *deliberately clustered* worst case for a
/// contiguous split, not a representative one. The count split then takes the shape it balances
/// worst -- every long series on the first partition while the rest read only short ones. Splitting
/// by the index's per-series row counts still improves the heaviest partition from ~3.5x the
/// average to ~2.0x: the count split gives `[70000, 10060, 70, 70]` rows over an average of 20050
/// (3.49x) and the cost split gives `[40000, 40000, 130, 70]` (2.00x). This test pins that
/// reduction, and the rows that survive the new assignment.
///
/// The weight is a series' row count plus the per-series overhead the split charges every series
/// (4096 rows' worth): at [`SKEW_LONG_ROWS`] a long series weighs its own 10000 rows plus that
/// overhead, so its rows decide where it goes, while a short series is mostly its own overhead.
/// The overhead is also why this split concentrates the short tail: the smallest maximum load that
/// packs eight long series into four contiguous runs is four of them, 56384, and thirteen short
/// series (4106 each, 53378) still fit under that bound and land together on the third partition.
#[tokio::test]
async fn work_weighted_split_matches_the_legacy_path_and_balances_partitions() {
    let (_env, engine, region) = skew_fixture().await;
    // Long series take the low TSIDs, so they are the first ordinals; values are distinct exact
    // binary fractions, so a mismatch can only come from the wrong series or row. They stay above
    // zero because the fixture predicate keeps only `field_0 > 0`.
    let value = |series: usize, row: usize| (series * SKEW_LONG_ROWS + row + 1) as f64 / 8.0;
    for series in 0..SKEW_LONG_SERIES {
        let points = (0..SKEW_LONG_ROWS)
            .map(|row| (1000 + row as i64, value(series, row)))
            .collect::<Vec<_>>();
        write_series(
            &engine,
            &region,
            &format!("long-{series:03}"),
            series as u64,
            &points,
        )
        .await;
    }
    for series in 0..SKEW_SHORT_SERIES {
        let points = (0..SKEW_SHORT_ROWS)
            .map(|row| (1000 + row as i64, value(SKEW_LONG_SERIES + series, row)))
            .collect::<Vec<_>>();
        write_series(
            &engine,
            &region,
            &format!("short-{series:03}"),
            100 + series as u64,
            &points,
        )
        .await;
    }
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;

    let total_rows = SKEW_LONG_SERIES * SKEW_LONG_ROWS + SKEW_SHORT_SERIES * SKEW_SHORT_ROWS;
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let store = region.series_index_store.clone().unwrap();
    let projection = (0..region.version().metadata.column_metadatas.len()).collect::<Vec<_>>();

    // The oracle reads the same SST through the legacy series-index path in a single partition.
    let legacy = scanner_with_projection(
        &region,
        store.clone(),
        SelectedFormat::Legacy,
        projection.clone(),
    );
    let legacy_binding = labels(legacy.metadata());
    assert!(
        legacy
            .prepare_series(&ctx, &metrics, &legacy_binding)
            .await
            .unwrap()
            .is_none(),
        "the oracle must read the legacy path"
    );
    let legacy_rows = legacy
        .scan_series_partition(&ctx, &metrics, 0, &legacy_binding)
        .unwrap()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    // The prepared path reads the same ranges, spread over the partitions DataFusion would run.
    let mut scan = scanner_with_projection(&region, store, SelectedFormat::Columnar, projection);
    let mut ranges = vec![Vec::new(); SKEW_PARTITIONS];
    for (index, range) in scan.properties().partitions[0].iter().enumerate() {
        ranges[index % SKEW_PARTITIONS].push(*range);
    }
    scan.prepare(PrepareRequest::default().with_ranges(ranges))
        .unwrap();
    assert_eq!(SKEW_PARTITIONS, scan.properties().num_partitions());
    let binding = labels(scan.metadata());
    // The columnar source, not the ordinary scan, must serve this read: the split below is the
    // prepared path's, and a fallback would silently compare the legacy path with itself.
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the columnar prepared source must serve the weighted split");
    let mut prepared_rows = Vec::new();
    let mut read = Vec::with_capacity(SKEW_PARTITIONS);
    for partition in 0..SKEW_PARTITIONS {
        let stream = scan
            .scan_series_partition(&ctx, &metrics, partition, &binding)
            .unwrap()
            .unwrap();
        assert_eq!(SeriesOrdering::KeyThenTime, stream.ordering());
        let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
        prepared_rows.extend(rows(&chunks));
        read.push(chunks);
    }

    // (a) Same series, timestamps and value bits as the legacy path, series by series.
    let expected = by_series(rows(&legacy_rows));
    let actual = by_series(prepared_rows);
    assert_eq!(total_rows, expected.values().map(Vec::len).sum::<usize>());
    assert_eq!(SKEW_LONG_SERIES + SKEW_SHORT_SERIES, expected.len());
    assert_eq!(expected, actual);

    // (b) The costs the split divides are the rows the index recorded for the written series.
    // Read them off the source that built them: the per-ordinal costs live on the concrete prepared
    // source, so the weights checked here are the ones the split used, not a second estimate.
    let columnar = scan
        .prepared
        .source
        .get()
        .expect("the prepared source must be installed")
        .as_ref()
        .expect("the prepared source must be usable");
    let costs = (0..source.num_series())
        .map(|ordinal| columnar.series_cost(ordinal))
        .collect::<Vec<_>>();
    assert_eq!(SKEW_LONG_SERIES + SKEW_SHORT_SERIES, costs.len());
    let mut written = actual.values().map(Vec::len).collect::<Vec<_>>();
    written.sort_unstable();
    let mut divided = costs.clone();
    divided.sort_unstable();
    assert_eq!(written, divided, "the costs are the per-series row counts");
    assert!(
        costs[..SKEW_LONG_SERIES]
            .iter()
            .all(|c| *c == SKEW_LONG_ROWS)
            && costs[SKEW_LONG_SERIES..]
                .iter()
                .all(|c| *c == SKEW_SHORT_ROWS),
        "the long series are the first ordinals: {costs:?}"
    );
    // Every partition reports the ordinals of the range the split gives it, and every series is
    // assigned to exactly one partition. The split itself is prepared once with the source, so the
    // partitions share it instead of dividing the whole source each.
    let boundaries = scan
        .prepared
        .boundaries()
        .expect("the prepared source publishes its split");
    assert_eq!(
        SKEW_PARTITIONS + 1,
        boundaries.len(),
        "one boundary per partition, plus the end: {boundaries:?}"
    );
    let ranges = (0..SKEW_PARTITIONS)
        .map(|partition| partition_balance::partition_range(&boundaries, partition))
        .collect::<Vec<_>>();
    // (c) Each partition actually reads the series of its own range, in ordinal order, and no two
    // partitions read the same series. The split and the reader that consumes it are separate steps,
    // so only the rows that came out say the two agree: a scan that kept the right plan and handed
    // `read_series` the equal-count range would still pass every check above.
    let mut rows_read = Vec::with_capacity(SKEW_PARTITIONS);
    let mut seen = BTreeSet::new();
    for (partition, range) in ranges.iter().enumerate() {
        let series = partition_series(&read[partition]);
        let expected = range
            .clone()
            .map(|ordinal| {
                let info = source
                    .series_info(ordinal)
                    .expect("every ordinal of the source has an identity");
                (info.primary_key.to_vec(), costs[ordinal])
            })
            .collect::<Vec<_>>();
        assert_eq!(
            expected, series,
            "partition {partition} does not read the series of {range:?} in ordinal order"
        );
        rows_read.push(series.iter().map(|(_, rows)| rows).sum::<usize>());
        for (key, _) in series {
            assert!(
                seen.insert(key),
                "partition {partition} read a series another partition had already read"
            );
        }
    }
    assert_eq!(
        // Eight 10000-row series weigh far more than twenty 10-row ones, so the split gives the
        // first two partitions four long series each and lets the short tail fill the other two.
        vec![40000, 40000, 130, 70],
        rows_read,
        "the partitions do not read the rows the cost split assigned them"
    );
    assert_eq!(
        SKEW_LONG_SERIES + SKEW_SHORT_SERIES,
        seen.len(),
        "not every series was read exactly once"
    );
    // (d) Every partition reports the series count of the range it was given, and the reported
    // counts add up to the whole source: every ordinal is assigned to exactly one partition.
    let mut assigned = Vec::with_capacity(SKEW_PARTITIONS);
    for (partition, range) in ranges.iter().enumerate() {
        let reported = assigned_series(
            &scan
                .metrics_list
                .get(partition)
                .expect("every scanned partition reports metrics"),
        );
        assert_eq!(
            range.len(),
            reported,
            "partition {partition} reports the ordinals of {range:?}"
        );
        assigned.push(reported);
    }
    assert_eq!(costs.len(), assigned.iter().sum::<usize>());

    // (e) The cost split leaves the heaviest partition far closer to the average than the
    // equal-count split, which is exactly what an unweighted split assigns.
    let total_cost = costs.iter().sum::<usize>();
    let average = total_cost as f64 / SKEW_PARTITIONS as f64;
    let weighted = ranges
        .iter()
        .map(|range| costs[range.clone()].iter().sum::<usize>())
        .collect::<Vec<_>>();
    let uniform = vec![1usize; costs.len()];
    let count_boundaries = partition_balance::partition_boundaries(&uniform, SKEW_PARTITIONS);
    let by_count = (0..SKEW_PARTITIONS)
        .map(|partition| {
            let range = partition_balance::partition_range(&count_boundaries, partition);
            costs[range].iter().sum::<usize>()
        })
        .collect::<Vec<_>>();
    assert_eq!(total_cost, weighted.iter().sum::<usize>());
    assert_eq!(total_cost, by_count.iter().sum::<usize>());
    let worst = |costs: &[usize]| *costs.iter().max().unwrap() as f64 / average;
    // The long series cannot be divided, so their row counts bound how even the split can get.
    // These 28 series divide as [40000, 40000, 130, 70] over an average of 20050, while the count
    // split gives [70000, 10060, 70, 70] and leaves the first partition with every long series.
    // The count split stays clearly worse: its heaviest partition is 3.49x the average against the
    // cost split's 2.00x, a ratio of 1.75, so the 1.5x margin asserted below is true with headroom
    // and still fails if the cost split ever stops beating the count split by half again as much.
    assert!(
        worst(&weighted) <= 2.1,
        "the cost split left a partition at {}x the average: {weighted:?}",
        worst(&weighted)
    );
    assert!(
        worst(&by_count) >= 3.0,
        "the equal-count split should starve the short-series partitions: {by_count:?}"
    );
    assert!(
        worst(&by_count) > 1.5 * worst(&weighted),
        "the cost split must be clearly better: {} vs {}",
        worst(&weighted),
        worst(&by_count)
    );
    engine.stop().await.unwrap();
}

/// Partitions the cross-bucket split test divides its series over.
const CROSS_BUCKET_PARTITIONS: usize = 2;
/// Series whose rows all fall in the first bucket.
const CROSS_BUCKET_LONG_SERIES: usize = 1;
/// Series that carry rows in both buckets.
const CROSS_BUCKET_SHARED_SERIES: usize = 7;
/// Rows the series of each kind have in total, spread differently over the two buckets.
const CROSS_BUCKET_ROWS: usize = 1000;
/// Rows in the first bucket for every series that also has rows in the second bucket.
const CROSS_BUCKET_FIRST_ROWS: usize = 1;

/// Event time of the first bucket, and of the second one [`CROSS_BUCKET_BUCKET_WIDTH_SECS`] later.
const CROSS_BUCKET_FIRST_TS: i64 = 1000;
const CROSS_BUCKET_SECOND_TS: i64 = 200_000;
/// The bucket width of [`cross_bucket_fixture`], which separates the two writes.
const CROSS_BUCKET_BUCKET_WIDTH_SECS: u64 = 100;

/// Creates an empty region whose series indexes are built one per 100 second bucket.
///
/// The width is what makes one flush per bucket produce one index artifact per bucket, so a series
/// written to both of them is discovered once per artifact, each reporting only its own rows.
async fn cross_bucket_fixture() -> (TestEnv, MitoEngine, MitoRegionRef) {
    let mut env = TestEnv::with_prefix("prepared-cross-bucket").await;
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
        &[],
        MitoConfig {
            experimental_enable_series_index: true,
            experimental_series_index_format: SelectedFormat::Columnar,
            experimental_series_index_bucket_width: Duration::from_secs(
                CROSS_BUCKET_BUCKET_WIDTH_SECS,
            ),
            experimental_series_index_maintenance_interval: Duration::from_secs(3600),
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        },
        Arc::new(metadata),
    )
    .await;
    (env, engine, region)
}

/// The costs of a prepared source must add up the buckets a series appears in.
///
/// `__series_row_count` is the row count of *one* index artifact, and an artifact covers one time
/// bucket. A query that spans buckets reads the samples of all of them, so the work of a series is
/// the sum of the counts its artifacts report, not the first one discovered: keeping only the first
/// would weigh a series by wherever its rows happen to fall in the bucket order, and the split would
/// then move series between partitions depending on which artifact was read first.
///
/// The source here is the reviewer's counterexample: one series with all of its rows in the first
/// bucket, and seven series with one row in the first bucket and the rest in the second, so every
/// series costs the same 1000 rows in total while their per-bucket counts differ wildly.
#[tokio::test]
async fn series_cost_adds_every_bucket_the_series_appears_in() {
    let (_env, engine, region) = cross_bucket_fixture().await;
    // The fixture predicate keeps only `field_0 > 0`, and every value is a distinct exact binary
    // fraction, so a mismatch can only come from the wrong series or row. The rows of the bucket a
    // series does not appear in are simply not written.
    let value = |series: usize, row: usize| (series * CROSS_BUCKET_ROWS + row + 1) as f64 / 8.0;
    // Bucket one: the single long series with everything, the shared ones with a single row.
    for series in 0..CROSS_BUCKET_LONG_SERIES {
        let points = (0..CROSS_BUCKET_ROWS)
            .map(|row| (CROSS_BUCKET_FIRST_TS + row as i64, value(series, row)))
            .collect::<Vec<_>>();
        write_series(
            &engine,
            &region,
            &format!("series-{series:03}"),
            series as u64,
            &points,
        )
        .await;
    }
    for series in 0..CROSS_BUCKET_SHARED_SERIES {
        let ordinal = CROSS_BUCKET_LONG_SERIES + series;
        // The same labels in both buckets: one series, not two, whatever bucket it is written to.
        write_series(
            &engine,
            &region,
            &format!("series-{ordinal:03}"),
            ordinal as u64,
            &[(
                CROSS_BUCKET_FIRST_TS,
                value(ordinal, CROSS_BUCKET_FIRST_ROWS - 1),
            )],
        )
        .await;
    }
    flush_region(&engine, region.region_id, None).await;
    // Bucket two: only the shared series, each with the rest of its rows.
    for series in 0..CROSS_BUCKET_SHARED_SERIES {
        let ordinal = CROSS_BUCKET_LONG_SERIES + series;
        let points = (CROSS_BUCKET_FIRST_ROWS..CROSS_BUCKET_ROWS)
            .map(|row| (CROSS_BUCKET_SECOND_TS + row as i64, value(ordinal, row)))
            .collect::<Vec<_>>();
        write_series(
            &engine,
            &region,
            &format!("series-{ordinal:03}"),
            ordinal as u64,
            &points,
        )
        .await;
    }
    flush_region(&engine, region.region_id, None).await;
    // One build over both flushes: each bucket of the two SSTs becomes its own artifact.
    build(&engine, &region).await;
    assert_eq!(
        2,
        region.series_index_version().series_indexes.len(),
        "the two buckets must build two index artifacts"
    );

    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let store = region.series_index_store.clone().unwrap();
    let projection = (0..region.version().metadata.column_metadatas.len()).collect::<Vec<_>>();

    // The oracle reads the same SSTs through the legacy series-index path in a single partition.
    let legacy = scanner_with_projection(
        &region,
        store.clone(),
        SelectedFormat::Legacy,
        projection.clone(),
    );
    let legacy_binding = labels(legacy.metadata());
    let legacy_rows = legacy
        .scan_series_partition(&ctx, &metrics, 0, &legacy_binding)
        .unwrap()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    // The prepared path reads the same ranges, spread over the partitions DataFusion would run.
    let mut scan = scanner_with_projection(&region, store, SelectedFormat::Columnar, projection);
    let mut ranges = vec![Vec::new(); CROSS_BUCKET_PARTITIONS];
    for (index, range) in scan.properties().partitions[0].iter().enumerate() {
        ranges[index % CROSS_BUCKET_PARTITIONS].push(*range);
    }
    scan.prepare(PrepareRequest::default().with_ranges(ranges))
        .unwrap();
    assert_eq!(CROSS_BUCKET_PARTITIONS, scan.properties().num_partitions());
    let binding = labels(scan.metadata());
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the columnar prepared source must serve the cross-bucket split");

    // (a) Every series costs the rows of both its buckets, not the rows of the first one found.
    let series = CROSS_BUCKET_LONG_SERIES + CROSS_BUCKET_SHARED_SERIES;
    assert_eq!(series, source.num_series());
    // The per-ordinal costs live on the concrete prepared source, so these are the weights the
    // split was built from rather than a second estimate of them.
    let columnar = scan
        .prepared
        .source
        .get()
        .expect("the prepared source must be installed")
        .as_ref()
        .expect("the prepared source must be usable");
    let costs = (0..source.num_series())
        .map(|ordinal| columnar.series_cost(ordinal))
        .collect::<Vec<_>>();
    assert_eq!(
        vec![CROSS_BUCKET_ROWS; series],
        costs,
        "each series costs the rows of every bucket it appears in"
    );
    // The split of eight equal weights over two partitions is four series each, i.e. 4000 rows each.
    // The first-artifact-only cost would be [1000, 1, 1, 1, 1, 1, 1, 1] or [1000, 999, ...] depending
    // on which artifact was read first, and the former splits into 1000 rows against 7000.
    let boundaries = scan
        .prepared
        .boundaries()
        .expect("the prepared source publishes its split");
    assert_eq!(
        vec![0, series / 2, series],
        boundaries.as_ref(),
        "the split must divide the total cost, not one bucket's"
    );
    let ranges = (0..CROSS_BUCKET_PARTITIONS)
        .map(|partition| partition_balance::partition_range(&boundaries, partition))
        .collect::<Vec<_>>();
    let loads = ranges
        .iter()
        .map(|range| costs[range.clone()].iter().sum::<usize>())
        .collect::<Vec<_>>();
    assert_eq!(
        vec![CROSS_BUCKET_ROWS * series / CROSS_BUCKET_PARTITIONS; CROSS_BUCKET_PARTITIONS],
        loads,
        "both partitions must carry half of the samples: {ranges:?}"
    );

    // (b) The partitions read the rows of the range the split gives them, and together they read
    // exactly what the legacy path reads.
    let mut prepared_rows = Vec::new();
    let mut read = Vec::with_capacity(CROSS_BUCKET_PARTITIONS);
    for partition in 0..CROSS_BUCKET_PARTITIONS {
        let stream = scan
            .scan_series_partition(&ctx, &metrics, partition, &binding)
            .unwrap()
            .unwrap();
        let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
        prepared_rows.extend(rows(&chunks));
        read.push(chunks);
    }
    for (partition, range) in ranges.iter().enumerate() {
        let reported = assigned_series(
            &scan
                .metrics_list
                .get(partition)
                .expect("every scanned partition reports metrics"),
        );
        assert_eq!(
            range.len(),
            reported,
            "partition {partition} reports the ordinals of {range:?}"
        );
    }

    // (c) Each partition reads the series of its own range, with the rows of every bucket it covers,
    // and no two partitions read the same series: the split is checked above, and this is what the
    // reader it was handed actually produced.
    let mut rows_read = Vec::with_capacity(CROSS_BUCKET_PARTITIONS);
    let mut seen = BTreeSet::new();
    for (partition, range) in ranges.iter().enumerate() {
        let series = partition_series(&read[partition]);
        let expected = range
            .clone()
            .map(|ordinal| {
                let info = source
                    .series_info(ordinal)
                    .expect("every ordinal of the source has an identity");
                (info.primary_key.to_vec(), costs[ordinal])
            })
            .collect::<Vec<_>>();
        assert_eq!(
            expected, series,
            "partition {partition} does not read the series of {range:?} in ordinal order"
        );
        rows_read.push(series.iter().map(|(_, rows)| rows).sum::<usize>());
        for (key, _) in series {
            assert!(
                seen.insert(key),
                "partition {partition} read a series another partition had already read"
            );
        }
    }
    assert_eq!(
        vec![CROSS_BUCKET_ROWS * series / CROSS_BUCKET_PARTITIONS; CROSS_BUCKET_PARTITIONS],
        rows_read,
        "the partitions do not read the rows the cost split assigned them"
    );
    assert_eq!(series, seen.len(), "not every series was read exactly once");
    let expected = by_series(rows(&legacy_rows));
    assert_eq!(
        CROSS_BUCKET_ROWS * series,
        expected.values().map(Vec::len).sum::<usize>()
    );
    assert_eq!(series, expected.len());
    assert_eq!(expected, by_series(prepared_rows));

    engine.stop().await.unwrap();
}

/// The cost of a series does not depend on the order its index artifacts are read in.
///
/// Artifacts come from a hash set, so their order is not stable, and a series may be reported by any
/// of them first. Accumulating the counts makes the total the same either way, which is what lets
/// the split be a property of the source rather than of an iteration order.
#[test]
fn series_cost_is_independent_of_the_artifact_order() {
    // The reviewer's counterexample as artifact rows: (table_id, tsid, rows in this artifact).
    let first = [
        (1, 0u64, CROSS_BUCKET_ROWS),
        (1, 1, CROSS_BUCKET_FIRST_ROWS),
        (1, 2, CROSS_BUCKET_FIRST_ROWS),
        (1, 3, CROSS_BUCKET_FIRST_ROWS),
        (1, 4, CROSS_BUCKET_FIRST_ROWS),
        (1, 5, CROSS_BUCKET_FIRST_ROWS),
        (1, 6, CROSS_BUCKET_FIRST_ROWS),
        (1, 7, CROSS_BUCKET_FIRST_ROWS),
    ];
    let second = (1..8)
        .map(|tsid| (1, tsid, CROSS_BUCKET_ROWS - CROSS_BUCKET_FIRST_ROWS))
        .collect::<Vec<_>>();

    // Costs of the source when the artifacts are read in the order given.
    let costs = |order: [&[(u32, u64, usize)]; 2]| {
        let mut candidates = BTreeMap::<MetricSeriesId, Candidate>::new();
        for rows in order {
            for &(table_id, tsid, cost) in rows {
                merge_candidate(
                    &mut candidates,
                    MetricSeriesId { table_id, tsid },
                    vec![tsid as u8],
                    HashMap::new(),
                    cost,
                )
                .unwrap();
            }
        }
        let mut costs = candidates
            .into_values()
            .map(|candidate| candidate.cost)
            .collect::<Vec<_>>();
        costs.sort_unstable();
        costs
    };

    // Both orders describe the same source, so they must produce the same weights and the same
    // assignment.
    let first_then_second = costs([&first, &second]);
    let second_then_first = costs([&second, &first]);
    assert_eq!(
        vec![CROSS_BUCKET_ROWS; CROSS_BUCKET_LONG_SERIES + CROSS_BUCKET_SHARED_SERIES],
        first_then_second
    );
    assert_eq!(first_then_second, second_then_first);
    let boundaries =
        partition_balance::partition_boundaries(&first_then_second, CROSS_BUCKET_PARTITIONS);
    assert_eq!(vec![0, 4, 8], boundaries);
    assert_eq!(
        boundaries,
        partition_balance::partition_boundaries(&second_then_first, CROSS_BUCKET_PARTITIONS),
    );

    // Keeping only the artifact that happened to be read first is what the accumulation fixes, and
    // its assignment depends on that order: the first bucket first weighs the seven short series at
    // one row each and loads one partition with all 1000 rows while the other reads the rest.
    let loads = |costs: &[usize]| {
        let boundaries = partition_balance::partition_boundaries(costs, CROSS_BUCKET_PARTITIONS);
        (0..CROSS_BUCKET_PARTITIONS)
            .map(|partition| {
                let range = partition_balance::partition_range(&boundaries, partition);
                costs[range].iter().sum::<usize>()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        vec![4000, 4000],
        loads(&first_then_second),
        "the accumulated cost splits evenly"
    );
    assert_eq!(
        vec![4000, 4000],
        loads(&second_then_first),
        "and the same way in either order"
    );
}
