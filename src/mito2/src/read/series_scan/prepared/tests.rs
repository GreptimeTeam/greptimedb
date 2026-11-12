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
use common_query::series_frame::{pack_raw_series, unpack_raw_series};
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
    BoundTag, PartitionRange, PrepareRequest, PreparedSeriesSource, QueryScanContext, RegionEngine,
    RegionScanner,
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
use crate::series_index::columnar::ColumnarSeriesFormat;
use crate::series_index::format::{SamplesFormat, SeriesIndexFormat};
use crate::series_index::samples::ColumnarSamplesFormat;
use crate::series_index::{SeriesIndexReadContext, SeriesIndexVersion};
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use crate::test_util::{TestEnv, flush_region};

async fn fixture() -> (TestEnv, MitoEngine, MitoRegionRef) {
    fixture_with(false).await
}

/// A region whose series index stays in the legacy artifact format, so the columnar gate is off
/// even after a build published coverage.
async fn legacy_fixture() -> (TestEnv, MitoEngine, MitoRegionRef) {
    fixture_with_format(SelectedFormat::Legacy, false).await
}

async fn fixture_with(experimental_series_scan_v2: bool) -> (TestEnv, MitoEngine, MitoRegionRef) {
    fixture_with_format(SelectedFormat::Columnar, experimental_series_scan_v2).await
}

async fn fixture_with_format(
    format: SelectedFormat,
    experimental_series_scan_v2: bool,
) -> (TestEnv, MitoEngine, MitoRegionRef) {
    fixture_with_timestamps(format, experimental_series_scan_v2, &[1000, 1000, 2000]).await
}

/// A columnar fixture whose flushed rows carry the given timestamps.
///
/// Distinct timestamps keep a frame read valid without dedup, which a test that reads the prepared
/// source in append mode needs.
async fn fixture_with_timestamps(
    format: SelectedFormat,
    experimental_series_scan_v2: bool,
    timestamps: &[i64],
) -> (TestEnv, MitoEngine, MitoRegionRef) {
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
        timestamps,
        MitoConfig {
            experimental_enable_series_index: true,
            experimental_series_index_format: format,
            experimental_series_scan_v2,
            experimental_series_index_maintenance_interval: Duration::from_secs(3600),
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        },
        Arc::new(metadata),
    )
    .await;
    // Each fixture flush also triggers its own background series-index build, so the region would
    // already publish coverage here. Drain those triggered builds and drop the published snapshot:
    // every test below starts from the uncovered region it had before the flush trigger existed and
    // decides for itself when (and over which SSTs) coverage exists.
    crate::series_index::tests::drain_series_index_builds(&engine, &region).await;
    crate::series_index::tests::reset_published_series_index(&region);
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
    write_with_tags(engine, region, tsid, timestamp, value, &["a", "x"]).await;
}

async fn write_with_tags(
    engine: &MitoEngine,
    region: &MitoRegionRef,
    tsid: u64,
    timestamp: i64,
    value: Option<f64>,
    tags: &[&str],
) {
    let metadata = region.version().metadata.clone();
    write_primary_key(
        engine,
        region,
        new_sparse_primary_key(tags, &metadata, 10, tsid),
        timestamp,
        value,
    )
    .await;
}

/// Writes one row with an explicit sparse key, so a test can force two distinct
/// series to share one tsid.
async fn write_primary_key(
    engine: &MitoEngine,
    region: &MitoRegionRef,
    primary_key: Vec<u8>,
    timestamp: i64,
    value: Option<f64>,
) {
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
        ValueData::BinaryValue(primary_key),
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
    scan_input_with_append(region, store, format, projection, false)
}

fn scan_input_with_append(
    region: &MitoRegionRef,
    store: ObjectStore,
    format: SelectedFormat,
    projection: Vec<usize>,
    append_mode: bool,
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
            cache_namespace: store_api::storage::FileId::random(),
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
        .with_append_mode(append_mode)
        .build()
}

/// A series scan over the region's live state, so the assignment includes its memtables.
async fn region_scan(region: &MitoRegionRef) -> SeriesScan {
    let scan_region = crate::read::scan_region::ScanRegion::new(
        region.version_control.current().version,
        region.access_layer.clone(),
        per_series_request(),
        crate::cache::CacheStrategy::Disabled,
    )
    .with_series_index(Some(SeriesIndexReadContext {
        cache_namespace: store_api::storage::FileId::random(),
        format: SelectedFormat::Columnar,
        store: region.series_index_store.clone().unwrap(),
        version: region.series_index_version(),
    }));
    let mut scan = scan_region.series_scan().await.unwrap();
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    scan
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
                    {
                        let key = if chunk.metric.is_some() {
                            &chunk.primary_key.as_slice()[8..]
                        } else {
                            chunk.primary_key.as_slice()
                        };
                        let (table, tsid) =
                            mito_codec::row_converter::SparsePrimaryKeyCodec::schemaless()
                                .decode_ids(key)
                                .unwrap();
                        [
                            table.to_be_bytes().as_slice(),
                            tsid.to_be_bytes().as_slice(),
                        ]
                        .concat()
                    },
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

/// Reads the raw path of every partition, checks each stream declares `ordering`, and returns its
/// `(key, time, value)` triples ordered by time so a check does not depend on how range grouping
/// spread a source.
async fn all_raw_rows(
    scanner: &dyn RegionScanner,
    ctx: &QueryScanContext,
    metrics: &ExecutionPlanMetricsSet,
    binding: &RawLabelSource,
    ordering: SeriesOrdering,
) -> Vec<(Vec<u8>, i64, f64)> {
    let mut collected = Vec::new();
    for partition in 0..scanner.properties().num_partitions().max(1) {
        let stream = scanner
            .scan_series_partition(ctx, metrics, partition, binding)
            .unwrap()
            .unwrap();
        assert_eq!(ordering, stream.ordering());
        collected.extend(rows(&stream.try_collect::<Vec<_>>().await.unwrap()));
    }
    collected.sort_unstable_by_key(|(_, time, _)| *time);
    collected
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
    all_raw_rows(scanner, ctx, metrics, binding, ordering)
        .await
        .into_iter()
        .map(|(_, time, value)| (time, value))
        .collect()
}

/// The `(table id, tsid)` label key of a series, encoded like the tail of a sparse primary key.
fn series_key(table_id: u32, tsid: u64) -> Vec<u8> {
    [
        table_id.to_be_bytes().as_slice(),
        tsid.to_be_bytes().as_slice(),
    ]
    .concat()
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
    let prepared = scanner.prepared.source.get().unwrap().as_ref().unwrap();
    let merge_schema = prepared.sample_schema.as_ref().unwrap();
    assert!(merge_schema.index_of("tag_0").is_err());
    assert!(
        merge_schema
            .index_of(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .is_err()
    );
    assert!(merge_schema.index_of(DATA_SCHEMA_TSID_COLUMN_NAME).is_err());
    assert_eq!(1, prepared.raw.predicates.len()); // Only the field predicate remains.

    assert!(
        !reads
            .lock()
            .unwrap()
            .iter()
            .any(|(p, _)| p.ends_with(".samples"))
    );
    let left = source.read_series(0..2).unwrap();
    assert!(left.label_decode_spec().is_none());
    assert!(same_labels(left.label_source(), &binding));
    for ordinal in 0..source.num_series() {
        let info = source.series_info(ordinal).unwrap();
        assert_eq!(info.primary_key, prepared.candidates[ordinal].key);
        assert_eq!(info.metric, prepared.candidates[ordinal].metric);
    }
    let right = source.read_series(0..2).unwrap();
    assert!(
        !reads
            .lock()
            .unwrap()
            .iter()
            .any(|(p, _)| p.ends_with(".samples"))
    );
    let (left, right) = tokio::join!(left.try_collect::<Vec<_>>(), right.try_collect::<Vec<_>>());
    let left = left.unwrap();
    let mut decoder = mito_codec::labels::PrimaryKeyLabelDecoder::from_decode_spec(
        &common_query::series_frame::label_decode_spec(&legacy_binding).unwrap(),
    )
    .unwrap();
    for (actual, expected) in left.iter().zip(&oracle) {
        assert_eq!(
            actual.metric.as_ref().unwrap(),
            &decoder.decode(expected.primary_key.as_slice()).unwrap()
        );
    }
    let expected = rows(&left);
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

#[tokio::test]
async fn two_phase_raw_scan_preserves_key_then_time_order() {
    let (_env, engine, region) = fixture_with(true).await;
    for round in 0..3 {
        for tsid in 0..4 {
            for offset in 0..3 {
                write(
                    &engine,
                    &region,
                    tsid,
                    round * 10_000 + offset * 1_000,
                    Some((tsid + 1) as f64),
                )
                .await;
            }
        }
        flush_region(&engine, region.region_id, None).await;
    }
    // A single series long enough to span several sample batches must still come back as one
    // ordered stream.
    for offset in 0..4_100 {
        write(&engine, &region, 0, 50_000 + offset, Some(1.0)).await;
    }
    flush_region(&engine, region.region_id, None).await;
    // The flush triggers an automatic build. Drain it and drop the published coverage so the
    // scan below starts from a gated region with nothing ready.
    crate::series_index::tests::drain_series_index_builds(&engine, &region).await;
    crate::series_index::tests::reset_published_series_index(&region);

    let make_scanner = || {
        SeriesScan::new(
            scan_input(
                &region,
                region.series_index_store.clone().unwrap(),
                SelectedFormat::Columnar,
                (0..region.version().metadata.column_metadatas.len()).collect(),
            ),
            true,
        )
    };
    // Moves every assigned range into two partitions so the per-series split is observable.
    let two_partitions = |mut scanner: SeriesScan| {
        assert_eq!("two_phase", scanner.mode.as_str());
        let ranges = scanner
            .properties()
            .partitions
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        assert!(
            ranges.len() >= 2,
            "fixture must produce multiple candidate assignment ranges"
        );
        scanner
            .prepare(PrepareRequest::default().with_ranges(vec![
                ranges.iter().step_by(2).copied().collect(),
                ranges.iter().skip(1).step_by(2).copied().collect(),
            ]))
            .unwrap();
        assert_eq!(2, scanner.properties().num_partitions());
        scanner
    };
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();

    // Nothing is ready yet: every partition answers with an empty native stream, never the
    // ordinary scan, and the empty answer declares the planned schema and ordering.
    let scanner = two_partitions(make_scanner());
    let binding = labels(scanner.metadata());
    assert_eq!(
        SeriesOrdering::KeyThenTime,
        scanner.raw_series_ordering().unwrap()
    );
    let declared = scanner.raw_series_schema(&binding).unwrap().unwrap();
    assert!(
        scanner.raw_series_has_explicit_labels(&binding).unwrap(),
        "a gated region plans explicit labels in both readiness states"
    );
    let empty = scanner
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("a gated region prepares an empty source instead of declining");
    assert_eq!(0, empty.num_series());
    for partition in 0..2 {
        let stream = scanner
            .scan_series_partition(&ctx, &metrics, partition, &binding)
            .unwrap()
            .unwrap();
        assert_eq!(SeriesOrdering::KeyThenTime, stream.ordering());
        assert_eq!(declared.fields, stream.schema().fields);
        assert!(same_labels(stream.label_source(), &binding));
        assert!(
            stream.try_collect::<Vec<_>>().await.unwrap().is_empty(),
            "an unbuilt region must not serve ordinary scan rows"
        );
    }

    // After publication the same scan returns exactly the native rows, still in key-then-time
    // order and still never splitting one series across partitions.
    build(&engine, &region).await;
    let scanner = two_partitions(make_scanner());
    let binding = labels(scanner.metadata());
    assert!(
        scanner.raw_series_has_explicit_labels(&binding).unwrap(),
        "publication must not change the planned label representation"
    );
    let mut owners = std::collections::HashMap::new();
    let mut total = 0;
    for partition in 0..2 {
        let stream = scanner
            .scan_series_partition(&ctx, &metrics, partition, &binding)
            .unwrap()
            .unwrap();
        assert_eq!(SeriesOrdering::KeyThenTime, stream.ordering());
        assert_eq!(declared.fields, stream.schema().fields);
        let mut previous_key: Option<Vec<u8>> = None;
        let mut previous_time = None;
        for chunk in stream.try_collect::<Vec<_>>().await.unwrap() {
            let key = chunk.primary_key.to_vec();
            if previous_key.as_ref().is_some_and(|prev| prev != &key) {
                assert!(previous_key.as_ref().unwrap() < &key);
                previous_time = None;
            }
            if let Some(time) = previous_time {
                assert!(chunk.time.timestamp(0).unwrap() > time);
            }
            for i in 0..chunk.time.len() {
                let time = chunk.time.timestamp(i).unwrap();
                if i > 0 {
                    assert!(time > chunk.time.timestamp(i - 1).unwrap());
                }
                previous_time = Some(time);
            }
            total += chunk.time.len();
            assert!(
                owners
                    .insert(key.clone(), partition)
                    .is_none_or(|owner| owner == partition)
            );
            previous_key = Some(key);
        }
    }
    assert_eq!(4, owners.len());
    // Three rounds of three offsets for four series, plus the long single-series tail.
    assert_eq!(4 * 3 * 3 + 4_100, total);
    engine.stop().await.unwrap();
}

#[rstest::rstest]
#[case::legacy(false)]
#[case::two_phase(true)]
#[tokio::test]
async fn native_scan_serves_nothing_until_columnar_coverage(#[case] v2: bool) {
    let (_env, engine, region) = fixture_with(v2).await;
    let request = ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        ..Default::default()
    };
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    // The three flushed SSTs only have columnar artifacts after `build`.
    let ordering = SeriesOrdering::KeyThenTime;
    let raw_scanner = engine
        .handle_query(region.region_id, request.clone())
        .await
        .unwrap();
    let binding = labels(raw_scanner.metadata());
    assert_eq!(ordering, raw_scanner.raw_series_ordering().unwrap());
    assert!(raw_scanner.raw_series_schema(&binding).unwrap().is_some());
    // Nothing ready: an empty native answer, never the ordinary scan rows of those SSTs.
    assert_eq!(
        Vec::<(i64, f64)>::new(),
        all_raw_samples(raw_scanner.as_ref(), &ctx, &metrics, &binding, ordering).await
    );
    // Known unready coverage prepares an empty source instead of declining the scan.
    let unready = raw_scanner
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("a gated region prepares an empty source instead of declining");
    assert_eq!(0, unready.num_series());

    build(&engine, &region).await;
    // A partial-day query still uses grouped samples when other sources were time-pruned.
    let mut bounded_request = request.clone();
    bounded_request.filters.push(col("ts").gt_eq(lit(
        datafusion_common::ScalarValue::TimestampMillisecond(Some(1500), None),
    )));
    let bounded = engine
        .handle_query(region.region_id, bounded_request)
        .await
        .unwrap();
    let bounded_labels = labels(bounded.metadata());
    let prepared = bounded
        .prepare_series(&ctx, &metrics, &bounded_labels)
        .await
        .unwrap()
        .expect("time-pruned daily sources remain prepared");
    let chunks = prepared
        .read_series(0..prepared.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(samples(&chunks), vec![(2000, 1.0)]);

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

    // An assigned memtable is unready, so both entries serve the ready SST rows and skip the new
    // row instead of disappearing as a whole: with the columnar gate on, the raw entry no longer
    // falls back to the ordinary scan just because a memtable is assigned.
    write(&engine, &region, 0, 3000, Some(4.0)).await;
    let raw_scanner = engine
        .handle_query(region.region_id, request)
        .await
        .unwrap();
    let binding = labels(raw_scanner.metadata());
    assert_eq!(ordering, raw_scanner.raw_series_ordering().unwrap());
    assert!(raw_scanner.raw_series_schema(&binding).unwrap().is_some());
    let native = raw_scanner
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("an assigned memtable must not blank the ready native sources");
    let native = native
        .read_series(0..native.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(samples(&native), vec![(1000, 1.0), (2000, 1.0)]);
    // The raw entry returns exactly the ready subset, and never the memtable row.
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0)],
        all_raw_samples(raw_scanner.as_ref(), &ctx, &metrics, &binding, ordering).await
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
    for old in original.samples.values() {
        let new = rebuilt
            .samples
            .values()
            .find(|new| {
                new.entry().window == old.entry().window
                    && new.entry().source_file_ids == old.entry().source_file_ids
            })
            .unwrap();
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
    ) -> Result<
        futures::stream::BoxStream<'static, Result<crate::series_index::format::SamplesBatch>>,
    > {
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
        selection: source.selection.clone(),
        labels: source.labels.clone(),
        raw: source.raw.clone(),
        sample_schema: source.sample_schema.clone(),
        candidates: source.candidates.clone(),
        pruner: source.pruner.clone(),
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
        // Selection is exact and key-only: the reader derives its coarse bounds from it.
        let keys = source
            .candidates
            .iter()
            .map(|c| c.merge_key.to_vec())
            .collect::<BTreeSet<_>>();
        for request in requests.iter() {
            let requested = request
                .keys
                .as_ref()
                .unwrap()
                .iter()
                .map(|key| key.bytes().to_vec())
                .collect::<BTreeSet<_>>();
            assert_eq!(keys, requested);
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
            .all(|r| r.keys.as_ref().unwrap().len() == 2)
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

    // A daily artifact cannot supply just one source from its grouped coverage.
    let mut partial = make_context();
    partial.ranges[ranges[0].identifier].row_group_indices = vec![RowGroupIndex {
        index: 0,
        row_group_index: -1,
    }]
    .into();
    assert!(!validate_inputs(&partial, &ranges[..1]).unwrap());

    // A catalog can outlive a source generation until reconciliation runs.
    let mut stale = make_context();
    let index = stale.input.series_index.as_mut().unwrap();
    let (purger, _purges) = crate::series_index::series_index_channel(store.clone());
    let samples = index
        .version
        .samples
        .iter()
        .map(|(id, handle)| {
            let mut entry = handle.entry().clone();
            entry.source_file_ids.push(FileId::random());
            (
                *id,
                crate::series_index::artifact::SamplesFileHandle::new(
                    handle.file_id(),
                    entry,
                    purger.clone(),
                ),
            )
        })
        .collect();
    index.version = Arc::new(SeriesIndexVersion {
        samples,
        series_indexes: index.version.series_indexes.clone(),
        ..Default::default()
    });
    assert!(!validate_inputs(&stale, &ranges).unwrap());

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

    // Ordering is independent of reader selection, but raw preflight and execution must still
    // reject the same malformed assignments rather than falling back to the ordinary scan.
    let mut scan = SeriesScan::new(make_context().input, false);
    scan.prepare(PrepareRequest::default()).unwrap();
    let malformed = PartitionRange {
        identifier: ranges[0].identifier + ranges.len(),
        ..ranges[0]
    };
    // Inject after valid preparation: prepare() also checks scan limits by indexing ranges.
    scan.properties.partitions = vec![vec![malformed]];
    let binding = labels(scan.metadata());
    assert_eq!(
        SeriesOrdering::KeyThenTime,
        scan.raw_series_ordering().unwrap()
    );
    assert!(
        scan.raw_series_schema(&binding)
            .unwrap_err()
            .to_string()
            .contains("invalid prepared scan range")
    );
    assert!(
        scan.scan_series_partition(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            0,
            &binding,
        )
        .err()
        .unwrap()
        .to_string()
        .contains("invalid prepared scan range")
    );

    let scan = SeriesScan::new(context.input, false);
    let binding = labels(scan.metadata());
    // The raw path stays available and preparation answers with an empty source: a gated region
    // never declines the native read, it only narrows it.
    assert!(scan.raw_series_schema(&binding).unwrap().is_some());
    assert_eq!(
        0,
        scan.prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &binding
        )
        .await
        .unwrap()
        .expect("the gate is on, so preparation never declines")
        .num_series()
    );
    // Nothing is ready, so no artifact may be opened.
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

#[tokio::test]
async fn metadata_cache_reuses_structures_and_isolates_storage() {
    use crate::cache::{CacheManager, CacheStrategy};
    use crate::series_index::columnar::reader::ColumnarReader;
    use crate::series_index::samples::reader::SamplesReader;
    use store_api::storage::FileId;

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
    let series = version.series_indexes.values().next().unwrap();
    let samples = version.samples.values().next().unwrap();
    let strategy = CacheStrategy::EnableAll(Arc::new(
        CacheManager::builder()
            .sst_meta_cache_size(8 * 1024 * 1024)
            .build(),
    ));
    let namespace = FileId::random();
    let contexts = [
        strategy.columnar_metadata(namespace),
        strategy.columnar_metadata(namespace),
        strategy.columnar_metadata(FileId::random()),
        None,
    ];
    let mut results = Vec::new();
    let mut counts = Vec::new();
    for context in contexts {
        reads.lock().unwrap().clear();
        let reader = ColumnarReader::open_cached(store.clone(), series.clone(), context.clone())
            .await
            .unwrap();
        let request = SeriesReadRequest {
            projection: reader
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
            ..Default::default()
        };
        let series_rows = reader
            .read(request)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let reader = SamplesReader::open_cached(store.clone(), samples.clone(), context)
            .await
            .unwrap();
        // The exact key is returned with each batch, so it is never a projected column.
        let request = SamplesReadRequest {
            projection: reader
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .filter(|name| name != store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME)
                .collect(),
            ..Default::default()
        };
        let sample_rows = reader
            .read(request)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        results.push((series_rows, sample_rows));
        counts.push(reads.lock().unwrap().len());
    }
    assert!(results.windows(2).all(|pair| pair[0] == pair[1]));
    assert!(
        counts[1] < counts[0],
        "warm metadata should avoid reads: {counts:?}"
    );
    assert_eq!(counts[0], counts[2], "a new storage namespace must miss");
    assert_eq!(counts[0], counts[3], "disabled caches must read metadata");
    engine.stop().await.unwrap();
}

/// Local diagnostic benchmark, intentionally excluded from routine CI. No timing threshold:
/// results depend on the machine, build profile, and filesystem page cache.
#[tokio::test]
#[ignore = "manual native preparation cold/warm measurement"]
#[allow(clippy::print_stdout)] // This manual benchmark reports measurements to its caller.
async fn native_preparation_metadata_benchmark() {
    use crate::cache::{CacheManager, CacheStrategy};
    use std::time::Instant;
    use store_api::metadata::RegionMetadataBuilder;
    use store_api::storage::FileId;

    // Keep wide-artifact setup small: this measures preparation, not sample writing.
    for (series_count, field_count) in [(4096, 1), (64, 1024)] {
        let tag_count = 78;
        let mut env = TestEnv::with_prefix("native-metadata-bench").await;
        let mut metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
        metadata
            .column_metadatas
            .iter_mut()
            .find(|column| column.column_id == 2)
            .unwrap()
            .column_schema
            .data_type = ConcreteDataType::float64_datatype();
        for index in 2..tag_count {
            let mut column = metadata
                .column_metadatas
                .iter()
                .find(|column| column.column_id == 0)
                .unwrap()
                .clone();
            column.column_id = 1000 + index;
            column.column_schema.name = format!("tag_{index:04}");
            metadata.primary_key.push(column.column_id);
            metadata.column_metadatas.push(column);
        }
        let field = metadata
            .column_metadatas
            .iter()
            .find(|column| column.column_id == 2)
            .unwrap()
            .clone();
        for index in 1..field_count {
            let mut column = field.clone();
            column.column_id = 10000 + index;
            column.column_schema.name = format!("field_{index}");
            metadata.column_metadatas.push(column);
        }
        let metadata = Arc::new(
            RegionMetadataBuilder::from_existing(metadata)
                .build()
                .unwrap(),
        );
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
            metadata.clone(),
        )
        .await;
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
        schema.extend((0..field_count).map(|index| ColumnSchema {
            column_name: format!("field_{index}"),
            datatype: ColumnDataType::Float64 as i32,
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        }));
        let codec = SparsePrimaryKeyCodec::schemaless();
        let mut rows = Vec::new();
        for id in 0..series_count {
            let mut key = Vec::new();
            codec.encode_internal(10, id, &mut key).unwrap();
            codec
                .encode_to_vec(
                    metadata
                        .column_metadatas
                        .iter()
                        .filter(|column| {
                            column.semantic_type == SemanticType::Tag && column.column_id < 10000
                        })
                        .map(|column| {
                            (
                                column.column_id,
                                ValueRef::String(if column.column_id == 1 { "x" } else { "a" }),
                            )
                        }),
                    &mut key,
                )
                .unwrap();
            for timestamp in [1000, 2000] {
                let mut values = vec![
                    ValueData::BinaryValue(key.clone()),
                    ValueData::TimestampMillisecondValue(timestamp),
                ];
                values.extend((0..field_count).map(|_| ValueData::F64Value(1.0)));
                rows.push(row(values));
            }
        }
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
        flush_region(&engine, region.region_id, None).await;
        build(&engine, &region).await;
        let store = region.series_index_store.clone().unwrap();
        let namespace = FileId::random();
        let mut cold = Vec::new();
        let mut warm = Vec::new();
        for _ in 0..7 {
            let cache = CacheStrategy::EnableAll(Arc::new(
                CacheManager::builder()
                    .sst_meta_cache_size(256 * 1024 * 1024)
                    .build(),
            ));
            for iteration in 0..8 {
                let mut input = scan_input(
                    &region,
                    store.clone(),
                    SelectedFormat::Columnar,
                    (0..metadata.column_metadatas.len()).collect(),
                );
                input.cache_strategy = cache.clone();
                input.series_index.as_mut().unwrap().cache_namespace = namespace;
                let scanner = SeriesScan::new(input, false);
                let binding = labels(scanner.metadata());
                let start = Instant::now();
                let source = scanner
                    .prepare_series(
                        &QueryScanContext::default(),
                        &ExecutionPlanMetricsSet::new(),
                        &binding,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(series_count as usize, source.num_series());
                if iteration == 0 {
                    cold.push(elapsed);
                } else if iteration >= 3 {
                    warm.push(elapsed);
                }
            }
        }
        cold.sort_by(f64::total_cmp);
        warm.sort_by(f64::total_cmp);
        println!(
            "native_prepare series={series_count} tags={tag_count} fields={field_count} cold_median_ms={:.3} warm_median_ms={:.3} cold_runs={} warm_runs={}",
            cold[cold.len() / 2],
            warm[warm.len() / 2],
            cold.len(),
            warm.len()
        );
        engine.stop().await.unwrap();
    }
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
        let predicate = Predicate::new(vec![expr]);
        let conjuncts = predicate_conjuncts(Some(&predicate));
        let (request, accepted) = series_read_request(&schema, &conjuncts);
        // Accepted flags stay aligned with the conjuncts and describe `request.filters`.
        assert_eq!(conjuncts.len(), accepted.len());
        let pushed: Vec<_> = accepted
            .iter()
            .zip(&conjuncts)
            .filter(|(accepted, _)| **accepted)
            .map(|(_, expr)| {
                let columns = expr.column_refs();
                assert_eq!(1, columns.len());
                columns.into_iter().next().unwrap().name.clone()
            })
            .collect();
        assert_eq!(
            pushed,
            request
                .filters
                .iter()
                .map(|filter| filter.column_name().to_owned())
                .collect::<Vec<_>>()
        );
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
    let predicate = Predicate::new(vec![mixed]);
    let conjuncts = predicate_conjuncts(Some(&predicate));
    let (request, accepted) = series_read_request(&schema, &conjuncts);
    assert_eq!(vec![false], accepted);
    assert!(request.filters.is_empty());
    assert_eq!(None, request.table_id);
    // A conjunct the reader cannot apply exactly is kept for the residual filters.
    let unsupported = Expr::Not(Box::new(col("tag").eq(lit("alpha"))));
    let predicate = Predicate::new(vec![unsupported]);
    let conjuncts = predicate_conjuncts(Some(&predicate));
    let (request, accepted) = series_read_request(&schema, &conjuncts);
    assert_eq!(vec![false], accepted);
    assert!(request.filters.is_empty());
    // A pushed conjunct and a visible rejected conjunct: only the latter needs the residual.
    let visible_mixed = table
        .clone()
        .eq(lit(10u32))
        .and(Expr::Not(Box::new(col("tag").eq(lit("alpha")))));
    let predicate = Predicate::new(vec![visible_mixed]);
    let conjuncts = predicate_conjuncts(Some(&predicate));
    assert_eq!(2, conjuncts.len());
    let (request, accepted) = series_read_request(&schema, &conjuncts);
    assert_eq!(vec![true, false], accepted);
    assert_eq!(
        vec![DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_owned()],
        request
            .filters
            .iter()
            .map(|filter| filter.column_name().to_owned())
            .collect::<Vec<_>>()
    );
    assert!(matches!(conjuncts[1], Expr::Not(_)));
    let projection: Vec<_> = request
        .projection
        .iter()
        .map(|name| schema.index_of(name).unwrap())
        .collect();
    let projected = Arc::new(schema.project(&projection).unwrap());
    assert!(Predicate::to_physical_expr(conjuncts[1], &projected).is_ok());
    let predicate = Predicate::new(vec![lit(20u32).eq(table)]);
    let conjuncts = predicate_conjuncts(Some(&predicate));
    let request = series_read_request(&schema, &conjuncts).0;
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
        .read(series_read_request(&schema, &predicate_conjuncts(None)).0)
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
    let predicate = Predicate::new(vec![
        col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME).eq(lit(u32::MAX)),
    ]);
    let request = series_read_request(&schema, &predicate_conjuncts(Some(&predicate))).0;
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

/// Pushed conjuncts are already exact, so only the rejected ones may filter discovery.
#[tokio::test]
async fn residual_conjuncts_still_filter_discovered_series() {
    let (_env, engine, region) = fixture().await;
    // The first series has `tag_1 = "x"` and fails the residual; the second (`"y"`) survives.
    write_with_tags(&engine, &region, 2, 3000, Some(2.0), &["a", "y"]).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    let predicate = col("tag_0")
        .eq(lit("a"))
        .and(Expr::Not(Box::new(col("tag_1").eq(lit("x")))));
    let mut results = Vec::new();
    for format in [SelectedFormat::Legacy, SelectedFormat::Columnar] {
        let mut input = scan_input(
            &region,
            region.series_index_store.clone().unwrap(),
            format,
            (0..region.version().metadata.column_metadatas.len()).collect(),
        );
        input.predicate =
            PredicateGroup::new(&region.version().metadata, std::slice::from_ref(&predicate))
                .unwrap();
        let scanner = SeriesScan::new(input, false);
        let binding = labels(scanner.metadata());
        let stream = scanner
            .scan_series_partition(
                &QueryScanContext::default(),
                &ExecutionPlanMetricsSet::new(),
                0,
                &binding,
            )
            .unwrap()
            .unwrap();
        let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
        if format == SelectedFormat::Columnar {
            let prepared = scanner.prepared.source.get().unwrap().as_ref().unwrap();
            // `tag_0 = 'a'` matches both series; only the residual drops `tag_1 = 'x'`.
            assert_eq!(1, prepared.candidates.len());
            assert_eq!(2, prepared.candidates[0].id.tsid);
        }
        results.push(samples(&chunks));
    }
    assert_eq!(vec![(3000, 2.0)], results[0]);
    assert_eq!(results[0], results[1]);
    engine.stop().await.unwrap();
}

/// Sparse timestamps must not create empty-day artifacts, and new SSTs must leave existing
/// source groups intact. Reopening exercises the persisted explicit coverage used by preflight.
#[tokio::test]
async fn daily_samples_preserve_sparse_coverage_and_existing_groups() {
    let (_env, engine, region) = fixture().await;
    const LATER: i64 = 20_000 * 86_400_000;
    write(&engine, &region, 0, 0, Some(1.0)).await;
    write(&engine, &region, 0, LATER, Some(2.0)).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    let original = region.series_index_version();
    assert_eq!(original.samples.len(), 2);
    assert_eq!(
        original
            .samples
            .values()
            .map(|h| h.entry().window.unwrap())
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 20_000].into()
    );
    let store = region.series_index_store.clone().unwrap();
    let (purger, _receiver) = crate::series_index::series_index_channel(store.clone());
    let reopened =
        crate::series_index::load_version_control(&store, region.region_id, &purger).await;
    for file in region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
    {
        assert!(crate::series_index::artifact::samples_cover_file(
            &reopened.current().samples,
            file
        ));
    }
    // Complete source coverage spans both days, but preparation must open only
    // intersecting artifacts. Compare rows with the ordinary scanner as well.
    for (start, end, expected_readers) in [
        (0, 86_400_000, 1),
        (86_400_000, LATER, 0),
        (LATER, LATER + 1, 1),
        (0, LATER + 1, 2),
    ] {
        let mut results = Vec::new();
        for format in [SelectedFormat::Legacy, SelectedFormat::Columnar] {
            let mut input = scan_input(
                &region,
                store.clone(),
                format,
                (0..region.version().metadata.column_metadatas.len()).collect(),
            );
            input.time_range = common_time::range::TimestampRange::new(
                common_time::Timestamp::new_millisecond(start),
                common_time::Timestamp::new_millisecond(end),
            );
            input.predicate = PredicateGroup::new(
                &region.version().metadata,
                &[
                    col("tag_0").eq(lit("a")),
                    col("field_0").gt(lit(0.0)),
                    col("ts").gt_eq(lit(datafusion_common::ScalarValue::TimestampMillisecond(
                        Some(start),
                        None,
                    ))),
                    col("ts").lt(lit(datafusion_common::ScalarValue::TimestampMillisecond(
                        Some(end),
                        None,
                    ))),
                ],
            )
            .unwrap();
            let scan = SeriesScan::new(input, false);
            let chunks = scan
                .scan_series_partition(
                    &QueryScanContext::default(),
                    &ExecutionPlanMetricsSet::new(),
                    0,
                    &labels(scan.metadata()),
                )
                .unwrap()
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            if format == SelectedFormat::Columnar {
                let prepared = scan.prepared.source.get().unwrap().as_ref().unwrap();
                assert_eq!(prepared.samples.len(), expected_readers);
            }
            results.push(rows(&chunks));
        }
        assert_eq!(results[0], results[1]);
    }
    write(&engine, &region, 0, 3000, Some(3.0)).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    let updated = region.series_index_version();
    assert_eq!(updated.samples.len(), original.samples.len() + 1);
    for (id, old) in &original.samples {
        assert_eq!(updated.samples[id].entry(), old.entry());
    }
    engine.stop().await.unwrap();
}

#[rstest::rstest]
#[case::last_row(false)]
#[case::last_non_null(true)]
#[tokio::test]
async fn identity_merge_preserves_tag_and_mixed_predicates(#[case] last_non_null: bool) {
    let (_env, engine, region) = fixture().await;
    if last_non_null {
        // A newer null in the first field must recover the older value across SSTs.
        let metadata = region.version().metadata.clone();
        engine
            .handle_request(
                region.region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: Rows {
                        schema: vec![
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
                        ],
                        rows: vec![api::v1::Row {
                            values: vec![
                                api::v1::Value {
                                    value_data: Some(ValueData::BinaryValue(
                                        new_sparse_primary_key(&["a", "x"], &metadata, 10, 0),
                                    )),
                                },
                                api::v1::Value {
                                    value_data: Some(ValueData::TimestampMillisecondValue(1000)),
                                },
                                api::v1::Value { value_data: None },
                            ],
                        }],
                    },
                    skip_wal: false,
                    hint: Some(api::v1::WriteHint {
                        primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse as i32,
                    }),
                    partition_expr_version: None,
                }),
            )
            .await
            .unwrap();
        flush_region(&engine, region.region_id, None).await;
    }
    build(&engine, &region).await;
    let cases = [
        (col("tag_0").eq(lit("a")), true),
        (col("tag_0").is_null().or(col("tag_0").eq(lit(""))), true),
        (
            col("tag_0").eq(lit("a")).or(col("field_0").gt(lit(0.0))),
            false,
        ),
        (col("field_0").gt(lit(0.0)), true),
        (
            col("tag_0").eq(lit("a")).and(col("field_0").gt(lit(0.0))),
            true,
        ),
    ];
    for (predicate, identity_only) in cases {
        let mut results = Vec::new();
        for format in [SelectedFormat::Legacy, SelectedFormat::Columnar] {
            let mut input = scan_input(
                &region,
                region.series_index_store.clone().unwrap(),
                format,
                (0..region.version().metadata.column_metadatas.len()).collect(),
            );
            if last_non_null {
                input.merge_mode = crate::region::options::MergeMode::LastNonNull;
            }
            input.predicate =
                PredicateGroup::new(&region.version().metadata, std::slice::from_ref(&predicate))
                    .unwrap();
            let scanner = SeriesScan::new(input, false);
            let binding = labels(scanner.metadata());
            let schema = scanner.raw_series_schema(&binding).unwrap().unwrap();
            let explicit = scanner.raw_series_has_explicit_labels(&binding).unwrap();
            assert_eq!(
                format == SelectedFormat::Columnar && identity_only,
                explicit
            );
            let stream = scanner
                .scan_series_partition(
                    &QueryScanContext::default(),
                    &ExecutionPlanMetricsSet::new(),
                    0,
                    &binding,
                )
                .unwrap()
                .unwrap();
            assert_eq!(schema.as_ref(), stream.schema().as_ref());
            assert!(same_labels(stream.label_source(), &binding));
            assert_eq!(explicit, stream.label_decode_spec().is_none());
            assert_eq!(SeriesOrdering::KeyThenTime, stream.ordering());
            assert_eq!(scanner.raw_series_ordering().unwrap(), stream.ordering());
            let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
            assert!(
                chunks
                    .iter()
                    .all(|chunk| chunk.metric.is_some() == explicit)
            );
            if format == SelectedFormat::Columnar {
                let prepared = scanner
                    .prepared
                    .source
                    .get()
                    .expect("columnar source must be prepared");
                assert_eq!(
                    identity_only,
                    prepared.as_ref().unwrap().sample_schema.is_some()
                );
            }
            results.push(rows(&chunks));
        }
        assert_eq!(results[0], results[1]);
    }
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn overlapping_buckets_retain_only_new_label_rows() {
    let (_env, engine, region) = fixture().await;
    const LATER: i64 = 6 * 86_400_000;
    for timestamp in [3000, LATER] {
        for id in 1..=32 {
            let tag = format!("tag-{id}");
            write_with_tags(&engine, &region, id, timestamp, Some(1.0), &["a", &tag]).await;
        }
        flush_region(&engine, region.region_id, None).await;
    }
    write_with_tags(&engine, &region, 33, LATER, Some(1.0), &["a", "tag-33"]).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    assert!(region.series_index_version().series_indexes.len() >= 2);
    let scan = scanner(&region, region.series_index_store.clone().unwrap());
    scan.prepare_series(
        &QueryScanContext::default(),
        &ExecutionPlanMetricsSet::new(),
        &labels(scan.metadata()),
    )
    .await
    .unwrap()
    .unwrap();
    let prepared = scan.prepared.source.get().unwrap().as_ref().unwrap();
    assert_eq!(34, prepared.candidates.len());
    // Duplicate rows from the overlapping bucket are dropped before labels are retained, so no
    // series keeps the same timestamp twice. Asserted on this preparation's own rows instead of
    // on the process-global counters: concurrent preparations in the same process move those
    // counters too, which made a delta comparison fail under parallel test runs.
    let chunks = prepared
        .read_series(0..prepared.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    for chunk in &chunks {
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..chunk.time.len() {
            let timestamp = chunk.time.timestamp(i).unwrap();
            assert!(
                seen.insert(timestamp),
                "series retained two rows at the same timestamp {timestamp}"
            );
        }
    }
    for candidate in prepared.candidates.iter() {
        let expected = if candidate.id.tsid == 0 {
            "x".to_string()
        } else {
            format!("tag-{}", candidate.id.tsid)
        };
        assert_eq!(
            candidate.metric.as_ref().unwrap().get("tag_1"),
            Some(expected.as_str())
        );
    }
    engine.stop().await.unwrap();
}

#[rstest::rstest]
#[case::direct(false)]
#[case::mixed_fallback(true)]
#[tokio::test]
async fn overlapping_buckets_keep_colliding_physical_tags(#[case] mixed: bool) {
    let (_env, engine, region) = fixture().await;
    write_with_tags(
        &engine,
        &region,
        0,
        6 * 86_400_000,
        Some(1.0),
        &["a", "different"],
    )
    .await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    assert!(region.series_index_version().series_indexes.len() >= 2);
    let mut input = scan_input(
        &region,
        region.series_index_store.clone().unwrap(),
        SelectedFormat::Columnar,
        (0..region.version().metadata.column_metadatas.len()).collect(),
    );
    if mixed {
        input.predicate = PredicateGroup::new(
            &region.version().metadata,
            &[col("tag_0").eq(lit("a")).or(col("field_0").gt(lit(0.0)))],
        )
        .unwrap();
    }
    let scan = SeriesScan::new(input, false);
    let mut binding = labels(scan.metadata());
    if let RawLabelSource::PrimaryKey { logical_tags, .. } = &mut binding {
        // The conflict is in an unbound physical tag, which must still be checked.
        *logical_tags = logical_tags[..1].to_vec().into();
    }
    scan.prepare_series(
        &QueryScanContext::default(),
        &ExecutionPlanMetricsSet::new(),
        &binding,
    )
    .await
    .expect("one tsid with two label sets prepares as two series");
    let source = scan.prepared.source.get().unwrap().as_ref().unwrap();
    // Same `(table_id, tsid)`, different physical tags: two series, never one merged row.
    assert_eq!(2, source.num_series());
    let keys: HashSet<Vec<u8>> = (0..source.num_series())
        .map(|ordinal| source.series_info(ordinal).unwrap().primary_key.to_vec())
        .collect();
    assert_eq!(2, keys.len());
    let chunks = source
        .read_series(0..source.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    // The differing physical tag is unbound here, so only the exact key separates the
    // two candidates; the previous code compared labels and failed preparation.
    let identities: HashSet<Vec<u8>> = source
        .candidates
        .iter()
        .map(|candidate| candidate.merge_key.to_vec())
        .collect();
    assert_eq!(2, identities.len(), "colliding series keep distinct keys");
    if !mixed {
        // Every returned chunk names one of the candidates' label sets; a chunk whose
        // labels match neither would mean the pair had been merged again.
        let labels = |metric: &SeriesLabels| {
            metric
                .tags()
                .iter()
                .map(|label| (label.name.to_string(), label.value.to_string()))
                .collect::<Vec<_>>()
        };
        let expected: HashSet<Vec<(String, String)>> = (0..source.num_series())
            .map(|ordinal| {
                labels(
                    source
                        .series_info(ordinal)
                        .unwrap()
                        .metric
                        .as_ref()
                        .unwrap(),
                )
            })
            .collect();
        for chunk in &chunks {
            let actual = labels(chunk.metric.as_ref().expect("explicit labels"));
            assert!(
                expected.contains(&actual),
                "chunk labels {actual:?} match no candidate: {expected:?}"
            );
        }
    }
    engine.stop().await.unwrap();
}

/// Two series forced to share one `tsid` must never receive each other's samples:
/// candidate discovery, sample attribution, and the merged primary key are all exact.
#[tokio::test]
async fn colliding_tsids_isolate_samples_and_labels_per_candidate() {
    use common_time::Timestamp;
    use common_time::range::TimestampRange;

    let (_env, engine, region) = fixture().await;
    let metadata = region.version().metadata.clone();
    let first = new_sparse_primary_key(&["host-020078", "z"], &metadata, 10, 0);
    let partner = new_sparse_primary_key(&["host-029838", "z"], &metadata, 10, 0);
    // Same `(table_id, tsid)`; only the labels differ.
    for (key, base) in [(first, 0.0), (partner, 100.0)] {
        for (index, timestamp) in [1000, 2000, 3000].into_iter().enumerate() {
            write_primary_key(
                &engine,
                &region,
                key.clone(),
                timestamp,
                Some(base + index as f64),
            )
            .await;
        }
    }
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    let make_scan = |predicates: Vec<datafusion_expr::Expr>, range: Option<TimestampRange>| {
        let mut input = scan_input(
            &region,
            region.series_index_store.clone().unwrap(),
            SelectedFormat::Columnar,
            (0..metadata.column_metadatas.len()).collect(),
        );
        // Replace the fixture's default predicate, which would hide both hosts.
        input.predicate = PredicateGroup::new(&metadata, &predicates).unwrap();
        if let Some(range) = range {
            input.time_range = Some(range);
        }
        SeriesScan::new(input, false)
    };
    // One colliding tsid keeps both series, and each ordinal owns only its own rows.
    let scan = make_scan(Vec::new(), None);
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let binding = labels(scan.metadata());
    scan.prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .unwrap();
    let source = scan.prepared.source.get().unwrap().as_ref().unwrap();
    assert_eq!(3, source.num_series(), "both colliding series must survive");
    let mut value_sets = Vec::new();
    for ordinal in 0..source.num_series() {
        let chunks = source
            .read_series(ordinal..ordinal + 1)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        value_sets.push(
            rows(&chunks)
                .into_iter()
                .map(|(_, _, value)| value)
                .collect::<Vec<_>>(),
        );
    }
    assert!(value_sets.contains(&vec![0.0, 1.0, 2.0]), "{value_sets:?}");
    assert!(
        value_sets.contains(&vec![100.0, 101.0, 102.0]),
        "{value_sets:?}"
    );
    assert!(
        !value_sets
            .iter()
            .any(|values| values.contains(&100.0) && values.contains(&0.0)),
        "a candidate must not mix both colliding series: {value_sets:?}"
    );
    // Filtering one host, inside a time range, returns only that host's samples.
    let scan = make_scan(
        vec![col("tag_0").eq(lit("host-020078"))],
        TimestampRange::new(
            Timestamp::new_millisecond(1500),
            Timestamp::new_millisecond(2500),
        ),
    );
    let binding = labels(scan.metadata());
    scan.prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .unwrap();
    let source = scan.prepared.source.get().unwrap().as_ref().unwrap();
    assert_eq!(1, source.num_series());
    let chunks = source
        .read_series(0..source.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        rows(&chunks)
            .into_iter()
            .map(|(_, timestamp, value)| (timestamp, value))
            .collect::<Vec<_>>(),
        vec![(2000, 1.0)],
        "the partner's samples must never be attached to the filtered host"
    );
    engine.stop().await.unwrap();
}

/// A key past 8 KiB must prepare and read like any other: the series-index writer, the
/// samples writer, and preparation all accept the same key, so no artifact can be published
/// that preparation then refuses.
#[tokio::test]
async fn long_keys_prepare_and_read() {
    let (_env, engine, region) = fixture().await;
    let metadata = region.version().metadata.clone();
    let label = "x".repeat(8 * 1024);
    let key = new_sparse_primary_key(&[label.as_str(), "z"], &metadata, 10, 0);
    assert!(key.len() > 8 * 1024, "{}", key.len());
    for (timestamp, value) in [(1000, 1.0), (2000, 2.0)] {
        write_primary_key(&engine, &region, key.clone(), timestamp, Some(value)).await;
    }
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;

    let mut input = scan_input(
        &region,
        region.series_index_store.clone().unwrap(),
        SelectedFormat::Columnar,
        (0..metadata.column_metadatas.len()).collect(),
    );
    // The fixture's default predicate would hide this series.
    input.predicate = PredicateGroup::new(&metadata, &[]).unwrap();
    let scan = SeriesScan::new(input, false);
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let binding = labels(scan.metadata());
    scan.prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .unwrap();
    let source = scan.prepared.source.get().unwrap().as_ref().unwrap();
    // The fixture's own series prepare beside this one; the long key must survive preparation
    // byte for byte and own its samples. Served samples make `primary_key` region-qualified.
    let mut expected = metadata.region_id.as_u64().to_be_bytes().to_vec();
    expected.extend_from_slice(&key);
    let ordinal = (0..source.num_series())
        .find(|ordinal| {
            source.series_info(*ordinal).unwrap().primary_key.as_ref() as &[u8]
                == expected.as_slice()
        })
        .expect("preparation must keep the series whose key is longer than 8 KiB");
    let chunks = source
        .read_series(ordinal..ordinal + 1)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        rows(&chunks)
            .into_iter()
            .map(|(_, timestamp, value)| (timestamp, value))
            .collect::<Vec<_>>(),
        vec![(1000, 1.0), (2000, 2.0)]
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn visibility_fast_path_preserves_buffers_and_partial_filtering() {
    use common_time::{Timestamp, range::TimestampRange};
    use datatypes::arrow::array::TimestampMillisecondArray;
    use store_api::storage::SequenceRange;

    let (_env, engine, region) = fixture().await;
    let metadata = region.version().metadata.clone();
    let time_name = metadata.time_index_column().column_schema.name.clone();
    // Deliberately unordered: first/last values alone cannot prove visibility.
    let sequences = vec![Some(3), Some(5), Some(2), Some(4)];
    let times = vec![Some(0), Some(2), Some(-1), Some(1)];
    let cases = [
        (None, None, None),
        (Some(5), SequenceRange::new(Some(1), Some(5)), Some((-1, 3))),
        (Some(5), SequenceRange::new(Some(1), None), None),
        (None, SequenceRange::new(None, Some(5)), None),
        (Some(4), None, None),
        (None, SequenceRange::new(Some(2), None), None),
        (None, SequenceRange::new(None, Some(4)), None),
        (None, SequenceRange::new(Some(5), Some(5)), None),
        (None, None, Some((-1, 2))),
        (None, None, Some((0, 3))),
    ];
    for nulls in [false, true] {
        for (snapshot, range, bounds) in cases {
            let mut sequences = sequences.clone();
            let mut times = times.clone();
            if nulls {
                sequences[0] = None;
                times[1] = None;
            }
            let input = ScanInput::builder(
                region.access_layer.clone(),
                FlatProjectionMapper::new(&metadata, vec![]).unwrap(),
            )
            .with_snapshot_sequence(snapshot)
            .with_sequence_range(range)
            .with_time_range(bounds.and_then(|(start, end)| {
                TimestampRange::new(
                    Timestamp::new_millisecond(start),
                    Timestamp::new_millisecond(end),
                )
            }))
            .build();
            let context = StreamContext::seq_scan_ctx(input);
            let batch = RecordBatch::try_from_iter(vec![
                (
                    time_name.as_str(),
                    Arc::new(TimestampMillisecondArray::from(times.clone())) as ArrayRef,
                ),
                (
                    SEQUENCE_COLUMN_NAME,
                    Arc::new(UInt64Array::from(sequences.clone())) as ArrayRef,
                ),
            ])
            .unwrap();
            let expected = sequences
                .iter()
                .zip(&times)
                .filter_map(|(seq, time)| {
                    let (seq, time) = ((*seq)?, (*time)?);
                    let visible = snapshot.is_none_or(|max| seq <= max)
                        && range.is_none_or(|range| match range {
                            SequenceRange::Gt { min } => seq > min,
                            SequenceRange::LtEq { max } => seq <= max,
                            SequenceRange::GtLtEq { min, max } => seq > min && seq <= max,
                        })
                        && bounds.is_none_or(|(start, end)| time >= start && time < end);
                    visible.then_some(seq)
                })
                .collect::<Vec<_>>();
            let filtered = filter_visibility(batch.clone(), &context).unwrap();
            let actual = filtered
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(expected.as_slice(), actual.values().as_ref());
            if expected.len() == batch.num_rows() {
                assert!(Arc::ptr_eq(batch.column(0), filtered.column(0)));
                assert!(Arc::ptr_eq(batch.column(1), filtered.column(1)));
            }
        }
    }
    engine.stop().await.unwrap();
}

/// Artifact reads recorded by [`recording_store`] as `(path, range)`.
type RecordedReads = Arc<Mutex<Vec<(String, mock::BytesRange)>>>;

/// Builds a store that records every artifact read, so a test can prove that a selection never
/// opened an artifact it was not allowed to read.
fn recording_store(region: &MitoRegionRef) -> (ObjectStore, RecordedReads) {
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
    (store, reads)
}

/// Recomputes the selection the scanner-wide entries capture, for direct assertions.
fn selection(scan: &SeriesScan) -> SelectedInputs {
    let ranges = scan
        .properties
        .partitions
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    select_inputs(&scan.stream_ctx, &ranges).unwrap()
}

/// Asserts that the only sources no artifact covers are memtable ranges.
fn assert_only_memtable_raw(selected: &SelectedInputs) {
    assert!(
        !selected.raw_sources.is_empty(),
        "an assigned memtable is read as raw rows"
    );
    assert!(
        selected
            .raw_sources
            .iter()
            .all(|source| matches!(source, RawSource::Mem { .. })),
        "a ready SST is carried by its artifact: {:?}",
        selected.raw_sources
    );
}

/// Reads every prepared ordinal of `source` as raw chunks.
async fn prepared_chunks(source: &Arc<dyn PreparedSeriesSource>) -> Vec<RawSeriesChunk> {
    source
        .read_series(0..source.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
}

/// `(labels, time, value)` triples of the chunks, ordered by labels and then time, so a check can
/// tell two series apart even when they share one `(table_id, tsid)`.
fn labelled_rows(chunks: &[RawSeriesChunk]) -> Vec<(Vec<String>, i64, f64)> {
    let mut rows = Vec::new();
    for chunk in chunks {
        let labels = chunk
            .metric
            .as_ref()
            .map(|metric| {
                metric
                    .tags()
                    .iter()
                    .map(|tag| format!("{}={}", tag.name, tag.value))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for index in 0..chunk.time.len() {
            let SampleRef::Float(value) = chunk.fields.get(0).unwrap().sample(index) else {
                panic!("expected float");
            };
            rows.push((labels.clone(), chunk.time.timestamp(index).unwrap(), value));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    rows
}

/// Reads every prepared ordinal of `source` and returns its samples ordered by time.
async fn prepared_samples(source: &Arc<dyn PreparedSeriesSource>) -> Vec<(i64, f64)> {
    let mut samples = samples(
        &source
            .read_series(0..source.num_series())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
    );
    samples.sort_unstable_by_key(|(time, _)| *time);
    samples
}

/// Returns the pinned SST file id whose time range starts at `start` milliseconds.
fn file_id_at(region: &MitoRegionRef, start: i64) -> FileId {
    let expected = common_time::Timestamp::new_millisecond(start);
    region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .find(|file| file.meta_ref().time_range.0 == expected)
        .unwrap_or_else(|| panic!("no SST starts at {start} ms"))
        .file_id()
        .file_id()
}

#[tokio::test]
async fn native_preparation_serves_the_ready_subset() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    // The three older sources are covered by this snapshot; it is restored at the end.
    let covered = region.series_index_version();
    // This SST is flushed after the build, so it carries no columnar artifact: the three older
    // sources are ready while this one is not.
    write(&engine, &region, 0, 3000, Some(4.0)).await;
    flush_region(&engine, region.region_id, None).await;
    // The flush triggers an automatic build that would cover the new SST as well. Drain it, then
    // re-publish the pre-flush snapshot as the last series-index write: no later flush, edit, or
    // periodic maintenance pass runs in this test, so the older three sources stay ready while
    // this one stays uncovered.
    crate::series_index::tests::drain_series_index_builds(&engine, &region).await;
    region.series_index_version_control.publish(covered);
    let (store, _reads) = recording_store(&region);
    let mut scan = scanner(&region, store);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    let selected = selection(&scan);
    assert!(!selected.all_ready);
    assert_eq!(3, selected.ready_files.len());
    assert_eq!(1, selected.sample_artifacts.len());

    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the ready subset must be served natively");
    // Only the ready sources contribute rows here; the unready SST stays with the ordinary scan.
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0)],
        prepared_samples(&source).await
    );
    assert_eq!(1, source.num_series());

    // The raw entry serves the same ready subset now: the unready SST has no artifact, so its row
    // is not returned by the raw path either.
    assert!(scan.raw_series_schema(&binding).unwrap().is_some());
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0)],
        all_raw_samples(&scan, &ctx, &metrics, &binding, SeriesOrdering::KeyThenTime).await
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn native_preparation_is_empty_without_artifacts() {
    let (_env, engine, region) = fixture().await;
    let (store, reads) = recording_store(&region);
    let mut scan = scanner(&region, store);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    let selected = selection(&scan);
    assert!(selected.ready_files.is_empty());
    assert!(selected.sample_artifacts.is_empty());
    assert!(!selected.all_ready);

    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    // The gate is on, so the answer is an empty source rather than a declined one: a caller
    // never has to fall back to the ordinary scan.
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("a gated region prepares an empty source instead of declining");
    assert_eq!(0, source.num_series());
    assert!(same_labels(source.label_source(), &binding));
    assert!(
        source
            .read_series(0..0)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    // No ready input means no artifact may be opened.
    assert!(reads.lock().unwrap().is_empty());
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn native_preparation_skips_an_assigned_memtable() {
    let (_env, engine, region) = fixture().await;
    let request = ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        ..Default::default()
    };
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();

    // An assigned memtable has no artifact, so with no ready SST there is nothing to prepare.
    write(&engine, &region, 0, 3000, Some(4.0)).await;
    let scanner = engine
        .handle_query(region.region_id, request.clone())
        .await
        .unwrap();
    let binding = labels(scanner.metadata());
    // The memtable has no artifact and no SST is covered yet, so the answer is empty: the raw
    // path no longer reads the memtable row with the ordinary scan.
    assert_eq!(
        Vec::<(i64, f64)>::new(),
        all_raw_samples(
            scanner.as_ref(),
            &ctx,
            &metrics,
            &binding,
            SeriesOrdering::KeyThenTime
        )
        .await
    );
    assert_eq!(
        0,
        scanner
            .prepare_series(&ctx, &metrics, &binding)
            .await
            .unwrap()
            .expect("a gated region prepares an empty source instead of declining")
            .num_series(),
        "an assigned memtable has no artifact, so nothing is ready"
    );

    // Ready SSTs are served and the memtable is skipped, not translated into a fallback.
    build(&engine, &region).await;
    let scanner = engine
        .handle_query(region.region_id, request)
        .await
        .unwrap();
    let binding = labels(scanner.metadata());
    let source = scanner
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("ready SSTs are served even when a memtable is assigned");
    let rows = source
        .read_series(0..source.num_series())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(vec![(1000, 1.0), (2000, 1.0)], samples(&rows));
    // The raw entry serves the same ready subset: an assigned memtable is skipped there too, so
    // its row is no longer part of the raw result.
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0)],
        all_raw_samples(
            scanner.as_ref(),
            &ctx,
            &metrics,
            &binding,
            SeriesOrdering::KeyThenTime
        )
        .await
    );
    engine.stop().await.unwrap();
}

/// Nothing is published here, so the native entry has no artifact at all; it still answers with
/// every assigned source, including the memtable-only series.
#[tokio::test]
async fn native_scan_serves_a_series_that_exists_only_in_the_memtable() {
    let (_env, engine, region) = fixture().await;
    // A second series lives in the memtable only: no SST holds it, so no artifact can.
    write_with_tags(&engine, &region, 1, 3000, Some(4.0), &["a", "y"]).await;
    let scan = region_scan(&region).await;
    let selected = selection(&scan);
    assert!(selected.ready_files.is_empty());
    assert!(selected.series_indexes.is_empty());
    // The three flushed SSTs and the assigned memtable are all read as raw sources.
    assert_eq!(
        3,
        selected
            .raw_sources
            .iter()
            .filter(|source| matches!(source, RawSource::File { .. }))
            .count()
    );
    assert!(
        selected
            .raw_sources
            .iter()
            .any(|source| matches!(source, RawSource::Mem { .. })),
        "an assigned memtable is part of the answer, not a reason to narrow it"
    );

    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the native entry serves the whole assignment");
    assert_eq!(2, source.num_series());
    assert_eq!(
        vec![
            (
                vec!["tag_0=a".to_string(), "tag_1=x".to_string()],
                1000,
                1.0
            ),
            (
                vec!["tag_0=a".to_string(), "tag_1=x".to_string()],
                2000,
                1.0
            ),
            (
                vec!["tag_0=a".to_string(), "tag_1=y".to_string()],
                3000,
                4.0
            ),
        ],
        labelled_rows(&prepared_chunks(&source).await)
    );
    engine.stop().await.unwrap();
}

/// A memtable row at a new timestamp belongs to a series an artifact already serves, so the
/// artifact is read for its own rows and the memtable supplies the rest.
#[tokio::test]
async fn native_scan_serves_a_memtable_row_of_an_artifact_series() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    // This row is newer than the published artifacts and only exists in the memtable.
    write(&engine, &region, 0, 3000, Some(4.0)).await;
    let scan = region_scan(&region).await;
    let selected = selection(&scan);
    assert_eq!(3, selected.ready_files.len());
    assert_eq!(1, selected.series_indexes.len());
    // Every assigned SST is covered by the selected artifact, so only the memtable is raw.
    assert_only_memtable_raw(&selected);

    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the native entry serves the whole assignment");
    assert_eq!(1, source.num_series());
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0), (3000, 4.0)],
        prepared_samples(&source).await
    );
    engine.stop().await.unwrap();
}

/// A delete in the memtable removes a row a published artifact contains, and an overwrite at
/// another timestamp returns the new value: artifact rows are not final.
#[tokio::test]
async fn native_scan_applies_memtable_deletes_and_overwrites_to_artifact_rows() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    // The artifacts carry (1000, 1.0) and (2000, 1.0) for this series.
    write(&engine, &region, 0, 1000, None).await;
    write(&engine, &region, 0, 2000, Some(9.0)).await;
    let scan = region_scan(&region).await;
    let selected = selection(&scan);
    assert_only_memtable_raw(&selected);

    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the native entry serves the whole assignment");
    assert_eq!(
        vec![(2000, 9.0)],
        prepared_samples(&source).await,
        "the memtable delete must remove the artifact row and the overwrite must win"
    );
    engine.stop().await.unwrap();
}

/// A selected grouped artifact already carries the rows of every source it lists, including a
/// source with no covering series index. That source must not also be read as raw rows, in query
/// or append mode.
#[tokio::test]
async fn native_scan_reads_a_source_a_grouped_artifact_already_carries_once() {
    // Distinct flushed timestamps keep the append-mode frame read valid: that mode has no dedup,
    // so two artifact rows at one timestamp would break the strictly increasing timestamp contract
    // for reasons unrelated to source coverage.
    let (_env, engine, region) =
        fixture_with_timestamps(SelectedFormat::Columnar, false, &[1000, 2000]).await;
    // A later bucket in the same UTC day shares the day's grouped sample artifact with the older
    // sources.
    write(&engine, &region, 0, 200_000, Some(2.0)).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    region.series_index_version_control.publish(Arc::default());
    build(&engine, &region).await;
    let unindexed = file_id_at(&region, 200_000);

    let mut input = scan_input(
        &region,
        region.series_index_store.clone().unwrap(),
        SelectedFormat::Columnar,
        (0..region.version().metadata.column_metadatas.len()).collect(),
    );
    // A source whose sequence the published bucket cannot prove is not covered by it, so this
    // source has no covering series index while the grouped artifact still lists it.
    let mut files = input.files.clone();
    for file in &mut files {
        if file.file_id().file_id() == unindexed {
            let mut meta = file.meta_ref().clone();
            meta.sequence = None;
            *file = crate::sst::file::FileHandle::new(meta, region.file_purger.clone());
        }
    }
    input.files = files;
    let mut scan = SeriesScan::new(input, false);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    let selected = selection(&scan);
    assert_eq!(2, selected.ready_files.len());
    assert_eq!(1, selected.sample_artifacts.len());
    // The partition is by artifact source coverage, never by readiness: the unready source is
    // already carried, so reading it again would duplicate every one of its rows.
    assert!(
        !selected.raw_sources.iter().any(|source| match source {
            RawSource::File { index } => {
                scan.stream_ctx.input.files[*index].file_id().file_id() == unindexed
            }
            RawSource::Mem { .. } => false,
        }),
        "a selected grouped artifact already carries this source"
    );
    assert!(selected.raw_sources.is_empty());

    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the covering source keeps the grouped artifact eligible");
    let query = prepared_samples(&source).await;
    assert_eq!(vec![(1000, 1.0), (2000, 1.0), (200_000, 2.0)], query);

    // Append mode has no dedup to repair a duplicated source, so the same partition must hold
    // there too. The native entry declines append mode at planning, so the test drives the
    // prepared source itself.
    let append_ctx = Arc::new(StreamContext::seq_scan_ctx(scan_input_with_append(
        &region,
        region.series_index_store.clone().unwrap(),
        SelectedFormat::Columnar,
        (0..region.version().metadata.column_metadatas.len()).collect(),
        true,
    )));
    let append_ranges = append_ctx.partition_ranges();
    let append_selection = select_inputs(&append_ctx, &append_ranges).unwrap();
    assert_eq!(selected.raw_sources, append_selection.raw_sources);
    let metrics = crate::read::series_scan::new_partition_metrics(
        &append_ctx,
        false,
        &ExecutionPlanMetricsSet::new(),
        0,
        &crate::read::scan_util::PartitionMetricsList::default(),
    );
    let preflight = scan
        .prepare_raw_scan(&binding, SeriesInputMode::Native)
        .unwrap()
        .expect("a gated native scan has a preflight");
    let append_source = ColumnarPreparedSource::prepare(
        append_ctx,
        append_selection,
        binding.clone(),
        preflight.raw,
        preflight.raw_pruner,
        metrics,
    )
    .await
    .unwrap();
    let append_source: Arc<dyn PreparedSeriesSource> = Arc::new(append_source);
    assert_eq!(
        query,
        prepared_samples(&append_source).await,
        "append mode has no dedup to repair a source that is read twice"
    );
    engine.stop().await.unwrap();
}

/// Two series can share one `(table_id, tsid)` because `tsid` is a hash. Keeping them apart must
/// not depend on which source each comes from.
#[tokio::test]
async fn native_scan_keeps_tsid_collisions_distinct_across_artifact_and_memtable() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    let metadata = region.version().metadata.clone();
    // A second series with the same table id and tsid as the artifact series, told apart only by
    // its labels, and written so that it exists in the memtable alone.
    write_primary_key(
        &engine,
        &region,
        new_sparse_primary_key(&["a", "y"], &metadata, 10, 0),
        3000,
        Some(5.0),
    )
    .await;
    let scan = region_scan(&region).await;
    let selected = selection(&scan);
    assert_eq!(3, selected.ready_files.len());
    assert_only_memtable_raw(&selected);

    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the native entry serves the whole assignment");
    assert_eq!(2, source.num_series());
    assert_eq!(
        vec![
            (
                vec!["tag_0=a".to_string(), "tag_1=x".to_string()],
                1000,
                1.0
            ),
            (
                vec!["tag_0=a".to_string(), "tag_1=x".to_string()],
                2000,
                1.0
            ),
            (
                vec!["tag_0=a".to_string(), "tag_1=y".to_string()],
                3000,
                5.0
            ),
        ],
        labelled_rows(&prepared_chunks(&source).await)
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn native_and_raw_preparation_agree_when_fully_ready() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    let (store, _reads) = recording_store(&region);
    let mut scan = scanner(&region, store);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    let selected = selection(&scan);
    assert!(selected.all_ready);
    assert_eq!(3, selected.ready_files.len());
    assert_eq!(1, selected.series_indexes.len());

    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .unwrap();
    let native = prepared_samples(&source).await;
    assert_eq!(1, source.num_series());
    let raw = all_raw_samples(&scan, &ctx, &metrics, &binding, SeriesOrdering::KeyThenTime).await;
    assert_eq!(raw, native);
    assert_eq!(vec![(1000, 1.0), (2000, 1.0)], native);
    // The raw entrypoint reuses the native preparation instead of rediscovering the sources.
    assert!(scan.prepared.source.get().is_some());
    engine.stop().await.unwrap();
}

/// `ScanRequest` for the raw-capable (per-series) read path of these tests.
fn per_series_request() -> ScanRequest {
    ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        ..Default::default()
    }
}

/// With the columnar gate on, the raw entry serves the ready subset: a partially covered region
/// with an assigned memtable returns the covered SST rows only, exactly as the artifacts hold them.
#[tokio::test]
async fn raw_scan_serves_the_ready_subset_and_skips_an_assigned_memtable() {
    let (_env, engine, region) = fixture().await;
    // Publish coverage over the three flushed sources.
    build(&engine, &region).await;
    let covered = region.series_index_version();
    // A fourth source flushed after the build stays uncovered, because the pre-build snapshot is
    // re-published below.
    write(&engine, &region, 0, 3000, Some(4.0)).await;
    flush_region(&engine, region.region_id, None).await;
    crate::series_index::tests::drain_series_index_builds(&engine, &region).await;
    region.series_index_version_control.publish(covered);
    // This row never leaves the memtable, which has no artifact.
    write(&engine, &region, 0, 5000, Some(5.0)).await;

    let scanner = engine
        .handle_query(region.region_id, per_series_request())
        .await
        .unwrap();
    let binding = labels(scanner.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    // Planning keeps the raw path available; only the served subset narrowed.
    assert!(scanner.raw_series_schema(&binding).unwrap().is_some());
    let expected = vec![
        (series_key(10, 0), 1000, 1.0),
        (series_key(10, 0), 2000, 1.0),
    ];
    assert_eq!(
        expected,
        all_raw_rows(
            scanner.as_ref(),
            &ctx,
            &metrics,
            &binding,
            SeriesOrdering::KeyThenTime
        )
        .await,
        "the raw entry must serve the covered sources, without the uncovered SST and memtable rows"
    );
    // The native entry serves the very same rows: one definition of the ready subset.
    let native = scanner
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the ready subset is prepared");
    let native_rows = rows(
        &native
            .read_series(0..native.num_series())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
    );
    assert_eq!(expected, native_rows);
    engine.stop().await.unwrap();
}

/// A fully covered region keeps returning the rows it returned before the wiring, and the label
/// representation declared at planning time is the one the stream actually carries.
#[tokio::test]
async fn raw_scan_of_a_fully_covered_region_keeps_its_rows_and_label_declaration() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    let (store, _reads) = recording_store(&region);
    let mut scan = scanner(&region, store);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    let selected = selection(&scan);
    assert!(selected.all_ready);
    assert!(!selected.ready_files.is_empty());

    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    assert!(scan.raw_series_schema(&binding).unwrap().is_some());
    let declared = scan.raw_series_has_explicit_labels(&binding).unwrap();
    assert!(
        declared,
        "an identity merge of a fully covered region carries explicit labels"
    );
    let stream = scan
        .scan_series_partition(&ctx, &metrics, 0, &binding)
        .unwrap()
        .unwrap();
    assert_eq!(SeriesOrdering::KeyThenTime, stream.ordering());
    // The declaration made before the first poll is the representation the stream carries.
    assert_eq!(declared, stream.label_decode_spec().is_none());
    let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
    assert!(chunks.iter().all(|chunk| chunk.metric.is_some()) == declared);
    let expected = vec![
        (series_key(10, 0), 1000, 1.0),
        (series_key(10, 0), 2000, 1.0),
    ];
    assert_eq!(expected, rows(&chunks));
    // Unchanged by the wiring: the packed frame carries the same rows and the same guarantee.
    let frame = pack_raw_series(
        scan.scan_series_partition(&ctx, &metrics, 0, &binding)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let unpacked = unpack_raw_series(frame, scan.raw_series_schema(&binding).unwrap()).unwrap();
    assert_eq!(SeriesOrdering::KeyThenTime, unpacked.ordering());
    assert_eq!(
        expected,
        rows(&unpacked.try_collect::<Vec<_>>().await.unwrap())
    );
    engine.stop().await.unwrap();
}

/// Outside the columnar gate the raw entry keeps the old behavior: every assigned source,
/// memtable rows included, is still read by the ordinary scan even after a build published legacy
/// coverage.
#[tokio::test]
async fn raw_scan_outside_the_columnar_gate_still_reads_every_source() {
    let (_env, engine, region) = legacy_fixture().await;
    build(&engine, &region).await;
    // This row stays in the memtable; no artifact can serve it.
    write(&engine, &region, 0, 3000, Some(4.0)).await;

    let scanner = engine
        .handle_query(region.region_id, per_series_request())
        .await
        .unwrap();
    let binding = labels(scanner.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    assert!(scanner.raw_series_schema(&binding).unwrap().is_some());
    // No prepared reader answers these sources, so the frames keep their key-encoded labels.
    assert!(!scanner.raw_series_has_explicit_labels(&binding).unwrap());
    assert_eq!(
        vec![
            (series_key(10, 0), 1000, 1.0),
            (series_key(10, 0), 2000, 1.0),
            (series_key(10, 0), 3000, 4.0),
        ],
        all_raw_rows(
            scanner.as_ref(),
            &ctx,
            &metrics,
            &binding,
            SeriesOrdering::KeyThenTime
        )
        .await
    );
    assert!(
        scanner
            .prepare_series(&ctx, &metrics, &binding)
            .await
            .unwrap()
            .is_none(),
        "legacy artifacts have no native subset to prepare"
    );
    engine.stop().await.unwrap();
}

/// A columnar region with no coverage at all answers with an empty native stream.
///
/// The gate is on, so the raw entry never reads the ordinary scan: the SST rows are not served
/// before their artifacts exist. The empty answer still declares the planned schema, label
/// representation and ordering, so planning and execution agree in both states.
#[tokio::test]
async fn raw_scan_without_ready_sources_returns_an_empty_native_stream() {
    let (_env, engine, region) = fixture().await;
    // The fixture drops the published snapshot, so no assigned source is columnar-ready.
    write(&engine, &region, 0, 3000, Some(4.0)).await;

    let scanner = engine
        .handle_query(region.region_id, per_series_request())
        .await
        .unwrap();
    let binding = labels(scanner.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let declared = scanner.raw_series_schema(&binding).unwrap().unwrap();
    let ordering = scanner.raw_series_ordering().unwrap();
    assert_eq!(SeriesOrdering::KeyThenTime, ordering);
    assert!(
        scanner.raw_series_has_explicit_labels(&binding).unwrap(),
        "a gated region plans explicit labels before anything is ready"
    );
    let stream = scanner
        .scan_series_partition(&ctx, &metrics, 0, &binding)
        .unwrap()
        .expect("the raw entry always answers once the gate is on");
    assert_eq!(ordering, stream.ordering());
    assert_eq!(declared.fields, stream.schema().fields);
    assert!(stream.schema().eval_grid.is_none());
    assert!(same_labels(stream.label_source(), &binding));
    // The ordinary scan would have returned the two SST rows plus the memtable row.
    assert!(
        stream.try_collect::<Vec<_>>().await.unwrap().is_empty(),
        "an unbuilt region must not serve ordinary scan rows"
    );
    assert_eq!(
        0,
        scanner
            .prepare_series(&ctx, &metrics, &binding)
            .await
            .unwrap()
            .expect("the gate is on, so preparation never declines")
            .num_series()
    );
    engine.stop().await.unwrap();
}

/// After publication the same query returns exactly the native rows of the covered sources.
///
/// The memtable row written before the build is still skipped, and the label representation the
/// scan declared while it was empty is the one it keeps once rows exist.
#[tokio::test]
async fn raw_scan_returns_exactly_the_native_rows_after_publication() {
    let (_env, engine, region) = fixture().await;
    write(&engine, &region, 0, 3000, Some(4.0)).await;

    let scanner = engine
        .handle_query(region.region_id, per_series_request())
        .await
        .unwrap();
    let binding = labels(scanner.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let declared = scanner.raw_series_schema(&binding).unwrap().unwrap();
    assert!(
        scanner.raw_series_has_explicit_labels(&binding).unwrap(),
        "the plan declares explicit labels before the first build too"
    );
    drop(scanner);

    build(&engine, &region).await;
    let scanner = engine
        .handle_query(region.region_id, per_series_request())
        .await
        .unwrap();
    let binding = labels(scanner.metadata());
    assert!(scanner.raw_series_has_explicit_labels(&binding).unwrap());
    assert_eq!(
        declared.fields,
        scanner.raw_series_schema(&binding).unwrap().unwrap().fields
    );
    assert_eq!(
        vec![
            (series_key(10, 0), 1000, 1.0),
            (series_key(10, 0), 2000, 1.0),
        ],
        all_raw_rows(
            scanner.as_ref(),
            &ctx,
            &metrics,
            &binding,
            SeriesOrdering::KeyThenTime
        )
        .await,
        "the native rows of the covered SSTs, never the memtable row"
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn both_preflights_report_a_malformed_assignment_after_an_unready_one() {
    use crate::read::range::RowGroupIndex;
    use crate::sst::file::FileHandle;
    use crate::sst::file_purger::NoopFilePurger;

    let (_env, engine, region) = fixture().await;
    let make_input = || {
        scan_input(
            &region,
            region.series_index_store.clone().unwrap(),
            SelectedFormat::Columnar,
            (0..region.version().metadata.column_metadatas.len()).collect(),
        )
    };
    // Without a build every assigned SST is unready, so readiness alone cannot answer.
    let mut unready = StreamContext::seq_scan_ctx(make_input());
    let ranges = unready.partition_ranges();
    assert!(!validate_inputs(&unready, &ranges).unwrap());
    // An unknown range identifier appended after the unready assignments is still an error.
    let mut unknown = ranges.clone();
    unknown.push(PartitionRange {
        identifier: ranges[0].identifier + ranges.len(),
        ..ranges[0]
    });
    assert!(
        select_inputs(&unready, &unknown)
            .unwrap_err()
            .to_string()
            .contains("invalid prepared scan range")
    );
    // An extension range closing the same assignment is rejected as well.
    let last = *ranges.last().unwrap();
    unready.ranges[last.identifier].row_group_indices = vec![RowGroupIndex {
        index: unready.input.num_files(),
        row_group_index: 0,
    }]
    .into();
    assert!(
        select_inputs(&unready, &unready.partition_ranges())
            .unwrap_err()
            .to_string()
            .contains("extension ranges")
    );

    // Both entrypoints reject a foreign SST that closes the assignment instead of answering
    // readiness for the unready sources in front of it.
    let mut input = make_input();
    let mut foreign = input.files[0].meta_ref().clone();
    foreign.region_id = store_api::storage::RegionId::new(123, 456);
    input.files.remove(0);
    input
        .files
        .push(FileHandle::new(foreign, Arc::new(NoopFilePurger)));
    let mut scan = SeriesScan::new(input, false);
    scan.prepare(PrepareRequest::default()).unwrap();
    let binding = labels(scan.metadata());
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let native = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .err()
        .expect("the native preflight must report structural corruption");
    assert!(native.output_msg().contains("foreign SST"), "{native}");
    assert!(
        scan.raw_series_schema(&binding)
            .unwrap_err()
            .to_string()
            .contains("foreign SST")
    );
    assert!(
        scan.scan_series_partition(&ctx, &metrics, 0, &binding)
            .err()
            .expect("the raw preflight must report structural corruption")
            .to_string()
            .contains("foreign SST")
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn grouped_samples_serve_rows_of_a_source_without_a_series_index() {
    use crate::sst::file::FileHandle;

    let (_env, engine, region) = fixture().await;
    // A later bucket in the same UTC day shares the day's grouped sample artifact with the older
    // sources.
    write(&engine, &region, 0, 200_000, Some(2.0)).await;
    flush_region(&engine, region.region_id, None).await;
    // This flush also triggers a build of its own partial day group in the background. Drain it,
    // then rebuild from a clean snapshot so day zero keeps one grouped artifact.
    build(&engine, &region).await;
    region.series_index_version_control.publish(Arc::default());
    build(&engine, &region).await;
    let unindexed = file_id_at(&region, 200_000);
    let original = region.series_index_version();
    assert_eq!(
        4,
        original
            .samples
            .values()
            .next()
            .unwrap()
            .entry()
            .source_file_ids
            .len()
    );
    assert_eq!(1, original.series_indexes.len());

    let mut input = scan_input(
        &region,
        region.series_index_store.clone().unwrap(),
        SelectedFormat::Columnar,
        (0..region.version().metadata.column_metadatas.len()).collect(),
    );
    // A source whose sequence the published bucket cannot prove is not covered by it, so this
    // source has no covering series index while the grouped artifact still lists it.
    let mut files = input.files.clone();
    for file in &mut files {
        if file.file_id().file_id() == unindexed {
            let mut meta = file.meta_ref().clone();
            meta.sequence = None;
            *file = FileHandle::new(meta, region.file_purger.clone());
        }
    }
    input.files = files;
    let mut scan = SeriesScan::new(input, false);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    let selected = selection(&scan);
    assert!(!selected.all_ready);
    assert_eq!(3, selected.ready_files.len());
    assert_eq!(1, selected.series_indexes.len());
    assert_eq!(1, selected.sample_artifacts.len());

    let binding = labels(scan.metadata());
    let source = scan
        .prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &binding,
        )
        .await
        .unwrap()
        .expect("the covering source keeps the grouped artifact eligible");
    // The 200_000 row belongs to the unindexed source and is still a live row of the discovered
    // series: readiness must never narrow `allowed` to the ready files.
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0), (200_000, 2.0)],
        prepared_samples(&source).await
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn replaced_source_invalidates_the_grouped_artifact() {
    const LATER: i64 = 20_000 * 86_400_000;

    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    // The later day gives the surviving source its own artifact and its own index bucket.
    write(&engine, &region, 0, LATER, Some(2.0)).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    let original = region.series_index_version();
    let day_zero = original
        .samples
        .values()
        .find(|handle| handle.entry().window == Some(0))
        .unwrap()
        .clone();
    let day_zero_paths = day_zero
        .entry()
        .artifact
        .objects
        .iter()
        .map(|object| object.path.clone())
        .collect::<Vec<_>>();
    let replaced = file_id_at(&region, 1000);
    let (store, reads) = recording_store(&region);
    let mut input = scan_input(
        &region,
        store,
        SelectedFormat::Columnar,
        (0..region.version().metadata.column_metadatas.len()).collect(),
    );
    // Replacing an SST removes the old file id from the snapshot while the grouped artifact still
    // lists it, so the whole affected generation must stop being eligible.
    input
        .files
        .retain(|file| file.file_id().file_id() != replaced);
    let mut scan = SeriesScan::new(input, false);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    let selected = selection(&scan);
    assert!(!selected.all_ready);
    // Only the separately covered source survives the replacement.
    assert_eq!(1, selected.ready_files.len());
    assert_eq!(1, selected.sample_artifacts.len());
    assert!(
        !selected
            .sample_artifacts
            .contains(&day_zero.file_id().file_id())
    );

    let binding = labels(scan.metadata());
    let source = scan
        .prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &binding,
        )
        .await
        .unwrap()
        .expect("the separately covered source stays served");
    assert_eq!(vec![(LATER, 2.0)], prepared_samples(&source).await);
    // The invalidated generation was never opened.
    assert!(
        reads
            .lock()
            .unwrap()
            .iter()
            .all(|(path, _)| !day_zero_paths.contains(path)),
        "the invalidated grouped artifact must not be read"
    );
    engine.stop().await.unwrap();
}

/// Compaction publishes SSTs that no artifact covers and removes the sources the published
/// artifacts were built from, so it must trigger the series-index rebuild that ingestion and flush
/// already trigger. Periodic maintenance is an hour away, so only that trigger can answer here.
#[tokio::test]
async fn compaction_triggers_series_index_build() {
    use api::v1::region::{StrictWindow, compact_request};
    use store_api::region_request::RegionCompactRequest;

    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let request = ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        ..Default::default()
    };

    // The three flushed SSTs are covered, so the native entry point serves their rows.
    let before = {
        let raw_scanner = engine
            .handle_query(region.region_id, request.clone())
            .await
            .unwrap();
        let binding = labels(raw_scanner.metadata());
        let source = raw_scanner
            .prepare_series(&ctx, &metrics, &binding)
            .await
            .unwrap()
            .expect("a covered region prepares a source");
        prepared_samples(&source).await
    };
    assert_eq!(vec![(1000, 1.0), (2000, 1.0)], before);

    // The published coverage names the three SSTs the compaction is about to replace.
    let stale = region.series_index_version();
    let stale_sources = stale
        .samples
        .values()
        .flat_map(|handle| handle.entry().source_file_ids.iter().copied())
        .collect::<HashSet<_>>();
    assert_eq!(3, stale_sources.len());
    let stale_artifacts = stale
        .samples
        .values()
        .map(|handle| handle.file_id().file_id())
        .collect::<HashSet<_>>();
    let stale_paths = stale
        .samples
        .values()
        .flat_map(|handle| {
            handle
                .entry()
                .artifact
                .objects
                .iter()
                .map(|object| object.path.clone())
        })
        .collect::<Vec<_>>();

    // A real in-process compaction rewrites all three SSTs into one output file.
    engine
        .handle_request(
            region.region_id,
            RegionRequest::Compact(RegionCompactRequest {
                options: compact_request::Options::StrictWindow(StrictWindow {
                    window_seconds: 3600,
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let compacted = region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .map(|file| file.file_id().file_id())
        .collect::<Vec<_>>();
    assert_eq!(
        1,
        compacted.len(),
        "the compaction must rewrite the three SSTs"
    );
    assert!(compacted.iter().all(|id| !stale_sources.contains(id)));

    // Before the hourly maintenance pass, only the compaction trigger can publish, so waiting for
    // coverage of the output file proves the trigger ran instead of a periodic reconcile.
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let covered = {
                let version = region.series_index_version();
                compacted.iter().all(|id| {
                    version
                        .samples
                        .values()
                        .any(|handle| handle.entry().covers(*id))
                })
            };
            if covered {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("compaction must trigger a series-index build instead of waiting for maintenance");

    // Without waiting any longer, the native entry point serves the rows of the output file.
    let raw_scanner = engine
        .handle_query(region.region_id, request)
        .await
        .unwrap();
    let binding = labels(raw_scanner.metadata());
    let served = raw_scanner
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the compaction output must be prepared");
    assert_eq!(before, prepared_samples(&served).await);

    // The answer never comes from the stale pre-compaction artifact: the rebuilt coverage replaces
    // that generation, and no read of this scan opens its objects.
    let (store, reads) = recording_store(&region);
    let mut scan = scanner(&region, store);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    let selected = selection(&scan);
    assert_eq!(vec![0], selected.ready_files);
    assert!(selected.all_ready);
    assert!(
        selected
            .sample_artifacts
            .iter()
            .all(|id| !stale_artifacts.contains(id)),
        "the stale pre-compaction artifact must not be selected"
    );
    let binding = labels(scan.metadata());
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("the post-compaction coverage prepares a source");
    assert_eq!(before, prepared_samples(&source).await);
    assert!(
        reads
            .lock()
            .unwrap()
            .iter()
            .all(|(path, _)| !stale_paths.contains(path)),
        "the stale pre-compaction artifact must not be read"
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn time_pruned_group_source_stays_eligible() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    // Bounds [1500, 2500) drop the two sources at 1000 from the assignment.
    let pruned = region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .filter(|file| file.meta_ref().time_range.0 < common_time::Timestamp::new_millisecond(1500))
        .map(|file| file.file_id().file_id())
        .collect::<HashSet<_>>();
    assert_eq!(2, pruned.len());
    let metadata = region.version().metadata.clone();
    let make_input = |visible: bool| {
        let mut input = scan_input(
            &region,
            region.series_index_store.clone().unwrap(),
            SelectedFormat::Columnar,
            (0..metadata.column_metadatas.len()).collect(),
        );
        input.time_range = common_time::range::TimestampRange::new(
            common_time::Timestamp::new_millisecond(1500),
            common_time::Timestamp::new_millisecond(2500),
        );
        input.predicate = PredicateGroup::new(
            &metadata,
            &[
                col("tag_0").eq(lit("a")),
                col("field_0").gt(lit(0.0)),
                col("ts").gt_eq(lit(datafusion_common::ScalarValue::TimestampMillisecond(
                    Some(1500),
                    None,
                ))),
            ],
        )
        .unwrap();
        // The pruner drops the older sources from the assignment; only the real request also
        // records them as excluded by time alone rather than by sequencing.
        input
            .files
            .retain(|file| !pruned.contains(&file.file_id().file_id()));
        if visible {
            input.time_pruned_files = pruned.clone();
        }
        input
    };
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();

    // The pruner keeps the older sources visible while only the newer one is assigned.
    let mut scan = SeriesScan::new(make_input(true), false);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    let selected = selection(&scan);
    assert_eq!(1, selected.ready_files.len());
    // The grouped artifact carries two unassigned (time-pruned) sources, so it is eligible
    // through `time_pruned_files` alone.
    assert_eq!(1, selected.sample_artifacts.len());
    let binding = labels(scan.metadata());
    let source = scan
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("a time-pruned source keeps the group eligible");
    assert_eq!(vec![(2000, 1.0)], prepared_samples(&source).await);

    // Control: without those sources the same assignment has no complete coverage at all.
    let mut control = SeriesScan::new(make_input(false), false);
    control
        .prepare(
            PrepareRequest::default().with_ranges(vec![control.properties.partitions[0].clone()]),
        )
        .unwrap();
    let control_selected = selection(&control);
    assert!(control_selected.ready_files.is_empty());
    assert!(control_selected.sample_artifacts.is_empty());
    let binding = labels(control.metadata());
    let empty = control
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .expect("an uncovered gated assignment prepares an empty source");
    assert_eq!(0, empty.num_series());
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn ready_inputs_without_discovered_series_yield_an_empty_source() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    let metadata = region.version().metadata.clone();
    let mut input = scan_input(
        &region,
        region.series_index_store.clone().unwrap(),
        SelectedFormat::Columnar,
        (0..metadata.column_metadatas.len()).collect(),
    );
    // No series carries this label, so discovery is empty while every input stays ready.
    input.predicate = PredicateGroup::new(&metadata, &[col("tag_0").eq(lit("absent"))]).unwrap();
    let mut scan = SeriesScan::new(input, false);
    scan.prepare(
        PrepareRequest::default().with_ranges(vec![scan.properties.partitions[0].clone()]),
    )
    .unwrap();
    assert!(selection(&scan).all_ready);

    let binding = labels(scan.metadata());
    let source = scan
        .prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &binding,
        )
        .await
        .unwrap()
        .expect("ready inputs are served even without a discovered series");
    assert_eq!(0, source.num_series());
    assert!(same_labels(source.label_source(), &binding));
    let stream = source.read_series(0..0).unwrap();
    assert_eq!(SeriesOrdering::KeyThenTime, stream.ordering());
    assert!(same_labels(stream.label_source(), &binding));
    let declared = scan.raw_series_schema(&binding).unwrap().unwrap();
    assert_eq!(declared.fields, stream.schema().fields);
    assert!(stream.try_collect::<Vec<_>>().await.unwrap().is_empty());
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn captured_selection_is_not_broadened_by_publication() {
    let (_env, engine, region) = fixture().await;
    build(&engine, &region).await;
    let (store, _reads) = recording_store(&region);
    let mut pinned = scanner(&region, store.clone());
    pinned
        .prepare(
            PrepareRequest::default().with_ranges(vec![pinned.properties.partitions[0].clone()]),
        )
        .unwrap();
    let pinned_samples = region
        .series_index_version()
        .samples
        .keys()
        .copied()
        .collect::<HashSet<_>>();

    // Publish a new source and a new grouped artifact while the first scanner stays pinned.
    write(&engine, &region, 0, 3000, Some(5.0)).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;
    let published = region.series_index_version();
    assert_ne!(
        pinned_samples,
        published.samples.keys().copied().collect::<HashSet<_>>()
    );

    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let binding = labels(pinned.metadata());
    let selected = selection(&pinned);
    assert!(selected.all_ready);
    assert_eq!(3, selected.ready_files.len());
    assert_eq!(pinned_samples, selected.sample_artifacts);
    let source = pinned
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0)],
        prepared_samples(&source).await
    );
    // The captured selection still describes the pinned version only.
    assert_eq!(pinned_samples, selection(&pinned).sample_artifacts);

    // A scanner created after publication sees the new version.
    let mut fresh = scanner(&region, store);
    fresh
        .prepare(
            PrepareRequest::default().with_ranges(vec![fresh.properties.partitions[0].clone()]),
        )
        .unwrap();
    let binding = labels(fresh.metadata());
    let source = fresh
        .prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        vec![(1000, 1.0), (2000, 1.0), (3000, 5.0)],
        prepared_samples(&source).await
    );
    engine.stop().await.unwrap();
}
