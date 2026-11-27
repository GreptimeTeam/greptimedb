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
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_expr::{col, lit};
use datatypes::prelude::ConcreteDataType;
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SampleRef;
use std::collections::HashSet;
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_engine::{
    BoundTag, PrepareRequest, QueryScanContext, RegionEngine, RegionScanner,
};
use store_api::region_request::{
    RegionBuildIndexRequest, RegionDeleteRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{ScanRequest, TimeSeriesDistribution};

use super::*;
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::scan_region::{PredicateGroup, ScanInput};
use crate::read::series_scan::SeriesScan;
use crate::region::MitoRegionRef;
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use crate::test_util::{TestEnv, flush_region};

#[test]
fn narthex_partition_filter_ignores_empty_and_matching_rules() {
    // Frontend-created unpartitioned regions persist Some("") rather than None.
    assert!(narthex_partition_filter(None, None).unwrap().is_none());
    assert!(narthex_partition_filter(Some(""), None).unwrap().is_none());
    let rule = partition::expr::col("tag").eq(Value::from("api"));
    let json = rule.as_json_str().unwrap();
    assert!(
        narthex_partition_filter(Some(&json), Some(&rule))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        narthex_partition_filter(Some(&json), None).unwrap(),
        Some(rule)
    );
    assert!(narthex_partition_filter(Some("invalid"), None).is_err());
}

async fn fixture() -> (TestEnv, MitoEngine, MitoRegionRef) {
    indexed_fixture("last_row").await
}

async fn indexed_fixture(merge_mode: &str) -> (TestEnv, MitoEngine, MitoRegionRef) {
    let (env, engine, region) = authoritative_fixture(merge_mode).await;
    for ts in [1000, 1000, 2000] {
        write(&engine, &region, 0, ts, Some(1.0)).await;
        flush_region(&engine, region.region_id, None).await;
    }
    (env, engine, region)
}

async fn parquet_fixture() -> (TestEnv, MitoEngine, MitoRegionRef) {
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
    let request = if let Some(value) = value {
        crate::test_util::sparse_metric_put(primary_key, timestamp, Some(value))
    } else {
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
        ];
        let values = vec![
            ValueData::BinaryValue(primary_key),
            ValueData::TimestampMillisecondValue(timestamp),
        ];
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

fn scanner(region: &MitoRegionRef) -> SeriesScan {
    SeriesScan::new(scan_input(
        region,
        true,
        (0..region.metadata().column_metadatas.len()).collect(),
    ))
}

fn scan_input(region: &MitoRegionRef, use_indexes: bool, projection: Vec<usize>) -> ScanInput {
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
        .with_merge_mode(version.options.merge_mode())
        .with_narthex_format(version.options.sst_format == Some(crate::sst::FormatType::Narthex))
        .with_narthex_series_indexes(if use_indexes {
            version.ssts.series_indexes().values().cloned().collect()
        } else {
            vec![]
        })
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
                    {
                        let (table, tsid) =
                            mito_codec::row_converter::SparsePrimaryKeyCodec::schemaless()
                                .decode_ids(chunk.primary_key.as_slice())
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
async fn parquet_raw_scan_preserves_key_then_time_order() {
    let (_env, engine, region) = parquet_fixture().await;
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
    // The residual field predicate must buffer past its 4096-row batch boundary while
    // retaining a single series' ordered stream.
    for offset in 0..4_100 {
        write(&engine, &region, 0, 50_000 + offset, Some(1.0)).await;
    }
    flush_region(&engine, region.region_id, None).await;

    let mut scanner = SeriesScan::new(scan_input(
        &region,
        true,
        (0..region.version().metadata.column_metadatas.len()).collect(),
    ));
    assert_eq!("legacy", scanner.mode.as_str());
    let binding = labels(scanner.metadata());
    assert_eq!(
        SeriesOrdering::KeyThenTime,
        scanner.raw_series_ordering().unwrap()
    );
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    assert!(
        scanner
            .prepare_series(&ctx, &metrics, &binding)
            .await
            .unwrap()
            .is_none()
    );
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

    let mut owners = std::collections::HashMap::new();
    for partition in 0..2 {
        let stream = scanner
            .scan_series_partition(&ctx, &metrics, partition, &binding)
            .unwrap()
            .unwrap();
        assert_eq!(SeriesOrdering::KeyThenTime, stream.ordering());
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
            assert!(
                owners
                    .insert(key.clone(), partition)
                    .is_none_or(|owner| owner == partition)
            );
            previous_key = Some(key);
        }
    }
    assert_eq!(4, owners.len());
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
    let make_scan = |use_indexes| {
        let mut input = scan_input(
            &region,
            use_indexes,
            (0..region.version().metadata.column_metadatas.len()).collect(),
        );
        // Bounds in a finer unit must round up for both inclusive start and exclusive end.
        input.time_range = TimestampRange::new(
            Timestamp::new_microsecond(1_000_001),
            Timestamp::new_microsecond(1_600_001),
        );
        SeriesScan::new(input)
    };
    let samples_only = make_scan(false);
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let oracle = samples_only
        .scan_series_partition(&ctx, &metrics, 0, &labels(samples_only.metadata()))
        .unwrap()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let scan = make_scan(true);
    scan.prepare_series(&ctx, &metrics, &labels(scan.metadata()))
        .await
        .unwrap()
        .unwrap();
    let source = scan.prepared.source.get().unwrap().as_ref().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let source = ColumnarPreparedSource {
        dedup_append_raw: false,
        sample_partition: None,
        scan_pool: source.scan_pool.clone(),
        merge_field_start: 0,
        context: source.context.clone(),
        labels: source.labels.clone(),
        raw: source.raw.clone(),
        sample_schema: source.sample_schema.clone(),
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
        // Selection is exact and key-only: the reader derives its coarse bounds from it.
        let keys = source
            .candidates
            .iter()
            .map(|c| c.key.to_vec())
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
    for use_indexes in [false, true] {
        let mut input = scan_input(
            &region,
            use_indexes,
            (0..region.version().metadata.column_metadatas.len()).collect(),
        );
        input.predicate =
            PredicateGroup::new(&region.version().metadata, std::slice::from_ref(&predicate))
                .unwrap();
        let scanner = SeriesScan::new(input);
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
        if use_indexes {
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

#[rstest::rstest]
#[case::last_row(false)]
#[case::last_non_null(true)]
#[tokio::test]
async fn identity_merge_preserves_tag_and_mixed_predicates(#[case] last_non_null: bool) {
    let mode = if last_non_null {
        "last_non_null"
    } else {
        "last_row"
    };
    let (_env, engine, region) = indexed_fixture(mode).await;
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
        for use_indexes in [false, true] {
            let mut input = scan_input(
                &region,
                use_indexes,
                (0..region.version().metadata.column_metadatas.len()).collect(),
            );
            input.predicate =
                PredicateGroup::new(&region.version().metadata, std::slice::from_ref(&predicate))
                    .unwrap();
            let scanner = SeriesScan::new(input);
            let binding = labels(scanner.metadata());
            let schema = scanner.raw_series_schema(&binding).unwrap().unwrap();
            let explicit = scanner.raw_series_has_explicit_labels(&binding).unwrap();
            assert_eq!(identity_only, explicit);
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
            if use_indexes {
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
    assert!(region.version().ssts.series_indexes().len() >= 2);
    let scan = scanner(&region);
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
    assert!(region.version().ssts.series_indexes().len() >= 2);
    let mut input = scan_input(
        &region,
        true,
        (0..region.version().metadata.column_metadatas.len()).collect(),
    );
    if mixed {
        input.predicate = PredicateGroup::new(
            &region.version().metadata,
            &[col("tag_0").eq(lit("a")).or(col("field_0").gt(lit(0.0)))],
        )
        .unwrap();
    }
    let scan = SeriesScan::new(input);
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
        .map(|candidate| candidate.key.to_vec())
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
            true,
            (0..metadata.column_metadatas.len()).collect(),
        );
        // Replace the fixture's default predicate, which would hide both hosts.
        input.predicate = PredicateGroup::new(&metadata, &predicates).unwrap();
        if let Some(range) = range {
            input.time_range = Some(range);
        }
        SeriesScan::new(input)
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
        true,
        (0..metadata.column_metadatas.len()).collect(),
    );
    // The fixture's default predicate would hide this series.
    input.predicate = PredicateGroup::new(&metadata, &[]).unwrap();
    let scan = SeriesScan::new(input);
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();
    let binding = labels(scan.metadata());
    scan.prepare_series(&ctx, &metrics, &binding)
        .await
        .unwrap()
        .unwrap();
    let source = scan.prepared.source.get().unwrap().as_ref().unwrap();
    // The fixture's own series prepare beside this one; the long key must survive preparation
    // byte for byte and own its samples. The identity is the stored key itself, not a
    // region-qualified form, so fragments of a series in other regions report the same one.
    let ordinal = (0..source.num_series())
        .find(|ordinal| {
            source.series_info(*ordinal).unwrap().primary_key.as_ref() as &[u8] == key.as_slice()
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

/// A label binding that carries the same labels over a distinct pinned metadata snapshot.
fn rebuilt_binding(binding: &RawLabelSource) -> RawLabelSource {
    let RawLabelSource::PrimaryKey {
        metric_name,
        physical_metadata,
        logical_tags,
    } = binding
    else {
        unreachable!("the fixture binds a primary key source");
    };
    RawLabelSource::PrimaryKey {
        metric_name: metric_name.clone(),
        physical_metadata: Arc::new((**physical_metadata).clone()),
        logical_tags: logical_tags.clone(),
    }
}

/// One execution asks the bound scanner for the same preflight from every output partition, in the
/// order `SeriesScanExec::execute` uses: `raw_series_schema`, then
/// `raw_series_has_explicit_labels`, then `scan_series_partition`. All three validate the same
/// pinned state and label binding, so the whole execution must run the preflight once, not once
/// per partition per call.
#[tokio::test]
async fn preflight_runs_once_for_every_partition_of_one_execution() {
    let (_env, engine, region) = fixture().await;
    write(&engine, &region, 0, 1000, Some(1.0)).await;
    write(&engine, &region, 0, 2000, Some(1.0)).await;
    write(&engine, &region, 1, 1000, Some(2.0)).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;

    let mut scanner = scanner(&region);
    let binding = labels(scanner.metadata());
    let ranges = scanner.properties().partitions[0].clone();
    scanner
        .prepare(PrepareRequest::default().with_ranges(vec![ranges, vec![], vec![]]))
        .unwrap();
    let partitions = scanner.properties().num_partitions();
    assert!(
        partitions > 1,
        "several partitions are what makes repeated preflights observable"
    );
    let ctx = QueryScanContext::default();
    let metrics = ExecutionPlanMetricsSet::new();

    for partition in 0..partitions {
        assert!(scanner.raw_series_schema(&binding).unwrap().is_some());
        assert!(scanner.raw_series_has_explicit_labels(&binding).unwrap());
        let stream = scanner
            .scan_series_partition(&ctx, &metrics, partition, &binding)
            .unwrap()
            .expect("the fixture prepares its columnar sources");
        stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            1,
            scanner.preflight_runs(),
            "partition {partition} re-ran a preflight the execution already answered"
        );
    }
    engine.stop().await.unwrap();
}

/// The memoized preflight must not outlive the state it was derived from, and it must never answer
/// for another binding: the preflight validates the pinned metadata snapshot, so a look-alike
/// binding is still rejected on every call.
#[tokio::test]
async fn reset_state_and_other_bindings_drop_the_preflight_cache() {
    let (_env, engine, region) = fixture().await;
    write(&engine, &region, 0, 1000, Some(1.0)).await;
    flush_region(&engine, region.region_id, None).await;
    build(&engine, &region).await;

    let mut scanner = scanner(&region);
    let binding = labels(scanner.metadata());
    assert!(scanner.raw_series_schema(&binding).unwrap().is_some());
    assert_eq!(1, scanner.preflight_runs());
    // An unchanged binding is served from the cache.
    assert!(scanner.raw_series_schema(&binding).unwrap().is_some());
    assert_eq!(1, scanner.preflight_runs());

    // A look-alike binding is not a cache hit, and its rejection is not cached either.
    let look_alike = rebuilt_binding(&binding);
    assert!(scanner.raw_series_schema(&look_alike).is_err());
    assert_eq!(2, scanner.preflight_runs());
    assert!(scanner.raw_series_schema(&look_alike).is_err());
    assert_eq!(3, scanner.preflight_runs());

    // `reset_state` rewrites the state the preflight read, so it must drop the cached answer.
    scanner.reset_state();
    assert!(scanner.raw_series_schema(&binding).unwrap().is_some());
    assert_eq!(
        4,
        scanner.preflight_runs(),
        "reset_state must drop the memoized preflight"
    );
    assert!(scanner.raw_series_schema(&binding).unwrap().is_some());
    assert_eq!(4, scanner.preflight_runs());

    // `prepare` rewrites the partition assignment the preflight read as well.
    scanner.prepare(PrepareRequest::default()).unwrap();
    assert!(scanner.raw_series_schema(&binding).unwrap().is_some());
    assert_eq!(
        5,
        scanner.preflight_runs(),
        "prepare must drop the memoized preflight"
    );
    engine.stop().await.unwrap();
}

/// A dynamic filter joins the predicate group the preflight inspects, and the preflight rejects
/// every dynamic filter because a fresh execution cannot promise to keep its semantics. An
/// execution that already holds an answer must therefore recompute it once a filter is added:
/// replaying the cached preparation would keep sending rows through the raw path that ignores the
/// new predicate.
#[tokio::test]
async fn added_dynamic_filters_drop_the_preflight_cache() {
    use datafusion::physical_plan::expressions::{DynamicFilterPhysicalExpr, lit as physical_lit};

    let (_env, engine, region) = parquet_fixture().await;
    let mut scanner = scanner(&region);
    let binding = labels(scanner.metadata());
    // The fixture's static filters pass the preflight, and the second ask is served from the cache
    // the first one filled.
    assert!(scanner.raw_series_schema(&binding).unwrap().is_some());
    assert!(scanner.raw_series_schema(&binding).unwrap().is_some());

    // Only the presence of a dynamic filter matters to the preflight, not its contents.
    let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![], physical_lit(true)));
    assert_eq!(
        vec![true],
        scanner.add_dyn_filter_to_predicate(vec![dynamic_filter])
    );

    // The cached answer no longer describes the scan's predicate, so asking again must reject the
    // raw path instead of replaying the preparation that ignored the filter.
    assert!(
        scanner.raw_series_schema(&binding).unwrap().is_none(),
        "an added dynamic filter must reach the next preflight"
    );
    assert!(!scanner.raw_series_has_explicit_labels(&binding).unwrap());
    assert!(
        scanner
            .prepare_series(
                &QueryScanContext::default(),
                &ExecutionPlanMetricsSet::new(),
                &binding,
            )
            .await
            .unwrap()
            .is_none(),
        "the ordinary scan covers a scan that added a dynamic filter"
    );
    engine.stop().await.unwrap();
}

#[rstest::rstest]
#[case::raw_bulk(false, false, "narthex")]
#[case::raw_parquet(false, false, "flat")]
#[case::encoded_bulk(true, false, "narthex")]
#[case::append_bulk(false, true, "narthex")]
#[case::append_raw_parquet(false, true, "flat")]
#[case::append_encoded_bulk(true, true, "narthex")]
#[tokio::test]
async fn authoritative_narthex_preparation_without_sidecars(
    #[case] encoded: bool,
    #[case] append: bool,
    #[case] format: &str,
) {
    let mut env = TestEnv::with_prefix("narthex-prepared-authoritative").await;
    let engine = env
        .create_engine(MitoConfig {
            experimental_enable_series_index: false,
            ..Default::default()
        })
        .await;
    let mut metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    metadata
        .column_metadatas
        .iter_mut()
        .find(|column| column.column_id == 2)
        .unwrap()
        .column_schema
        .data_type = ConcreteDataType::float64_datatype();
    let mut create = crate::test_util::CreateRequestBuilder::new()
        .insert_option("primary_key_encoding", "sparse")
        .insert_option("sst_format", format)
        .insert_option("memtable.type", "bulk")
        .build();
    create.column_metadatas = metadata.column_metadatas;
    create.primary_key = metadata.primary_key;
    // Encoded writes must work and raw writes must be rejected regardless of metadata order.
    create.primary_key[2..].reverse();
    create
        .options
        .insert("append_mode".into(), append.to_string());
    if encoded {
        for (key, value) in [
            ("memtable.bulk.merge_threshold", "2"),
            ("memtable.bulk.encode_row_threshold", "1"),
            ("memtable.bulk.encode_bytes_threshold", "1"),
        ] {
            create.options.insert(key.into(), value.into());
        }
    }
    let id = store_api::storage::RegionId::new(42, 0);
    let raw_schema = crate::test_util::rows_schema(&create);
    let reopen = create.clone();
    engine
        .handle_request(id, RegionRequest::Create(create))
        .await
        .unwrap();
    let region = engine.get_region(id).unwrap();
    let raw_count = 1;
    if encoded {
        write(&engine, &region, 7, 1000, Some(10.0)).await;
    } else {
        let values = raw_schema
            .iter()
            .map(|column| match column.column_name.as_str() {
                "__table_id" => ValueData::U32Value(10),
                "__tsid" => ValueData::U64Value(7),
                "tag_0" => ValueData::StringValue("a".into()),
                "tag_1" => ValueData::StringValue("x".into()),
                "ts" => ValueData::TimestampMillisecondValue(1000),
                "field_0" => ValueData::F64Value(10.0),
                other => panic!("unexpected column {other}"),
            })
            .collect::<Vec<_>>();
        let error = engine
            .handle_request(
                id,
                RegionRequest::Put(store_api::region_request::RegionPutRequest {
                    rows: Rows {
                        schema: raw_schema.clone(),
                        rows: vec![row(values)],
                    },
                    hint: None,
                    partition_expr_version: None,
                    skip_wal: false,
                }),
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("sparse regions require pre-encoded primary keys"),
            "{error}"
        );
        engine.stop().await.unwrap();
        return;
    }
    write_with_tags(&engine, &region, 7, 1000, Some(20.0), &["b", "x"]).await;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    let pool = Arc::new(GreedyMemoryPool::new(32 * 1024 * 1024));
    let ctx = QueryScanContext {
        memory_pool: pool.clone(),
        ..Default::default()
    };
    let metrics = ExecutionPlanMetricsSet::new();
    for stage in 0..3 {
        if stage == 1 {
            flush_region(&engine, id, None).await;
            crate::test_util::reopen_region(
                &engine,
                id,
                reopen.table_dir.clone(),
                true,
                reopen.options.clone(),
            )
            .await;
        }
        if stage == 2 {
            write(&engine, &region, 7, 1000, Some(1.0)).await;
        }
        let request = ScanRequest {
            distribution: Some(TimeSeriesDistribution::PerSeries),
            filters: vec![col("field_0").gt(lit(5.0))],
            ..Default::default()
        };
        let constrained = engine.handle_query(id, request.clone()).await.unwrap();
        let limited = QueryScanContext {
            memory_pool: Arc::new(GreedyMemoryPool::new(1)),
            ..Default::default()
        };
        assert!(
            constrained
                .prepare_series(&limited, &metrics, &labels(constrained.metadata()))
                .await
                .is_err()
        );
        drop(constrained);
        assert_eq!(limited.memory_pool.reserved(), 0);
        let scan = engine.handle_query(id, request.clone()).await.unwrap();
        let binding = labels(scan.metadata());
        let prepared = scan
            .prepare_series(&ctx, &metrics, &binding)
            .await
            .unwrap()
            .expect("narthex reads must prepare without sidecars, including live memory");
        assert!(pool.reserved() > 0);
        let mut cancelled = prepared.read_series(0..prepared.num_series()).unwrap();
        assert!(cancelled.try_next().await.unwrap().is_some());
        drop(cancelled);
        let chunks = prepared
            .read_series(0..prepared.num_series())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut actual = samples(&chunks);
        actual.sort_by(|a, b| a.1.total_cmp(&b.1));
        let expected = if stage < 2 {
            vec![(1000, 10.0), (1000, 20.0)]
        } else {
            vec![(1000, 20.0)]
        };
        assert_eq!(expected, actual, "raw stage {stage}");
        let scan = engine.handle_query(id, request).await.unwrap();
        let mut rows = 0;
        for partition in 0..scan.properties().num_partitions() {
            let batches = scan
                .scan_partition(&ctx, &metrics, partition)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            rows += batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        }
        assert_eq!(
            if append {
                raw_count + 1
            } else {
                expected.len()
            },
            rows,
            "batch stage {stage}"
        );
    }
    assert_eq!(pool.reserved(), 0);
    engine.stop().await.unwrap();
}

async fn authoritative_fixture(merge_mode: &str) -> (TestEnv, MitoEngine, MitoRegionRef) {
    authoritative_fixture_with(merge_mode, None).await
}

async fn authoritative_fixture_with(
    merge_mode: &str,
    partition_expr: Option<String>,
) -> (TestEnv, MitoEngine, MitoRegionRef) {
    let mut env = TestEnv::with_prefix("narthex-review").await;
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            experimental_enable_series_index: true,
            experimental_series_index_bucket_width: Duration::from_secs(100),
            experimental_series_index_min_files: 1,
            ..Default::default()
        })
        .await;
    let mut metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    metadata
        .column_metadatas
        .iter_mut()
        .find(|c| c.column_id == 2)
        .unwrap()
        .column_schema
        .data_type = ConcreteDataType::float64_datatype();
    let mut create = crate::test_util::CreateRequestBuilder::new()
        .insert_option("primary_key_encoding", "sparse")
        .insert_option("sst_format", "narthex")
        .insert_option("merge_mode", merge_mode)
        .partition_expr_json(partition_expr)
        .build();
    create.column_metadatas = metadata.column_metadatas;
    create.primary_key = metadata.primary_key;
    let id = store_api::storage::RegionId::new(43, 0);
    engine
        .handle_request(id, RegionRequest::Create(create))
        .await
        .unwrap();
    let region = engine.get_region(id).unwrap();
    (env, engine, region)
}

/// A time-partitioned region keeps only its own rows, though a file shared after repartition
/// holds rows of both partitions and series rows carry no time index to filter on. Key columns
/// a mixed rule references take the series' values, `__table_id` included.
#[rstest::rstest]
#[case::time(None)]
#[case::tag_and_time(Some(partition::expr::col("tag_0").eq(Value::from("a"))))]
#[case::table_id_and_time(Some(
    partition::expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME).eq(Value::UInt32(10))
))]
#[tokio::test]
async fn authoritative_time_partition_filters_sample_rows(
    #[case] key_rule: Option<partition::expr::PartitionExpr>,
) {
    let time_rule = partition::expr::col("ts").lt(Value::Timestamp(
        common_time::Timestamp::new_millisecond(60_000),
    ));
    let rule = match key_rule {
        Some(key_rule) => key_rule.and(time_rule),
        None => time_rule,
    };
    let (_env, engine, region) =
        authoritative_fixture_with("last_row", Some(rule.as_json_str().unwrap())).await;
    // 61_000 belongs to the other partition, as rows of a shared file do.
    for timestamp in [1000, 61_000] {
        write(&engine, &region, 7, timestamp, Some(7.0)).await;
    }
    flush_region(&engine, region.region_id, None).await;

    let stream = engine
        .scan_to_stream(region.region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = common_recordbatch::RecordBatches::try_collect(stream)
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );

    // Compaction reads without the region predicate, so only the rule keeps the other
    // partition's row out of the output.
    engine
        .handle_request(
            region.region_id,
            RegionRequest::Compact(store_api::region_request::RegionCompactRequest {
                options: api::v1::region::compact_request::Options::StrictWindow(
                    api::v1::region::StrictWindow { window_seconds: 60 },
                ),
                parallelism: None,
                time_range: None,
            }),
        )
        .await
        .unwrap();
    let version = region.version();
    let rows = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .map(|file| file.meta_ref().num_rows)
        .sum::<u64>();
    assert_eq!(rows, 1);
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn authoritative_strict_windows_preserve_rows() {
    let (_env, engine, region) = authoritative_fixture("last_row").await;
    for timestamp in [1000, 59_999, 60_000, 61_000] {
        write(&engine, &region, 7, timestamp, Some(7.0)).await;
    }
    flush_region(&engine, region.region_id, None).await;
    use store_api::region_request::{AlterKind, ModifyColumnType, RegionAlterRequest};
    engine
        .handle_request(
            region.region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::ModifyColumnTypes {
                    columns: vec![ModifyColumnType {
                        column_name: "ts".into(),
                        target_type: ConcreteDataType::timestamp_microsecond_datatype(),
                    }],
                },
            }),
        )
        .await
        .unwrap();

    let ts = |value| {
        lit(datafusion_common::ScalarValue::TimestampMicrosecond(
            Some(value),
            None,
        ))
    };
    for (start, expected) in [(1_000_000, 1), (1_000_001, 0)] {
        let stream = engine
            .scan_to_stream(
                region.region_id,
                ScanRequest {
                    filters: vec![col("ts").gt_eq(ts(start)), col("ts").lt(ts(start + 1))],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let batches = common_recordbatch::RecordBatches::try_collect(stream)
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            expected
        );
    }
    engine
        .handle_request(
            region.region_id,
            RegionRequest::Compact(store_api::region_request::RegionCompactRequest {
                options: api::v1::region::compact_request::Options::StrictWindow(
                    api::v1::region::StrictWindow { window_seconds: 60 },
                ),
                parallelism: None,
                time_range: None,
            }),
        )
        .await
        .unwrap();
    let version = region.version();
    let files: Vec<_> = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .collect();
    assert_eq!(files.len(), 2);
    assert_eq!(
        files
            .iter()
            .map(|file| file.meta_ref().num_rows)
            .sum::<u64>(),
        4
    );
    let mut windows = files
        .iter()
        .map(|file| {
            let (start, end) = file.meta_ref().time_range;
            (start, end, file.meta_ref().num_rows)
        })
        .collect::<Vec<_>>();
    windows.sort_unstable();
    use common_time::Timestamp;
    assert_eq!(
        windows,
        vec![
            (
                Timestamp::new_millisecond(1000),
                Timestamp::new_millisecond(59_999),
                2
            ),
            (
                Timestamp::new_millisecond(60_000),
                Timestamp::new_millisecond(61_000),
                2
            ),
        ]
    );
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn authoritative_compaction_and_raw_reads_use_last_non_null() {
    let (_env, engine, region) = authoritative_fixture("last_non_null").await;
    use mito_codec::row_converter::{PrimaryKeyCodec, SparsePrimaryKeyCodec};
    use store_api::storage::consts::ReservedColumnId;
    let mut key = Vec::new();
    SparsePrimaryKeyCodec::schemaless()
        .encode_values(
            &[
                (
                    ReservedColumnId::table_id(),
                    datatypes::value::Value::UInt32(10),
                ),
                (ReservedColumnId::tsid(), datatypes::value::Value::UInt64(7)),
                (0, datatypes::value::Value::from("a")),
            ],
            &mut key,
        )
        .unwrap();
    write_primary_key(&engine, &region, key.clone(), 1000, Some(7.0)).await;
    flush_region(&engine, region.region_id, None).await;
    engine
        .handle_request(
            region.region_id,
            crate::test_util::sparse_metric_put(key, 1000, None),
        )
        .await
        .unwrap();
    flush_region(&engine, region.region_id, None).await;
    let scan = engine
        .handle_query(
            region.region_id,
            ScanRequest {
                distribution: Some(TimeSeriesDistribution::PerSeries),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let source = scan
        .prepare_series(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &labels(scan.metadata()),
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
    assert_eq!(samples(&chunks), vec![(1000, 7.0)]);
    let version = region.version();
    let inputs: Vec<_> = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values().cloned())
        .collect();
    let mut source = crate::compaction::CompactionSstReaderBuilder {
        metadata: region.metadata(),
        sst_layer: region.access_layer.clone(),
        cache: engine.cache_manager(),
        inputs: &inputs,
        append_mode: false,
        narthex_format: true,
        filter_deleted: false,
        time_range: None,
        merge_mode: crate::region::options::MergeMode::LastNonNull,
        read_parallelism: 1,
    }
    .build_flat_sst_reader()
    .await
    .unwrap();
    let batch = source.next_batch().await.unwrap().unwrap();
    let values = batch
        .column_by_name("field_0")
        .unwrap()
        .as_any()
        .downcast_ref::<datatypes::arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values.value(0), 7.0);
    assert!(source.next_batch().await.unwrap().is_none());
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn authoritative_compaction_opener_uses_persisted_format() {
    use crate::compaction::compactor::{CompactionRegionOpener, OpenCompactionRegionRequest};
    let (env, engine, region) = authoritative_fixture("last_row").await;
    write(&engine, &region, 7, 1000, Some(7.0)).await;
    flush_region(&engine, region.region_id, None).await;
    let mut config = MitoConfig::default();
    config.index.aux_path = env
        .data_home()
        .join("remote-compaction")
        .display()
        .to_string();
    let opener = CompactionRegionOpener::new(config, env.get_object_store_manager().unwrap())
        .await
        .unwrap();
    for format in [
        None,
        Some(crate::sst::FormatType::Narthex),
        Some(crate::sst::FormatType::Flat),
    ] {
        let request = OpenCompactionRegionRequest {
            region_id: region.region_id,
            table_dir: region.table_dir().to_string(),
            path_type: region.path_type(),
            region_options: crate::region::options::RegionOptions {
                sst_format: format,
                ..Default::default()
            },
            max_parallelism: 1,
            plugins: common_base::Plugins::new(),
        };
        let opened = opener
            .open(
                &request,
                either::Either::Left(common_time::TimeToLive::default()),
            )
            .await
            .unwrap();
        assert_eq!(
            opened.region_options.sst_format,
            Some(crate::sst::FormatType::Narthex)
        );
        opened.stop_purger_scheduler().await.unwrap();
    }
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn authoritative_copy_failure_cleans_only_planned_objects() {
    use crate::region::{FileDescriptor, RegionFileCopier};
    let (_env, engine, region) = authoritative_fixture("last_row").await;
    let store = region.access_layer.object_store();
    store.write("copy/source", "payload").await.unwrap();
    store
        .write("copy/target/existing", "payload")
        .await
        .unwrap();
    let descriptor = |source: &str, target: &str| FileDescriptor::NarthexObject {
        source: source.into(),
        target: target.into(),
        size: 7,
        manifest: None,
    };
    let copier = RegionFileCopier::new(region.access_layer.clone());
    assert!(
        copier
            .copy_files(
                store_api::storage::RegionId::new(43, 1),
                region.region_id,
                vec![
                    descriptor("copy/source", "copy/target/new"),
                    descriptor("copy/missing", "copy/target/failing")
                ],
                1,
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .read("copy/target/existing")
            .await
            .unwrap()
            .to_bytes()
            .as_ref(),
        b"payload"
    );
    assert!(!store.exists("copy/target/new").await.unwrap());
    engine.stop().await.unwrap();
}

#[rstest::rstest]
#[case::newer_put(false)]
#[case::newer_delete(true)]
#[tokio::test]
async fn authoritative_copy_preserves_source_order(#[case] delete: bool) {
    let (_env, engine, source) = authoritative_fixture("last_row").await;
    write(&engine, &source, 7, 1000, Some(7.0)).await;
    flush_region(&engine, source.region_id, None).await;
    write(
        &engine,
        &source,
        7,
        1000,
        if delete { None } else { Some(9.0) },
    )
    .await;
    flush_region(&engine, source.region_id, None).await;
    let metadata = source.metadata();
    let mut create = crate::test_util::CreateRequestBuilder::new()
        .table_dir(source.table_dir())
        .insert_option("primary_key_encoding", "sparse")
        .insert_option("sst_format", "narthex")
        .build();
    create.column_metadatas = metadata.column_metadatas.clone();
    create.primary_key = metadata.primary_key.clone();
    let target = store_api::storage::RegionId::new(43, 1);
    engine
        .handle_request(target, RegionRequest::Create(create.clone()))
        .await
        .unwrap();
    engine
        .copy_region_from(
            target,
            store_api::region_engine::MitoCopyRegionFromRequest {
                source_region_id: source.region_id,
                parallelism: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .get_region(target)
            .unwrap()
            .version_control
            .committed_sequence(),
        0
    );
    for reopen in [false, true] {
        if reopen {
            crate::test_util::reopen_region(
                &engine,
                target,
                create.table_dir.clone(),
                true,
                create.options.clone(),
            )
            .await;
        }
        let scan = engine
            .handle_query(
                target,
                ScanRequest {
                    distribution: Some(TimeSeriesDistribution::PerSeries),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let values = all_raw_samples(
            scan.as_ref(),
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            &labels(scan.metadata()),
            SeriesOrdering::KeyThenTime,
        )
        .await;
        assert_eq!(values, if delete { vec![] } else { vec![(1000, 9.0)] });
        assert!(
            engine
                .handle_query(
                    target,
                    ScanRequest {
                        exact_sequence_range: true,
                        memtable_min_sequence: Some(0),
                        memtable_max_sequence: Some(1),
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
    }
    engine.stop().await.unwrap();
}
