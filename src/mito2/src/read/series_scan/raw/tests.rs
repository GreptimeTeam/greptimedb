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

use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use common_error::ext::ErrorExt;
use common_query::native_histogram::{
    CounterResetHint, NativeHistogram, Span, build_histogram_array, native_histogram_value_type,
};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, lit as physical_lit};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, Operator};
use datatypes::arrow::array::{
    ArrayRef, BinaryArray, Int64Array, StructArray, TimestampMillisecondArray, UInt8Array,
    UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::DataType as _;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{FieldSpec, SampleRef, SeriesSchema};
use futures::{Stream, StreamExt, stream};
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_engine::{BoundTag, RawLabelSource, SendableRawSeriesStream};
use store_api::storage::RegionId;

use super::make_stream;
use crate::error::{Error, InvalidRecordBatchSnafu, Result};
use crate::read::scan_util::PartitionMetrics;
use crate::read::series_scan::{FlatSeriesBatch, SeriesBatch};
use crate::read::stream::{ScanBatch, ScanBatchStream};
use crate::sst::parquet::flat_format::wrap_pk_binary_to_dict;
use crate::sst::parquet::format::PrimaryKeyArray;

fn metrics() -> PartitionMetrics {
    PartitionMetrics::new(
        RegionId::new(1, 0),
        0,
        "raw-test",
        Instant::now(),
        false,
        &ExecutionPlanMetricsSet::new(),
    )
}

fn schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: None,
    })
}

fn binary_batch(keys: &[&[u8]], times: &[i64], values: &[Option<i64>]) -> RecordBatch {
    assert_eq!(keys.len(), times.len());
    assert_eq!(keys.len(), values.len());
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(values.to_vec())),
        Arc::new(TimestampMillisecondArray::from(times.to_vec())),
        Arc::new(BinaryArray::from_iter_values(keys.iter().copied())),
        Arc::new(UInt64Array::from(vec![1; keys.len()])),
        Arc::new(UInt8Array::from(vec![0; keys.len()])),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("__primary_key", DataType::Binary, false),
        Field::new("__sequence", DataType::UInt64, false),
        Field::new("__op_type", DataType::UInt8, false),
    ]));
    RecordBatch::try_new(schema, columns).unwrap()
}

fn dictionary_batch(keys: &[&[u8]], times: &[i64], values: &[Option<i64>]) -> RecordBatch {
    let binary = binary_batch(keys, times, values);
    let schema = Arc::new(Schema::new(
        binary
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if index == binary.num_columns() - 3 {
                    Field::new(
                        field.name(),
                        DataType::Dictionary(
                            Box::new(DataType::UInt32),
                            Box::new(DataType::Binary),
                        ),
                        field.is_nullable(),
                    )
                } else {
                    field.as_ref().clone()
                }
            })
            .collect::<Vec<_>>(),
    ));
    wrap_pk_binary_to_dict(binary, &schema).unwrap()
}

fn histogram_input() -> (RecordBatch, Vec<NativeHistogram>) {
    let expected = vec![NativeHistogram {
        schema: 3,
        zero_threshold: 0.25,
        sum: 42.5,
        reset_hint: CounterResetHint::Unrecognized(77),
        start_timestamp: Some(1234),
        custom_values: vec![1.5, 2.5],
        positive_spans: vec![Span {
            offset: 2,
            length: 2,
        }],
        negative_spans: vec![Span {
            offset: -1,
            length: 1,
        }],
        count: 8.0,
        zero_count: 2.0,
        positive_buckets: vec![3.0, 4.0],
        negative_buckets: vec![1.0],
    }];
    let array = build_histogram_array(&[Some(expected[0].clone())]);
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(array),
        Arc::new(TimestampMillisecondArray::from(vec![Some(1)])),
        Arc::new(BinaryArray::from_iter_values([b"hist-key".as_slice()])),
        Arc::new(UInt64Array::from(vec![1])),
        Arc::new(UInt8Array::from(vec![0])),
    ];
    let fields = vec![
        Field::new(
            "histogram",
            native_histogram_value_type().as_arrow_type(),
            true,
        ),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("__primary_key", DataType::Binary, false),
        Field::new("__sequence", DataType::UInt64, false),
        Field::new("__op_type", DataType::UInt8, false),
    ];
    (
        wrap_pk_binary_to_dict(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
            &Arc::new(Schema::new(vec![
                Field::new(
                    "histogram",
                    native_histogram_value_type().as_arrow_type(),
                    true,
                ),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new(
                    "__primary_key",
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                    false,
                ),
                Field::new("__sequence", DataType::UInt64, false),
                Field::new("__op_type", DataType::UInt8, false),
            ])),
        )
        .unwrap(),
        expected,
    )
}

fn input(items: Vec<Result<ScanBatch>>) -> ScanBatchStream {
    Box::pin(stream::iter(items))
}

fn metadata() -> Arc<RegionMetadata> {
    metadata_with_field_type(ConcreteDataType::int64_datatype())
}

fn metadata_with_field_type(field_type: ConcreteDataType) -> Arc<RegionMetadata> {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 0));
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new("tag", ConcreteDataType::string_datatype(), true),
        semantic_type: SemanticType::Tag,
        column_id: 0,
    });
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
        column_id: 1,
    });
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new("value", field_type, true),
        semantic_type: SemanticType::Field,
        column_id: 2,
    });
    builder.primary_key(vec![0]);
    Arc::new(builder.build().unwrap())
}

fn convert_with_source(
    inner: ScanBatchStream,
    schema: Arc<SeriesSchema>,
    source: RawLabelSource,
    field_type: ConcreteDataType,
    ordering: SeriesOrdering,
) -> SendableRawSeriesStream {
    make_stream(
        inner,
        schema,
        source,
        vec![(0, field_type)],
        metrics(),
        vec![],
        ordering,
    )
}

fn primary_key_source(metadata: Arc<RegionMetadata>) -> RawLabelSource {
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from("metric"),
        physical_metadata: metadata,
        logical_tags: Arc::from([BoundTag {
            column_id: 0,
            name: Arc::from("tag"),
        }]),
    }
}

fn convert(inner: ScanBatchStream, schema: Arc<SeriesSchema>) -> SendableRawSeriesStream {
    convert_with_source(
        inner,
        schema,
        primary_key_source(metadata()),
        ConcreteDataType::int64_datatype(),
        SeriesOrdering::Contiguous,
    )
}

fn convert_ordered(inner: ScanBatchStream, schema: Arc<SeriesSchema>) -> SendableRawSeriesStream {
    convert_with_source(
        inner,
        schema,
        primary_key_source(metadata()),
        ConcreteDataType::int64_datatype(),
        SeriesOrdering::KeyThenTime,
    )
}

fn convert_with_predicate(
    inner: ScanBatchStream,
    schema: Arc<SeriesSchema>,
    predicate: Arc<dyn PhysicalExpr>,
) -> SendableRawSeriesStream {
    convert_with_predicate_ordering(inner, schema, predicate, SeriesOrdering::Contiguous)
}

fn convert_with_predicate_ordering(
    inner: ScanBatchStream,
    schema: Arc<SeriesSchema>,
    predicate: Arc<dyn PhysicalExpr>,
    ordering: SeriesOrdering,
) -> SendableRawSeriesStream {
    make_stream(
        inner,
        schema,
        primary_key_source(metadata()),
        vec![(0, ConcreteDataType::int64_datatype())],
        metrics(),
        vec![predicate],
        ordering,
    )
}

struct DropSpy {
    inner: ScanBatchStream,
    drops: Arc<AtomicUsize>,
}

impl Stream for DropSpy {
    type Item = Result<ScanBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl Drop for DropSpy {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn values(chunk: &RawSeriesChunk) -> Vec<Option<f64>> {
    (0..chunk.time.len())
        .map(|i| match chunk.fields.get(0).unwrap().sample(i) {
            SampleRef::Missing => None,
            SampleRef::Float(v) => Some(v),
            _ => unreachable!(),
        })
        .collect()
}

/// A predicate that counts its evaluations, so tests can observe predicate passes through the
/// existing `PhysicalExpr` hook instead of a production counter.
#[derive(Debug)]
struct CountingPredicate {
    inner: Arc<dyn PhysicalExpr>,
    calls: Arc<AtomicUsize>,
}

impl fmt::Display for CountingPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

impl PartialEq for CountingPredicate {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for CountingPredicate {}

impl Hash for CountingPredicate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.inner), state)
    }
}

impl PhysicalExpr for CountingPredicate {
    fn data_type(&self, input_schema: &Schema) -> datafusion::error::Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion::error::Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::error::Result<ColumnarValue> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CountingPredicate {
            inner: children[0].clone(),
            calls: self.calls.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Builds a counting `value > 0` predicate.
fn positive_value_predicate(calls: Arc<AtomicUsize>) -> Arc<dyn PhysicalExpr> {
    let inner: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("value", 0)),
        Operator::Gt,
        physical_lit(ScalarValue::Int64(Some(0))),
    ));
    Arc::new(CountingPredicate { inner, calls })
}

/// Builds a record of `key` that is exactly [`super::PREDICATE_BATCH_ROWS`] rows long, so it is
/// processed on its own instead of being coalesced with its neighbours.
///
/// The rows after `payload` carry the value `-1`, which the `value > 0` predicate drops.
fn padded_batch(key: &[u8], payload: &[(i64, Option<i64>)]) -> RecordBatch {
    let filler = super::PREDICATE_BATCH_ROWS - payload.len();
    let keys = vec![key; payload.len() + filler];
    let times: Vec<i64> = payload
        .iter()
        .map(|(time, _)| *time)
        .chain((0..filler).map(|offset| 10_000 + offset as i64))
        .collect();
    let values: Vec<Option<i64>> = payload
        .iter()
        .map(|(_, value)| *value)
        .chain((0..filler).map(|_| Some(-1)))
        .collect();
    dictionary_batch(&keys, &times, &values)
}

/// A chunk reduced to what the predicate tests compare: key, timestamps, values and `is_last`.
type ChunkSummary = (Vec<u8>, Vec<i64>, Vec<Option<f64>>, bool);

async fn collect_chunks(mut output: SendableRawSeriesStream) -> Vec<ChunkSummary> {
    let mut chunks = Vec::new();
    while let Some(chunk) = output.next().await {
        let chunk = chunk.unwrap();
        chunks.push((
            chunk.primary_key.as_slice().to_vec(),
            (0..chunk.time.len())
                .map(|i| chunk.time.timestamp(i).unwrap())
                .collect(),
            values(&chunk),
            chunk.is_last,
        ));
    }
    chunks
}

#[tokio::test]
async fn chunks_preserve_keys_values_times_and_final_flags_for_both_input_shapes() {
    let schema = schema();
    let first = dictionary_batch(
        &[b"prefix-full-key-a", b"prefix-full-key-a"],
        &[1, 2],
        &[Some(1), None],
    );
    let second = dictionary_batch(
        &[
            b"prefix-full-key-a",
            b"prefix-full-key-a",
            b"prefix-full-key-b",
            b"prefix-full-key-b",
        ],
        &[3, 4, 1, 2],
        &[Some(3), Some(4), Some(5), Some(6)],
    );
    for flat in [true, false] {
        let items = if flat {
            let mut batches = smallvec::SmallVec::new();
            batches.extend([first.clone(), second.clone()]);
            vec![Ok(ScanBatch::Series(SeriesBatch::Flat(FlatSeriesBatch {
                batches,
            })))]
        } else {
            vec![
                Ok(ScanBatch::RecordBatch(first.clone())),
                Ok(ScanBatch::RecordBatch(second.clone())),
            ]
        };
        let mut output = convert(input(items), schema.clone());
        let mut chunks = Vec::new();
        while let Some(chunk) = output.next().await {
            chunks.push(chunk.unwrap());
        }
        assert_eq!(
            chunks
                .iter()
                .map(|c| c.primary_key.as_slice())
                .collect::<Vec<_>>(),
            vec![
                b"prefix-full-key-a".as_slice(),
                b"prefix-full-key-a",
                b"prefix-full-key-b"
            ]
        );
        assert_eq!(
            chunks.iter().map(|c| c.is_last).collect::<Vec<_>>(),
            vec![false, true, true]
        );
        assert_eq!(
            chunks
                .iter()
                .map(|c| (0..c.time.len())
                    .map(|i| c.time.timestamp(i).unwrap())
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![1, 2], vec![3, 4], vec![1, 2]]
        );
        assert_eq!(
            chunks.iter().map(values).collect::<Vec<_>>(),
            vec![
                vec![Some(1.0), None],
                vec![Some(3.0), Some(4.0)],
                vec![Some(5.0), Some(6.0)]
            ]
        );
    }
}

#[tokio::test]
async fn raw_stream_reports_and_enforces_declared_ordering() {
    let stream = convert(input(vec![]), schema());
    assert_eq!(stream.ordering(), SeriesOrdering::Contiguous);
    drop(stream);
    let stream = convert_ordered(input(vec![]), schema());
    assert_eq!(stream.ordering(), SeriesOrdering::KeyThenTime);
    drop(stream);

    let mut output = convert_ordered(
        input(vec![
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"z"],
                &[1],
                &[Some(1)],
            ))),
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"a"],
                &[1],
                &[Some(2)],
            ))),
        ]),
        schema(),
    );
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());

    let mut output = convert(
        input(vec![
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"z"],
                &[1],
                &[Some(1)],
            ))),
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"a"],
                &[1],
                &[Some(2)],
            ))),
        ]),
        schema(),
    );
    assert_eq!(
        output.next().await.unwrap().unwrap().primary_key.as_slice(),
        b"z"
    );
    assert_eq!(
        output.next().await.unwrap().unwrap().primary_key.as_slice(),
        b"a"
    );
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn typed_predicate_filters_before_numeric_samples_are_cast() {
    let threshold = 1_i64 << 53;
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("value", 0)),
        Operator::Gt,
        physical_lit(ScalarValue::Int64(Some(threshold))),
    ));
    let mut output = make_stream(
        input(vec![Ok(ScanBatch::RecordBatch(dictionary_batch(
            &[b"a", b"a", b"a", b"b", b"c"],
            &[1, 2, 3, 1, 1],
            &[
                Some(threshold),
                None,
                Some(threshold + 1),
                Some(threshold),
                Some(threshold + 1),
            ],
        )))]),
        schema(),
        primary_key_source(metadata()),
        vec![(0, ConcreteDataType::int64_datatype())],
        metrics(),
        vec![predicate],
        SeriesOrdering::Contiguous,
    );
    let chunk = output.next().await.unwrap().unwrap();
    assert!(chunk.is_last);
    assert_eq!(chunk.primary_key.as_slice(), b"a");
    assert_eq!(chunk.time.len(), 1);
    assert_eq!(chunk.time.timestamp(0), Some(3));
    assert_eq!(values(&chunk), vec![Some((threshold + 1) as f64)]);
    let chunk = output.next().await.unwrap().unwrap();
    assert_eq!(chunk.primary_key.as_slice(), b"c");
    assert!(chunk.is_last);
    assert_eq!(chunk.time.timestamp(0), Some(1));
    assert_eq!(values(&chunk), vec![Some((threshold + 1) as f64)]);
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn predicate_error_is_terminal() {
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 0));
    let mut output = make_stream(
        input(vec![Ok(ScanBatch::RecordBatch(dictionary_batch(
            &[b"a"],
            &[1],
            &[Some(1)],
        )))]),
        schema(),
        primary_key_source(metadata()),
        vec![(0, ConcreteDataType::int64_datatype())],
        metrics(),
        vec![predicate],
        SeriesOrdering::Contiguous,
    );
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn upstream_error_is_terminal_after_yielded_prefix_without_flushing_pending() {
    let upstream_error = InvalidRecordBatchSnafu {
        reason: "upstream injected failure".to_string(),
    }
    .build();
    let mut output = convert(
        input(vec![
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"a"],
                &[1],
                &[Some(1)],
            ))),
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"a"],
                &[2],
                &[Some(2)],
            ))),
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"a"],
                &[3],
                &[Some(3)],
            ))),
            Err(upstream_error),
        ]),
        schema(),
    );
    let first = output.next().await.unwrap().unwrap();
    assert_eq!(first.primary_key.as_slice(), b"a");
    assert_eq!(first.time.timestamp(0), Some(1));
    assert_eq!(values(&first), vec![Some(1.0)]);
    assert!(first.metric.is_none());
    assert!(!first.is_last);
    let second = output.next().await.unwrap().unwrap();
    assert_eq!(second.primary_key.as_slice(), b"a");
    assert_eq!(second.time.timestamp(0), Some(2));
    assert_eq!(values(&second), vec![Some(2.0)]);
    assert!(second.metric.is_none());
    assert!(!second.is_last);
    let error = output.next().await.unwrap().unwrap_err();
    let Some(Error::InvalidRecordBatch { reason, .. }) = error.as_any().downcast_ref::<Error>()
    else {
        panic!("unexpected upstream error: {error}");
    };
    assert_eq!(reason, "upstream injected failure");
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn dropping_raw_stream_drops_unfinished_input() {
    let drops = Arc::new(AtomicUsize::new(0));
    let upstream = input(vec![
        Ok(ScanBatch::RecordBatch(dictionary_batch(
            &[b"a"],
            &[1],
            &[Some(1)],
        ))),
        Ok(ScanBatch::RecordBatch(dictionary_batch(
            &[b"b"],
            &[1],
            &[Some(2)],
        ))),
        Ok(ScanBatch::RecordBatch(dictionary_batch(
            &[b"c"],
            &[1],
            &[Some(3)],
        ))),
    ]);
    let spy = DropSpy {
        inner: upstream,
        drops: drops.clone(),
    };
    let mut raw = convert(Box::pin(spy), schema());
    assert_eq!(
        raw.next().await.unwrap().unwrap().primary_key.as_slice(),
        b"a"
    );
    drop(raw);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_series_requires_increasing_keys_but_record_batches_do_not() {
    let mut batches = smallvec::SmallVec::new();
    batches.push(dictionary_batch(&[b"b"], &[1], &[Some(1)]));
    batches.push(dictionary_batch(&[b"a"], &[2], &[Some(2)]));
    let mut output = convert_ordered(
        input(vec![Ok(ScanBatch::Series(SeriesBatch::Flat(
            FlatSeriesBatch { batches },
        )))]),
        schema(),
    );
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());

    let mut batches = smallvec::SmallVec::new();
    batches.push(dictionary_batch(&[b"a"], &[1], &[Some(1)]));
    batches.push(dictionary_batch(&[b"b"], &[1], &[Some(2)]));
    batches.push(dictionary_batch(&[b"a"], &[2], &[Some(3)]));
    let mut output = convert_ordered(
        input(vec![Ok(ScanBatch::Series(SeriesBatch::Flat(
            FlatSeriesBatch { batches },
        )))]),
        schema(),
    );
    let prefix = output.next().await.unwrap().unwrap();
    assert_eq!(prefix.primary_key.as_slice(), b"a");
    assert_eq!(values(&prefix), vec![Some(1.0)]);
    assert_eq!(prefix.time.timestamp(0), Some(1));
    assert!(prefix.is_last);
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());

    let mut batches = smallvec::SmallVec::new();
    batches.push(dictionary_batch(&[b"a"], &[1], &[Some(1)]));
    batches.push(dictionary_batch(&[b"b"], &[1], &[Some(2)]));
    let mut output = convert_ordered(
        input(vec![Ok(ScanBatch::Series(SeriesBatch::Flat(
            FlatSeriesBatch { batches },
        )))]),
        schema(),
    );
    let first = output.next().await.unwrap().unwrap();
    assert_eq!(first.primary_key.as_slice(), b"a");
    assert_eq!(values(&first), vec![Some(1.0)]);
    assert_eq!(first.time.timestamp(0), Some(1));
    assert!(first.is_last);
    let second = output.next().await.unwrap().unwrap();
    assert_eq!(second.primary_key.as_slice(), b"b");
    assert_eq!(values(&second), vec![Some(2.0)]);
    assert_eq!(second.time.timestamp(0), Some(1));
    assert!(second.is_last);
    assert!(output.next().await.is_none());

    let mut output = convert(
        input(vec![
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"b"],
                &[1],
                &[Some(1)],
            ))),
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"a"],
                &[1],
                &[Some(2)],
            ))),
        ]),
        schema(),
    );
    let first = output.next().await.unwrap().unwrap();
    assert_eq!(first.primary_key.as_slice(), b"b");
    assert_eq!(values(&first), vec![Some(1.0)]);
    assert_eq!(first.time.timestamp(0), Some(1));
    assert!(first.is_last);
    let second = output.next().await.unwrap().unwrap();
    assert_eq!(second.primary_key.as_slice(), b"a");
    assert_eq!(values(&second), vec![Some(2.0)]);
    assert_eq!(second.time.timestamp(0), Some(1));
    assert!(second.is_last);
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn raw_key_shares_dictionary_values_and_survives_batch_drop() {
    let record = dictionary_batch(
        &[b"unused", b"prefix-key", b"other"],
        &[0, 1, 1],
        &[Some(0), Some(1), Some(2)],
    )
    .slice(1, 2);
    let pk = record
        .column(record.num_columns() - 3)
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()
        .unwrap();
    let values = pk.values().as_any().downcast_ref::<BinaryArray>().unwrap();
    let expected_ptr = values.value(1).as_ptr();
    let mut output = convert(input(vec![Ok(ScanBatch::RecordBatch(record))]), schema());
    let chunk = output.next().await.unwrap().unwrap();
    assert_eq!(chunk.primary_key.as_slice(), b"prefix-key");
    assert_eq!(chunk.primary_key.as_ptr(), expected_ptr);
    drop(output);
    assert_eq!(chunk.primary_key.as_slice(), b"prefix-key");
}

#[tokio::test]
async fn empty_input_is_a_successful_empty_stream() {
    let mut output = convert(input(vec![]), schema());
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn native_histogram_adapter_preserves_complete_payload() {
    let (record, expected) = histogram_input();
    let mut fields = schema().fields.to_vec();
    fields[0].name = Arc::from("histogram");
    let series_schema = Arc::new(SeriesSchema {
        fields: fields.into(),
        eval_grid: None,
    });
    let mut raw = convert_with_source(
        input(vec![Ok(ScanBatch::RecordBatch(record))]),
        series_schema,
        primary_key_source(metadata_with_field_type(
            native_histogram_value_type().clone(),
        )),
        native_histogram_value_type().clone(),
        SeriesOrdering::Contiguous,
    );
    assert!(matches!(
        raw.label_source(),
        RawLabelSource::PrimaryKey { .. }
    ));
    let chunk = raw.next().await.unwrap().unwrap();
    assert!(chunk.metric.is_none());
    assert!(chunk.is_last);
    let SampleRef::Histogram(actual) = chunk.fields.get(0).unwrap().sample(0) else {
        panic!("expected native histogram sample");
    };
    let expected = &expected[0];
    assert_eq!(actual.schema, expected.schema);
    assert_eq!(actual.zero_threshold, expected.zero_threshold);
    assert_eq!(actual.sum, expected.sum);
    assert_eq!(actual.count, expected.count);
    assert_eq!(actual.zero_count, expected.zero_count);
    assert_eq!(actual.reset_hint, expected.reset_hint);
    assert_eq!(actual.start_timestamp, expected.start_timestamp);
    assert_eq!(actual.custom_values, expected.custom_values);
    assert_eq!(actual.positive.span_offsets, vec![2]);
    assert_eq!(actual.positive.span_lengths, vec![2]);
    assert_eq!(actual.positive.buckets, expected.positive_buckets);
    assert_eq!(actual.negative.span_offsets, vec![-1]);
    assert_eq!(actual.negative.span_lengths, vec![1]);
    assert_eq!(actual.negative.buckets, expected.negative_buckets);
    assert!(raw.next().await.is_none());
}

#[tokio::test]
async fn duplicate_or_descending_cross_batch_time_is_terminal() {
    for next in [2, 1] {
        let mut output = convert(
            input(vec![
                Ok(ScanBatch::RecordBatch(dictionary_batch(
                    &[b"k", b"k"],
                    &[1, 2],
                    &[Some(1), Some(2)],
                ))),
                Ok(ScanBatch::RecordBatch(dictionary_batch(
                    &[b"k"],
                    &[next],
                    &[Some(3)],
                ))),
            ]),
            schema(),
        );
        assert!(output.next().await.unwrap().is_err());
        assert!(output.next().await.is_none());
    }
}

#[tokio::test]
async fn coalesced_groups_evaluate_predicates_once_and_keep_the_chunk_sequence() {
    let package_keys: Vec<Vec<u8>> = (0..2000).map(|i| format!("{i:04}").into_bytes()).collect();
    let packages: Vec<ScanBatch> = package_keys
        .iter()
        .map(|key| {
            package(vec![dictionary_batch(
                &[key, key],
                &[1, 2],
                &[Some(1), Some(2)],
            )])
        })
        .collect();
    let calls = Arc::new(AtomicUsize::new(0));
    let chunks = collect_chunks(convert_with_predicate_ordering(
        input(packages.into_iter().map(Ok).collect()),
        schema(),
        positive_value_predicate(calls.clone()),
        SeriesOrdering::KeyThenTime,
    ))
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        chunks,
        package_keys
            .iter()
            .map(|key| (key.clone(), vec![1, 2], vec![Some(1.0), Some(2.0)], true))
            .collect::<Vec<_>>()
    );

    let payload: Vec<(i64, Option<i64>)> = vec![(1, Some(1)), (2, Some(2))];
    let keys: [&[u8]; 3] = [b"a", b"b", b"c"];
    let small: Vec<RecordBatch> = keys
        .iter()
        .map(|key| dictionary_batch(&[*key, *key], &[1, 2], &[Some(1), Some(2)]))
        .collect();
    let padded: Vec<RecordBatch> = keys.iter().map(|key| padded_batch(key, &payload)).collect();

    // Every record is padded to the buffer threshold, so each one is processed alone.
    let calls = Arc::new(AtomicUsize::new(0));
    let per_record = collect_chunks(convert_with_predicate(
        input(
            padded
                .iter()
                .cloned()
                .map(|record| Ok(ScanBatch::RecordBatch(record)))
                .collect(),
        ),
        schema(),
        positive_value_predicate(calls.clone()),
    ))
    .await;
    let per_record_calls = calls.load(Ordering::SeqCst);

    // The same rows arrive as small records, so one predicate pass covers all of them.
    let calls = Arc::new(AtomicUsize::new(0));
    let coalesced = collect_chunks(convert_with_predicate(
        input(
            small
                .iter()
                .cloned()
                .map(|record| Ok(ScanBatch::RecordBatch(record)))
                .collect(),
        ),
        schema(),
        positive_value_predicate(calls.clone()),
    ))
    .await;
    let coalesced_calls = calls.load(Ordering::SeqCst);

    // The flat shape coalesces the same way.
    let calls = Arc::new(AtomicUsize::new(0));
    let mut batches = smallvec::SmallVec::new();
    batches.extend(small.iter().cloned());
    let coalesced_flat = collect_chunks(convert_with_predicate(
        input(vec![Ok(ScanBatch::Series(SeriesBatch::Flat(
            FlatSeriesBatch { batches },
        )))]),
        schema(),
        positive_value_predicate(calls.clone()),
    ))
    .await;
    let coalesced_flat_calls = calls.load(Ordering::SeqCst);

    let expected: Vec<ChunkSummary> = keys
        .iter()
        .map(|key| (key.to_vec(), vec![1, 2], vec![Some(1.0), Some(2.0)], true))
        .collect();
    assert_eq!(per_record, expected);
    assert_eq!(coalesced, expected);
    assert_eq!(coalesced_flat, expected);
    assert_eq!(per_record_calls, keys.len());
    assert_eq!(coalesced_calls, 1);
    assert_eq!(coalesced_flat_calls, 1);
}

#[tokio::test]
async fn fully_filtered_group_yields_no_chunk_and_leaves_the_next_group_intact() {
    let calls = Arc::new(AtomicUsize::new(0));
    let chunks = collect_chunks(convert_with_predicate(
        input(vec![
            Ok(ScanBatch::RecordBatch(padded_batch(b"a", &[]))),
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"b", b"b"],
                &[1, 2],
                &[Some(1), Some(2)],
            ))),
        ]),
        schema(),
        positive_value_predicate(calls.clone()),
    ))
    .await;

    assert_eq!(
        chunks,
        vec![(b"b".to_vec(), vec![1, 2], vec![Some(1.0), Some(2.0)], true)]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn empty_filtered_group_keeps_the_preceding_series_last_flag() {
    // A group that filters to nothing between two series: `a` is closed by `c`, never by the
    // empty group, and the empty group yields no chunk of its own.
    let calls = Arc::new(AtomicUsize::new(0));
    let chunks = collect_chunks(convert_with_predicate(
        input(vec![
            Ok(ScanBatch::RecordBatch(padded_batch(
                b"a",
                &[(1, Some(1)), (2, Some(2))],
            ))),
            Ok(ScanBatch::RecordBatch(padded_batch(b"b", &[]))),
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"c"],
                &[1],
                &[Some(5)],
            ))),
        ]),
        schema(),
        positive_value_predicate(calls.clone()),
    ))
    .await;

    assert_eq!(
        chunks,
        vec![
            (b"a".to_vec(), vec![1, 2], vec![Some(1.0), Some(2.0)], true),
            (b"c".to_vec(), vec![1], vec![Some(5.0)], true),
        ]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    // A group that filters to nothing right before the input ends must not consume the pending
    // `a` chunk, drop it, or mark it `is_last` twice: the tail flush still closes `a`.
    let calls = Arc::new(AtomicUsize::new(0));
    let chunks = collect_chunks(convert_with_predicate(
        input(vec![
            Ok(ScanBatch::RecordBatch(padded_batch(
                b"a",
                &[(1, Some(1)), (2, Some(2))],
            ))),
            Ok(ScanBatch::RecordBatch(padded_batch(b"b", &[]))),
        ]),
        schema(),
        positive_value_predicate(calls.clone()),
    ))
    .await;

    assert_eq!(
        chunks,
        vec![(b"a".to_vec(), vec![1, 2], vec![Some(1.0), Some(2.0)], true)]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

/// A predicate that drops the rows of one series can leave two runs of another series adjacent,
/// which is how coalescing can fuse two chunks the per-record stream kept apart.
///
/// Fusing them is permitted: the stream contract fixes per-series ordering and finality, not record
/// granularity. What must not change is that a fused run still ends up as one chunk here rather than
/// being withheld, so the test pins the fusion itself.
#[tokio::test]
async fn filtered_boundary_fuses_chunks_where_the_per_record_stream_kept_two() {
    // `a(1)` then `b(1), a(2)`: the `value > 0` predicate drops `b`, so `a`'s two runs become
    // adjacent, and their boundary keys (`a` then `b`) differ at the source record boundary.
    let records = || {
        vec![
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"a"],
                &[1],
                &[Some(1)],
            ))),
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"b", b"a"],
                &[1, 2],
                &[Some(-1), Some(2)],
            ))),
        ]
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let chunks = collect_chunks(convert_with_predicate(
        input(records()),
        schema(),
        positive_value_predicate(calls.clone()),
    ))
    .await;
    // Coalesced into one predicate pass, so both rows are one chunk.
    assert_eq!(
        chunks,
        vec![(b"a".to_vec(), vec![1, 2], vec![Some(1.0), Some(2.0)], true)]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// The longest a test waits for a chunk the producer must derive on its own. The producer does its
/// work synchronously before it polls again, so a second is well above the time it needs and still
/// tells "waiting for input" from "working".
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// A stream that yields `items` and then never produces anything again: no next package, no EOF.
///
/// Its `Poll::Pending` never registers a waker, so a producer that waits for the next package or for
/// the end of the input to close a series is caught by [`CLOSE_TIMEOUT`] instead of hanging the test.
struct PendingAfter {
    items: std::vec::IntoIter<Result<ScanBatch>>,
}

impl Stream for PendingAfter {
    type Item = Result<ScanBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.items.next() {
            Some(item) => Poll::Ready(Some(item)),
            None => Poll::Pending,
        }
    }
}

/// Returns the next chunk, failing instead of hanging when the producer withholds it.
async fn next_within(output: &mut SendableRawSeriesStream) -> RawSeriesChunk {
    tokio::time::timeout(CLOSE_TIMEOUT, output.next())
        .await
        .expect("the producer must not wait for more input to produce this chunk")
        .expect("the stream must not end before this chunk")
        .unwrap()
}

/// Asserts that the producer waits for input instead of producing another chunk.
async fn assert_waits_for_input(output: &mut SendableRawSeriesStream) {
    assert!(
        tokio::time::timeout(CLOSE_TIMEOUT, output.next())
            .await
            .is_err(),
        "the producer must not produce another chunk"
    );
}

fn package(batches: Vec<RecordBatch>) -> ScanBatch {
    let mut flat = smallvec::SmallVec::new();
    flat.extend(batches);
    ScanBatch::Series(SeriesBatch::Flat(FlatSeriesBatch { batches: flat }))
}

/// A package that holds several complete series must close its last one by itself: the stream is
/// never asked for the next package and never ends, so only the package end can close `b`.
#[tokio::test]
async fn a_complete_package_closes_its_last_series_without_the_next_package() {
    let mut output = convert(
        Box::pin(PendingAfter {
            items: vec![Ok(package(vec![
                dictionary_batch(&[b"a"], &[1], &[Some(1)]),
                dictionary_batch(&[b"b", b"b"], &[1, 2], &[Some(2), Some(3)]),
            ]))]
            .into_iter(),
        }),
        schema(),
    );

    // `a` is closed by the primary-key change and `b` ends the package, so both are complete and
    // arrive before the producer asks for anything else.
    let first = next_within(&mut output).await;
    assert_eq!(first.primary_key.as_slice(), b"a");
    assert_eq!(values(&first), vec![Some(1.0)]);
    assert!(first.is_last);
    let second = next_within(&mut output).await;
    assert_eq!(second.primary_key.as_slice(), b"b");
    assert_eq!(values(&second), vec![Some(2.0), Some(3.0)]);
    assert!(second.is_last);
    assert_waits_for_input(&mut output).await;
}

/// A package end closes the last series even when the predicate drops the run that would have closed
/// it, and an emptied package adds no chunk of its own.
#[tokio::test]
async fn multiple_ready_packages_close_before_pending_with_predicate() {
    let calls = Arc::new(AtomicUsize::new(0));
    let packages: Vec<Result<ScanBatch>> = (0..3)
        .map(|i| {
            let key = format!("{i}").into_bytes();
            Ok(package(vec![dictionary_batch(
                &[&key, &key],
                &[1, 2],
                &[Some(1), Some(2)],
            )]))
        })
        .collect();
    let mut output = convert_with_predicate_ordering(
        Box::pin(PendingAfter {
            items: packages.into_iter(),
        }),
        schema(),
        positive_value_predicate(calls.clone()),
        SeriesOrdering::KeyThenTime,
    );
    for i in 0..3 {
        let chunk = next_within(&mut output).await;
        assert_eq!(chunk.primary_key.as_slice(), format!("{i}").as_bytes());
        assert_eq!(values(&chunk), vec![Some(1.0), Some(2.0)]);
        assert!(chunk.is_last);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_waits_for_input(&mut output).await;
}

#[tokio::test]
async fn filtered_package_tails_do_not_close_an_incomplete_ordinary_tail() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut output = convert_with_predicate(
        Box::pin(PendingAfter {
            items: vec![
                Ok(package(vec![dictionary_batch(&[b"a"], &[1], &[Some(1)])])),
                Ok(package(vec![dictionary_batch(&[b"b"], &[1], &[Some(-1)])])),
                Ok(ScanBatch::RecordBatch(dictionary_batch(
                    &[b"c"],
                    &[1],
                    &[Some(3)],
                ))),
            ]
            .into_iter(),
        }),
        schema(),
        positive_value_predicate(calls.clone()),
    );
    let first = next_within(&mut output).await;
    assert_eq!(first.primary_key.as_slice(), b"a");
    assert_eq!(values(&first), vec![Some(1.0)]);
    assert!(first.is_last);
    assert_waits_for_input(&mut output).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn key_then_time_filtered_tail_closes_package_but_not_later_ordinary_tail() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut output = convert_with_predicate_ordering(
        Box::pin(PendingAfter {
            items: vec![
                Ok(package(vec![
                    dictionary_batch(&[b"a"], &[1], &[Some(1)]),
                    dictionary_batch(&[b"b"], &[1], &[Some(-1)]),
                ])),
                Ok(ScanBatch::RecordBatch(dictionary_batch(
                    &[b"c"],
                    &[1],
                    &[Some(3)],
                ))),
            ]
            .into_iter(),
        }),
        schema(),
        positive_value_predicate(calls.clone()),
        SeriesOrdering::KeyThenTime,
    );
    let first = next_within(&mut output).await;
    assert_eq!(first.primary_key.as_slice(), b"a");
    assert_eq!(values(&first), vec![Some(1.0)]);
    assert!(first.is_last);
    // `c` remains pending: the ordinary RecordBatch boundary does not close it, and upstream
    // Pending must not make the package-complete `a` chunk depend on that incomplete tail.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_waits_for_input(&mut output).await;
}

#[tokio::test]
async fn filtered_package_tail_closes_earlier_surviving_series() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut output = convert_with_predicate_ordering(
        Box::pin(PendingAfter {
            items: vec![Ok(package(vec![
                dictionary_batch(&[b"a"], &[1], &[Some(1)]),
                dictionary_batch(&[b"b"], &[1], &[Some(-1)]),
            ]))]
            .into_iter(),
        }),
        schema(),
        positive_value_predicate(calls.clone()),
        SeriesOrdering::KeyThenTime,
    );
    let chunk = next_within(&mut output).await;
    assert_eq!(chunk.primary_key.as_slice(), b"a");
    assert_eq!(values(&chunk), vec![Some(1.0)]);
    assert!(chunk.is_last);
    assert_waits_for_input(&mut output).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn filtered_flat_package_cannot_hide_descending_next_package() {
    let mut output = convert_with_predicate_ordering(
        input(vec![
            Ok(package(vec![dictionary_batch(&[b"b"], &[1], &[Some(-1)])])),
            Ok(package(vec![dictionary_batch(&[b"a"], &[1], &[Some(1)])])),
        ]),
        schema(),
        positive_value_predicate(Arc::new(AtomicUsize::new(0))),
        SeriesOrdering::KeyThenTime,
    );
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn completed_filtered_flat_key_cannot_reopen_in_ordinary_batch() {
    let mut output = convert_with_predicate_ordering(
        input(vec![
            Ok(package(vec![dictionary_batch(&[b"a"], &[1], &[Some(-1)])])),
            Ok(ScanBatch::RecordBatch(dictionary_batch(
                &[b"a"],
                &[2],
                &[Some(1)],
            ))),
        ]),
        schema(),
        positive_value_predicate(Arc::new(AtomicUsize::new(0))),
        SeriesOrdering::KeyThenTime,
    );
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn a_package_end_closes_the_series_whose_tail_run_the_predicate_dropped() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut output = convert_with_predicate(
        Box::pin(PendingAfter {
            items: vec![Ok(package(vec![
                dictionary_batch(&[b"a", b"a"], &[1, 2], &[Some(1), Some(2)]),
                dictionary_batch(&[b"a", b"a"], &[3, 4], &[Some(-1), Some(-2)]),
            ]))]
            .into_iter(),
        }),
        schema(),
        positive_value_predicate(calls.clone()),
    );

    // The second record holds the rest of `a` and ends the package; the predicate empties it, so the
    // chunk built from the first record is the one the package end has to close.
    let chunk = next_within(&mut output).await;
    assert_eq!(chunk.primary_key.as_slice(), b"a");
    assert_eq!(values(&chunk), vec![Some(1.0), Some(2.0)]);
    assert!(chunk.is_last);
    assert_waits_for_input(&mut output).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // A package the predicate empties entirely yields no chunk, and the end of the input has nothing
    // left to close: closing twice would either duplicate or lose the last `a`.
    let chunks = collect_chunks(convert_with_predicate(
        input(vec![
            Ok(package(vec![dictionary_batch(
                &[b"a", b"a"],
                &[1, 2],
                &[Some(1), Some(2)],
            )])),
            Ok(package(vec![dictionary_batch(
                &[b"b", b"b"],
                &[1, 2],
                &[Some(-1), Some(-2)],
            )])),
        ]),
        schema(),
        positive_value_predicate(Arc::new(AtomicUsize::new(0))),
    ))
    .await;
    assert_eq!(
        chunks,
        vec![(b"a".to_vec(), vec![1, 2], vec![Some(1.0), Some(2.0)], true)]
    );
}

/// A series that continues past the package that completed it must be rejected: yielding it again
/// would split one series into two runs that both claim to be final.
#[tokio::test]
async fn a_series_continued_after_its_package_is_terminal() {
    let mut output = convert_ordered(
        input(vec![
            Ok(package(vec![
                dictionary_batch(&[b"a"], &[1], &[Some(1)]),
                dictionary_batch(&[b"b"], &[1], &[Some(2)]),
            ])),
            Ok(package(vec![dictionary_batch(&[b"a"], &[2], &[Some(3)])])),
        ]),
        schema(),
    );

    let first = output.next().await.unwrap().unwrap();
    assert_eq!(first.primary_key.as_slice(), b"a");
    assert!(first.is_last);
    let second = output.next().await.unwrap().unwrap();
    assert_eq!(second.primary_key.as_slice(), b"b");
    assert!(second.is_last);
    let error = output.next().await.unwrap().unwrap_err();
    let Some(Error::InvalidRecordBatch { reason, .. }) = error.as_any().downcast_ref::<Error>()
    else {
        panic!("unexpected error: {error}");
    };
    assert_eq!(
        reason,
        "legacy series primary keys are not strictly increasing"
    );
    assert!(output.next().await.is_none());
}

/// A new run of the key a package closed means the input keeps that series going past the package
/// that completed it, which the flat contract forbids. It is rejected whatever the declared
/// ordering, instead of being yielded as a second complete run of the same series.
#[tokio::test]
async fn a_key_closed_by_a_package_must_not_continue_in_a_later_one() {
    let mut output = convert(
        input(vec![
            Ok(package(vec![dictionary_batch(&[b"a"], &[1], &[Some(1)])])),
            Ok(package(vec![dictionary_batch(&[b"a"], &[2], &[Some(2)])])),
        ]),
        schema(),
    );

    let first = output.next().await.unwrap().unwrap();
    assert_eq!(first.primary_key.as_slice(), b"a");
    assert_eq!(values(&first), vec![Some(1.0)]);
    assert!(first.is_last);
    let error = output.next().await.unwrap().unwrap_err();
    let Some(Error::InvalidRecordBatch { reason, .. }) = error.as_any().downcast_ref::<Error>()
    else {
        panic!("unexpected error: {error}");
    };
    assert_eq!(
        reason,
        "raw series continued after the package that completed it"
    );
    assert!(output.next().await.is_none());
}

/// An ordinary `RecordBatch` says nothing about where a series ends, so the same rows in that shape
/// keep the old behaviour: the last series stays open until the next run of another series or the
/// end of the input arrives.
///
/// This is the same input as `a_complete_package_closes_its_last_series_without_the_next_package`,
/// and it is also the check that the package test is the package fact alone closing `b`: with the
/// fact removed the producer waits forever, which is exactly what happens for a plain record batch.
#[tokio::test]
async fn record_batch_boundaries_do_not_close_a_series() {
    let mut output = convert(
        Box::pin(PendingAfter {
            items: vec![
                Ok(ScanBatch::RecordBatch(dictionary_batch(
                    &[b"a"],
                    &[1],
                    &[Some(1)],
                ))),
                Ok(ScanBatch::RecordBatch(dictionary_batch(
                    &[b"b", b"b"],
                    &[1, 2],
                    &[Some(2), Some(3)],
                ))),
            ]
            .into_iter(),
        }),
        schema(),
    );

    // `a` is closed by the primary-key change as usual.
    let first = next_within(&mut output).await;
    assert_eq!(first.primary_key.as_slice(), b"a");
    assert!(first.is_last);
    // `b` has no package end behind it, so it is still open here.
    assert_waits_for_input(&mut output).await;
}
