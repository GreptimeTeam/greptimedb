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

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use common_error::ext::ErrorExt;
use common_query::native_histogram::{
    CounterResetHint, NativeHistogram, Span, build_histogram_array,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::{
    ArrayRef, BinaryArray, Int64Array, StructArray, TimestampMillisecondArray, UInt8Array,
    UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::DataType as _;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SampleRef;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use futures::{StreamExt, stream};
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
use store_api::region_engine::{BoundTag, RawLabelSource, SendableRawSeriesStream};
use store_api::storage::RegionId;

use super::make_stream;
use crate::read::scan_util::PartitionMetrics;
use crate::read::series_scan::{FlatSeriesBatch, SeriesBatch};
use crate::read::stream::{ScanBatch, ScanBatchStream};

fn metrics() -> PartitionMetrics {
    PartitionMetrics::new(
        store_api::storage::RegionId::new(1, 0),
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

fn batch(keys: &[&[u8]], times: &[i64], values: &[Option<i64>]) -> RecordBatch {
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
            DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("__primary_key", DataType::Binary, false),
        Field::new("__sequence", DataType::UInt64, false),
        Field::new("__op_type", DataType::UInt8, false),
    ]));
    RecordBatch::try_new(schema, columns).unwrap()
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
            common_query::native_histogram::native_histogram_value_type().as_arrow_type(),
            true,
        ),
        Field::new(
            "ts",
            DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("__primary_key", DataType::Binary, false),
        Field::new("__sequence", DataType::UInt64, false),
        Field::new("__op_type", DataType::UInt8, false),
    ];
    (
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
        expected,
    )
}

fn input(items: Vec<crate::error::Result<ScanBatch>>) -> ScanBatchStream {
    Box::pin(stream::iter(items))
}

fn metadata() -> Arc<store_api::metadata::RegionMetadata> {
    metadata_with_field_type(datatypes::prelude::ConcreteDataType::int64_datatype())
}

fn metadata_with_field_type(
    field_type: datatypes::prelude::ConcreteDataType,
) -> Arc<store_api::metadata::RegionMetadata> {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 0));
    builder.push_column_metadata(ColumnMetadata {
        column_schema: datatypes::schema::ColumnSchema::new(
            "tag",
            datatypes::prelude::ConcreteDataType::string_datatype(),
            true,
        ),
        semantic_type: api::v1::SemanticType::Tag,
        column_id: 0,
    });
    builder.push_column_metadata(ColumnMetadata {
        column_schema: datatypes::schema::ColumnSchema::new(
            "ts",
            datatypes::prelude::ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: api::v1::SemanticType::Timestamp,
        column_id: 1,
    });
    builder.push_column_metadata(ColumnMetadata {
        column_schema: datatypes::schema::ColumnSchema::new("value", field_type, true),
        semantic_type: api::v1::SemanticType::Field,
        column_id: 2,
    });
    builder.primary_key(vec![0]);
    Arc::new(builder.build().unwrap())
}

fn convert_with_source(
    inner: ScanBatchStream,
    schema: Arc<SeriesSchema>,
    source: RawLabelSource,
    field_type: datatypes::prelude::ConcreteDataType,
) -> SendableRawSeriesStream {
    make_stream(inner, schema, source, vec![(0, field_type)], metrics())
}

fn primary_key_source(metadata: Arc<store_api::metadata::RegionMetadata>) -> RawLabelSource {
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
        datatypes::prelude::ConcreteDataType::int64_datatype(),
    )
}

struct DropSpy {
    inner: ScanBatchStream,
    drops: Arc<AtomicUsize>,
}

impl futures::Stream for DropSpy {
    type Item = crate::error::Result<ScanBatch>;

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
            datatypes::series::samples::SampleRef::Missing => None,
            datatypes::series::samples::SampleRef::Float(v) => Some(v),
            _ => unreachable!(),
        })
        .collect()
}

#[tokio::test]
async fn chunks_preserve_keys_values_times_and_final_flags_for_both_input_shapes() {
    let schema = schema();
    let first = batch(
        &[b"prefix-full-key-a", b"prefix-full-key-a"],
        &[1, 2],
        &[Some(1), None],
    );
    let second = batch(
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
                .map(|c| c.read_key.as_slice())
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
async fn upstream_error_is_terminal_after_yielded_prefix_without_flushing_pending() {
    let upstream_error = crate::error::InvalidRecordBatchSnafu {
        reason: "upstream injected failure".to_string(),
    }
    .build();
    let mut output = convert(
        input(vec![
            Ok(ScanBatch::RecordBatch(batch(&[b"a"], &[1], &[Some(1)]))),
            Ok(ScanBatch::RecordBatch(batch(&[b"a"], &[2], &[Some(2)]))),
            Ok(ScanBatch::RecordBatch(batch(&[b"a"], &[3], &[Some(3)]))),
            Err(upstream_error),
        ]),
        schema(),
    );
    let first = output.next().await.unwrap().unwrap();
    assert_eq!(first.read_key.as_slice(), b"a");
    assert_eq!(first.time.timestamp(0), Some(1));
    assert_eq!(values(&first), vec![Some(1.0)]);
    assert!(first.metric.is_none());
    assert!(!first.is_last);
    let second = output.next().await.unwrap().unwrap();
    assert_eq!(second.read_key.as_slice(), b"a");
    assert_eq!(second.time.timestamp(0), Some(2));
    assert_eq!(values(&second), vec![Some(2.0)]);
    assert!(second.metric.is_none());
    assert!(!second.is_last);
    let error = output.next().await.unwrap().unwrap_err();
    let Some(crate::error::Error::InvalidRecordBatch { reason, .. }) =
        error.as_any().downcast_ref::<crate::error::Error>()
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
        Ok(ScanBatch::RecordBatch(batch(&[b"a"], &[1], &[Some(1)]))),
        Ok(ScanBatch::RecordBatch(batch(&[b"b"], &[1], &[Some(2)]))),
        Ok(ScanBatch::RecordBatch(batch(&[b"c"], &[1], &[Some(3)]))),
    ]);
    let spy = DropSpy {
        inner: upstream,
        drops: drops.clone(),
    };
    let mut raw = convert(Box::pin(spy), schema());
    assert_eq!(raw.next().await.unwrap().unwrap().read_key.as_slice(), b"a");
    drop(raw);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn raw_stream_owns_schema_and_bound_source_metadata() {
    let schema = schema();
    let schema_weak = Arc::downgrade(&schema);
    let metadata = metadata();
    let metadata_weak = Arc::downgrade(&metadata);
    let mut raw = convert_with_source(
        input(vec![]),
        schema.clone(),
        primary_key_source(metadata.clone()),
        datatypes::prelude::ConcreteDataType::int64_datatype(),
    );
    drop(schema);
    drop(metadata);
    assert!(schema_weak.upgrade().is_some());
    assert!(metadata_weak.upgrade().is_some());
    assert_eq!(raw.schema().fields[0].name.as_ref(), "value");
    match raw.label_source() {
        RawLabelSource::PrimaryKey {
            metric_name,
            physical_metadata,
            logical_tags,
        } => {
            assert_eq!(metric_name.as_ref(), "metric");
            assert_eq!(physical_metadata.region_id, RegionId::new(1, 0));
            assert_eq!(
                physical_metadata.column_metadatas[0].column_schema.name,
                "tag"
            );
            assert_eq!(logical_tags[0].column_id, 0);
            assert_eq!(logical_tags[0].name.as_ref(), "tag");
            assert!(Arc::ptr_eq(
                physical_metadata,
                &metadata_weak.upgrade().unwrap()
            ));
        }
        RawLabelSource::Explicit => panic!("expected bound primary-key labels"),
    }
    assert!(raw.next().await.is_none());
    drop(raw);
    assert!(schema_weak.upgrade().is_none());
    assert!(metadata_weak.upgrade().is_none());
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
            common_query::native_histogram::native_histogram_value_type().clone(),
        )),
        common_query::native_histogram::native_histogram_value_type().clone(),
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
                Ok(ScanBatch::RecordBatch(batch(
                    &[b"k", b"k"],
                    &[1, 2],
                    &[Some(1), Some(2)],
                ))),
                Ok(ScanBatch::RecordBatch(batch(&[b"k"], &[next], &[Some(3)]))),
            ]),
            schema(),
        );
        assert!(output.next().await.unwrap().is_err());
        assert!(output.next().await.is_none());
    }
}
