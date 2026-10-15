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

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use datatypes::arrow::array::{
    Array, Float64Array, Int64Array, StringArray, StringDictionaryBuilder, StructArray,
    TimestampMillisecondArray,
};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::raw::RawSeriesOrdering;
use datatypes::series::samples::{DenseSamplesRef, FieldSpec, SampleRef};
use futures_util::{Stream, StreamExt};

use super::{InputMapping, record_batches_to_raw_series};

struct CountWake(Arc<AtomicUsize>);

impl std::task::Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

enum InputItem {
    Batch(datafusion_common::Result<RecordBatch>),
    Pending,
}

struct BatchStream {
    schema: SchemaRef,
    batches: VecDeque<InputItem>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Drop for BatchStream {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl Stream for BatchStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        match self.batches.pop_front() {
            Some(InputItem::Batch(item)) => Poll::Ready(Some(item)),
            Some(InputItem::Pending) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for BatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("predicate", DataType::Int64, false),
    ]))
}

fn batch(
    times: &[i64],
    hosts: &[Option<&str>],
    values: &[Option<f64>],
    predicates: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMillisecondArray::from(times.to_vec())),
            Arc::new(StringArray::from(hosts.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(predicates.to_vec())),
        ],
    )
    .unwrap()
}

fn mapping() -> InputMapping {
    InputMapping {
        time_index: 0,
        metric_name: Some(Arc::from("cpu")),
        tags: vec![(Arc::from("host"), 1)],
        fields: vec![(
            FieldSpec {
                name: Arc::from("value"),
            },
            2,
        )],
    }
}

fn source(
    batches: Vec<RecordBatch>,
) -> (
    DfSendableRecordBatchStream,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let input = Box::pin(BatchStream {
        schema: schema(),
        batches: batches
            .into_iter()
            .map(|batch| InputItem::Batch(Ok(batch)))
            .collect(),
        polls: polls.clone(),
        drops: drops.clone(),
    });
    (input, polls, drops)
}

async fn collect(
    mut stream: store_api::region_engine::SendableRawSeriesStream,
) -> Vec<RawSeriesChunk> {
    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
        chunks.push(item.unwrap());
    }
    chunks
}

fn key(chunk: &RawSeriesChunk) -> &[u8] {
    chunk.read_key.as_slice()
}

#[tokio::test]
async fn maps_identity_and_payload_columns_across_batches() {
    let (input, _, _) = source(vec![
        batch(
            &[1, 2],
            &[Some("a"), Some("a")],
            &[Some(10.0), None],
            &[9, 9],
        ),
        RecordBatch::new_empty(schema()),
        batch(
            &[3, 1],
            &[Some("a"), Some("b")],
            &[None, Some(11.0)],
            &[9, 9],
        ),
    ]);
    let output = record_batches_to_raw_series(input, mapping()).unwrap();
    assert!(matches!(
        output.label_source(),
        store_api::region_engine::RawLabelSource::Explicit
    ));
    assert_eq!(output.ordering(), RawSeriesOrdering::Contiguous);
    assert_eq!(
        output
            .schema()
            .fields
            .iter()
            .map(|field| field.name.as_ref())
            .collect::<Vec<_>>(),
        ["value"]
    );
    let chunks = collect(output).await;
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].time.as_explicit().unwrap().as_ref(), &[1, 2]);
    assert_eq!(chunks[1].time.timestamp(0), Some(3));
    assert_eq!(chunks[2].time.timestamp(0), Some(1));
    assert_eq!(key(&chunks[0]), key(&chunks[1]));
    assert_ne!(key(&chunks[1]), key(&chunks[2]));
    assert_eq!(
        chunks.iter().map(|chunk| chunk.is_last).collect::<Vec<_>>(),
        [false, true, true]
    );
    for chunk in &chunks {
        assert_eq!(chunk.metric.as_ref().unwrap().name(), Some("cpu"));
        assert_eq!(chunk.fields.as_slice().len(), 1);
    }
    let DenseSamplesRef::Float(a) = chunks[0].fields.as_slice()[0].segments()[0]
        .dense()
        .unwrap()
    else {
        panic!("float field expected")
    };
    assert_eq!(a.value(0), 10.0);
    assert!(a.is_null(1));
    let DenseSamplesRef::Float(a) = chunks[1].fields.as_slice()[0].segments()[0]
        .dense()
        .unwrap()
    else {
        panic!("float field expected")
    };
    assert!(a.is_null(0));
    let DenseSamplesRef::Float(a) = chunks[2].fields.as_slice()[0].segments()[0]
        .dense()
        .unwrap()
    else {
        panic!("float field expected")
    };
    assert_eq!(a.value(0), 11.0);
}

#[tokio::test]
async fn identity_uses_complete_labels_and_null_is_not_empty() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("left", DataType::Utf8, true),
        Field::new("right", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("unmapped", DataType::Int64, false),
    ]));
    let make = |hosts: Vec<Option<&str>>, rights: Vec<Option<&str>>| {
        let len = hosts.len();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1; len])),
                Arc::new(StringArray::from(hosts)),
                Arc::new(StringArray::from(rights)),
                Arc::new(Float64Array::from(vec![Some(1.0); len])),
                Arc::new(Int64Array::from(vec![i64::MAX; len])),
            ],
        )
        .unwrap()
    };
    let mapping = InputMapping {
        time_index: 0,
        metric_name: None,
        tags: vec![(Arc::from("right"), 2), (Arc::from("left"), 1)],
        fields: vec![(
            FieldSpec {
                name: Arc::from("v"),
            },
            3,
        )],
    };
    let (input, _, _) = source_custom(
        schema.clone(),
        vec![make(
            vec![None, Some(""), Some("ab")],
            vec![Some("x"), None, Some("c")],
        )],
    );
    let chunks = collect(record_batches_to_raw_series(input, mapping).unwrap()).await;
    assert_eq!(chunks.len(), 3);
    assert!(chunks.iter().all(|chunk| chunk.is_last));
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.metric.as_ref().unwrap().name().is_none())
    );
    let labels = chunks
        .iter()
        .map(|chunk| {
            chunk
                .metric
                .as_ref()
                .unwrap()
                .tags()
                .iter()
                .map(|l| (l.name.as_ref(), l.value.as_ref()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(labels[0], vec![("right", "x")]);
    assert_eq!(labels[1], vec![("left", "")]);
    assert_eq!(labels[2], vec![("left", "ab"), ("right", "c")]);
    assert_ne!(key(&chunks[0]), key(&chunks[1]));
    assert_ne!(key(&chunks[1]), key(&chunks[2]));
}

fn scripted_source(
    schema: SchemaRef,
    batches: Vec<InputItem>,
) -> (
    DfSendableRecordBatchStream,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(BatchStream {
            schema,
            batches: batches.into(),
            polls: polls.clone(),
            drops: drops.clone(),
        }),
        polls,
        drops,
    )
}

fn source_custom(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> (
    DfSendableRecordBatchStream,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(BatchStream {
            schema,
            batches: batches
                .into_iter()
                .map(|batch| InputItem::Batch(Ok(batch)))
                .collect(),
            polls: polls.clone(),
            drops: drops.clone(),
        }),
        polls,
        drops,
    )
}

#[tokio::test]
async fn preserves_sliced_timestamp_and_float_buffers_and_exact_values() {
    let schema = schema();
    let times_owner = TimestampMillisecondArray::from(vec![99, 4, 5, 6, 100]);
    let floats_owner = Float64Array::from(vec![
        Some(88.0),
        Some(-0.0),
        None,
        Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        Some(77.0),
    ]);
    let times = times_owner.slice(1, 3);
    let floats = floats_owner.slice(1, 3);
    let input_time_ptr = times.values().as_ptr();
    let input_float_ptr = floats.values().as_ptr();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(times),
            Arc::new(StringArray::from(vec![Some("h"); 3])),
            Arc::new(floats),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    let (input, _, _) = source_custom(schema, vec![batch]);
    let chunks = collect(record_batches_to_raw_series(input, mapping()).unwrap()).await;
    assert_eq!(chunks.len(), 1);
    let time = chunks[0].time.as_explicit().unwrap();
    assert_eq!(time.as_ptr(), input_time_ptr);
    let DenseSamplesRef::Float(values) = chunks[0].fields.as_slice()[0].segments()[0]
        .dense()
        .unwrap()
    else {
        panic!("float field expected")
    };
    assert_eq!(values.values().as_ptr(), input_float_ptr);
    assert_eq!(time.as_ref(), &[4, 5, 6]);
    assert_eq!(values.value(0).to_bits(), (-0.0_f64).to_bits());
    assert!(values.is_null(1));
    assert_eq!(values.value(2).to_bits(), 0x7ff8_0000_0000_0042);
    drop(times_owner);
    drop(floats_owner);
    assert_eq!(time.as_ref(), &[4, 5, 6]);
    assert_eq!(values.value(2).to_bits(), 0x7ff8_0000_0000_0042);
}

#[tokio::test]
async fn native_histogram_payload_and_null_positions_are_preserved() {
    use crate::native_histogram::{
        NativeHistogram, Span, build_histogram_array, native_histogram_arrow_type,
    };

    let histogram = NativeHistogram {
        schema: -53,
        zero_threshold: 0.25,
        sum: -17.5,
        reset_hint: crate::native_histogram::CounterResetHint::Unrecognized(37),
        start_timestamp: Some(-123),
        custom_values: vec![1.5, 4.5],
        positive_spans: vec![Span {
            offset: 1,
            length: 2,
        }],
        negative_spans: vec![Span {
            offset: -2,
            length: 1,
        }],
        count: 17.25,
        zero_count: 2.5,
        positive_buckets: vec![3.25, 7.5],
        negative_buckets: vec![0.75],
    };
    let values = build_histogram_array(&[Some(histogram.clone()), None]);
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8, false),
        Field::new("hist", native_histogram_arrow_type(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["h", "h"])),
            values,
        ],
    )
    .unwrap();
    let mapping = InputMapping {
        time_index: 0,
        metric_name: Some(Arc::from("cpu")),
        tags: vec![(Arc::from("host"), 1)],
        fields: vec![(
            FieldSpec {
                name: Arc::from("hist"),
            },
            2,
        )],
    };
    let (input, _, _) = source_custom(schema, vec![batch]);
    let chunks = collect(record_batches_to_raw_series(input, mapping).unwrap()).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].time.as_explicit().unwrap().as_ref(), &[1, 2]);
    let samples = &chunks[0].fields.as_slice()[0].segments()[0];
    let SampleRef::Histogram(view) = samples.sample(0) else {
        panic!("histogram expected")
    };
    assert_eq!(view.schema, histogram.schema);
    assert_eq!(view.zero_threshold, histogram.zero_threshold);
    assert_eq!(view.sum.to_bits(), histogram.sum.to_bits());
    assert_eq!(view.count, histogram.count);
    assert_eq!(view.zero_count, histogram.zero_count);
    assert_eq!(view.reset_hint, histogram.reset_hint);
    assert_eq!(view.start_timestamp, histogram.start_timestamp);
    assert_eq!(view.custom_values, histogram.custom_values);
    assert_eq!(view.positive.span_offsets, [1]);
    assert_eq!(view.positive.span_lengths, [2]);
    assert_eq!(view.positive.buckets, [3.25, 7.5]);
    assert_eq!(view.negative.span_offsets, [-2]);
    assert_eq!(view.negative.span_lengths, [1]);
    assert_eq!(view.negative.buckets, [0.75]);
    assert!(matches!(samples.sample(1), SampleRef::Missing));
}

#[tokio::test]
async fn malformed_native_histogram_is_terminal_after_completed_prefix() {
    use crate::native_histogram::{build_histogram_array, native_histogram_arrow_type};
    let valid = build_histogram_array(&[None]);
    let valid = valid
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone();
    let mut columns = valid.columns().to_vec();
    let bad_values = Arc::new(Float64Array::new_null(1)) as Arc<dyn Array>;
    columns[1] = bad_values;
    let malformed = StructArray::try_new(valid.fields().clone(), columns, None).unwrap();
    let hist_type = native_histogram_arrow_type();
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8, false),
        Field::new("hist", hist_type, true),
    ]));
    let good = build_histogram_array(&[None]);
    let good_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![10])),
            Arc::new(StringArray::from(vec!["a"])),
            good,
        ],
    )
    .unwrap();
    let bad_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1])),
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(malformed),
        ],
    )
    .unwrap();
    let mapping = InputMapping {
        time_index: 0,
        metric_name: None,
        tags: vec![(Arc::from("host"), 1)],
        fields: vec![(
            FieldSpec {
                name: Arc::from("hist"),
            },
            2,
        )],
    };
    let (input, _, _) = source_custom(schema, vec![good_batch, bad_batch]);
    let mut stream = record_batches_to_raw_series(input, mapping).unwrap();
    let prefix = stream.next().await.unwrap().unwrap();
    assert_eq!(prefix.metric.as_ref().unwrap().get("host"), Some("a"));
    assert_eq!(prefix.time.timestamp(0), Some(10));
    assert!(prefix.is_last);
    assert!(stream.next().await.unwrap().is_err());
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn string_view_and_dictionary_tags_use_logical_values_for_identity() {
    let encodings = [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ];
    let mut output_chunks = Vec::new();
    for tag_type in encodings {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("tag", tag_type.clone(), false),
            Field::new("value", DataType::Float64, false),
        ]));
        let tag_values: Arc<dyn Array> =
            Arc::new(StringArray::from(vec![Some("same"), Some("same")]));
        let tag = if tag_type == DataType::Utf8 {
            tag_values
        } else {
            cast(tag_values.as_ref(), &tag_type).unwrap()
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1, 2])),
                tag,
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap();
        let mapping = InputMapping {
            time_index: 0,
            metric_name: None,
            tags: vec![(Arc::from("tag"), 1)],
            fields: vec![(
                FieldSpec {
                    name: Arc::from("value"),
                },
                2,
            )],
        };
        let (input, _, _) = source_custom(schema.clone(), vec![batch]);
        let chunks = collect(record_batches_to_raw_series(input, mapping).unwrap()).await;
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_last);
        assert_eq!(chunks[0].metric.as_ref().unwrap().get("tag"), Some("same"));
        output_chunks.push(chunks.into_iter().next().unwrap());
    }
    for chunk in &output_chunks {
        assert_eq!(chunk.metric.as_ref().unwrap().get("tag"), Some("same"));
        assert_eq!(chunk.time.timestamp(0), Some(1));
    }
}

#[tokio::test]
async fn dictionary_index_changes_preserve_same_identity_across_batches() {
    use datatypes::arrow::datatypes::Int8Type;

    let tag_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("tag", tag_type, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let mut first_dict = StringDictionaryBuilder::<Int8Type>::new();
    first_dict.append("other").unwrap();
    first_dict.append("same").unwrap();
    let first_tag = Arc::new(first_dict.finish().slice(1, 1)) as Arc<dyn Array>;
    let mut second_dict = StringDictionaryBuilder::<Int8Type>::new();
    second_dict.append("same").unwrap();
    second_dict.append("other").unwrap();
    let second_tag = Arc::new(second_dict.finish().slice(0, 1)) as Arc<dyn Array>;
    let batch_with = |time, tag| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![time])),
                tag,
                Arc::new(Float64Array::from(vec![time as f64])),
            ],
        )
        .unwrap()
    };
    let mapping = InputMapping {
        time_index: 0,
        metric_name: None,
        tags: vec![(Arc::from("tag"), 1)],
        fields: vec![(
            FieldSpec {
                name: Arc::from("value"),
            },
            2,
        )],
    };
    let (input, _, _) = source_custom(
        schema.clone(),
        vec![batch_with(1, first_tag), batch_with(2, second_tag)],
    );
    let chunks = collect(record_batches_to_raw_series(input, mapping).unwrap()).await;
    assert_eq!(chunks.len(), 2);
    assert_eq!(key(&chunks[0]), key(&chunks[1]));
    assert_eq!(chunks[0].time.timestamp(0), Some(1));
    assert_eq!(chunks[1].time.timestamp(0), Some(2));
    assert_eq!(chunks[0].metric.as_ref().unwrap().get("tag"), Some("same"));
    assert_eq!(chunks[1].metric.as_ref().unwrap().get("tag"), Some("same"));
    assert_eq!((chunks[0].is_last, chunks[1].is_last), (false, true));
}

#[tokio::test]
async fn constructor_rejects_bad_mapping_without_polling_and_drops_source() {
    let alternate = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8, true),
        Field::new("other_tag", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("other_value", DataType::Float64, true),
        Field::new("predicate", DataType::Int64, false),
    ]));
    let alternate_batch = RecordBatch::new_empty(alternate.clone());
    let duplicate_tag = InputMapping {
        time_index: 0,
        metric_name: None,
        tags: vec![(Arc::from("dup"), 1), (Arc::from("dup"), 2)],
        fields: vec![(
            FieldSpec {
                name: Arc::from("value"),
            },
            3,
        )],
    };
    let duplicate_fields = InputMapping {
        time_index: 0,
        metric_name: None,
        tags: vec![],
        fields: vec![
            (
                FieldSpec {
                    name: Arc::from("same"),
                },
                3,
            ),
            (
                FieldSpec {
                    name: Arc::from("same"),
                },
                4,
            ),
        ],
    };
    for invalid in [duplicate_tag, duplicate_fields] {
        let (input, polls, drops) = source_custom(alternate.clone(), vec![alternate_batch.clone()]);
        assert!(record_batches_to_raw_series(input, invalid).is_err());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    let (wrong_time, time_mapping) = {
        let schema = schema();
        let mut fields = schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[0] = Field::new("time", DataType::Int64, false);
        let schema = Arc::new(Schema::new(fields));
        let mapping = InputMapping {
            time_index: 0,
            ..mapping()
        };
        (schema, mapping)
    };
    let (wrong_tag, tag_mapping) = {
        let schema = schema();
        let mut fields = schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[1] = Field::new("host", DataType::Boolean, true);
        let schema = Arc::new(Schema::new(fields));
        let mapping = mapping();
        (schema, mapping)
    };
    let (wrong_field, field_mapping) = {
        let schema = schema();
        let mut fields = schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[2] = Field::new("value", DataType::Int64, true);
        let schema = Arc::new(Schema::new(fields));
        let mapping = mapping();
        (schema, mapping)
    };
    for (schema, invalid) in [
        (wrong_time, time_mapping),
        (wrong_tag, tag_mapping),
        (wrong_field, field_mapping),
    ] {
        let (input, polls, drops) =
            source_custom(schema.clone(), vec![RecordBatch::new_empty(schema)]);
        assert!(record_batches_to_raw_series(input, invalid).is_err());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    let invalids = [
        InputMapping {
            time_index: 99,
            ..mapping()
        },
        InputMapping {
            tags: vec![(Arc::from("host"), 99)],
            ..mapping()
        },
        InputMapping {
            fields: vec![(
                FieldSpec {
                    name: Arc::from("value"),
                },
                99,
            )],
            ..mapping()
        },
        InputMapping {
            tags: vec![(Arc::from("__name__"), 1)],
            ..mapping()
        },
        InputMapping {
            fields: vec![],
            ..mapping()
        },
    ];
    for invalid in invalids {
        let (input, polls, drops) = source(vec![]);
        assert!(record_batches_to_raw_series(input, invalid).is_err());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn pending_upstream_does_not_flush_buffered_series() {
    let first = batch(&[1], &[Some("a")], &[Some(10.0)], &[1]);
    let second = batch(&[2], &[Some("a")], &[Some(20.0)], &[2]);
    let (input, polls, drops) = scripted_source(
        schema(),
        vec![
            InputItem::Batch(Ok(first)),
            InputItem::Pending,
            InputItem::Batch(Ok(second)),
        ],
    );
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = std::task::Waker::from(Arc::new(CountWake(wakes.clone())));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut stream).poll_next(&mut cx),
        Poll::Pending
    ));
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let Poll::Ready(Some(Ok(first))) = Pin::new(&mut stream).poll_next(&mut cx) else {
        panic!("expected completed prefix")
    };
    assert!(!first.is_last);
    assert_eq!(first.time.timestamp(0), Some(1));
    let Poll::Ready(Some(Ok(last))) = Pin::new(&mut stream).poll_next(&mut cx) else {
        panic!("expected final chunk")
    };
    assert!(last.is_last);
    assert_eq!(last.time.timestamp(0), Some(2));
    assert!(matches!(
        Pin::new(&mut stream).poll_next(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn upstream_error_drops_unfinished_buffer_without_emitting_it() {
    let first = batch(&[1], &[Some("a")], &[Some(10.0)], &[1]);
    let error = datafusion_common::DataFusionError::External(Box::new(BoxedError::new(
        PlainError::new("upstream failure".into(), StatusCode::RequestOutdated),
    )));
    let (input, polls, drops) = scripted_source(
        schema(),
        vec![
            InputItem::Batch(Ok(first)),
            InputItem::Pending,
            InputItem::Batch(Err(error)),
        ],
    );
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = std::task::Waker::from(Arc::new(CountWake(wakes.clone())));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut stream).poll_next(&mut cx),
        Poll::Pending
    ));
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    let Poll::Ready(Some(Err(error))) = Pin::new(&mut stream).poll_next(&mut cx) else {
        panic!("expected upstream failure")
    };
    assert_eq!(
        common_error::ext::ErrorExt::status_code(&error),
        StatusCode::RequestOutdated
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        Pin::new(&mut stream).poll_next(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn completed_key_is_emitted_before_next_key_validation_fails() {
    let (input, _, drops) = source(vec![
        batch(&[10], &[Some("a")], &[Some(10.0)], &[1]),
        batch(
            &[3, 2],
            &[Some("b"), Some("b")],
            &[Some(30.0), Some(20.0)],
            &[2, 2],
        ),
    ]);
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    let prefix = stream.next().await.unwrap().unwrap();
    assert_eq!(prefix.metric.as_ref().unwrap().get("host"), Some("a"));
    assert_eq!(prefix.time.timestamp(0), Some(10));
    assert!(prefix.is_last);
    let DenseSamplesRef::Float(values) = prefix.fields.as_slice()[0].segments()[0].dense().unwrap()
    else {
        panic!("float expected")
    };
    assert_eq!(values.value(0), 10.0);
    assert!(stream.next().await.unwrap().is_err());
    assert!(stream.next().await.is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn eof_releases_source_batch_and_emitted_chunks_own_sliced_buffers() {
    let times = TimestampMillisecondArray::from(vec![1, 2]);
    let floats = Float64Array::from(vec![Some(10.0), Some(20.0)]);
    let time_buffer = times.values().inner().clone();
    let float_buffer = floats.values().inner().clone();
    let input_batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(times),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            Arc::new(floats),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    let (input, _, drops) = source(vec![input_batch]);
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert!(first.is_last);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(time_buffer.strong_count() > 1);
    assert!(float_buffer.strong_count() > 1);
    drop(first);
    assert!(stream.next().await.unwrap().unwrap().is_last);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(time_buffer.strong_count(), 1);
    assert_eq!(float_buffer.strong_count(), 1);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn canceling_pending_stream_releases_buffered_batch_and_owner() {
    let time_owner = TimestampMillisecondArray::from(vec![1]);
    let float_owner = Float64Array::from(vec![Some(10.0)]);
    let time_buffer = time_owner.values().inner().clone();
    let float_buffer = float_owner.values().inner().clone();
    let input_batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(time_owner),
            Arc::new(StringArray::from(vec![Some("a")])),
            Arc::new(float_owner),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let (input, _, drops) = scripted_source(
        schema(),
        vec![InputItem::Batch(Ok(input_batch)), InputItem::Pending],
    );
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = std::task::Waker::from(Arc::new(CountWake(wakes.clone())));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut stream).poll_next(&mut cx),
        Poll::Pending
    ));
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert!(time_buffer.strong_count() > 1);
    assert!(float_buffer.strong_count() > 1);
    drop(stream);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(time_buffer.strong_count(), 1);
    assert_eq!(float_buffer.strong_count(), 1);
}

#[tokio::test]
async fn runtime_errors_terminate_once_and_keep_no_unfinished_identity() {
    let (input, polls, drops) = source(vec![
        batch(
            &[1, 1],
            &[Some("a"), Some("b")],
            &[Some(1.0), Some(2.0)],
            &[1, 2],
        ),
        batch(&[1], &[Some("b")], &[Some(3.0)], &[3]),
        batch(&[3], &[Some("c")], &[Some(4.0)], &[4]),
    ]);
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    assert!(stream.next().await.unwrap().is_err());
    assert!(stream.next().await.is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(polls.load(Ordering::SeqCst) <= 3);
}

#[tokio::test]
async fn empty_batch_with_changed_schema_is_terminal_before_skip() {
    let declared = schema();
    let changed = Arc::new(Schema::new(vec![Field::new(
        "different",
        DataType::Int64,
        false,
    )]));
    let (input, polls, drops) = scripted_source(
        declared,
        vec![InputItem::Batch(Ok(RecordBatch::new_empty(changed)))],
    );
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    assert!(matches!(stream.next().await, Some(Err(_))));
    assert!(stream.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn null_timestamp_is_terminal() {
    let nullable = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("host", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("predicate", DataType::Int64, false),
    ]));
    let input_batch = RecordBatch::try_new(
        nullable.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![None])),
            Arc::new(StringArray::from(vec![Some("h")])),
            Arc::new(Float64Array::from(vec![Some(1.0)])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let (input, _, _) = source_custom(nullable, vec![input_batch]);
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    assert!(stream.next().await.unwrap().is_err());
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn rejects_descending_time_inside_source_batch() {
    let (input, _, _) = source(vec![batch(
        &[2, 1],
        &[Some("h"), Some("h")],
        &[Some(1.0), Some(2.0)],
        &[1, 1],
    )]);
    let mut output = record_batches_to_raw_series(input, mapping()).unwrap();
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn exact_int64_filter_sort_adapter_frame_pipeline() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("gate", DataType::Int64, false),
    ]));
    let threshold = 9_007_199_254_740_993_i64;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![4, 2, 1, 3])),
            Arc::new(StringArray::from(vec!["h", "h", "h", "h"])),
            Arc::new(Float64Array::from(vec![40.0, 20.0, 10.0, 30.0])),
            Arc::new(Int64Array::from(vec![
                threshold - 1,
                threshold,
                threshold,
                threshold + 1,
            ])),
        ],
    )
    .unwrap();
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
    let gate: Arc<dyn PhysicalExpr> = Arc::new(Column::new("gate", 3));
    let predicate = Arc::new(BinaryExpr::new(
        gate,
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Int64(Some(threshold)))),
    ));
    let filter = Arc::new(FilterExec::try_new(predicate, source).unwrap());
    let time: Arc<dyn PhysicalExpr> = Arc::new(Column::new("time", 0));
    let host: Arc<dyn PhysicalExpr> = Arc::new(Column::new("host", 1));
    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new_default(host),
        PhysicalSortExpr::new_default(time),
    ])
    .unwrap();
    let sort = SortExec::new(ordering, filter);
    let ctx = SessionContext::new();
    let input = sort.execute(0, ctx.task_ctx()).unwrap();
    let mapping = InputMapping {
        time_index: 0,
        metric_name: Some(Arc::from("cpu")),
        tags: vec![(Arc::from("host"), 1)],
        fields: vec![(
            FieldSpec {
                name: Arc::from("value"),
            },
            2,
        )],
    };
    let raw = record_batches_to_raw_series(input, mapping).unwrap();
    let frames = super::super::pack_raw_series(raw).unwrap();
    let expected = Arc::new(datatypes::series::samples::SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: None,
    });
    let mut decoded = super::super::unpack_raw_series(
        frames,
        expected,
        store_api::region_engine::RawLabelSource::Explicit,
        RawSeriesOrdering::Contiguous,
    )
    .unwrap();
    let chunk = decoded.next().await.unwrap().unwrap();
    assert!(chunk.is_last);
    assert_eq!(chunk.time.as_explicit().unwrap().as_ref(), &[1, 2]);
    assert_eq!(chunk.metric.as_ref().unwrap().name(), Some("cpu"));
    assert_eq!(chunk.metric.as_ref().unwrap().get("host"), Some("h"));
    let DenseSamplesRef::Float(values) = chunk.fields.as_slice()[0].segments()[0].dense().unwrap()
    else {
        panic!("float expected")
    };
    assert_eq!(values.value(0), 10.0);
    assert_eq!(values.value(1), 20.0);
    assert!(decoded.next().await.is_none());
}

#[tokio::test]
async fn empty_input_finishes_with_explicit_contiguous_metadata() {
    let (input, polls, drops) = source(vec![RecordBatch::new_empty(schema())]);
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    assert!(matches!(
        stream.label_source(),
        store_api::region_engine::RawLabelSource::Explicit
    ));
    assert_eq!(stream.ordering(), RawSeriesOrdering::Contiguous);
    assert!(stream.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_batches_are_bounded_per_poll_and_eventually_progress() {
    let mut batches = vec![RecordBatch::new_empty(schema()); 65];
    batches.push(batch(&[7], &[Some("h")], &[Some(7.0)], &[7]));
    let (input, polls, _) = source(batches);
    let mut stream = record_batches_to_raw_series(input, mapping()).unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = std::task::Waker::from(Arc::new(CountWake(wakes.clone())));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut stream).poll_next(&mut cx),
        Poll::Pending
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 32);
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        Pin::new(&mut stream).poll_next(&mut cx),
        Poll::Pending
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 64);
    assert_eq!(wakes.load(Ordering::SeqCst), 2);
    let first = Pin::new(&mut stream).poll_next(&mut cx);
    let Poll::Ready(Some(Ok(chunk))) = first else {
        panic!("expected row chunk")
    };
    assert_eq!(chunk.time.timestamp(0), Some(7));
    assert!(chunk.is_last);
    assert!(matches!(
        Pin::new(&mut stream).poll_next(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 67);
}

#[tokio::test]
async fn output_schema_and_field_order_follow_logical_mapping() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8, true),
        Field::new("first", DataType::Float64, true),
        Field::new("second", DataType::Float64, true),
        Field::new("predicate", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("h")])),
            Arc::new(Float64Array::from(vec![Some(11.0)])),
            Arc::new(Float64Array::from(vec![Some(22.0)])),
            Arc::new(Int64Array::from(vec![7])),
        ],
    )
    .unwrap();
    let input_mapping = InputMapping {
        time_index: 0,
        metric_name: Some(Arc::from("cpu")),
        tags: vec![(Arc::from("host"), 1)],
        fields: vec![
            (
                FieldSpec {
                    name: Arc::from("logical_second"),
                },
                3,
            ),
            (
                FieldSpec {
                    name: Arc::from("logical_first"),
                },
                2,
            ),
        ],
    };
    let (input, _, _) = source_custom(schema, vec![batch]);
    let mut stream = record_batches_to_raw_series(input, input_mapping).unwrap();
    assert_eq!(
        stream
            .schema()
            .fields
            .iter()
            .map(|field| field.name.as_ref())
            .collect::<Vec<_>>(),
        ["logical_second", "logical_first"]
    );
    let chunk = stream.next().await.unwrap().unwrap();
    let DenseSamplesRef::Float(second) = chunk.fields.as_slice()[0].segments()[0].dense().unwrap()
    else {
        panic!("float expected")
    };
    let DenseSamplesRef::Float(first) = chunk.fields.as_slice()[1].segments()[0].dense().unwrap()
    else {
        panic!("float expected")
    };
    assert_eq!(second.value(0), 22.0);
    assert_eq!(first.value(0), 11.0);
    assert_eq!(chunk.metric.as_ref().unwrap().name(), Some("cpu"));
    assert!(chunk.is_last);
}
