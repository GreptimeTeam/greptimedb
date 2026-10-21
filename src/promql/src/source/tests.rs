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

use api::v1::SemanticType;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::series_frame::{label_decode_spec, pack_raw_series, unpack_raw_series};
use datatypes::arrow::array::{Array, Float64Array, UInt32Array};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::schema::ColumnSchema;
use datatypes::series::frame::LabelDecodeSpec;
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, HistogramSamples, SampleRef, Samples, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use futures::{Stream, StreamExt};
use mito_codec::row_converter::{
    DensePrimaryKeyCodec, PrimaryKeyCodec, SortField, SparsePrimaryKeyCodec,
};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_engine::{
    BoundTag, RawLabelSource, RawSeriesStream, SendableRawSeriesStream,
};
use store_api::storage::RegionId;

use super::materialize_raw_labels;

fn metadata(encoding: PrimaryKeyEncoding) -> Arc<RegionMetadata> {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(7, 1));
    for (id, name, ty, semantic_type) in [
        (
            0,
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            SemanticType::Timestamp,
        ),
        (
            8,
            "zeta",
            ConcreteDataType::string_datatype(),
            SemanticType::Tag,
        ),
        (
            2,
            "private",
            ConcreteDataType::string_datatype(),
            SemanticType::Tag,
        ),
        (
            41,
            "alpha",
            ConcreteDataType::string_datatype(),
            SemanticType::Tag,
        ),
        (
            99,
            "value",
            ConcreteDataType::float64_datatype(),
            SemanticType::Field,
        ),
    ] {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(name, ty, semantic_type != SemanticType::Timestamp),
            semantic_type,
            column_id: id,
        });
    }
    builder.primary_key(vec![8, 2, 41]);
    builder.primary_key_encoding(encoding);
    Arc::new(builder.build().unwrap())
}

fn source(encoding: PrimaryKeyEncoding) -> RawLabelSource {
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from("requests_total"),
        physical_metadata: metadata(encoding),
        logical_tags: Arc::from([
            BoundTag {
                column_id: 8,
                name: Arc::from("zeta"),
            },
            BoundTag {
                column_id: 41,
                name: Arc::from("alpha"),
            },
        ]),
    }
}

fn key(encoding: PrimaryKeyEncoding, values: &[(u32, Value)]) -> Buffer {
    let mut bytes = Vec::new();
    match encoding {
        PrimaryKeyEncoding::Dense => {
            let fields = values
                .iter()
                .map(|(id, value)| {
                    let data_type = if matches!(value, Value::Null) {
                        ConcreteDataType::string_datatype()
                    } else {
                        value.data_type()
                    };
                    (*id, SortField::new(data_type))
                })
                .collect();
            DensePrimaryKeyCodec::with_fields(fields)
                .encode_values(values, &mut bytes)
                .unwrap();
        }
        PrimaryKeyEncoding::Sparse => {
            let fields = values
                .iter()
                .map(|(id, _)| (*id, SortField::new(ConcreteDataType::string_datatype())))
                .collect();
            let codec = SparsePrimaryKeyCodec::with_fields(fields);
            codec.encode_internal(42, 9, &mut bytes).unwrap();
            codec.encode_values(values, &mut bytes).unwrap();
        }
    }
    Buffer::from(bytes)
}

fn schema() -> Arc<SeriesSchema> {
    schema_with_fields(&["value"])
}

fn schema_with_fields(names: &[&str]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: names
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(*name),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    })
}

fn chunk(read_key: Buffer, metric: Option<MetricName>, final_chunk: bool) -> RawSeriesChunk {
    let times = ScalarBuffer::from(vec![100, 200]);
    RawSeriesChunk::try_new(
        &schema(),
        read_key,
        metric,
        RawTimestamps::try_new(times).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0, 2.0]))),
        final_chunk,
    )
    .unwrap()
}

struct MockStream {
    schema: Arc<SeriesSchema>,
    source: RawLabelSource,
    /// The label decode description this stream declares; a key-mode fake derives it from its
    /// binding, as a frame producer would write it into the schema.
    decode: Option<LabelDecodeSpec>,
    ordering: SeriesOrdering,
    items: VecDeque<Poll<Option<Result<RawSeriesChunk, BoxedError>>>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Stream for MockStream {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.items.pop_front().unwrap_or(Poll::Ready(None))
    }
}

impl RawSeriesStream for MockStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.source
    }
    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        self.decode.as_ref()
    }
    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}

impl Drop for MockStream {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn mock_with(
    schema: Arc<SeriesSchema>,
    source: RawLabelSource,
    ordering: SeriesOrdering,
    items: Vec<Poll<Option<Result<RawSeriesChunk, BoxedError>>>>,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let decode = label_decode_spec(&source);
    (
        Box::pin(MockStream {
            schema,
            source,
            decode,
            ordering,
            items: items.into(),
            polls: polls.clone(),
            drops: drops.clone(),
        }),
        polls,
        drops,
    )
}

fn mock(
    source: RawLabelSource,
    items: Vec<Poll<Option<Result<RawSeriesChunk, BoxedError>>>>,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    mock_with(schema(), source, SeriesOrdering::Contiguous, items)
}

fn histogram_batch() -> Arc<HistogramBatch> {
    let custom = [1.5, 4.5];
    let positive_offsets = [1];
    let positive_lengths = [2];
    let positive_buckets = [3.25, 7.5];
    let negative_offsets = [-2];
    let negative_lengths = [1];
    let negative_buckets = [0.75];
    let mut builder = HistogramBatchBuilder::new();
    for (sum, start_timestamp) in [(10.5, Some(-123)), (-9.5, Some(456)), (7.25, None)] {
        builder
            .push(HistogramView {
                schema: -7,
                zero_threshold: 0.125,
                sum,
                count: 17.25,
                zero_count: 2.5,
                reset_hint: CounterResetHint::Unrecognized(37),
                start_timestamp,
                custom_values: &custom,
                positive: BucketSideView {
                    span_offsets: &positive_offsets,
                    span_lengths: &positive_lengths,
                    buckets: &positive_buckets,
                },
                negative: BucketSideView {
                    span_offsets: &negative_offsets,
                    span_lengths: &negative_lengths,
                    buckets: &negative_buckets,
                },
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

fn dense_key() -> Buffer {
    key(
        PrimaryKeyEncoding::Dense,
        &[
            (8, Value::String("last".into())),
            (2, Value::String("hidden".into())),
            (41, Value::String("first".into())),
        ],
    )
}

#[tokio::test]
async fn materializes_complete_labels_without_changing_raw_chunks() {
    let original_key = dense_key();
    let (raw, _, _) = mock(
        source(PrimaryKeyEncoding::Dense),
        vec![Poll::Ready(Some(Ok(chunk(
            original_key.clone(),
            None,
            true,
        ))))],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    assert_eq!(output.ordering(), SeriesOrdering::Contiguous);
    assert_eq!(output.schema().fields.len(), 1);
    let decoded = output.next().await.unwrap().unwrap();
    assert_eq!(decoded.primary_key, original_key);
    assert!(decoded.is_last);
    let metric = decoded.metric.unwrap();
    assert_eq!(metric.name(), Some("requests_total"));
    assert_eq!(metric.tags().len(), 2);
    assert_eq!(metric.get("alpha"), Some("first"));
    assert_eq!(metric.get("zeta"), Some("last"));
    let (raw, _, _) = mock(
        RawLabelSource::Explicit,
        vec![Poll::Ready(Some(Ok(chunk(
            Buffer::from(vec![1, 2]),
            Some(
                MetricName::try_new(
                    None,
                    vec![Label {
                        name: Arc::from("x"),
                        value: Arc::from(""),
                    }],
                )
                .unwrap(),
            ),
            true,
        ))))],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    let decoded = output.next().await.unwrap().unwrap();
    let metric = decoded.metric.unwrap();
    assert_eq!(metric.name(), None);
    assert_eq!(metric.get("x"), Some(""));

    let explicit_metric = MetricName::try_new(
        Some(Arc::from("explicit_metric")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from("a"),
        }],
    )
    .unwrap();
    let (raw, _, _) = mock(
        RawLabelSource::Explicit,
        vec![Poll::Ready(Some(Ok(chunk(
            Buffer::from(vec![1]),
            Some(explicit_metric.clone()),
            true,
        ))))],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    assert_eq!(
        output.next().await.unwrap().unwrap().metric.unwrap(),
        explicit_metric
    );

    let read_key = key(
        PrimaryKeyEncoding::Dense,
        &[
            (8, Value::Null),
            (2, Value::String("hidden".into())),
            (41, Value::String("first".into())),
        ],
    );
    let (raw, _, _) = mock(
        source(PrimaryKeyEncoding::Dense),
        vec![Poll::Ready(Some(Ok(chunk(read_key, None, false))))],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    let metric = output.next().await.unwrap().unwrap().metric.unwrap();
    assert_eq!(metric.get("zeta"), None);
    assert_eq!(metric.get("alpha"), Some("first"));
    assert_eq!(metric.get("private"), None);

    let absent_key = key(
        PrimaryKeyEncoding::Sparse,
        &[(41, Value::String("present".into()))],
    );
    let empty_key = key(
        PrimaryKeyEncoding::Sparse,
        &[
            (8, Value::String("".into())),
            (41, Value::String("present".into())),
        ],
    );
    for (read_key, expected_zeta) in [(absent_key, None), (empty_key, Some(""))] {
        let (raw, _, _) = mock(
            source(PrimaryKeyEncoding::Sparse),
            vec![Poll::Ready(Some(Ok(chunk(read_key, None, true))))],
        );
        let mut output = materialize_raw_labels(raw).unwrap();
        let metric = output.next().await.unwrap().unwrap().metric.unwrap();
        assert_eq!(metric.get("zeta"), expected_zeta);
        assert_eq!(metric.get("alpha"), Some("present"));
    }
}

#[tokio::test]
async fn preserves_sliced_multifield_payload_and_shared_owners() {
    let schema = schema_with_fields(&["float", "histogram", "mixed", "chunks"]);
    let time_owner = ScalarBuffer::from(vec![0, 11, 22, 33, 44]);
    let time_slice = time_owner.slice(1, 3);
    let times = RawTimestamps::try_new(time_slice.clone()).unwrap();
    let float_owner = Float64Array::from(vec![
        Some(90.0),
        None,
        Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        Some(-0.0),
        Some(80.0),
    ]);
    let float_a = float_owner.slice(1, 2);
    let float_b = float_owner.slice(3, 1);
    let floats = Samples::try_from_chunks(vec![
        Samples::from_float(float_a.clone()),
        Samples::from_float(float_b.clone()),
    ])
    .unwrap();

    let table_owner = histogram_batch();
    let table_weak = Arc::downgrade(&table_owner);
    let sliced_table =
        Arc::new(HistogramBatch::try_from_arrow(table_owner.as_arrow().slice(1, 2)).unwrap());
    let sliced_weak = Arc::downgrade(&sliced_table);
    let index_owner = UInt32Array::from(vec![Some(2), Some(0), Some(1), Some(0), Some(1)]);
    let indices = index_owner.slice(1, 3);
    let histograms = Samples::try_from_histogram(indices.clone(), sliced_table.clone()).unwrap();
    let mixed_float_owner =
        Float64Array::from(vec![Some(99.0), Some(1.25), None, None, Some(88.0)]);
    let mixed_float = mixed_float_owner.slice(1, 3);
    let mixed_index_owner = UInt32Array::from(vec![Some(1), None, Some(0), None, Some(1)]);
    let mixed_indices = mixed_index_owner.slice(1, 3);
    let mixed = Samples::try_from_mixed(
        mixed_float.clone(),
        HistogramSamples::try_new(mixed_indices.clone(), sliced_table.clone()).unwrap(),
    )
    .unwrap();
    let chunk_owner_a = Float64Array::from(vec![90.0, 1.0, 80.0]);
    let chunk_owner_b = Float64Array::from(vec![70.0, 2.0, 3.0, 60.0]);
    let chunk_a = chunk_owner_a.slice(1, 1);
    let chunk_b = chunk_owner_b.slice(1, 2);
    let fields = SeriesFields::try_from_vec(vec![
        floats,
        histograms,
        mixed,
        Samples::try_from_chunks(vec![
            Samples::from_float(chunk_a.clone()),
            Samples::from_float(chunk_b.clone()),
        ])
        .unwrap(),
    ])
    .unwrap();
    let payload_key = dense_key();
    let input_chunk =
        RawSeriesChunk::try_new(&schema, payload_key.clone(), None, times, fields, true).unwrap();
    let time_ptr = time_slice.as_ptr();
    let float_ptr = float_a.values().as_ptr();
    let float_b_ptr = float_b.values().as_ptr();
    let histogram_indices_ptr = indices.values().as_ptr();
    let mixed_float_ptr = mixed_float.values().as_ptr();
    let mixed_indices_ptr = mixed_indices.values().as_ptr();
    let chunk_a_ptr = chunk_a.values().as_ptr();
    let chunk_b_ptr = chunk_b.values().as_ptr();
    let (raw, _, drops) = mock_with(
        schema.clone(),
        source(PrimaryKeyEncoding::Dense),
        SeriesOrdering::KeyThenTime,
        vec![Poll::Ready(Some(Ok(input_chunk))), Poll::Ready(None)],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    assert!(Arc::ptr_eq(output.schema(), &schema));
    assert_eq!(output.ordering(), SeriesOrdering::KeyThenTime);
    assert!(matches!(output.label_source(), RawLabelSource::Explicit));
    let decoded = output.next().await.unwrap().unwrap();
    assert_eq!(decoded.time.as_buffer().as_ptr(), time_ptr);
    assert_eq!(decoded.time.as_buffer().as_ref(), &[11, 22, 33]);
    assert_eq!(
        decoded.time.as_buffer().as_ptr(),
        time_owner.as_ptr().wrapping_add(1)
    );
    let DenseSamplesRef::Float(decoded_float) = decoded.fields.get(0).unwrap().segments()[0]
        .dense()
        .unwrap()
    else {
        panic!("expected float")
    };
    assert_eq!(decoded_float.values().as_ptr(), float_ptr);
    assert_eq!(
        decoded_float.values().as_ptr(),
        float_owner.values().as_ptr().wrapping_add(1)
    );
    let DenseSamplesRef::Histogram(decoded_histogram) =
        decoded.fields.get(1).unwrap().dense().unwrap()
    else {
        panic!("expected histogram")
    };
    assert_eq!(
        decoded_histogram.indices().values().as_ptr(),
        histogram_indices_ptr
    );
    assert_eq!(
        decoded_histogram.indices().values().as_ptr(),
        index_owner.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(decoded_histogram.indices().values(), &[0, 1, 0]);
    assert_eq!(decoded_histogram.indices().null_count(), 0);
    assert!(
        decoded_histogram
            .batch()
            .as_arrow()
            .to_data()
            .ptr_eq(&sliced_table.as_arrow().to_data())
    );
    let DenseSamplesRef::Mixed(decoded_mixed) = decoded.fields.get(2).unwrap().dense().unwrap()
    else {
        panic!("expected mixed")
    };
    assert_eq!(decoded_mixed.floats().values().as_ptr(), mixed_float_ptr);
    assert_eq!(
        decoded_mixed.floats().values().as_ptr(),
        mixed_float_owner.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(
        decoded_mixed.histograms().indices().values().as_ptr(),
        mixed_indices_ptr
    );
    assert_eq!(
        decoded_mixed.histograms().indices().values().as_ptr(),
        mixed_index_owner.values().as_ptr().wrapping_add(1)
    );
    assert!(
        decoded_mixed
            .histograms()
            .batch()
            .as_arrow()
            .to_data()
            .ptr_eq(&sliced_table.as_arrow().to_data())
    );
    assert_eq!(decoded.primary_key, payload_key);
    assert_eq!(decoded.primary_key.as_ptr(), payload_key.as_ptr());
    assert!(decoded.is_last);
    assert!(output.next().await.is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    drop(table_owner);
    drop(sliced_table);
    drop(index_owner);
    drop(indices);
    drop(mixed_float_owner);
    drop(mixed_index_owner);
    drop(mixed_float);
    drop(mixed_indices);
    drop(payload_key);
    drop(chunk_owner_a);
    drop(chunk_owner_b);
    drop(chunk_a);
    drop(chunk_b);
    drop(float_owner);
    drop(float_a);
    drop(float_b);
    drop(time_owner);
    drop(time_slice);
    assert!(table_weak.upgrade().is_none());
    assert!(sliced_weak.upgrade().is_some());
    assert_eq!(decoded.time.timestamp(0), Some(11));
    let float_segments = decoded.fields.get(0).unwrap().segments();
    assert_eq!(float_segments.len(), 2);
    for (segment, (ptr, expected_len)) in float_segments
        .iter()
        .zip([(float_ptr, 2), (float_b_ptr, 1)])
    {
        let DenseSamplesRef::Float(values) = segment.dense().unwrap() else {
            panic!("expected float segment");
        };
        assert_eq!(values.values().as_ptr(), ptr);
        assert_eq!(values.len(), expected_len);
    }
    let float_values: Vec<_> = (0..decoded.fields.get(0).unwrap().len())
        .map(|i| match decoded.fields.get(0).unwrap().sample(i) {
            SampleRef::Missing => None,
            SampleRef::Float(v) => Some(v.to_bits()),
            SampleRef::Histogram(_) => panic!("expected float"),
        })
        .collect();
    assert_eq!(
        float_values,
        [
            None,
            Some(0x7ff8_0000_0000_0042),
            Some((-0.0_f64).to_bits())
        ]
    );
    let sums: Vec<_> = (0..3)
        .map(|i| match decoded.fields.get(1).unwrap().sample(i) {
            SampleRef::Histogram(value) => (value.sum, value.start_timestamp),
            _ => panic!("expected histogram"),
        })
        .collect();
    assert_eq!(sums, [(-9.5, Some(456)), (7.25, None), (-9.5, Some(456))]);
    let mixed: Vec<_> = (0..3)
        .map(|i| match decoded.fields.get(2).unwrap().sample(i) {
            SampleRef::Float(v) => Some(v.to_bits()),
            SampleRef::Missing => None,
            SampleRef::Histogram(value) => {
                assert_eq!(value.sum, -9.5);
                assert_eq!(value.start_timestamp, Some(456));
                assert_eq!(value.schema, -7);
                assert_eq!(value.zero_threshold, 0.125);
                assert_eq!(value.count, 17.25);
                assert_eq!(value.zero_count, 2.5);
                assert_eq!(value.reset_hint, CounterResetHint::Unrecognized(37));
                assert_eq!(value.custom_values, &[1.5, 4.5]);
                assert_eq!(value.positive.span_offsets, &[1]);
                assert_eq!(value.positive.span_lengths, &[2]);
                assert_eq!(value.positive.buckets, &[3.25, 7.5]);
                assert_eq!(value.negative.span_offsets, &[-2]);
                assert_eq!(value.negative.span_lengths, &[1]);
                assert_eq!(value.negative.buckets, &[0.75]);
                Some(0)
            }
        })
        .collect();
    assert_eq!(mixed, [Some(1.25_f64.to_bits()), Some(0), None]);
    assert_eq!(decoded.fields.get(3).unwrap().segments().len(), 2);
    for ((segment, expected), ptr) in decoded
        .fields
        .get(3)
        .unwrap()
        .segments()
        .iter()
        .zip([&[1.0][..], &[2.0, 3.0][..]])
        .zip([chunk_a_ptr, chunk_b_ptr])
    {
        let DenseSamplesRef::Float(values) = segment.dense().unwrap() else {
            panic!("expected chunked floats");
        };
        assert_eq!(values.values().as_ref(), expected);
        assert_eq!(values.values().as_ptr(), ptr);
    }
    match decoded.fields.get(3).unwrap().sample(2) {
        SampleRef::Float(value) => assert_eq!(value, 3.0),
        _ => panic!("expected float chunk sample"),
    }
    assert_eq!(sliced_weak.upgrade().unwrap().len(), 2);
    drop(decoded);
    assert!(sliced_weak.upgrade().is_none());
}

#[tokio::test]
async fn pending_is_not_spun_and_eof_drops_input() {
    let physical_metadata = metadata(PrimaryKeyEncoding::Dense);
    let metadata_weak = Arc::downgrade(&physical_metadata);
    let (raw, polls, drops) = mock(
        RawLabelSource::PrimaryKey {
            metric_name: Arc::from("m"),
            physical_metadata,
            logical_tags: Arc::from([]),
        },
        vec![Poll::Pending, Poll::Ready(None)],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    futures::future::poll_fn(|cx| match output.as_mut().poll_next(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("one Pending input must not yield output"),
    })
    .await;
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(output.next().await.is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(metadata_weak.upgrade().is_none());
    assert!(output.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn upstream_error_and_decode_error_drop_source_terminally() {
    let upstream = BoxedError::new(PlainError::new(
        "upstream".to_string(),
        StatusCode::Internal,
    ));
    let physical_metadata = metadata(PrimaryKeyEncoding::Dense);
    let metadata_weak = Arc::downgrade(&physical_metadata);
    let (raw, polls, drops) = mock(
        RawLabelSource::PrimaryKey {
            metric_name: Arc::from("m"),
            physical_metadata,
            logical_tags: Arc::from([]),
        },
        vec![Poll::Ready(Some(Err(upstream)))],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    assert!(output.next().await.unwrap().is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(metadata_weak.upgrade().is_none());
    assert!(output.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    let physical_metadata = metadata(PrimaryKeyEncoding::Dense);
    let metadata_weak = Arc::downgrade(&physical_metadata);
    let (raw, polls, drops) = mock(
        RawLabelSource::PrimaryKey {
            metric_name: Arc::from("m"),
            physical_metadata,
            logical_tags: Arc::from([]),
        },
        vec![Poll::Ready(Some(Ok(chunk(
            Buffer::from(vec![0xff]),
            None,
            false,
        ))))],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    assert!(output.next().await.unwrap().is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(metadata_weak.upgrade().is_none());
    assert!(output.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    let upstream = BoxedError::new(PlainError::new(
        "after chunk".to_string(),
        StatusCode::Internal,
    ));
    let (raw, polls, drops) = mock(
        RawLabelSource::Explicit,
        vec![
            Poll::Ready(Some(Ok(chunk(
                Buffer::from(vec![3]),
                Some(MetricName::try_new(None, vec![]).unwrap()),
                false,
            )))),
            Poll::Ready(Some(Err(upstream))),
            Poll::Ready(Some(Ok(chunk(
                Buffer::from(vec![4]),
                Some(MetricName::try_new(None, vec![]).unwrap()),
                true,
            )))),
        ],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    assert!(output.next().await.unwrap().is_ok());
    assert!(output.next().await.unwrap().is_err());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(output.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancellation_drops_owned_raw_stream_before_and_after_output() {
    let physical_metadata = metadata(PrimaryKeyEncoding::Dense);
    let metadata_weak = Arc::downgrade(&physical_metadata);
    let (raw, _, drops) = mock(
        RawLabelSource::PrimaryKey {
            metric_name: Arc::from("m"),
            physical_metadata,
            logical_tags: Arc::from([]),
        },
        vec![Poll::Pending],
    );
    let output = materialize_raw_labels(raw).unwrap();
    drop(output);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(metadata_weak.upgrade().is_none());

    let physical_metadata = metadata(PrimaryKeyEncoding::Dense);
    let metadata_weak = Arc::downgrade(&physical_metadata);
    let (raw, _, drops) = mock(
        RawLabelSource::PrimaryKey {
            metric_name: Arc::from("requests_total"),
            physical_metadata,
            logical_tags: Arc::from([
                BoundTag {
                    column_id: 8,
                    name: Arc::from("zeta"),
                },
                BoundTag {
                    column_id: 41,
                    name: Arc::from("alpha"),
                },
            ]),
        },
        vec![Poll::Ready(Some(Ok(chunk(dense_key(), None, false))))],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    assert!(output.next().await.unwrap().is_ok());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(metadata_weak.upgrade().is_some());
    drop(output);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(metadata_weak.upgrade().is_none());
}

#[tokio::test]
async fn constructor_and_mode_errors_drop_input_without_polling_ahead() {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let invalid_schemas = [
        Arc::new(SeriesSchema {
            fields: Arc::from([]),
            eval_grid: None,
        }),
        Arc::new(SeriesSchema {
            fields: schema().fields.clone(),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(10, 5, 2).unwrap()).unwrap(),
            )),
        }),
    ];
    for invalid_schema in invalid_schemas {
        let raw: SendableRawSeriesStream = Box::pin(MockStream {
            schema: invalid_schema,
            source: RawLabelSource::Explicit,
            decode: None,
            ordering: SeriesOrdering::Contiguous,
            items: VecDeque::new(),
            polls: polls.clone(),
            drops: drops.clone(),
        });
        assert!(materialize_raw_labels(raw).is_err());
    }
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(polls.load(Ordering::SeqCst), 0);

    for bindings in [
        vec![BoundTag {
            column_id: 8,
            name: Arc::from("wrong"),
        }],
        vec![
            BoundTag {
                column_id: 8,
                name: Arc::from("same"),
            },
            BoundTag {
                column_id: 41,
                name: Arc::from("same"),
            },
        ],
    ] {
        let invalid_source = RawLabelSource::PrimaryKey {
            metric_name: Arc::from("m"),
            physical_metadata: metadata(PrimaryKeyEncoding::Dense),
            logical_tags: bindings.into(),
        };
        let (raw, polls, drops) = mock(invalid_source, vec![]);
        assert!(materialize_raw_labels(raw).is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }

    let malformed_shape = RawSeriesChunk {
        primary_key: Buffer::from(vec![1]),
        metric: Some(MetricName::try_new(None, vec![]).unwrap()),
        time: RawTimestamps::try_new(ScalarBuffer::from(vec![100, 200])).unwrap(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0]))),
        is_last: false,
    };
    let malformed_empty_time = RawSeriesChunk {
        primary_key: Buffer::from(vec![2]),
        metric: Some(MetricName::try_new(None, vec![]).unwrap()),
        time: RawTimestamps::try_new(ScalarBuffer::from(Vec::<i64>::new())).unwrap(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(Vec::<f64>::new()))),
        is_last: false,
    };
    // `RawTimestamps` rejects duplicate or descending axes at construction, so a chunk
    // carrying an out-of-order time axis can no longer be constructed.
    assert!(RawTimestamps::try_new(ScalarBuffer::from(vec![100, 100, 90])).is_err());
    for malformed in [malformed_shape, malformed_empty_time] {
        let (raw, polls, drops) = mock(
            RawLabelSource::Explicit,
            vec![Poll::Ready(Some(Ok(malformed))), Poll::Ready(None)],
        );
        let mut output = materialize_raw_labels(raw).unwrap();
        assert!(output.next().await.unwrap().is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(output.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    for (binding, metric) in [
        (
            source(PrimaryKeyEncoding::Dense),
            Some(MetricName::try_new(None, vec![]).unwrap()),
        ),
        (RawLabelSource::Explicit, None),
    ] {
        let (raw, polls, drops) = mock(
            binding,
            vec![Poll::Ready(Some(Ok(chunk(dense_key(), metric, false))))],
        );
        let mut output = materialize_raw_labels(raw).unwrap();
        assert!(output.next().await.unwrap().is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(output.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn preserves_split_keys_finality_and_independent_streams() {
    let first_key = key(
        PrimaryKeyEncoding::Sparse,
        &[
            (8, Value::String("z".into())),
            (2, Value::String("secret".into())),
            (41, Value::String("a".into())),
        ],
    );
    let second_key = key(
        PrimaryKeyEncoding::Sparse,
        &[
            (8, Value::String("y".into())),
            (2, Value::String("secret".into())),
            (41, Value::String("b".into())),
        ],
    );
    let later_times = RawTimestamps::try_new(ScalarBuffer::from(vec![300, 400])).unwrap();
    let later_chunk = RawSeriesChunk::try_new(
        &schema(),
        first_key.clone(),
        None,
        later_times,
        SeriesFields::one(Samples::from_float(Float64Array::from(vec![3.0, 4.0]))),
        true,
    )
    .unwrap();
    let (raw, _, _) = mock(
        source(PrimaryKeyEncoding::Sparse),
        vec![
            Poll::Ready(Some(Ok(chunk(first_key.clone(), None, false)))),
            Poll::Ready(Some(Ok(later_chunk))),
            Poll::Ready(Some(Ok(chunk(second_key.clone(), None, true)))),
        ],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    let first = output.next().await.unwrap().unwrap();
    let second = output.next().await.unwrap().unwrap();
    let third = output.next().await.unwrap().unwrap();
    assert_eq!(first.primary_key, first_key);
    assert_eq!(second.primary_key, first_key);
    assert_eq!(third.primary_key, second_key);
    assert_eq!(first.time.as_buffer().as_ref(), &[100, 200]);
    assert_eq!(second.time.as_buffer().as_ref(), &[300, 400]);
    assert_eq!(second.metric.as_ref().unwrap().get("zeta"), Some("z"));
    assert!(!first.is_last);
    assert!(second.is_last);
    assert!(third.is_last);
    assert_eq!(first.metric.as_ref().unwrap().get("alpha"), Some("a"));
    assert_eq!(third.metric.as_ref().unwrap().get("alpha"), Some("b"));
    assert!(output.next().await.is_none());

    for _ in 0..2 {
        let (raw, _, _) = mock(
            source(PrimaryKeyEncoding::Sparse),
            vec![Poll::Ready(Some(Ok(chunk(first_key.clone(), None, true))))],
        );
        let mut independent = materialize_raw_labels(raw).unwrap();
        let item = independent.next().await.unwrap().unwrap();
        assert!(item.is_last);
        assert_eq!(item.metric.unwrap().get("alpha"), Some("a"));
        assert!(independent.next().await.is_none());
    }
}

#[tokio::test]
async fn sparse_unknown_id_fails_all_and_drops_source() {
    let mut bytes = Vec::new();
    let codec = SparsePrimaryKeyCodec::schemaless();
    codec.encode_internal(42, 9, &mut bytes).unwrap();
    codec
        .encode_raw_tag_value(
            [
                (41, b"first".as_slice()),
                (2, b"hidden".as_slice()),
                (8, b"last".as_slice()),
                (90, b"unknown".as_slice()),
            ]
            .into_iter(),
            &mut bytes,
        )
        .unwrap();
    let physical_metadata = metadata(PrimaryKeyEncoding::Sparse);
    let metadata_weak = Arc::downgrade(&physical_metadata);
    let (raw, polls, drops) = mock(
        RawLabelSource::PrimaryKey {
            metric_name: Arc::from("requests_total"),
            physical_metadata,
            logical_tags: Arc::from([
                BoundTag {
                    column_id: 8,
                    name: Arc::from("zeta"),
                },
                BoundTag {
                    column_id: 41,
                    name: Arc::from("alpha"),
                },
            ]),
        },
        vec![Poll::Ready(Some(Ok(chunk(
            Buffer::from(bytes),
            None,
            true,
        ))))],
    );
    let mut output = materialize_raw_labels(raw).unwrap();
    assert!(output.next().await.unwrap().is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(metadata_weak.upgrade().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert!(output.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dense_and_sparse_sources_materialize_complete_labels() {
    for encoding in [PrimaryKeyEncoding::Dense, PrimaryKeyEncoding::Sparse] {
        let read_key = key(
            encoding,
            &[
                (8, Value::String("last".into())),
                (2, Value::String("hidden".into())),
                (41, Value::String("first".into())),
            ],
        );
        let (raw, _, _) = mock(
            source(encoding),
            vec![Poll::Ready(Some(Ok(chunk(read_key.clone(), None, true))))],
        );
        let mut output = materialize_raw_labels(raw).unwrap();
        let decoded = output.next().await.unwrap().unwrap();
        assert_eq!(decoded.primary_key, read_key);
        assert_eq!(
            decoded.metric.as_ref().unwrap().name(),
            Some("requests_total")
        );
        let metric = decoded.metric.unwrap();
        assert_eq!(metric.get("alpha"), Some("first"));
        assert_eq!(metric.get("zeta"), Some("last"));
        assert_eq!(metric.get("private"), None);
        assert_eq!(metric.tags().len(), 2);
    }
}

/// A consumer that received only the raw series frame decodes labels with no scan-bound source:
/// the producer's description travels inside the frame schema, so unpacking needs no binding and
/// materialization builds its decoder from that description alone.
#[tokio::test]
async fn materializes_labels_from_a_frame_with_no_supplied_binding() {
    for encoding in [PrimaryKeyEncoding::Dense, PrimaryKeyEncoding::Sparse] {
        let binding = source(encoding);
        let description =
            label_decode_spec(&binding).expect("a primary-key binding describes its key");
        let read_key = key(
            encoding,
            &[
                (8, Value::String("last".into())),
                (2, Value::String("hidden".into())),
                (41, Value::String("first".into())),
            ],
        );
        let (raw, _, _) = mock(
            binding,
            vec![Poll::Ready(Some(Ok(chunk(read_key.clone(), None, true))))],
        );
        let framed = pack_raw_series(raw).unwrap();
        assert_eq!(
            datatypes::series::frame::raw_series_frame_label_decode(framed.schema().as_ref())
                .unwrap(),
            Some(description.clone()),
            "{encoding:?}: the frame schema is the producer's only label statement"
        );

        // Everything the consumer keeps is the frame stream: no expected schema, no binding, no
        // region metadata anywhere on the decode path.
        let framed = unpack_raw_series(framed, None).unwrap();
        assert_eq!(
            framed.label_decode_spec(),
            Some(&description),
            "{encoding:?}: the parsed description survives unpacking without a binding"
        );
        assert!(
            !matches!(framed.label_source(), RawLabelSource::PrimaryKey { .. }),
            "{encoding:?}: no physical region key layout was supplied"
        );
        assert_eq!(
            framed
                .schema()
                .fields
                .iter()
                .map(|field| field.name.to_string())
                .collect::<Vec<_>>(),
            schema()
                .fields
                .iter()
                .map(|field| field.name.to_string())
                .collect::<Vec<_>>(),
            "{encoding:?}: the consumer held no schema, so these are the producer's declared \
             fields read back from the frame"
        );

        let mut output = materialize_raw_labels(framed).unwrap();
        let materialized = output.next().await.unwrap().unwrap();
        assert_eq!(materialized.primary_key, read_key);
        let metric = materialized
            .metric
            .expect("labels were materialized from the frame description");
        assert_eq!(metric.name(), Some("requests_total"));
        assert_eq!(metric.get("zeta"), Some("last"));
        assert_eq!(metric.get("alpha"), Some("first"));
        assert_eq!(metric.get("private"), None);
        assert_eq!(metric.tags().len(), 2);
        assert!(output.next().await.is_none());
    }
}
