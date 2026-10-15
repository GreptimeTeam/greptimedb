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

mod lifecycle;

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::physical_plan::RecordBatchStream;
use datatypes::arrow::array::{Array, Float64Array};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::arrow::compute::concat_batches;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::series::Label;
use datatypes::series::MetricName;
use datatypes::series::raw::{RawSeriesChunk, RawSeriesOrdering, RawTimeAxis};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use futures_util::{Stream, StreamExt};
use store_api::region_engine::{
    BoundTag, RawLabelSource, RawSeriesStream, SendableRawSeriesStream,
};

use super::{pack_raw_series, unpack_raw_series};

fn schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: None,
    })
}
fn metric() -> MetricName {
    MetricName::try_new(Some(Arc::from("cpu")), vec![]).unwrap()
}
fn labeled_metric(host: &str) -> MetricName {
    MetricName::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(host),
        }],
    )
    .unwrap()
}
fn chunk_with(
    key: &[u8],
    times: &[i64],
    values: Vec<Option<f64>>,
    metric: Option<MetricName>,
    last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        &schema(),
        Buffer::from(key.to_vec()),
        metric,
        RawTimeAxis::try_new(ScalarBuffer::from(times.to_vec())).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(values))),
        last,
    )
    .unwrap()
}
fn chunk(key: &[u8], times: &[i64], last: bool) -> RawSeriesChunk {
    chunk_with(
        key,
        times,
        times.iter().map(|t| Some(*t as f64)).collect(),
        Some(metric()),
        last,
    )
}

#[derive(Debug)]
struct RawMock {
    schema: Arc<SeriesSchema>,
    items: VecDeque<Result<RawSeriesChunk, BoxedError>>,
    binding: RawLabelSource,
    ordering: RawSeriesOrdering,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    pending: bool,
    emitted: bool,
}
impl Drop for RawMock {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Stream for RawMock {
    type Item = Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.pending && self.emitted {
            self.pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let item = self.items.pop_front();
        if item.is_some() {
            self.emitted = true;
        }
        Poll::Ready(item)
    }
}
impl RawSeriesStream for RawMock {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.binding
    }
    fn ordering(&self) -> RawSeriesOrdering {
        self.ordering
    }
}
struct DfMock {
    schema: SchemaRef,
    items: VecDeque<datafusion_common::Result<RecordBatch>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    pending: bool,
    emitted: bool,
}
impl Drop for DfMock {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Stream for DfMock {
    type Item = datafusion_common::Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.pending && self.emitted {
            self.pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let item = self.items.pop_front();
        if item.is_some() {
            self.emitted = true;
        }
        Poll::Ready(item)
    }
}
impl RecordBatchStream for DfMock {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
fn raw(
    items: Vec<RawSeriesChunk>,
    ordering: RawSeriesOrdering,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    raw_custom(items, ordering, RawLabelSource::Explicit, false)
}
fn raw_custom(
    items: Vec<RawSeriesChunk>,
    ordering: RawSeriesOrdering,
    binding: RawLabelSource,
    pending: bool,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(RawMock {
            schema: schema(),
            items: items.into_iter().map(Ok).collect(),
            binding,
            ordering,
            polls: polls.clone(),
            drops: drops.clone(),
            pending,
            emitted: false,
        }),
        polls,
        drops,
    )
}
fn df_source(
    batches: Vec<RecordBatch>,
    declared: SchemaRef,
    pending: bool,
) -> (
    DfSendableRecordBatchStream,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(DfMock {
            schema: declared,
            items: batches.into_iter().map(Ok).collect(),
            polls: polls.clone(),
            drops: drops.clone(),
            pending,
            emitted: false,
        }),
        polls,
        drops,
    )
}
async fn pack_all(input: SendableRawSeriesStream) -> Vec<RecordBatch> {
    let mut stream = pack_raw_series(input).unwrap();
    let mut out = vec![];
    while let Some(v) = stream.next().await {
        out.push(v.unwrap());
    }
    out
}
fn assert_metric(chunk: &RawSeriesChunk, host: Option<&str>) {
    let actual = chunk
        .metric
        .as_ref()
        .expect("explicit chunks retain metric");
    assert_eq!(actual.name(), Some("cpu"));
    assert_eq!(actual.tags().len(), usize::from(host.is_some()));
    if let Some(host) = host {
        assert_eq!(actual.tags()[0].name.as_ref(), "host");
        assert_eq!(actual.tags()[0].value.as_ref(), host);
    }
}

fn assert_chunk(
    actual: &RawSeriesChunk,
    key: &[u8],
    times: &[i64],
    values: &[Option<f64>],
    last: bool,
) {
    assert_eq!(actual.read_key.as_slice(), key);
    assert_eq!(
        (0..actual.time.len())
            .map(|i| actual.time.timestamp(i).unwrap())
            .collect::<Vec<_>>(),
        times
    );
    assert_eq!(actual.is_last, last);
    let arr = match actual.fields.as_slice()[0].dense().unwrap() {
        datatypes::series::samples::DenseSamplesRef::Float(a) => a,
        _ => panic!("expected float field"),
    };
    assert_eq!(arr.len(), values.len());
    for (i, value) in values.iter().enumerate() {
        assert_eq!(arr.is_valid(i), value.is_some());
        if let Some(value) = value {
            assert_eq!(arr.value(i).to_bits(), value.to_bits());
        }
    }
}

#[tokio::test]
async fn roundtrip_explicit_unordered_contiguous_preserves_complete_chunk_contents() {
    let chunks = vec![
        chunk_with(
            b"complete-b",
            &[1, 2],
            vec![Some(-0.0), None],
            Some(metric()),
            false,
        ),
        chunk_with(
            b"complete-b",
            &[3],
            vec![Some(f64::from_bits(0x7ff8_0000_0000_0042))],
            Some(metric()),
            true,
        ),
        chunk_with(b"complete-a", &[4], vec![Some(9.25)], Some(metric()), true),
    ];
    let (input, polls, _) = raw(chunks, RawSeriesOrdering::Contiguous);
    let mut packed = pack_raw_series(input).unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let frame_schema = packed.schema();
    let first = packed.next().await.unwrap().unwrap();
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "first frame must precede drain"
    );
    let mut batches = vec![first];
    while let Some(b) = packed.next().await {
        batches.push(b.unwrap());
    }
    let source: DfSendableRecordBatchStream = Box::pin(DfMock {
        schema: frame_schema,
        items: batches.into_iter().map(Ok).collect(),
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
        pending: false,
        emitted: false,
    });
    let mut output = unpack_raw_series(
        source,
        schema(),
        RawLabelSource::Explicit,
        RawSeriesOrdering::Contiguous,
    )
    .unwrap();
    assert_chunk(
        &output.next().await.unwrap().unwrap(),
        b"complete-b",
        &[1, 2],
        &[Some(-0.0), None],
        false,
    );
    assert_chunk(
        &output.next().await.unwrap().unwrap(),
        b"complete-b",
        &[3],
        &[Some(f64::from_bits(0x7ff8_0000_0000_0042))],
        true,
    );
    assert_chunk(
        &output.next().await.unwrap().unwrap(),
        b"complete-a",
        &[4],
        &[Some(9.25)],
        true,
    );
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn read_key_then_time_valid_roundtrip() {
    let (input, _, _) = raw(
        vec![chunk(b"a", &[1], true), chunk(b"b", &[2], true)],
        RawSeriesOrdering::ReadKeyThenTime,
    );
    let batches = pack_all(input).await;
    let frame_schema = batches[0].schema();
    let source: DfSendableRecordBatchStream = Box::pin(DfMock {
        schema: frame_schema,
        items: batches.into_iter().map(Ok).collect(),
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
        pending: false,
        emitted: false,
    });
    let mut out = unpack_raw_series(
        source,
        schema(),
        RawLabelSource::Explicit,
        RawSeriesOrdering::ReadKeyThenTime,
    )
    .unwrap();
    assert_chunk(
        &out.next().await.unwrap().unwrap(),
        b"a",
        &[1],
        &[Some(1.0)],
        true,
    );
    assert_chunk(
        &out.next().await.unwrap().unwrap(),
        b"b",
        &[2],
        &[Some(2.0)],
        true,
    );
    assert!(out.next().await.is_none());
}

#[tokio::test]
async fn invalid_sequence_reports_valid_prefix_then_one_terminal_error() {
    struct Case {
        name: &'static str,
        items: Vec<RawSeriesChunk>,
        order: RawSeriesOrdering,
        expected_prefix_count: usize,
        prefix: Vec<(&'static [u8], i64, bool, Option<&'static str>)>,
    }
    let cases = vec![
        Case {
            name: "final_then_same_key",
            items: vec![chunk(b"a", &[1], true), chunk(b"a", &[2], true)],
            order: RawSeriesOrdering::Contiguous,
            expected_prefix_count: 1,
            prefix: vec![(b"a", 1, true, None)],
        },
        Case {
            name: "unfinished_then_new_key",
            items: vec![chunk(b"a", &[1], false), chunk(b"b", &[2], true)],
            order: RawSeriesOrdering::Contiguous,
            expected_prefix_count: 1,
            prefix: vec![(b"a", 1, false, None)],
        },
        Case {
            name: "nonlocal_recurrence",
            items: vec![
                chunk(b"a", &[1], true),
                chunk(b"b", &[2], true),
                chunk(b"a", &[3], true),
            ],
            order: RawSeriesOrdering::Contiguous,
            expected_prefix_count: 2,
            prefix: vec![(b"a", 1, true, None), (b"b", 2, true, None)],
        },
        Case {
            name: "sorted_key_decrease",
            items: vec![chunk(b"b", &[1], true), chunk(b"a", &[2], true)],
            order: RawSeriesOrdering::ReadKeyThenTime,
            expected_prefix_count: 1,
            prefix: vec![(b"b", 1, true, None)],
        },
        Case {
            name: "timestamp_equal_across_chunks",
            items: vec![chunk(b"a", &[2], false), chunk(b"a", &[2], true)],
            order: RawSeriesOrdering::Contiguous,
            expected_prefix_count: 1,
            prefix: vec![(b"a", 2, false, None)],
        },
        Case {
            name: "timestamp_decreases_across_chunks",
            items: vec![chunk(b"a", &[3], false), chunk(b"a", &[2], true)],
            order: RawSeriesOrdering::Contiguous,
            expected_prefix_count: 1,
            prefix: vec![(b"a", 3, false, None)],
        },
        Case {
            name: "metric_changes_within_key",
            items: vec![
                chunk_with(
                    b"a",
                    &[1],
                    vec![Some(1.0)],
                    Some(labeled_metric("x")),
                    false,
                ),
                chunk_with(b"a", &[2], vec![Some(2.0)], Some(labeled_metric("y")), true),
            ],
            order: RawSeriesOrdering::Contiguous,
            expected_prefix_count: 1,
            prefix: vec![(b"a", 1, false, Some("x"))],
        },
    ];
    for case in cases {
        let _case_name = case.name;
        assert_eq!(case.prefix.len(), case.expected_prefix_count);
        let (input, _, _) = raw(case.items.clone(), case.order);
        let mut s = pack_raw_series(input).unwrap();
        for (key, time, last, host) in &case.prefix {
            let batch = s.next().await.unwrap().unwrap();
            let chunks = datatypes::series::frame::unpack_raw_series_frame(
                &schema(),
                datatypes::series::frame::RawLabelMode::Explicit,
                &batch,
            )
            .unwrap();
            assert_eq!(chunks.len(), 1);
            assert_chunk(&chunks[0], key, &[*time], &[Some(*time as f64)], *last);
            assert_metric(&chunks[0], *host);
        }
        let err = s.next().await.unwrap().unwrap_err();
        assert_eq!(
            crate::error::datafusion_status_code::<crate::error::Error>(&err, None),
            StatusCode::InvalidArguments
        );
        assert!(s.next().await.is_none());

        // Encode chunks independently, bypassing stream validation, to exercise unpack's
        // own cross-frame state machine against precisely the same invalid sequence.
        let mut frames = Vec::new();
        for item in &case.items {
            frames.extend(
                datatypes::series::frame::pack_raw_series_chunk(
                    &schema(),
                    datatypes::series::frame::RawLabelMode::Explicit,
                    item,
                )
                .unwrap(),
            );
        }
        let frame_schema = frames[0].schema();
        let (source, _, _) = df_source(frames, frame_schema, false);
        let mut decoded =
            unpack_raw_series(source, schema(), RawLabelSource::Explicit, case.order).unwrap();
        for (key, time, last, host) in &case.prefix {
            let chunk = decoded.next().await.unwrap().unwrap();
            assert_chunk(&chunk, key, &[*time], &[Some(*time as f64)], *last);
            assert_metric(&chunk, *host);
        }
        let error = decoded.next().await.unwrap().unwrap_err();
        assert_eq!(
            common_error::ext::ErrorExt::status_code(&error),
            StatusCode::InvalidArguments
        );
        assert!(decoded.next().await.is_none());
    }
    let (input, _, _) = raw(
        vec![chunk(b"unfinished", &[7], false)],
        RawSeriesOrdering::Contiguous,
    );
    let mut s = pack_raw_series(input).unwrap();
    assert!(s.next().await.unwrap().is_ok());
    assert!(s.next().await.unwrap().is_err());
    assert!(s.next().await.is_none());
    let frame = datatypes::series::frame::pack_raw_series_chunk(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &chunk(b"unfinished", &[7], false),
    )
    .unwrap();
    let frame_schema = frame[0].schema();
    let (source, _, _) = df_source(frame, frame_schema, false);
    let mut decoded = unpack_raw_series(
        source,
        schema(),
        RawLabelSource::Explicit,
        RawSeriesOrdering::Contiguous,
    )
    .unwrap();
    let first = decoded.next().await.unwrap().unwrap();
    assert_chunk(&first, b"unfinished", &[7], &[Some(7.0)], false);
    assert_eq!(
        common_error::ext::ErrorExt::status_code(&decoded.next().await.unwrap().unwrap_err()),
        StatusCode::InvalidArguments
    );
    assert!(decoded.next().await.is_none());
}

fn primary_binding(metadata: store_api::metadata::RegionMetadataRef) -> RawLabelSource {
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from("cpu"),
        physical_metadata: metadata,
        logical_tags: Arc::from([BoundTag {
            column_id: 0,
            name: Arc::from("host"),
        }]),
    }
}

#[tokio::test]
async fn primary_key_binding_is_retained_exactly() {
    use api::v1::SemanticType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "host",
                datatypes::data_type::ConcreteDataType::string_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                datatypes::data_type::ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 1,
        })
        .primary_key(vec![0]);
    let metadata = Arc::new(builder.build().unwrap());
    let binding = primary_binding(metadata.clone());
    let (input, _, _) = raw_custom(
        vec![
            RawSeriesChunk::try_new(
                &schema(),
                Buffer::from(b"encoded-complete-key".to_vec()),
                None,
                RawTimeAxis::try_new(ScalarBuffer::from(vec![1])).unwrap(),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(3.0)]))),
                true,
            )
            .unwrap(),
        ],
        RawSeriesOrdering::Contiguous,
        binding.clone(),
        false,
    );
    let mut packed = pack_raw_series(input).unwrap();
    let batch = packed.next().await.unwrap().unwrap();
    let source: DfSendableRecordBatchStream = Box::pin(DfMock {
        schema: batch.schema(),
        items: VecDeque::from([Ok(batch)]),
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
        pending: false,
        emitted: false,
    });
    let mut out =
        unpack_raw_series(source, schema(), binding, RawSeriesOrdering::Contiguous).unwrap();
    let decoded = out.next().await.unwrap().unwrap();
    assert_eq!(decoded.read_key.as_slice(), b"encoded-complete-key");
    assert!(decoded.metric.is_none());
    let retained = out.label_source();
    match retained {
        RawLabelSource::PrimaryKey {
            metric_name,
            physical_metadata,
            logical_tags,
        } => {
            assert!(Arc::ptr_eq(physical_metadata, &metadata));
            assert_eq!(metric_name.as_ref(), "cpu");
            assert_eq!(logical_tags.as_ref().len(), 1);
            assert_eq!(logical_tags[0].column_id, 0);
            assert_eq!(logical_tags[0].name.as_ref(), "host");
        }
        _ => panic!("binding changed"),
    }
}

#[tokio::test]
async fn explicit_mode_rejects_key_payload_and_primary_mode_rejects_explicit_metric() {
    let (input, _, _) = raw(
        vec![
            RawSeriesChunk::try_new(
                &schema(),
                Buffer::from(b"a".to_vec()),
                None,
                RawTimeAxis::try_new(ScalarBuffer::from(vec![1])).unwrap(),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(1.0)]))),
                true,
            )
            .unwrap(),
        ],
        RawSeriesOrdering::Contiguous,
    );
    let mut s = pack_raw_series(input).unwrap();
    assert!(s.next().await.unwrap().is_err());
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn primary_key_mode_rejects_explicit_metric_payload() {
    let mut builder =
        store_api::metadata::RegionMetadataBuilder::new(store_api::storage::RegionId::new(1, 2));
    use api::v1::SemanticType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "host",
                datatypes::data_type::ConcreteDataType::string_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                datatypes::data_type::ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 1,
        })
        .primary_key(vec![0]);
    let metadata = Arc::new(builder.build().unwrap());
    let (input, _, _) = raw_custom(
        vec![chunk(b"a", &[1], true)],
        RawSeriesOrdering::Contiguous,
        primary_binding(metadata),
        false,
    );
    let mut stream = pack_raw_series(input).unwrap();
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(
            &stream.next().await.unwrap().unwrap_err(),
            None,
        ),
        StatusCode::InvalidArguments
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn explicit_metric_must_remain_stable_within_key() {
    let (input, _, _) = raw(
        vec![
            chunk_with(
                b"a",
                &[1],
                vec![Some(1.0)],
                Some(labeled_metric("x")),
                false,
            ),
            chunk_with(b"a", &[2], vec![Some(2.0)], Some(labeled_metric("y")), true),
        ],
        RawSeriesOrdering::Contiguous,
    );
    let mut s = pack_raw_series(input).unwrap();
    assert!(s.next().await.unwrap().is_ok());
    assert!(s.next().await.unwrap().is_err());
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn locally_invalid_public_raw_chunk_is_rejected() {
    let mut bad = chunk(b"a", &[1], true);
    bad.fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
    ])));
    let polls = Arc::new(AtomicUsize::new(0));
    let source = Box::pin(RawMock {
        schema: schema(),
        items: VecDeque::from([Ok(bad)]),
        binding: RawLabelSource::Explicit,
        ordering: RawSeriesOrdering::Contiguous,
        polls,
        drops: Arc::new(AtomicUsize::new(0)),
        pending: false,
        emitted: false,
    });
    let mut s = pack_raw_series(source).unwrap();
    let err = s.next().await.unwrap().unwrap_err();
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&err, None),
        StatusCode::InvalidArguments
    );
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn empty_streams_keep_self_describing_schema() {
    let (input, _, _) = raw(vec![], RawSeriesOrdering::Contiguous);
    let mut packed = pack_raw_series(input).unwrap();
    let frame_schema = packed.schema();
    assert!(packed.next().await.is_none());
    let (source, polls, _) = df_source(vec![], frame_schema.clone(), false);
    let mut unpacked = unpack_raw_series(
        source,
        schema(),
        RawLabelSource::Explicit,
        RawSeriesOrdering::Contiguous,
    )
    .unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(unpacked.schema().fields.len(), 1);
    assert!(unpacked.next().await.is_none());
}

#[tokio::test]
async fn upstream_error_status_is_preserved_once_and_releases_source() {
    for status in [StatusCode::RequestOutdated, StatusCode::Unsupported] {
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let e = BoxedError::new(PlainError::new("upstream".into(), status));
        let source = Box::pin(RawMock {
            schema: schema(),
            items: VecDeque::from([Err(e)]),
            binding: RawLabelSource::Explicit,
            ordering: RawSeriesOrdering::Contiguous,
            polls,
            drops: drops.clone(),
            pending: false,
            emitted: false,
        });
        let mut packed = pack_raw_series(source).unwrap();
        let error = packed.next().await.unwrap().unwrap_err();
        assert_eq!(
            crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
            status
        );
        assert!(packed.next().await.is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let batch_schema = datatypes::series::frame::raw_series_frame_schema(
            &schema(),
            datatypes::series::frame::RawLabelMode::Explicit,
        )
        .unwrap();
        let plain = PlainError::new("upstream df".into(), status);
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let source: DfSendableRecordBatchStream = Box::pin(DfMock {
            schema: batch_schema,
            items: VecDeque::from([Err(datafusion_common::DataFusionError::External(Box::new(
                BoxedError::new(plain),
            )))]),
            polls: polls.clone(),
            drops: drops.clone(),
            pending: false,
            emitted: false,
        });
        let mut unpacked = unpack_raw_series(
            source,
            schema(),
            RawLabelSource::Explicit,
            RawSeriesOrdering::Contiguous,
        )
        .unwrap();
        let error = unpacked.next().await.unwrap().unwrap_err();
        assert_eq!(common_error::ext::ErrorExt::status_code(&error), status);
        assert!(unpacked.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn dropping_adapter_releases_upstream_and_pending_item_is_resumed() {
    let (input, polls, drops) = raw_custom(
        vec![chunk(b"a", &[1], true), chunk(b"b", &[2], true)],
        RawSeriesOrdering::Contiguous,
        RawLabelSource::Explicit,
        true,
    );
    let mut packed = pack_raw_series(input).unwrap();
    assert!(packed.next().await.unwrap().is_ok());
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "first item is emitted immediately"
    );
    assert!(
        packed.next().await.unwrap().is_ok(),
        "Pending is resumed before the next item"
    );
    assert_eq!(polls.load(Ordering::SeqCst), 3);
    drop(packed);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unpack_rejects_mismatched_batch_schema_before_polling() {
    let wrong = datatypes::arrow::datatypes::Schema::empty().into();
    let (source, polls, drops) = df_source(vec![], wrong, false);
    assert!(
        unpack_raw_series(
            source,
            schema(),
            RawLabelSource::Explicit,
            RawSeriesOrdering::Contiguous
        )
        .is_err()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_grid_schema_rejected_for_empty_raw_stream() {
    let mut invalid = (*schema()).clone();
    invalid.eval_grid = Some(Arc::new(
        datatypes::series::time::EvalGrid::try_new(
            datatypes::series::time::GridSpec::try_new(0, 10, 1).unwrap(),
        )
        .unwrap(),
    ));
    let polls = Arc::new(AtomicUsize::new(0));
    let source = Box::pin(RawMock {
        schema: Arc::new(invalid),
        items: VecDeque::new(),
        binding: RawLabelSource::Explicit,
        ordering: RawSeriesOrdering::Contiguous,
        polls: polls.clone(),
        drops: Arc::new(AtomicUsize::new(0)),
        pending: false,
        emitted: false,
    });
    assert!(pack_raw_series(source).is_err());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unpack_batch_sequence_error_is_atomic_for_all_rows() {
    let mut batches = Vec::new();
    let frame_schema = datatypes::series::frame::raw_series_frame_schema(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
    )
    .unwrap();
    for (key, time) in [
        (b"a".as_slice(), 1),
        (b"b".as_slice(), 2),
        (b"a".as_slice(), 3),
    ] {
        let (input, _, _) = raw(
            vec![chunk(key, &[time], true)],
            RawSeriesOrdering::Contiguous,
        );
        batches.extend(pack_all(input).await);
    }
    let multi = concat_batches(&frame_schema, &batches).unwrap();
    let (source, _, _) = df_source(vec![multi], frame_schema, false);
    let mut unpacked = unpack_raw_series(
        source,
        schema(),
        RawLabelSource::Explicit,
        RawSeriesOrdering::Contiguous,
    )
    .unwrap();
    assert!(unpacked.next().await.unwrap().is_err());
    assert!(unpacked.next().await.is_none());
}

#[tokio::test]
async fn unpack_rejects_late_invalid_frame_row_before_prefix() {
    let frame_schema = datatypes::series::frame::raw_series_frame_schema(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
    )
    .unwrap();
    let first = datatypes::series::frame::pack_raw_series_chunk(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &chunk(b"a", &[1], true),
    )
    .unwrap()
    .remove(0);
    let mut second = datatypes::series::frame::pack_raw_series_chunk(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &chunk(b"b", &[2], true),
    )
    .unwrap()
    .remove(0);
    let mut columns = second.columns().to_vec();
    let time_struct = columns[2]
        .as_any()
        .downcast_ref::<datatypes::arrow::array::StructArray>()
        .unwrap();
    let lists = time_struct
        .column(0)
        .as_any()
        .downcast_ref::<datatypes::arrow::array::ListArray>()
        .unwrap();
    let datatypes::arrow::datatypes::DataType::List(list_field) = lists.data_type() else {
        panic!("expected list")
    };
    let invalid_times = datatypes::arrow::array::Int64Array::from(vec![2, 1]);
    let invalid_list = datatypes::arrow::array::ListArray::new(
        list_field.clone(),
        datatypes::arrow::buffer::OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
        Arc::new(invalid_times),
        None,
    );
    columns[2] = Arc::new(
        datatypes::arrow::array::StructArray::try_new(
            time_struct.fields().clone(),
            vec![Arc::new(invalid_list)],
            None,
        )
        .unwrap(),
    );
    second = RecordBatch::try_new(frame_schema.clone(), columns).unwrap();
    let multi = concat_batches(&frame_schema, &[first, second]).unwrap();
    let (source, _, _) = df_source(vec![multi], frame_schema, false);
    let mut unpacked = unpack_raw_series(
        source,
        schema(),
        RawLabelSource::Explicit,
        RawSeriesOrdering::Contiguous,
    )
    .unwrap();
    let error = unpacked.next().await.unwrap().unwrap_err();
    assert_eq!(
        common_error::ext::ErrorExt::status_code(&error),
        StatusCode::InvalidArguments
    );
    assert!(unpacked.next().await.is_none());
}

#[tokio::test]
async fn frame_application_metadata_is_stable_and_schema_change_rejected() {
    let base_schema = datatypes::series::frame::raw_series_frame_schema(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
    )
    .unwrap();
    let mut application_metadata = base_schema.metadata().clone();
    application_metadata.insert("app".into(), "v1".into());
    let declared = Arc::new(
        (*base_schema)
            .clone()
            .with_metadata(application_metadata.clone()),
    );
    let first = datatypes::series::frame::pack_raw_series_chunk(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &chunk(b"a", &[1], true),
    )
    .unwrap()
    .remove(0)
    .with_schema(declared.clone())
    .unwrap();
    let stable_second = datatypes::series::frame::pack_raw_series_chunk(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &chunk(b"b", &[2], true),
    )
    .unwrap()
    .remove(0)
    .with_schema(declared.clone())
    .unwrap();
    let (source, _, _) = df_source(vec![first.clone(), stable_second], declared.clone(), false);
    let mut stable = unpack_raw_series(
        source,
        schema(),
        RawLabelSource::Explicit,
        RawSeriesOrdering::Contiguous,
    )
    .unwrap();
    assert_chunk(
        &stable.next().await.unwrap().unwrap(),
        b"a",
        &[1],
        &[Some(1.0)],
        true,
    );
    assert_chunk(
        &stable.next().await.unwrap().unwrap(),
        b"b",
        &[2],
        &[Some(2.0)],
        true,
    );
    assert!(stable.next().await.is_none());

    for changed_value in [Some("v2"), None] {
        let mut changed = application_metadata.clone();
        if let Some(value) = changed_value {
            changed.insert("app".into(), value.into());
        } else {
            changed.remove("app");
        }
        let changed_schema = Arc::new((*base_schema).clone().with_metadata(changed));
        let bad_batch = datatypes::series::frame::pack_raw_series_chunk(
            &schema(),
            datatypes::series::frame::RawLabelMode::Explicit,
            &chunk(b"b", &[2], true),
        )
        .unwrap()
        .remove(0)
        .with_schema(changed_schema)
        .unwrap();
        let (source, _, _) = df_source(vec![first.clone(), bad_batch], declared.clone(), false);
        let mut unpacked = unpack_raw_series(
            source,
            schema(),
            RawLabelSource::Explicit,
            RawSeriesOrdering::Contiguous,
        )
        .unwrap();
        assert_chunk(
            &unpacked.next().await.unwrap().unwrap(),
            b"a",
            &[1],
            &[Some(1.0)],
            true,
        );
        let error = unpacked.next().await.unwrap().unwrap_err();
        assert_eq!(
            common_error::ext::ErrorExt::status_code(&error),
            StatusCode::InvalidArguments
        );
        assert!(unpacked.next().await.is_none());
    }
}

#[tokio::test]
async fn unpack_drains_many_empty_batches_before_the_next_row() {
    let (input, _, _) = raw(
        vec![chunk(b"after-empty", &[9], true)],
        RawSeriesOrdering::Contiguous,
    );
    let mut packed = pack_raw_series(input).unwrap();
    let row = packed.next().await.unwrap().unwrap();
    let frame_schema = row.schema();
    let mut batches = vec![RecordBatch::new_empty(frame_schema.clone()); 10_000];
    batches.push(row);
    let (source, polls, _) = df_source(batches, frame_schema, false);
    let mut unpacked = unpack_raw_series(
        source,
        schema(),
        RawLabelSource::Explicit,
        RawSeriesOrdering::Contiguous,
    )
    .unwrap();
    assert_chunk(
        &unpacked.next().await.unwrap().unwrap(),
        b"after-empty",
        &[9],
        &[Some(9.0)],
        true,
    );
    assert_eq!(polls.load(Ordering::SeqCst), 10_001);
    assert!(unpacked.next().await.is_none());
}

#[tokio::test]
async fn unpack_drains_decoded_rows_before_polling_another_batch() {
    let (input, _, _) = raw(
        vec![chunk(b"a", &[1], true), chunk(b"b", &[2], true)],
        RawSeriesOrdering::ReadKeyThenTime,
    );
    let batches = pack_all(input).await;
    let frame_schema = batches[0].schema();
    let multi = concat_batches(&frame_schema, &batches).unwrap();
    let (source, polls, _) = df_source(vec![multi], frame_schema, false);
    let mut unpacked = unpack_raw_series(
        source,
        schema(),
        RawLabelSource::Explicit,
        RawSeriesOrdering::ReadKeyThenTime,
    )
    .unwrap();
    assert_chunk(
        &unpacked.next().await.unwrap().unwrap(),
        b"a",
        &[1],
        &[Some(1.0)],
        true,
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_chunk(
        &unpacked.next().await.unwrap().unwrap(),
        b"b",
        &[2],
        &[Some(2.0)],
        true,
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "decoded pending row precedes upstream polling"
    );
    assert!(unpacked.next().await.is_none());
}

#[tokio::test]
async fn unpack_preflights_empty_and_grid_schemas_without_polling() {
    let invalid_schemas = [
        SeriesSchema {
            fields: Arc::from([]),
            eval_grid: None,
        },
        SeriesSchema {
            fields: schema().fields.clone(),
            eval_grid: Some(Arc::new(
                datatypes::series::time::EvalGrid::try_new(
                    datatypes::series::time::GridSpec::try_new(0, 10, 1).unwrap(),
                )
                .unwrap(),
            )),
        },
    ];
    for expected in invalid_schemas {
        let frame_schema = datatypes::series::frame::raw_series_frame_schema(
            &schema(),
            datatypes::series::frame::RawLabelMode::Explicit,
        )
        .unwrap();
        let (source, polls, drops) = df_source(vec![], frame_schema, false);
        assert!(
            unpack_raw_series(
                source,
                Arc::new(expected),
                RawLabelSource::Explicit,
                RawSeriesOrdering::Contiguous,
            )
            .is_err()
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
