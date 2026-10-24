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

use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::physical_plan::RecordBatchStream;
use datatypes::arrow::array::{Array, Float64Array, UInt32Array};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::arrow::compute::concat_batches;
use datatypes::arrow::datatypes::{DataType, Schema, SchemaRef};
use datatypes::arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::series::Label;
use datatypes::series::SeriesLabels;
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{LabelDecodeSpec, RawLabelMode, raw_series_frame_schema};
use datatypes::series::histogram::{BucketSideView, CounterResetHint, HistogramBatchBuilder};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema,
};
use futures_util::{Stream, StreamExt};
use store_api::region_engine::{
    BoundTag, RawLabelSource, RawSeriesStream, SendableRawSeriesStream,
};

use super::{
    ensure_frame_matches_label_source, label_decode_spec, pack_raw_series,
    raw_series_frame_ordering, unpack_raw_series,
};

fn schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: None,
    })
}
fn metric() -> SeriesLabels {
    SeriesLabels::try_new(Some(Arc::from("cpu")), vec![]).unwrap()
}
fn labeled_metric(host: &str) -> SeriesLabels {
    SeriesLabels::try_new(
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
    metric: Option<SeriesLabels>,
    last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        &schema(),
        Buffer::from(key.to_vec()),
        metric,
        RawTimestamps::try_new(ScalarBuffer::from(times.to_vec())).unwrap(),
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
    /// The description this fixture declares, derived once from `binding`.
    label_decode_spec: Option<LabelDecodeSpec>,
    ordering: SeriesOrdering,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    /// After this many rows were returned, the next poll reports `Pending` once and wakes the
    /// task. `None` keeps the source always ready.
    pending_after: Option<usize>,
    pending_reported: bool,
    /// Fails instead of polling, proving a constructor rejected the source before any poll.
    panic_on_poll: bool,
    emitted: usize,
}
impl Drop for RawMock {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Stream for RawMock {
    type Item = Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(
            !self.panic_on_poll,
            "a source rejected before polling must never be polled"
        );
        self.polls.fetch_add(1, Ordering::SeqCst);
        if !self.pending_reported && self.pending_after == Some(self.emitted) {
            self.pending_reported = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let item = self.items.pop_front();
        if item.is_some() {
            self.emitted += 1;
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
    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        self.label_decode_spec.as_ref()
    }
    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}
struct DfMock {
    schema: SchemaRef,
    items: VecDeque<datafusion_common::Result<RecordBatch>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    /// After this many batches were returned, the next poll reports `Pending` once and wakes
    /// the task. `None` keeps the source always ready.
    pending_after: Option<usize>,
    pending_reported: bool,
    /// Fails instead of polling, proving a constructor rejected the source before any poll.
    panic_on_poll: bool,
    emitted: usize,
}
impl Drop for DfMock {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Stream for DfMock {
    type Item = datafusion_common::Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(
            !self.panic_on_poll,
            "a source rejected before polling must never be polled"
        );
        self.polls.fetch_add(1, Ordering::SeqCst);
        if !self.pending_reported && self.pending_after == Some(self.emitted) {
            self.pending_reported = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let item = self.items.pop_front();
        if item.is_some() {
            self.emitted += 1;
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
    ordering: SeriesOrdering,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    raw_items(
        items.into_iter().map(Ok).collect(),
        schema(),
        RawLabelSource::Explicit,
        ordering,
        None,
    )
}
fn raw_pending(
    items: Vec<RawSeriesChunk>,
    ordering: SeriesOrdering,
    pending_after: usize,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    raw_items(
        items.into_iter().map(Ok).collect(),
        schema(),
        RawLabelSource::Explicit,
        ordering,
        Some(pending_after),
    )
}
fn raw_custom(
    items: Vec<RawSeriesChunk>,
    ordering: SeriesOrdering,
    binding: RawLabelSource,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    raw_items(
        items.into_iter().map(Ok).collect(),
        schema(),
        binding,
        ordering,
        None,
    )
}
fn raw_never_polled(
    ordering: SeriesOrdering,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(RawMock {
            schema: schema(),
            items: VecDeque::new(),
            binding: RawLabelSource::Explicit,
            label_decode_spec: None,
            ordering,
            polls: polls.clone(),
            drops: drops.clone(),
            pending_after: None,
            pending_reported: false,
            panic_on_poll: true,
            emitted: 0,
        }),
        polls,
        drops,
    )
}
fn raw_items(
    items: Vec<Result<RawSeriesChunk, BoxedError>>,
    schema: Arc<SeriesSchema>,
    binding: RawLabelSource,
    ordering: SeriesOrdering,
    pending_after: Option<usize>,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    // A fixture holding a key binding declares the layout it encodes, like a real producer; a
    // fixture with explicit labels already carries complete ones and declares none.
    let label_decode_spec = label_decode_spec(&binding);
    (
        Box::pin(RawMock {
            schema,
            items: items.into(),
            binding,
            label_decode_spec,
            ordering,
            polls: polls.clone(),
            drops: drops.clone(),
            pending_after,
            pending_reported: false,
            panic_on_poll: false,
            emitted: 0,
        }),
        polls,
        drops,
    )
}
fn df_source(
    batches: Vec<RecordBatch>,
    declared: SchemaRef,
) -> (
    DfSendableRecordBatchStream,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    df_items(batches.into_iter().map(Ok).collect(), declared, None)
}
fn df_pending(
    batches: Vec<RecordBatch>,
    declared: SchemaRef,
    pending_after: usize,
) -> (
    DfSendableRecordBatchStream,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    df_items(
        batches.into_iter().map(Ok).collect(),
        declared,
        Some(pending_after),
    )
}
fn df_never_polled(
    declared: SchemaRef,
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
            items: VecDeque::new(),
            polls: polls.clone(),
            drops: drops.clone(),
            pending_after: None,
            pending_reported: false,
            panic_on_poll: true,
            emitted: 0,
        }),
        polls,
        drops,
    )
}
fn df_items(
    items: Vec<datafusion_common::Result<RecordBatch>>,
    declared: SchemaRef,
    pending_after: Option<usize>,
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
            items: items.into(),
            polls: polls.clone(),
            drops: drops.clone(),
            pending_after,
            pending_reported: false,
            panic_on_poll: false,
            emitted: 0,
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

#[tokio::test]
async fn contiguous_adapter_does_not_certify_nonadjacent_recurrence() {
    // A/B/A violates the producer's contiguous-key contract; adapters validate adjacent chunks
    // only and preserve the supplied sequence rather than sorting, merging, or dropping rows.
    let expected = [
        (b"a".as_slice(), 1, 11.0),
        (b"b".as_slice(), 2, 22.0),
        (b"a".as_slice(), 3, 33.0),
    ];
    let input = expected
        .iter()
        .map(|(key, time, value)| {
            chunk_with(key, &[*time], vec![Some(*value)], Some(metric()), true)
        })
        .collect();
    let (source, _, _) = raw(input, SeriesOrdering::Contiguous);
    let batches = pack_all(source).await;
    assert_eq!(batches.len(), 1, "ready chunks coalesce into one batch");
    assert_eq!(batches[0].num_rows(), 3);
    let frame_schema = batches[0].schema();
    let (source, _, _) = df_source(batches, frame_schema);
    let mut decoded = unpack_raw_series(source, Some(schema())).unwrap();

    for (key, time, value) in expected {
        let actual = decoded.next().await.unwrap().unwrap();
        assert_chunk(&actual, key, &[time], &[Some(value)], true);
        assert_metric(&actual, None);
    }
    assert!(decoded.next().await.is_none());
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
    assert_eq!(actual.primary_key.as_slice(), key);
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
    let (input, polls, _) = raw(chunks, SeriesOrdering::Contiguous);
    let mut packed = pack_raw_series(input).unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let frame_schema = packed.schema();
    let first = packed.next().await.unwrap().unwrap();
    assert_eq!(
        first.num_rows(),
        3,
        "three dense chunks coalesce into three frame rows, containing four positions"
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        4,
        "the complete batch is flushed once EOF is observed"
    );
    assert!(packed.next().await.is_none());
    let batches = vec![first];
    let source: DfSendableRecordBatchStream = Box::pin(DfMock {
        schema: frame_schema,
        items: batches.into_iter().map(Ok).collect(),
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
        pending_after: None,
        pending_reported: false,
        panic_on_poll: false,
        emitted: 0,
    });
    let mut output = unpack_raw_series(source, Some(schema())).unwrap();
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
async fn key_then_time_valid_roundtrip() {
    let (input, _, _) = raw(
        vec![chunk(b"a", &[1], true), chunk(b"b", &[2], true)],
        SeriesOrdering::KeyThenTime,
    );
    let batches = pack_all(input).await;
    assert_eq!(batches.len(), 1, "cross-chunk rows share one bounded batch");
    assert_eq!(batches[0].num_rows(), 2);
    let frame_schema = batches[0].schema();
    let source: DfSendableRecordBatchStream = Box::pin(DfMock {
        schema: frame_schema,
        items: batches.into_iter().map(Ok).collect(),
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
        pending_after: None,
        pending_reported: false,
        panic_on_poll: false,
        emitted: 0,
    });
    let mut out = unpack_raw_series(source, Some(schema())).unwrap();
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
        order: SeriesOrdering,
        prefix: Vec<(&'static [u8], i64, bool, Option<&'static str>)>,
    }
    let cases = vec![
        Case {
            name: "final_then_same_key",
            items: vec![chunk(b"a", &[1], true), chunk(b"a", &[2], true)],
            order: SeriesOrdering::Contiguous,
            prefix: vec![(b"a", 1, true, None)],
        },
        Case {
            name: "unfinished_then_new_key",
            items: vec![chunk(b"a", &[1], false), chunk(b"b", &[2], true)],
            order: SeriesOrdering::Contiguous,
            prefix: vec![(b"a", 1, false, None)],
        },
        Case {
            name: "key_decrease_after_intervening_keys",
            items: vec![
                chunk(b"a", &[1], true),
                chunk(b"b", &[2], true),
                chunk(b"a", &[3], true),
            ],
            order: SeriesOrdering::KeyThenTime,
            prefix: vec![(b"a", 1, true, None), (b"b", 2, true, None)],
        },
        Case {
            name: "sorted_key_decrease",
            items: vec![chunk(b"b", &[1], true), chunk(b"a", &[2], true)],
            order: SeriesOrdering::KeyThenTime,
            prefix: vec![(b"b", 1, true, None)],
        },
        Case {
            name: "timestamp_equal_across_chunks",
            items: vec![chunk(b"a", &[2], false), chunk(b"a", &[2], true)],
            order: SeriesOrdering::Contiguous,
            prefix: vec![(b"a", 2, false, None)],
        },
        Case {
            name: "timestamp_decreases_across_chunks",
            items: vec![chunk(b"a", &[3], false), chunk(b"a", &[2], true)],
            order: SeriesOrdering::Contiguous,
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
            order: SeriesOrdering::Contiguous,
            prefix: vec![(b"a", 1, false, Some("x"))],
        },
    ];
    for case in cases {
        // The valid prefix is flushed by an upstream `Pending`, so the test observes both the
        // retained rows and the single terminal error honestly under the batching builder.
        let (input, _, _) = raw_pending(case.items.clone(), case.order, case.prefix.len());
        let mut s = pack_raw_series(input).unwrap();
        if !case.prefix.is_empty() {
            let batch = s.next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), case.prefix.len(), "{}", case.name);
            let chunks = datatypes::series::frame::unpack_raw_series_frame(
                &schema(),
                datatypes::series::frame::RawLabelMode::Explicit,
                &batch,
            )
            .unwrap();
            assert_eq!(chunks.len(), case.prefix.len(), "{}", case.name);
            for (chunk, prefix) in chunks.iter().zip(&case.prefix) {
                let (key, time, last, host) = *prefix;
                assert_chunk(chunk, key, &[time], &[Some(time as f64)], last);
                assert_metric(chunk, host);
            }
        }
        let err = s.next().await.unwrap().unwrap_err();
        assert_eq!(
            crate::error::datafusion_status_code::<crate::error::Error>(&err, None),
            StatusCode::InvalidArguments,
            "{}",
            case.name
        );
        assert!(s.next().await.is_none(), "{}", case.name);

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
        // The ordering is a property of the frame, so the frames must declare the case's one for
        // unpack to validate the same sequence the producer rejected.
        let frame_schema = super::raw_series_frame_schema_with_ordering(
            &schema(),
            &RawLabelSource::Explicit,
            case.order,
        )
        .unwrap();
        let frames = frames
            .into_iter()
            .map(|frame| frame.with_schema(frame_schema.clone()).unwrap())
            .collect();
        let (source, _, _) = df_source(frames, frame_schema);
        let mut decoded = unpack_raw_series(source, Some(schema())).unwrap();
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
    // The unpack side of a non-final stream: the row arrives with `is_last` false and the
    // missing final key is reported once, at EOF.
    let frame = datatypes::series::frame::pack_raw_series_chunk(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &chunk(b"unfinished", &[7], false),
    )
    .unwrap();
    let frame_schema = frame[0].schema();
    let (source, _, _) = df_source(frame, frame_schema);
    let mut decoded = unpack_raw_series(source, Some(schema())).unwrap();
    let first = decoded.next().await.unwrap().unwrap();
    assert_chunk(&first, b"unfinished", &[7], &[Some(7.0)], false);
    assert_eq!(
        common_error::ext::ErrorExt::status_code(&decoded.next().await.unwrap().unwrap_err()),
        StatusCode::InvalidArguments
    );
    assert!(decoded.next().await.is_none());
}

/// EOF before a non-final key is an error: buffered rows are not reported as a partial success.
#[tokio::test]
async fn pack_rejects_nonfinal_eof_without_buffered_partial_success() {
    let (input, polls, _) = raw(
        vec![chunk_with(
            b"unfinished",
            &[1, 2],
            vec![Some(1.0), Some(2.0)],
            Some(metric()),
            false,
        )],
        SeriesOrdering::Contiguous,
    );
    let mut packed = pack_raw_series(input).unwrap();
    let error = packed.next().await.unwrap().unwrap_err();
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
        StatusCode::InvalidArguments
    );
    assert!(
        packed.next().await.is_none(),
        "the buffered rows are discarded, never emitted as a successful batch"
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        2,
        "the row was buffered, then EOF rejected the missing final key"
    );
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

/// A key frame states its whole label layout, so unpacking keeps exactly that description and no
/// binding: the description is what a consumer decodes from, and the physical region metadata the
/// producer read under never reaches the receiver.
#[tokio::test]
async fn the_declared_description_is_retained_and_no_binding_is_invented() {
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
    let description = label_decode_spec(&binding).expect("a primary-key binding describes its key");
    let (input, _, _) = raw_custom(
        vec![
            RawSeriesChunk::try_new(
                &schema(),
                Buffer::from(b"encoded-complete-key".to_vec()),
                None,
                RawTimestamps::try_new(ScalarBuffer::from(vec![1])).unwrap(),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(3.0)]))),
                true,
            )
            .unwrap(),
        ],
        SeriesOrdering::Contiguous,
        binding.clone(),
    );
    let mut packed = pack_raw_series(input).unwrap();
    let batch = packed.next().await.unwrap().unwrap();
    let source: DfSendableRecordBatchStream = Box::pin(DfMock {
        schema: batch.schema(),
        items: VecDeque::from([Ok(batch)]),
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
        pending_after: None,
        pending_reported: false,
        panic_on_poll: false,
        emitted: 0,
    });
    let mut out = unpack_raw_series(source, None).unwrap();
    let decoded = out.next().await.unwrap().unwrap();
    assert_eq!(decoded.primary_key.as_slice(), b"encoded-complete-key");
    assert!(decoded.metric.is_none());
    assert_eq!(
        out.label_decode_spec(),
        Some(&description),
        "the declaration is retained exactly as the frame states it"
    );
    assert_eq!(description.metric_name, "cpu");
    assert_eq!(description.primary_key.len(), 1);
    assert_eq!(description.primary_key[0].column_id, 0);
    assert_eq!(description.primary_key[0].name, "host");
    assert_eq!(description.logical_tags.len(), 1);
    assert_eq!(description.logical_tags[0].name, "host");
    assert!(
        matches!(out.label_source(), RawLabelSource::Explicit),
        "the physical region metadata the producer read under is not carried across the frame"
    );
}

#[tokio::test]
async fn explicit_mode_rejects_key_payload_and_primary_mode_rejects_explicit_metric() {
    let (input, _, _) = raw(
        vec![
            RawSeriesChunk::try_new(
                &schema(),
                Buffer::from(b"a".to_vec()),
                None,
                RawTimestamps::try_new(ScalarBuffer::from(vec![1])).unwrap(),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(1.0)]))),
                true,
            )
            .unwrap(),
        ],
        SeriesOrdering::Contiguous,
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
        SeriesOrdering::Contiguous,
        primary_binding(metadata),
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
    // The first row is flushed by `Pending` so the retained frame and the later violation are
    // both observed under the batching builder.
    let (input, polls, _) = raw_pending(
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
        SeriesOrdering::Contiguous,
        1,
    );
    let mut s = pack_raw_series(input).unwrap();
    let batch = s.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        polls.load(Ordering::SeqCst),
        2,
        "the stable row is flushed at Pending before the second chunk is read"
    );
    let chunks = datatypes::series::frame::unpack_raw_series_frame(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &batch,
    )
    .unwrap();
    assert_chunk(&chunks[0], b"a", &[1], &[Some(1.0)], false);
    assert_metric(&chunks[0], Some("x"));
    let err = s.next().await.unwrap().unwrap_err();
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&err, None),
        StatusCode::InvalidArguments
    );
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
        label_decode_spec: None,
        ordering: SeriesOrdering::Contiguous,
        polls,
        drops: Arc::new(AtomicUsize::new(0)),
        pending_after: None,
        pending_reported: false,
        panic_on_poll: false,
        emitted: 0,
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
    let (input, _, _) = raw(vec![], SeriesOrdering::Contiguous);
    let mut packed = pack_raw_series(input).unwrap();
    let frame_schema = packed.schema();
    assert_eq!(
        frame_schema.field(0).name(),
        "read_key",
        "the v1 wire column name is unchanged"
    );
    assert!(packed.next().await.is_none());
    // A declared-but-empty stream is still validated against the expected schema before any
    // poll, and then ends without producing rows.
    let (source, polls, _) = df_source(vec![], frame_schema.clone());
    let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(unpacked.schema().fields.len(), 1);
    assert_eq!(unpacked.schema().fields[0].name.as_ref(), "value");
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
            label_decode_spec: None,
            ordering: SeriesOrdering::Contiguous,
            polls: polls.clone(),
            drops: drops.clone(),
            pending_after: None,
            pending_reported: false,
            panic_on_poll: false,
            emitted: 0,
        });
        let mut packed = pack_raw_series(source).unwrap();
        let error = packed.next().await.unwrap().unwrap_err();
        assert_eq!(
            crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
            status
        );
        assert!(packed.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
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
            pending_after: None,
            pending_reported: false,
            panic_on_poll: false,
            emitted: 0,
        });
        let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
        let error = unpacked.next().await.unwrap().unwrap_err();
        assert_eq!(common_error::ext::ErrorExt::status_code(&error), status);
        assert!(unpacked.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn dropping_adapter_releases_upstream_and_pending_item_is_resumed() {
    let (input, polls, drops) = raw_pending(
        vec![chunk(b"a", &[1], true), chunk(b"b", &[2], true)],
        SeriesOrdering::Contiguous,
        1,
    );
    let mut packed = pack_raw_series(input).unwrap();
    let first = packed.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(
        polls.load(Ordering::SeqCst),
        2,
        "the buffered row is flushed at Pending before the next item"
    );
    let chunks = datatypes::series::frame::unpack_raw_series_frame(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &first,
    )
    .unwrap();
    assert_chunk(&chunks[0], b"a", &[1], &[Some(1.0)], true);
    let second = packed.next().await.unwrap().unwrap();
    assert_eq!(second.num_rows(), 1);
    assert_eq!(
        polls.load(Ordering::SeqCst),
        4,
        "Pending is resumed and the remaining row is flushed at EOF"
    );
    let chunks = datatypes::series::frame::unpack_raw_series_frame(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &second,
    )
    .unwrap();
    assert_chunk(&chunks[0], b"b", &[2], &[Some(2.0)], true);
    drop(packed);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unpack_rejects_mismatched_batch_schema_before_polling() {
    let wrong = datatypes::arrow::datatypes::Schema::empty().into();
    let (source, polls, drops) = df_source(vec![], wrong);
    assert!(unpack_raw_series(source, Some(schema())).is_err());
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
        label_decode_spec: None,
        ordering: SeriesOrdering::Contiguous,
        polls: polls.clone(),
        drops: Arc::new(AtomicUsize::new(0)),
        pending_after: None,
        pending_reported: false,
        panic_on_poll: false,
        emitted: 0,
    });
    assert!(pack_raw_series(source).is_err());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unpack_batch_sequence_error_is_atomic_for_all_rows() {
    let mut batches = Vec::new();
    // `KeyThenTime` is what makes a recurring key an error, so the frame has to declare it.
    let frame_schema = super::raw_series_frame_schema_with_ordering(
        &schema(),
        &RawLabelSource::Explicit,
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    for (key, time) in [
        (b"a".as_slice(), 1),
        (b"b".as_slice(), 2),
        (b"a".as_slice(), 3),
    ] {
        let (input, _, _) = raw(vec![chunk(key, &[time], true)], SeriesOrdering::KeyThenTime);
        batches.extend(pack_all(input).await);
    }
    let multi = concat_batches(&frame_schema, &batches).unwrap();
    let (source, _, _) = df_source(vec![multi], frame_schema);
    let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
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
    let (source, _, _) = df_source(vec![multi], frame_schema);
    let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
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
    let (source, _, _) = df_source(vec![first.clone(), stable_second], declared.clone());
    let mut stable = unpack_raw_series(source, Some(schema())).unwrap();
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
        let (source, _, _) = df_source(vec![first.clone(), bad_batch], declared.clone());
        let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
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

struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_unpacked(
    stream: &mut SendableRawSeriesStream,
    waker: &Waker,
) -> Poll<Option<Result<RawSeriesChunk, BoxedError>>> {
    stream.as_mut().poll_next(&mut Context::from_waker(waker))
}

fn poll_packed(
    stream: &mut DfSendableRecordBatchStream,
    waker: &Waker,
) -> Poll<Option<datafusion_common::Result<RecordBatch>>> {
    stream.as_mut().poll_next(&mut Context::from_waker(waker))
}

#[tokio::test]
async fn unpack_bounds_empty_batches_per_poll_and_eventually_yields_row() {
    const BUDGET: usize = 32;
    let (input, _, _) = raw(
        vec![chunk(b"after-empty", &[9], true)],
        SeriesOrdering::Contiguous,
    );
    let mut packed = pack_raw_series(input).unwrap();
    let row = packed.next().await.unwrap().unwrap();
    let frame_schema = row.schema();
    let mut batches = vec![RecordBatch::new_empty(frame_schema.clone()); 10_000];
    batches.push(row);
    let (source, polls, _) = df_source(batches, frame_schema);
    let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());

    let before = polls.load(Ordering::SeqCst);
    assert!(matches!(
        poll_unpacked(&mut unpacked, &waker),
        Poll::Pending
    ));
    assert_eq!(polls.load(Ordering::SeqCst) - before, BUDGET);
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);

    let mut yielded = false;
    let mut expected_wakes = 1;
    for _ in 0..=10_000 / BUDGET {
        let before = polls.load(Ordering::SeqCst);
        let wakes_before = wake.0.load(Ordering::SeqCst);
        match poll_unpacked(&mut unpacked, &waker) {
            Poll::Pending => {
                assert_eq!(polls.load(Ordering::SeqCst) - before, BUDGET);
                expected_wakes += 1;
                assert_eq!(wake.0.load(Ordering::SeqCst) - wakes_before, 1);
                assert_eq!(wake.0.load(Ordering::SeqCst), expected_wakes);
            }
            Poll::Ready(Some(Ok(chunk))) => {
                assert!(polls.load(Ordering::SeqCst) - before <= BUDGET + 1);
                assert_eq!(polls.load(Ordering::SeqCst), 10_001);
                assert_eq!(wake.0.load(Ordering::SeqCst), wakes_before);
                assert_eq!(wake.0.load(Ordering::SeqCst), 10_000 / BUDGET);
                assert_chunk(&chunk, b"after-empty", &[9], &[Some(9.0)], true);
                yielded = true;
                break;
            }
            other => panic!("unexpected poll result: {other:?}"),
        }
    }
    assert!(yielded, "bounded polling should eventually yield the row");
    assert!(matches!(
        poll_unpacked(&mut unpacked, &waker),
        Poll::Ready(None)
    ));
    assert_eq!(wake.0.load(Ordering::SeqCst), 10_000 / BUDGET);
}

#[test]
fn unpack_yields_at_empty_batch_budget_before_observing_eof() {
    const BUDGET: usize = 32;
    let frame_schema = datatypes::series::frame::raw_series_frame_schema(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
    )
    .unwrap();
    let batches = vec![RecordBatch::new_empty(frame_schema.clone()); BUDGET];
    let (source, polls, _) = df_source(batches, frame_schema);
    let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());

    assert!(matches!(
        poll_unpacked(&mut unpacked, &waker),
        Poll::Pending
    ));
    assert_eq!(polls.load(Ordering::SeqCst), BUDGET);
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        poll_unpacked(&mut unpacked, &waker),
        Poll::Ready(None)
    ));
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
}

#[test]
fn unpack_propagates_upstream_pending_without_extra_wake() {
    let frame_schema = datatypes::series::frame::raw_series_frame_schema(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
    )
    .unwrap();
    let (source, polls, _) = df_pending(
        vec![RecordBatch::new_empty(frame_schema.clone())],
        frame_schema,
        1,
    );
    let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());

    assert!(matches!(
        poll_unpacked(&mut unpacked, &waker),
        Poll::Pending
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(wake.0.load(Ordering::SeqCst), 1, "only upstream wakes");
}

#[test]
fn unpack_reports_bad_batch_at_budget_boundary_before_yielding() {
    const BUDGET: usize = 32;
    let frame_schema = datatypes::series::frame::raw_series_frame_schema(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
    )
    .unwrap();
    let mut batches = vec![RecordBatch::new_empty(frame_schema.clone()); BUDGET - 1];
    batches.push(RecordBatch::new_empty(
        datatypes::arrow::datatypes::Schema::empty().into(),
    ));
    let (source, polls, _) = df_source(batches, frame_schema);
    let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());

    assert!(matches!(
        poll_unpacked(&mut unpacked, &waker),
        Poll::Ready(Some(Err(_)))
    ));
    assert_eq!(polls.load(Ordering::SeqCst), BUDGET);
    assert_eq!(wake.0.load(Ordering::SeqCst), 0);
    assert!(matches!(
        poll_unpacked(&mut unpacked, &waker),
        Poll::Ready(None)
    ));
}

#[test]
fn unpack_surfaces_upstream_error_after_budget_yield_once() {
    const BUDGET: usize = 32;
    let frame_schema = datatypes::series::frame::raw_series_frame_schema(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
    )
    .unwrap();
    let error = datafusion_common::DataFusionError::External(Box::new(BoxedError::new(
        PlainError::new("after empty batches".into(), StatusCode::Unsupported),
    )));
    let mut items = (0..BUDGET)
        .map(|_| Ok(RecordBatch::new_empty(frame_schema.clone())))
        .collect::<Vec<_>>();
    items.push(Err(error));
    let polls = Arc::new(AtomicUsize::new(0));
    let source = Box::pin(DfMock {
        schema: frame_schema,
        items: items.into(),
        polls: polls.clone(),
        drops: Arc::new(AtomicUsize::new(0)),
        pending_after: None,
        pending_reported: false,
        panic_on_poll: false,
        emitted: 0,
    });
    let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());

    assert!(matches!(
        poll_unpacked(&mut unpacked, &waker),
        Poll::Pending
    ));
    assert_eq!(polls.load(Ordering::SeqCst), BUDGET);
    let Poll::Ready(Some(Err(error))) = poll_unpacked(&mut unpacked, &waker) else {
        panic!("upstream error should be returned after budget yield");
    };
    assert_eq!(
        common_error::ext::ErrorExt::status_code(&error),
        StatusCode::Unsupported
    );
    assert!(matches!(
        poll_unpacked(&mut unpacked, &waker),
        Poll::Ready(None)
    ));
}

#[tokio::test]
async fn unpack_drains_decoded_rows_before_polling_another_batch() {
    let (input, _, _) = raw(
        vec![chunk(b"a", &[1], true), chunk(b"b", &[2], true)],
        SeriesOrdering::KeyThenTime,
    );
    let batches = pack_all(input).await;
    let frame_schema = batches[0].schema();
    let multi = concat_batches(&frame_schema, &batches).unwrap();
    let (source, polls, _) = df_source(vec![multi], frame_schema);
    let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
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
        let (source, polls, drops) = df_source(vec![], frame_schema);
        assert!(unpack_raw_series(source, Some(Arc::new(expected))).is_err());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

fn named_fields_schema(names: [&str; 2]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: names
            .into_iter()
            .map(|name| FieldSpec {
                name: Arc::from(name),
            })
            .collect(),
        eval_grid: None,
    })
}

fn named_fields_chunk(schema: &SeriesSchema) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        schema,
        Buffer::from(b"series-key".to_vec()),
        Some(metric()),
        RawTimestamps::try_new(ScalarBuffer::from(vec![7_i64, 8])).unwrap(),
        SeriesFields::try_from_vec(vec![
            Samples::from_float(Float64Array::from(vec![Some(1.0), Some(2.0)])),
            Samples::from_float(Float64Array::from(vec![Some(10.0), Some(20.0)])),
        ])
        .unwrap(),
        true,
    )
    .unwrap()
}

#[tokio::test]
async fn named_frame_fields_roundtrip_and_reject_wrong_bindings_before_polling() {
    let expected = named_fields_schema(["cpu_usage", "cpu_idle"]);
    let input_chunk = named_fields_chunk(&expected);
    let input: SendableRawSeriesStream = Box::pin(RawMock {
        schema: expected.clone(),
        items: VecDeque::from([Ok(input_chunk)]),
        binding: RawLabelSource::Explicit,
        label_decode_spec: None,
        ordering: SeriesOrdering::KeyThenTime,
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
        pending_after: None,
        pending_reported: false,
        panic_on_poll: false,
        emitted: 0,
    });
    let batches = pack_all(input).await;
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &batches[0].schema()).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    let reader = StreamReader::try_new(Cursor::new(ipc), None).unwrap();
    let frame_schema = reader.schema();
    let ipc_batches = reader.map(Result::unwrap).collect::<Vec<_>>();
    let payload_fields = frame_schema.field_with_name("fields").unwrap().data_type();
    let DataType::Struct(payload_fields) = payload_fields else {
        panic!("expected struct payload field");
    };
    assert_eq!(
        payload_fields
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["cpu_usage", "cpu_idle"]
    );

    let (source, _, _) = df_source(ipc_batches.clone(), frame_schema.clone());
    let mut decoded = unpack_raw_series(source, Some(expected.clone())).unwrap();
    let actual = decoded.next().await.unwrap().unwrap();
    assert_chunk(
        &actual,
        b"series-key",
        &[7, 8],
        &[Some(1.0), Some(2.0)],
        true,
    );
    assert_eq!(actual.fields.len(), 2);
    let second = match actual.fields.as_slice()[1].dense().unwrap() {
        DenseSamplesRef::Float(values) => values,
        _ => panic!("expected float field"),
    };
    assert_eq!(second.null_count(), 0);
    assert_eq!(second.values(), &[10.0, 20.0]);
    assert_metric(&actual, None);
    assert!(decoded.next().await.is_none());

    for (wrong_names, empty) in [
        (["cpu_idle", "cpu_usage"], false),
        (["cpu_usage", "renamed_idle"], false),
        (["cpu_idle", "cpu_usage"], true),
        (["cpu_usage", "renamed_idle"], true),
    ] {
        let wrong_schema = named_fields_schema(wrong_names);
        let batches = if empty { vec![] } else { ipc_batches.clone() };
        let (source, polls, drops) = df_source(batches, frame_schema.clone());
        assert!(unpack_raw_series(source, Some(wrong_schema)).is_err());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

fn histogram_table() -> Arc<datatypes::series::histogram::HistogramBatch> {
    let mut builder = HistogramBatchBuilder::new();
    for (sum, start_timestamp) in [(10.5, Some(-123)), (-9.5, Some(456)), (7.25, None)] {
        builder
            .push(datatypes::series::histogram::HistogramView {
                schema: -7,
                zero_threshold: 0.125,
                sum,
                count: 17.25,
                zero_count: 2.5,
                reset_hint: CounterResetHint::Unrecognized(37),
                start_timestamp,
                custom_values: &[1.5, 4.5],
                positive: BucketSideView {
                    span_offsets: &[1],
                    span_lengths: &[2],
                    buckets: &[3.25, 7.5],
                },
                negative: BucketSideView {
                    span_offsets: &[-2],
                    span_lengths: &[1],
                    buckets: &[0.75],
                },
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

/// Ready rows are bounded to the default row limit and emitted as soon as that limit is reached,
/// without reading ahead to EOF; the remainder is flushed when EOF is observed.
#[tokio::test]
async fn pack_bounds_ready_rows_at_default_row_limit_before_eof() {
    const TOTAL: usize = 300;
    const ROW_LIMIT: usize = 256;
    let items = (0..TOTAL)
        .map(|i| {
            RawSeriesChunk::try_new(
                &schema(),
                Buffer::from(format!("s{i:03}").into_bytes()),
                Some(metric()),
                RawTimestamps::try_new(ScalarBuffer::from(vec![i as i64])).unwrap(),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(
                    i as f64 + 0.5,
                )]))),
                true,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let (input, polls, drops) = raw(items, SeriesOrdering::KeyThenTime);
    let mut packed = pack_raw_series(input).unwrap();

    let first = packed.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), ROW_LIMIT);
    assert_eq!(
        polls.load(Ordering::SeqCst),
        ROW_LIMIT,
        "the row bound emits a batch without observing EOF or polling a further chunk"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0, "the source is still live");

    let second = packed.next().await.unwrap().unwrap();
    assert_eq!(second.num_rows(), TOTAL - ROW_LIMIT);
    assert_eq!(polls.load(Ordering::SeqCst), TOTAL + 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1, "EOF releases the source");
    assert!(packed.next().await.is_none());
    assert_eq!(
        polls.load(Ordering::SeqCst),
        TOTAL + 1,
        "the terminal poll does not revisit the released source"
    );

    // Compare every series' full payload, not only the batch shape.
    let mut seen = 0usize;
    for batch in [&first, &second] {
        let decoded = datatypes::series::frame::unpack_raw_series_frame(
            &schema(),
            datatypes::series::frame::RawLabelMode::Explicit,
            batch,
        )
        .unwrap();
        for chunk in decoded {
            let key = format!("s{seen:03}");
            assert_chunk(
                &chunk,
                key.as_bytes(),
                &[seen as i64],
                &[Some(seen as f64 + 0.5)],
                true,
            );
            assert_metric(&chunk, None);
            seen += 1;
        }
    }
    assert_eq!(seen, TOTAL, "every ready series survives exactly once");
}

/// One chunk of `rows` single-sample float segments (so every row is a union-of-segment row),
/// plus a clone of every per-row value array, used to observe that a retained batch stays
/// readable once these source-side owners are dropped.
fn per_row_segment_chunk(rows: usize, key: &[u8]) -> (RawSeriesChunk, Vec<Float64Array>) {
    let owners = (0..rows)
        .map(|i| Float64Array::from(vec![Some(i as f64)]))
        .collect::<Vec<_>>();
    let samples = owners
        .clone()
        .into_iter()
        .map(Samples::from_float)
        .collect::<Vec<_>>();
    let chunk = RawSeriesChunk::try_new(
        &schema(),
        Buffer::from(key.to_vec()),
        Some(metric()),
        RawTimestamps::try_new(ScalarBuffer::from((0..rows as i64).collect::<Vec<_>>())).unwrap(),
        SeriesFields::one(Samples::try_from_chunks(samples).unwrap()),
        true,
    )
    .unwrap();
    (chunk, owners)
}

/// One chunk with more rows than the limit yields exactly one bounded batch per poll: the first
/// batch needs no second source poll, and the remainder needs only EOF. The stream never builds a
/// chunk-wide list of batches.
#[tokio::test]
async fn pack_splits_one_chunk_into_bounded_batches_without_repolling_the_source() {
    const ROWS: usize = 300;
    let (chunk, _owners) = per_row_segment_chunk(ROWS, b"single");
    let (input, polls, drops) = raw(vec![chunk], SeriesOrdering::Contiguous);
    let mut packed = pack_raw_series(input).unwrap();

    let first = packed.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 256);
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "a single source poll fills the bounded first batch"
    );
    let second = packed.next().await.unwrap().unwrap();
    assert_eq!(second.num_rows(), ROWS - 256);
    assert_eq!(
        polls.load(Ordering::SeqCst),
        2,
        "the remainder needs only the EOF poll, not another chunk"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(packed.next().await.is_none());

    let mut seen = 0usize;
    for batch in [&first, &second] {
        let decoded = datatypes::series::frame::unpack_raw_series_frame(
            &schema(),
            datatypes::series::frame::RawLabelMode::Explicit,
            batch,
        )
        .unwrap();
        for chunk in decoded {
            assert_chunk(
                &chunk,
                b"single",
                &[seen as i64],
                &[Some(seen as f64)],
                seen + 1 == ROWS,
            );
            seen += 1;
        }
    }
    assert_eq!(seen, ROWS);
}

/// Dropping the pack stream while its chunk cursor is still live: the upstream source and the
/// remaining rows go away without another poll, and the last emitted batch stays self-contained,
/// readable after every source-side owner is dropped.
#[tokio::test]
async fn dropping_pack_mid_chunk_keeps_last_batch_and_releases_source() {
    const ROWS: usize = 300;
    let (chunk, owners) = per_row_segment_chunk(ROWS, b"single");
    let (input, polls, drops) = raw(vec![chunk], SeriesOrdering::Contiguous);
    let mut packed = pack_raw_series(input).unwrap();

    let batch = packed.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 256);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "the live cursor keeps its chunk and the source alive"
    );

    drop(packed);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the drop releases upstream"
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "the drop neither polls upstream nor drains the rest of the chunk"
    );

    // The retained batch owns its payload: it decodes fully after the source-side owners go.
    drop(owners);
    let decoded = datatypes::series::frame::unpack_raw_series_frame(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &batch,
    )
    .unwrap();
    assert_eq!(decoded.len(), 256);
    for (index, chunk) in decoded.iter().enumerate() {
        // `is_last` stays false: the final row of the chunk is not in this batch.
        assert_chunk(
            chunk,
            b"single",
            &[index as i64],
            &[Some(index as f64)],
            false,
        );
    }
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

/// Two ready chunks of one key, with more rows buffered than `Pending` reports: the partial
/// batch is flushed complete at that `Pending` without EOF, keeping `is_last` false while the
/// series is not final in the source, and the remainder follows at EOF.
#[tokio::test]
async fn pack_flushes_ready_rows_at_pending_then_remainder_at_eof() {
    let (input, polls, drops) = raw_pending(
        vec![
            chunk_with(b"k", &[1], vec![Some(1.0)], Some(metric()), false),
            chunk_with(b"k", &[2], vec![Some(2.0)], Some(metric()), false),
            chunk_with(b"k", &[3], vec![Some(3.0)], Some(metric()), true),
        ],
        SeriesOrdering::Contiguous,
        2,
    );
    let mut packed = pack_raw_series(input).unwrap();
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());

    let first = match poll_packed(&mut packed, &waker) {
        Poll::Ready(Some(Ok(batch))) => batch,
        other => panic!("expected a flush at Pending, got {other:?}"),
    };
    assert_eq!(first.num_rows(), 2, "both ready rows are complete results");
    assert_eq!(
        polls.load(Ordering::SeqCst),
        3,
        "two ready rows and one Pending poll, without reaching EOF"
    );
    assert_eq!(wake.0.load(Ordering::SeqCst), 1, "upstream wakes the task");
    let decoded = datatypes::series::frame::unpack_raw_series_frame(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &first,
    )
    .unwrap();
    assert_eq!(decoded.len(), 2);
    assert_chunk(&decoded[0], b"k", &[1], &[Some(1.0)], false);
    assert_chunk(&decoded[1], b"k", &[2], &[Some(2.0)], false);

    let second = match poll_packed(&mut packed, &waker) {
        Poll::Ready(Some(Ok(batch))) => batch,
        other => panic!("expected the remainder at EOF, got {other:?}"),
    };
    assert_eq!(second.num_rows(), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 5);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let decoded = datatypes::series::frame::unpack_raw_series_frame(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
        &second,
    )
    .unwrap();
    assert_chunk(&decoded[0], b"k", &[3], &[Some(3.0)], true);
    assert!(matches!(
        poll_packed(&mut packed, &waker),
        Poll::Ready(None)
    ));
}

/// The byte bound is a work estimate, not a memory cap: a legal row larger than the estimate is
/// emitted whole as its own batch and never split or dropped.
#[tokio::test]
async fn pack_emits_oversized_legal_rows_whole() {
    const SAMPLES: usize = 200_000;
    let big = |key: &[u8], offset: i64| {
        RawSeriesChunk::try_new(
            &schema(),
            Buffer::from(key.to_vec()),
            Some(metric()),
            RawTimestamps::try_new(ScalarBuffer::from((0..SAMPLES as i64).collect::<Vec<_>>()))
                .unwrap(),
            SeriesFields::one(Samples::from_float(Float64Array::from(
                (0..SAMPLES)
                    .map(|i| Some(offset as f64 + i as f64))
                    .collect::<Vec<_>>(),
            ))),
            true,
        )
        .unwrap()
    };
    let (input, polls, _) = raw(
        vec![big(b"large-a", 0), big(b"large-b", SAMPLES as i64)],
        SeriesOrdering::Contiguous,
    );
    let mut packed = pack_raw_series(input).unwrap();

    let first = packed.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 1, "an oversized row is not split");
    assert!(
        polls.load(Ordering::SeqCst) <= 2,
        "the oversized row is emitted before EOF, at most after one look-ahead poll"
    );
    let second = packed.next().await.unwrap().unwrap();
    assert_eq!(second.num_rows(), 1);
    assert!(packed.next().await.is_none());

    for (batch, key, offset) in [
        (&first, b"large-a".as_slice(), 0_i64),
        (&second, b"large-b", 200_000),
    ] {
        let chunks = datatypes::series::frame::unpack_raw_series_frame(
            &schema(),
            datatypes::series::frame::RawLabelMode::Explicit,
            batch,
        )
        .unwrap();
        assert_eq!(chunks.len(), 1);
        let chunk = &chunks[0];
        assert_eq!(chunk.primary_key.as_slice(), key);
        assert!(chunk.is_last);
        assert_eq!(chunk.time.len(), SAMPLES);
        assert_eq!(chunk.time.timestamp(0), Some(0));
        assert_eq!(chunk.time.timestamp(SAMPLES - 1), Some(SAMPLES as i64 - 1));
        let samples = chunk.fields.get(0).unwrap();
        assert_eq!(samples.len(), SAMPLES);
        for index in [0, SAMPLES / 2, SAMPLES - 1] {
            match samples.sample(index) {
                SampleRef::Float(value) => {
                    assert_eq!(value.to_bits(), (offset as f64 + index as f64).to_bits());
                }
                SampleRef::Missing | SampleRef::Histogram(_) => panic!("expected float samples"),
            }
        }
    }
}

/// Two chunks with different per-field segment boundaries coalesce into one frame, survive a real
/// Arrow IPC round trip, and decode back to every value, sample bit, row-local histogram index,
/// and finality flag.
#[tokio::test]
async fn multirow_ipc_roundtrip_preserves_cross_chunk_variable_segments() {
    let schema = named_fields_schema(["value", "latency"]);
    let table = histogram_table();
    let first = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(b"series-key".to_vec()),
        Some(metric()),
        RawTimestamps::try_new(ScalarBuffer::from(vec![10_i64, 20, 30])).unwrap(),
        SeriesFields::try_from_vec(vec![
            Samples::try_from_chunks(vec![
                Samples::from_float(Float64Array::from(vec![Some(1.0), None])),
                Samples::from_float(Float64Array::from(vec![Some(f64::from_bits(
                    0x7ff8_0000_0000_0042,
                ))])),
            ])
            .unwrap(),
            Samples::try_from_histogram(
                UInt32Array::from(vec![Some(1), None, Some(0)]),
                table.clone(),
            )
            .unwrap(),
        ])
        .unwrap(),
        false,
    )
    .unwrap();
    let second = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(b"series-key".to_vec()),
        Some(metric()),
        RawTimestamps::try_new(ScalarBuffer::from(vec![40_i64, 50])).unwrap(),
        SeriesFields::try_from_vec(vec![
            Samples::try_from_chunks(vec![
                Samples::from_float(Float64Array::from(vec![Some(-0.0)])),
                Samples::from_float(Float64Array::from(vec![Some(f64::from_bits(
                    crate::prometheus::PROMETHEUS_STALE_NAN_BITS,
                ))])),
            ])
            .unwrap(),
            Samples::try_from_histogram(UInt32Array::from(vec![Some(0), Some(2)]), table).unwrap(),
        ])
        .unwrap(),
        true,
    )
    .unwrap();

    let (input, polls, _) = raw_items(
        vec![Ok(first), Ok(second)],
        schema.clone(),
        RawLabelSource::Explicit,
        SeriesOrdering::KeyThenTime,
        None,
    );
    let batches = pack_all(input).await;
    assert_eq!(polls.load(Ordering::SeqCst), 3);
    assert_eq!(batches.len(), 1, "cross-chunk rows share one bounded batch");
    assert_eq!(batches[0].num_rows(), 4);

    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &batches[0].schema()).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    let reader = StreamReader::try_new(Cursor::new(ipc), None).unwrap();
    assert_eq!(
        reader.schema(),
        batches[0].schema(),
        "IPC preserves the v1 layout, metadata, and field order"
    );
    let frame_schema = reader.schema();
    let ipc_batches = reader.map(Result::unwrap).collect::<Vec<_>>();
    let (source, _, _) = df_source(ipc_batches, frame_schema);
    let mut decoded = unpack_raw_series(source, Some(schema.clone())).unwrap();

    struct Expected {
        time: i64,
        float_bits: Option<u64>,
        hist_index: Option<u32>,
    }
    // Table rows: 0 => sum 10.5, 1 => sum -9.5, 2 => sum 7.25. Row-local indices stay
    // unchanged, so equal indices in different rows keep the same sum.
    let nan_bits = f64::from_bits(0x7ff8_0000_0000_0042).to_bits();
    let rows: [(Vec<Expected>, bool); 4] = [
        (
            vec![
                Expected {
                    time: 10,
                    float_bits: Some(1.0_f64.to_bits()),
                    hist_index: Some(1),
                },
                Expected {
                    time: 20,
                    float_bits: None,
                    hist_index: None,
                },
            ],
            false,
        ),
        (
            vec![Expected {
                time: 30,
                float_bits: Some(nan_bits),
                hist_index: Some(0),
            }],
            false,
        ),
        (
            vec![Expected {
                time: 40,
                float_bits: Some((-0.0_f64).to_bits()),
                hist_index: Some(0),
            }],
            false,
        ),
        (
            vec![Expected {
                time: 50,
                float_bits: Some(crate::prometheus::PROMETHEUS_STALE_NAN_BITS),
                hist_index: Some(2),
            }],
            true,
        ),
    ];
    let sums = [10.5_f64, -9.5, 7.25];
    for (expected, last) in rows {
        let chunk = decoded.next().await.unwrap().unwrap();
        assert_eq!(chunk.primary_key.as_slice(), b"series-key");
        assert_eq!(chunk.is_last, last);
        assert_metric(&chunk, None);
        assert_eq!(chunk.time.len(), expected.len());
        let DenseSamplesRef::Histogram(hist) = chunk.fields.get(1).unwrap().dense().unwrap() else {
            panic!("expected a dense histogram field")
        };
        for (index, expected) in expected.iter().enumerate() {
            assert_eq!(chunk.time.timestamp(index), Some(expected.time));
            match chunk.fields.get(0).unwrap().sample(index) {
                SampleRef::Float(value) => {
                    assert_eq!(Some(value.to_bits()), expected.float_bits);
                }
                SampleRef::Missing => assert_eq!(expected.float_bits, None),
                SampleRef::Histogram(_) => panic!("expected a float field"),
            }
            match expected.hist_index {
                Some(index_value) => {
                    assert!(hist.indices().is_valid(index));
                    assert_eq!(hist.indices().value(index), index_value);
                    let view = hist.view_at(index).expect("histogram sample present");
                    assert_eq!(view.sum.to_bits(), sums[index_value as usize].to_bits());
                    assert_eq!(view.schema, -7);
                    assert_eq!(view.reset_hint, CounterResetHint::Unrecognized(37));
                    assert_eq!(view.custom_values, [1.5, 4.5]);
                }
                None => {
                    assert!(hist.indices().is_null(index), "null index survives IPC");
                    assert!(hist.view_at(index).is_none());
                }
            }
        }
    }
    assert!(decoded.next().await.is_none());
}

/// A `Pending` flush after ready rows and a later upstream error: the buffered row is never
/// reported, the original error surfaces immediately and once, and the source is released.
#[tokio::test]
async fn pack_discards_buffered_rows_and_reports_upstream_error_once() {
    let status = StatusCode::Unsupported;
    let (input, polls, drops) = raw_items(
        vec![
            Ok(chunk(b"a", &[1], true)),
            Ok(chunk(b"b", &[2], true)),
            Err(BoxedError::new(PlainError::new(
                "mid-stream".into(),
                status,
            ))),
        ],
        schema(),
        RawLabelSource::Explicit,
        SeriesOrdering::Contiguous,
        Some(1),
    );
    let mut packed = pack_raw_series(input).unwrap();
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());

    let first = match poll_packed(&mut packed, &waker) {
        Poll::Ready(Some(Ok(batch))) => batch,
        other => panic!("expected the buffered row at Pending, got {other:?}"),
    };
    assert_eq!(first.num_rows(), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 2);

    let error = match poll_packed(&mut packed, &waker) {
        Poll::Ready(Some(Err(error))) => error,
        other => panic!("expected the upstream error, got {other:?}"),
    };
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
        status
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        4,
        "the second row is buffered, then the error is polled"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the error releases the source"
    );
    assert!(
        matches!(poll_packed(&mut packed, &waker), Poll::Ready(None)),
        "no buffered row may follow the error"
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        4,
        "the error is reported exactly once"
    );
}

/// A malformed public chunk that bypasses `RawSeriesChunk::try_new` reports the existing
/// invalid-arguments error instead of panicking, and still releases the source.
#[tokio::test]
async fn public_chunk_with_empty_times_is_rejected_without_panic() {
    let empty_times = || RawTimestamps::try_new(ScalarBuffer::from(Vec::<i64>::new())).unwrap();
    let mut aligned = chunk(b"a", &[1], true);
    aligned.time = empty_times();
    aligned.fields = SeriesFields::one(Samples::from_float(Float64Array::new_null(0)));
    let mut mismatched = chunk(b"a", &[1], true);
    mismatched.time = empty_times();

    for (label, bad) in [("empty", aligned), ("mismatched", mismatched)] {
        let (input, polls, drops) = raw_items(
            vec![Ok(bad)],
            schema(),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
            None,
        );
        let mut packed = pack_raw_series(input).unwrap();
        let error = match packed.next().await {
            Some(Err(error)) => error,
            other => panic!("{label}: expected a rejection, got {other:?}"),
        };
        assert_eq!(
            crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
            StatusCode::InvalidArguments,
            "{label}"
        );
        assert!(packed.next().await.is_none(), "{label}");
        assert_eq!(polls.load(Ordering::SeqCst), 1, "{label}");
        assert_eq!(drops.load(Ordering::SeqCst), 1, "{label}");
    }
}

/// `Interleaved` is rejected by the constructor before any source poll, for a pack source and for
/// an empty unpack stream; the wire-schema helper rejects it before assembling a schema, since no
/// v1 metadata value represents it. No adjacent-only fallback is installed.
#[tokio::test]
async fn interleaved_ordering_is_rejected_before_any_poll() {
    assert!(
        super::raw_series_frame_schema_with_ordering(
            &schema(),
            &RawLabelSource::Explicit,
            SeriesOrdering::Interleaved,
        )
        .is_err(),
        "interleaved cannot be declared on the wire schema"
    );

    let (source, polls, drops) = raw_never_polled(SeriesOrdering::Interleaved);
    let error = match pack_raw_series(source) {
        Ok(_) => panic!("interleaved pack stream must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
        StatusCode::InvalidArguments
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "a rejected source is never polled"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    // An unpack has no ordering argument, so the only way an `Interleaved` stream could reach it is
    // a frame that declares one; that spelling has no wire representation and is rejected.
    let frame = datatypes::series::frame::raw_series_frame_schema(
        &schema(),
        datatypes::series::frame::RawLabelMode::Explicit,
    )
    .unwrap();
    let mut metadata = frame.metadata().clone();
    metadata.insert(
        "greptime.series.ordering".to_string(),
        "interleaved".to_string(),
    );
    let frame = Arc::new(Schema::new_with_metadata(frame.fields().clone(), metadata));
    let (source, polls, drops) = df_never_polled(frame);
    let error = match unpack_raw_series(source, Some(schema())) {
        Ok(_) => panic!("an interleaved declaration must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
        StatusCode::InvalidArguments
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// The unpack stream rejects a batch whose schema differs from the stream's declared schema,
/// including drift in metadata the codec itself does not interpret and an empty batch that
/// carries the drift; the reserved layout metadata is rejected as well.
#[tokio::test]
async fn unpack_rejects_unrelated_metadata_drift_and_empty_schema_changes() {
    let base = datatypes::series::frame::raw_series_frame_schema(
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

    let mut unrelated = base.metadata().clone();
    unrelated.insert("app.unrelated".into(), "x".into());
    let mut reserved = base.metadata().clone();
    reserved.insert("greptime.series.layout".into(), "v2".into());
    let mut renamed = base.metadata().clone();
    renamed.insert("app.renamed".into(), "y".into());

    let cases: [(&str, Arc<datatypes::arrow::datatypes::Schema>, bool); 3] = [
        (
            "unrelated metadata added",
            Arc::new((*base).clone().with_metadata(unrelated)),
            false,
        ),
        (
            "reserved layout metadata changed",
            Arc::new((*base).clone().with_metadata(reserved)),
            false,
        ),
        (
            "unrelated metadata on an empty batch",
            Arc::new((*base).clone().with_metadata(renamed)),
            true,
        ),
    ];
    for (label, changed, empty) in cases {
        let bad = if empty {
            RecordBatch::new_empty(changed)
        } else {
            RecordBatch::try_new(changed, first.columns().to_vec()).unwrap()
        };
        let (source, _, _) = df_source(vec![first.clone(), bad], base.clone());
        let mut unpacked = unpack_raw_series(source, Some(schema())).unwrap();
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
            StatusCode::InvalidArguments,
            "{label}"
        );
        assert!(unpacked.next().await.is_none(), "{label}");
    }
}

#[test]
fn raw_series_ordering_metadata_fixture() {
    #[derive(serde::Deserialize)]
    struct Case {
        name: String,
        metadata: HashMap<String, String>,
        ordering: Option<String>,
    }

    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../../../../../tests/compatibility/wire/raw_series_ordering/v1_metadata.json"
    ))
    .unwrap();
    for case in cases {
        let literal_ordering = match case.ordering.as_deref() {
            Some("contiguous") => Some(SeriesOrdering::Contiguous),
            Some("read_key_then_time") => Some(SeriesOrdering::KeyThenTime),
            Some(value) => panic!("{} has invalid fixture ordering {value}", case.name),
            None => None,
        };
        let frame_schema = raw_series_frame_schema(&schema(), RawLabelMode::Explicit).unwrap();
        let frame_schema = frame_schema.as_ref().clone().with_metadata(case.metadata);
        match literal_ordering {
            Some(expected) => assert_eq!(
                raw_series_frame_ordering(&frame_schema)
                    .unwrap_or_else(|error| panic!("{}: {error}", case.name)),
                expected,
                "{}",
                case.name
            ),
            None => assert!(
                raw_series_frame_ordering(&frame_schema).is_err(),
                "{}",
                case.name
            ),
        }
    }
}

#[tokio::test]
async fn pack_declares_ordering_on_every_batch_and_ipc_roundtrips_it() {
    for ordering in [SeriesOrdering::Contiguous, SeriesOrdering::KeyThenTime] {
        for chunks in [
            vec![
                chunk_with(b"a", &[1], vec![Some(1.0)], Some(metric()), false),
                chunk_with(b"a", &[2], vec![None], Some(metric()), true),
                chunk_with(b"b", &[3], vec![Some(3.0)], Some(metric()), true),
            ],
            vec![],
        ] {
            let is_empty = chunks.is_empty();
            // The bounded multi-row builder coalesces these three rows into one batch; the
            // replaced single-row packing emitted one batch per chunk.
            let expected_batch_count = if is_empty { 0 } else { 1 };
            let canonical_schema =
                raw_series_frame_schema(&schema(), RawLabelMode::Explicit).unwrap();
            let mut expected_metadata = canonical_schema.metadata().clone();
            expected_metadata.insert(
                "greptime.series.ordering".to_string(),
                match ordering {
                    SeriesOrdering::Contiguous => "contiguous",
                    SeriesOrdering::KeyThenTime => "read_key_then_time",
                    // The v1 wire carries exactly these two spellings, so the loop above
                    // admits only the two representable modes; `Interleaved` is rejected
                    // before any poll (see `interleaved_ordering_is_rejected_before_any_poll`).
                    SeriesOrdering::Interleaved => {
                        unreachable!("interleaved raw series ordering has no wire spelling")
                    }
                }
                .to_string(),
            );
            let (input, _, _) = raw(chunks, ordering);
            let mut packed = pack_raw_series(input).unwrap();
            let declared = packed.schema();
            assert_eq!(declared.fields(), canonical_schema.fields());
            assert_eq!(declared.metadata(), &expected_metadata);
            assert_eq!(raw_series_frame_ordering(&declared).unwrap(), ordering);
            let mut batches = Vec::new();
            while let Some(batch) = packed.next().await {
                let batch = batch.unwrap();
                assert_eq!(batch.schema(), declared);
                batches.push(batch);
            }

            let mut ipc = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut ipc, &declared).unwrap();
                for batch in &batches {
                    writer.write(batch).unwrap();
                }
                writer.finish().unwrap();
            }
            let reader = StreamReader::try_new(Cursor::new(ipc), None).unwrap();
            assert_eq!(reader.schema(), declared);
            assert_eq!(
                raw_series_frame_ordering(&reader.schema()).unwrap(),
                ordering
            );
            let ipc_batches = reader.map(Result::unwrap).collect::<Vec<_>>();
            assert_eq!(ipc_batches.len(), batches.len());
            assert_eq!(ipc_batches.len(), expected_batch_count);
            assert!(ipc_batches.iter().all(|batch| batch.schema() == declared));
            let (source, _, _) = df_source(ipc_batches, declared);
            let mut decoded = unpack_raw_series(source, Some(schema())).unwrap();
            if is_empty {
                assert!(decoded.next().await.is_none());
            } else {
                for (key, time, value, last) in [
                    (b"a".as_slice(), 1, Some(1.0), false),
                    (b"a".as_slice(), 2, None, true),
                    (b"b".as_slice(), 3, Some(3.0), true),
                ] {
                    let actual = decoded.next().await.unwrap().unwrap();
                    assert_chunk(&actual, key, &[time], &[value], last);
                    assert_metric(&actual, None);
                }
                assert!(decoded.next().await.is_none());
            }
        }
    }
}

/// A primary-key binding over a region whose only tag is `host`.
fn key_binding(metric: &str) -> RawLabelSource {
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
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from(metric),
        physical_metadata: metadata,
        logical_tags: Arc::from([BoundTag {
            column_id: 0,
            name: Arc::from("host"),
        }]),
    }
}

/// A primary-key binding over a region whose two tags are `host` then `zone`.
fn two_tag_binding(metric: &str) -> RawLabelSource {
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
                "zone",
                datatypes::data_type::ConcreteDataType::string_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                datatypes::data_type::ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        })
        .primary_key(vec![0, 1]);
    let metadata = Arc::new(builder.build().unwrap());
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from(metric),
        physical_metadata: metadata,
        logical_tags: Arc::from([
            BoundTag {
                column_id: 0,
                name: Arc::from("host"),
            },
            BoundTag {
                column_id: 1,
                name: Arc::from("zone"),
            },
        ]),
    }
}

/// A key frame whose metadata carries exactly the supplied description and nothing else.
fn key_frame_with(description: &datatypes::series::frame::LabelDecodeSpec) -> SchemaRef {
    let layout = raw_series_frame_schema(&schema(), RawLabelMode::Key).unwrap();
    let mut metadata = layout.metadata().clone();
    metadata.insert(
        "greptime.series.label_decode".to_string(),
        serde_json::to_string(description).unwrap(),
    );
    Arc::new(Schema::new_with_metadata(layout.fields().clone(), metadata))
}

/// Logical tag order is fixed by the writer, so the wire form has one spelling and the comparison
/// is plain equality. The ordered key columns are the encoding, so the same columns in another
/// order are a different layout and are rejected.
///
/// The tag order is canonical rather than meaningful: the decoder sorts tags by name when it binds
/// them (`src/mito-codec/src/labels.rs:235`), so sorting in the writer cannot change a decoded label
/// set. What it does change is that the written text no longer depends on the caller's order, which
/// is what makes one comparison rule enough for both a local check and the byte-exact remote one.
#[tokio::test]
async fn frame_check_compares_the_canonical_description_and_key_order_exactly() {
    let binding = two_tag_binding("cpu");
    let description = label_decode_spec(&binding).expect("a primary-key binding describes its key");
    assert_eq!(
        description.logical_tags.len(),
        2,
        "two tags to canonicalize"
    );

    // A description derived from the binding is already canonical, whichever order the binding
    // lists its tags in.
    let RawLabelSource::PrimaryKey {
        metric_name,
        physical_metadata,
        logical_tags,
    } = &binding
    else {
        unreachable!("the fixture is a primary-key binding")
    };
    let mut reversed_tags = logical_tags.to_vec();
    reversed_tags.reverse();
    let reordered_binding = RawLabelSource::PrimaryKey {
        metric_name: metric_name.clone(),
        physical_metadata: physical_metadata.clone(),
        logical_tags: reversed_tags.into(),
    };
    assert_eq!(
        label_decode_spec(&reordered_binding).expect("a primary-key binding describes its key"),
        description,
        "a binding's tag order does not reach the description"
    );
    let reordered =
        label_decode_spec(&reordered_binding).expect("a primary-key binding describes its key");
    assert_eq!(
        key_frame_with(&reordered).metadata(),
        key_frame_with(&description).metadata(),
        "and so two producers with the same tags write the same bytes"
    );

    // A frame that is not canonical is compared as written instead of being guessed equal.
    let mut non_canonical = description.clone();
    non_canonical.logical_tags.reverse();
    let frame = key_frame_with(&non_canonical);
    assert!(
        ensure_frame_matches_label_source(frame.as_ref(), &binding).is_err(),
        "the reader does not reorder what a writer should have ordered"
    );

    // The ordered key columns are the encoding and are never reordered.
    let mut reordered_key = description;
    reordered_key.primary_key.reverse();
    let frame = key_frame_with(&reordered_key);
    assert!(
        ensure_frame_matches_label_source(frame.as_ref(), &binding).is_err(),
        "primary-key column order is the encoding and is compared as written"
    );
}

/// The identity check a caller performs on its own compares the frame's declaration with the
/// binding it holds: a different metric, another key layout, or an explicit frame with no
/// declaration at all is a disagreement, and the frame alone still decides what is decoded.
#[tokio::test]
async fn frame_check_rejects_a_binding_the_frame_does_not_describe() {
    let description =
        label_decode_spec(&key_binding("cpu")).expect("a primary-key binding describes its key");
    let frame = key_frame_with(&description);
    assert!(
        ensure_frame_matches_label_source(frame.as_ref(), &key_binding("cpu")).is_ok(),
        "the same binding is the same layout"
    );
    assert!(
        ensure_frame_matches_label_source(frame.as_ref(), &key_binding("other_metric")).is_err(),
        "another metric is another label layout"
    );
    assert!(
        ensure_frame_matches_label_source(frame.as_ref(), &RawLabelSource::Explicit).is_err(),
        "an explicit identity expects a frame that carries complete labels"
    );

    let explicit = raw_series_frame_schema(&schema(), RawLabelMode::Explicit).unwrap();
    assert!(
        ensure_frame_matches_label_source(explicit.as_ref(), &key_binding("cpu")).is_err(),
        "a frame declaring no layout cannot satisfy a primary-key identity"
    );
    assert!(
        ensure_frame_matches_label_source(explicit.as_ref(), &RawLabelSource::Explicit).is_ok(),
        "an explicit frame is what an explicit identity expects"
    );
}

/// The description parsed at unpack time is retained on the returned stream, so a consumer that
/// holds only the frame can decode; an explicit frame retains none.
#[tokio::test]
async fn unpack_retains_the_declared_description_without_a_binding() {
    let binding = key_binding("cpu");
    let description = label_decode_spec(&binding).expect("a primary-key binding describes its key");

    let (source, _, _) = df_source(vec![], key_frame_with(&description));
    let unpacked = unpack_raw_series(source, None).unwrap();
    assert_eq!(unpacked.label_decode_spec(), Some(&description));
    assert!(
        matches!(unpacked.label_source(), RawLabelSource::Explicit),
        "no physical region key layout is invented for a frame that carries none"
    );

    let explicit = raw_series_frame_schema(&schema(), RawLabelMode::Explicit).unwrap();
    let (source, _, _) = df_source(vec![], explicit);
    let unpacked = unpack_raw_series(source, None).unwrap();
    assert_eq!(
        unpacked.label_decode_spec(),
        None,
        "explicit labels are already complete and need no decoder"
    );
}

/// A key frame whose `greptime.series.label_decode` entry is exactly the supplied text.
fn key_frame_with_encoded(encoded: &str) -> SchemaRef {
    let layout = raw_series_frame_schema(&schema(), RawLabelMode::Key).unwrap();
    let mut metadata = layout.metadata().clone();
    metadata.insert(
        "greptime.series.label_decode".to_string(),
        encoded.to_string(),
    );
    Arc::new(Schema::new_with_metadata(layout.fields().clone(), metadata))
}

/// The description is validated whether or not a binding is supplied, so trusting the frame never
/// means skipping validation: an unknown version, an unknown field, a malformed entry, a logical
/// tag outside the key, a key frame without a description, and an explicit frame carrying one are
/// all rejected before the stream is returned.
#[tokio::test]
async fn unpack_rejects_invalid_descriptions_without_a_binding() {
    let description =
        label_decode_spec(&key_binding("cpu")).expect("a primary-key binding describes its key");
    let encoded = |value: &serde_json::Value| serde_json::to_string(value).unwrap();
    let mut unknown_version = serde_json::to_value(&description).unwrap();
    unknown_version["version"] =
        serde_json::json!(datatypes::series::frame::LABEL_DECODE_VERSION + 1);
    let mut unknown_field = serde_json::to_value(&description).unwrap();
    unknown_field["future_field"] = serde_json::json!(1);
    let mut unlisted_tag = serde_json::to_value(&description).unwrap();
    unlisted_tag["logical_tags"] = serde_json::json!([{ "column_id": 7, "name": "absent" }]);
    let cases = [
        ("unknown version", encoded(&unknown_version)),
        ("unknown field", encoded(&unknown_field)),
        (
            "logical tag outside the primary key",
            encoded(&unlisted_tag),
        ),
        ("malformed entry", "{".to_string()),
    ];
    for (label, text) in cases {
        let (source, polls, _) = df_source(vec![], key_frame_with_encoded(&text));
        let error = match unpack_raw_series(source, None) {
            Ok(_) => panic!("{label}: an invalid description must be rejected"),
            Err(error) => error,
        };
        assert_eq!(
            crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
            StatusCode::InvalidArguments,
            "{label}"
        );
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "{label}: a rejected frame is never polled"
        );
    }

    let key_without_description = raw_series_frame_schema(&schema(), RawLabelMode::Key).unwrap();
    let (source, _, _) = df_source(vec![], key_without_description);
    assert!(
        unpack_raw_series(source, None).is_err(),
        "a key frame nobody can decode is rejected instead of being trusted"
    );

    let explicit = raw_series_frame_schema(&schema(), RawLabelMode::Explicit).unwrap();
    let mut metadata = explicit.metadata().clone();
    metadata.insert(
        "greptime.series.label_decode".to_string(),
        encoded(&serde_json::to_value(&description).unwrap()),
    );
    let explicit_with_description = Arc::new(Schema::new_with_metadata(
        explicit.fields().clone(),
        metadata,
    ));
    let (source, _, _) = df_source(vec![], explicit_with_description);
    assert!(
        unpack_raw_series(source, None).is_err(),
        "an explicit frame declares no description, so one is a contradiction"
    );
}

fn key_chunk() -> RawSeriesChunk {
    chunk_with(b"encoded-complete-key", &[1], vec![Some(3.0)], None, true)
}

/// Every packed key frame declares the label decode description derived from its binding, so a
/// consumer needs no scan-side label source. Explicit frames declare none.
#[tokio::test]
async fn pack_declares_the_label_decode_description_of_its_binding() {
    let binding = key_binding("cpu");
    let (input, _, _) = raw_custom(
        vec![key_chunk()],
        SeriesOrdering::Contiguous,
        binding.clone(),
    );
    let mut packed = pack_raw_series(input).unwrap();
    let declared = packed.schema();
    let canonical = raw_series_frame_schema(&schema(), RawLabelMode::Key).unwrap();
    assert_eq!(
        declared.metadata().len(),
        canonical.metadata().len() + 2,
        "a key frame adds the producer ordering and the label decode description"
    );
    assert_eq!(
        datatypes::series::frame::raw_series_frame_label_decode(&declared).unwrap(),
        label_decode_spec(&binding),
        "the declared description is derived from the binding, not from the first row"
    );
    let batch = packed.next().await.unwrap().unwrap();
    assert_eq!(batch.schema(), declared);

    let (input, _, _) = raw_custom(
        vec![key_chunk()],
        SeriesOrdering::Contiguous,
        RawLabelSource::Explicit,
    );
    let packed = pack_raw_series(input).unwrap();
    let declared = packed.schema();
    assert_eq!(
        datatypes::series::frame::raw_series_frame_label_decode(&declared).unwrap(),
        None,
        "explicit labels are already complete"
    );
    assert_eq!(declared.metadata().len(), canonical.metadata().len() + 1);
}

/// A key frame that omits its description is not decodable, and unpacking says so instead of falling
/// back to a caller-side layout; the description that is present survives the round trip.
#[tokio::test]
async fn unpack_round_trips_a_key_frame_and_rejects_one_without_a_description() {
    let binding = key_binding("cpu");
    let (input, _, _) = raw_custom(
        vec![key_chunk()],
        SeriesOrdering::Contiguous,
        binding.clone(),
    );
    let mut packed = pack_raw_series(input).unwrap();
    let declared = packed.schema();
    let batch = packed.next().await.unwrap().unwrap();

    // The frame and the binding agree, so the round trip decodes.
    let (source, _, _) = df_source(vec![batch.clone()], declared.clone());
    let mut decoded = unpack_raw_series(source, Some(schema())).unwrap();
    let chunk = decoded.next().await.unwrap().unwrap();
    assert_eq!(chunk.primary_key.as_slice(), b"encoded-complete-key");

    // A frame that omits the description is not decodable from the schema alone.
    let canonical = raw_series_frame_schema(&schema(), RawLabelMode::Key).unwrap();
    let mut metadata = canonical.metadata().clone();
    metadata.insert(
        "greptime.series.ordering".to_string(),
        "contiguous".to_string(),
    );
    let stripped = Arc::new(datatypes::arrow::datatypes::Schema::new_with_metadata(
        declared.fields().clone(),
        metadata,
    ));
    let stripped_batch = RecordBatch::try_new(stripped.clone(), batch.columns().to_vec()).unwrap();
    let (source, _, _) = df_source(vec![stripped_batch], stripped);
    let error = match unpack_raw_series(source, Some(schema())) {
        Ok(_) => panic!("a key frame without a description must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
        StatusCode::InvalidArguments
    );
}

/// The ordering a frame declares is the only ordering a consumer has: unpacking a `KeyThenTime`
/// frame reports `KeyThenTime` although nobody passed it in.
#[tokio::test]
async fn unpack_reports_the_ordering_the_frame_declares() {
    let (input, _, _) = raw_custom(
        vec![key_chunk()],
        SeriesOrdering::KeyThenTime,
        key_binding("cpu"),
    );
    let packed = pack_raw_series(input).unwrap();
    assert_eq!(
        raw_series_frame_ordering(packed.schema().as_ref()).unwrap(),
        SeriesOrdering::KeyThenTime,
        "the producer declaration reaches the wire"
    );

    let unpacked = unpack_raw_series(packed, None).unwrap();
    assert_eq!(
        unpacked.ordering(),
        SeriesOrdering::KeyThenTime,
        "unpacking derives the ordering from the frame, not from a caller"
    );

    // A frame that declares no ordering is a legacy contiguous frame.
    let frame = raw_series_frame_schema(&schema(), RawLabelMode::Explicit).unwrap();
    let (source, _, _) = df_source(vec![], frame);
    assert_eq!(
        unpack_raw_series(source, None).unwrap().ordering(),
        SeriesOrdering::Contiguous
    );
}

/// Reserved layout metadata this revision does not write is rejected instead of being read as the
/// current layout, so a frame cannot be decoded under a contract it did not declare.
#[tokio::test]
async fn unpack_rejects_unknown_layout_metadata() {
    let cases = [
        ("greptime.series.layout", "v2"),
        ("greptime.series.content", "series"),
        ("greptime.series.time", "timestamps_v2"),
        ("greptime.series.ordering", "hash_then_time"),
    ];
    for (key, value) in cases {
        let frame = raw_series_frame_schema(&schema(), RawLabelMode::Explicit).unwrap();
        let mut metadata = frame.metadata().clone();
        metadata.insert(key.to_string(), value.to_string());
        let frame = Arc::new(Schema::new_with_metadata(frame.fields().clone(), metadata));
        let (source, polls, _) = df_source(vec![], frame);
        assert!(
            unpack_raw_series(source, None).is_err(),
            "{key} = {value} is not a declaration this revision reads"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0, "{key}");
    }
}

/// `expected` is a check, not an input: a schema the frame does not declare is rejected, and the
/// same frame unpacks without it because the frame is the only source of decode information.
#[tokio::test]
async fn unpack_rejects_an_expected_schema_the_frame_does_not_declare() {
    let frame = raw_series_frame_schema(&schema(), RawLabelMode::Explicit).unwrap();

    let other_field = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("other_value"),
        }]
        .into(),
        eval_grid: None,
    });
    let (source, polls, _) = df_source(vec![], frame.clone());
    let error = match unpack_raw_series(source, Some(other_field)) {
        Ok(_) => panic!("a field layout the frame does not declare must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
        StatusCode::InvalidArguments
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0, "nothing is polled");

    let two_fields = Arc::new(SeriesSchema {
        fields: vec![
            FieldSpec {
                name: Arc::from("value"),
            },
            FieldSpec {
                name: Arc::from("second"),
            },
        ]
        .into(),
        eval_grid: None,
    });
    let (source, _, _) = df_source(vec![], frame.clone());
    assert!(
        unpack_raw_series(source, Some(two_fields)).is_err(),
        "the same names in a longer list are another layout"
    );

    let (source, _, _) = df_source(vec![], frame.clone());
    assert!(
        unpack_raw_series(source, Some(schema())).is_ok(),
        "the schema the frame declares is accepted"
    );

    let (source, _, _) = df_source(vec![], frame);
    let unpacked = unpack_raw_series(source, None).unwrap();
    assert_eq!(
        unpacked
            .schema()
            .fields
            .iter()
            .map(|field| field.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["value"],
        "the returned schema is the frame's, whether or not an expectation was given"
    );
}

/// Unpacking a frame and packing it again keeps both declarations, so a relayed stream is the same
/// stream: the description still decodes the same labels and the ordering still holds.
#[tokio::test]
async fn unpack_pack_unpack_preserves_the_description_and_the_ordering() {
    let binding = key_binding("cpu");
    let description = label_decode_spec(&binding).expect("a primary-key binding describes its key");
    let (input, _, _) = raw_custom(vec![key_chunk()], SeriesOrdering::KeyThenTime, binding);
    let packed = pack_raw_series(input).unwrap();
    assert_eq!(
        datatypes::series::frame::raw_series_frame_label_decode(packed.schema().as_ref()).unwrap(),
        Some(description.clone())
    );

    let unpacked = unpack_raw_series(packed, None).unwrap();
    assert_eq!(unpacked.label_decode_spec(), Some(&description));
    assert_eq!(unpacked.ordering(), SeriesOrdering::KeyThenTime);

    let repacked = pack_raw_series(unpacked).unwrap();
    assert_eq!(
        datatypes::series::frame::raw_series_frame_label_decode(repacked.schema().as_ref())
            .unwrap(),
        Some(description.clone()),
        "the repacked frame declares what the unpacked stream declared"
    );
    assert_eq!(
        raw_series_frame_ordering(repacked.schema().as_ref()).unwrap(),
        SeriesOrdering::KeyThenTime,
        "the repacked frame keeps the ordering it was unpacked under"
    );

    let mut again = unpack_raw_series(repacked, None).unwrap();
    assert_eq!(again.label_decode_spec(), Some(&description));
    assert_eq!(again.ordering(), SeriesOrdering::KeyThenTime);
    let chunk = again.next().await.unwrap().unwrap();
    assert_eq!(chunk.primary_key.as_slice(), b"encoded-complete-key");
    assert!(chunk.metric.is_none(), "key chunks stay undecoded");
    assert!(again.next().await.is_none());
}
