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

mod payload;

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use api::v1::SemanticType;
use common_error::ext::{BoxedError, ErrorExt, PlainError};
use common_error::status_code::StatusCode;
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::schema::ColumnSchema;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{MetricName, SeriesOrdering};
use futures::{Stream, StreamExt, TryStreamExt};
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodec, SortField};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
use store_api::region_engine::{
    BoundTag, RawLabelSource, RawSeriesStream, SendableRawSeriesStream,
};
use store_api::storage::RegionId;

use super::{Input, merge_ordered_raw_series};

fn schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("temperature"),
        }]
        .into(),
        eval_grid: None,
    })
}

fn label_source(region: u32) -> RawLabelSource {
    label_source_with(region, PrimaryKeyEncoding::Dense, "room_temperature")
}

fn label_source_with(region: u32, encoding: PrimaryKeyEncoding, metric: &str) -> RawLabelSource {
    label_source_with_tags(region, encoding, metric, vec![(8, "rack"), (9, "sensor")])
}

fn label_source_with_tags(
    region: u32,
    encoding: PrimaryKeyEncoding,
    metric: &str,
    tags: Vec<(u32, &str)>,
) -> RawLabelSource {
    label_source_with_schema_version(region, encoding, metric, tags, 0)
}

fn label_source_with_schema_version(
    region: u32,
    encoding: PrimaryKeyEncoding,
    metric: &str,
    tags: Vec<(u32, &str)>,
    schema_version: u64,
) -> RawLabelSource {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(region, 1));
    for _ in 0..schema_version {
        builder.bump_version();
    }
    let columns = [
        (
            0,
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            SemanticType::Timestamp,
        ),
        (
            20,
            "temperature",
            ConcreteDataType::float64_datatype(),
            SemanticType::Field,
        ),
    ];
    for (column_id, name, ty, semantic_type) in
        columns.into_iter().chain(tags.iter().map(|(id, name)| {
            (
                *id,
                *name,
                ConcreteDataType::string_datatype(),
                SemanticType::Tag,
            )
        }))
    {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(name, ty, semantic_type != SemanticType::Timestamp),
            semantic_type,
            column_id,
        });
    }
    builder.primary_key(tags.iter().map(|(id, _)| *id).collect());
    builder.primary_key_encoding(encoding);
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from(metric),
        physical_metadata: Arc::new(builder.build().unwrap()),
        logical_tags: tags
            .iter()
            .map(|(column_id, name)| BoundTag {
                column_id: *column_id,
                name: Arc::from(*name),
            })
            .collect::<Vec<_>>()
            .into(),
    }
}

// Keys use the real DensePrimaryKeyCodec and the same primary-key field layout as metadata.
fn read_key(rack: &str, sensor: &str) -> Buffer {
    let codec = DensePrimaryKeyCodec::with_fields(vec![
        (8, SortField::new(ConcreteDataType::string_datatype())),
        (9, SortField::new(ConcreteDataType::string_datatype())),
    ]);
    let mut bytes = Vec::new();
    codec
        .encode_values(
            &[
                (8, Value::String(rack.into())),
                (9, Value::String(sensor.into())),
            ],
            &mut bytes,
        )
        .unwrap();
    Buffer::from(bytes)
}

fn chunk(key: Buffer, times: &[i64], values: &[f64], is_last: bool) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        &schema(),
        key,
        None,
        RawTimestamps::try_new(ScalarBuffer::from(times.to_vec())).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(values.to_vec()))),
        is_last,
    )
    .unwrap()
}

struct TestStream {
    schema: Arc<SeriesSchema>,
    labels: RawLabelSource,
    ordering: SeriesOrdering,
    items: VecDeque<Poll<Option<Result<RawSeriesChunk, BoxedError>>>>,
    // Delivered once the gate opens, so a blocked body can resume with later chunks.
    deferred: VecDeque<Poll<Option<Result<RawSeriesChunk, BoxedError>>>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    gate: Option<Arc<Mutex<Option<Waker>>>>,
    release: Option<Arc<AtomicUsize>>,
}

impl Stream for TestStream {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate
            && self.items.is_empty()
            && self.release.as_ref().unwrap().load(Ordering::SeqCst) == 0
        {
            *gate.lock().unwrap() = Some(_cx.waker().clone());
            return Poll::Pending;
        }
        self.items
            .pop_front()
            .or_else(|| self.deferred.pop_front())
            .unwrap_or(Poll::Ready(None))
    }
}

impl Drop for TestStream {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl RawSeriesStream for TestStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.labels
    }
    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}

fn stream(
    labels: RawLabelSource,
    ordering: SeriesOrdering,
    items: impl IntoIterator<Item = Poll<Option<Result<RawSeriesChunk, BoxedError>>>>,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    stream_with(schema(), labels, ordering, items)
}

fn stream_with(
    schema: Arc<SeriesSchema>,
    labels: RawLabelSource,
    ordering: SeriesOrdering,
    items: impl IntoIterator<Item = Poll<Option<Result<RawSeriesChunk, BoxedError>>>>,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(TestStream {
            schema,
            labels,
            ordering,
            items: items.into_iter().collect(),
            deferred: VecDeque::new(),
            polls: polls.clone(),
            drops: drops.clone(),
            gate: None,
            release: None,
        }),
        polls,
        drops,
    )
}

fn gated_stream(
    labels: RawLabelSource,
    first: RawSeriesChunk,
    gate: Arc<Mutex<Option<Waker>>>,
    release: Arc<AtomicUsize>,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(TestStream {
            schema: schema(),
            labels,
            ordering: SeriesOrdering::KeyThenTime,
            items: [ready(first)].into(),
            deferred: VecDeque::new(),
            polls: polls.clone(),
            drops: drops.clone(),
            gate: Some(gate),
            release: Some(release),
        }),
        polls,
        drops,
    )
}

// Yields `first` immediately, then blocks while `release` is zero, then yields `resumed` and EOF.
fn gated_resuming_stream(
    labels: RawLabelSource,
    first: RawSeriesChunk,
    resumed: impl IntoIterator<Item = Poll<Option<Result<RawSeriesChunk, BoxedError>>>>,
    gate: Arc<Mutex<Option<Waker>>>,
    release: Arc<AtomicUsize>,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(TestStream {
            schema: schema(),
            labels,
            ordering: SeriesOrdering::KeyThenTime,
            items: [ready(first)].into(),
            deferred: resumed.into_iter().collect(),
            polls: polls.clone(),
            drops: drops.clone(),
            gate: Some(gate),
            release: Some(release),
        }),
        polls,
        drops,
    )
}

struct KeyOwner {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl Drop for KeyOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn owned_key(bytes: Vec<u8>, drops: Arc<AtomicUsize>) -> (Buffer, std::sync::Weak<KeyOwner>) {
    let mut owner = KeyOwner { bytes, drops };
    let pointer = std::ptr::NonNull::new(owner.bytes.as_mut_ptr()).unwrap();
    let len = owner.bytes.len();
    let owner = Arc::new(owner);
    let weak = Arc::downgrade(&owner);
    // SAFETY: Buffer keeps the Arc owner alive for the entire allocation lifetime.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, len, owner) };
    (buffer, weak)
}

fn gated_pending_stream(
    labels: RawLabelSource,
    gate: Arc<Mutex<Option<Waker>>>,
    release: Arc<AtomicUsize>,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(TestStream {
            schema: schema(),
            labels,
            ordering: SeriesOrdering::KeyThenTime,
            items: VecDeque::new(),
            deferred: VecDeque::new(),
            polls: polls.clone(),
            drops: drops.clone(),
            gate: Some(gate),
            release: Some(release),
        }),
        polls,
        drops,
    )
}

fn ready(chunk: RawSeriesChunk) -> Poll<Option<Result<RawSeriesChunk, BoxedError>>> {
    Poll::Ready(Some(Ok(chunk)))
}

fn float_sample(chunk: &RawSeriesChunk, index: usize) -> f64 {
    match chunk.fields.get(0).unwrap().sample(index) {
        SampleRef::Float(value) => value,
        _ => panic!("expected float sample"),
    }
}

#[tokio::test]
async fn merges_full_encoded_keys_and_interleaved_chunks_with_global_finality() {
    let rack_a_sensor_1 = read_key("rack-a", "sensor-1");
    let rack_a_sensor_2 = read_key("rack-a", "sensor-2");
    let rack_b_sensor_1 = read_key("rack-b", "sensor-1");
    let (left, _, _) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [
            ready(chunk(rack_a_sensor_1.clone(), &[10, 30], &[1.0, 3.0], true)),
            ready(chunk(rack_b_sensor_1.clone(), &[5, 25], &[5.0, 25.0], true)),
        ],
    );
    let (right, _, _) = stream(
        label_source(2),
        SeriesOrdering::KeyThenTime,
        [
            ready(chunk(rack_a_sensor_1.clone(), &[20, 40], &[2.0, 4.0], true)),
            ready(chunk(rack_a_sensor_2.clone(), &[7, 17], &[7.0, 17.0], true)),
        ],
    );

    let mut merged = merge_ordered_raw_series(vec![left, right]).unwrap();
    assert_eq!(merged.ordering(), SeriesOrdering::KeyThenTime);
    assert!(merged.schema().eval_grid.is_none());
    assert!(matches!(
        merged.label_source(),
        RawLabelSource::PrimaryKey { .. }
    ));
    let mut observed = Vec::new();
    while let Some(item) = merged.next().await {
        let item = item.unwrap();
        let values = (0..item.fields.get(0).unwrap().len())
            .map(|index| match item.fields.get(0).unwrap().sample(index) {
                SampleRef::Float(value) => value.to_bits(),
                _ => panic!("expected float sample"),
            })
            .collect::<Vec<_>>();
        observed.push((
            item.primary_key,
            item.time.as_buffer().to_vec(),
            values,
            item.is_last,
        ));
    }
    let mut series = Vec::<(Buffer, Vec<i64>, Vec<u64>, Vec<bool>)>::new();
    for (key, times, values, is_last) in observed {
        if series.last().is_none_or(|current| current.0 != key) {
            series.push((key.clone(), Vec::new(), Vec::new(), Vec::new()));
        }
        let current = series.last_mut().unwrap();
        current.1.extend(times);
        current.2.extend(values);
        current.3.push(is_last);
    }
    assert_eq!(series.len(), 3);
    assert_eq!(series[0].0, rack_a_sensor_1);
    assert_eq!(series[0].1, [10, 20, 30, 40]);
    assert_eq!(
        series[0].2,
        [
            1.0_f64.to_bits(),
            2.0_f64.to_bits(),
            3.0_f64.to_bits(),
            4.0_f64.to_bits()
        ]
    );
    assert!(series[0].3.last().unwrap());
    assert!(
        series[0].3[..series[0].3.len() - 1]
            .iter()
            .all(|last| !last)
    );
    assert_eq!(series[1].0, rack_a_sensor_2);
    assert_eq!(series[1].1, [7, 17]);
    assert!(series[1].3.last().unwrap());
    assert_eq!(series[2].0, rack_b_sensor_1);
    assert_eq!(series[2].1, [5, 25]);
    assert!(series[2].3.last().unwrap());
}

#[tokio::test]
async fn rejects_equal_timestamp_from_separate_heads() {
    let key = read_key("rack-a", "sensor-1");
    let (left, _, _) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key.clone(), &[10], &[1.0], true))],
    );
    let (right, _, _) = stream(
        label_source(2),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key, &[10], &[1.0], true))],
    );
    let mut merged = merge_ordered_raw_series(vec![left, right]).unwrap();
    let error = merged.next().await.unwrap().unwrap_err().to_string();
    assert!(
        error.contains("timestamp") || error.contains("time"),
        "unexpected error: {error}"
    );
    assert!(merged.next().await.is_none());
}

#[tokio::test]
async fn rejects_unsupported_ordering_and_empty_input_without_polling() {
    for ordering in [SeriesOrdering::Contiguous, SeriesOrdering::Interleaved] {
        let (input, polls, drops) = stream(label_source(1), ordering, []);
        let error = match merge_ordered_raw_series(vec![input]) {
            Ok(_) => panic!("{ordering:?} ordering must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("ordering") || error.contains("ordered"),
            "{ordering:?}: {error}"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0, "{ordering:?}");
        assert_eq!(drops.load(Ordering::SeqCst), 1, "{ordering:?}");
    }

    let error = match merge_ordered_raw_series(vec![]) {
        Ok(_) => panic!("empty inputs must be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("empty") || error.contains("input"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn preflight_rejects_binding_and_schema_mismatches_without_polling() {
    let (explicit, explicit_polls, explicit_drops) =
        stream(RawLabelSource::Explicit, SeriesOrdering::KeyThenTime, []);
    let error = match merge_ordered_raw_series(vec![explicit]) {
        Ok(_) => panic!("Explicit label binding must be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.to_lowercase().contains("primary") || error.to_lowercase().contains("label"),
        "{error}"
    );
    assert_eq!(explicit_polls.load(Ordering::SeqCst), 0);
    assert_eq!(explicit_drops.load(Ordering::SeqCst), 1);

    let (single, single_polls, single_drops) =
        stream(label_source(1), SeriesOrdering::KeyThenTime, []);
    let _merged = merge_ordered_raw_series(vec![single]).unwrap();
    assert_eq!(
        single_polls.load(Ordering::SeqCst),
        0,
        "single input construction must not poll eagerly"
    );
    drop(_merged);
    assert_eq!(single_drops.load(Ordering::SeqCst), 1);

    let cases = [
        (
            "different field",
            vec![FieldSpec {
                name: Arc::from("humidity"),
            }],
            None,
        ),
        ("zero fields", vec![], None),
        (
            "evaluation grid",
            vec![FieldSpec {
                name: Arc::from("temperature"),
            }],
            Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(0, 1, 2).unwrap()).unwrap(),
            )),
        ),
    ];
    let (invalid_mode, _, invalid_mode_drops) =
        stream(label_source(1), SeriesOrdering::Contiguous, []);
    let ordering_error = match merge_ordered_raw_series(vec![invalid_mode]) {
        Ok(_) => panic!("Contiguous ordering must be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(
        ordering_error.to_lowercase().contains("ordering"),
        "{ordering_error}"
    );
    assert_eq!(invalid_mode_drops.load(Ordering::SeqCst), 1);

    for (description, fields, eval_grid) in cases {
        let (first, first_polls, first_drops) =
            stream(label_source(1), SeriesOrdering::KeyThenTime, []);
        let incompatible = Arc::new(SeriesSchema {
            fields: fields.into(),
            eval_grid,
        });
        let (second, second_polls, second_drops) = stream_with(
            incompatible,
            label_source(2),
            SeriesOrdering::KeyThenTime,
            [],
        );
        let error = match merge_ordered_raw_series(vec![first, second]) {
            Ok(_) => panic!("{description} must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.to_lowercase().contains("schema") || error.to_lowercase().contains("field"),
            "{description}: {error}"
        );
        assert_eq!(first_polls.load(Ordering::SeqCst), 0);
        assert_eq!(second_polls.load(Ordering::SeqCst), 0);
        assert_eq!(first_drops.load(Ordering::SeqCst), 1);
        assert_eq!(second_drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn later_error_drops_held_candidate_and_heads_before_returning_error() {
    let key_a = read_key("rack-a", "sensor-1");
    let key_b = read_key("rack-b", "sensor-1");
    let key_a_drops = Arc::new(AtomicUsize::new(0));
    let key_b_drops = Arc::new(AtomicUsize::new(0));
    let (owned_a, weak_a) = owned_key(key_a.as_slice().to_vec(), key_a_drops.clone());
    let (owned_b, weak_b) = owned_key(key_b.as_slice().to_vec(), key_b_drops.clone());
    let (first, _, first_drops) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [
            ready(chunk(owned_a, &[10], &[1.0], false)),
            Poll::Pending,
            Poll::Ready(Some(Err(BoxedError::new(PlainError::new(
                "upstream boom".to_string(),
                StatusCode::Internal,
            ))))),
        ],
    );
    let (second, _, second_drops) = stream(
        label_source(2),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(owned_b, &[20], &[2.0], true))],
    );
    let mut merged = merge_ordered_raw_series(vec![first, second]).unwrap();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Pending
    ));
    assert!(weak_a.upgrade().is_some() && weak_b.upgrade().is_some());
    let error = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Err(error))) => error.to_string(),
        _ => panic!("upstream failure should be returned"),
    };
    assert!(error.contains("upstream boom"), "{error}");
    assert!(weak_a.upgrade().is_none() && weak_b.upgrade().is_none());
    assert_eq!(key_a_drops.load(Ordering::SeqCst), 1);
    assert_eq!(key_b_drops.load(Ordering::SeqCst), 1);
    assert_eq!(first_drops.load(Ordering::SeqCst), 1);
    assert_eq!(second_drops.load(Ordering::SeqCst), 1);
    assert!(merged.next().await.is_none());
}

// A chunk marked `is_last` proves its input cannot extend that read key, so a body that never
// becomes ready again must postpone neither the already-decided prefix nor the late error waiting
// behind another input's greater head. Global finality is still recomputed over all inputs.
#[test]
fn completed_key_pending_body_does_not_starve_ready_prefix_or_late_error() {
    for failing_input_first in [false, true] {
        let key = read_key("rack-a", "sensor-1");
        let marker = "late alpha body behind the completed key never unblocked";
        let key_drops = Arc::new(AtomicUsize::new(0));
        let (owned_lower_key, weak_lower_key) =
            owned_key(key.as_slice().to_vec(), key_drops.clone());
        let gate = Arc::new(Mutex::new(None::<Waker>));
        let release = Arc::new(AtomicUsize::new(0));
        // The lower chunk completes the read key; its body stays unreleased, so every later poll of
        // that input is a genuine, permanent `Pending`.
        let (completed, _, completed_drops) = gated_stream(
            label_source(1),
            chunk(owned_lower_key, &[10], &[1.0], true),
            gate,
            release,
        );
        let (failing, _, failing_drops) = stream(
            label_source(2),
            SeriesOrdering::KeyThenTime,
            [
                ready(chunk(key.clone(), &[30], &[3.0], true)),
                Poll::Ready(Some(Err(BoxedError::new(PlainError::new(
                    marker.to_string(),
                    StatusCode::Internal,
                ))))),
            ],
        );
        let inputs = if failing_input_first {
            vec![failing, completed]
        } else {
            vec![completed, failing]
        };
        let mut merged = merge_ordered_raw_series(inputs).unwrap();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // The decided lower chunk must not wait for the pending body; the greater same-key head
        // still holds the key open, so the prefix is not final.
        let prefix = match Pin::new(&mut merged).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(prefix))) => prefix,
            other => panic!(
                "completed lower chunk must be ready before the pending body settles (failing \
                 input first: {failing_input_first}): {other:?}"
            ),
        };
        assert_eq!(prefix.primary_key, key);
        assert_eq!(prefix.time.as_buffer().as_ref(), &[10]);
        assert!(
            !prefix.is_last,
            "the greater same-key head keeps the prefix non-final"
        );
        match prefix.fields.get(0).unwrap().sample(0) {
            SampleRef::Float(value) => assert_eq!(value, 1.0),
            _ => panic!("expected float sample"),
        }
        assert_eq!(
            key_drops.load(Ordering::SeqCst),
            0,
            "the emitted prefix retains its key allocation"
        );
        drop(prefix);
        assert!(
            weak_lower_key.upgrade().is_none(),
            "dropping the prefix releases its key allocation"
        );
        assert_eq!(key_drops.load(Ordering::SeqCst), 1);

        // The late error waits behind the held greater-time head, and that head's own input is
        // polled again as soon as the head is consumed, so the error must surface on this very
        // demand: neither the pending body nor the already-decided successor chunk may delay it.
        let error = match Pin::new(&mut merged).poll_next(&mut cx) {
            Poll::Ready(Some(Err(error))) => error,
            other => panic!(
                "the late error must be returned before the greater-time successor is emitted \
                 (failing input first: {failing_input_first}): {other:?}"
            ),
        };
        assert_eq!(error.status_code(), StatusCode::Internal);
        assert!(error.to_string().contains(marker), "{error}");
        assert_eq!(
            completed_drops.load(Ordering::SeqCst),
            1,
            "both inputs drop before the error is observed"
        );
        assert_eq!(failing_drops.load(Ordering::SeqCst), 1);
        assert!(weak_lower_key.upgrade().is_none());
        assert_eq!(key_drops.load(Ordering::SeqCst), 1);
        assert!(matches!(
            Pin::new(&mut merged).poll_next(&mut cx),
            Poll::Ready(None)
        ));
    }
}

// A `is_last` chunk completes its read key even while the input stays live, so the final chunk is
// emitted without waiting for the body; the live input must not be dropped or treated as EOF.
#[test]
fn live_input_completed_at_its_last_chunk_emits_final_chunk_without_dropping_input() {
    let key = read_key("rack-a", "sensor-1");
    let gate = Arc::new(Mutex::new(None::<Waker>));
    let release = Arc::new(AtomicUsize::new(0));
    let (input, _, drops) = gated_stream(
        label_source(1),
        chunk(key.clone(), &[10], &[1.0], true),
        gate,
        release.clone(),
    );
    let mut merged = merge_ordered_raw_series(vec![input]).unwrap();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    let output = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(output))) => output,
        other => panic!("a completed series must not wait for its live body: {other:?}"),
    };
    assert_eq!(output.primary_key, key);
    assert_eq!(output.time.as_buffer().as_ref(), &[10]);
    assert_eq!(float_sample(&output, 0), 1.0);
    assert!(
        output.is_last,
        "the last chunk of a completed series is final"
    );
    drop(output);

    // The input is live, so nothing may be dropped or reported as EOF yet.
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Pending
    ));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "a live input stays owned after its final chunk"
    );
    release.store(1, Ordering::SeqCst);
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

// A live input that completed one read key says nothing about other keys, so a greater head waits
// until that input resolves, and is still emitted afterwards.
#[test]
fn live_completed_input_withholds_other_key_head_until_it_resolves() {
    let completed_key = read_key("rack-a", "sensor-1");
    let later_key = read_key("rack-b", "sensor-1");
    let gate = Arc::new(Mutex::new(None::<Waker>));
    let release = Arc::new(AtomicUsize::new(0));
    let (completed, _, completed_drops) = gated_stream(
        label_source(1),
        chunk(completed_key.clone(), &[10], &[1.0], true),
        gate,
        release.clone(),
    );
    let (later, _, later_drops) = stream(
        label_source(2),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(later_key.clone(), &[50], &[5.0], true))],
    );
    let mut merged = merge_ordered_raw_series(vec![completed, later]).unwrap();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    let first = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(first))) => first,
        other => panic!("the completed key must be emitted despite the live body: {other:?}"),
    };
    assert_eq!(first.primary_key, completed_key);
    assert_eq!(float_sample(&first, 0), 1.0);
    assert!(
        first.is_last,
        "the greater head proves the completed key is globally final"
    );
    drop(first);

    // The live input may still reveal keys before `rack-b`, so that head is not provable yet.
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Pending
    ));
    assert_eq!(completed_drops.load(Ordering::SeqCst), 0);
    release.store(1, Ordering::SeqCst);
    let second = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(second))) => second,
        other => panic!("the held head must be emitted once the live input resolves: {other:?}"),
    };
    assert_eq!(second.primary_key, later_key);
    assert_eq!(second.time.as_buffer().as_ref(), &[50]);
    assert_eq!(float_sample(&second, 0), 5.0);
    assert!(second.is_last);
    drop(second);
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(completed_drops.load(Ordering::SeqCst), 1);
    assert_eq!(later_drops.load(Ordering::SeqCst), 1);
}

// An input that has not finished a read key may still add chunks to it, so even its already-decided
// slice waits; its own final chunk then unsticks the exact ordered sequence.
#[test]
fn unfinished_same_key_pending_input_blocks_its_slice_until_its_final_chunk() {
    let key = read_key("rack-a", "sensor-1");
    let gate = Arc::new(Mutex::new(None::<Waker>));
    let release = Arc::new(AtomicUsize::new(0));
    let (unfinished, _, unfinished_drops) = gated_resuming_stream(
        label_source(1),
        chunk(key.clone(), &[10], &[1.0], false),
        [ready(chunk(key.clone(), &[20], &[2.0], true))],
        gate,
        release.clone(),
    );
    let (later, _, later_drops) = stream(
        label_source(2),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key.clone(), &[30], &[3.0], true))],
    );
    let mut merged = merge_ordered_raw_series(vec![unfinished, later]).unwrap();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert!(
        matches!(Pin::new(&mut merged).poll_next(&mut cx), Poll::Pending),
        "an unfinished same-key input holds its whole key back"
    );
    assert_eq!(unfinished_drops.load(Ordering::SeqCst), 0);
    release.store(1, Ordering::SeqCst);

    let mut observed = Vec::new();
    loop {
        match Pin::new(&mut merged).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(next))) => observed.push((
                next.primary_key.clone(),
                next.time.as_buffer().to_vec(),
                float_sample(&next, 0).to_bits(),
                next.is_last,
            )),
            Poll::Ready(None) => break,
            other => panic!("expected the ordered slices or EOF, got {other:?}"),
        }
    }
    assert_eq!(
        observed,
        vec![
            (key.clone(), vec![10], 1.0_f64.to_bits(), false),
            (key.clone(), vec![20], 2.0_f64.to_bits(), false),
            (key, vec![30], 3.0_f64.to_bits(), true),
        ]
    );
    assert_eq!(unfinished_drops.load(Ordering::SeqCst), 1);
    assert_eq!(later_drops.load(Ordering::SeqCst), 1);
}

// After completing a series, a still-live input keeps being polled: once its blocked body resumes,
// its later keys are emitted in order with complete values and global finality, and no input is
// dropped or treated as EOF while the merge is merely blocked.
#[test]
fn completed_input_resumes_with_later_keys_after_being_blocked() {
    let completed_key = read_key("rack-a", "sensor-1");
    let later_key = read_key("rack-c", "sensor-1");
    let gate_a = Arc::new(Mutex::new(None::<Waker>));
    let gate_b = Arc::new(Mutex::new(None::<Waker>));
    let release_a = Arc::new(AtomicUsize::new(0));
    let release_b = Arc::new(AtomicUsize::new(0));
    let (resuming, _, resuming_drops) = gated_resuming_stream(
        label_source(1),
        chunk(completed_key.clone(), &[10], &[1.0], true),
        [ready(chunk(later_key.clone(), &[40], &[4.0], true))],
        gate_a,
        release_a.clone(),
    );
    let (completed, _, completed_drops) = gated_stream(
        label_source(2),
        chunk(completed_key.clone(), &[30], &[3.0], true),
        gate_b,
        release_b.clone(),
    );
    let mut merged = merge_ordered_raw_series(vec![resuming, completed]).unwrap();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    let first = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(first))) => first,
        other => panic!("the completed prefix must be emitted despite live bodies: {other:?}"),
    };
    assert_eq!(first.primary_key, completed_key);
    assert_eq!(first.time.as_buffer().as_ref(), &[10]);
    assert_eq!(float_sample(&first, 0), 1.0);
    assert!(!first.is_last);
    drop(first);

    let second = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(second))) => second,
        other => panic!("the remaining completed chunk must be emitted before waiting: {other:?}"),
    };
    assert_eq!(second.primary_key, completed_key);
    assert_eq!(second.time.as_buffer().as_ref(), &[30]);
    assert_eq!(float_sample(&second, 0), 3.0);
    assert!(second.is_last);
    drop(second);

    // Both bodies are blocked: the merge waits and keeps both inputs (and their data) owned.
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Pending
    ));
    assert_eq!(resuming_drops.load(Ordering::SeqCst), 0);
    assert_eq!(completed_drops.load(Ordering::SeqCst), 0);
    release_a.store(1, Ordering::SeqCst);
    release_b.store(1, Ordering::SeqCst);
    let third = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(third))) => third,
        other => panic!("the resumed key must be emitted after the bodies unblock: {other:?}"),
    };
    assert_eq!(third.primary_key, later_key);
    assert_eq!(third.time.as_buffer().as_ref(), &[40]);
    assert_eq!(float_sample(&third, 0), 4.0);
    assert!(third.is_last);
    drop(third);
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(resuming_drops.load(Ordering::SeqCst), 1);
    assert_eq!(completed_drops.load(Ordering::SeqCst), 1);
}

// A chunk split against a later same-key head leaves a remainder that is locally final, but its key
// stays open until that remainder itself is emitted.
#[test]
fn split_remainder_head_keeps_its_key_open_despite_local_final_chunk() {
    let key = read_key("rack-a", "sensor-1");
    let (left, _, left_drops) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key.clone(), &[10, 40], &[1.0, 4.0], true))],
    );
    let (right, _, right_drops) = stream(
        label_source(2),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key.clone(), &[30], &[3.0], true))],
    );
    let mut merged = merge_ordered_raw_series(vec![left, right]).unwrap();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    let first = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(first))) => first,
        other => panic!("expected the split prefix, got {other:?}"),
    };
    assert_eq!(first.time.as_buffer().as_ref(), &[10]);
    assert_eq!(float_sample(&first, 0), 1.0);
    assert!(
        !first.is_last,
        "the locally final remainder head still holds the key open"
    );
    drop(first);

    let mut observed = Vec::new();
    loop {
        match Pin::new(&mut merged).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(next))) => observed.push((
                next.time.as_buffer().to_vec(),
                float_sample(&next, 0).to_bits(),
                next.is_last,
            )),
            Poll::Ready(None) => break,
            other => panic!("expected the remaining slices or EOF, got {other:?}"),
        }
    }
    assert_eq!(
        observed,
        vec![
            (vec![30], 3.0_f64.to_bits(), false),
            (vec![40], 4.0_f64.to_bits(), true),
        ]
    );
    assert_eq!(left_drops.load(Ordering::SeqCst), 1);
    assert_eq!(right_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rejects_malformed_public_chunk_and_unfinished_series_at_eof() {
    let malformed = RawSeriesChunk {
        primary_key: read_key("rack-a", "sensor-1"),
        metric: None,
        time: RawTimestamps::try_new(ScalarBuffer::from(vec![10, 20])).unwrap(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0]))),
        is_last: true,
    };
    let (input, _, drops) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [ready(malformed)],
    );
    let mut merged = merge_ordered_raw_series(vec![input]).unwrap();
    let error = merged.next().await.unwrap().unwrap_err().to_string();
    assert!(
        error.to_lowercase().contains("field") || error.to_lowercase().contains("length"),
        "{error}"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(merged.next().await.is_none());

    for (malformed, reason) in [
        (
            RawSeriesChunk {
                primary_key: read_key("rack-a", "sensor-2"),
                metric: None,
                time: RawTimestamps::try_new(ScalarBuffer::from(Vec::<i64>::new())).unwrap(),
                fields: SeriesFields::one(Samples::from_float(Float64Array::from(
                    Vec::<f64>::new(),
                ))),
                is_last: true,
            },
            "raw series chunks must not be empty",
        ),
        (
            RawSeriesChunk {
                primary_key: read_key("rack-a", "sensor-3"),
                metric: None,
                time: RawTimestamps::try_new(ScalarBuffer::from(vec![30])).unwrap(),
                fields: SeriesFields::try_from_vec(vec![
                    Samples::from_float(Float64Array::from(vec![3.0])),
                    Samples::from_float(Float64Array::from(vec![4.0])),
                ])
                .unwrap(),
                is_last: true,
            },
            "field count",
        ),
    ] {
        let (input, _, drops) = stream(
            label_source(1),
            SeriesOrdering::KeyThenTime,
            [ready(malformed)],
        );
        let mut merged = merge_ordered_raw_series(vec![input]).unwrap();
        let error = merged.next().await.unwrap().unwrap_err().to_string();
        assert!(
            error.to_lowercase().contains(reason),
            "expected {reason:?}: {error}"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(merged.next().await.is_none());
    }

    let unfinished = RawSeriesChunk {
        primary_key: read_key("rack-a", "sensor-1"),
        metric: None,
        time: RawTimestamps::try_new(ScalarBuffer::from(vec![30])).unwrap(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![3.0]))),
        is_last: false,
    };
    let (input, _, drops) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [ready(unfinished), Poll::Ready(None)],
    );
    let mut merged = merge_ordered_raw_series(vec![input]).unwrap();
    let error = merged.next().await.unwrap().unwrap_err().to_string();
    assert!(
        error.to_lowercase().contains("last") || error.to_lowercase().contains("final"),
        "{error}"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(merged.next().await.is_none());
}

#[tokio::test]
async fn equal_timestamp_at_successor_boundary_is_an_error() {
    let key = read_key("rack-a", "sensor-1");
    let (left, _, drops_a) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key.clone(), &[10, 30], &[1.0, 3.0], true))],
    );
    let (right, _, drops_b) = stream(
        label_source(2),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key, &[20, 30], &[2.0, 30.0], true))],
    );
    let mut merged = merge_ordered_raw_series(vec![left, right]).unwrap();
    let first = merged.next().await.unwrap().unwrap();
    let second = merged.next().await.unwrap().unwrap();
    assert_eq!(first.time.as_buffer().as_ref(), &[10]);
    assert_eq!(second.time.as_buffer().as_ref(), &[20]);
    assert!(!first.is_last && !second.is_last);
    let error = merged.next().await.unwrap().unwrap_err().to_string();
    assert!(
        error.to_lowercase().contains("duplicate") || error.to_lowercase().contains("timestamp"),
        "{error}"
    );
    assert_eq!(drops_a.load(Ordering::SeqCst), 1);
    assert_eq!(drops_b.load(Ordering::SeqCst), 1);
    assert!(merged.next().await.is_none());
}

#[tokio::test]
async fn greater_key_head_allows_finished_series_before_future_eof() {
    let key_a = read_key("rack-a", "sensor-1");
    let key_b = read_key("rack-b", "sensor-1");
    let gate = Arc::new(Mutex::new(None::<Waker>));
    let release = Arc::new(AtomicUsize::new(0));
    let (right, right_polls, right_drops) = gated_stream(
        label_source(2),
        chunk(key_b, &[5], &[5.0], true),
        gate.clone(),
        release.clone(),
    );
    let (left, _, left_drops) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key_a.clone(), &[10], &[1.0], true))],
    );
    let mut merged = merge_ordered_raw_series(vec![left, right]).unwrap();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let item = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(item))) => item,
        _ => panic!("expected finished smaller key"),
    };
    assert_eq!(item.primary_key, key_a);
    assert!(item.is_last);
    assert!(
        gate.lock().unwrap().is_none(),
        "known greater head makes an EOF poll unnecessary"
    );
    assert_eq!(
        right_polls.load(Ordering::SeqCst),
        1,
        "known greater head must not be polled again"
    );
    drop(item);
    drop(merged);
    assert_eq!(left_drops.load(Ordering::SeqCst), 1);
    assert_eq!(right_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn preflight_rejects_incompatible_primary_key_bindings_without_polling() {
    let full_baseline = label_source(1);
    let physical_comparison_baseline = {
        let RawLabelSource::PrimaryKey {
            metric_name,
            physical_metadata,
            ..
        } = label_source(1)
        else {
            unreachable!()
        };
        RawLabelSource::PrimaryKey {
            metric_name,
            physical_metadata,
            logical_tags: Arc::from([BoundTag {
                column_id: 9,
                name: Arc::from("sensor"),
            }]),
        }
    };
    let cases = [
        (
            "metric name",
            label_source_with(2, PrimaryKeyEncoding::Dense, "other_metric"),
            "binding",
        ),
        (
            "encoding",
            label_source_with(2, PrimaryKeyEncoding::Sparse, "room_temperature"),
            "binding",
        ),
        (
            "primary key column ids",
            {
                let RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata,
                    logical_tags,
                } = label_source(2)
                else {
                    unreachable!()
                };
                let mut changed = (*physical_metadata).clone();
                changed.primary_key = vec![9, 8];
                RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata: Arc::new(changed),
                    logical_tags,
                }
            },
            "binding",
        ),
        (
            "primary key physical name mismatch",
            {
                let RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata,
                    ..
                } = label_source(2)
                else {
                    unreachable!()
                };
                let mut changed = (*physical_metadata).clone();
                changed
                    .column_metadatas
                    .iter_mut()
                    .find(|column| column.column_id == 8)
                    .unwrap()
                    .column_schema
                    .name = "shelf".to_string();
                RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata: Arc::new(changed),
                    logical_tags: Arc::from([BoundTag {
                        column_id: 9,
                        name: Arc::from("sensor"),
                    }]),
                }
            },
            "merge compatibility",
        ),
        (
            "primary key semantic role",
            {
                let RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata,
                    ..
                } = label_source(2)
                else {
                    unreachable!()
                };
                let mut changed = (*physical_metadata).clone();
                changed
                    .column_metadatas
                    .iter_mut()
                    .find(|column| column.column_id == 8)
                    .unwrap()
                    .semantic_type = SemanticType::Field;
                RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata: Arc::new(changed),
                    logical_tags: Arc::from([BoundTag {
                        column_id: 9,
                        name: Arc::from("sensor"),
                    }]),
                }
            },
            "merge compatibility",
        ),
        (
            "primary key physical type",
            {
                let RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata,
                    logical_tags,
                } = label_source(2)
                else {
                    unreachable!()
                };
                let mut changed = (*physical_metadata).clone();
                changed
                    .column_metadatas
                    .iter_mut()
                    .find(|column| column.column_id == 8)
                    .unwrap()
                    .column_schema
                    .data_type = ConcreteDataType::int64_datatype();
                RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata: Arc::new(changed),
                    logical_tags,
                }
            },
            "binding",
        ),
        (
            "logical tag mapping",
            {
                let RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata,
                    ..
                } = label_source(2)
                else {
                    unreachable!()
                };
                RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata,
                    logical_tags: Arc::from([BoundTag {
                        column_id: 8,
                        name: Arc::from("rack"),
                    }]),
                }
            },
            "binding",
        ),
    ];
    for (description, incompatible, reason) in cases {
        let baseline = if reason == "merge compatibility" {
            &physical_comparison_baseline
        } else {
            &full_baseline
        };
        let (first, polls_a, drops_a) =
            stream((*baseline).clone(), SeriesOrdering::KeyThenTime, []);
        let (second, polls_b, drops_b) = stream(incompatible, SeriesOrdering::KeyThenTime, []);
        let error = match merge_ordered_raw_series(vec![first, second]) {
            Ok(_) => panic!("{description} mismatch must be rejected"),
            Err(error) => error.to_string(),
        };
        if reason == "merge compatibility" {
            assert_eq!(
                error,
                "Invalid series: raw merge requires equal raw schemas, primary-key bindings, and ReadKeyThenTime ordering",
                "{description}"
            );
        } else {
            assert!(
                error.to_lowercase().contains(reason),
                "{description}: {error}"
            );
        }
        assert_eq!(polls_a.load(Ordering::SeqCst), 0);
        assert_eq!(polls_b.load(Ordering::SeqCst), 0);
        assert_eq!(drops_a.load(Ordering::SeqCst), 1);
        assert_eq!(drops_b.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn metadata_snapshots_with_same_layout_are_accepted() {
    let base = label_source(1);
    let RawLabelSource::PrimaryKey {
        metric_name,
        physical_metadata,
        logical_tags,
    } = base
    else {
        unreachable!()
    };
    let mut builder = RegionMetadataBuilder::from_existing((*physical_metadata).clone());
    builder.bump_version();
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "unrelated_note",
            ConcreteDataType::string_datatype(),
            true,
        ),
        semantic_type: SemanticType::Field,
        column_id: 99,
    });
    let reordered_tags = Arc::from([
        BoundTag {
            column_id: 9,
            name: Arc::from("sensor"),
        },
        BoundTag {
            column_id: 8,
            name: Arc::from("rack"),
        },
    ]);
    let mut second_metadata = builder.build().unwrap();
    second_metadata.region_id = RegionId::new(2, 1);
    let second_binding = RawLabelSource::PrimaryKey {
        metric_name,
        physical_metadata: Arc::new(second_metadata),
        logical_tags: reordered_tags,
    };
    assert_ne!(
        physical_metadata.region_id,
        match &second_binding {
            RawLabelSource::PrimaryKey {
                physical_metadata, ..
            } => physical_metadata.region_id,
            _ => unreachable!(),
        }
    );
    let first_binding = RawLabelSource::PrimaryKey {
        metric_name: Arc::from("room_temperature"),
        physical_metadata,
        logical_tags,
    };
    let (left, _, _) = stream(
        first_binding,
        SeriesOrdering::KeyThenTime,
        [ready(chunk(
            read_key("rack-a", "sensor-1"),
            &[1],
            &[1.0],
            true,
        ))],
    );
    let (right, _, _) = stream(
        second_binding,
        SeriesOrdering::KeyThenTime,
        [ready(chunk(
            read_key("rack-b", "sensor-1"),
            &[2],
            &[2.0],
            true,
        ))],
    );
    let mut merged = merge_ordered_raw_series(vec![left, right]).unwrap();
    let first = merged.next().await.unwrap().unwrap();
    assert_eq!(first.time.as_buffer().as_ref(), &[1]);
    drop(first);
    assert_eq!(
        merged
            .next()
            .await
            .unwrap()
            .unwrap()
            .time
            .as_buffer()
            .as_ref(),
        &[2]
    );
    assert!(merged.next().await.is_none());
}

#[tokio::test]
async fn pending_inputs_are_all_polled_and_wakers_registered_without_spinning() {
    use futures::task::{ArcWake, waker_ref};

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);
    impl ArcWake for WakeCounter {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let gate_a = Arc::new(Mutex::new(None::<Waker>));
    let gate_b = Arc::new(Mutex::new(None::<Waker>));
    let release_a = Arc::new(AtomicUsize::new(0));
    let release_b = Arc::new(AtomicUsize::new(0));
    let (left, left_polls, left_drops) =
        gated_pending_stream(label_source(1), gate_a.clone(), release_a.clone());
    let (right, right_polls, right_drops) =
        gated_pending_stream(label_source(2), gate_b.clone(), release_b.clone());
    let mut merged = merge_ordered_raw_series(vec![left, right]).unwrap();
    let counter = Arc::new(WakeCounter::default());
    let waker = waker_ref(&counter);
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Pending
    ));
    assert!(gate_a.lock().unwrap().is_some());
    assert!(gate_b.lock().unwrap().is_some());
    let before = (
        left_polls.load(Ordering::SeqCst),
        right_polls.load(Ordering::SeqCst),
    );
    assert_eq!(
        before,
        (1, 1),
        "poll each missing head once in this outer poll"
    );
    assert_eq!(
        counter.0.load(Ordering::SeqCst),
        0,
        "Pending inputs must not self-wake"
    );
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Pending
    ));
    assert_eq!(
        (
            left_polls.load(Ordering::SeqCst),
            right_polls.load(Ordering::SeqCst)
        ),
        (2, 2),
        "each new outer poll checks each pending input once"
    );
    release_a.store(1, Ordering::SeqCst);
    release_b.store(1, Ordering::SeqCst);
    gate_a.lock().unwrap().take().unwrap().wake();
    gate_b.lock().unwrap().take().unwrap().wake();
    assert_eq!(counter.0.load(Ordering::SeqCst), 2);
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(left_drops.load(Ordering::SeqCst), 1);
    assert_eq!(right_drops.load(Ordering::SeqCst), 1);
    drop(merged);
}

#[tokio::test]
async fn cancel_and_eof_drop_owned_inputs_and_candidate_key_buffers() {
    let key = read_key("rack-a", "sensor-1");
    let bytes = key.as_slice().to_vec();
    let key_drops = Arc::new(AtomicUsize::new(0));
    let (owned, weak_owner) = owned_key(bytes, key_drops.clone());
    let (input, _, drops) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(owned, &[1], &[1.0], true))],
    );
    let mut merged = merge_ordered_raw_series(vec![input]).unwrap();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let output = match Pin::new(&mut merged).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(output))) => output,
        _ => panic!("expected emitted output"),
    };
    assert_eq!(
        key_drops.load(Ordering::SeqCst),
        0,
        "output retains its actual key allocation"
    );
    drop(merged);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        key_drops.load(Ordering::SeqCst),
        0,
        "output remains the owner after upstream drop"
    );
    assert!(weak_owner.upgrade().is_some());
    drop(output);
    assert!(
        weak_owner.upgrade().is_none(),
        "dropping output releases the allocation"
    );
    assert_eq!(key_drops.load(Ordering::SeqCst), 1);

    let gate = Arc::new(Mutex::new(None::<Waker>));
    let release = Arc::new(AtomicUsize::new(0));
    let pending_drops = Arc::new(AtomicUsize::new(0));
    let (pending_key, pending_weak) = owned_key(
        read_key("rack-z", "sensor-9").as_slice().to_vec(),
        pending_drops.clone(),
    );
    let (input, _, drops) = gated_stream(
        label_source(2),
        chunk(pending_key, &[1], &[1.0], false),
        gate.clone(),
        release,
    );
    let mut merged = merge_ordered_raw_series(vec![input]).unwrap();
    assert!(matches!(
        Pin::new(&mut merged).poll_next(&mut cx),
        Poll::Pending
    ));
    assert!(gate.lock().unwrap().is_some());
    assert!(
        pending_weak.upgrade().is_some(),
        "pending merge retains its candidate payload"
    );
    drop(merged);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        pending_weak.upgrade().is_none(),
        "cancellation releases candidate key owner"
    );
    assert_eq!(pending_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn previous_key_validation_retains_only_key_bytes_not_input_allocation() {
    let parent = (0..32).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let owner_drops = Arc::new(AtomicUsize::new(0));
    let (parent_key, weak_owner) = owned_key(parent, owner_drops.clone());
    let offset = 7;
    let key_len = read_key("rack-a", "sensor-1").len();
    let owner_key = parent_key.slice_with_length(offset, key_len);
    drop(parent_key);
    let input_chunk = chunk(owner_key, &[10], &[1.0], true);
    let (upstream, _, _) = stream(label_source(1), SeriesOrdering::KeyThenTime, []);
    let mut input = Input::new(upstream);
    input.validate(&input_chunk, &schema()).unwrap();
    drop(input_chunk);
    assert!(
        weak_owner.upgrade().is_none(),
        "previous_key should copy key bytes, not retain its allocation owner"
    );
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ordered_input_contract_rejects_bad_key_time_and_finality_transitions() {
    let a = read_key("rack-a", "sensor-1");
    let b = read_key("rack-b", "sensor-1");
    // `RawTimestamps` rejects descending axes at construction; a merge only observes
    // chunks that already satisfy the strictly increasing time contract.
    assert!(RawTimestamps::try_new(ScalarBuffer::from(vec![20, 10])).is_err());
    let cases = [
        (
            "series recurs after another key",
            vec![
                chunk(a.clone(), &[1], &[1.0], true),
                chunk(b.clone(), &[2], &[2.0], true),
                chunk(a.clone(), &[3], &[3.0], true),
            ],
            "key",
        ),
        (
            "unfinished series at key change",
            vec![
                chunk(a.clone(), &[1], &[1.0], false),
                chunk(b.clone(), &[2], &[2.0], true),
            ],
            "last",
        ),
        (
            "series recurs after final chunk",
            vec![
                chunk(a.clone(), &[1], &[1.0], true),
                chunk(a.clone(), &[2], &[2.0], true),
            ],
            "last",
        ),
        (
            "non-increasing timestamps across chunks",
            vec![
                chunk(a.clone(), &[10], &[1.0], false),
                chunk(a.clone(), &[10], &[2.0], true),
            ],
            "time",
        ),
        (
            "explicit metric in primary-key mode",
            vec![RawSeriesChunk {
                primary_key: a.clone(),
                metric: Some(MetricName::try_new(None, vec![]).unwrap()),
                time: RawTimestamps::try_new(ScalarBuffer::from(vec![1])).unwrap(),
                fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0]))),
                is_last: true,
            }],
            "primary-key",
        ),
    ];
    for (description, chunks, reason) in cases {
        let (input, _, drops) = stream(
            label_source(1),
            SeriesOrdering::KeyThenTime,
            chunks.into_iter().map(ready),
        );
        let mut merged = merge_ordered_raw_series(vec![input]).unwrap();
        let error = loop {
            match merged.next().await {
                Some(Ok(_)) => {}
                Some(Err(error)) => break error.to_string(),
                None => panic!("{description} should fail"),
            }
        };
        assert!(
            error.to_lowercase().contains(reason),
            "{description}: {error}"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1, "{description}");
        assert!(merged.next().await.is_none(), "{description} must be fused");
    }
}

#[tokio::test]
async fn sparse_complete_keys_keep_tag_and_internal_identity_distinct() {
    use mito_codec::row_converter::SparsePrimaryKeyCodec;

    let sparse_source = label_source_with_tags(
        1,
        PrimaryKeyEncoding::Sparse,
        "room_temperature",
        vec![(42, "rack"), (43, "sensor")],
    );
    let codec = SparsePrimaryKeyCodec::with_fields(vec![
        (42, SortField::new(ConcreteDataType::string_datatype())),
        (43, SortField::new(ConcreteDataType::string_datatype())),
    ]);
    let sparse_key = |table_id, tsid, rack: &str, sensor: &str| {
        let mut bytes = Vec::new();
        codec.encode_internal(table_id, tsid, &mut bytes).unwrap();
        codec
            .encode_values(
                &[
                    (42, Value::String(rack.into())),
                    (43, Value::String(sensor.into())),
                ],
                &mut bytes,
            )
            .unwrap();
        Buffer::from(bytes)
    };
    let key_one = sparse_key(42, 9, "rack-a", "sensor-1");
    let key_two = sparse_key(42, 9, "rack-a", "sensor-2");
    let key_three = sparse_key(43, 9, "rack-a", "sensor-1");
    let mut key_records = [
        (key_one.clone(), 42, 9, "sensor-1"),
        (key_two.clone(), 42, 9, "sensor-2"),
        (key_three.clone(), 43, 9, "sensor-1"),
    ];
    key_records.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()));
    let left_chunks = [key_records[0].0.clone(), key_records[2].0.clone()]
        .into_iter()
        .enumerate()
        .map(|(index, key)| {
            let (time, value) = if index == 0 { (1, 10.0) } else { (3, 30.0) };
            ready(chunk(key, &[time], &[value], true))
        });
    let right_chunks = [key_records[1].0.clone()]
        .into_iter()
        .map(|key| ready(chunk(key, &[2], &[20.0], true)));
    let (left, _, _) = stream(
        sparse_source.clone(),
        SeriesOrdering::KeyThenTime,
        left_chunks,
    );
    let (right, _, _) = stream(sparse_source, SeriesOrdering::KeyThenTime, right_chunks);
    let merged = merge_ordered_raw_series(vec![left, right]).unwrap();
    let rows = super::super::materialize_raw_labels(merged)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    let observed = rows
        .iter()
        .map(|chunk| chunk.primary_key.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        key_records
            .iter()
            .map(|record| record.0.clone())
            .collect::<Vec<_>>()
    );
    assert!(rows.iter().all(|chunk| chunk.is_last));
    let expected_times_and_values = [
        (1, 10.0_f64.to_bits()),
        (2, 20.0_f64.to_bits()),
        (3, 30.0_f64.to_bits()),
    ];
    for (row, (expected_time, expected_value)) in rows.iter().zip(expected_times_and_values) {
        assert_eq!(row.time.timestamp(0), Some(expected_time));
        assert!(
            matches!(row.fields.get(0).unwrap().sample(0), SampleRef::Float(value) if value.to_bits() == expected_value)
        );
    }
    let metrics = rows
        .iter()
        .map(|row| row.metric.as_ref().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(metrics[0].get("rack"), Some("rack-a"));
    assert_eq!(metrics[1].get("rack"), Some("rack-a"));
    assert_eq!(metrics[2].get("rack"), Some("rack-a"));
    assert_eq!(metrics[0].tags().len(), 2);
    assert_eq!(metrics[0].get("sensor"), Some(key_records[0].3));
    assert_eq!(metrics[1].get("sensor"), Some(key_records[1].3));
    assert_eq!(metrics[2].get("sensor"), Some(key_records[2].3));
    assert_eq!(metrics[0].name(), Some("room_temperature"));
    assert_eq!(
        metrics[0], metrics[2],
        "different complete internal keys with same labels remain separate"
    );
    assert_ne!(rows[0].primary_key, rows[2].primary_key);
}

#[tokio::test]
async fn primary_key_codec_identity_survives_merge_then_label_materialization() {
    let key = read_key("rack-z", "sensor-4");
    let (input, _, _) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(
            key.clone(),
            &[1, 2],
            &[18.5, f64::from_bits(0x7ff8_0000_0000_0042)],
            true,
        ))],
    );
    let merged = merge_ordered_raw_series(vec![input]).unwrap();
    let mut materialized = super::super::materialize_raw_labels(merged).unwrap();
    let output = materialized.next().await.unwrap().unwrap();
    assert_eq!(output.primary_key, key);
    assert_eq!(
        output.metric.as_ref().unwrap().name(),
        Some("room_temperature")
    );
    assert_eq!(output.metric.as_ref().unwrap().get("rack"), Some("rack-z"));
    assert_eq!(
        output.metric.as_ref().unwrap().get("sensor"),
        Some("sensor-4")
    );
    assert_eq!(output.time.as_buffer().as_ref(), &[1, 2]);
    match output.fields.get(0).unwrap().sample(0) {
        SampleRef::Float(value) => assert_eq!(value, 18.5),
        _ => panic!("expected float sample"),
    }
    match output.fields.get(0).unwrap().sample(1) {
        SampleRef::Float(value) => {
            assert_eq!(value.to_bits(), 0x7ff8_0000_0000_0042)
        }
        _ => panic!("expected NaN sample"),
    }
    assert!(output.is_last);
}

/// A merged stream's label binding survives the frame wire form: the packed schema declares the
/// decode description derived from the merge binding, so a consumer holding only that schema can
/// rebuild a decoder and decode the merged keys without a request-side label source.
#[tokio::test]
async fn merged_frame_declares_a_label_decode_description_that_decodes_its_keys() {
    use common_query::series_frame::{label_decode_spec, pack_raw_series, unpack_raw_series};
    use datatypes::series::frame::raw_series_frame_label_decode;
    use mito_codec::labels::{LabelProjection, PrimaryKeyLabelDecoder};

    let key = read_key("rack-a", "sensor-1");
    let (left, _, _) = stream(
        label_source(1),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key.clone(), &[10, 30], &[1.0, 3.0], true))],
    );
    let (right, _, _) = stream(
        label_source(2),
        SeriesOrdering::KeyThenTime,
        [ready(chunk(key.clone(), &[20, 40], &[2.0, 4.0], true))],
    );
    let merged = merge_ordered_raw_series(vec![left, right]).unwrap();
    let expected = label_decode_spec(merged.label_source()).unwrap();
    let series_schema = merged.schema().clone();
    let binding = merged.label_source().clone();
    let ordering = merged.ordering();
    let framed = pack_raw_series(merged).unwrap();
    let declared = framed.schema();
    assert_eq!(
        raw_series_frame_label_decode(declared.as_ref()).unwrap(),
        Some(expected.clone())
    );

    // The recovered description alone rebuilds an equivalent decoder; no metadata snapshot from
    // the merge is consulted.
    let mut from_schema =
        PrimaryKeyLabelDecoder::from_decode_spec(&expected, LabelProjection::All).unwrap();
    let mut from_binding = PrimaryKeyLabelDecoder::new(&binding, LabelProjection::All).unwrap();
    assert_eq!(
        from_schema.decode(&key).unwrap(),
        from_binding.decode(&key).unwrap()
    );

    let mut decoded = unpack_raw_series(framed, series_schema, binding, ordering).unwrap();
    let first = decoded.next().await.unwrap().unwrap();
    assert_eq!(first.primary_key.as_slice(), key.as_slice());
    assert!(first.metric.is_none(), "key labels stay undecoded");
}
