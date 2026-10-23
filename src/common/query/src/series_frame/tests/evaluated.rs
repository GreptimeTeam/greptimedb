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

//! Streaming round trips through an evaluated series frame.
//!
//! An evaluated frame names its series by their complete labels and addresses time on the grid its
//! schema declares, so every test here drives the real path - pack, a record batch stream, unpack -
//! and compares what a consumer receives against what a producer sent. A schema snapshot would
//! prove the declaration; these prove the values.
//!
//! A frame row is one run of positions on the shared grid, so unpacking yields one [`Series`] per
//! row rather than one per source chunk. A consumer rebuilds a chunk chain by concatenating rows up
//! to the one that carries `is_last`, which is what these tests do.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_recordbatch::DfSendableRecordBatchStream;
use datatypes::arrow::array::{
    Array, BooleanArray, Float64Array, ListArray, StructArray, UInt32Array, UInt64Array,
};
use datatypes::arrow::buffer::OffsetBuffer;
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use datatypes::arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use datatypes::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datatypes::series::frame::{FrameLabelMode, SeriesFrameBuilder, SeriesFrameCursor};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{GridSpan, TimeAxis};
use datatypes::series::{Label, SeriesLabels, SeriesOrdering};
use futures_util::StreamExt;

use super::super::{
    SendableEvaluatedSeriesStream, pack_evaluated_series, series_frame_ordering,
    unpack_evaluated_series, unpack_raw_series,
};
use super::{df_source, evaluated_grid, evaluated_schema, schema};

/// Complete labels of the `cpu` metric on one host.
fn labels(host: &str) -> SeriesLabels {
    SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(host),
        }],
    )
    .unwrap()
}

/// One validated evaluated chunk: `values` on the runs `spans` of the shared test grid.
fn series(spans: &[(usize, usize)], values: Vec<Option<f64>>, metric: SeriesLabels) -> Series {
    let time = TimeAxis::from_grid_spans(
        evaluated_grid(),
        spans
            .iter()
            .map(|(start, len)| GridSpan::new(*start, *len))
            .collect(),
    )
    .unwrap();
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(values)));
    Series::try_new(&evaluated_schema(), metric, time, fields).unwrap()
}

/// An evaluated producer stream over the supplied chunks and their finality.
fn source(items: Vec<(Series, bool)>) -> SendableEvaluatedSeriesStream {
    Box::pin(futures_util::stream::iter(
        items.into_iter().map(Ok::<_, BoxedError>),
    ))
}

/// Packs the supplied chunks, keeping the stream so a test can observe its errors.
fn pack(items: Vec<(Series, bool)>) -> DfSendableRecordBatchStream {
    pack_evaluated_series(source(items), evaluated_schema()).unwrap()
}

/// Packs the supplied chunks and drains every ready batch.
async fn packed(items: Vec<(Series, bool)>) -> Vec<RecordBatch> {
    let mut stream = pack(items);
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch.unwrap());
    }
    out
}

/// Unpacks the batches a producer published, under the schema they declare.
fn unpack(batches: Vec<RecordBatch>, declared: SchemaRef) -> SendableEvaluatedSeriesStream {
    let (source, _, _) = df_source(batches, declared);
    unpack_evaluated_series(source, Some(evaluated_schema())).unwrap()
}

/// Drains one unpacked stream, so a test can assert on the whole sequence at once.
async fn drain(mut stream: SendableEvaluatedSeriesStream) -> Vec<(Series, bool)> {
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.push(chunk.unwrap());
    }
    out
}

/// The absolute grid positions a decoded chunk carries, in order.
fn positions(series: &Series) -> Vec<usize> {
    series
        .time
        .positions()
        .map(|(position, _)| position)
        .collect()
}

/// The float payload of the one `value` field, in compact-row order.
fn values(series: &Series) -> Vec<Option<f64>> {
    series
        .fields
        .as_slice()
        .first()
        .expect("the fixture has one value field")
        .iter()
        .map(|sample| match sample {
            SampleRef::Missing => None,
            SampleRef::Float(value) => Some(value),
            _ => panic!("the fixture carries float samples"),
        })
        .collect()
}

/// The positions and values of every decoded row, concatenated in stream order.
fn flat(rows: &[(Series, bool)]) -> (Vec<usize>, Vec<Option<f64>>) {
    let mut all_positions = Vec::new();
    let mut all_values = Vec::new();
    for (row, _) in rows {
        all_positions.extend(positions(row));
        all_values.extend(values(row));
    }
    (all_positions, all_values)
}

/// Rewrites the `is_last` column of a frame, so a test can produce a row a producer refuses to write
/// and check that the reader refuses it too.
fn with_is_last(batch: &RecordBatch, is_last: bool) -> RecordBatch {
    let mut columns = batch.columns().to_vec();
    columns[1] = Arc::new(BooleanArray::from(vec![is_last; batch.num_rows()]));
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

/// Rewrites the `read_key` column of a frame, so a test can attach the storage key an evaluated row
/// must not carry.
fn with_read_key(batch: &RecordBatch, key: &[u8]) -> RecordBatch {
    let keys = vec![Some(key); batch.num_rows()];
    let mut columns = batch.columns().to_vec();
    columns[0] = Arc::new(datatypes::arrow::array::BinaryArray::from(keys));
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

/// The frame schema a producer's batches carry, with one metadata entry changed the way a divergent
/// producer would write it.
fn relabelled(declared: &SchemaRef, key: &str, value: Option<&str>) -> SchemaRef {
    let mut metadata = declared.metadata().clone();
    match value {
        Some(value) => metadata.insert(key.to_string(), value.to_string()),
        None => metadata.remove(key),
    };
    Arc::new(datatypes::arrow::datatypes::Schema::new_with_metadata(
        declared.fields().clone(),
        metadata,
    ))
}

/// One dense chunk keeps every position, label, and value, and its finality.
#[tokio::test]
async fn dense_roundtrip_preserves_positions_labels_and_values() {
    let metric = labels("a");
    let expected: Vec<Option<f64>> = (0..8)
        .map(|row| (row % 3 != 0).then_some(row as f64))
        .collect();
    let batches = packed(vec![(
        series(&[(0, 8)], expected.clone(), metric.clone()),
        true,
    )])
    .await;
    assert_eq!(batches.len(), 1, "eight positions are one row");
    assert_eq!(batches[0].num_rows(), 1);

    let declared = batches[0].schema();
    assert_eq!(
        series_frame_ordering(declared.as_ref()).unwrap(),
        SeriesOrdering::Contiguous,
        "an evaluated frame declares contiguous ordering"
    );
    let declaration = datatypes::series::frame::SeriesFrameDeclaration::parse(declared.as_ref())
        .expect("an evaluated frame declares itself");
    assert_eq!(
        declaration.content(),
        datatypes::series::frame::FrameContent::Evaluated
    );
    assert_eq!(
        declaration.labels(),
        datatypes::series::frame::FrameLabelMode::Explicit
    );
    assert_eq!(
        declared
            .metadata()
            .get("greptime.series.time")
            .map(String::as_str),
        Some("grid_spans"),
        "an evaluated frame addresses time on its grid"
    );
    assert!(declaration.label_decode().is_none(), "no key is decoded");

    let rows = drain(unpack(batches, declared)).await;
    assert_eq!(rows.len(), 1);
    let (chunk, is_last) = &rows[0];
    assert!(is_last, "the source closed the chain");
    assert_eq!(chunk.metric, metric, "labels round trip");
    assert_eq!(positions(chunk), (0..8).collect::<Vec<_>>());
    assert_eq!(values(chunk), expected, "every value, including nulls");
    assert!(
        chunk
            .time
            .grid()
            .is_some_and(|grid| grid.spec() == evaluated_grid().spec()),
        "the decoded chunk is addressed on the frame's own grid"
    );
}

/// A sparse chunk keeps its holes: the decoded positions are exactly the ones the producer sent, and
/// every field stays aligned with them.
#[tokio::test]
async fn sparse_roundtrip_keeps_holes_and_field_alignment() {
    let metric = labels("b");
    let expected = vec![Some(1.0), None, Some(3.0), Some(4.0), None];
    let batches = packed(vec![(
        series(&[(0, 2), (5, 3)], expected.clone(), metric.clone()),
        true,
    )])
    .await;
    assert_eq!(batches.len(), 1);
    let declared = batches[0].schema();
    let rows = drain(unpack(batches, declared)).await;

    let (decoded_positions, decoded_values) = flat(&rows);
    assert_eq!(
        decoded_positions,
        vec![0, 1, 5, 6, 7],
        "the hole between the runs is not filled"
    );
    assert_eq!(
        decoded_values, expected,
        "values stay aligned to positions, hole included"
    );
    for (row, _) in &rows {
        assert_eq!(row.metric, metric);
        for hole in [2, 3, 4] {
            assert!(
                row.time.row_of(hole).is_none(),
                "no row exists at a position the chunk does not hold"
            );
        }
    }
    assert!(
        rows.last().expect("at least one row").1,
        "only the last row of the chunk closes the chain"
    );
    assert_eq!(
        rows.iter().filter(|(_, is_last)| *is_last).count(),
        1,
        "one chain, one close"
    );
}

/// A chunk with no positions that closes its chain is one frame row, not a dropped chunk: a consumer
/// has to see the finality it carries.
#[tokio::test]
async fn zero_sample_closing_chunk_is_kept_as_one_row() {
    let metric = labels("c");
    let batches = packed(vec![
        (
            series(&[(3, 2)], vec![Some(1.0), Some(2.0)], metric.clone()),
            false,
        ),
        (series(&[], vec![], metric.clone()), true),
    ])
    .await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].num_rows(),
        2,
        "the closing chunk keeps its own row"
    );

    let declared = batches[0].schema();
    let rows = drain(unpack(batches, declared)).await;
    assert_eq!(rows.len(), 2);
    let (carried, carried_is_last) = &rows[0];
    assert!(!carried_is_last, "the first chunk stays open");
    assert_eq!(positions(carried), vec![3, 4]);
    assert_eq!(values(carried), vec![Some(1.0), Some(2.0)]);
    let (closing, closing_is_last) = &rows[1];
    assert!(closing_is_last, "the closing chunk keeps its finality");
    assert_eq!(
        positions(closing),
        Vec::<usize>::new(),
        "and carries no position"
    );
    assert_eq!(values(closing), Vec::<Option<f64>>::new());
    assert_eq!(closing.metric, metric, "labels survive on a closing chunk");
}

/// One grid position per run, so a chunk with `count` positions starting at `start` becomes `count`
/// frame rows: one row per run, which is what a batch bound can split.
fn single_position_spans(start: usize, count: usize) -> Vec<(usize, usize)> {
    (0..count).map(|offset| (start + offset, 1)).collect()
}

/// Rows split across batches keep their order and finality: every position appears once and in
/// order, and only the last row of the source's final chunk closes the chain.
#[tokio::test]
async fn multi_chunk_multi_batch_stream_preserves_order_and_finality() {
    let metric = labels("d");
    let first: Vec<Option<f64>> = (0..300).map(|row| Some(row as f64)).collect();
    let second: Vec<Option<f64>> = (0..10).map(|row| Some(row as f64)).collect();
    let batches = packed(vec![
        (
            series(&single_position_spans(0, 300), first, metric.clone()),
            false,
        ),
        (
            series(&single_position_spans(300, 10), second, metric.clone()),
            true,
        ),
    ])
    .await;
    assert!(
        batches.len() > 1,
        "the default row bound splits 310 rows into several batches"
    );
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        310
    );

    let declared = batches[0].schema();
    assert!(batches.iter().all(|batch| batch.schema() == declared));
    let rows = drain(unpack(batches, declared)).await;
    let (decoded_positions, _) = flat(&rows);
    assert_eq!(
        decoded_positions,
        (0..310).collect::<Vec<_>>(),
        "no position is repeated, reordered, or dropped across batches"
    );
    assert!(rows.iter().all(|(row, _)| row.metric == metric));
    let closes = rows
        .iter()
        .enumerate()
        .filter(|(_, (_, is_last))| *is_last)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert_eq!(
        closes,
        vec![rows.len() - 1],
        "exactly the last row closes the chain, inherited from the source's final chunk"
    );
}

/// Two chains whose labels happen to be equal are two chains: each closes itself, and the second is
/// not mistaken for a reopened series.
#[tokio::test]
async fn consecutive_chains_with_equal_labels_each_close_independently() {
    let metric = labels("e");
    let batches = packed(vec![
        (
            series(&[(0, 2)], vec![Some(1.0), Some(2.0)], metric.clone()),
            true,
        ),
        (
            series(&[(2, 2)], vec![Some(3.0), Some(4.0)], metric.clone()),
            true,
        ),
    ])
    .await;

    let declared = batches[0].schema();
    let rows = drain(unpack(batches, declared)).await;
    assert_eq!(rows.len(), 2, "one row per source chunk");
    assert!(
        rows.iter().all(|(_, is_last)| *is_last),
        "each chain closes itself, so neither is refused as a reopened series"
    );
    assert!(rows.iter().all(|(row, _)| row.metric == metric));
    assert_eq!(positions(&rows[0].0), vec![0, 1]);
    assert_eq!(values(&rows[0].0), vec![Some(1.0), Some(2.0)]);
    assert_eq!(positions(&rows[1].0), vec![2, 3]);
    assert_eq!(values(&rows[1].0), vec![Some(3.0), Some(4.0)]);
}

/// A chain still open at EOF is an error on both sides: a producer that never closed a series cannot
/// finish, and a receiver cannot pretend the stream ended cleanly.
#[tokio::test]
async fn eof_with_an_open_chain_is_an_error() {
    let metric = labels("f");
    let mut stream = pack(vec![(
        series(&[(0, 2)], vec![Some(1.0), Some(2.0)], metric.clone()),
        false,
    )]);
    // The rows are buffered until EOF, where the missing close is reported instead of a batch.
    let error = loop {
        match stream.next().await {
            Some(Ok(_)) => continue,
            Some(Err(error)) => break error,
            None => panic!("an open chain at EOF must be reported"),
        }
    };
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
        StatusCode::InvalidArguments
    );
    assert!(stream.next().await.is_none(), "the error is reported once");

    // The receiving side reaches the same conclusion from the frame alone.
    let closing = packed(vec![(
        series(&[(0, 2)], vec![Some(1.0), Some(2.0)], metric),
        true,
    )])
    .await;
    let declared = closing[0].schema();
    let opened = with_is_last(&closing[0], false);
    let (source, _, _) = df_source(vec![opened], declared);
    let mut unpacked = unpack_evaluated_series(source, Some(evaluated_schema())).unwrap();
    assert!(!unpacked.next().await.unwrap().unwrap().1);
    let error = unpacked.next().await.unwrap().unwrap_err();
    assert_eq!(error.status_code(), StatusCode::InvalidArguments);
    assert!(unpacked.next().await.is_none());
}

/// A chunk with no position that does not close its chain can never make progress, so both sides
/// refuse it.
#[tokio::test]
async fn zero_sample_chunk_that_does_not_close_is_rejected() {
    let metric = labels("g");
    let mut stream = pack(vec![(series(&[], vec![], metric.clone()), false)]);
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(
        crate::error::datafusion_status_code::<crate::error::Error>(&error, None),
        StatusCode::InvalidArguments
    );
    assert!(stream.next().await.is_none());

    let closing = packed(vec![(series(&[], vec![], metric), true)]).await;
    assert_eq!(closing[0].num_rows(), 1);
    let declared = closing[0].schema();
    let not_closing = with_is_last(&closing[0], false);
    let (source, _, _) = df_source(vec![not_closing], declared);
    let mut unpacked = unpack_evaluated_series(source, Some(evaluated_schema())).unwrap();
    let error = unpacked.next().await.unwrap().unwrap_err();
    assert_eq!(error.status_code(), StatusCode::InvalidArguments);
    assert!(unpacked.next().await.is_none());
}

/// Computed values have no storage key to be ordered by, so an evaluated frame that declares any
/// other ordering, or a spelling the wire does not define, is refused before the stream is returned.
#[tokio::test]
async fn evaluated_frame_rejects_non_contiguous_ordering() {
    let batches = packed(vec![(
        series(&[(0, 1)], vec![Some(1.0)], labels("h")),
        true,
    )])
    .await;
    let declared = batches[0].schema();
    for ordering in ["read_key_then_time", "interleaved"] {
        let schema = relabelled(&declared, "greptime.series.ordering", Some(ordering));
        let (source, polls, _) = df_source(vec![], schema);
        assert!(
            unpack_evaluated_series(source, Some(evaluated_schema())).is_err(),
            "{ordering} is not an ordering computed values can claim"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
}

/// Every contradictory or unreadable declaration is refused instead of decoded: a missing or
/// malformed grid, a time mode that contradicts the content, and keyed identity on computed values.
#[tokio::test]
async fn evaluated_frame_rejects_contradictory_declarations() {
    let batches = packed(vec![(
        series(&[(0, 1)], vec![Some(1.0)], labels("i")),
        true,
    )])
    .await;
    let declared = batches[0].schema();
    let cases: [(&str, SchemaRef); 6] = [
        (
            "no grid",
            relabelled(&declared, "greptime.series.grid", None),
        ),
        (
            "malformed grid",
            relabelled(&declared, "greptime.series.grid", Some("grid")),
        ),
        (
            "incomplete grid",
            relabelled(&declared, "greptime.series.grid", Some(r#"{"start_ms":0}"#)),
        ),
        (
            "evaluated with timestamps",
            relabelled(&declared, "greptime.series.time", Some("timestamps")),
        ),
        (
            "evaluated with key labels",
            relabelled(&declared, "greptime.series.labels", Some("key")),
        ),
        (
            "raw content",
            relabelled(&declared, "greptime.series.content", Some("raw")),
        ),
    ];
    for (label, schema) in cases {
        let (source, polls, _) = df_source(vec![], schema);
        assert!(
            unpack_evaluated_series(source, None).is_err(),
            "{label}: must be rejected"
        );
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "{label}: a rejected frame is never polled"
        );
    }

    // `expected` is a check, never an input: a schema without the frame's grid is a disagreement.
    let (source, _, _) = df_source(vec![], declared);
    assert!(
        unpack_evaluated_series(source, Some(schema())).is_err(),
        "a raw expectation cannot decode an evaluated frame"
    );
}

/// The two contents are not interchangeable: each unpack entry point refuses the other's frame.
#[tokio::test]
async fn each_unpack_entry_point_refuses_the_other_content() {
    let evaluated = packed(vec![(
        series(&[(0, 1)], vec![Some(1.0)], labels("j")),
        true,
    )])
    .await;
    let declared = evaluated[0].schema();
    let (source, _, _) = df_source(evaluated, declared);
    assert!(
        unpack_raw_series(source, None).is_err(),
        "a raw reader cannot decode grid-addressed rows"
    );

    let raw_frame = datatypes::series::frame::raw_series_frame_schema(
        &schema(),
        datatypes::series::frame::FrameLabelMode::Explicit,
    )
    .unwrap();
    let (source, _, _) = df_source(vec![], raw_frame);
    assert!(
        unpack_evaluated_series(source, None).is_err(),
        "an evaluated reader cannot decode timestamp-addressed rows"
    );
}

/// An evaluated row names its series by label, so a row that also carries a storage key is a
/// contradiction: the reader refuses it instead of decoding an identity it cannot check.
#[tokio::test]
async fn evaluated_frame_rejects_a_storage_key_on_a_row() {
    let batches = packed(vec![(
        series(&[(0, 2)], vec![Some(1.0), Some(2.0)], labels("l")),
        true,
    )])
    .await;
    let declared = batches[0].schema();
    let keyed = with_read_key(&batches[0], b"encoded-key");
    let (source, _, _) = df_source(vec![keyed], declared);
    let mut unpacked = unpack_evaluated_series(source, Some(evaluated_schema())).unwrap();
    let error = unpacked.next().await.unwrap().unwrap_err();
    assert_eq!(error.status_code(), StatusCode::InvalidArguments);
    assert!(unpacked.next().await.is_none());
}

/// An evaluated frame is self-describing across Arrow IPC: the schema, its metadata, and its rows
/// survive serialization, and a consumer decodes them with no planning-side copy of the grid.
#[tokio::test]
async fn evaluated_frame_roundtrips_through_arrow_ipc() {
    let metric = labels("k");
    let expected: Vec<Option<f64>> = vec![Some(1.5), None, Some(3.5)];
    let batches = packed(vec![(
        series(&[(0, 2), (4, 1)], expected.clone(), metric.clone()),
        true,
    )])
    .await;
    let declared = batches[0].schema();

    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &declared).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    let reader = StreamReader::try_new(Cursor::new(ipc), None).unwrap();
    let read_schema = reader.schema();
    assert_eq!(read_schema, declared);
    assert_eq!(
        datatypes::series::frame::SeriesFrameDeclaration::parse(read_schema.as_ref())
            .unwrap()
            .series()
            .eval_grid
            .as_ref()
            .expect("the frame still declares its grid")
            .spec(),
        evaluated_grid().spec()
    );

    let read: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    assert_eq!(read.len(), batches.len());
    let (source, _, _) = df_source(read, read_schema);
    // No `expected` schema is supplied: the frame alone says what its rows mean.
    let rows = drain(unpack_evaluated_series(source, None).unwrap()).await;
    let (decoded_positions, decoded_values) = flat(&rows);
    assert_eq!(decoded_positions, vec![0, 1, 4]);
    assert_eq!(decoded_values, expected);
    assert!(rows.iter().all(|(row, _)| row.metric == metric));
    assert!(rows.last().expect("at least one row").1);
}

/// A schema with two value fields addressed on [`evaluated_grid`], so a fixture can carry more than
/// the single float field [`series`] declares.
fn pair_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![
            FieldSpec {
                name: Arc::from("value"),
            },
            FieldSpec {
                name: Arc::from("score"),
            },
        ]
        .into(),
        eval_grid: Some(evaluated_grid()),
    })
}

/// One validated evaluated chunk: `fields` on the runs `spans` of the shared test grid, declared
/// under `schema`.
fn chunk_on(
    schema: &SeriesSchema,
    spans: &[(usize, usize)],
    fields: SeriesFields,
    metric: SeriesLabels,
) -> Series {
    let time = TimeAxis::from_grid_spans(
        evaluated_grid(),
        spans
            .iter()
            .map(|(start, len)| GridSpan::new(*start, *len))
            .collect(),
    )
    .unwrap();
    Series::try_new(schema, metric, time, fields).unwrap()
}

/// Packs the supplied chunks under `schema` and drains every ready batch, for a fixture whose field
/// list is not the single `value` field [`evaluated_schema`] declares.
async fn packed_under(schema: Arc<SeriesSchema>, items: Vec<(Series, bool)>) -> Vec<RecordBatch> {
    let mut stream = pack_evaluated_series(source(items), schema).unwrap();
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch.unwrap());
    }
    out
}

/// Unpacks a producer's batches as a consumer that planned the same series schema would, which is
/// how a fixture with more than the one `value` field is read back.
fn unpack_as(
    batches: Vec<RecordBatch>,
    declared: SchemaRef,
    expected: Arc<SeriesSchema>,
) -> SendableEvaluatedSeriesStream {
    let (source, _, _) = df_source(batches, declared);
    unpack_evaluated_series(source, Some(expected)).unwrap()
}

/// The float payload of field `index`, in compact-row order.
fn field_values(series: &Series, index: usize) -> Vec<Option<f64>> {
    series
        .fields
        .get(index)
        .expect("the fixture reads a field it declared")
        .iter()
        .map(|sample| match sample {
            SampleRef::Missing => None,
            SampleRef::Float(value) => Some(value),
            _ => panic!("the fixture carries float samples"),
        })
        .collect()
}

/// Compares two per-position float payloads, with `NaN` comparing equal to itself.
///
/// `Option<f64>` cannot be compared with `==` here: the fixtures carry `NaN` as a *value*, and
/// `NaN != NaN`, so equality would report a faithful round trip as a mismatch. Comparing bit
/// patterns keeps a value distinct from a missing position, and `None` still has to meet `None`.
fn assert_float_payload_eq(actual: &[Option<f64>], expected: &[Option<f64>], field: &str) {
    assert_eq!(actual.len(), expected.len(), "{field}: position count");
    for (position, (lhs, rhs)) in actual.iter().zip(expected).enumerate() {
        match (lhs, rhs) {
            (None, None) => {}
            (Some(lhs), Some(rhs)) => assert_eq!(
                lhs.to_bits(),
                rhs.to_bits(),
                "{field}: position {position} must keep its value"
            ),
            _ => panic!("{field}: position {position} is {lhs:?}, expected {rhs:?}"),
        }
    }
}

/// How many positions carry a `NaN` value and how many carry nothing, counted separately because
/// the two must not be confused on the way through the codec.
fn nan_and_null_counts(values: &[Option<f64>]) -> (usize, usize) {
    let nan = values
        .iter()
        .filter(|value| value.is_some_and(|value| value.is_nan()))
        .count();
    let null = values.iter().filter(|value| value.is_none()).count();
    (nan, null)
}

/// The native histogram table the histogram fixture references: two rows with distinct sums, the
/// first with a start timestamp and the second without.
///
/// The shape is the one `datatypes::series::frame::tests::histogram_batch_with_sum` builds, so this
/// test compares against the same payload the frame layer's own tests use instead of inventing a
/// second fixture.
fn histogram_batch() -> Arc<HistogramBatch> {
    let custom = [1.5, 4.5];
    let positive_offsets = [1];
    let positive_lengths = [2];
    let positive_buckets = [3.25, 7.5];
    let negative_offsets = [-2];
    let negative_lengths = [1];
    let negative_buckets = [0.75];
    let view = |sum, start_timestamp| HistogramView {
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
    };
    let mut builder = HistogramBatchBuilder::new();
    builder.push(view(-9.5, Some(-123))).unwrap();
    builder.push(view(90.5, None)).unwrap();
    builder.finish().unwrap()
}

/// The payload struct of one field of a frame batch: the float list and the histogram payload.
///
/// A field is one column of the frame's `fields` struct, so this is how a test can tell which
/// representation the codec put on the wire instead of only what came back out of it.
fn frame_field_payload(batch: &RecordBatch, field: usize) -> &StructArray {
    batch
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("a frame carries its fields as a struct")
        .column(field)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("a field payload is a struct")
}

/// A multi-field chunk keeps each field's missing positions independent: one position carries a
/// value in one field and nothing in the other, and a `NaN` value never collapses into a missing one.
///
/// This is the spec's `evaluated/multi-field` case. A schema snapshot cannot stand in for it, because
/// `NaN` and a missing value are both "no ordinary number" once the payload is only a list: which of
/// the two a position holds is visible only after a round trip.
#[tokio::test]
async fn multi_field_partial_nulls_are_not_nan() {
    // Position 0 carries only `value` and position 2 only `score`. `NaN` is a value in both fields,
    // and the position next to it in the other field is missing.
    let value: Vec<Option<f64>> = vec![Some(1.0), Some(f64::NAN), None, Some(f64::NAN), Some(4.0)];
    let score: Vec<Option<f64>> = vec![None, None, Some(3.0), Some(5.0), Some(f64::NAN)];
    let metric = labels("n");
    let schema = pair_schema();
    let fields = SeriesFields::try_from_vec(vec![
        Samples::from_float(Float64Array::from(value.clone())),
        Samples::from_float(Float64Array::from(score.clone())),
    ])
    .unwrap();
    let batches = packed_under(
        schema.clone(),
        vec![(
            chunk_on(&schema, &[(0, 2), (5, 3)], fields, metric.clone()),
            true,
        )],
    )
    .await;
    // One row per carried run, so the hole between the runs is not what the frame trades away.
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
    // Both fields are columns of every row, and both carry float values of their own.
    for row in 0..batches[0].num_rows() {
        for field in [0, 1] {
            let payload = frame_field_payload(&batches[0], field);
            assert!(
                !payload.column(0).is_null(row),
                "field {field} carries a float payload on row {row}"
            );
        }
    }

    let declared = batches[0].schema();
    let rows = drain(unpack_as(batches, declared, schema)).await;
    assert_eq!(rows.len(), 2, "one decoded chunk per frame row");
    assert!(
        rows.iter().all(|(row, _)| row.metric == metric),
        "both rows name the same series"
    );
    assert!(!rows[0].1, "only the last row closes the chain");
    assert!(rows[1].1);
    // Concatenated in stream order, exactly as the producer's single chunk was laid out.
    let decoded_positions: Vec<usize> = rows.iter().flat_map(|(row, _)| positions(row)).collect();
    assert_eq!(
        decoded_positions,
        vec![0, 1, 5, 6, 7],
        "the hole between the two runs is not filled"
    );
    let decoded_value: Vec<Option<f64>> = rows
        .iter()
        .flat_map(|(row, _)| field_values(row, 0))
        .collect();
    let decoded_score: Vec<Option<f64>> = rows
        .iter()
        .flat_map(|(row, _)| field_values(row, 1))
        .collect();
    // Per position, because each field decides for itself whether it holds a value.
    assert_float_payload_eq(&decoded_value, &value, "value");
    assert_float_payload_eq(&decoded_score, &score, "score");
    // `NaN` and null are counted apart: a codec that turned one into the other would keep the
    // position count and the alignment while losing the distinction this test is about.
    assert_eq!(
        nan_and_null_counts(&decoded_value),
        nan_and_null_counts(&value),
        "value: the NaN and null counts survive"
    );
    assert_eq!(
        nan_and_null_counts(&decoded_score),
        nan_and_null_counts(&score),
        "score: the NaN and null counts survive"
    );
    assert_eq!(
        nan_and_null_counts(&decoded_value),
        (2, 1),
        "two NaN values and one null, not three nulls"
    );
    assert_eq!(
        nan_and_null_counts(&decoded_score),
        (1, 2),
        "one NaN value and two nulls, not three values"
    );
    // The same position carries a value in one field and nothing in the other, in both directions.
    assert_eq!(decoded_value[0], Some(1.0));
    assert_eq!(decoded_score[0], None);
    assert_eq!(decoded_value[2], None);
    assert_eq!(decoded_score[2], Some(3.0));
    // And a NaN is a value: it keeps its own position, where the other field is missing.
    assert!(decoded_value[1].is_some_and(f64::is_nan));
    assert_eq!(decoded_score[1], None);
}

/// A histogram field keeps its native payload across the wire: the frame carries the table and the
/// per-position references to it, so what a consumer decodes is still a histogram.
///
/// This is the spec's `native histogram/mixed` case. A length check cannot stand in for it, because a
/// silent float rendition preserves the position count; what rules the conversion out is the payload
/// kind on both sides of the wire and the values behind the references.
#[tokio::test]
async fn histogram_payload_roundtrips_without_float_conversion() {
    let table = histogram_batch();
    let indices = UInt32Array::from(vec![Some(0), None, Some(1)]);
    let samples = Samples::try_from_histogram(indices.clone(), table).unwrap();
    let metric = labels("m");
    let batches = packed(vec![(
        chunk_on(
            &evaluated_schema(),
            &[(3, 3)],
            SeriesFields::one(samples),
            metric.clone(),
        ),
        true,
    )])
    .await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1, "one run is one frame row");

    // On the wire the field is a payload struct that carries the histogram and no float list, so the
    // codec never filled a float rendition in on the way out.
    let payload = frame_field_payload(&batches[0], 0);
    assert!(
        payload.column(0).is_null(0),
        "a histogram field has no float payload"
    );
    assert!(
        !payload.column(1).is_null(0),
        "the histogram payload is what the frame carries"
    );

    let declared = batches[0].schema();
    let rows = drain(unpack(batches, declared)).await;
    assert_eq!(rows.len(), 1);
    let (decoded, is_last) = &rows[0];
    assert!(is_last);
    assert_eq!(decoded.metric, metric);
    assert_eq!(positions(decoded), vec![3, 4, 5]);
    let field = decoded.fields.get(0).expect("the fixture has one field");
    let DenseSamplesRef::Histogram(histograms) = field.dense().expect("the field is dense") else {
        panic!("a histogram field must decode as a histogram, not as a float or mixed segment");
    };
    assert_eq!(histograms.len(), 3, "every position is still there");
    assert_eq!(
        histograms.batch().len(),
        2,
        "both input histograms are still referenced"
    );
    let decoded_indices = histograms.indices();
    assert_eq!(decoded_indices.len(), indices.len());
    for position in 0..indices.len() {
        assert_eq!(
            decoded_indices.is_valid(position),
            indices.is_valid(position),
            "position {position}: a null reference stays null"
        );
        if indices.is_valid(position) {
            assert_eq!(decoded_indices.value(position), indices.value(position));
        }
    }
    // The values behind the references are the input payload, not a lossy rendition of it.
    let first = histograms
        .view_at(0)
        .expect("position 0 references a histogram");
    assert_eq!(first.count.to_bits(), 17.25_f64.to_bits());
    assert_eq!(first.sum.to_bits(), (-9.5_f64).to_bits());
    assert_eq!(first.start_timestamp, Some(-123));
    assert_eq!(
        first
            .positive
            .buckets
            .iter()
            .map(|bucket| bucket.to_bits())
            .collect::<Vec<_>>(),
        [3.25_f64.to_bits(), 7.5_f64.to_bits()]
    );
    assert!(
        histograms.view_at(1).is_none(),
        "a null reference stays missing instead of resolving to a neighbour's payload"
    );
    let second = histograms
        .view_at(2)
        .expect("position 2 references a histogram");
    assert_eq!(second.count.to_bits(), 17.25_f64.to_bits());
    assert_eq!(second.sum.to_bits(), 90.5_f64.to_bits());
    assert_eq!(second.start_timestamp, None);
    // The per-position kind survives as well: a referenced position is a histogram sample, never a
    // float one.
    assert!(matches!(field.sample(0), SampleRef::Histogram(_)));
    assert!(matches!(field.sample(1), SampleRef::Missing));
    assert!(matches!(field.sample(2), SampleRef::Histogram(_)));
}

/// One field can hold floats and histograms at different positions, and the codec keeps them apart:
/// each position decodes as the kind it was written as.
///
/// This is the `native histogram/mixed` case in its harder form. A codec could keep the position
/// count and still promote one kind into the other in a single position, which only a per-position
/// kind comparison catches.
#[tokio::test]
async fn mixed_payload_roundtrips_without_dropping_either_kind() {
    let table = histogram_batch();
    // Position 0 is a float, 1 a histogram, 2 nothing, and 3 a NaN float - which must not be read as
    // a missing position, nor as the neighbouring histogram.
    let floats = Float64Array::from(vec![Some(1.5), None, None, Some(f64::NAN)]);
    let indices = UInt32Array::from(vec![None, Some(0), None, None]);
    let histograms = datatypes::series::samples::HistogramSamples::try_new(indices, table).unwrap();
    let samples = Samples::try_from_mixed(floats, histograms).unwrap();
    let metric = labels("o");
    let batches = packed(vec![(
        chunk_on(
            &evaluated_schema(),
            &[(6, 4)],
            SeriesFields::one(samples),
            metric.clone(),
        ),
        true,
    )])
    .await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    // A mixed field is the one payload shape that carries both halves, so the frame neither dropped
    // the float positions nor flattened the histogram ones.
    let payload = frame_field_payload(&batches[0], 0);
    assert!(
        !payload.column(0).is_null(0),
        "a mixed field carries its floats"
    );
    assert!(
        !payload.column(1).is_null(0),
        "a mixed field carries its histograms"
    );

    let declared = batches[0].schema();
    let rows = drain(unpack(batches, declared)).await;
    assert_eq!(rows.len(), 1);
    let (decoded, is_last) = &rows[0];
    assert!(is_last);
    assert_eq!(decoded.metric, metric);
    assert_eq!(positions(decoded), vec![6, 7, 8, 9]);
    let field = decoded.fields.get(0).expect("the fixture has one field");
    let DenseSamplesRef::Mixed(mixed) = field.dense().expect("the field is dense") else {
        panic!("a mixed field must decode as mixed, not as one of its two halves");
    };
    assert_eq!(mixed.floats().len(), 4);
    assert_eq!(mixed.histograms().len(), 4);
    assert_eq!(
        mixed.histograms().batch().len(),
        2,
        "the referenced table survives whole"
    );
    match field.sample(0) {
        SampleRef::Float(value) => assert_eq!(value.to_bits(), 1.5_f64.to_bits()),
        _ => panic!("position 0 was a float"),
    }
    match field.sample(1) {
        SampleRef::Histogram(histogram) => {
            assert_eq!(histogram.count.to_bits(), 17.25_f64.to_bits());
            assert_eq!(histogram.sum.to_bits(), (-9.5_f64).to_bits());
        }
        _ => panic!("position 1 was a histogram"),
    }
    assert!(matches!(field.sample(2), SampleRef::Missing));
    match field.sample(3) {
        // A NaN float stays a float value, and its histogram slot stays empty.
        SampleRef::Float(value) => assert!(value.is_nan()),
        _ => panic!("position 3 was a NaN float"),
    }
}

/// A frame whose spans add up beyond `usize` is refused, not answered with a wrapped count.
///
/// The count of a row's positions is the first arithmetic a decoded row does, and it happens before
/// the row is checked against the grid it claims: two spans of `usize::MAX / 2 + 1` each are neither
/// ordered nor inside any grid, but summing them is what a decoder would do first. The sum is
/// therefore checked, so the frame is refused with an error instead of panicking in a debug build or
/// wrapping in a release one.
#[tokio::test]
async fn spans_that_overflow_the_position_count_are_refused() {
    let batches = packed(vec![(
        series(&[(0, 2)], vec![Some(1.0), Some(2.0)], labels("v")),
        true,
    )])
    .await;
    let declared = batches[0].schema();
    let huge = u64::try_from(usize::MAX / 2 + 1).expect("a stride that leaves usize behind");
    // Two spans, because one is already representable: only their sum leaves `usize`, and that sum
    // is what a decoder must check before it can report which span was wrong.
    let hostile = with_spans(&batches[0], &[(0, huge), (huge, huge)], true);
    let mut stream = unpack(vec![hostile], declared);
    let error = stream
        .next()
        .await
        .expect("a refused frame still yields its error")
        .expect_err("the position count overflows");
    assert!(
        error.to_string().contains("grid span length overflow"),
        "refused for another reason: {error}"
    );
}

/// An empty row that closes nothing is refused by the decoder itself, not only by the stream.
///
/// A row covering no position is the closing chunk's shape: it exists to carry `is_last`. One that
/// carries `is_last = false` opens a chain it can never advance, so a consumer would wait for a
/// close that no later row owes. The stream validator refuses this, and so does the public decoder,
/// because a caller driving the decoder directly must not be handed a chunk no producer could have
/// written.
#[tokio::test]
async fn an_empty_row_that_closes_nothing_is_refused_by_the_decoder() {
    // A legal empty closing chunk is packed first: the illegal shape cannot be produced, which is
    // the point of the check below. Making it illegal means clearing its finality and nothing else,
    // so the emptiness and the payload lengths stay consistent and only finality can refuse it.
    let batches = packed(vec![(series(&[], vec![], labels("w")), true)]).await;
    let declared = batches[0].schema();
    assert_eq!(1, batches[0].num_rows(), "the closing chunk is one row");
    let emptied = with_spans(&batches[0], &[], false);
    let mut stream = unpack(vec![emptied], declared);
    let error = stream
        .next()
        .await
        .expect("a refused frame still yields its error")
        .expect_err("an empty row that closes nothing is refused");
    assert!(
        error
            .to_string()
            .contains("an evaluated chunk without positions must close its series"),
        "refused for another reason: {error}"
    );
}

/// Rewrites the span list of every row in `batch` to `spans`, and its finality to `is_last`.
///
/// Both are rewritten together because a test that breaks a row's shape usually breaks its finality
/// as well: a producer cannot write an empty row that closes nothing, so such a row only exists once
/// a test builds it.
fn with_spans(batch: &RecordBatch, spans: &[(u64, u64)], is_last: bool) -> RecordBatch {
    let declared = batch.schema();
    let DataType::Struct(time_members) = declared.field(2).data_type() else {
        panic!("time is a struct");
    };
    let DataType::List(span_item) = time_members[0].data_type() else {
        panic!("spans are a list");
    };
    let DataType::Struct(span_fields) = span_item.data_type() else {
        panic!("a span is a struct");
    };
    let starts: Vec<u64> = spans.iter().map(|(start, _)| *start).collect();
    let lengths: Vec<u64> = spans.iter().map(|(_, len)| *len).collect();
    let values = StructArray::try_new(
        span_fields.clone(),
        vec![
            Arc::new(UInt64Array::from(starts)),
            Arc::new(UInt64Array::from(lengths)),
        ],
        None,
    )
    .unwrap();
    let lengths_per_row = vec![spans.len(); batch.num_rows()];
    let list = ListArray::try_new(
        span_item.clone(),
        OffsetBuffer::new(offset_buffer(&lengths_per_row).into()),
        Arc::new(values),
        None,
    )
    .unwrap();
    let time = StructArray::try_new(time_members.clone(), vec![Arc::new(list)], None).unwrap();
    let mut columns = batch.columns().to_vec();
    columns[1] = Arc::new(BooleanArray::from(vec![is_last; batch.num_rows()]));
    columns[2] = Arc::new(time);
    RecordBatch::try_new_with_options(
        declared,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .unwrap()
}

/// The cumulative list offsets of the given row lengths.
fn offset_buffer(lengths: &[usize]) -> Vec<i32> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    let mut total = 0i32;
    offsets.push(0);
    for length in lengths {
        total += i32::try_from(*length).expect("a test row fits an Arrow offset");
        offsets.push(total);
    }
    offsets
}

/// A cursor holding positions on one grid cannot fill a builder that declares another.
///
/// Both grids describe the same number of points, so every position the cursor writes is in range
/// for the builder and the frame would decode cleanly. What it would decode to is a different set of
/// instants, which no later check could detect: the numbers are the only thing on the wire. The
/// builder therefore compares the grids themselves, not whether a grid is present.
#[test]
fn a_cursor_on_one_grid_cannot_fill_a_builder_declaring_another() {
    // Both grids have four points, so the cursor's positions fit the builder exactly.
    let other_grid = || {
        Arc::new(
            datatypes::series::time::EvalGrid::try_new(
                datatypes::series::time::GridSpec::try_new(60_000, 10_000, 4)
                    .expect("a valid grid"),
            )
            .expect("a valid grid"),
        )
    };
    let other_schema = SeriesSchema {
        fields: evaluated_schema().fields.clone(),
        eval_grid: Some(other_grid()),
    };
    // A chunk that is legal on its own grid: the series and the schema agree, so nothing but the
    // builder's own declaration can refuse it.
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(1.0)])));
    let time = TimeAxis::from_grid_spans(other_grid(), vec![GridSpan::new(0, 1)])
        .expect("one span on its own grid");
    let chunk = Series::try_new(&other_schema, labels("g"), time, fields).expect("a legal chunk");
    let mut cursor =
        SeriesFrameCursor::try_new_evaluated(&other_schema, chunk, true).expect("a legal cursor");

    let mut builder = SeriesFrameBuilder::try_new(
        &evaluated_schema(),
        FrameLabelMode::Explicit,
        None,
        SeriesFrameBuilder::DEFAULT_MAX_ROWS,
        SeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )
    .expect("the schema is a frame");
    let error = cursor
        .append_to(&mut builder)
        .expect_err("the grids differ");
    assert!(
        error.to_string().contains("schema mismatch"),
        "refused for another reason: {error}"
    );
}
