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

use std::io::Cursor;
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_recordbatch::DfSendableRecordBatchStream;
use datatypes::arrow::compute::concat_batches;
use datatypes::arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::series::aggregate_state::{
    AggregateStateKind, AggregateStateSchema, AggregateStateValues, StateComponent, StateSeries,
};
use datatypes::series::frame::{FrameLabelMode, StateFrameRow};
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{GridSpan, TimeAxis};
use datatypes::series::{Label, SeriesLabels};
use futures_util::StreamExt;

use super::super::{
    SendableStateSeriesStream, pack_aggregate_state, pack_evaluated_series, unpack_aggregate_state,
    unpack_evaluated_series,
};
use super::{df_source, evaluated_grid, evaluated_schema};

fn group() -> SeriesLabels {
    SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from("a"),
        }],
    )
    .unwrap()
}

fn state_schema(kind: AggregateStateKind) -> Arc<AggregateStateSchema> {
    let components = match kind {
        AggregateStateKind::Group => vec![],
        AggregateStateKind::Sum => vec![StateComponent::Value, StateComponent::Compensation],
        AggregateStateKind::Count => vec![StateComponent::Count],
        AggregateStateKind::Avg => vec![
            StateComponent::Value,
            StateComponent::Mean,
            StateComponent::Compensation,
            StateComponent::Count,
            StateComponent::IncrementalMean,
        ],
        AggregateStateKind::Min | AggregateStateKind::Max => vec![StateComponent::Value],
        AggregateStateKind::Stddev | AggregateStateKind::Stdvar => vec![
            StateComponent::Value,
            StateComponent::Mean,
            StateComponent::Count,
        ],
    };
    Arc::new(AggregateStateSchema::try_new(kind, components).unwrap())
}

fn series_schema(fields: &[&str]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(*name),
            })
            .collect(),
        eval_grid: Some(evaluated_grid()),
    })
}

fn wrong_grid_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("a"),
        }]
        .into(),
        eval_grid: Some(Arc::new(
            datatypes::series::time::EvalGrid::try_new(
                datatypes::series::time::GridSpec::try_new(1, 1000, 512).unwrap(),
            )
            .unwrap(),
        )),
    })
}

fn input(fields: Vec<Vec<Option<AggregateStateValues>>>) -> (StateSeries, bool) {
    let time = TimeAxis::from_grid_spans(evaluated_grid(), vec![GridSpan::new(1, 3)]).unwrap();
    (
        StateSeries {
            metric: group(),
            time,
            states: fields,
        },
        true,
    )
}

fn source(items: Vec<(StateSeries, bool)>) -> SendableStateSeriesStream {
    Box::pin(futures_util::stream::iter(
        items.into_iter().map(Ok::<_, BoxedError>),
    ))
}

async fn packed(
    rows: Vec<(StateSeries, bool)>,
    state: Arc<AggregateStateSchema>,
    schema: Arc<SeriesSchema>,
) -> Vec<RecordBatch> {
    let mut stream = pack_aggregate_state(source(rows), state, schema).unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    batches
}

fn unpack_source(
    batches: Vec<RecordBatch>,
    schema: Arc<datatypes::arrow::datatypes::Schema>,
) -> DfSendableRecordBatchStream {
    df_source(batches, schema).0
}

async fn rows(
    batches: Vec<RecordBatch>,
    state: Option<Arc<AggregateStateSchema>>,
    schema: Option<Arc<SeriesSchema>>,
) -> Vec<StateFrameRow> {
    let frame = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| {
            datatypes::series::frame::aggregate_state_frame_schema(
                state.as_ref().unwrap(),
                schema.as_ref().unwrap(),
                FrameLabelMode::Explicit,
            )
            .unwrap()
        });
    let mut stream = unpack_aggregate_state(unpack_source(batches, frame), state, schema).unwrap();
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.unwrap());
    }
    rows
}

fn values(value: f64) -> Option<AggregateStateValues> {
    Some(AggregateStateValues {
        value,
        compensation: value / 10.0,
        ..Default::default()
    })
}

fn assert_state_row(row: &StateFrameRow, expected: &[f64], last: bool) {
    assert_eq!(row.metric(), &group());
    assert!(row.is_last() == last);
    assert_eq!(
        row.time()
            .positions()
            .map(|(position, _)| position)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let fields = row.fields().unwrap();
    for (position, value) in expected.iter().enumerate() {
        assert!(fields.is_present(0, position).unwrap());
        assert_eq!(
            fields
                .field_values(0, StateComponent::Value)
                .unwrap()
                .unwrap()
                .value(position),
            *value
        );
    }
}

fn assert_group_row(row: &StateFrameRow, last: bool) {
    assert_eq!(row.metric(), &group());
    assert_eq!(row.is_last(), last);
    assert_eq!(
        row.time()
            .positions()
            .map(|(position, _)| position)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let fields = row.fields().unwrap();
    for field in 0..fields.len() {
        for position in 0..3 {
            assert!(fields.is_present(field, position).unwrap());
        }
    }
}

#[tokio::test]
async fn aggregate_state_roundtrip_preserves_components_and_group_rows() {
    let sum = state_schema(AggregateStateKind::Sum);
    let schema = series_schema(&["a"]);
    let input_rows = vec![input(vec![vec![values(1.0), values(2.0), values(3.0)]])];
    let batches = packed(input_rows, sum.clone(), schema.clone()).await;
    let decoded = rows(batches, Some(sum), Some(schema)).await;
    assert_eq!(decoded.len(), 1);
    assert_state_row(&decoded[0], &[1.0, 2.0, 3.0], true);

    let group_state = state_schema(AggregateStateKind::Group);
    let group_schema = series_schema(&["a"]);
    let group_batches = packed(
        vec![input(vec![vec![values(0.0), values(0.0), values(0.0)]])],
        group_state.clone(),
        group_schema.clone(),
    )
    .await;
    let group_rows = rows(group_batches, Some(group_state), Some(group_schema)).await;
    assert_eq!(group_rows.len(), 1);
    assert_group_row(&group_rows[0], true);
}

#[tokio::test]
async fn group_presence_is_per_field() {
    let state = state_schema(AggregateStateKind::Group);
    let schema = series_schema(&["a", "b"]);
    let batches = packed(
        vec![input(vec![
            vec![values(0.0), None, values(0.0)],
            vec![None, values(0.0), None],
        ])],
        state.clone(),
        schema.clone(),
    )
    .await;
    let decoded = rows(batches, Some(state), Some(schema)).await;
    let fields = decoded[0].fields().unwrap();
    assert_eq!(
        (0..3)
            .map(|i| fields.is_present(0, i).unwrap())
            .collect::<Vec<_>>(),
        vec![true, false, true]
    );
    assert_eq!(
        (0..3)
            .map(|i| fields.is_present(1, i).unwrap())
            .collect::<Vec<_>>(),
        vec![false, true, false]
    );
}

#[tokio::test]
async fn aggregate_state_ipc_concat_roundtrip() {
    let state = state_schema(AggregateStateKind::Sum);
    let schema = series_schema(&["a"]);
    let batches = packed(
        vec![input(vec![vec![values(1.0), values(2.0), values(3.0)]])],
        state.clone(),
        schema.clone(),
    )
    .await;
    let expected = rows(batches.clone(), Some(state.clone()), Some(schema.clone())).await;
    let mut bytes = Cursor::new(Vec::new());
    {
        let mut writer = StreamWriter::try_new(&mut bytes, batches[0].schema().as_ref()).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    bytes.set_position(0);
    let reader = StreamReader::try_new(bytes, None).unwrap();
    let read = reader.map(Result::unwrap).collect::<Vec<_>>();
    let concatenated = concat_batches(&batches[0].schema(), &read).unwrap();
    let decoded = rows(vec![concatenated], Some(state), Some(schema)).await;
    assert_eq!(decoded.len(), expected.len());
    assert_state_row(&decoded[0], &[1.0, 2.0, 3.0], true);
}

#[tokio::test]
async fn aggregate_state_refusals_name_mismatched_parts() {
    let state = state_schema(AggregateStateKind::Sum);
    let schema = series_schema(&["a"]);
    let batches = packed(
        vec![input(vec![vec![values(1.0), values(2.0), values(3.0)]])],
        state.clone(),
        schema.clone(),
    )
    .await;
    for (expected, expected_schema, part) in [
        (
            Some(state_schema(AggregateStateKind::Count)),
            Some(schema.clone()),
            "kind",
        ),
        (
            Some(Arc::new(AggregateStateSchema {
                kind: AggregateStateKind::Sum,
                components: vec![StateComponent::Compensation, StateComponent::Value].into(),
            })),
            Some(schema.clone()),
            "components",
        ),
        (Some(state.clone()), Some(wrong_grid_schema()), "grid"),
        (
            Some(state.clone()),
            Some(series_schema(&["a", "b"])),
            "fields",
        ),
    ] {
        let frame = batches[0].schema();
        let (source, _, _) = df_source(batches.clone(), frame);
        let error = unpack_aggregate_state(source, expected, expected_schema)
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.to_lowercase().contains(part),
            "{error} did not name {part}"
        );
    }
}

#[tokio::test]
async fn aggregate_state_rejects_evaluated_and_evaluated_rejects_state() {
    let state = state_schema(AggregateStateKind::Sum);
    let schema = series_schema(&["a"]);
    let batches = packed(
        vec![input(vec![vec![values(1.0), values(2.0), values(3.0)]])],
        state,
        schema,
    )
    .await;
    let eval =
        pack_evaluated_series(Box::pin(futures_util::stream::empty()), evaluated_schema()).unwrap();
    let frame = eval.schema();
    let (source, _, _) = df_source(vec![], frame.clone());
    assert!(
        unpack_aggregate_state(source, None, None)
            .err()
            .unwrap()
            .to_string()
            .contains("evaluated")
    );
    let (source, _, _) = df_source(batches.clone(), batches[0].schema());
    assert!(
        unpack_evaluated_series(source, None)
            .err()
            .unwrap()
            .to_string()
            .contains("aggregate_state")
    );
    let eval =
        pack_evaluated_series(Box::pin(futures_util::stream::empty()), evaluated_schema()).unwrap();
    let frame = eval.schema();
    let (source, _, _) = df_source(vec![], frame);
    assert!(
        unpack_aggregate_state(source, None, None)
            .err()
            .unwrap()
            .to_string()
            .contains("evaluated")
    );
}

#[tokio::test]
async fn aggregate_state_empty_stream_terminates() {
    let state = state_schema(AggregateStateKind::Sum);
    let schema = series_schema(&["a"]);
    let batches = packed(vec![], state.clone(), schema.clone()).await;
    assert!(batches.is_empty());
    let frame = datatypes::series::frame::aggregate_state_frame_schema(
        &state,
        &schema,
        datatypes::series::frame::FrameLabelMode::Explicit,
    )
    .unwrap();
    let (source, _, _) = df_source(vec![], frame);
    let mut decoded = unpack_aggregate_state(source, Some(state), Some(schema)).unwrap();
    assert!(decoded.next().await.is_none());
}
