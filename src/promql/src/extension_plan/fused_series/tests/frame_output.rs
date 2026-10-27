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

use super::*;
use crate::map::MathOp;
use crate::map_label::LabelRewrite;
use crate::native::plan::{BoundMap, InstantValue, MapOp};

fn instant_tree(schema: Arc<SeriesSchema>, grid: Arc<EvalGrid>) -> NativeTree {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: schema.fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    }));
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: 0,
                    output: NativeType::Samples(Arc::clone(&schema)),
                    ordering: SeriesOrdering::Contiguous,
                }),
                output: NativeType::Samples(Arc::clone(&schema)),
            }),
            op: BoundInstant {
                lookback_ms: 0,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: grid,
            },
        },
        output,
    })
    .expect("instant selector tree is valid")
}

async fn frame_decode(
    exec: Arc<dyn ExecutionPlan>,
) -> Vec<(datatypes::series::samples::Series, bool)> {
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("frame output executes");
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        exec.schema(),
        futures::stream::iter(batches.into_iter().map(Ok)),
    ));
    unpack_evaluated_series(stream, None)
        .expect("evaluated frame schema decodes")
        .try_collect::<Vec<_>>()
        .await
        .expect("evaluated frame decodes")
}

fn compact_float_rows(
    decoded: &[(datatypes::series::samples::Series, bool)],
) -> Vec<(i64, Vec<Option<u64>>)> {
    let mut rows = Vec::new();
    for (series, _) in decoded {
        let timestamps = series.time.timestamps().expect("grid timestamps");
        for (row, timestamp) in timestamps.iter().enumerate() {
            let values = series
                .fields
                .as_slice()
                .iter()
                .map(
                    |field| match field.iter().nth(row).expect("sample at timestamp") {
                        datatypes::series::samples::SampleRef::Float(value) => {
                            Some(value.to_bits())
                        }
                        datatypes::series::samples::SampleRef::Missing => None,
                        _ => panic!("float or null sample expected"),
                    },
                )
                .collect();
            rows.push((*timestamp, values));
        }
    }
    rows.sort_by_key(|(timestamp, _)| *timestamp);
    rows
}

fn two_field_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new(OTHER, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

fn plain_float_rows(batches: &[RecordBatch], fields: &[&str]) -> Vec<(i64, Vec<Option<u64>>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let time = batch
            .column_by_name(TIME)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let columns = fields
            .iter()
            .map(|field| {
                batch
                    .column_by_name(field)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            rows.push((
                time.value(row),
                columns
                    .iter()
                    .map(|column| column.is_valid(row).then(|| column.value(row).to_bits()))
                    .collect(),
            ));
        }
    }
    rows.sort_by_key(|(timestamp, _)| *timestamp);
    rows
}

#[tokio::test]
async fn sparse_instant_frame_preserves_positions_nulls_and_nan_across_formats() {
    let schema = two_field_schema();
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 6).unwrap()).unwrap());
    let tree = instant_tree(Arc::clone(&schema), grid);
    let chunk = chunk_of(
        &schema,
        "sparse",
        metric(&[(HOST, "sparse")], Some(METRIC)),
        &[0, 2_000, 5_000],
        vec![
            vec![Some(1.0), None, Some(f64::from_bits(0x7ff8_0000_0000_0042))],
            vec![None, Some(2.0), None],
        ],
        true,
    );
    let batches = vec![frame_of(&schema, &chunk)];
    let binding = binding_of(&tree, Arc::clone(&schema), vec![HOST.into(), NAME.into()]);
    let make_node = |format| {
        FusedSeries::try_new_with_format(
            Arc::from("instant(x)"),
            tree.clone(),
            binding.clone(),
            vec![input_plan(frame_schema_of(&schema))],
            two_field_output_schema().to_dfschema_ref().unwrap(),
            format,
        )
        .expect("instant tree has the declared frame output")
    };

    let frame_exec = plan_of(
        make_node(FusedOutputFormat::SeriesFrame),
        memory_exec_with(frame_schema_of(&schema), batches.clone()),
    )
    .unwrap();
    let decoded = frame_decode(frame_exec).await;
    assert_eq!(1, decoded.iter().filter(|(_, last)| *last).count());
    let expected = vec![
        (0, vec![Some(1.0f64.to_bits()), None]),
        (2_000, vec![None, Some(2.0f64.to_bits())]),
        (5_000, vec![Some(0x7ff8_0000_0000_0042), None]),
    ];
    assert_eq!(expected, compact_float_rows(&decoded));

    let plain_exec = plan_of(
        make_node(FusedOutputFormat::PlainRows),
        memory_exec_with(frame_schema_of(&schema), batches),
    )
    .unwrap();
    let plain_batches = collect(plain_exec, SessionContext::default().task_ctx())
        .await
        .unwrap();
    assert_eq!(expected, plain_float_rows(&plain_batches, &[VALUE, OTHER]));
    assert!(decoded.iter().all(|(series, _)| {
        series.metric.get(HOST) == Some("sparse") && series.metric.name() == Some(METRIC)
    }));
}

#[tokio::test]
async fn abs_rate_frame_matches_plain_rows_with_exact_values_and_labels() {
    let rate = rate_tree();
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(rate.root().clone()),
            op: BoundMap {
                ops: Arc::from([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: rate.root().output.clone(),
    })
    .unwrap();
    let batches = two_series_frames();
    let binding = binding(&tree);
    let make_node = |format| {
        FusedSeries::try_new_with_format(
            Arc::from("abs(rate(x[5m]))"),
            tree.clone(),
            binding.clone(),
            vec![input_plan(frame_schema())],
            output_schema().to_dfschema_ref().unwrap(),
            format,
        )
        .unwrap()
    };
    let plain_exec = plan_of(
        make_node(FusedOutputFormat::PlainRows),
        memory_exec(batches.clone()),
    )
    .unwrap();
    let plain_batches = collect(plain_exec, SessionContext::default().task_ctx())
        .await
        .unwrap();
    let mut expected = rows(&plain_batches)
        .into_iter()
        .map(|(time, value, host, name)| (time, value.map(f64::to_bits), host, name))
        .collect::<Vec<_>>();
    expected.sort();
    let mut exact = Vec::new();
    for (host, scale) in [("web-1", 1.0_f64), ("web-2", 2.0)] {
        for position in 0..GRID_LEN {
            exact.push((
                GRID_START + position as i64 * GRID_STEP,
                Some(scale.to_bits()),
                Some(host.to_string()),
                None,
            ));
        }
    }
    exact.sort();
    assert_eq!(exact, expected);

    let frame_exec = plan_of(
        make_node(FusedOutputFormat::SeriesFrame),
        memory_exec(batches),
    )
    .unwrap();
    let decoded = frame_decode(frame_exec).await;
    assert_eq!(2, decoded.iter().filter(|(_, last)| *last).count());
    let mut actual = Vec::new();
    for (series, _) in &decoded {
        assert_eq!(None, series.metric.name());
        let host = series.metric.get(HOST).unwrap().to_string();
        for (time, values) in compact_float_rows(&[(series.clone(), true)]) {
            actual.push((time, values[0], Some(host.clone()), None));
        }
    }
    actual.sort();
    assert_eq!(exact, actual);
}

#[tokio::test]
async fn rewritten_identical_labels_keep_disjoint_chains_in_one_encoder() {
    let schema = raw_schema();
    let semantic_grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 6).unwrap()).unwrap());
    let base = instant_tree(Arc::clone(&schema), semantic_grid);
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(base.root().clone()),
            op: BoundMap {
                ops: Arc::from([
                    MapOp::Label(Arc::new(
                        LabelRewrite::replace("host", "shared", "host", ".*").unwrap(),
                    )),
                    MapOp::Label(Arc::new(
                        LabelRewrite::replace("zone", "edge", "host", ".*").unwrap(),
                    )),
                ]),
            },
        },
        output: base.root().output.clone(),
    })
    .unwrap();
    let chunks = vec![
        chunk_of(
            &schema,
            "a",
            metric(&[(HOST, "a")], Some(METRIC)),
            &[0, 2_000],
            vec![vec![Some(10.0), Some(12.0)]],
            true,
        ),
        chunk_of(
            &schema,
            "b",
            metric(&[(HOST, "b")], Some(METRIC)),
            &[1_000, 5_000],
            vec![vec![Some(21.0), Some(25.0)]],
            true,
        ),
    ];
    let batches = frames_of(&schema, &chunks);
    let binding = binding_of(&tree, Arc::clone(&schema), vec![HOST.into(), NAME.into()]);
    let make_node = |format| {
        FusedSeries::try_new_with_format(
            Arc::from("rewritten instant"),
            tree.clone(),
            binding.clone(),
            vec![input_plan(frame_schema())],
            output_schema().to_dfschema_ref().unwrap(),
            format,
        )
        .unwrap()
    };

    let exec = plan_of(
        make_node(FusedOutputFormat::SeriesFrame),
        memory_exec(batches.clone()),
    )
    .unwrap();
    let decoded = frame_decode(exec).await;
    assert_eq!(
        2,
        decoded.iter().filter(|(_, last)| *last).count(),
        "each chain closes once"
    );
    let mut groups = Vec::new();
    let mut group_rows = Vec::new();
    let mut closes = 0;
    for (series, last) in &decoded {
        assert_eq!(Some("shared"), series.metric.get(HOST));
        assert_eq!(Some("edge"), series.metric.get("zone"));
        assert_eq!(Some(METRIC), series.metric.name());
        let timestamps = series.time.timestamps().unwrap();
        let values = series.fields.get(0).unwrap();
        for (i, time) in timestamps.iter().enumerate() {
            let value = match values.iter().nth(i).unwrap() {
                datatypes::series::samples::SampleRef::Float(value) => value.to_bits(),
                _ => panic!("float sample expected"),
            };
            group_rows.push((*time, value));
        }
        if *last {
            closes += 1;
            group_rows.sort_by_key(|(time, _)| *time);
            groups.push(std::mem::take(&mut group_rows));
        }
    }
    assert_eq!(2, closes, "each chain closes exactly once");
    assert!(group_rows.is_empty(), "all output chains close");
    groups.sort();
    assert_eq!(
        vec![
            vec![(0, 10.0f64.to_bits()), (2_000, 12.0f64.to_bits())],
            vec![(1_000, 21.0f64.to_bits()), (5_000, 25.0f64.to_bits())],
        ],
        groups
    );

    let plain_exec = plan_of(
        make_node(FusedOutputFormat::PlainRows),
        memory_exec_with(frame_schema(), batches),
    )
    .unwrap();
    let plain = collect(plain_exec, SessionContext::default().task_ctx())
        .await
        .unwrap();
    let mut plain = rows(&plain)
        .into_iter()
        .map(|(time, value, host, name)| (time, value.map(f64::to_bits), host, name))
        .collect::<Vec<_>>();
    plain.sort();
    assert_eq!(
        vec![
            (
                0,
                Some(10.0f64.to_bits()),
                Some("shared".into()),
                Some(METRIC.into())
            ),
            (
                1_000,
                Some(21.0f64.to_bits()),
                Some("shared".into()),
                Some(METRIC.into())
            ),
            (
                2_000,
                Some(12.0f64.to_bits()),
                Some("shared".into()),
                Some(METRIC.into())
            ),
            (
                5_000,
                Some(25.0f64.to_bits()),
                Some("shared".into()),
                Some(METRIC.into())
            ),
        ],
        plain
    );
}
