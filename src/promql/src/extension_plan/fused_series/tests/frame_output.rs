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
use common_query::series_frame::pack_evaluated_series;
use datatypes::series::time::GridSpan;

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
    let mut decoded_all = Vec::new();
    for partition in 0..exec.properties().output_partitioning().partition_count() {
        let batches = exec
            .execute(partition, SessionContext::default().task_ctx())
            .expect("frame output partition executes")
            .try_collect::<Vec<_>>()
            .await
            .expect("frame output executes");
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            exec.schema(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        let decoded = unpack_evaluated_series(stream, None)
            .expect("evaluated frame schema decodes")
            .try_collect::<Vec<_>>()
            .await
            .expect("evaluated frame decodes");
        assert_strict_series_order(&decoded);
        decoded_all.extend(decoded);
    }
    decoded_all
}

fn assert_strict_series_order(decoded: &[(datatypes::series::samples::Series, bool)]) {
    let mut current: Option<(datatypes::series::SeriesLabels, i64)> = None;
    let mut closed = Vec::new();
    for (series, last) in decoded {
        let labels = series.metric.clone();
        if current
            .as_ref()
            .is_none_or(|(identity, _)| identity != &labels)
        {
            if let Some((identity, _)) = current.take() {
                assert!(
                    closed.contains(&identity),
                    "series identity changed before finality"
                );
            }
            assert!(!closed.contains(&labels), "closed identity reopened");
            current = Some((labels.clone(), i64::MIN));
        }
        let (identity, previous) = current.as_mut().unwrap();
        assert_eq!(identity, &labels);
        let times = series.time.timestamps().expect("grid timestamps");
        for timestamp in times.iter() {
            assert!(
                *timestamp > *previous,
                "positions are strictly increasing per identity"
            );
            *previous = *timestamp;
        }
        if *last {
            assert!(
                !closed.contains(&labels),
                "identity finalized more than once"
            );
            closed.push(labels);
            current = None;
        }
    }
    assert!(current.is_none(), "EOF leaves no open series identity");
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
async fn evaluated_input_frame_composes_through_native_map() {
    let raw = raw_schema();
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 6).unwrap()).unwrap());
    let evaluated_schema = Arc::new(SeriesSchema {
        fields: raw.fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    });
    let evaluated_leaf = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: NativeType::Samples(Arc::clone(&evaluated_schema)),
            ordering: SeriesOrdering::Contiguous,
        }),
        output: NativeType::Samples(Arc::clone(&evaluated_schema)),
    };
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(evaluated_leaf),
            op: BoundMap {
                ops: Arc::from([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: NativeType::Samples(Arc::clone(&evaluated_schema)),
    })
    .unwrap();
    let source_schema = Arc::clone(&evaluated_schema);
    let make_chunk = |host: &str, span: usize, last: bool, value: f64| {
        let series = datatypes::series::samples::Series::try_new(
            &source_schema,
            datatypes::series::SeriesLabels::try_new(
                Some(METRIC.into()),
                vec![datatypes::series::Label {
                    name: HOST.into(),
                    value: host.into(),
                }],
            )
            .unwrap(),
            datatypes::series::time::TimeAxis::from_grid_spans(
                Arc::clone(&grid),
                vec![GridSpan::new(span, 1)],
            )
            .unwrap(),
            datatypes::series::samples::SeriesFields::one(
                datatypes::series::samples::Samples::from_float(Float64Array::from(vec![value])),
            ),
        )
        .unwrap();
        (series, last)
    };
    let left_stream = pack_evaluated_series(
        Box::pin(futures::stream::iter(
            vec![
                make_chunk("web-1", 0, false, 1.0),
                make_chunk("web-1", 1, true, 3.0),
            ]
            .into_iter()
            .map(Ok),
        )),
        Arc::clone(&source_schema),
    )
    .unwrap();
    let frame_schema = left_stream.schema();
    let left_frames = left_stream.try_collect::<Vec<_>>().await.unwrap();
    let right_stream = pack_evaluated_series(
        Box::pin(futures::stream::iter(
            vec![
                make_chunk("web-2", 0, false, 2.0),
                make_chunk("web-2", 1, true, 4.0),
            ]
            .into_iter()
            .map(Ok),
        )),
        Arc::clone(&source_schema),
    )
    .unwrap();
    let right_frames = right_stream.try_collect::<Vec<_>>().await.unwrap();
    let source = memory_exec_partitioned_with(frame_schema, vec![left_frames, right_frames]);
    let planner_schema = source.schema();
    let outer = FusedSeries::try_new_with_format(
        Arc::from("abs(evaluated)"),
        tree.clone(),
        binding_of(&tree, evaluated_schema, vec![HOST.into(), NAME.into()]),
        vec![input_plan(planner_schema)],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let output = plan_of(outer, source).unwrap();
    assert_eq!(2, output.properties().partitioning.partition_count());
    let decoded = frame_decode(output).await;
    let identities = decoded
        .iter()
        .map(|(series, _)| series.metric.get(HOST).unwrap().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        std::collections::BTreeSet::from(["web-1".to_string(), "web-2".to_string()]),
        identities,
        "the output has the two expected identities"
    );
    for host in ["web-1", "web-2"] {
        let fragments = decoded
            .iter()
            .filter(|(series, _)| series.metric.get(HOST) == Some(host))
            .collect::<Vec<_>>();
        let expected_labels = metric(&[(HOST, host)], None);
        assert!(
            fragments
                .iter()
                .all(|(series, _)| series.metric == expected_labels),
            "host {host}: expected labels {expected_labels:?}, actual labels: {:?}",
            fragments
                .iter()
                .map(|(series, _)| &series.metric)
                .collect::<Vec<_>>()
        );
        assert_eq!(1, fragments.iter().filter(|(_, last)| *last).count());
        assert!(
            fragments.last().unwrap().1,
            "only the final fragment closes"
        );
    }
    let mut actual = Vec::new();
    for (series, _) in &decoded {
        let host = series.metric.get(HOST).unwrap().to_string();
        for (time, values) in compact_float_rows(&[(series.clone(), true)]) {
            actual.push((time, values[0], Some(host.clone())));
        }
    }
    actual.sort();
    assert_eq!(
        vec![
            (0, Some(1.0f64.to_bits()), Some("web-1".into())),
            (0, Some(2.0f64.to_bits()), Some("web-2".into())),
            (1_000, Some(3.0f64.to_bits()), Some("web-1".into())),
            (1_000, Some(4.0f64.to_bits()), Some("web-2".into())),
        ],
        actual
    );
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
            if format == FusedOutputFormat::SeriesFrame {
                canonical_frame_schema(&tree)
            } else {
                two_field_output_schema().to_dfschema_ref().unwrap()
            },
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
            if format == FusedOutputFormat::SeriesFrame {
                canonical_frame_schema(&tree)
            } else {
                output_schema().to_dfschema_ref().unwrap()
            },
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
async fn rewritten_identical_labels_merge_disjoint_positions_into_one_series() {
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
            if format == FusedOutputFormat::SeriesFrame {
                canonical_frame_schema(&tree)
            } else {
                output_schema().to_dfschema_ref().unwrap()
            },
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
    assert!(!decoded.is_empty(), "the rewritten identity is emitted");
    let expected_labels = metric(&[(HOST, "shared"), ("zone", "edge")], Some(METRIC));
    assert!(
        decoded
            .iter()
            .all(|(series, _)| series.metric == expected_labels),
        "every fragment has the complete rewritten identity"
    );
    assert_eq!(
        1,
        decoded.iter().filter(|(_, last)| *last).count(),
        "the merged series closes exactly once"
    );
    assert!(decoded.last().unwrap().1, "only the final fragment closes");
    assert_eq!(Some("shared"), decoded[0].0.metric.get(HOST));
    assert_eq!(Some("edge"), decoded[0].0.metric.get("zone"));
    assert_eq!(Some(METRIC), decoded[0].0.metric.name());
    assert_eq!(
        vec![
            (0, vec![Some(10.0f64.to_bits())]),
            (1_000, vec![Some(21.0f64.to_bits())]),
            (2_000, vec![Some(12.0f64.to_bits())]),
            (5_000, vec![Some(25.0f64.to_bits())]),
        ],
        compact_float_rows(&decoded)
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

    let collision = chunk_of(
        &schema,
        "c",
        metric(&[(HOST, "c")], Some(METRIC)),
        &[2_000],
        vec![vec![Some(999.0)]],
        true,
    );
    let mut conflicting = chunks;
    conflicting.push(collision);
    let conflicting_exec = plan_of(
        make_node(FusedOutputFormat::SeriesFrame),
        memory_exec(frames_of(&schema, &conflicting)),
    )
    .unwrap();
    let result = conflicting_exec
        .execute(0, SessionContext::default().task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await;
    assert!(
        result.is_err(),
        "different valid values at one rewritten identity/position must conflict, not overwrite"
    );
}

#[tokio::test]
async fn extension_planner_routes_fused_evaluated_input_before_global_topk_sum() {
    use crate::extension_plan::{PromExtensionPlanner, SeriesRepartitionExec};
    use crate::native::plan::{
        AggregateKind, AggregateStage, BoundAggregate, BoundVectorOp, Grouping,
    };
    use datafusion::logical_expr::Extension;
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner};

    let raw = raw_schema();
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 1).unwrap()).unwrap());
    let evaluated = Arc::new(SeriesSchema {
        fields: raw.fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    });
    let prefix_tree = instant_tree(Arc::clone(&raw), Arc::clone(&grid));
    let frames = [
        frame_of(
            &raw,
            &chunk_of(
                &raw,
                "left",
                metric(&[(HOST, "left"), (JOB, "g")], Some(METRIC)),
                &[0],
                vec![vec![Some(5.0)]],
                true,
            ),
        ),
        frame_of(
            &raw,
            &chunk_of(
                &raw,
                "right",
                metric(&[(HOST, "right"), (JOB, "g")], Some(METRIC)),
                &[0],
                vec![vec![Some(9.0)]],
                true,
            ),
        ),
    ];
    let physical_source = memory_exec_partitioned_with(
        frame_schema(),
        vec![vec![frames[0].clone()], vec![frames[1].clone()]],
    );
    let planner = DefaultPhysicalPlanner::default();
    let session = SessionContext::default();
    let prefix = FusedSeries::try_new_with_format(
        Arc::from("instant(x)"),
        prefix_tree.clone(),
        binding_of(
            &prefix_tree,
            Arc::clone(&raw),
            vec![HOST.into(), JOB.into(), NAME.into()],
        ),
        vec![input_plan(frame_schema())],
        FusedSeries::series_frame_schema(&prefix_tree)
            .unwrap()
            .to_dfschema_ref()
            .unwrap(),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let prefix_exec = PromExtensionPlanner
        .plan_extension(
            &planner,
            &prefix,
            &[],
            &[Arc::clone(&physical_source)],
            &session.state(),
            &PhysicalPlanningContext::default(),
        )
        .await
        .unwrap()
        .expect("planner handles fused prefix");
    assert_eq!(2, prefix_exec.properties().partitioning.partition_count());
    let probe = PromExtensionPlanner
        .plan_extension(
            &planner,
            &prefix,
            &[],
            &[Arc::clone(&physical_source)],
            &session.state(),
            &PhysicalPlanningContext::default(),
        )
        .await
        .unwrap()
        .unwrap();
    for partition in 0..2 {
        let batches = probe
            .execute(partition, session.task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(
            batches.iter().any(|batch| batch.num_rows() > 0),
            "prefix partition {partition} produces frames"
        );
    }

    let prefix_logical = LogicalPlan::Extension(Extension {
        node: Arc::new(prefix),
    });
    let leaf = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: NativeType::Samples(Arc::clone(&evaluated)),
            ordering: SeriesOrdering::Contiguous,
        }),
        output: NativeType::Samples(Arc::clone(&evaluated)),
    };
    let selected = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(leaf),
            op: BoundVectorOp::TopK {
                k: 1.0,
                top: true,
                group: Grouping::By(Arc::from([Arc::from(JOB)])),
            },
        },
        output: NativeType::Samples(Arc::clone(&evaluated)),
    })
    .unwrap();
    let final_tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(selected.root().clone()),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::from([])),
                stage: AggregateStage::Complete,
            },
        },
        output: NativeType::Samples(Arc::clone(&evaluated)),
    })
    .unwrap();
    let suffix = FusedSeries::try_new_with_format(
        Arc::from("sum(topk(1, instant(x)))"),
        final_tree.clone(),
        binding_of(
            &final_tree,
            Arc::clone(&evaluated),
            vec![HOST.into(), JOB.into(), NAME.into()],
        ),
        vec![prefix_logical.clone()],
        FusedSeries::series_frame_schema(&final_tree)
            .unwrap()
            .to_dfschema_ref()
            .unwrap(),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let suffix_exec = PromExtensionPlanner
        .plan_extension(
            &planner,
            &suffix,
            &[&prefix_logical],
            &[Arc::clone(&prefix_exec)],
            &session.state(),
            &PhysicalPlanningContext::default(),
        )
        .await
        .unwrap()
        .expect("planner handles fused suffix");
    assert_eq!(1, suffix_exec.properties().partitioning.partition_count());

    let pre_routed: Arc<dyn ExecutionPlan> =
        Arc::new(SeriesRepartitionExec::try_new(Arc::clone(&prefix_exec), 2).unwrap());
    let pre_routed_suffix = PromExtensionPlanner
        .plan_extension(
            &planner,
            &suffix,
            &[&prefix_logical],
            &[pre_routed],
            &session.state(),
            &PhysicalPlanningContext::default(),
        )
        .await
        .unwrap()
        .expect("planner handles already-routed global TopK input");
    assert_eq!(
        1,
        pre_routed_suffix
            .properties()
            .partitioning
            .partition_count()
    );
    let outer_router = pre_routed_suffix.children()[0]
        .downcast_ref::<SeriesRepartitionExec>()
        .expect("global TopK requires a one-partition router");
    assert_eq!(1, outer_router.properties().partitioning.partition_count());
    let inner_router = outer_router.children()[0]
        .downcast_ref::<SeriesRepartitionExec>()
        .expect("preprovided two-partition router is retained beneath normalization");
    assert_eq!(2, inner_router.properties().partitioning.partition_count());
    assert!(Arc::ptr_eq(&prefix_exec, inner_router.children()[0]));

    let one_partition_input: Arc<dyn ExecutionPlan> = memory_exec_partitioned_with(
        series_frame_schema(&evaluated, FrameLabelMode::Explicit, None).unwrap(),
        vec![Vec::new()],
    );
    let one_partition_input_for_assert = Arc::clone(&one_partition_input);
    let one_partition_suffix = PromExtensionPlanner
        .plan_extension(
            &planner,
            &suffix,
            &[&prefix_logical],
            &[Arc::clone(&one_partition_input)],
            &session.state(),
            &PhysicalPlanningContext::default(),
        )
        .await
        .unwrap()
        .expect("planner handles one-partition global TopK input");
    assert_eq!(
        1,
        one_partition_suffix
            .properties()
            .partitioning
            .partition_count()
    );
    assert!(
        one_partition_suffix.children()[0]
            .downcast_ref::<SeriesRepartitionExec>()
            .is_none()
    );
    assert!(Arc::ptr_eq(
        &one_partition_input_for_assert,
        one_partition_suffix.children()[0]
    ));

    let chain = suffix_exec.children()[0]
        .downcast_ref::<SeriesRepartitionExec>()
        .expect("multi-partition evaluated input is routed");
    assert_eq!(
        2,
        chain.children()[0]
            .properties()
            .partitioning
            .partition_count()
    );
    let batches = collect(suffix_exec, session.task_ctx()).await.unwrap();
    let decoded = frame_decode(memory_exec_with(
        FusedSeries::series_frame_schema(&final_tree).unwrap(),
        batches,
    ))
    .await;
    assert_eq!(1, decoded.len());
    assert_eq!(
        vec![(0, vec![Some(9.0f64.to_bits())])],
        compact_float_rows(&[(decoded[0].0.clone(), decoded[0].1)]),
    );
    assert!(decoded[0].1);
    assert_eq!(None, decoded[0].0.metric.get(JOB));
    assert_eq!(None, decoded[0].0.metric.get(HOST));
    assert_eq!(None, decoded[0].0.metric.name());
}

#[tokio::test]
async fn actual_window_after_series_repartition_keeps_both_prefix_partitions() {
    use crate::extension_plan::{PromExtensionPlanner, SeriesRepartitionExec};
    use crate::native::plan::BoundWindow;
    use crate::range_eval::{RangeFunction, RangeWindow};
    use datafusion::logical_expr::Extension;
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner};

    let raw = raw_schema();
    let prefix_grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 2).unwrap()).unwrap());
    let output_grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(1_000, 1_000, 1).unwrap()).unwrap());
    let evaluated = Arc::new(SeriesSchema {
        fields: raw.fields.clone(),
        eval_grid: Some(Arc::clone(&prefix_grid)),
    });
    let output = Arc::new(SeriesSchema {
        fields: raw.fields.clone(),
        eval_grid: Some(Arc::clone(&output_grid)),
    });
    let prefix_tree = instant_tree(Arc::clone(&raw), Arc::clone(&prefix_grid));
    let frames = [
        frame_of(
            &raw,
            &chunk_of(
                &raw,
                "shared",
                metric(&[(HOST, "shared")], Some(METRIC)),
                &[0],
                vec![vec![Some(10.0)]],
                true,
            ),
        ),
        frame_of(
            &raw,
            &chunk_of(
                &raw,
                "shared",
                metric(&[(HOST, "shared")], Some(METRIC)),
                &[1_000],
                vec![vec![Some(20.0)]],
                true,
            ),
        ),
    ];
    let source = memory_exec_partitioned_with(
        frame_schema(),
        vec![vec![frames[0].clone()], vec![frames[1].clone()]],
    );
    let planner = DefaultPhysicalPlanner::default();
    let session = SessionContext::default();
    let prefix = FusedSeries::try_new_with_format(
        Arc::from("instant(cpu)"),
        prefix_tree.clone(),
        binding_of(
            &prefix_tree,
            Arc::clone(&raw),
            vec![HOST.into(), NAME.into()],
        ),
        vec![input_plan(frame_schema())],
        FusedSeries::series_frame_schema(&prefix_tree)
            .unwrap()
            .to_dfschema_ref()
            .unwrap(),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let prefix_exec = PromExtensionPlanner
        .plan_extension(
            &planner,
            &prefix,
            &[],
            &[Arc::clone(&source)],
            &session.state(),
            &PhysicalPlanningContext::default(),
        )
        .await
        .unwrap()
        .expect("planner handles fused prefix");
    assert_eq!(2, prefix_exec.properties().partitioning.partition_count());
    for partition in 0..2 {
        let batches = prefix_exec
            .execute(partition, session.task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(
            batches.iter().any(|batch| batch.num_rows() > 0),
            "prefix partition {partition} produces frames"
        );
    }

    let prefix_logical = LogicalPlan::Extension(Extension {
        node: Arc::new(prefix),
    });
    let leaf = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: NativeType::Samples(Arc::clone(&evaluated)),
            ordering: SeriesOrdering::Contiguous,
        }),
        output: NativeType::Samples(Arc::clone(&evaluated)),
    };
    let window_tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(leaf),
            op: BoundWindow {
                function: RangeFunction::SumOverTime,
                window: RangeWindow {
                    range_ms: 2_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: Some(Arc::clone(&prefix_grid)),
                output_grid: Arc::clone(&output_grid),
            },
        },
        output: NativeType::Samples(Arc::clone(&output)),
    })
    .unwrap();
    let suffix = FusedSeries::try_new_with_format(
        Arc::from("sum_over_time(instant(cpu)[2s:1s])"),
        window_tree.clone(),
        binding_of(
            &window_tree,
            Arc::clone(&evaluated),
            vec![HOST.into(), NAME.into()],
        ),
        vec![prefix_logical.clone()],
        FusedSeries::series_frame_schema(&window_tree)
            .unwrap()
            .to_dfschema_ref()
            .unwrap(),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    // The same window on one raw chain is the uncut reference: both samples are visible
    // together, so its expected output is independently established as 10 + 20.
    let baseline_tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(prefix_tree.root().clone()),
            op: BoundWindow {
                function: RangeFunction::SumOverTime,
                window: RangeWindow {
                    range_ms: 2_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: Some(Arc::clone(&prefix_grid)),
                output_grid: Arc::clone(&output_grid),
            },
        },
        output: NativeType::Samples(Arc::clone(&output)),
    })
    .unwrap();
    let baseline_input = frame_of(
        &raw,
        &chunk_of(
            &raw,
            "shared",
            metric(&[(HOST, "shared")], Some(METRIC)),
            &[0, 1_000],
            vec![vec![Some(10.0), Some(20.0)]],
            true,
        ),
    );
    let baseline = FusedSeries::try_new_with_format(
        Arc::from("sum_over_time(instant(cpu)[2s:1s])"),
        baseline_tree.clone(),
        binding_of(
            &baseline_tree,
            Arc::clone(&raw),
            vec![HOST.into(), NAME.into()],
        ),
        vec![input_plan(frame_schema())],
        FusedSeries::series_frame_schema(&baseline_tree)
            .unwrap()
            .to_dfschema_ref()
            .unwrap(),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let baseline_exec = plan_of(
        baseline,
        memory_exec_with(frame_schema(), vec![baseline_input]),
    )
    .unwrap();
    let baseline_batches = collect(baseline_exec, session.task_ctx()).await.unwrap();
    let baseline_decoded = frame_decode(memory_exec_with(
        FusedSeries::series_frame_schema(&baseline_tree).unwrap(),
        baseline_batches,
    ))
    .await;
    assert_eq!(1, baseline_decoded.len());
    assert_eq!(
        vec![(1_000, vec![Some(30.0f64.to_bits())])],
        compact_float_rows(&[(baseline_decoded[0].0.clone(), baseline_decoded[0].1)])
    );
    assert!(baseline_decoded[0].1);

    let prefix_exec_for_assert = Arc::clone(&prefix_exec);
    let suffix_exec = PromExtensionPlanner
        .plan_extension(
            &planner,
            &suffix,
            &[&prefix_logical],
            &[Arc::clone(&prefix_exec)],
            &session.state(),
            &PhysicalPlanningContext::default(),
        )
        .await
        .unwrap()
        .expect("planner handles actual window suffix");
    assert_eq!(2, suffix_exec.properties().partitioning.partition_count());
    let routed = suffix_exec.children()[0]
        .downcast_ref::<SeriesRepartitionExec>()
        .expect("planner inserts direct series router for window input");
    assert_eq!(
        2,
        routed.children()[0]
            .properties()
            .partitioning
            .partition_count()
    );
    assert!(Arc::ptr_eq(&prefix_exec_for_assert, routed.children()[0]));
    let batches = collect(suffix_exec, session.task_ctx()).await.unwrap();
    let decoded = frame_decode(memory_exec_with(
        FusedSeries::series_frame_schema(&window_tree).unwrap(),
        batches,
    ))
    .await;
    assert_eq!(1, decoded.len());
    assert_eq!(
        vec![(1_000, vec![Some(30.0f64.to_bits())])],
        compact_float_rows(&[(decoded[0].0.clone(), decoded[0].1)])
    );
    assert!(decoded[0].1);
    assert_eq!(Some("shared"), decoded[0].0.metric.get(HOST));
    assert_eq!(None, decoded[0].0.metric.name());
}
