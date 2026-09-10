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

use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::ToDFSchema;
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{IsNotNullExpr, Literal};
use datafusion::physical_plan::aggregates::group_values::new_group_values;
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{Partitioning, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{FutureExt, TryStreamExt};

use super::*;
use crate::extension_plan::{RangeManipulate, SeriesDivide, SeriesNormalize};

const START: i64 = -1000;
const STEP: i64 = 1000;

fn fixture(label_type: DataType, label_count: usize, all_null: bool) -> Vec<Vec<RecordBatch>> {
    let mut fields: Vec<_> = (0..label_count)
        .map(|i| Field::new(format!("label{i}"), label_type.clone(), true))
        .collect();
    fields.insert(
        0,
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    );
    fields.push(Field::new("value", DataType::Float64, true));
    let schema = Arc::new(Schema::new(fields));
    let mut partitions = vec![Vec::new(); 4];
    for series in 0..48 {
        let timestamps: Vec<_> = (0..37).filter(|step| (step + series) % 3 != 0).collect();
        let mut arrays: Vec<ArrayRef> =
            vec![Arc::new(TimestampMillisecondArray::from_iter_values(
                timestamps.iter().map(|step| START + step * STEP),
            ))];
        for label in 0..label_count {
            let value = format!("region-or-host-{label}-{}", series % 5);
            let value = (series % 5 != 0).then_some(value.as_str());
            let values = Arc::new(StringViewArray::from(vec![value; timestamps.len()])) as ArrayRef;
            arrays.push(cast(&values, &label_type).unwrap());
        }
        arrays.push(Arc::new(Float64Array::from_iter(timestamps.iter().map(
            |step| {
                if all_null || step % 13 == 0 {
                    return None;
                }
                Some(match step % 13 {
                    1 => f64::NAN,
                    2 => f64::INFINITY,
                    3 => f64::NEG_INFINITY,
                    4 => -0.0,
                    _ => series as f64 * 0.125 + *step as f64 * 0.0625,
                })
            },
        ))));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        partitions[(series % 4) as usize].push(batch.slice(1, batch.num_rows() - 1));
    }
    partitions
}

fn context(limit: usize) -> (Arc<TaskContext>, Arc<GreedyMemoryPool>) {
    let pool = Arc::new(GreedyMemoryPool::new(limit));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()
        .unwrap();
    let config = SessionConfig::new()
        .with_target_partitions(4)
        .with_batch_size(8);
    (
        SessionContext::new_with_config_rt(config, runtime).task_ctx(),
        pool,
    )
}

fn partial(
    partitions: &[Vec<RecordBatch>],
    schema: SchemaRef,
    candidate: bool,
) -> Arc<dyn ExecutionPlan> {
    let input = MemorySourceConfig::try_new_exec(partitions, schema.clone(), None).unwrap();
    let value = schema.fields().len() - 1;
    let filter = Arc::new(
        FilterExec::try_new(
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("value", value)))),
            input,
        )
        .unwrap(),
    );
    let groups = PhysicalGroupBy::new_single(
        schema.fields()[..value]
            .iter()
            .enumerate()
            .map(|(index, field)| {
                (
                    Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect(),
    );
    let aggregates = [avg_udaf(), sum_udaf()]
        .into_iter()
        .map(|udaf| {
            Arc::new(
                AggregateExprBuilder::new(
                    udaf.clone(),
                    vec![Arc::new(Column::new("value", value))],
                )
                .schema(schema.clone())
                .alias(udaf.name())
                .build()
                .unwrap(),
            )
        })
        .collect();
    let partial = AggregateExec::try_new(
        AggregateMode::Partial,
        groups,
        aggregates,
        vec![None, None],
        filter,
        schema,
    )
    .unwrap();
    if candidate {
        Arc::new(
            SeriesAggregateExec::try_new(&partial, START, START + 36 * STEP, STEP)
                .unwrap()
                .unwrap(),
        )
    } else {
        Arc::new(partial)
    }
}

fn final_plan(input: Arc<dyn ExecutionPlan>, original: &AggregateExec) -> Arc<dyn ExecutionPlan> {
    let repartition = Arc::new(
        RepartitionExec::try_new(input, Partitioning::Hash(original.output_group_expr(), 4))
            .unwrap(),
    );
    let group = PhysicalGroupBy::new_single(
        original
            .output_group_expr()
            .into_iter()
            .enumerate()
            .map(|(index, expr)| (expr, original.schema().field(index).name().clone()))
            .collect(),
    );
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group,
            original.aggr_expr().to_vec(),
            vec![None, None],
            repartition,
            original.input_schema(),
        )
        .unwrap(),
    )
}

/// (timestamp, labels, aggregate state bit patterns), sorted for order-insensitive comparison.
type CanonicalRow = (i64, Vec<Option<String>>, Vec<Option<u64>>);

fn canonical(batches: &[RecordBatch], label_count: usize) -> Vec<CanonicalRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let labels: Vec<_> = batch.columns()[1..=label_count]
            .iter()
            .map(|a| cast(a, &DataType::Utf8View).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            rows.push((
                timestamps.value(row),
                labels
                    .iter()
                    .map(|a| string_at(a.as_ref(), row).unwrap().map(str::to_owned))
                    .collect(),
                batch.columns()[label_count + 1..]
                    .iter()
                    .map(|a| {
                        let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
                        (!a.is_null(row)).then(|| {
                            if a.value(row).is_nan() {
                                f64::NAN.to_bits()
                            } else {
                                a.value(row).to_bits()
                            }
                        })
                    })
                    .collect(),
            ));
        }
    }
    rows.sort();
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_final_matches_datafusion() {
    for label_type in [
        DataType::Utf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8View)),
    ] {
        for label_count in [0, 1, 3, 4] {
            for all_null in [false, true] {
                let partitions = fixture(label_type.clone(), label_count, all_null);
                let schema = partitions[0][0].schema();
                let original = partial(&partitions, schema.clone(), false);
                let original_agg = original.as_any().downcast_ref::<AggregateExec>().unwrap();
                let (ctx, pool) = context(64 * 1024 * 1024);
                let baseline = collect(final_plan(original.clone(), original_agg), ctx.clone())
                    .await
                    .unwrap();
                let candidate = collect(
                    final_plan(partial(&partitions, schema, true), original_agg),
                    ctx,
                )
                .await
                .unwrap();
                assert_eq!(
                    canonical(&baseline, label_count),
                    canonical(&candidate, label_count),
                    "{label_type:?}, labels={label_count}"
                );
                if all_null {
                    assert!(canonical(&candidate, label_count).is_empty());
                }
                assert_eq!(pool.reserved(), 0);
            }
        }
    }
}

/// The materialized `rate` pipeline and the fused rewrite of it, over `partitions`.
fn fused_rate_plans(
    partitions: &[Vec<RecordBatch>],
    schema: SchemaRef,
    divide_tag: &str,
    end: i64,
    lookback: i64,
) -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
    let input = MemorySourceConfig::try_new_exec(partitions, schema.clone(), None).unwrap();
    let logical = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(schema.as_ref().clone().to_dfschema().unwrap()),
    });
    let input = SeriesDivide::new(
        vec![divide_tag.to_string()],
        "timestamp".to_string(),
        logical.clone(),
    )
    .to_execution_plan(input);
    let input = SeriesNormalize::new(
        0,
        "timestamp",
        true,
        vec![divide_tag.to_string()],
        logical.clone(),
    )
    .to_execution_plan(input);
    let range = RangeManipulate::new(
        START,
        end,
        STEP,
        lookback,
        "timestamp".to_string(),
        vec!["value".to_string()],
        logical,
    )
    .unwrap()
    .to_execution_plan(input);
    let range_schema = range.schema();
    let rate = Arc::new(
        ScalarFunctionExpr::try_new(
            Arc::new(Rate::scalar_udf()),
            vec![
                Arc::new(Column::new("timestamp_range", 4)),
                Arc::new(Column::new("value", 3)),
                Arc::new(Column::new("timestamp", 0)),
                Arc::new(Literal::new(ScalarValue::Int64(Some(lookback)))),
            ],
            range_schema.as_ref(),
            Arc::new(ConfigOptions::new()),
        )
        .unwrap(),
    ) as Arc<dyn PhysicalExpr>;
    let projection = Arc::new(
        ProjectionExec::try_new(
            vec![
                (
                    Arc::new(Column::new("timestamp", 0)) as Arc<dyn PhysicalExpr>,
                    "timestamp".to_string(),
                ),
                (rate, "rate".to_string()),
                (
                    Arc::new(Column::new("host", 1)) as Arc<dyn PhysicalExpr>,
                    "host".to_string(),
                ),
            ],
            range,
        )
        .unwrap(),
    );
    let projected_schema = projection.schema();
    let filter = Arc::new(
        FilterExec::try_new(
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("rate", 1)))),
            projection,
        )
        .unwrap(),
    );
    let groups = PhysicalGroupBy::new_single(vec![
        (
            Arc::new(Column::new("timestamp", 0)),
            "timestamp".to_string(),
        ),
        (Arc::new(Column::new("host", 2)), "host".to_string()),
    ]);
    let aggregates = [avg_udaf(), sum_udaf()]
        .into_iter()
        .map(|udaf| {
            Arc::new(
                AggregateExprBuilder::new(udaf.clone(), vec![Arc::new(Column::new("rate", 1))])
                    .schema(projected_schema.clone())
                    .alias(udaf.name())
                    .build()
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let original = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            groups,
            aggregates,
            vec![None, None],
            filter,
            projected_schema,
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    let original_aggregate = original.as_any().downcast_ref::<AggregateExec>().unwrap();
    let candidate = Arc::new(
        SeriesAggregateExec::try_new(original_aggregate, START, end, STEP)
            .unwrap()
            .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    (original, candidate)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fused_rate_aggregate_matches_materialized_range_pipeline() {
    // "pod" and "host" cut the input into the same series, but only the tag the divide
    // splits on is proven constant per batch, so grouping by the other one must fall back
    // to the materialized range pipeline and still agree with it.
    for (divide_tag, fused) in [("host", true), ("pod", false)] {
        fused_rate_case(divide_tag, fused).await;
    }
}

async fn fused_rate_case(divide_tag: &str, fused: bool) {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8View, false),
        Field::new("pod", DataType::Utf8View, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let mut partitions = vec![Vec::new()];
    for series in 0..8 {
        let timestamps = [-2000, -1000, 0, 1000, 2000, 4000, 5000, 6000];
        let stale = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
        let values = [
            stale,
            stale,
            10.0 + series as f64,
            1.0 + series as f64,
            3.0 + series as f64,
            5.0 + series as f64,
            8.0 + series as f64,
            13.0 + series as f64,
        ];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(StringViewArray::from(vec![
                    format!("host-{series}");
                    timestamps.len()
                ])),
                Arc::new(StringViewArray::from(vec![
                    format!("pod-{series}");
                    timestamps.len()
                ])),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap();
        partitions[0].extend([batch.slice(0, 2), batch.slice(2, 3), batch.slice(5, 3)]);
    }
    let (original, candidate) = fused_rate_plans(&partitions, schema, divide_tag, 6000, 3000);
    let original_aggregate = original.as_any().downcast_ref::<AggregateExec>().unwrap();
    let candidate_plan = format!("{}", displayable(candidate.as_ref()).one_line());
    assert_eq!(candidate_plan.contains("fused=prom_rate"), fused);

    let (ctx, pool) = context(64 * 1024 * 1024);
    let expected = collect(
        final_plan(original.clone(), original_aggregate),
        ctx.clone(),
    )
    .await
    .unwrap();
    let actual = collect(final_plan(candidate, original_aggregate), ctx)
        .await
        .unwrap();
    let expected = canonical(&expected, 1);
    // 8 series, and the two leading stale samples leave 6 evaluable points each.
    assert_eq!(expected.len(), 8 * 6);
    assert_eq!(canonical(&actual, 1), expected);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn series_without_any_rate_retain_no_group_state() {
    // Every series holds a single sample, so every window is too short for a rate and the
    // aggregate produces nothing. The materialized plan drops those rows before grouping;
    // the fused path has to reach the same state, or one label and one slot table per
    // series pile up unaccounted for.
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8View, false),
        Field::new("pod", DataType::Utf8View, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let mut partitions = vec![Vec::new()];
    for series in 0..64 {
        partitions[0].push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![0])),
                    Arc::new(StringViewArray::from(vec![format!("host-{series}")])),
                    Arc::new(StringViewArray::from(vec![format!("pod-{series}")])),
                    Arc::new(Float64Array::from(vec![1.0])),
                ],
            )
            .unwrap(),
        );
    }
    let (original, candidate) = fused_rate_plans(&partitions, schema, "host", 1000, 1000);
    let original_aggregate = original.as_any().downcast_ref::<AggregateExec>().unwrap();
    let (ctx, pool) = context(64 * 1024 * 1024);
    assert!(
        collect(
            final_plan(original.clone(), original_aggregate),
            ctx.clone()
        )
        .await
        .unwrap()
        .iter()
        .all(|batch| batch.num_rows() == 0)
    );

    let candidate = candidate
        .as_any()
        .downcast_ref::<SeriesAggregateExec>()
        .unwrap();
    let mut stream = candidate.stream(0, ctx).unwrap();
    assert!(stream.next().await.is_none());
    assert!(stream.groups.labels.is_empty());
    assert!(stream.groups.slots.is_empty());
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dictionary_domains_preserve_full_labels_and_logical_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("scope", DataType::Utf8View, false),
        Field::new(
            "host",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8View)),
            true,
        ),
        Field::new("value", DataType::Float64, true),
    ]));
    let cases = [
        ("scope-a", vec![Some("host-a")], Some(0), 1.0),
        (
            "scope-a",
            vec![Some("unused"), Some("host-a")],
            Some(1),
            2.0,
        ),
        ("scope-a", vec![Some("host-b")], Some(0), 4.0),
        ("scope-b", vec![Some("host-a")], Some(0), 8.0),
        ("scope-a", vec![Some("unused")], None, 16.0),
        ("scope-a", vec![None], Some(0), 32.0),
        ("scope-a", vec![Some("")], Some(0), 64.0),
    ];
    let mut partitions = vec![Vec::new(); 4];
    for (index, (scope, values, key, value)) in cases.into_iter().enumerate() {
        let labels = DictionaryArray::<UInt32Type>::try_new(
            UInt32Array::from(vec![key; 4]),
            Arc::new(StringViewArray::from(values)),
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![
                    START - STEP,
                    START,
                    START + STEP,
                    START + 2 * STEP,
                ])),
                Arc::new(StringViewArray::from(vec![scope; 4])),
                Arc::new(labels),
                Arc::new(Float64Array::from(vec![value; 4])),
            ],
        )
        .unwrap();
        partitions[index % 4].push(batch.slice(1, 3));
    }
    let original = partial(&partitions, schema.clone(), false);
    let original_agg = original.as_any().downcast_ref::<AggregateExec>().unwrap();
    let (ctx, pool) = context(64 * 1024 * 1024);
    let expected = collect(final_plan(original.clone(), original_agg), ctx.clone())
        .await
        .unwrap();
    let actual = collect(
        final_plan(partial(&partitions, schema, true), original_agg),
        ctx,
    )
    .await
    .unwrap();
    let expected = canonical(&expected, 2);
    assert_eq!(expected.len(), 15);
    assert_eq!(canonical(&actual, 2), expected);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_pressure_emits_partial_states() {
    let partitions = fixture(DataType::Utf8View, 3, false);
    let schema = partitions[0][0].schema();
    let original = partial(&partitions, schema.clone(), false);
    let original_agg = original.as_any().downcast_ref::<AggregateExec>().unwrap();
    let (ctx, _) = context(64 * 1024 * 1024);
    let expected = collect(final_plan(original.clone(), original_agg), ctx.clone())
        .await
        .unwrap();
    let (limited, pool) = context(1024);
    let output = collect(partial(&partitions, schema, true), limited)
        .await
        .unwrap();
    assert_eq!(pool.reserved(), 0);
    assert!(output.iter().all(|batch| batch.num_rows() <= 8));
    assert!(
        output.iter().map(RecordBatch::num_rows).sum::<usize>() > canonical(&expected, 3).len()
    );
    let source =
        MemorySourceConfig::try_new_exec(std::slice::from_ref(&output), output[0].schema(), None)
            .unwrap();
    let actual = collect(final_plan(source, original_agg), ctx)
        .await
        .unwrap();
    assert_eq!(canonical(&actual, 3), canonical(&expected, 3));
}

#[tokio::test]
async fn cancellation_errors_and_empty_input_release_memory() {
    let mut partitions = fixture(DataType::Utf8View, 1, false);
    let schema = partitions[0][0].schema();
    let (ctx, pool) = context(64 * 1024 * 1024);
    let mut stream = partial(&partitions, schema.clone(), true)
        .execute(0, ctx.clone())
        .unwrap();
    assert!(stream.next().now_or_never().is_none());
    assert!(pool.reserved() > 0);
    drop(stream);
    assert_eq!(pool.reserved(), 0);

    let mut columns = partitions[0][0].columns().to_vec();
    columns[0] = Arc::new(TimestampMillisecondArray::from(vec![
        START + 1;
        partitions[0][0]
            .num_rows()
    ]));
    partitions[0][0] = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut stream = partial(&partitions, schema.clone(), true)
        .execute(0, ctx.clone())
        .unwrap();
    assert!(
        stream
            .try_next()
            .await
            .unwrap_err()
            .to_string()
            .contains("outside the evaluation grid")
    );
    assert_eq!(pool.reserved(), 0);
    assert!(stream.next().await.is_none());
    for partition in &mut partitions {
        partition.clear();
    }
    assert!(
        collect(partial(&partitions, schema, true), ctx)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn sparse_grid_and_dense_promotion_preserve_group_ids() {
    let mut slots = TimeSlots::Sparse(HashMap::default());
    assert_eq!(slots.intern(999_999_999, 0, 1_000_000_000), 0);
    assert_eq!(slots.intern(999_999_999, 1, 1_000_000_000), 0);
    assert!(slots.size() < 1024);
    let mut slots = TimeSlots::Sparse(HashMap::default());
    for i in 0..128 {
        assert_eq!(slots.intern(i, i, 128), i);
    }
    assert!(matches!(slots, TimeSlots::Dense(_)));
    for i in (0..128).rev() {
        assert_eq!(slots.intern(i, 128, 128), i);
    }
}

#[test]
fn grouping_matches_datafusion_and_rejects_mixed_labels() {
    let partitions = fixture(
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8View)),
        3,
        false,
    );
    let schema = Arc::new(Schema::new(
        partitions[0][0].schema().fields()[..4].to_vec(),
    ));
    let mut baseline = new_group_values(schema, &GroupOrdering::None).unwrap();
    let mut candidate = SeriesGroups::default();
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    let grid = Grid::new(START, START + 36 * STEP, STEP).unwrap();
    for batch in partitions.iter().flatten() {
        baseline
            .intern(&batch.columns()[..4], &mut expected)
            .unwrap();
        candidate
            .intern(batch, &[0, 1, 2, 3], 0, grid, &mut actual)
            .unwrap();
        assert_eq!(actual, expected);
    }
    let values = StringViewArray::from(vec![Some("a"), Some("b")]);
    assert!(
        series_label(&values, true)
            .unwrap_err()
            .to_string()
            .contains("mixed series labels")
    );
    assert!(Grid::new(i64::MIN, i64::MAX, 1).is_none());
    assert!(Grid::new(0, 1, 0).is_none());
}

#[test]
fn dictionary_output_is_bounded_by_batch_size() {
    let count = 65_537;
    let labels: Vec<_> = (0..count).map(|i| format!("host-{i}")).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "host",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View)),
            true,
        ),
    ]));
    for batch_size in [8192, 100_000] {
        let mut output = PendingOutput {
            labels: vec![Arc::new(StringViewArray::from_iter_values(
                labels.iter().map(String::as_str),
            ))],
            keys: (0..count).map(|i| (i, START)).collect(),
            state: vec![],
            offset: 0,
        };
        let mut actual = Vec::new();
        while let Some(batch) = output.next(&schema, 0, batch_size).unwrap() {
            assert!(batch.num_rows() <= batch_size.min(65_536));
            let values = cast(batch.column(1), &DataType::Utf8View).unwrap();
            actual.extend(
                (0..batch.num_rows())
                    .map(|row| string_at(values.as_ref(), row).unwrap().unwrap().to_owned()),
            );
        }
        assert_eq!(actual, labels);
    }
}
