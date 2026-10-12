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

use std::sync::Arc;

use arrow::array::{Array, Float64Array, StringArray};
use arrow::util::display::array_value_to_string;
use catalog::RegisterTableRequest;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::OutputData;
use common_recordbatch::{RecordBatch, util};
use datafusion::functions_aggregate::sum::Sum;
use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::ScalarValue;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::{LogicalPlan, Operator};
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use session::context::QueryContext;
use table::test_util::MemTable;

use crate::optimizer::count_join::PayloadFunction;
use crate::options::QueryOptions as EngineOptions;
use crate::parser::QueryLanguageParser;
use crate::query_engine::QueryEngineFactory;

fn engine(enabled: bool, fact: RecordBatch, dim: RecordBatch) -> crate::query_engine::QueryEngineRef {
    let catalog = catalog::memory::new_memory_catalog_manager().unwrap();
    for (name, table_id, batch) in [("fact", 101, fact), ("dim", 102, dim)] {
        let table = MemTable::new_with_catalog(name, batch, table_id,
            DEFAULT_CATALOG_NAME.to_string(), DEFAULT_SCHEMA_NAME.to_string());
        catalog.register_table_sync(RegisterTableRequest { catalog: DEFAULT_CATALOG_NAME.to_string(), schema: DEFAULT_SCHEMA_NAME.to_string(), table_name: name.to_string(), table_id, table }).unwrap();
    }
    QueryEngineFactory::new(catalog, None, None, None, None, None, false,
        EngineOptions { parallelism: 1, experimental_enable_factorized_join_agg: enabled, ..Default::default() })
        .query_engine()
}

fn batches(
    facts: &[Option<&str>],
    values: &[Option<f64>],
    dims: &[(Option<&str>, Option<&str>)],
) -> (RecordBatch, RecordBatch) {
    let fact = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(
                (0..facts.len() as i64).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(facts.to_vec())) as _,
            Arc::new(Float64Vector::from(values.to_vec())) as _,
        ],
    )
    .unwrap();
    let dim = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("area", ConcreteDataType::string_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(
                (0..dims.len() as i64).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(
                dims.iter().map(|(host, _)| *host).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(
                dims.iter().map(|(_, area)| *area).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap();
    (fact, dim)
}

/// Builds a `fact`/`dim` fixture pair whose every row carries a nullable
/// payload: `fact.value` on the left, `dim.weight` on the right.
fn weighted_batches(
    facts: &[Option<&str>],
    values: &[Option<f64>],
    dims: &[(Option<&str>, Option<&str>, Option<f64>)],
) -> (RecordBatch, RecordBatch) {
    let fact = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(
                (0..facts.len() as i64).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(facts.to_vec())) as _,
            Arc::new(Float64Vector::from(values.to_vec())) as _,
        ],
    )
    .unwrap();
    let dim = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("area", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("weight", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(
                (0..dims.len() as i64).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(
                dims.iter().map(|(host, _, _)| *host).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(
                dims.iter().map(|(_, area, _)| *area).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Float64Vector::from(
                dims.iter()
                    .map(|(_, _, weight)| *weight)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap();
    (fact, dim)
}

/// Detects any aggregate that dominates a join through arbitrary
/// intermediate nodes, so projections between them cannot hide the shape.
fn contains_aggregate_above_join(plan: &LogicalPlan) -> bool {
    (matches!(plan, LogicalPlan::Aggregate(_))
        && plan
            .inputs()
            .iter()
            .any(|input| contains_logical_join(input)))
        || plan
            .inputs()
            .iter()
            .any(|input| contains_aggregate_above_join(input))
}

async fn optimized_logical(engine: &crate::query_engine::QueryEngineRef, sql: &str) -> LogicalPlan {
    let ctx = QueryContext::arc();
    let stmt = QueryLanguageParser::parse_sql(sql, &ctx).unwrap();
    let plan = engine.planner().plan(&stmt, ctx).await.unwrap();
    engine.engine_state().optimize_logical_plan(plan).unwrap()
}

async fn run(
    engine: &crate::query_engine::QueryEngineRef,
    sql: &str,
) -> (
    Vec<(String, String, bool)>,
    Vec<String>,
    Arc<dyn ExecutionPlan>,
) {
    let ctx = QueryContext::arc();
    let stmt = QueryLanguageParser::parse_sql(sql, &ctx).unwrap();
    let plan = engine.planner().plan(&stmt, ctx.clone()).await.unwrap();
    let output = engine.execute(plan, ctx).await.unwrap();
    let physical_plan = output
        .meta
        .plan
        .expect("query execution provides a physical plan");
    let OutputData::Stream(stream) = output.data else {
        panic!("expected query stream")
    };
    let schema = stream.schema();
    let signature = schema
        .column_schemas()
        .iter()
        .map(|field| {
            (
                field.name.clone(),
                format!("{:?}", field.data_type.as_arrow_type()),
                field.is_nullable(),
            )
        })
        .collect();
    let rows = util::collect(stream)
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                (0..batch.num_columns())
                    .map(|col| {
                        if batch.column(col).is_null(row) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(batch.column(col).as_ref(), row).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect();
    (signature, rows, physical_plan)
}

// ---------------------------------------------------------------------------
// `AVG(value * weight)` / `SUM(value * weight)`: shape-based, opt-in, and
// explicitly approximate.
//
// The rule never inspects the payloads. Every engine below installs the same
// shape-based rule (`CountJoinRewrite::new` with the fixture's row counts); what
// separates the rewritten query from the baseline query is only whether the rule
// is installed at all.
//
// For well-conditioned payloads the factorized scores are compared against the
// baseline within `APPROX_TOLERANCE`. The one accepted exception — a heavily
// cancelling group, where the two plans may disagree arbitrarily — has its own
// test below.
// ---------------------------------------------------------------------------

/// Standing tolerance of the comparison against the baseline: relative for
/// scores far from zero, absolute for the zero neighbourhood. It leaves several
/// orders of magnitude between the factorization's own rounding error and the
/// assertion.
const APPROX_TOLERANCE: f64 = 1e-9;

/// The supported `AVG(value * weight)` query, ordered the way the paper's
/// fixture orders its scores.
const AVG_SQL: &str = "SELECT f.host, d.area, AVG(f.value * d.weight) AS score FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area ORDER BY score DESC NULLS LAST, f.host NULLS LAST, d.area NULLS LAST";

/// The supported `SUM(value * weight)` query, ordered the way [`AVG_SQL`] orders
/// its scores.
const SUM_SQL: &str = "SELECT f.host, d.area, SUM(f.value * d.weight) AS score FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area ORDER BY score DESC NULLS LAST, f.host NULLS LAST, d.area NULLS LAST";

/// Builds an engine whose rule is the ordinary shape-based one: no data is
/// inspected, so the same construction rewrites `AVG and SUM`.
fn payload_engine(fact: RecordBatch, dim: RecordBatch) -> crate::query_engine::QueryEngineRef {
    engine(true, fact, dim)
}

/// The two supported payload queries with the aggregate each one reconstructs.
fn payload_queries() -> [(&'static str, &'static str, PayloadFunction); 2] {
    [
        ("AVG", AVG_SQL, PayloadFunction::Avg),
        ("SUM", SUM_SQL, PayloadFunction::Sum),
    ]
}

/// Facts and dims built from ordinary floats: non-integral values, non-quarter
/// weights, a fact-only host and a dim-only host. Nothing is classified here —
/// the same fixture is rewritten because its shape matches, and compared against
/// the baseline within `APPROX_TOLERANCE`.
fn general_float_batches() -> (RecordBatch, RecordBatch) {
    weighted_batches(
        &[
            Some("a"),
            Some("a"),
            Some("a"),
            Some("b"),
            Some("b"),
            Some("c"),
        ],
        &[
            Some(0.273),
            Some(1.8e-5),
            Some(-0.6),
            Some(2.5),
            Some(0.125),
            Some(7.0),
        ],
        &[
            (Some("a"), Some("west"), Some(0.3)),
            (Some("a"), Some("east"), Some(1.7)),
            (Some("a"), Some("east"), Some(-0.25)),
            (Some("b"), Some("west"), Some(-1.5)),
            (Some("orphan"), Some("north"), Some(3.0)),
        ],
    )
}

/// The hand-computed scores of [`general_float_batches`] per `(host, area)`:
/// `SUM` is the payload sum product, `AVG` that product over the pair count.
/// `(a, *)` sums to `-0.326982` over `3` fact rows, `(b, west)` to `2.625` over
/// `2`. The fact-only `(c, *)` and the dim-only `(orphan, north)` never pair up
/// and produce no row.
fn general_float_expected() -> Vec<(&'static str, &'static str, f64, f64)> {
    vec![
        ("a", "west", -0.326982 * 0.3, -0.326982 * 0.3 / 3.0),
        ("a", "east", -0.326982 * 1.45, -0.326982 * 1.45 / 6.0),
        ("b", "west", 2.625 * -1.5, 2.625 * -1.5 / 2.0),
    ]
}

/// The expected scores of [`general_float_batches`] for one payload function.
fn general_float_scores(function: PayloadFunction) -> Vec<(&'static str, &'static str, f64)> {
    general_float_expected()
        .into_iter()
        .map(|(host, area, sum, avg)| match function {
            PayloadFunction::Avg => (host, area, avg),
            PayloadFunction::Sum => (host, area, sum),
        })
        .collect()
}

/// Facts and dims of integers and quarter steps: duplicate `(host, area)` dim
/// rows, a fact-only host and a dim-only host. Such payloads factorize exactly,
/// so the two plans are compared without a tolerance.
///
/// Hand-computed scores per `(host, area)`:
///
/// - `(a, east)`: `(2, -1, 3) x (2.5)` sums to `10`, over 3 pairs -> `10/3`
/// - `(b, west)`: `(4, 0) x (1, -0.5)` sums to `2`, over 4 pairs -> `0.5`
/// - `(b, east)`: `(4, 0) x (0)` sums to `0`, over 2 pairs -> `+0.0`
/// - `(a, west)`: `(2, -1, 3) x (-1, 0.25)` sums to `-3`, over 6 pairs -> `-0.5`
/// - `(d, *)` and `(c, west)` never pair up, so they produce no row.
fn integer_batches() -> (RecordBatch, RecordBatch) {
    weighted_batches(
        &[
            Some("a"),
            Some("a"),
            Some("a"),
            Some("b"),
            Some("b"),
            Some("d"),
        ],
        &[
            Some(2.0),
            Some(-1.0),
            Some(3.0),
            Some(4.0),
            Some(0.0),
            Some(7.0),
        ],
        &[
            (Some("a"), Some("west"), Some(-1.0)),
            (Some("a"), Some("west"), Some(0.25)),
            (Some("a"), Some("east"), Some(2.5)),
            (Some("b"), Some("west"), Some(1.0)),
            (Some("b"), Some("west"), Some(-0.5)),
            (Some("b"), Some("east"), Some(0.0)),
            (Some("c"), Some("west"), Some(3.75)),
        ],
    )
}

/// The hand-computed rows of one payload query over [`integer_batches`], in the
/// emitted order, with each score reduced to its bits.
fn integer_expected_scores(function: PayloadFunction) -> Vec<(String, String, Option<u64>)> {
    let bits = |score: f64| Some(score.to_bits());
    let (a_east, b_west, a_west) = match function {
        PayloadFunction::Avg => (10.0f64 / 3.0, 0.5, -0.5),
        PayloadFunction::Sum => (10.0, 2.0, -3.0),
    };
    vec![
        ("a".into(), "east".into(), bits(a_east)),
        ("b".into(), "west".into(), bits(b_west)),
        ("b".into(), "east".into(), bits(0.0)),
        ("a".into(), "west".into(), bits(a_west)),
    ]
}

/// Runs a query and returns `(host, area, score bits)` per row, keeping the
/// emitted order and distinguishing a `NULL` score from any bit pattern.
async fn score_rows(
    engine: &crate::query_engine::QueryEngineRef,
    sql: &str,
) -> Vec<(String, String, Option<u64>)> {
    let ctx = QueryContext::arc();
    let stmt = QueryLanguageParser::parse_sql(sql, &ctx).unwrap();
    let plan = engine.planner().plan(&stmt, ctx.clone()).await.unwrap();
    let output = engine.execute(plan, ctx).await.unwrap();
    let OutputData::Stream(stream) = output.data else {
        panic!("expected query stream")
    };
    let mut rows = Vec::new();
    for batch in util::collect(stream).await.unwrap() {
        let host = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let area = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let score = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let text = |array: &StringArray, row: usize| {
                (!array.is_null(row)).then(|| array.value(row).to_string())
            };
            rows.push((
                text(host, row).unwrap_or_else(|| "NULL".to_string()),
                text(area, row).unwrap_or_else(|| "NULL".to_string()),
                (!score.is_null(row)).then(|| score.value(row).to_bits()),
            ));
        }
    }
    rows
}

/// Asserts `got` is within `tolerance` of `want`, relative to `want` for values
/// far from zero and absolute around it, so a tiny score and a cancelled one are
/// both compared meaningfully.
fn assert_close(got: f64, want: f64, tolerance: f64, what: &str) {
    let scale = want.abs().max(1.0);
    assert!(
        (got - want).abs() <= tolerance * scale,
        "{what}: {got} is not within {tolerance} of {want}"
    );
}

/// Asserts every `(host, area)` score is within `tolerance` of its expected
/// value, regardless of the emitted order.
fn assert_scores_near(
    rows: &[(String, String, Option<u64>)],
    expected: &[(&str, &str, f64)],
    tolerance: f64,
) {
    assert_eq!(expected.len(), rows.len(), "{rows:?}");
    for (host, area, want) in expected {
        let (_, _, bits) = rows
            .iter()
            .find(|(row_host, row_area, _)| row_host == host && row_area == area)
            .unwrap_or_else(|| panic!("no row for ({host}, {area}) in {rows:?}"));
        assert_close(
            f64::from_bits(bits.unwrap_or_else(|| panic!("({host}, {area}) scored NULL"))),
            *want,
            tolerance,
            &format!("({host}, {area})"),
        );
    }
}

/// Asserts the candidate's scores match the baseline's row for row, in the
/// emitted order, within `tolerance` — and that a `NULL` on one side is a `NULL`
/// on the other.
fn assert_scores_match(
    baseline: &[(String, String, Option<u64>)],
    candidate: &[(String, String, Option<u64>)],
    tolerance: f64,
) {
    assert_eq!(baseline.len(), candidate.len(), "{candidate:?}");
    for (base, scored) in baseline.iter().zip(candidate) {
        assert_eq!(
            (&base.0, &base.1),
            (&scored.0, &scored.1),
            "the rewritten plan changed the group rows"
        );
        assert_eq!(base.2.is_some(), scored.2.is_some(), "{scored:?}");
        if let (Some(base_bits), Some(scored_bits)) = (base.2, scored.2) {
            assert_close(
                f64::from_bits(scored_bits),
                f64::from_bits(base_bits),
                tolerance,
                &format!("({}, {})", base.0, base.1),
            );
        }
    }
}

/// Sorts `(host, area, score bits)` rows so a comparison can ignore the emitted
/// order — used where two groups tie on the score.
fn sorted_score_rows(rows: &[(String, String, Option<u64>)]) -> Vec<(String, String, Option<u64>)> {
    let mut rows = rows.to_vec();
    rows.sort();
    rows
}

/// The rewritten `AVG` plan must join exactly two directly aggregated inputs,
/// keep no aggregate above the join, and reconstruct the average from the
/// branch statistics.
fn assert_rewritten_avg_logical_shape(plan: &LogicalPlan) {
    assert_rewritten_payload_logical_shape(plan, PayloadFunction::Avg);
}

/// The rewritten `SUM` plan must join exactly two directly aggregated inputs,
/// keep no aggregate above the join, and reconstruct the sum from the branch
/// statistics.
fn assert_rewritten_sum_logical_shape(plan: &LogicalPlan) {
    assert_rewritten_payload_logical_shape(plan, PayloadFunction::Sum);
}

/// Dispatches the rewritten payload plan check to the function's shape.
fn assert_rewritten_payload_plan(plan: &LogicalPlan, function: PayloadFunction) {
    match function {
        PayloadFunction::Avg => assert_rewritten_avg_logical_shape(plan),
        PayloadFunction::Sum => assert_rewritten_sum_logical_shape(plan),
    }
}

/// Checks the rewritten payload plan shape: two directly aggregated join
/// branches, no aggregate above the join, and a reconstruction for `function`.
fn assert_rewritten_payload_logical_shape(plan: &LogicalPlan, function: PayloadFunction) {
    let joins = find_logical_joins(plan);
    assert_eq!(1, joins.len(), "{plan:?}");
    let LogicalPlan::Join(join) = joins[0] else {
        unreachable!("find_logical_joins only returns joins");
    };
    let left = assert_branch_partial_stats(join.left.as_ref(), 1, "left", plan);
    let right = assert_branch_partial_stats(join.right.as_ref(), 2, "right", plan);
    assert!(
        !contains_aggregate_above_join(plan),
        "aggregate remains above the join: {plan:?}"
    );
    assert_payload_reconstruction(plan, &left, &right, function);
}

/// Checks a join branch is a direct aggregate of the partial statistics the
/// rewrite needs, in order: an optional `COUNT(*)`, then `COUNT(payload)` and
/// `SUM(payload)`. The `COUNT(*)` partial is dropped again by the optimizer's
/// projection pruning whenever the reconstruction does not read the raw pair
/// count, so both lengths are accepted — but no other statistic is.
///
/// Returns the `[rows, payloads, sum]` output names, with the row count only
/// present when the branch still aggregates it.
fn assert_branch_partial_stats(
    input: &LogicalPlan,
    expected_groups: usize,
    side: &str,
    plan: &LogicalPlan,
) -> [Option<String>; 3] {
    let LogicalPlan::Aggregate(aggregate) = input else {
        panic!("{side} join branch is not a direct aggregate: {plan:?}")
    };
    assert_eq!(
        expected_groups,
        aggregate.group_expr.len(),
        "{side} branch group columns: {plan:?}"
    );
    assert!(
        aggregate
            .group_expr
            .iter()
            .all(|expr| matches!(expr, datafusion_expr::Expr::Column(_))),
        "{side} branch groups by non-column expressions: {plan:?}"
    );
    let fields = aggregate.schema.fields();
    let kinds = aggregate
        .aggr_expr
        .iter()
        .enumerate()
        .map(|(index, expr)| {
            let function = match expr {
                datafusion_expr::Expr::AggregateFunction(function) => function,
                datafusion_expr::Expr::Alias(alias) => match alias.expr.as_ref() {
                    datafusion_expr::Expr::AggregateFunction(function) => function,
                    _ => panic!("{side} branch alias does not wrap a function: {plan:?}"),
                },
                _ => panic!("{side} branch does not aggregate a function: {plan:?}"),
            };
            let params = &function.params;
            assert!(
                !params.distinct
                    && params.filter.is_none()
                    && params.order_by.is_empty()
                    && params.null_treatment.is_none(),
                "{side} branch aggregate carries a modifier: {plan:?}"
            );
            let name = fields[expected_groups + index].name().clone();
            let is_count = function.func.inner().downcast_ref::<Count>().is_some();
            let is_sum = function.func.inner().downcast_ref::<Sum>().is_some();
            assert!(
                is_count || is_sum,
                "{side} branch aggregate is neither COUNT nor SUM: {plan:?}"
            );
            if is_count
                && params.args == [datafusion_expr::Expr::Literal(COUNT_STAR_EXPANSION, None)]
            {
                return ("count(*)", name);
            }
            assert_eq!(1, params.args.len(), "{plan:?}");
            assert!(
                matches!(params.args[0], datafusion_expr::Expr::Column(_)),
                "{side} branch aggregate argument is not a column: {plan:?}"
            );
            if is_count {
                ("count(payload)", name)
            } else {
                ("sum(payload)", name)
            }
        })
        .collect::<Vec<_>>();
    let mut rows = None;
    let mut payloads = None;
    let mut sum = None;
    for (kind, name) in &kinds {
        let slot = match *kind {
            "count(*)" => &mut rows,
            "count(payload)" => &mut payloads,
            "sum(payload)" => &mut sum,
            _ => unreachable!(),
        };
        assert!(
            slot.replace(name.clone()).is_none(),
            "{side} branch repeats the {kind} statistic: {plan:?}"
        );
    }
    // Order check: every emitted statistic is the next one the rewrite emits,
    // so `COUNT(*)` may only be missing, never reordered or replaced.
    let expected_order = kinds.iter().map(|(kind, _)| *kind).collect::<Vec<_>>();
    assert!(
        matches!(
            expected_order.as_slice(),
            ["count(*)", "count(payload)", "sum(payload)"] | ["count(payload)", "sum(payload)"]
        ),
        "{side} branch partial statistics are out of order: {plan:?}"
    );
    assert!(
        payloads.is_some() && sum.is_some(),
        "{side} branch does not aggregate the payload statistics: {plan:?}"
    );
    [rows, payloads, sum]
}

/// Checks the projection above the rewritten join reconstructs the payload
/// function from the branch statistics: `AVG` divides the zero-normalized payload
/// sum product by the (cast) payload count product, `SUM` is that product
/// itself. Both guard an empty payload product with `NULL`.
///
/// The optimizer may hoist the shared payload-count product into its own
/// projection (`__common_expr_*`), so the referenced column names are collected
/// from every projection above the join while the shape checks run on the
/// reconstruction case expression itself.
fn assert_payload_reconstruction(
    plan: &LogicalPlan,
    left: &[Option<String>; 3],
    right: &[Option<String>; 3],
    function: PayloadFunction,
) {
    fn find_case(plan: &LogicalPlan) -> Option<datafusion_expr::Expr> {
        if let LogicalPlan::Projection(projection) = plan {
            for expr in &projection.expr {
                let candidate = match expr {
                    datafusion_expr::Expr::Alias(alias) => alias.expr.as_ref(),
                    expr => expr,
                };
                if matches!(candidate, datafusion_expr::Expr::Case(_)) {
                    return Some(candidate.clone());
                }
            }
        }
        plan.inputs().iter().find_map(|input| find_case(input))
    }

    /// Column names referenced by the expressions of every projection in the
    /// plan.
    fn projected_columns(plan: &LogicalPlan, columns: &mut Vec<String>) {
        if let LogicalPlan::Projection(projection) = plan {
            for expr in &projection.expr {
                expr.apply(|node| {
                    if let datafusion_expr::Expr::Column(column) = node {
                        columns.push(column.name.clone());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
            }
        }
        for input in plan.inputs() {
            projected_columns(input, columns);
        }
    }

    let expr = find_case(plan).unwrap_or_else(|| panic!("no payload reconstruction: {plan:?}"));
    let mut columns = Vec::new();
    projected_columns(plan, &mut columns);
    let mut divides = 0;
    let mut casts = 0;
    let mut zero_normalizations = 0;
    expr.apply(|node| {
        match node {
            datafusion_expr::Expr::BinaryExpr(binary) if binary.op == Operator::Divide => {
                divides += 1
            }
            datafusion_expr::Expr::Cast(_) => casts += 1,
            _ => {}
        }
        if let datafusion_expr::Expr::Case(case) = node
            && case.when_then_expr.iter().any(|(when, then)| {
                let against_zero = matches!(
                    when.as_ref(),
                    datafusion_expr::Expr::BinaryExpr(binary)
                        if binary.op == Operator::Eq
                            && matches!(binary.right.as_ref(), datafusion_expr::Expr::Literal(ScalarValue::Float64(Some(zero)), _) if *zero == 0.0)
                );
                let yields_zero = matches!(
                    then.as_ref(),
                    datafusion_expr::Expr::Literal(ScalarValue::Float64(Some(zero)), _) if *zero == 0.0
                );
                against_zero && yields_zero
            })
        {
            zero_normalizations += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    let (expected_divides, expected_casts) = match function {
        PayloadFunction::Avg => (1, 1),
        PayloadFunction::Sum => (0, 0),
    };
    assert_eq!(
        expected_divides, divides,
        "unexpected division in the {function:?} reconstruction {expr:?}"
    );
    assert_eq!(
        expected_casts, casts,
        "unexpected cast in the {function:?} reconstruction {expr:?}"
    );
    assert_eq!(
        1, zero_normalizations,
        "no signed-zero normalization in {expr:?}"
    );
    let payloads = [&left[1], &right[1]];
    let sums = [&left[2], &right[2]];
    for name in payloads.into_iter().chain(sums).flatten() {
        assert!(
            columns.contains(name),
            "reconstruction misses branch statistic {name}: {expr:?}"
        );
    }
    for name in [&left[0], &right[0]].into_iter().flatten() {
        assert!(
            !columns.contains(name),
            "reconstruction references the branch row count {name}: {expr:?}"
        );
    }
}

/// The rewritten payload physical plan must hash-join aggregated branches with
/// no aggregate above the join.
fn assert_rewritten_payload_physical_shape(plan: &dyn ExecutionPlan) {
    assert_eq!(1, count_hash_joins(plan), "{plan:?}");
    assert!(
        check_join_branches(plan, false),
        "no HashJoinExec with aggregated branches: {plan:?}"
    );
}

/// The shape-based rule rewrites every supported payload shape without
/// inspecting the data: both `AVG(value * weight)` and `SUM(value * weight)`
/// leave the baseline plan for the factorized topology, return the baseline's
/// group rows, and stay within `APPROX_TOLERANCE` of the baseline's scores.
#[tokio::test]
async fn supported_payload_shapes_rewrite_and_stay_near_the_baseline() {
    let (fact, dim) = general_float_batches();
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = payload_engine(fact, dim);

    for (name, sql, function) in payload_queries() {
        let (base_schema, _, _) = run(&baseline, sql).await;
        let (candidate_schema, _, candidate_plan) = run(&candidate, sql).await;
        assert_eq!(base_schema, candidate_schema, "{name}");
        assert_eq!(
            ("score".to_string(), "Float64".to_string(), true),
            candidate_schema[2],
            "{name}"
        );

        let base_scores = score_rows(&baseline, sql).await;
        let candidate_scores = score_rows(&candidate, sql).await;
        assert_scores_match(&base_scores, &candidate_scores, APPROX_TOLERANCE);
        assert_scores_near(
            &candidate_scores,
            &general_float_scores(function),
            APPROX_TOLERANCE,
        );

        assert_rewritten_payload_plan(&optimized_logical(&candidate, sql).await, function);
        assert_baseline_logical_shape(&optimized_logical(&baseline, sql).await);
        assert_rewritten_payload_physical_shape(candidate_plan.as_ref());
    }

}

/// Payloads that factorize exactly — integers and quarter steps — equal the
/// baseline without any tolerance, and the shape alone is what decides that:
/// no classifier is consulted here.
#[tokio::test]
async fn integer_and_quarter_payloads_match_the_baseline() {
    let (fact, dim) = integer_batches();
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = payload_engine(fact, dim);

    for (name, sql, function) in payload_queries() {
        let (base_schema, base_rows, _) = run(&baseline, sql).await;
        let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, sql).await;
        assert_eq!(base_schema, candidate_schema, "{name}");
        assert_eq!(base_rows, candidate_rows, "{name}");

        let base_scores = score_rows(&baseline, sql).await;
        let candidate_scores = score_rows(&candidate, sql).await;
        assert_eq!(base_scores, candidate_scores, "{name}");
        assert_eq!(
            integer_expected_scores(function),
            candidate_scores,
            "{name}"
        );

        assert_rewritten_payload_plan(&optimized_logical(&candidate, sql).await, function);
        assert_baseline_logical_shape(&optimized_logical(&baseline, sql).await);
        assert_rewritten_payload_physical_shape(candidate_plan.as_ref());
    }
}

/// Shapes near the one supported payload form keep the baseline plan and the
/// baseline's results: the rule rewrites a single plain `AVG(value * weight)` or
/// `SUM(value * weight)` and nothing else.
#[tokio::test]
async fn near_miss_payload_shapes_keep_the_baseline_plan() {
    let (fact, dim) = general_float_batches();
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = payload_engine(fact, dim);
    assert_rewritten_avg_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);

    let select = |exprs: &str| {
        format!(
            "SELECT f.host, d.area, {exprs} FROM fact f JOIN dim d ON f.host = d.host \
             GROUP BY f.host, d.area ORDER BY score DESC NULLS LAST, f.host NULLS LAST, d.area NULLS LAST"
        )
    };
    let shapes = [
        ("avg of one payload", select("AVG(f.value) AS score")),
        (
            "product plus a constant",
            select("AVG(f.value * d.weight + 1) AS score"),
        ),
        (
            "distinct product",
            select("AVG(DISTINCT f.value * d.weight) AS score"),
        ),
        (
            "filtered product",
            select("AVG(f.value * d.weight) FILTER (WHERE f.value > 0) AS score"),
        ),
        (
            "extra aggregate",
            select("AVG(f.value * d.weight) AS score, COUNT(*) AS n"),
        ),
        ("sum of one payload", select("SUM(f.value) AS score")),
    ];
    for (name, sql) in shapes {
        let (base_schema, base_rows, _) = run(&baseline, &sql).await;
        let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, &sql).await;
        assert_eq!(base_schema, candidate_schema, "{name}");
        assert_eq!(base_rows, candidate_rows, "{name}");
        assert_eq!(
            score_rows(&baseline, &sql).await,
            score_rows(&candidate, &sql).await,
            "{name}"
        );
        assert_baseline_logical_shape(&optimized_logical(&candidate, &sql).await);
        assert_unrewritten_physical_shape(candidate_plan.as_ref());
    }
}

/// Nulls, zeros, duplicate dim rows and empty inputs behave exactly like the
/// baseline. A group without a single non-null product is `NULL` for both
/// rewrites, and a group whose products are all zero is `+0.0` — never the
/// `-0.0` a naive factorization of a cancelling zero numerator would produce.
#[tokio::test]
async fn null_empty_and_duplicate_payloads_match_the_baseline() {
    // `z`: every product is a genuine zero, so the score is `+0.0`. `w`: the
    // fact payloads are a signed zero and a zero, the weight is negative, so the
    // branch sums multiply into `-0.0` while the pairwise sum is `+0.0` — the one
    // place the free zero normalization is load-bearing. `n`: no non-null
    // payload at all, so the score is `NULL` like the baseline's empty-pair-set
    // `AVG`/`SUM`. `(z, west)` appears twice, so the pair count is a product of
    // duplicated dim rows.
    let (fact, dim) = weighted_batches(
        &[
            Some("z"),
            Some("z"),
            Some("w"),
            Some("w"),
            Some("n"),
            Some("n"),
        ],
        &[Some(0.0), Some(0.0), Some(-0.0), Some(0.0), None, None],
        &[
            (Some("z"), Some("west"), Some(0.3)),
            (Some("z"), Some("west"), Some(0.3)),
            (Some("w"), Some("west"), Some(-1.5)),
            (Some("n"), Some("west"), Some(0.3)),
        ],
    );
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = payload_engine(fact, dim);

    // The `(host, area, score)` rows, with the two `0.0` groups tied on the
    // score and the empty group `NULL`; the tie order is not asserted.
    let expected_rows = vec!["w|west|0.0", "z|west|0.0", "n|west|NULL"];
    let bits_of = |rows: &[(String, String, Option<u64>)], host: &str| {
        rows.iter()
            .find(|(row_host, _, _)| row_host == host)
            .unwrap_or_else(|| panic!("no row for {host} in {rows:?}"))
            .2
    };

    for (name, sql, function) in payload_queries() {
        let (base_schema, base_rows, _) = run(&baseline, sql).await;
        let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, sql).await;
        assert_eq!(base_schema, candidate_schema, "{name}");
        // The two `0.0` groups tie on the score, so only the row set is
        // compared, not the tie order.
        let sorted = |rows: &[String]| {
            let mut rows = rows.to_vec();
            rows.sort();
            rows
        };
        assert_eq!(sorted(&base_rows), sorted(&candidate_rows), "{name}");
        let mut sorted_expected = expected_rows.clone();
        sorted_expected.sort();
        assert_eq!(sorted_expected, sorted(&candidate_rows), "{name}");

        let scores = score_rows(&candidate, sql).await;
        assert_eq!(
            sorted_score_rows(&score_rows(&baseline, sql).await),
            sorted_score_rows(&scores),
            "{name}"
        );
        assert_eq!(
            Some(0.0f64.to_bits()),
            bits_of(&scores, "z"),
            "{name}: the all-zero group is +0.0, not NULL"
        );
        assert_eq!(
            Some(0.0f64.to_bits()),
            bits_of(&scores, "w"),
            "{name}: the signed-zero group is normalized to +0.0, not -0.0"
        );
        assert_eq!(None, bits_of(&scores, "n"), "{name}: empty group is NULL");

        assert_rewritten_payload_plan(&optimized_logical(&candidate, sql).await, function);
        assert_rewritten_payload_physical_shape(candidate_plan.as_ref());
    }

    // Empty inputs keep the baseline's (empty) results as well.
    for (name, empty) in [
        (
            "empty fact",
            weighted_batches(&[], &[], &[(Some("a"), Some("west"), Some(1.0))]),
        ),
        (
            "empty dim",
            weighted_batches(&[Some("a")], &[Some(1.0)], &[]),
        ),
    ] {
        let (fact, dim) = empty;
        let baseline = engine(false, fact.clone(), dim.clone());
        let candidate = payload_engine(fact, dim);
        for (query, sql, _) in payload_queries() {
            let (base_schema, base_rows, _) = run(&baseline, sql).await;
            let (candidate_schema, candidate_rows, _) = run(&candidate, sql).await;
            assert_eq!(base_schema, candidate_schema, "{name}/{query}");
            assert!(base_rows.is_empty(), "{name}/{query}");
            assert_eq!(base_rows, candidate_rows, "{name}/{query}");
        }
    }
}

/// The documented accepted risk of the opt-in mode: the rewrite is shape-based,
/// so a heavily cancelling group is rewritten too, and its factorized score can
/// differ from the pairwise baseline arbitrarily — here by many orders of
/// magnitude, including a different sign. This is **not** a fallback: the plan
/// stays rewritten and the result is the factorized one.
#[tokio::test]
async fn heavy_cancellation_is_rewritten_and_differs_from_the_baseline() {
    // Both sides nearly cancel: the fact payloads sum to `0` while their
    // magnitudes are `1e16`, and the weights cancel `1e16 - 1e16` down to
    // `1e16`. The pair products are `1e32`-sized, so the baseline's own
    // summation is dominated by rounding at that magnitude while the
    // factorization keeps only the cancelling residuals.
    let (fact, dim) = weighted_batches(
        &[Some("a"), Some("a"), Some("a")],
        &[Some(1e16), Some(1.0), Some(-1e16)],
        &[
            (Some("a"), Some("west"), Some(1e16)),
            (Some("a"), Some("west"), Some(-1e16)),
            (Some("a"), Some("west"), Some(1e16)),
        ],
    );
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = payload_engine(fact, dim);

    for (name, sql, function) in payload_queries() {
        // The shape matches, so the query is rewritten: no classifier keeps this
        // ill-conditioned pair on the baseline plan.
        assert_rewritten_payload_plan(&optimized_logical(&candidate, sql).await, function);
        assert_baseline_logical_shape(&optimized_logical(&baseline, sql).await);

        let base = score_rows(&baseline, sql).await;
        let rewritten = score_rows(&candidate, sql).await;
        assert_eq!(1, base.len(), "{name}");
        let base_score = f64::from_bits(base[0].2.unwrap());
        let rewritten_score = f64::from_bits(rewritten[0].2.unwrap());
        // The baseline's own summation is dominated by rounding at these
        // magnitudes — the pair products are `1e32`-sized and the factorized
        // residuals are not — so the two plans disagree by many orders of
        // magnitude. That is exactly what this opt-in mode accepts.
        let difference = (base_score - rewritten_score).abs();
        assert!(
            difference > 1e9,
            "{name}: expected the factorized score to be far from the baseline's, \
             got baseline {base_score} and factorized {rewritten_score}"
        );
    }
}
