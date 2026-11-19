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

//! Benchmarks for the native PromQL vector-vector kernel (`binary_vectors`).
//!
//! The shape is the one the per-pairing result-row lookup was measured on: [`SERIES`] distinct
//! result label sets on an evaluation grid of [`POINTS`] points, so every iteration files
//! `SERIES * POINTS` points. The kernel used to rebuild and rehash the whole result label set for
//! each filed point; the three cases separate that cost from the operator's own work by varying
//! only the number of labels the result key carries.

use std::error::Error;
use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group};
use datatypes::arrow::array::Float64Array;
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels};
use promql::vector_match::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels, binary_vectors,
};

/// Distinct result label sets, which is also the number of series on each side of a one-to-one
/// case.
const SERIES: usize = 160;

/// Grid points every series carries, dense: no hole, no chunk.
const POINTS: usize = 2017;

/// One side's series of a `group_left` case: how many of them share a match group.
const GROUPS: usize = 4;

/// First grid timestamp of the evaluation grid, in milliseconds.
const START_MS: i64 = 1_700_000_000_000;

/// Grid step, in milliseconds: a 15 s scrape interval.
const STEP_MS: i64 = 15_000;

/// Every error path of the fixture and of the checks, erased to one type.
type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// One timed case: the operands, the matching between them, and what a correct result holds.
struct Case {
    name: &'static str,
    matching: BinaryMatching,
    /// Shared by both operands: one field, one evaluation grid.
    schema: SeriesSchema,
    lhs: Vec<Series>,
    rhs: Vec<Series>,
    /// The right-hand series a left-hand series pairs with.
    one_index: fn(usize) -> usize,
    /// Distinct result label sets a correct operation files.
    result_series: usize,
    /// Labels one result label set carries: what the kernel rebuilds per filed point.
    result_labels: usize,
    /// Filed points a correct operation files.
    filed_points: usize,
}

/// Erases an error of either operand type into [`Result`]'s.
fn boxed<E: Error + 'static>(error: E) -> Box<dyn Error> {
    Box::new(error)
}

/// The evaluation grid every series of every case sits on.
fn build_grid() -> Result<Arc<EvalGrid>> {
    let spec = GridSpec::try_new(START_MS, STEP_MS, POINTS as u64).map_err(boxed)?;
    Ok(Arc::new(EvalGrid::try_new(spec).map_err(boxed)?))
}

/// One field over the case grid, which is what `binary_vectors` pairs positionally.
fn build_schema(grid: &Arc<EvalGrid>) -> SeriesSchema {
    SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: Some(grid.clone()),
    }
}

/// One series carrying a value at every point of the grid, as one dense run.
fn build_series(
    schema: &SeriesSchema,
    grid: &Arc<EvalGrid>,
    metric: SeriesLabels,
    value_at: impl Fn(usize) -> f64,
) -> Result<Series> {
    let values = (0..POINTS).map(value_at).collect::<Vec<f64>>();
    let samples = Samples::from_float(Float64Array::from(values));
    let range = GridRange::try_new(grid.clone(), 0, POINTS).map_err(boxed)?;
    Series::try_new(
        schema,
        metric,
        TimeAxis::Grid(range),
        SeriesFields::one(samples),
    )
    .map_err(boxed)
}

/// The identity pairing: both sides carry the same label set at the same index.
fn paired(index: usize) -> usize {
    index
}

/// The `group_left` pairing: left-hand series `index` joins the right-hand series of its code.
fn code_of(index: usize) -> usize {
    index % GROUPS
}

/// One label with an owned value.
fn label(name: &str, value: String) -> Label {
    Label {
        name: Arc::from(name),
        value: Arc::from(value),
    }
}

/// The label set of the narrow case: the one label the operation matches and keeps.
fn narrow_metric(index: usize) -> Result<SeriesLabels> {
    SeriesLabels::try_new(
        Some(Arc::from("http_requests_total")),
        vec![label("instance", format!("inst-{index}"))],
    )
    .map_err(boxed)
}

/// The label set of the wide cases: eight labels, one of which is the `group_left` match label.
fn wide_metric(index: usize) -> Result<SeriesLabels> {
    let tags = vec![
        label("cluster", "c1".to_string()),
        label("code", format!("code-{}", index % GROUPS)),
        label("instance", format!("inst-{index}")),
        label("job", "greptimedb".to_string()),
        label("namespace", "prod".to_string()),
        label("pod", format!("pod-{index}")),
        label("region", "r1".to_string()),
        label("zone", "z1".to_string()),
    ];
    SeriesLabels::try_new(Some(Arc::from("http_requests_total")), tags).map_err(boxed)
}

/// The label set of a `group_left` one side: only the label the match is on.
fn code_metric(code: usize) -> Result<SeriesLabels> {
    SeriesLabels::try_new(
        Some(Arc::from("http_requests_total")),
        vec![label("code", format!("code-{code}"))],
    )
    .map_err(boxed)
}

/// The left value of series `index` at `point`: `1000 + index + 0.25 * point`, exact in `f64`.
fn lhs_value(index: usize, point: usize) -> f64 {
    1000.0 + index as f64 + 0.25 * point as f64
}

/// The right value of series `index` at `point`: `7 * (index + 1) - 0.5 * point`, exact in `f64`.
fn rhs_value(index: usize, point: usize) -> f64 {
    7.0 * (index as f64 + 1.0) - 0.5 * point as f64
}

/// What `+` files for left series `index` and its right partner at `point`, computed the way the
/// kernel computes it: the same two exact operands, added.
fn expected_value(index: usize, point: usize, one_index: fn(usize) -> usize) -> f64 {
    lhs_value(index, point) + rhs_value(one_index(index), point)
}

/// A one-to-one case: `SERIES` distinct label sets on each side, each pairing with the one series
/// of its own match group.
fn build_one_to_one(
    name: &'static str,
    grid: &Arc<EvalGrid>,
    schema: &SeriesSchema,
    metric_of: fn(usize) -> Result<SeriesLabels>,
    labels: MatchingLabels,
    result_labels: usize,
) -> Result<Case> {
    let mut lhs = Vec::with_capacity(SERIES);
    let mut rhs = Vec::with_capacity(SERIES);
    for index in 0..SERIES {
        let metric = metric_of(index)?;
        lhs.push(build_series(schema, grid, metric.clone(), |point| {
            lhs_value(index, point)
        })?);
        rhs.push(build_series(schema, grid, metric, |point| {
            rhs_value(index, point)
        })?);
    }
    Ok(Case {
        name,
        matching: BinaryMatching {
            op: BinaryOp::Add,
            return_bool: false,
            labels,
            cardinality: Cardinality::OneToOne,
            fill: FillValues::default(),
        },
        schema: schema.clone(),
        lhs,
        rhs,
        one_index: paired,
        result_series: SERIES,
        result_labels,
        filed_points: SERIES * POINTS,
    })
}

/// A `group_left` case: `SERIES` left-hand series sharing `GROUPS` match groups, matched by `code`
/// against the `GROUPS` right-hand series that carry it.
fn build_many_to_one(
    name: &'static str,
    grid: &Arc<EvalGrid>,
    schema: &SeriesSchema,
) -> Result<Case> {
    let mut lhs = Vec::with_capacity(SERIES);
    for index in 0..SERIES {
        lhs.push(build_series(schema, grid, wide_metric(index)?, |point| {
            lhs_value(index, point)
        })?);
    }
    let mut rhs = Vec::with_capacity(GROUPS);
    for code in 0..GROUPS {
        rhs.push(build_series(schema, grid, code_metric(code)?, |point| {
            rhs_value(code, point)
        })?);
    }
    Ok(Case {
        name,
        matching: BinaryMatching {
            op: BinaryOp::Add,
            return_bool: false,
            labels: MatchingLabels::On(vec![Arc::from("code")]),
            cardinality: Cardinality::ManyToOne(Vec::new()),
            fill: FillValues::default(),
        },
        schema: schema.clone(),
        lhs,
        rhs,
        one_index: code_of,
        result_series: SERIES,
        result_labels: 8,
        filed_points: SERIES * POINTS,
    })
}

/// Runs the kernel once over the case's operands.
fn run_binary_vectors(case: &Case) -> Result<Vec<Series>> {
    binary_vectors(
        &case.matching,
        &case.schema,
        &case.lhs,
        &case.schema,
        &case.rhs,
    )
    .map_err(boxed)
}

/// Checks one call's result against the case's shape and the operator's own arithmetic: every
/// result label set, every label it keeps, every grid point and every value.
fn check_case(case: &Case, results: &[Series]) -> Result<()> {
    if results.len() != case.result_series {
        return Err(format!(
            "{}: {} result series, expected {}",
            case.name,
            results.len(),
            case.result_series
        )
        .into());
    }
    let mut filed = 0usize;
    let mut checksum = 0.0f64;
    for result in results {
        if result.metric.name().is_some() {
            return Err(format!("{}: an arithmetic result kept __name__", case.name).into());
        }
        if result.metric.tags().len() != case.result_labels {
            return Err(format!(
                "{}: a result label set carries {} labels, expected {}",
                case.name,
                result.metric.tags().len(),
                case.result_labels
            )
            .into());
        }
        let instance = result
            .metric
            .get("instance")
            .ok_or_else(|| format!("{}: a result label set has no instance label", case.name))?;
        let index = instance
            .strip_prefix("inst-")
            .and_then(|rest| rest.parse::<usize>().ok())
            .filter(|index| *index < SERIES)
            .ok_or_else(|| format!("{}: unreadable instance label {instance}", case.name))?;
        if result.time.len() != POINTS {
            return Err(format!(
                "{}: a result series carries {} rows, expected {POINTS}",
                case.name,
                result.time.len()
            )
            .into());
        }
        let field = result
            .fields
            .as_slice()
            .first()
            .ok_or_else(|| format!("{}: a result series has no field", case.name))?;
        for (point, sample) in field.iter().enumerate() {
            let SampleRef::Float(value) = sample else {
                return Err(format!(
                    "{}: {instance} at point {point} is not a float sample",
                    case.name
                )
                .into());
            };
            let expected = expected_value(index, point, case.one_index);
            if value != expected {
                return Err(format!(
                    "{}: {instance} at point {point} is {value}, expected {expected}",
                    case.name
                )
                .into());
            }
            filed += 1;
            checksum += value;
        }
    }
    black_box(checksum);
    if filed != case.filed_points {
        return Err(format!(
            "{}: filed {filed} points, expected {}",
            case.name, case.filed_points
        )
        .into());
    }
    Ok(())
}

/// Runs `case` once and panics unless the kernel answered with exactly the expected rows and
/// values, so a wrongly built fixture cannot produce a benchmark number.
fn verified(case: &Case) {
    let results = run_binary_vectors(case).expect("the fixture is a valid binary operation");
    check_case(case, &results).expect("the kernel answers the fixture correctly");
}

fn benches(c: &mut Criterion) {
    let grid = build_grid().expect("a valid grid");
    let schema = build_schema(&grid);
    let cases = [
        build_one_to_one(
            "one_to_one_1_label",
            &grid,
            &schema,
            narrow_metric,
            MatchingLabels::On(vec![Arc::from("instance")]),
            1,
        )
        .expect("a valid narrow case"),
        build_one_to_one(
            "one_to_one_8_labels",
            &grid,
            &schema,
            wide_metric,
            MatchingLabels::Ignoring(Vec::new()),
            8,
        )
        .expect("a valid wide case"),
        build_many_to_one("group_left_many_to_one", &grid, &schema)
            .expect("a valid many-to-one case"),
    ];

    let mut group = c.benchmark_group("vector_match_binary");
    for case in &cases {
        verified(case);
        group.throughput(Throughput::Elements(case.filed_points as u64));
        group.bench_function(case.name, |b| {
            b.iter(|| black_box(run_binary_vectors(case).expect("a valid binary operation")));
        });
    }
    group.finish();
}

criterion_group!(benches_group, benches);
