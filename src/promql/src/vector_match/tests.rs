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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, UInt32Array};
use datafusion::error::DataFusionError;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{
    FieldSpec, HistogramSamples, RowCursor, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels, RESULT_ROW_BYTES, SetOp,
    binary_vectors, binary_vectors_with_reservation, set_vectors,
};
use crate::grid_series::reference::{
    Dense, OwnedHistogram, Points, assert_histogram_close, assert_result_eq, golden_cases,
    mixed_series, piece_ranges, random_dense, run_ranges, sparse_series,
};
use crate::grid_series::{GridSample, GridSeriesBuilder};
use crate::histogram_eval::float_histogram::FloatHistogram;

/// Labels with `__name__` as an ordinary entry, the way Prometheus holds them.
type Labels = BTreeMap<String, String>;

const OPS: [BinaryOp; 13] = [
    BinaryOp::Add,
    BinaryOp::Sub,
    BinaryOp::Mul,
    BinaryOp::Div,
    BinaryOp::Mod,
    BinaryOp::Pow,
    BinaryOp::Atan2,
    BinaryOp::Eq,
    BinaryOp::Ne,
    BinaryOp::Gt,
    BinaryOp::Lt,
    BinaryOp::Ge,
    BinaryOp::Le,
];

const LABEL_NAMES: [&str; 4] = ["__name__", "host", "job", "zone"];

fn schema(fields: usize, grid: &Arc<EvalGrid>) -> SeriesSchema {
    SeriesSchema {
        fields: (0..fields)
            .map(|index| FieldSpec {
                name: Arc::from(format!("f{index}")),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: Some(grid.clone()),
    }
}

fn metric(labels: &Labels) -> MetricName {
    let mut labels = labels.clone();
    let name = labels.remove("__name__").map(Arc::from);
    MetricName::try_new(
        name,
        labels
            .into_iter()
            .map(|(name, value)| Label {
                name: Arc::from(name),
                value: Arc::from(value),
            })
            .collect(),
    )
    .unwrap()
}

fn labels_of(metric: &MetricName) -> Labels {
    let mut labels: Labels = metric
        .tags()
        .iter()
        .map(|tag| (tag.name.to_string(), tag.value.to_string()))
        .collect();
    if let Some(name) = metric.name() {
        labels.insert("__name__".to_string(), name.to_string());
    }
    labels
}

/// One operand: distinct label sets with their dense values.
struct Side {
    schema: SeriesSchema,
    series: Vec<Series>,
    dense: Vec<(Labels, Dense)>,
}

fn random_side(rng: &mut StdRng, grid: &Arc<EvalGrid>, fields: usize) -> Side {
    let schema = schema(fields, grid);
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone())).unwrap();
    let mut seen = HashSet::new();
    let mut series = Vec::new();
    let mut dense = Vec::new();
    for _ in 0..rng.random_range(0..6) {
        let mut labels = Labels::new();
        for (name, values) in [
            ("__name__", ["m1", "m2", ""]),
            ("host", ["a", "b", ""]),
            ("job", ["x", "y", "y"]),
            ("zone", ["z1", "", ""]),
            ("__type__", ["counter", "", ""]),
        ] {
            let value = values[rng.random_range(0..values.len())];
            if !value.is_empty() {
                labels.insert(name.to_string(), value.to_string());
            }
        }
        if !seen.insert(labels.clone()) {
            continue;
        }
        // Each label set starts its own series: only `begin_series` resets the builder.
        builder.begin_series();
        let values = random_dense(rng, grid.timestamps().len(), fields);
        for (index, row) in values.iter().enumerate() {
            builder.push(index, row).unwrap();
        }
        // Random rows can hold holes, so the series carries one run per stretch of valued
        // positions: together they cover exactly those positions, which is what the dense matrix
        // behind the reference holds.
        series.extend(builder.finish(metric(&labels)).unwrap());
        dense.push((labels, values));
    }
    Side {
        schema,
        series,
        dense,
    }
}

fn random_names(rng: &mut StdRng, candidates: &[&str]) -> Vec<Arc<str>> {
    candidates
        .iter()
        .filter(|_| rng.random_bool(0.4))
        .map(|name| Arc::from(*name))
        .collect()
}

fn random_labels(rng: &mut StdRng) -> MatchingLabels {
    if rng.random_bool(0.5) {
        MatchingLabels::On(random_names(rng, &LABEL_NAMES))
    } else {
        MatchingLabels::Ignoring(random_names(rng, &LABEL_NAMES))
    }
}

fn listed(names: &[Arc<str>], name: &str) -> bool {
    names.iter().any(|listed| &**listed == name)
}

/// Prometheus `signatureFunc`: `on` keeps the listed labels, `ignoring` every label except the
/// listed ones and `__name__`.
fn signature(labels: &Labels, matching: &MatchingLabels) -> Labels {
    labels
        .iter()
        .filter(|(name, _)| match matching {
            MatchingLabels::On(names) => listed(names, name),
            MatchingLabels::Ignoring(names) => name.as_str() != "__name__" && !listed(names, name),
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Prometheus `resultMetric`.
fn result_labels(many: &Labels, one: &Labels, matching: &BinaryMatching) -> Labels {
    let mut labels = many.clone();
    if matching.return_bool || !matching.op.is_comparison() {
        for name in ["__name__", "__type__", "__unit__"] {
            labels.remove(name);
        }
    }
    let include = match &matching.cardinality {
        Cardinality::OneToOne => {
            match &matching.labels {
                MatchingLabels::On(names) => labels.retain(|name, _| listed(names, name)),
                MatchingLabels::Ignoring(names) => labels.retain(|name, _| !listed(names, name)),
            }
            &[][..]
        }
        Cardinality::ManyToOne(include) | Cardinality::OneToMany(include) => &include[..],
    };
    for name in include {
        match one.get(&**name) {
            Some(value) => labels.insert(name.to_string(), value.clone()),
            None => labels.remove(&**name),
        };
    }
    labels
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Violation {
    DuplicateOneSide,
    ImplicitManyToOne,
    Grouping,
    DuplicateLabelSet,
}

fn violation_of(error: &DataFusionError) -> Violation {
    let message = error.to_string();
    if message.contains("duplicate series for the match group") {
        Violation::DuplicateOneSide
    } else if message.contains("many-to-one matching must be explicit") {
        Violation::ImplicitManyToOne
    } else if message.contains("grouping labels must ensure unique matches") {
        Violation::Grouping
    } else if message.contains("same labelset") {
        Violation::DuplicateLabelSet
    } else {
        panic!("unexpected error {message}")
    }
}

type Expected = Result<BTreeMap<Labels, Dense>, BTreeSet<Violation>>;

fn present(row: &[Option<f64>]) -> bool {
    row.iter().any(Option::is_some)
}

fn present_at(side: &[(Labels, Dense)], point: usize) -> Vec<&(Labels, Dense)> {
    side.iter()
        .filter(|(_, dense)| present(&dense[point]))
        .collect()
}

/// Field `field` of every series of `side`, as single-field series.
fn project(side: &Side, field: usize) -> Vec<(Labels, Dense)> {
    side.dense
        .iter()
        .map(|(labels, dense)| {
            let column = dense.iter().map(|row| vec![row[field]]).collect();
            (labels.clone(), column)
        })
        .collect()
}

/// Evaluates `reference` on each field projection and joins the results by label set.
fn per_field(
    fields: usize,
    points: usize,
    mut reference: impl FnMut(usize) -> Expected,
) -> Expected {
    let mut output: BTreeMap<Labels, Dense> = BTreeMap::new();
    let mut violations = BTreeSet::new();
    for field in 0..fields {
        match reference(field) {
            Ok(results) => {
                for (labels, dense) in results {
                    let target = output
                        .entry(labels)
                        .or_insert_with(|| vec![vec![None; fields]; points]);
                    for (row, value) in target.iter_mut().zip(dense) {
                        row[field] = value[0];
                    }
                }
            }
            Err(found) => violations.extend(found),
        }
    }
    if violations.is_empty() {
        Ok(output)
    } else {
        Err(violations)
    }
}

/// Prometheus `VectorBinop` on single-field operands, evaluated on its own at every point,
/// collecting every violation instead of stopping at the first.
fn reference_binary(
    matching: &BinaryMatching,
    lhs: &[(Labels, Dense)],
    rhs: &[(Labels, Dense)],
    points: usize,
) -> Expected {
    let fields = 1;
    let mut output: BTreeMap<Labels, Dense> = BTreeMap::new();
    let mut violations = BTreeSet::new();
    let swapped = matches!(matching.cardinality, Cardinality::OneToMany(_));
    for point in 0..points {
        let left = present_at(lhs, point);
        let right = present_at(rhs, point);
        if left.is_empty() || right.is_empty() {
            continue;
        }
        let (many, one) = if swapped {
            (&right, &left)
        } else {
            (&left, &right)
        };
        let mut one_by_signature = HashMap::new();
        let mut duplicate = false;
        for (labels, dense) in one {
            duplicate |= one_by_signature
                .insert(signature(labels, &matching.labels), (labels, dense))
                .is_some();
        }
        if duplicate {
            violations.insert(Violation::DuplicateOneSide);
            continue;
        }
        let mut matched = HashSet::new();
        let mut inserted: HashMap<Labels, HashSet<Labels>> = HashMap::new();
        for (many_labels, many_dense) in many {
            let signature = signature(many_labels, &matching.labels);
            let Some((one_labels, one_dense)) = one_by_signature.get(&signature) else {
                continue;
            };
            let result = result_labels(many_labels, one_labels, matching);
            if matching.cardinality == Cardinality::OneToOne {
                if !matched.insert(signature.clone()) {
                    violations.insert(Violation::ImplicitManyToOne);
                    continue;
                }
            } else if !inserted
                .entry(signature.clone())
                .or_default()
                .insert(result.clone())
            {
                violations.insert(Violation::Grouping);
                continue;
            }
            let (lhs_row, rhs_row) = if swapped {
                (&one_dense[point], &many_dense[point])
            } else {
                (&many_dense[point], &one_dense[point])
            };
            let row: Vec<_> = (0..fields)
                .map(|field| {
                    let (value, holds) = matching.op.apply(lhs_row[field]?, rhs_row[field]?);
                    if !matching.op.is_comparison() {
                        Some(value)
                    } else if matching.return_bool {
                        Some(if holds { 1.0 } else { 0.0 })
                    } else {
                        holds.then_some(value)
                    }
                })
                .collect();
            let dense = output
                .entry(result)
                .or_insert_with(|| vec![vec![None; fields]; points]);
            // Prometheus rejects the step once the operator is done.
            if present(&dense[point]) {
                violations.insert(Violation::DuplicateLabelSet);
                continue;
            }
            dense[point] = row;
        }
    }
    if violations.is_empty() {
        Ok(output)
    } else {
        Err(violations)
    }
}

/// Prometheus `VectorAnd`, `VectorOr` and `VectorUnless` on single-field operands at every
/// point.
fn reference_set(
    op: SetOp,
    labels: &MatchingLabels,
    lhs: &[(Labels, Dense)],
    rhs: &[(Labels, Dense)],
    points: usize,
) -> Expected {
    let fields = 1;
    let mut output: BTreeMap<Labels, Dense> = BTreeMap::new();
    let mut violations = BTreeSet::new();
    for point in 0..points {
        let left = present_at(lhs, point);
        let right = present_at(rhs, point);
        let signatures = |side: &[&(Labels, Dense)]| -> HashSet<Labels> {
            side.iter().map(|(l, _)| signature(l, labels)).collect()
        };
        let (left_signatures, right_signatures) = (signatures(&left), signatures(&right));
        let kept: Vec<_> = match op {
            SetOp::And => left
                .into_iter()
                .filter(|(l, _)| right_signatures.contains(&signature(l, labels)))
                .collect(),
            SetOp::Unless => left
                .into_iter()
                .filter(|(l, _)| !right_signatures.contains(&signature(l, labels)))
                .collect(),
            SetOp::Or => left
                .into_iter()
                .chain(
                    right
                        .into_iter()
                        .filter(|(l, _)| !left_signatures.contains(&signature(l, labels))),
                )
                .collect(),
        };
        for (labels, dense) in kept {
            let target = output
                .entry(labels.clone())
                .or_insert_with(|| vec![vec![None; fields]; points]);
            if present(&target[point]) {
                violations.insert(Violation::DuplicateLabelSet);
            }
            target[point] = dense[point].clone();
        }
    }
    if violations.is_empty() {
        Ok(output)
    } else {
        Err(violations)
    }
}

fn assert_output(actual: Result<Vec<Series>, DataFusionError>, expected: Expected, seed: u64) {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            // A result arrives as one piece per run of valued positions; the reference is dense,
            // so the pieces of each label set are folded back together first.
            let actual: BTreeMap<Labels, Series> = crate::golden::fold_pieces(actual)
                .into_iter()
                .map(|series| (labels_of(&series.metric), series))
                .collect();
            let expected: BTreeMap<_, _> = expected
                .into_iter()
                .filter(|(_, dense)| dense.iter().any(|row| present(row)))
                .collect();
            assert_eq!(
                actual.keys().collect::<Vec<_>>(),
                expected.keys().collect::<Vec<_>>(),
                "seed {seed}"
            );
            for (labels, dense) in &expected {
                assert_result_eq(actual.get(labels), dense, &metric(labels));
            }
        }
        (Err(error), Err(violations)) => assert!(
            violations.contains(&violation_of(&error)),
            "seed {seed}: {error} not in {violations:?}"
        ),
        (actual, expected) => panic!("seed {seed}: got {actual:?}, expected {expected:?}"),
    }
}

fn random_grid(rng: &mut StdRng) -> Arc<EvalGrid> {
    let spec = GridSpec::try_new(rng.random_range(-1_000..1_000), 10, rng.random_range(1..40));
    Arc::new(EvalGrid::try_new(spec.unwrap()).unwrap())
}

#[test]
fn binary_vectors_match_prometheus_reference() {
    let (mut matched, mut failed) = (0, 0);
    for seed in 0..3_000 {
        let mut rng = StdRng::seed_from_u64(seed);
        let grid = random_grid(&mut rng);
        let lhs_fields = rng.random_range(1..=2);
        let lhs = random_side(&mut rng, &grid, lhs_fields);
        let rhs_fields = rng.random_range(1..=2);
        let rhs = random_side(&mut rng, &grid, rhs_fields);
        let op = OPS[rng.random_range(0..OPS.len())];
        let return_bool = op.is_comparison() && rng.random_bool(0.5);
        let labels = random_labels(&mut rng);
        let include = random_names(&mut rng, &["host", "job", "zone"]);
        let cardinality = match rng.random_range(0..3) {
            0 => Cardinality::OneToOne,
            1 => Cardinality::ManyToOne(include),
            _ => Cardinality::OneToMany(include),
        };
        let matching = BinaryMatching {
            op,
            return_bool,
            labels,
            cardinality,
            fill: FillValues::default(),
        };
        let fields = lhs_fields.min(rhs_fields);
        let points = grid.timestamps().len();
        let expected = per_field(fields, points, |field| {
            reference_binary(
                &matching,
                &project(&lhs, field),
                &project(&rhs, field),
                points,
            )
        });
        if expected.is_ok() {
            matched += 1;
        } else {
            failed += 1;
        }
        let actual = binary_vectors(
            &matching,
            &lhs.schema,
            &lhs.series,
            &rhs.schema,
            &rhs.series,
        );
        // The budgeted entry runs the same kernel: it admits the live workspace it holds, asking
        // again only when that figure changes. Every request is a different figure, because a step
        // replaces the one before it instead of adding to it.
        let (reserved, seen) = budgeted(
            &matching,
            &lhs.schema,
            &lhs.series,
            &rhs.schema,
            &rhs.series,
            usize::MAX,
        );
        assert!(
            seen.windows(2).all(|pair| pair[0] != pair[1]),
            "seed {seed}: {seen:?}"
        );
        // Both entries are held to the reference rather than to each other's error text: two
        // label sets failing at one point are reported in the order the result table walks them,
        // which is a hash order, so the message can name either. The class of failure is what a
        // run must share with the other entry, and `assert_output` checks it against the reference.
        match (&reserved, &actual) {
            (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
            (reserved, actual) => {
                panic!("seed {seed}: the entries disagree on failing: {reserved:?} vs {actual:?}")
            }
        }
        if let (Err(reserved), Err(actual)) = (&reserved, &actual) {
            assert_eq!(
                violation_of(reserved),
                violation_of(actual),
                "seed {seed}: {reserved} vs {actual}"
            );
        }
        assert_output(reserved, expected.clone(), seed);
        assert_output(actual, expected, seed);
    }
    assert!(
        matched > 1_000 && failed > 100,
        "{matched} matched, {failed} failed"
    );
}

#[test]
fn set_vectors_match_prometheus_reference() {
    for seed in 0..3_000 {
        let mut rng = StdRng::seed_from_u64(seed);
        let grid = random_grid(&mut rng);
        let fields = rng.random_range(1..=2);
        let lhs = random_side(&mut rng, &grid, fields);
        let rhs = random_side(&mut rng, &grid, fields);
        let op = [SetOp::And, SetOp::Or, SetOp::Unless][rng.random_range(0..3)];
        let labels = random_labels(&mut rng);
        let points = grid.timestamps().len();
        let expected = per_field(fields, points, |field| {
            reference_set(
                op,
                &labels,
                &project(&lhs, field),
                &project(&rhs, field),
                points,
            )
        });
        let actual = set_vectors(
            op,
            &labels,
            &lhs.schema,
            &lhs.series,
            &rhs.schema,
            &rhs.series,
        );
        assert_output(actual, expected, seed);
    }
}

#[test]
fn rejects_operands_on_other_grids_and_unpaired_or_fields() {
    let mut rng = StdRng::seed_from_u64(0);
    let grid = random_grid(&mut rng);
    let other = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 7, 3).unwrap()).unwrap());
    let lhs = random_side(&mut rng, &grid, 1);
    let matching = BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    };
    assert!(binary_vectors(&matching, &lhs.schema, &lhs.series, &schema(1, &other), &[]).is_err());
    assert!(matches!(
        set_vectors(
            SetOp::Or,
            &matching.labels,
            &lhs.schema,
            &lhs.series,
            &schema(2, &grid),
            &[]
        ),
        Err(DataFusionError::NotImplemented(_))
    ));
}

/// Prometheus fails at the first failing step: a grouping violation at the first point wins
/// over a duplicate on the one side at the second, whichever groups fail later.
#[test]
fn reports_the_earliest_failing_point() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(100_000, 10_000, 3).unwrap()).unwrap());
    let schema = schema(1, &grid);
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone())).unwrap();
    let mut series = |labels: &[(&str, &str)], points: &[usize]| {
        // One builder serves every operand series, so each one starts explicitly.
        builder.begin_series();
        for &point in points {
            builder.push(point, &[Some(point as f64)]).unwrap();
        }
        let labels = labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        builder
            .finish(metric(&labels))
            .unwrap()
            .expect("each call pushes one run")
    };
    let mut lhs = vec![
        series(&[("__name__", "a"), ("job", "z")], &[0]),
        series(&[("__name__", "b"), ("job", "z")], &[0]),
        series(&[("__name__", "a"), ("job", "w")], &[1]),
    ];
    let mut rhs = vec![
        series(&[("__name__", "c"), ("job", "z")], &[0]),
        series(&[("__name__", "c"), ("job", "x"), ("host", "h1")], &[1]),
        series(&[("__name__", "c"), ("job", "x"), ("host", "h2")], &[1]),
    ];
    // Many later failures, so a search that stops at the first failing group in hash order
    // almost never lands on the earliest one.
    for job in (0..32).map(|job| format!("y{job}")) {
        lhs.push(series(&[("__name__", "a"), ("job", &job)], &[2]));
        lhs.push(series(&[("__name__", "b"), ("job", &job)], &[2]));
        rhs.push(series(&[("__name__", "c"), ("job", &job)], &[2]));
    }
    let matching = BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::On(vec![Arc::from("job")]),
        cardinality: Cardinality::ManyToOne(Vec::new()),
        fill: FillValues::default(),
    };
    let error = binary_vectors(&matching, &schema, &lhs, &schema, &rhs)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("at 100000 ms: grouping labels must ensure unique matches"),
        "{error}"
    );
}

/// Values of `series` by grid point, any NaN as the same NaN: Prometheus' NaN payloads depend
/// on how the Go compiler orders commutative operands.
fn points_of(series: &Series) -> BTreeMap<usize, u64> {
    if series.time.grid().is_none() {
        panic!("expected a grid series, got {:?}", series.time);
    }
    let mut cursor = series.fields.as_slice()[0].cursor();
    series
        .time
        .positions()
        .filter_map(|(point, row)| {
            match cursor.at(row).expect("the axis and the fields are aligned") {
                SampleRef::Float(value) => Some((point, canonical_bits(value))),
                _ => None,
            }
        })
        .collect()
}

fn canonical_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

/// Binary and set operations evaluated by the Prometheus v3.15.0 engine; see the header of
/// `prometheus_cases.txt`.
#[test]
fn vector_operations_match_prometheus() {
    let (matched, failed) = check_cases(include_str!("prometheus_cases.txt"));
    assert!(
        matched > 100 && failed > 20,
        "{matched} matched, {failed} failed"
    );
}

/// Binary operations with fill modifiers, evaluated by the Prometheus v3.15.0 engine; see the
/// header of `fill_cases.txt`.
#[test]
fn fill_modifiers_match_prometheus() {
    let (matched, failed) = check_cases(include_str!("fill_cases.txt"));
    assert!(
        matched > 100 && failed > 20,
        "{matched} matched, {failed} failed"
    );
}

/// Checks each case of a fixture and returns how many matched results and how many failed.
fn check_cases(text: &str) -> (usize, usize) {
    let parse_labels = |labels: &str| -> Labels {
        labels
            .split(',')
            .filter(|pair| *pair != "-")
            .map(|pair| {
                let (name, value) = pair.split_once('=').unwrap();
                (name.to_string(), value.to_string())
            })
            .collect()
    };
    let names = |list: &str| -> Vec<Arc<str>> {
        list.split(',')
            .filter(|name| *name != "-")
            .map(Arc::from)
            .collect()
    };
    let (mut matched, mut failed) = (0, 0);
    for case in text.split("end\n").filter(|case| case.contains("case ")) {
        let lines: Vec<&str> = case.lines().filter(|line| !line.starts_with('#')).collect();
        let spec: Vec<&str> = lines[0].split_whitespace().skip(1).collect();
        let points: u64 = lines[1].split_whitespace().nth(3).unwrap().parse().unwrap();
        let grid = Arc::new(
            EvalGrid::try_new(GridSpec::try_new(100_000, 10_000, points).unwrap()).unwrap(),
        );
        let schema = schema(1, &grid);
        let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone())).unwrap();
        let (mut lhs, mut rhs) = (Vec::new(), Vec::new());
        let mut expected: BTreeMap<Labels, BTreeMap<usize, u64>> = BTreeMap::new();
        let mut error = None;
        for line in &lines[2..] {
            if let Some(message) = line.strip_prefix("error ") {
                error = Some(message.to_string());
                continue;
            }
            let (kind, rest) = line.split_once(' ').unwrap();
            let (labels, values) = rest.split_once(" ;").unwrap();
            let labels = parse_labels(labels);
            let values: BTreeMap<usize, u64> = values
                .split_whitespace()
                .map(|point| {
                    let (index, bits) = point.split_once(':').unwrap();
                    (
                        index.parse().unwrap(),
                        u64::from_str_radix(bits, 16).unwrap(),
                    )
                })
                .collect();
            match kind {
                "series" => {
                    builder.begin_series();
                    for point in 0..points as usize {
                        let value = values.get(&point).map(|bits| f64::from_bits(*bits));
                        builder.push(point, &[value]).unwrap();
                    }
                    // A series line can hold holes, so the result carries one run per stretch of
                    // valued positions, all of them in the one series.
                    let side = if labels.get("__name__").map(String::as_str) == Some("a") {
                        &mut lhs
                    } else {
                        &mut rhs
                    };
                    side.extend(builder.finish(metric(&labels)).unwrap());
                }
                "result" => {
                    let values = values
                        .into_iter()
                        .map(|(point, bits)| (point, canonical_bits(f64::from_bits(bits))))
                        .collect();
                    expected.insert(labels, values);
                }
                other => panic!("unexpected line {other}"),
            }
        }
        let fill = |value: Option<&&str>| match value {
            None | Some(&"-") => None,
            Some(value) => Some(value.parse::<f64>().unwrap()),
        };
        let labels = |kind: &str, list: &str| match kind {
            "on" => MatchingLabels::On(names(list)),
            _ => MatchingLabels::Ignoring(names(list)),
        };
        let actual = match spec[0] {
            "binary" => {
                let op = match spec[1] {
                    "+" => BinaryOp::Add,
                    "-" => BinaryOp::Sub,
                    "*" => BinaryOp::Mul,
                    "/" => BinaryOp::Div,
                    "%" => BinaryOp::Mod,
                    "^" => BinaryOp::Pow,
                    "atan2" => BinaryOp::Atan2,
                    "==" => BinaryOp::Eq,
                    "!=" => BinaryOp::Ne,
                    ">" => BinaryOp::Gt,
                    "<" => BinaryOp::Lt,
                    ">=" => BinaryOp::Ge,
                    "<=" => BinaryOp::Le,
                    other => panic!("unknown operator {other}"),
                };
                let matching = BinaryMatching {
                    op,
                    return_bool: spec[2] == "true",
                    labels: labels(spec[3], spec[4]),
                    cardinality: match spec[5] {
                        "left" => Cardinality::ManyToOne(names(spec[6])),
                        "right" => Cardinality::OneToMany(names(spec[6])),
                        _ => Cardinality::OneToOne,
                    },
                    fill: FillValues {
                        lhs: fill(spec.get(7)),
                        rhs: fill(spec.get(8)),
                    },
                };
                binary_vectors(&matching, &schema, &lhs, &schema, &rhs)
            }
            "set" => {
                let op = match spec[1] {
                    "and" => SetOp::And,
                    "or" => SetOp::Or,
                    _ => SetOp::Unless,
                };
                set_vectors(op, &labels(spec[2], spec[3]), &schema, &lhs, &schema, &rhs)
            }
            other => panic!("unknown case {other}"),
        };
        match (actual, error) {
            (Ok(actual), None) => {
                // A label set arrives as one piece per run of valued positions; the values of all
                // its pieces together are the series Prometheus evaluated at those points.
                let mut folded: BTreeMap<Labels, BTreeMap<usize, u64>> = BTreeMap::new();
                for series in &actual {
                    let points = folded.entry(labels_of(&series.metric)).or_default();
                    for (point, bits) in points_of(series) {
                        assert!(
                            points.insert(point, bits).is_none(),
                            "{case}: {point} handed out twice"
                        );
                    }
                }
                assert_close(&folded, &expected, case);
                matched += 1;
            }
            (Err(actual), Some(expected)) => {
                let class = |message: &str| {
                    [
                        "found duplicate series for the match group",
                        "many-to-one matching must be explicit",
                        "grouping labels must ensure unique matches",
                    ]
                    .into_iter()
                    .find(|class| message.contains(class))
                };
                assert_eq!(class(&actual.to_string()), class(&expected), "{case}");
                failed += 1;
            }
            (actual, expected) => panic!("{case}\ngot {actual:?}, expected error {expected:?}"),
        }
    }
    (matched, failed)
}

/// Results that share a label set once `__type__` is dropped fail as a grouping violation within
/// one match group, and as duplicate label sets across match groups, which Prometheus detects
/// only after the operator.
#[test]
fn colliding_results_fail_by_where_they_collide() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 10_000, 1).unwrap()).unwrap());
    let schema = schema(1, &grid);
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone())).unwrap();
    let mut series = |labels: &[(&str, &str)]| {
        builder.begin_series();
        builder.push(0, &[Some(1.0)]).unwrap();
        let labels = labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        builder
            .finish(metric(&labels))
            .unwrap()
            .expect("each call pushes one point")
    };
    let lhs = vec![
        series(&[("__name__", "a"), ("__type__", "counter"), ("job", "x")]),
        series(&[("__name__", "a"), ("__type__", "gauge"), ("job", "x")]),
    ];
    let rhs = [
        series(&[("__name__", "b"), ("__type__", "counter"), ("job", "x")]),
        series(&[("__name__", "b"), ("__type__", "gauge"), ("job", "x")]),
    ];
    for (labels, expected) in [
        // `__type__` is part of the match group.
        (MatchingLabels::Ignoring(Vec::new()), "same labelset"),
        // Both left series fall into the group of each right series.
        (
            MatchingLabels::On(vec![Arc::from("job")]),
            "grouping labels must ensure unique matches",
        ),
    ] {
        let rhs = match &labels {
            MatchingLabels::On(_) => &rhs[..1],
            MatchingLabels::Ignoring(_) => &rhs[..],
        };
        let matching = BinaryMatching {
            op: BinaryOp::Add,
            return_bool: false,
            labels,
            cardinality: Cardinality::ManyToOne(Vec::new()),
            fill: FillValues::default(),
        };
        let error = binary_vectors(&matching, &schema, &lhs, &schema, rhs)
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }
}

/// Prometheus `almost.Equal` with relative tolerance 1e-10, treating results below 1e-10 as
/// equal and any NaN as equal to NaN, as promql-compat compares engines.
fn close(a: f64, b: f64) -> bool {
    const EPSILON: f64 = 1e-10;
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if a == b {
        return true;
    }
    let (diff, sum) = ((a - b).abs(), a.abs() + b.abs());
    let equal = if a == 0.0 || b == 0.0 || sum < f64::MIN_POSITIVE {
        diff < EPSILON * f64::MIN_POSITIVE
    } else {
        diff / sum.min(f64::MAX) < EPSILON
    };
    equal || (a.abs() < EPSILON && b.abs() < EPSILON)
}

fn assert_close(
    actual: &BTreeMap<Labels, BTreeMap<usize, u64>>,
    expected: &BTreeMap<Labels, BTreeMap<usize, u64>>,
    case: &str,
) {
    let points = |series: &BTreeMap<Labels, BTreeMap<usize, u64>>| {
        series
            .iter()
            .map(|(labels, points)| (labels.clone(), points.keys().copied().collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };
    assert_eq!(points(actual), points(expected), "{case}");
    for (labels, points) in actual {
        for (point, &bits) in points {
            let (got, want) = (
                f64::from_bits(bits),
                f64::from_bits(expected[labels][point]),
            );
            assert!(
                close(got, want),
                "{case}: {labels:?} at {point}: {got} vs {want}"
            );
        }
    }
}

/// Prometheus' `^` and `atan2` are Go's `math.Pow` and `math.Atan2`; the standard library
/// agrees within the comparison tolerance, special cases included.
#[test]
fn pow_and_atan2_match_go() {
    let mut checked = 0;
    for line in include_str!("go_math_cases.txt").lines() {
        if line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split(' ').collect();
        let value = |field: &str| f64::from_bits(u64::from_str_radix(field, 16).unwrap());
        let (a, b, expected) = (value(fields[1]), value(fields[2]), value(fields[3]));
        let actual = match fields[0] {
            "pow" => BinaryOp::Pow.apply(a, b).0,
            "atan2" => BinaryOp::Atan2.apply(a, b).0,
            function => panic!("unknown function {function}"),
        };
        assert!(close(actual, expected), "{line}: got {actual}");
        checked += 1;
    }
    assert_eq!(checked, 452);
}

/// Binary and set operators between vectors of floats and native histograms, evaluated by the
/// Prometheus v3.15.0 engine (`histogram_cases.txt`, promql-compat
/// `cmd/narthex-golden-native -binary`). Floats compare within 1e-10, histograms bucket by
/// bucket.
#[test]
fn histogram_operators_match_prometheus() {
    let (mut matched, mut failed, mut histograms) = (0, 0, 0);
    for case in golden_cases(include_str!("histogram_cases.txt")) {
        let schema = Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("value"),
            }]
            .into(),
            eval_grid: Some(case.grid.clone()),
        });
        let (mut lhs, mut rhs) = (Vec::new(), Vec::new());
        for ((labels, floats), texts) in case.inputs.iter().zip(&case.histograms) {
            let Some(series) = mixed_series(&schema, labels, floats, texts) else {
                continue;
            };
            if labels["__name__"] == "a" {
                lhs.push(series);
            } else {
                rhs.push(series);
            }
        }
        let spec: Vec<&str> = case.spec.iter().map(String::as_str).collect();
        let names: Vec<Arc<str>> = spec[3]
            .split(',')
            .filter(|name| *name != "-")
            .map(Arc::from)
            .collect();
        let labels = match spec[2] {
            "on" => MatchingLabels::On(names),
            _ => MatchingLabels::Ignoring(names),
        };
        let set_op = match spec[0] {
            "and" => Some(SetOp::And),
            "or" => Some(SetOp::Or),
            "unless" => Some(SetOp::Unless),
            _ => None,
        };
        let actual = match set_op {
            Some(op) => set_vectors(op, &labels, &schema, &lhs, &schema, &rhs),
            None => {
                let op = match spec[0] {
                    "+" => BinaryOp::Add,
                    "-" => BinaryOp::Sub,
                    "*" => BinaryOp::Mul,
                    "/" => BinaryOp::Div,
                    "%" => BinaryOp::Mod,
                    "^" => BinaryOp::Pow,
                    "atan2" => BinaryOp::Atan2,
                    "==" => BinaryOp::Eq,
                    "!=" => BinaryOp::Ne,
                    ">" => BinaryOp::Gt,
                    "<=" => BinaryOp::Le,
                    other => panic!("unknown operator {other}"),
                };
                let matching = BinaryMatching {
                    op,
                    return_bool: spec[1] == "bool",
                    labels,
                    cardinality: match spec[4] {
                        "left" => Cardinality::ManyToOne(Vec::new()),
                        "right" => Cardinality::OneToMany(Vec::new()),
                        _ => Cardinality::OneToOne,
                    },
                    fill: FillValues::default(),
                };
                binary_vectors(&matching, &schema, &lhs, &schema, &rhs)
            }
        };
        let context = format!("{:?}", case.spec);
        let expected = match (&case.outcome, actual) {
            (Err(expected), Err(actual)) => {
                let class = |message: &str| {
                    [
                        "found duplicate series for the match group",
                        "many-to-one matching must be explicit",
                        "grouping labels must ensure unique matches",
                        "same labelset",
                    ]
                    .into_iter()
                    .find(|class| message.contains(class))
                };
                assert_eq!(class(&actual.to_string()), class(expected), "{context}");
                failed += 1;
                continue;
            }
            (Ok(expected), Ok(actual)) => (expected, actual),
            (expected, actual) => panic!("{context}: got {actual:?}, expected {expected:?}"),
        };
        let (expected, actual) = expected;
        // A label set arrives as one piece per run of valued positions, so its pieces are gathered
        // before comparing: together they must cover exactly the points Prometheus reported.
        let mut folded: BTreeMap<Labels, Vec<Series>> = BTreeMap::new();
        for series in actual {
            folded
                .entry(labels_of(&series.metric))
                .or_default()
                .push(series);
        }
        assert_eq!(
            folded.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>(),
            "{context}"
        );
        let no_histograms = BTreeMap::new();
        for (labels, pieces) in &folded {
            let floats = &expected[labels];
            let texts = case.result_histograms.get(labels).unwrap_or(&no_histograms);
            let mut seen = BTreeSet::new();
            for series in pieces {
                if series.time.grid().is_none() {
                    panic!("expected a grid series, got {:?}", series.time);
                }
                let mut cursor = series.fields.as_slice()[0].cursor();
                for (point, row) in series.time.positions() {
                    let sample = cursor.at(row).expect("the axis and the fields are aligned");
                    match sample {
                        SampleRef::Missing => continue,
                        SampleRef::Float(value) => {
                            let want = f64::from_bits(floats[&point]);
                            assert!(
                                crate::grid_series::reference::close(value, want, 1e-10),
                                "{context} at {point}: {value} vs {want}"
                            );
                        }
                        SampleRef::Histogram(view) => {
                            let want = OwnedHistogram::parse(&texts[&point]);
                            assert_histogram_close(
                                &FloatHistogram::try_from_view(&view).unwrap(),
                                &FloatHistogram::try_from_view(&want.view()).unwrap(),
                                1e-10,
                                &format!("{context} at {point}"),
                            );
                            histograms += 1;
                        }
                    }
                    assert!(seen.insert(point), "{context}: {point} handed out twice");
                }
            }
            let wanted: BTreeSet<usize> = floats.keys().chain(texts.keys()).copied().collect();
            assert_eq!(seen, wanted, "{context}");
        }
        matched += 1;
    }
    assert!(
        matched > 150 && failed > 30 && histograms > 100,
        "{matched} matched, {failed} failed, {histograms} histograms"
    );
}

/// Prometheus skips a histogram `+` or `-` whose counter reset hints collide, before counting
/// the match: the colliding pair gives nothing and does not make the other pair of its group a
/// second match.
#[test]
fn colliding_reset_hints_skip_the_pair() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 10_000, 1).unwrap()).unwrap());
    let schema = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: Some(grid),
    });
    let series = |labels: &[(&str, &str)], hint: u8| {
        let labels: Labels = labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        let text =
            format!("0/0/0/4014000000000000/4014000000000000/{hint}/0_1/4014000000000000///");
        mixed_series(
            &schema,
            &labels,
            &BTreeMap::new(),
            &BTreeMap::from([(0, text)]),
        )
        .unwrap()
    };
    // Hints: 0 unknown, 1 counter reset, 2 not a counter reset.
    let lhs = [
        series(&[("__name__", "a"), ("instance", "1"), ("job", "x")], 1),
        series(&[("__name__", "a"), ("instance", "2"), ("job", "x")], 0),
    ];
    let rhs = [series(&[("__name__", "b"), ("job", "x")], 2)];
    for op in [BinaryOp::Add, BinaryOp::Sub] {
        let matching = BinaryMatching {
            op,
            return_bool: false,
            labels: MatchingLabels::On(vec![Arc::from("job")]),
            cardinality: Cardinality::OneToOne,
            fill: FillValues::default(),
        };
        let result = binary_vectors(&matching, &schema, &lhs, &schema, &rhs).unwrap();
        assert_eq!(result.len(), 1, "{op:?}");
    }
}

/// A pair whose operation fails is not a match, so `fill_left` still fills for the right-hand
/// sample of that pair; the fixtures hold floats only.
#[test]
fn a_failed_pair_leaves_its_sample_to_the_fill() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 10_000, 1).unwrap()).unwrap());
    let schema = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: Some(grid),
    });
    let labels = |pairs: &[(&str, &str)]| -> Labels {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    };
    let histogram = "0/0/0/4014000000000000/4014000000000000/0/0_1/4014000000000000///";
    let lhs = [mixed_series(
        &schema,
        &labels(&[("__name__", "a"), ("job", "x")]),
        &BTreeMap::new(),
        &BTreeMap::from([(0, histogram.to_string())]),
    )
    .unwrap()];
    let rhs = [mixed_series(
        &schema,
        &labels(&[("__name__", "b"), ("job", "x")]),
        &BTreeMap::from([(0, 2.0f64.to_bits())]),
        &BTreeMap::new(),
    )
    .unwrap()];
    // A histogram plus a float is skipped, so `b` is unmatched: 10 + 2 under the match labels.
    let matching = BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::On(vec![Arc::from("job")]),
        cardinality: Cardinality::OneToOne,
        fill: FillValues {
            lhs: Some(10.0),
            rhs: None,
        },
    };
    let result = binary_vectors(&matching, &schema, &lhs, &schema, &rhs).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(labels_of(&result[0].metric), labels(&[("job", "x")]));
    assert!(matches!(
        result[0].fields.as_slice()[0].sample(0),
        SampleRef::Float(value) if value == 12.0
    ));
}

/// A binary pairing walks two operands of many single-position runs and still reports every
/// position exactly once.
///
/// The rows of a pairing only increase -- `intersect_runs` merges the two run lists forward and
/// each shared run carries the row of each side -- which is what lets one cursor per operand serve
/// every run of the pairing. This pins that order and the values under heavy fragmentation; it
/// does not measure cost, since a cursor rebuilt per run returns the same values.
#[test]
fn a_pairing_reads_many_single_position_runs_once() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 10_000, 8).unwrap()).unwrap());
    let schema = Arc::new(schema(1, &grid));
    let points: Points = (0..8)
        .step_by(2)
        .map(|position| (position, f64::to_bits(position as f64)))
        .collect();
    let operand = |name: &str, offset: f64| -> Series {
        let labels = Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), "x".to_string()),
        ]);
        // Every other position is its own run: eight positions become four one-row runs.
        let points: Points = points
            .iter()
            .map(|(&position, &bits)| (position, (f64::from_bits(bits) + offset).to_bits()))
            .collect();
        sparse_series(&schema, &labels, &points).unwrap()
    };
    let lhs = operand("a", 0.0);
    let rhs = operand("b", 100.0);
    assert_eq!(run_ranges(&lhs), vec![(0, 1), (2, 1), (4, 1), (6, 1)]);
    assert_eq!(run_ranges(&rhs), vec![(0, 1), (2, 1), (4, 1), (6, 1)]);

    let matching = BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::On(vec![Arc::from("job")]),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    };
    let result = binary_vectors(
        &matching,
        &schema,
        std::slice::from_ref(&lhs),
        &schema,
        std::slice::from_ref(&rhs),
    )
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(run_ranges(&result[0]), vec![(0, 1), (2, 1), (4, 1), (6, 1)]);
    let expected: Points = (0..8)
        .step_by(2)
        .map(|position| (position, f64::to_bits((2 * position) as f64 + 100.0)))
        .collect();
    assert_eq!(points_of(&result[0]), expected);
}

/// F: a binary operation between two sparse operands evaluates only the shared positions, and its
/// output's runs are exactly those positions -- never the envelope of the pairing.
///
/// LHS carries 1 and 8, RHS carries 8 and 10, so the only shared position is 8. A pairing that
/// offset one side's rows by its first position would evaluate 1 against 8 and 8 against 10.
#[test]
fn a_binary_pairing_of_sparse_operands_keeps_the_shared_positions() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 12).unwrap()).unwrap());
    let schema = Arc::new(schema(1, &grid));
    let operand = |name: &str, points: &[(usize, f64)]| -> Series {
        let labels = Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), "x".to_string()),
        ]);
        let points: Points = points
            .iter()
            .map(|&(position, value)| (position, f64::to_bits(value)))
            .collect();
        sparse_series(&schema, &labels, &points).expect("the operand holds a value")
    };
    let lhs = operand("l", &[(1, 10.0), (8, 20.0)]);
    let rhs = operand("r", &[(8, 2.0), (10, 5.0)]);
    assert_eq!(run_ranges(&lhs), vec![(1, 1), (8, 1)]);
    assert_eq!(run_ranges(&rhs), vec![(8, 1), (10, 1)]);

    let matching = BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    };
    let result = binary_vectors(
        &matching,
        &schema,
        std::slice::from_ref(&lhs),
        &schema,
        std::slice::from_ref(&rhs),
    )
    .unwrap();
    assert_eq!(result.len(), 1, "one matched pair: {result:?}");
    assert_eq!(
        run_ranges(&result[0]),
        vec![(8, 1)],
        "only position 8 is shared, so the result holds exactly one row"
    );
    assert_eq!(result[0].time.len(), 1);
    let mut expected: Dense = vec![vec![None]; 12];
    expected[8] = vec![Some(22.0)];
    // Arithmetic drops the metric name, as Prometheus does.
    assert_result_eq(
        Some(&result[0]),
        &expected,
        &metric(&Labels::from([("job".to_string(), "x".to_string())])),
    );

    // A filtered comparison keeps the left value where it holds, at the same positions.
    let filtered = BinaryMatching {
        op: BinaryOp::Gt,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    };
    let result = binary_vectors(
        &filtered,
        &schema,
        std::slice::from_ref(&lhs),
        &schema,
        std::slice::from_ref(&rhs),
    )
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(run_ranges(&result[0]), vec![(8, 1)]);
    let mut expected: Dense = vec![vec![None]; 12];
    expected[8] = vec![Some(20.0)];
    // A filtered comparison keeps the left value and its metric, name included.
    assert_result_eq(
        Some(&result[0]),
        &expected,
        &metric(&Labels::from([
            ("__name__".to_string(), "l".to_string()),
            ("job".to_string(), "x".to_string()),
        ])),
    );

    // A `bool` comparison emits 0/1 at every shared position, so the axis is the intersection's.
    let booled = BinaryMatching {
        op: BinaryOp::Gt,
        return_bool: true,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    };
    let result = binary_vectors(
        &booled,
        &schema,
        std::slice::from_ref(&lhs),
        &schema,
        std::slice::from_ref(&rhs),
    )
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(run_ranges(&result[0]), vec![(8, 1)]);
    let mut expected: Dense = vec![vec![None]; 12];
    expected[8] = vec![Some(1.0)];
    assert_result_eq(
        Some(&result[0]),
        &expected,
        &metric(&Labels::from([("job".to_string(), "x".to_string())])),
    );
}

/// F: a set operator keeps each series' own positions, so a sparse operand comes back with the
/// runs it had and a dense one is cut to the sparse side's positions.
///
/// `and`: the positions both sides carry. `or`: the union. `unless`: the LHS positions the RHS
/// does not carry.
#[test]
fn set_operators_of_sparse_operands_keep_their_own_positions() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 12).unwrap()).unwrap());
    let schema = Arc::new(schema(1, &grid));
    let operand = |name: &str, points: &[(usize, f64)]| -> Series {
        let labels = Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), "x".to_string()),
        ]);
        let points: Points = points
            .iter()
            .map(|&(position, value)| (position, f64::to_bits(value)))
            .collect();
        sparse_series(&schema, &labels, &points).expect("the operand holds a value")
    };
    let lhs = operand("l", &[(1, 10.0), (5, 50.0), (9, 90.0)]);
    let rhs = operand("r", &[(3, 30.0), (5, 55.0)]);
    assert_eq!(run_ranges(&lhs), vec![(1, 1), (5, 1), (9, 1)]);
    assert_eq!(run_ranges(&rhs), vec![(3, 1), (5, 1)]);
    let labels = MatchingLabels::Ignoring(Vec::new());
    let input = |side: &[Series]| side.to_vec();

    // `and`: only position 5, where both sides hold a value.
    let result = set_vectors(
        SetOp::And,
        &labels,
        &schema,
        &input(std::slice::from_ref(&lhs)),
        &schema,
        &input(std::slice::from_ref(&rhs)),
    )
    .unwrap();
    assert_eq!(result.len(), 1, "an `and` keeps the left side: {result:?}");
    assert_eq!(run_ranges(&result[0]), vec![(5, 1)]);
    let mut expected: Dense = vec![vec![None]; 12];
    expected[5] = vec![Some(50.0)];
    assert_result_eq(Some(&result[0]), &expected, &lhs.metric);

    // `or`: both sides' positions, each with the value it had.
    let result = set_vectors(
        SetOp::Or,
        &labels,
        &schema,
        &input(std::slice::from_ref(&lhs)),
        &schema,
        &input(std::slice::from_ref(&rhs)),
    )
    .unwrap();
    assert_eq!(result.len(), 2, "an `or` keeps the series: {result:?}");
    let mut ranges = piece_ranges(&result);
    ranges.sort_unstable();
    assert_eq!(ranges, vec![(1, 1), (3, 1), (5, 1), (9, 1)]);
    // Results come back by label set, not in input order, so each side is found by its labels.
    let side = |name: &str| -> Series {
        result
            .iter()
            .find(|series| series.metric.name() == Some(name))
            .unwrap_or_else(|| panic!("no series {name}: {result:?}"))
            .clone()
    };
    let left = side("l");
    assert_eq!(
        left.time.len(),
        3,
        "the left side keeps its three rows: {left:?}"
    );
    assert_eq!(run_ranges(&left), vec![(1, 1), (5, 1), (9, 1)]);
    let right = side("r");
    assert_eq!(
        right.time.len(),
        1,
        "`or` keeps only the right side's positions the left side does not carry: {right:?}"
    );
    assert_eq!(run_ranges(&right), vec![(3, 1)]);

    // `unless`: the LHS positions the RHS does not carry, so 5 is dropped.
    let result = set_vectors(
        SetOp::Unless,
        &labels,
        &schema,
        &input(std::slice::from_ref(&lhs)),
        &schema,
        &input(std::slice::from_ref(&rhs)),
    )
    .unwrap();
    assert_eq!(result.len(), 1, "{result:?}");
    assert_eq!(
        run_ranges(&result[0]),
        vec![(1, 1), (9, 1)],
        "the dropped position costs no row and splits the run"
    );
    let mut expected: Dense = vec![vec![None]; 12];
    expected[1] = vec![Some(10.0)];
    expected[9] = vec![Some(90.0)];
    assert_result_eq(Some(&result[0]), &expected, &lhs.metric);
}

/// One row of the reservation that cannot be omitted: the point and the match group a filed row
/// stores, each charged twice for the capacity its `Vec` grows to.
const FILED_ROW: usize = 4 * std::mem::size_of::<usize>();

/// A grid of `points` positions one millisecond apart.
fn budget_grid(points: usize) -> Arc<EvalGrid> {
    // The grid counts its points as a `u64`; every point count here is a small literal.
    let len = u64::try_from(points).unwrap();
    Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1, len).unwrap()).unwrap())
}

/// A single-field operand of `schema` holding its position number at each of `at`, with `job` set
/// to `job`.
fn jobbed_operand(schema: &Arc<SeriesSchema>, name: &str, job: &str, at: &[usize]) -> Series {
    let labels = Labels::from([
        ("__name__".to_string(), name.to_string()),
        ("job".to_string(), job.to_string()),
    ]);
    let points: Points = at
        .iter()
        .map(|&position| (position, f64::to_bits(position as f64)))
        .collect();
    sparse_series(schema, &labels, &points).expect("the operand holds a value")
}

/// A single-field operand of `schema` holding its position number at each of `at`, with the same
/// `job` label.
fn budget_operand(schema: &Arc<SeriesSchema>, name: &str, at: &[usize]) -> Series {
    jobbed_operand(schema, name, "x", at)
}

/// The first sample of a one-field, one-row series.
fn first_sample(series: &Series) -> SampleRef<'_> {
    series.fields.as_slice()[0]
        .cursor()
        .at(0)
        .expect("the one-row operand holds a value")
}

/// A one-field operand on `grid` whose first point holds a histogram of `buckets` buckets with
/// `count` as its count, and whose remaining points hold the float 1.
///
/// A comparison can therefore discard the histogram at the first point and keep the floats after
/// it, which is the shape the ownership regressions need.
fn wide_histogram_operand(
    grid: &Arc<EvalGrid>,
    name: &str,
    points: usize,
    buckets: usize,
    count: &str,
) -> Series {
    let labels = Labels::from([
        ("__name__".to_string(), name.to_string()),
        ("job".to_string(), "x".to_string()),
    ]);
    let floats: Float64Array = std::iter::once(None::<f64>)
        .chain((1..points).map(|_| Some(1.0)))
        .collect();
    let list = vec!["4014000000000000"; buckets].join(",");
    let text = format!("0/0/0/{count}/4014000000000000/0/0_{buckets}/{list}///");
    let mut builder = HistogramBatchBuilder::new();
    let index = builder.push(OwnedHistogram::parse(&text).view()).unwrap();
    let indices: Vec<Option<u32>> = std::iter::once(Some(index))
        .chain((1..points).map(|_| None))
        .collect();
    let samples = Samples::try_from_mixed(
        floats,
        HistogramSamples::try_new(UInt32Array::from(indices), builder.finish().unwrap()).unwrap(),
    )
    .unwrap();
    Series {
        metric: metric(&labels),
        time: TimeAxis::Grid(GridRange::try_new(grid.clone(), 0, points).unwrap()),
        fields: SeriesFields::one(samples),
    }
}

fn budget_matching(op: BinaryOp, labels: MatchingLabels) -> BinaryMatching {
    BinaryMatching {
        op,
        return_bool: false,
        labels,
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    }
}

/// Runs a binary operation with a budget admitting at most `limit` bytes, recording every total it
/// is asked for. The totals are the live workspace, so they fall as a step's scratch ends.
fn budgeted(
    matching: &BinaryMatching,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
    limit: usize,
) -> (Result<Vec<Series>, DataFusionError>, Vec<usize>) {
    let mut seen = Vec::new();
    let output =
        binary_vectors_with_reservation(matching, lhs_schema, lhs, rhs_schema, rhs, &mut |total| {
            if total > limit {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "a binary operation needs {total} bytes, more than the {limit} admitted"
                )));
            }
            seen.push(total);
            Ok(())
        });
    (output, seen)
}

/// The widest workspace a binary operation asks for: the largest total it holds, which is also the
/// smallest limit it runs under.
fn budget_total(
    matching: &BinaryMatching,
    schema: &SeriesSchema,
    lhs: &[Series],
    rhs: &[Series],
) -> usize {
    let (output, seen) = budgeted(matching, schema, lhs, schema, rhs, usize::MAX);
    assert!(output.is_ok(), "{output:?}");
    *seen
        .iter()
        .max()
        .expect("the operation asks for its workspace")
}

/// The total the operation asks for once its last field has released its tables: what the results
/// keep until the merge takes over. The field's tables are dropped right before the merge, so the
/// value after the last fall is exactly the retained figure.
fn retained_total(seen: &[usize]) -> usize {
    seen.windows(2)
        .rfind(|pair| pair[0] > pair[1])
        .map(|pair| pair[1])
        .unwrap_or_else(|| panic!("the operation never released a step's workspace: {seen:?}"))
}

/// The workspace grows with the pairings a field walks, not with the product of the two sides: each
/// series of the many side here is one pairing, and a figure that reserved for every pair of every
/// match group would charge their square.
#[test]
fn the_budget_grows_with_the_pairings_not_their_product() {
    let grid = budget_grid(120);
    let schema = Arc::new(schema(1, &grid));
    // One dense series on the one side, and `n` series of the many side in its match group, each
    // carrying two positions of its own.
    let one = vec![budget_operand(&schema, "b", &(0..120).collect::<Vec<_>>())];
    let many = |n: usize| -> Vec<Series> {
        (0..n)
            .map(|index| {
                let labels = Labels::from([
                    ("__name__".to_string(), "a".to_string()),
                    ("instance".to_string(), format!("i{index}")),
                    ("job".to_string(), "x".to_string()),
                ]);
                let points: Points = [2 * index, 2 * index + 1]
                    .iter()
                    .map(|&position| (position, f64::to_bits(position as f64)))
                    .collect();
                sparse_series(&schema, &labels, &points).expect("the operand holds a value")
            })
            .collect()
    };
    let matching = BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::On(vec![Arc::from("job")]),
        cardinality: Cardinality::ManyToOne(Vec::new()),
        fill: FillValues::default(),
    };
    let single = budget_total(&matching, &schema, &many(25), &one);
    let double = budget_total(&matching, &schema, &many(50), &one);
    // Twice the pairings cost about twice the workspace; four times would mean the figure charged
    // the pairs of the group with each other.
    assert!(double < 3 * single, "single {single}, double {double}");
    assert!(double > single, "single {single}, double {double}");
}

/// An `on(...)` value is copied into each match key, so a longer value is a larger figure: the
/// group table the one side builds and the key every row of the many side is looked up by both copy
/// it.
#[test]
fn the_budget_charges_the_values_on_matching_copies() {
    let grid = budget_grid(8);
    let schema = Arc::new(schema(1, &grid));
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::On(vec![Arc::from("job")]));
    let measure = |len: usize| -> usize {
        let labels = Labels::from([
            ("__name__".to_string(), "a".to_string()),
            ("job".to_string(), "x".repeat(len)),
        ]);
        let points: Points = (0..8)
            .map(|position| (position, f64::to_bits(position as f64)))
            .collect();
        let series =
            vec![sparse_series(&schema, &labels, &points).expect("the operand holds a value")];
        budget_total(&matching, &schema, &series, &series)
    };
    let (short, long) = (measure(8), measure(4_096));
    let copied = 4_096 - 8;
    assert!(long >= short + copied, "long {long}, short {short}");
}

/// `+` and `-` of histograms keep the union of both sides' buckets -- subtracting disjoint buckets
/// adds the right side's -- so the figure grows with both payloads instead of the wider side.
#[test]
fn the_budget_charges_the_bucket_union() {
    let grid = budget_grid(1);
    let schema = Arc::new(schema(1, &grid));
    let five = "4014000000000000";
    let series = |name: &str, spans: &str, buckets: &str| -> Series {
        let labels = Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), "x".to_string()),
        ]);
        let text = format!("0/0/0/{five}/{five}/0/{spans}/{buckets}///");
        mixed_series(
            &schema,
            &labels,
            &Points::new(),
            &BTreeMap::from([(0, text)]),
        )
        .expect("the sample holds a histogram")
    };
    // `0_1` is bucket 0; `0_1,4_2` is bucket 0 and the two buckets after the gap at 1..5.
    let lhs = series("a", "0_1", five);
    let shared = series("b", "0_1", five);
    let disjoint = series("b", "0_1,4_2", &format!("{five},{five},{five}"));
    let budget = |op: BinaryOp, rhs: &Series| -> usize {
        let matching = BinaryMatching {
            op,
            return_bool: false,
            labels: MatchingLabels::On(vec![Arc::from("job")]),
            cardinality: Cardinality::OneToOne,
            fill: FillValues::default(),
        };
        budget_total(
            &matching,
            &schema,
            std::slice::from_ref(&lhs),
            std::slice::from_ref(rhs),
        )
    };
    for op in [BinaryOp::Add, BinaryOp::Sub] {
        let (shared_bytes, disjoint_bytes) = (budget(op, &shared), budget(op, &disjoint));
        assert!(
            disjoint_bytes >= shared_bytes + 2 * std::mem::size_of::<(i32, f64)>(),
            "{op:?}: {disjoint_bytes} vs {shared_bytes}"
        );
    }
}

/// `group_left` with thousands of labels the many side does not carry charges their descriptors and
/// stays linear, and a pairing whose axes share no position is charged no row at all.
#[test]
fn included_labels_cost_their_descriptors_and_disjoint_axes_cost_no_rows() {
    let grid = budget_grid(4);
    let shared_schema = Arc::new(schema(1, &grid));
    let dense = vec![budget_operand(&shared_schema, "b", &[0, 1, 2, 3])];
    let included = |n: usize| -> Vec<Arc<str>> {
        (0..n).map(|index| Arc::from(format!("l{index}"))).collect()
    };
    let measure = |n: usize| -> usize {
        // An empty `on()` puts both sides in one group, so the included labels are the only
        // descriptors the result label set gains, and the many side carries none of them.
        let matching = BinaryMatching {
            op: BinaryOp::Add,
            return_bool: false,
            labels: MatchingLabels::On(Vec::new()),
            cardinality: Cardinality::ManyToOne(included(n)),
            fill: FillValues::default(),
        };
        budget_total(&matching, &shared_schema, &dense, &dense)
    };
    let (none, many) = (measure(0), measure(2_000));
    // Every included label costs its descriptor in the label list the rewrite builds, whether or
    // not the many side carries it.
    assert!(
        many >= none + 2_000 * std::mem::size_of::<Label>(),
        "none {none}, many {many}"
    );
    // And the cost stays linear: a few hundred bytes each, never their product.
    assert!(many <= none + 2_000 * 512, "none {none}, many {many}");

    // Two operands on one long grid whose axes share no position: the pairing builds its label set,
    // intersects nothing and files no row, so only the presence flags grow with the grid.
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::On(vec![Arc::from("job")]));
    let measure = |points: usize| -> (Vec<Series>, usize) {
        let grid = budget_grid(points);
        let operand_schema = Arc::new(schema(1, &grid));
        let lhs = vec![budget_operand(&operand_schema, "a", &[0])];
        let rhs = vec![budget_operand(&operand_schema, "b", &[points - 1])];
        let (output, seen) = budgeted(
            &matching,
            &operand_schema,
            &lhs,
            &operand_schema,
            &rhs,
            usize::MAX,
        );
        (
            output.expect("the operands are both readable"),
            *seen
                .iter()
                .max()
                .expect("the operation asks for its workspace"),
        )
    };
    let (short_output, short) = measure(2_000);
    let (long_output, long) = measure(20_000);
    assert!(
        short_output.is_empty() && long_output.is_empty(),
        "{short_output:?} {long_output:?}"
    );
    // Three presence flags per added grid point and nothing else: a figure that assumed a row per
    // position would grow by a row's slots, and one that assumed the grid's square far more.
    let flags = 3 * (20_000 - 2_000);
    assert!(long >= short + flags / 2, "short {short}, long {long}");
    assert!(
        long <= short + 4 * (20_000 - 2_000) + 16 * 1024,
        "short {short}, long {long}"
    );
}

/// A pairing's scratch is replaced by the next pairing's, never added to it: fifty pairings that
/// each build a result label set of two hundred labels and intersect nothing cost one such set.
#[test]
fn a_pairings_scratch_is_replaced_by_the_next_one() {
    let grid = budget_grid(2);
    let schema = Arc::new(schema(1, &grid));
    let wide = |index: usize| -> Series {
        let mut labels = Labels::from([
            ("__name__".to_string(), "a".to_string()),
            ("instance".to_string(), format!("i{index}")),
            ("job".to_string(), "x".to_string()),
        ]);
        for label in 0..200 {
            labels.insert(format!("l{label}"), format!("v{label}"));
        }
        sparse_series(&schema, &labels, &Points::from([(0, f64::to_bits(1.0))]))
            .expect("the operand holds a value")
    };
    let many = |n: usize| -> Vec<Series> { (0..n).map(wide).collect() };
    // The one side holds the position none of the many side's series carry, so every pairing
    // intersects nothing and nothing of it is retained.
    let one = vec![budget_operand(&schema, "b", &[1])];
    let matching = BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::On(vec![Arc::from("job")]),
        cardinality: Cardinality::ManyToOne(Vec::new()),
        fill: FillValues::default(),
    };
    let single = budget_total(&matching, &schema, &many(1), &one);
    let fifty = budget_total(&matching, &schema, &many(50), &one);
    // One pairing's label set of two hundred labels is about twenty kilobytes, so fifty of them
    // added up would cost a megabyte.
    assert!(
        fifty <= single + 64 * 1024,
        "single {single}, fifty {fifty}"
    );
}

/// A group the one side holds twice is charged the rows it files: the kernel pairs every member of
/// the group and keeps those rows until it reports the duplicate, which is what the run costs on top
/// of the same group with one member.
///
/// The figures are coarse on purpose -- rows are reserved in the blocks their vectors grow into, so a
/// run costs a block rather than a row -- and what is checked is the order of the growth, not an
/// exact figure per point.
#[test]
fn a_duplicated_group_is_charged_the_rows_it_files() {
    let points = 512;
    let grid = budget_grid(points);
    let schema = Arc::new(schema(1, &grid));
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::On(vec![Arc::from("job")]));
    let at: Vec<usize> = (0..points).collect();
    let one = vec![budget_operand(&schema, "b", &at)];
    // One member of the group is a valid one-to-one match.
    let (output, single_seen) = budgeted(
        &matching,
        &schema,
        &[budget_operand(&schema, "a", &at)],
        &schema,
        &one,
        usize::MAX,
    );
    assert!(output.is_ok(), "one member per group is a valid match");
    // A second member of the same group matches the same points again.
    let (output, seen) = budgeted(
        &matching,
        &schema,
        &[
            budget_operand(&schema, "a", &at),
            jobbed_operand(&schema, "c", "x", &at),
        ],
        &schema,
        &one,
        usize::MAX,
    );
    let error = output.expect_err("a second member of one group is rejected");
    assert_eq!(
        violation_of(&error),
        Violation::ImplicitManyToOne,
        "{error}"
    );
    // The rows of the second member are what the run costs on top of the single pairing, and they
    // are what its result table holds rather than a fraction of it.
    let single = *single_seen
        .iter()
        .max()
        .expect("the operation asks for its workspace");
    let duplicated = *seen.iter().max().unwrap();
    let rows = points * FILED_ROW;
    assert!(
        duplicated >= single + rows / 2,
        "single {single}, duplicated {duplicated}: the second member's {rows} bytes of rows are not \
         charged"
    );
    assert!(
        duplicated <= single + 4 * rows,
        "single {single}, duplicated {duplicated}: the second member's {rows} bytes of rows are \
         charged several times over"
    );
    // What the operation holds once its tables are gone is the rows the two members filed, and no
    // match set kept past its field: the set is part of the field's phase and goes with it.
    let floor = retained_total(&seen);
    let filed = 2 * points * RESULT_ROW_BYTES;
    assert!(floor >= filed, "floor {floor}, rows {filed}");
    assert!(
        floor <= 4 * filed + 64 * 1024,
        "floor {floor}: a matched point is charged per attempt, or the set is kept past its field"
    );
}

/// A pairing the operator skips files no row, while a value it keeps is charged with what it holds.
#[test]
fn a_pairing_the_operator_skips_files_no_row() {
    let grid = budget_grid(1);
    let schema = Arc::new(schema(1, &grid));
    let text = "0/0/0/4014000000000000/4014000000000000/0/0_1/4014000000000000///";
    let empty = "0/0/0/0/0/0/////";
    let series = |name: &str, text: &str| -> Series {
        let labels = Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), "x".to_string()),
        ]);
        mixed_series(
            &schema,
            &labels,
            &Points::new(),
            &BTreeMap::from([(0, text.to_string())]),
        )
        .expect("the sample holds a histogram")
    };
    let matching = |op: BinaryOp, return_bool: bool| BinaryMatching {
        op,
        return_bool,
        labels: MatchingLabels::On(vec![Arc::from("job")]),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    };
    // One series per side is one pairing: a second of either side would share the match group and
    // be rejected instead of measuring what the pairing keeps.
    let run = |op: BinaryOp, return_bool: bool, lhs: &str, rhs: &str| {
        budgeted(
            &matching(op, return_bool),
            &schema,
            &[series("a", lhs)],
            &schema,
            &[series("b", rhs)],
            usize::MAX,
        )
    };
    let (added, seen) = run(BinaryOp::Add, false, text, text);
    let added = added.expect("`+` keeps the pair");
    assert_eq!(added.len(), 1, "{added:?}");
    let added_total = *seen.iter().max().unwrap();
    // `%` is one of the pairings Prometheus skips: nothing of it is retained, so nothing of it is
    // charged as output.
    let (skipped, seen) = run(BinaryOp::Mod, false, text, text);
    assert!(
        skipped
            .expect("a skipped pairing is not an error")
            .is_empty()
    );
    let skipped_total = *seen.iter().max().unwrap();
    assert!(
        added_total > skipped_total,
        "added {added_total}, skipped {skipped_total}"
    );
    // A `bool` comparison keeps a float, so its rows hold no histogram heap.
    let (booled, seen) = run(BinaryOp::Ne, true, text, text);
    let booled = booled.expect("a `bool` comparison holds");
    assert_eq!(booled.len(), 1, "{booled:?}");
    let booled_total = *seen.iter().max().unwrap();
    assert!(
        booled_total < added_total,
        "booled {booled_total}, added {added_total}"
    );
    let mut cursor = booled[0].fields.as_slice()[0].cursor();
    for (_, row) in booled[0].time.positions() {
        assert!(
            matches!(
                cursor.at(row).expect("the axis and the fields are aligned"),
                SampleRef::Float(_)
            ),
            "a `bool` comparison keeps floats"
        );
    }
    // An empty histogram still keeps its row, so its table row is charged even without buckets.
    let (kept, kept_seen) = run(BinaryOp::Add, false, empty, empty);
    assert_eq!(
        kept.expect("an empty histogram is a histogram").len(),
        1,
        "an empty histogram keeps its row"
    );
    // The same pair under an operator it does not support keeps nothing.
    let (unkept, unkept_seen) = run(BinaryOp::Mod, false, empty, empty);
    assert!(
        unkept
            .expect("a skipped pairing is not an error")
            .is_empty()
    );
    assert!(
        *kept_seen.iter().max().unwrap() > *unkept_seen.iter().max().unwrap(),
        "an empty histogram keeps its row"
    );
}

/// A kept histogram's payload is what the operation genuinely holds, and a budget one byte short of
/// it refuses the run; the same operands with float values answer under a budget of a few kilobytes.
///
/// The two sides of one figure: a result the operation really allocates is refused by a pool that
/// cannot hold it, and a small result is not refused by a reservation nothing allocates for.
#[test]
fn a_kept_histogram_payload_is_refused_and_a_small_result_is_not() {
    let grid = budget_grid(1);
    let schema = Arc::new(schema(1, &grid));
    let buckets = 4_096;
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::On(vec![Arc::from("job")]));
    // One point holding a histogram of `buckets` buckets on both sides: the row is kept and the
    // payload of the union is retained until the operation returns.
    let lhs = vec![wide_histogram_operand(
        &grid,
        "a",
        1,
        buckets,
        "4014000000000000",
    )];
    let rhs = vec![wide_histogram_operand(
        &grid,
        "b",
        1,
        buckets,
        "4014000000000000",
    )];
    let (output, seen) = budgeted(&matching, &schema, &lhs, &schema, &rhs, usize::MAX);
    let output = output.expect("`+` of two histograms keeps the union");
    assert_eq!(output.len(), 1, "{output:?}");
    match first_sample(&output[0]) {
        SampleRef::Histogram(view) => assert_eq!(
            view.positive.buckets.len(),
            buckets,
            "the row keeps the union of both bucket lists"
        ),
        _ => panic!("`+` of two histograms keeps a histogram"),
    }
    // What the operation holds is at least the payload of the value it keeps.
    let payload = buckets * std::mem::size_of::<(i32, f64)>();
    let peak = *seen
        .iter()
        .max()
        .expect("the operation asks for its workspace");
    assert!(
        peak >= payload,
        "peak {peak} does not hold the {payload} bytes of the kept histogram"
    );
    // A pool one byte short of the widest figure refuses the run rather than allocate past it.
    let error = budgeted(&matching, &schema, &lhs, &schema, &rhs, peak - 1)
        .0
        .expect_err("the kept payload is reserved before it is filed");
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );

    // The same shape with float values: two operands of one point, one result row. Nothing of that
    // is measured per grid point, so a budget of a few kilobytes admits it.
    let small = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    let floats = |name: &str| vec![budget_operand(&schema, name, &[0])];
    let (output, _) = budgeted(
        &small,
        &schema,
        &floats("a"),
        &schema,
        &floats("b"),
        16 * 1024,
    );
    assert_eq!(
        output
            .expect("a one-row float result fits a few kilobytes")
            .len(),
        1
    );
}

/// A large result is genuinely refused by a bounded pool, and a sparse small result over the same
/// grid is not: the reservation follows the rows and columns the operation writes, not the grid.
///
/// Both operands sit on a grid of a hundred thousand points. The dense pair answers every point, so
/// its result columns are millions of bytes and a pool of a few hundred kilobytes refuses it. The
/// sparse pair shares eight points, so its result is a handful of rows and the same pool admits it --
/// a figure that reserved for the grid rather than for the rows would refuse it too.
#[test]
fn a_dense_result_is_refused_where_a_sparse_one_is_admitted() {
    let points = 100_000;
    let grid = budget_grid(points);
    let schema = Arc::new(schema(1, &grid));
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::On(vec![Arc::from("job")]));
    let dense: Vec<usize> = (0..points).collect();
    let sparse: Vec<usize> = (0..8).map(|point| 1_000 * point).collect();
    let limit = 512 * 1024;

    // The dense pair answers every point: its columns are far past the pool, and it is refused.
    let (output, _) = budgeted(
        &matching,
        &schema,
        &[budget_operand(&schema, "a", &dense)],
        &schema,
        &[budget_operand(&schema, "b", &dense)],
        limit,
    );
    let error = output.expect_err("a dense result over the whole grid is refused");
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );

    // The sparse pair shares eight points on the same grid: a few rows, and the same pool holds it.
    let (output, seen) = budgeted(
        &matching,
        &schema,
        &[budget_operand(&schema, "a", &sparse)],
        &schema,
        &[budget_operand(&schema, "b", &sparse)],
        limit,
    );
    let output = output.expect("a sparse result of eight rows fits a few hundred kilobytes");
    assert_eq!(output.len(), 1, "{output:?}");
    assert_eq!(
        run_ranges(&output[0]).len(),
        sparse.len(),
        "the sparse pair keeps its eight points, one run each"
    );
    // The widest figure is the presence flags plus those rows, not a column per grid point.
    let peak = *seen
        .iter()
        .max()
        .expect("the operation asks for its workspace");
    assert!(
        peak < limit,
        "the sparse run asked for {peak} bytes, past the {limit} it ran under"
    );
}

/// A field that meets its first histogram after a float backfills the index column of the rows
/// before it, and the budget covers the column all the same.
#[test]
fn a_field_that_meets_a_histogram_late_backfills_its_column() {
    let grid = budget_grid(2);
    let schema = Arc::new(schema(1, &grid));
    let text = "0/0/0/4014000000000000/4014000000000000/0/0_1/4014000000000000///";
    let labels = Labels::from([
        ("__name__".to_string(), "a".to_string()),
        ("job".to_string(), "x".to_string()),
    ]);
    // The left side holds a float at the first point and a histogram at the second.
    let lhs = vec![
        mixed_series(
            &schema,
            &labels,
            &Points::from([(0, f64::to_bits(2.0))]),
            &BTreeMap::from([(1, text.to_string())]),
        )
        .expect("the sample holds both kinds"),
    ];
    let rhs = vec![budget_operand(&schema, "b", &[0, 1])];
    let matching = budget_matching(BinaryOp::Mul, MatchingLabels::Ignoring(Vec::new()));
    let (output, _) = budgeted(&matching, &schema, &lhs, &schema, &rhs, usize::MAX);
    let output = output.expect("a histogram times a float is a histogram");
    assert_eq!(output.len(), 1, "{output:?}");
    let mut kinds = Vec::new();
    let mut cursor = output[0].fields.as_slice()[0].cursor();
    for (point, row) in output[0].time.positions() {
        kinds.push((
            point,
            match cursor.at(row).expect("the axis and the fields are aligned") {
                SampleRef::Missing => panic!("the row holds a value"),
                SampleRef::Float(_) => "float",
                SampleRef::Histogram(_) => "histogram",
            },
        ));
    }
    assert_eq!(
        kinds,
        vec![(0, "float"), (1, "histogram")],
        "the histogram column starts at the second row"
    );
}

/// A comparison that filters its rows still files them: Prometheus counts the match, so the result
/// holds no series but the rows are kept.
#[test]
fn a_filtered_comparison_files_a_row_that_carries_no_value() {
    let grid = budget_grid(4);
    let schema = Arc::new(schema(1, &grid));
    let at = [0, 1, 2, 3];
    let lhs = vec![budget_operand(&schema, "a", &at)];
    let matching = budget_matching(BinaryOp::Gt, MatchingLabels::On(vec![Arc::from("job")]));
    // The right side holds the same values under the same `job`, so `>` holds nowhere; the same
    // operand under another `job` matches nothing at all.
    let equal = vec![budget_operand(&schema, "b", &at)];
    let other = vec![jobbed_operand(&schema, "b", "y", &at)];
    let total = |rhs: &[Series]| -> usize {
        let (output, seen) = budgeted(&matching, &schema, &lhs, &schema, rhs, usize::MAX);
        assert!(output.expect("a filtered comparison holds").is_empty());
        *seen
            .iter()
            .max()
            .expect("the operation asks for its workspace")
    };
    let filtered = total(&equal);
    let unmatched = total(&other);
    // The filtered run holds four rows the unmatched one does not, and both hold no output value.
    assert!(
        filtered >= unmatched + 4 * FILED_ROW,
        "filtered {filtered}, unmatched {unmatched}"
    );
}

/// Fields that hold different positions of one label set are merged into one series: the triples
/// arrive field by field, so the merge sorts them before it writes the series out.
#[test]
fn fields_holding_different_positions_merge_into_one_series() {
    let grid = budget_grid(2);
    let schema = Arc::new(schema(2, &grid));
    let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
    // The left side holds its first field at the later point and its second at the earlier one.
    builder.begin_series();
    builder.push(0, &[None, Some(10.0)]).unwrap();
    builder.push(1, &[Some(1.0), None]).unwrap();
    let lhs: Vec<Series> = builder
        .finish(metric(&Labels::from([(
            "job".to_string(),
            "x".to_string(),
        )])))
        .unwrap()
        .into_iter()
        .collect();
    let labels = Labels::from([
        ("__name__".to_string(), "b".to_string()),
        ("job".to_string(), "x".to_string()),
    ]);
    builder.begin_series();
    builder.push(0, &[Some(2.0), Some(20.0)]).unwrap();
    builder.push(1, &[Some(3.0), Some(30.0)]).unwrap();
    let rhs: Vec<Series> = builder
        .finish(metric(&labels))
        .unwrap()
        .into_iter()
        .collect();
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    let (output, _) = budgeted(&matching, &schema, &lhs, &schema, &rhs, usize::MAX);
    let output = output.expect("the fields merge");
    assert_eq!(output.len(), 1, "{output:?}");
    let series = &output[0];
    let mut first = series.fields.as_slice()[0].cursor();
    let mut second = series.fields.as_slice()[1].cursor();
    let value = |cursor: &mut RowCursor<'_>, row: usize| -> Option<f64> {
        match cursor.at(row).expect("a row never goes back") {
            SampleRef::Missing => None,
            SampleRef::Float(value) => Some(value),
            SampleRef::Histogram(_) => panic!("a float field holds no histogram"),
        }
    };
    let mut rows = Vec::new();
    for (point, row) in series.time.positions() {
        rows.push((point, [value(&mut first, row), value(&mut second, row)]));
    }
    assert_eq!(
        rows,
        vec![(0, [None, Some(30.0)]), (1, [Some(4.0), None])],
        "each field keeps its own point"
    );
}

/// Only the field that matches keeps storage: a second field whose operands share no value files
/// nothing, and the match slots of the first field are released with it.
#[test]
fn only_the_field_that_matches_keeps_storage() {
    let points = 512;
    let grid = budget_grid(points);
    let schema = Arc::new(schema(2, &grid));
    let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
    let metric_of = |name: &str| {
        metric(&Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), "x".to_string()),
        ]))
    };
    // The first field holds a value on both sides at every point; the second holds none on the
    // left, so that pairing has nothing to match.
    builder.begin_series();
    for point in 0..points {
        builder.push(point, &[Some(1.0), None]).unwrap();
    }
    let lhs: Vec<Series> = builder
        .finish(metric_of("a"))
        .unwrap()
        .into_iter()
        .collect();
    builder.begin_series();
    for point in 0..points {
        builder.push(point, &[Some(1.0), Some(2.0)]).unwrap();
    }
    let rhs: Vec<Series> = builder
        .finish(metric_of("b"))
        .unwrap()
        .into_iter()
        .collect();
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    let (output, seen) = budgeted(&matching, &schema, &lhs, &schema, &rhs, usize::MAX);
    let output = output.expect("the fields pair");
    assert_eq!(output.len(), 1, "{output:?}");
    let mut first = output[0].fields.as_slice()[0].cursor();
    let mut second = output[0].fields.as_slice()[1].cursor();
    let mut kept = 0;
    for (_, row) in output[0].time.positions() {
        if let SampleRef::Float(value) = first.at(row).expect("a row never goes back") {
            assert_eq!(value, 2.0);
            kept += 1;
        }
        assert!(matches!(
            second.at(row).expect("a row never goes back"),
            SampleRef::Missing
        ));
    }
    assert_eq!(kept, points, "the matching field keeps every point");
    // What the operation still holds once its last field released its tables is the rows it filed:
    // no match slot of the field that matched, no table of the field that did not.
    let floor = retained_total(&seen);
    let rows = points * RESULT_ROW_BYTES;
    assert!(floor >= rows, "floor {floor}, rows {rows}");
    assert!(
        floor <= rows + 8 * 1024,
        "floor {floor}: a released field's slots or tables are still charged"
    );
}

/// A field that reports nothing keeps no message storage: what the successful fields leave behind
/// is the same figure whatever the text of their labels, while the messages they could have written
/// grow with it.
///
/// `ignoring(...)` matching shares the label `Arc`s with the operand, so the result label set keeps
/// the same descriptors and the same key in both runs; only the text of each label differs. A field
/// that reserved for the messages it might report would leave the wide run's result storage larger by
/// three label sets, which this comparison rules out without any allowance of its own.
#[test]
fn successful_fields_keep_no_message_storage() {
    let points = 16;
    let grid = budget_grid(points);
    let schema = Arc::new(schema(2, &grid));
    let operand = |builder: &mut GridSeriesBuilder, name: &str, text: &str, values: [f64; 2]| {
        let mut labels = Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), "x".to_string()),
        ]);
        for index in 0..40 {
            labels.insert(format!("l{index}"), text.to_string());
        }
        builder.begin_series();
        for point in 0..points {
            builder
                .push(point, &[Some(values[0]), Some(values[1])])
                .unwrap();
        }
        builder
            .finish(metric(&labels))
            .unwrap()
            .into_iter()
            .collect::<Vec<Series>>()
    };
    // The figure the operation still holds once its last field released its tables, with the values
    // it answers checked as sums of the two operands' fields.
    let floor = |text: &str| -> usize {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        let lhs = operand(&mut builder, "a", text, [1.0, 2.0]);
        let rhs = operand(&mut builder, "b", text, [3.0, 4.0]);
        let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
        let (output, seen) = budgeted(&matching, &schema, &lhs, &schema, &rhs, usize::MAX);
        let output = output.expect("both fields match");
        assert_eq!(output.len(), 1, "{output:?}");
        // Each field answers the sum of its operands at every point: `1+3` and `2+4`.
        for (field, expected) in [4.0, 6.0].into_iter().enumerate() {
            let mut cursor = output[0].fields.as_slice()[field].cursor();
            let mut rows = 0;
            for (_, row) in output[0].time.positions() {
                match cursor.at(row).expect("the axis and the fields are aligned") {
                    SampleRef::Float(value) => assert_eq!(value, expected, "{text}"),
                    _ => panic!("a field of the sum holds no float at {row}"),
                }
                rows += 1;
            }
            assert_eq!(rows, points, "every point of the field is kept: {text}");
        }
        retained_total(&seen)
    };
    let narrow = floor("v");
    let wide_text = "v".repeat(200);
    let wide = floor(&wide_text);
    // Both runs file the same rows and keep the same label-set descriptors: a field that reserved
    // for the messages it could have written would leave the wide run larger by their text.
    assert_eq!(narrow, wide, "narrow {narrow}, wide {wide}");
    // The rows themselves are part of that figure, so neither run is trivially zero.
    assert!(
        narrow >= 2 * points * RESULT_ROW_BYTES,
        "floor {narrow} does not hold the rows both fields filed"
    );
}

/// The matching label names are collected into a reference vector before the label list is
/// rewritten, so a wide `ignoring(...)` and a wide `on(...)` cost their names even when no series
/// carries them and the pairing's axes share no position.
#[test]
fn the_budget_charges_the_matching_names_it_collects() {
    let grid = budget_grid(2);
    let schema = Arc::new(schema(1, &grid));
    // Almost no labels on either side, and axes that share no position: the operands still share
    // one match group, and the pairing files no row.
    let lhs = vec![budget_operand(&schema, "a", &[0])];
    let rhs = vec![budget_operand(&schema, "b", &[1])];
    let absent = |n: usize| -> Vec<Arc<str>> {
        (0..n)
            .map(|index| Arc::from(format!("absent_{index}")))
            .collect()
    };
    // The widest figure of the whole run: no row is filed here, so what it holds is the field's
    // phase, which is where the names are collected.
    let measure = |labels: fn(Vec<Arc<str>>) -> MatchingLabels, n: usize| -> usize {
        let matching = budget_matching(BinaryOp::Add, labels(absent(n)));
        let (output, seen) = budgeted(&matching, &schema, &lhs, &schema, &rhs, usize::MAX);
        assert!(
            output.expect("the operands are readable").is_empty(),
            "no position is shared"
        );
        *seen
            .iter()
            .max()
            .expect("the operation asks for its workspace")
    };
    let kinds = [
        MatchingLabels::On as fn(Vec<Arc<str>>) -> MatchingLabels,
        MatchingLabels::Ignoring,
    ];
    for labels in kinds {
        let (none_peak, many_peak) = (measure(labels, 0), measure(labels, 2_000));
        // Two thousand names are collected before the rewrite, so the widest figure grows with
        // them, in the order of one reference per name: a figure that never read them would not
        // move at all, and one that charged them per row would grow by far more.
        let names = 2_000 * std::mem::size_of::<&str>();
        assert!(
            many_peak >= none_peak + names / 2,
            "the names are not in the widest figure: {none_peak} vs {many_peak}"
        );
        assert!(
            many_peak <= none_peak + 4 * names,
            "the names are charged more than once each: {none_peak} vs {many_peak}"
        );
        // A limit the run without the names admits is refused once they are collected.
        let matching = budget_matching(BinaryOp::Add, labels(absent(2_000)));
        let (output, admitted) = budgeted(&matching, &schema, &lhs, &schema, &rhs, none_peak);
        let error = output.expect_err("the names are charged before the rewrite");
        assert!(
            matches!(error, DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        assert!(
            admitted.iter().all(|&total| total <= none_peak),
            "{admitted:?}"
        );
    }
}

/// The output schema is allocated after the first reservation, so a wide schema's field list is
/// covered before it exists, and a schema with no paired fields still answers an empty vector.
#[test]
fn the_first_request_covers_the_output_schema() {
    let grid = budget_grid(1);
    let wide = Arc::new(schema(10, &grid));
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    let (output, seen) = budgeted(&matching, &wide, &[], &wide, &[], usize::MAX);
    assert!(output.expect("nothing pairs with nothing").is_empty());
    let first = *seen.first().expect("the schema is reserved first");
    assert!(
        first >= 10 * 2 * std::mem::size_of::<FieldSpec>(),
        "the first request holds the field list: {first}"
    );
    // A limit below the schema's own descriptors is refused before the schema is built.
    let (output, admitted) = budgeted(&matching, &wide, &[], &wide, &[], first - 1);
    let error = output.expect_err("the schema's descriptors are reserved before it exists");
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert!(admitted.is_empty(), "{admitted:?}");

    // No paired fields pair nothing: the operands are never read, so a series that disagrees with
    // a fieldless schema is answered with an empty vector, as the unbudgeted form answers.
    let none = Arc::new(schema(0, &grid));
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let points: Points = BTreeMap::from([(0, f64::to_bits(1.0))]);
    let operand = vec![
        sparse_series(&Arc::new(schema(1, &grid)), &labels, &points)
            .expect("the operand holds a value"),
    ];
    assert!(
        binary_vectors(&matching, &none, &operand, &none, &[])
            .expect("no field pairs")
            .is_empty()
    );
    let (output, _) = budgeted(&matching, &none, &operand, &none, &[], usize::MAX);
    assert!(output.expect("no field pairs").is_empty());
}

/// A budget below what the operation needs stops it at the step that outgrows it, and the totals it
/// asks for are the live ones: the widest is what it needs, and anything under it is refused.
#[test]
fn a_budget_below_the_workspace_stops_the_operation() {
    let grid = budget_grid(8);
    let schema = Arc::new(schema(1, &grid));
    let at: Vec<usize> = (0..8).collect();
    let lhs = vec![budget_operand(&schema, "a", &at)];
    let rhs = vec![budget_operand(&schema, "b", &at)];
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    let (output, seen) = budgeted(&matching, &schema, &lhs, &schema, &rhs, usize::MAX);
    assert!(output.is_ok());
    assert!(
        seen.len() > 2,
        "the workspace is admitted in steps: {seen:?}"
    );
    // Every request is a different figure, and a step replaces the one before it, so the sequence
    // falls as well as rises.
    assert!(seen.windows(2).all(|pair| pair[0] != pair[1]), "{seen:?}");
    assert!(
        seen.windows(2).any(|pair| pair[0] > pair[1]),
        "no step's workspace was released: {seen:?}"
    );
    // The widest total is what the operation needs; every figure under it is refused at the step
    // that would hold more.
    let needed = *seen.iter().max().unwrap();
    assert!(
        budgeted(&matching, &schema, &lhs, &schema, &rhs, needed)
            .0
            .is_ok()
    );
    let mut limits: Vec<usize> = seen
        .iter()
        .copied()
        .filter(|&total| total < needed)
        .collect();
    limits.sort_unstable();
    limits.dedup();
    assert!(!limits.is_empty(), "{seen:?}");
    for limit in limits {
        let (output, admitted) = budgeted(&matching, &schema, &lhs, &schema, &rhs, limit);
        let error = output.unwrap_err().to_string();
        assert!(error.contains("more than the"), "{error}");
        assert!(admitted.iter().all(|&total| total <= limit), "{admitted:?}");
    }
    // A budget of zero admits nothing, so the first step already fails.
    let (output, admitted) = budgeted(&matching, &schema, &lhs, &schema, &rhs, 0);
    assert!(output.is_err());
    assert!(admitted.is_empty(), "{admitted:?}");
}

/// The public form and the budgeted one report the same error for the same operands, whether the
/// grid or a series' layout is what disagrees, and neither reads a field it cannot place.
#[test]
fn a_budgeted_run_reports_the_public_rejection() {
    let grid = budget_grid(4);
    let shared_schema = Arc::new(schema(1, &grid));
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    // A schema evaluated on another grid: refused before anything is reserved for the operands.
    let elsewhere = Arc::new(schema(1, &budget_grid(8)));
    let public = binary_vectors(&matching, &shared_schema, &[], &elsewhere, &[]).unwrap_err();
    let (output, seen) = budgeted(&matching, &shared_schema, &[], &elsewhere, &[], usize::MAX);
    assert_eq!(output.unwrap_err().to_string(), public.to_string());
    assert!(seen.is_empty(), "{seen:?}");

    // A series whose axis places two rows and whose field carries one: the kernel cannot read it,
    // and only the output descriptors were admitted before it said so.
    let short = Series {
        metric: metric(&Labels::from([("job".to_string(), "x".to_string())])),
        time: TimeAxis::Grid(GridRange::try_new(grid.clone(), 0, 2).unwrap()),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(1.0)]))),
    };
    let public = binary_vectors(
        &matching,
        &shared_schema,
        std::slice::from_ref(&short),
        &shared_schema,
        &[],
    )
    .unwrap_err();
    let (output, seen) = budgeted(
        &matching,
        &shared_schema,
        std::slice::from_ref(&short),
        &shared_schema,
        &[],
        usize::MAX,
    );
    assert_eq!(output.unwrap_err().to_string(), public.to_string());
    assert_eq!(seen.len(), 1, "{seen:?}");
}

/// A match group of one member cannot hold two rows at a point, so its duplicate scan collects
/// nothing and the figure charges it no point list: many such groups over a long grid cost their
/// presence flags and their table entries, not one sixteen-byte slot per row of every one-side
/// series.
#[test]
fn singleton_groups_are_charged_no_duplicate_point_list() {
    let points = 10_000;
    let grid = budget_grid(points);
    let schema = Arc::new(schema(1, &grid));
    let at: Vec<usize> = (0..points).collect();
    // One left-hand series under a `job` no right-hand series carries, and `groups` right-hand
    // series that each carry their own `job` and a value at every point: every group has one
    // member, nothing matches and no row is filed.
    let lhs = vec![jobbed_operand(&schema, "a", "absent", &at)];
    let one = |groups: usize| -> Vec<Series> {
        (0..groups)
            .map(|group| jobbed_operand(&schema, "b", &format!("j{group}"), &at))
            .collect()
    };
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::On(vec![Arc::from("job")]));
    let measure = |groups: usize| -> usize {
        let (output, seen) = budgeted(&matching, &schema, &lhs, &schema, &one(groups), usize::MAX);
        assert!(
            output.expect("no group matches").is_empty(),
            "{groups}: no `job` is shared"
        );
        *seen
            .iter()
            .max()
            .expect("the operation asks for its workspace")
    };
    let single = measure(1);
    let many = measure(16);
    // What a run of this shape holds: the two presence flags and the intersection of them, one
    // byte per grid point, and nothing proportional to the rows the one side carries.
    let presence = 3 * points;
    assert!(
        single <= presence + 64 * 1024,
        "single {single} for {presence} presence bytes"
    );
    assert!(
        many <= presence + 64 * 1024,
        "sixteen singleton groups {many} for {presence} presence bytes: the scan collected the \
         rows of every group"
    );
    // A limit in the same order as the presence flags alone runs the wider operand: the sixteen
    // groups' rows are sixteen times the grid, and a figure that charged their points as held
    // storage would need megabytes of them.
    let (output, _) = budgeted(&matching, &schema, &lhs, &schema, &one(16), 512 * 1024);
    assert!(
        output
            .expect("no group matches under a small budget")
            .is_empty(),
        "no `job` is shared"
    );
}

/// The duplicate scan's list of present points is one group's workspace: the next group's scan
/// replaces it instead of adding to it, and the figure charges the points the group's members carry
/// rather than one slot per row of every series of the field.
///
/// The list is charged by the block it grows into, so the figures are compared as growth rather than
/// as an exact count of slots: what is checked is that the points a group carries are in the figure
/// and that eight groups do not hold eight lists.
#[test]
fn a_groups_duplicate_scan_is_replaced_by_the_one_after_it() {
    // Two members of a group hold the even and the odd points of at most the first half, so the
    // scan collects every point of both and no point of the group is present twice: no group is
    // reported as a duplicate whatever the other side carries.
    let rows_max = 512;
    let points = 2 * rows_max;
    let grid = budget_grid(points);
    let schema = Arc::new(schema(1, &grid));
    // The left side holds one value at the first point, whose `job` no right-hand series carries,
    // so no group matches and no row is filed.
    let lhs = vec![jobbed_operand(&schema, "a", "absent", &[0])];
    let one = |groups: usize, rows: usize| -> Vec<Series> {
        // The schema is borrowed, so the iterator below captures the reference and not the `Arc`.
        let schema = &schema;
        (0..groups)
            .flat_map(|group| {
                let job = format!("j{group}");
                (0..2).map(move |member| {
                    let at: Vec<usize> = (0..rows).map(|row| 2 * row + member).collect();
                    jobbed_operand(schema, "b", &job, &at)
                })
            })
            .collect()
    };
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::On(vec![Arc::from("job")]));
    let measure = |groups: usize, rows: usize| -> usize {
        let (output, seen) = budgeted(
            &matching,
            &schema,
            &lhs,
            &schema,
            &one(groups, rows),
            usize::MAX,
        );
        assert!(
            output
                .expect("no point is present on both sides")
                .is_empty(),
            "{groups} groups of {rows} rows file no row"
        );
        *seen
            .iter()
            .max()
            .expect("the operation asks for its workspace")
    };
    // The list is charged: a group whose two members carry `rows_max` points each costs more than
    // the same shape with eight points a member, and the growth is in the order of those points.
    let short = measure(1, 8);
    let long = measure(1, rows_max);
    let slots = (2 * rows_max - 16) * std::mem::size_of::<usize>();
    assert!(
        long >= short + slots / 4,
        "short {short}, long {long}: the scan's {slots} bytes of points are not charged"
    );
    assert!(
        long <= short + 4 * slots + 16 * 1024,
        "short {short}, long {long}: the scan's points are charged as the whole grid"
    );
    // And it is replaced, not accumulated: eight groups hold the same list as one group does, so
    // what the eight add is their table entries, never their sequences of points.
    let eight = measure(8, rows_max);
    assert!(
        eight <= long + 16 * 1024,
        "one group {long}, eight {eight}: the scans' lists added up"
    );
}

/// A dense run of `fields` fields writes one value column and one validity bit per field at every
/// output row, and the figure charges those columns once per row: a run four times as wide costs
/// about four times as much, never the width's square.
#[test]
fn a_dense_run_charges_its_output_columns_once_per_row() {
    let points = 32;
    let grid = budget_grid(points);
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    // Both operands hold every field at every point, so every field of the single result series
    // keeps every point.
    let operands = |fields: usize| -> (Arc<SeriesSchema>, Vec<Series>, Vec<Series>) {
        let schema = Arc::new(schema(fields, &grid));
        let side = |name: &str, value: f64| -> Vec<Series> {
            let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
            builder.begin_series();
            let row = vec![Some(value); fields];
            for point in 0..points {
                builder.push(point, &row).unwrap();
            }
            builder
                .finish(metric(&Labels::from([
                    ("__name__".to_string(), name.to_string()),
                    ("job".to_string(), "x".to_string()),
                ])))
                .unwrap()
                .into_iter()
                .collect()
        };
        let a = side("a", 1.0);
        let b = side("b", 2.0);
        (schema, a, b)
    };
    let run = |fields: usize, limit: usize| {
        let (schema, lhs, rhs) = operands(fields);
        budgeted(&matching, &schema, &lhs, &schema, &rhs, limit)
    };
    let measure = |fields: usize| -> usize {
        let (output, seen) = run(fields, usize::MAX);
        let output = output.expect("the fields pair");
        assert_eq!(output.len(), 1, "{fields} fields: {output:?}");
        assert_eq!(
            run_ranges(&output[0]),
            vec![(0, points)],
            "{fields} fields answer every point"
        );
        // Every field of the single result series holds the sum of both operands at every point.
        for field in 0..fields {
            let mut cursor = output[0].fields.as_slice()[field].cursor();
            let mut rows = 0;
            for (_, row) in output[0].time.positions() {
                match cursor.at(row).expect("a row never goes back") {
                    SampleRef::Float(value) => assert_eq!(value, 3.0, "{fields} fields"),
                    _ => panic!("a field of the sum holds no float"),
                }
                rows += 1;
            }
            assert_eq!(rows, points, "{fields} fields answer every point");
        }
        *seen
            .iter()
            .max()
            .expect("the operation asks for its workspace")
    };
    // What one field of this run costs: its rows, its slices of the triples, its columns and its
    // share of the builder. Every term grows with the width and none with its square, so the cost
    // per field is what stays put as the run widens.
    let narrow = measure(4);
    let wide = measure(64);
    assert!(
        wide <= 2 * (64 / 4) * narrow,
        "sixteen times the fields cost {wide} beside the {narrow} four cost: a figure that charged \
         every field's columns to every field's sample multiplies the cost per field by the width"
    );
    // The columns are part of the figure, so it is not trivially small.
    assert!(
        narrow >= points * 4 * (std::mem::size_of::<f64>() + 1),
        "four fields cost {narrow}, less than their columns"
    );
    // A run of every width is admitted by a limit in the order of its columns, not of the width's
    // square: the widest dense result is not a phantom megabyte per field.
    for fields in [1, 4, 16, 64] {
        assert!(
            run(fields, 2 * 1024 * 1024).0.is_ok(),
            "{fields} fields are refused under two megabytes, {wide} bytes for sixty-four"
        );
    }
}

/// Fields that hold different points of one label set still write every field's column at every
/// output row: the figure charges the schema's fields per row, so a run whose fields never share a
/// point is covered as fully as a dense one, and each field keeps its own values.
#[test]
fn fields_holding_disjoint_points_are_charged_their_columns() {
    let points = 64;
    let fields = 4;
    let grid = budget_grid(points);
    let schema = Arc::new(schema(fields, &grid));
    // Field `f` holds a value only at the points `f` apart, so the four fields share no point and
    // each output row fills one field's column and leaves the other three null.
    let side = |name: &str, offset: f64| -> Vec<Series> {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        builder.begin_series();
        for point in 0..points {
            let mut row = vec![None; fields];
            row[point % fields] = Some(point as f64 + offset);
            builder.push(point, &row).unwrap();
        }
        builder
            .finish(metric(&Labels::from([
                ("__name__".to_string(), name.to_string()),
                ("job".to_string(), "x".to_string()),
            ])))
            .unwrap()
            .into_iter()
            .collect()
    };
    let lhs = side("a", 1.0);
    let rhs = side("b", 2.0);
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    let (output, seen) = budgeted(&matching, &schema, &lhs, &schema, &rhs, usize::MAX);
    let output = output.expect("every point of every field is kept");
    assert_eq!(output.len(), 1, "{output:?}");
    assert_eq!(
        run_ranges(&output[0]),
        vec![(0, points)],
        "one series over every point"
    );
    // Each point holds the sum of both operands in the field it belongs to and nothing elsewhere.
    let mut cursors: Vec<RowCursor<'_>> = output[0]
        .fields
        .iter()
        .map(|field| field.cursor())
        .collect();
    for (point, row) in output[0].time.positions() {
        for (field, cursor) in cursors.iter_mut().enumerate() {
            let value = match cursor.at(row).expect("a row never goes back") {
                SampleRef::Missing => None,
                SampleRef::Float(value) => Some(value),
                SampleRef::Histogram(_) => panic!("a float field holds no histogram"),
            };
            // The lhs holds `point + 1` in its field and the rhs `point + 2`, so their sum is
            // `2 * point + 3`, in the field the point belongs to and nowhere else.
            let expected = (field == point % fields).then_some(2.0 * point as f64 + 3.0);
            assert_eq!(value, expected, "point {point}, field {field}");
        }
    }
    // The figure covers the composite a row costs: the rows the fields filed, the triple of each
    // kept sample and the value and validity columns every field writes at every output row --
    // the columns of the fields a row leaves empty included, since the builder writes them too. No
    // row filling more than one field lets the columns be divided among the fields.
    let peak = *seen
        .iter()
        .max()
        .expect("the operation asks for its workspace");
    let filed = points * RESULT_ROW_BYTES;
    let triples = points * 2 * std::mem::size_of::<(usize, usize, GridSample)>();
    let columns = points * fields * (std::mem::size_of::<f64>() + 1);
    assert!(
        peak >= filed + triples + columns,
        "peak {peak} below the {filed} rows, {triples} triples and {columns} columns it holds"
    );
    assert!(
        peak <= points * (fields * 2 * (std::mem::size_of::<f64>() + 1) + 4 * RESULT_ROW_BYTES)
            + 128 * 1024,
        "peak {peak} for {points} rows of {fields} fields"
    );
}

/// A duplicate scan is charged the points it collects: a group whose members carry no sample
/// collects nothing, however long their axes are, while the points a group does carry are its own
/// slots.
#[test]
fn a_duplicate_scan_is_charged_the_points_it_collects() {
    let points = 100_000;
    let grid = budget_grid(points);
    let schema = Arc::new(schema(1, &grid));
    // A member of the group with no value at any point: its axis is the whole grid.
    let null_member = |name: &str| -> Series {
        Series {
            metric: metric(&Labels::from([
                ("__name__".to_string(), name.to_string()),
                ("job".to_string(), "x".to_string()),
            ])),
            time: TimeAxis::Grid(GridRange::try_new(grid.clone(), 0, points).unwrap()),
            fields: SeriesFields::one(Samples::from_float(Float64Array::new_null(points))),
        }
    };
    let sparse_member = |name: &str, at: &[usize]| -> Series {
        let labels = Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), "x".to_string()),
        ]);
        let values: Points = at
            .iter()
            .map(|&position| (position, f64::to_bits(position as f64)))
            .collect();
        sparse_series(&schema, &labels, &values).expect("the operand holds a value")
    };
    // The left side carries one value, at a point no member carries, so the group is never a
    // duplicate and no row is filed.
    let lhs = vec![sparse_member("a", &[90_000])];
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::On(vec![Arc::from("job")]));
    // What the run holds beside the list: three presence flags per grid point.
    let floor = 3 * points;
    let run = |rhs: &[Series], limit: usize| -> Result<Vec<Series>, DataFusionError> {
        budgeted(&matching, &schema, &lhs, &schema, rhs, limit).0
    };
    // Two members of one group, each with a value at no point: their axes are the whole grid and
    // the list stays empty.
    let empty_group = vec![null_member("b"), null_member("c")];
    assert!(
        binary_vectors(&matching, &schema, &lhs, &schema, &empty_group)
            .expect("no member carries a sample")
            .is_empty()
    );
    let output = run(&empty_group, floor + 64 * 1024).expect("an empty list costs nothing");
    assert!(output.is_empty(), "{output:?}");
    // A few points of each member are the list's own slots, and still fit beside the flags.
    let few = vec![
        sparse_member("b", &[10, 20, 30, 40]),
        sparse_member("c", &[50, 60, 70, 80]),
    ];
    assert!(
        run(&few, floor + 64 * 1024)
            .expect("a few points cost a few slots")
            .is_empty()
    );
    // Thousands of points are charged as they are collected: the same limit refuses them, since
    // their slots alone are tens of kilobytes.
    let many = vec![
        sparse_member("b", &(0..4_000).collect::<Vec<_>>()),
        sparse_member("c", &(4_000..8_000).collect::<Vec<_>>()),
    ];
    let error = run(&many, floor + 64 * 1024)
        .expect_err("the collected points are charged as they are pushed");
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
}

/// A field's histogram index column is charged with the value columns, block of rows by block of
/// rows: a wide result whose fields meet their histograms late still holds those columns beside its
/// value columns, and a figure that only charged a column once its first histogram arrived would
/// miss the rows the builder backfills it over.
#[test]
fn sparse_wide_results_are_charged_their_index_columns() {
    let points = 1_025;
    let fields = 64;
    let grid = budget_grid(points);
    let schema = Arc::new(schema(fields, &grid));
    // The value of a field at its one histogram point.
    let histogram = || {
        GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&OwnedHistogram::parse("0/0/0/0/0/0/////").view())
                .expect("an empty histogram is readable"),
        ))
    };
    // Field `f` holds a value only at the points `f` apart: a float at every one of them but the
    // last, and an empty histogram at its single point of the last `fields` rows. Every field meets
    // its first histogram late and keeps the column to the end.
    let side = |name: &str, offset: f64| -> Vec<Series> {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        builder.begin_series();
        for point in 0..points {
            let mut row: Vec<Option<GridSample>> = Vec::with_capacity(fields);
            row.resize_with(fields, || None);
            row[point % fields] = Some(if point >= points - fields {
                histogram()
            } else {
                GridSample::Float(point as f64 + offset)
            });
            builder.push_samples(point, &row).unwrap();
        }
        builder
            .finish(metric(&Labels::from([
                ("__name__".to_string(), name.to_string()),
                ("job".to_string(), "x".to_string()),
            ])))
            .unwrap()
            .into_iter()
            .collect()
    };
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    let (output, seen) = budgeted(
        &matching,
        &schema,
        &side("a", 1.0),
        &schema,
        &side("b", 2.0),
        usize::MAX,
    );
    let output = output.expect("every field adds up");
    assert_eq!(output.len(), 1, "{output:?}");
    // Every point keeps the field it belongs to and nothing in the others, the floats add up and
    // the empty histograms still hold an empty histogram.
    let mut cursors: Vec<RowCursor<'_>> = output[0]
        .fields
        .iter()
        .map(|field| field.cursor())
        .collect();
    let mut rows = 0;
    for (point, row) in output[0].time.positions() {
        rows += 1;
        for (field, cursor) in cursors.iter_mut().enumerate() {
            let owned = field == point % fields;
            match cursor.at(row).expect("a row never goes back") {
                SampleRef::Float(value) => {
                    assert!(owned, "point {point} keeps field {field}");
                    assert!(point < points - fields, "point {point} is not a histogram");
                    assert_eq!(value, 2.0 * point as f64 + 3.0, "point {point}");
                }
                SampleRef::Histogram(view) => {
                    assert!(owned, "point {point} keeps field {field}");
                    assert!(point >= points - fields, "point {point} is a histogram");
                    assert_eq!(view.count, 0.0, "empty histograms add up empty");
                    assert!(
                        view.positive.buckets.is_empty() && view.negative.buckets.is_empty(),
                        "point {point} keeps an empty histogram"
                    );
                }
                SampleRef::Missing => assert!(!owned, "point {point} holds field {field}"),
            }
        }
    }
    assert_eq!(rows, points, "every point is a row");
    // One value column and one index column per field, one entry per output row. The figure the
    // merge reserves is a coarse one -- the block of rows times every field's columns -- so what is
    // checked is that it covers the columns' order of magnitude: every field's index entries are
    // reserved for every row, including the rows before that field's first histogram, and no
    // capacity rule of the builder's is reproduced here.
    let columns =
        fields * points * (std::mem::size_of::<f64>() + std::mem::size_of::<Option<u32>>());
    let peak = *seen
        .iter()
        .max()
        .expect("the operation asks for its workspace");
    assert!(
        peak >= columns,
        "peak {peak} below the {columns} bytes of value and index columns the result holds"
    );
    assert!(
        peak <= 4 * columns,
        "peak {peak} is far more than the {columns} bytes of columns the result holds"
    );
}


/// A comparison filters, so the histogram it keeps is the left operand's, payload included.
///
/// A non-`bool` `==` or `!=` between two histograms keeps the left sample where the comparison
/// holds, so the histogram that leaves is the one that arrived on the left, start timestamp
/// included, and never the right operand's timestamp. `==` holds only for equal values, so that
/// pair differs in reset hint and start timestamp alone; `!=` holds for differing values, so its
/// pair differs in count and sum as well. What this pins is that a comparison passes the stored
/// payload through, not a Prometheus result. A `bool` comparison answers in floats, so nothing it
/// hands out is a histogram and no start timestamp leaves it.
#[test]
fn a_histogram_comparison_filter_keeps_the_payload_of_the_operand_it_keeps() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 3).unwrap()).unwrap());
    let schema = Arc::new(schema(1, &grid));
    // The start timestamp each operand was read with: written into its samples by `operand` below
    // and repeated in every expectation, never read back through the operators under test.
    const LEFT_START: i64 = 1_500;
    const RIGHT_START: i64 = 2_500;
    // count 4, sum 6, one span of two buckets holding 1 and 3, reset hint 2 (`NotCounterReset`).
    let left = "0/0/0/4010000000000000/4018000000000000/2/0_2/3ff0000000000000,4008000000000000///";
    // The same value with reset hint 1 (`CounterReset`): `==` ignores the hint, so the pair is kept
    // and only the hint and the start timestamp say which side survived.
    let equal =
        "0/0/0/4010000000000000/4018000000000000/1/0_2/3ff0000000000000,4008000000000000///";
    // count 5, sum 8: a value that differs, which is what `!=` keeps.
    let other =
        "0/0/0/4014000000000000/4020000000000000/2/0_2/4000000000000000,4008000000000000///";
    let operand = |name: &str, job: &str, text: &str, start: i64| -> Series {
        let labels = Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), job.to_string()),
        ]);
        let histogram = OwnedHistogram::parse(text);
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        for point in [0, 2] {
            let mut read = FloatHistogram::try_from_view(&histogram.view()).unwrap();
            read.start_timestamp = Some(start);
            builder
                .push_samples(point, &[Some(GridSample::Histogram(Box::new(read)))])
                .unwrap();
        }
        builder.finish(metric(&labels)).unwrap().unwrap()
    };
    // Two pairs, one match group each: `job=eq` holds histograms `==` keeps, `job=ne` ones `!=`
    // keeps, so each comparison passes exactly one pair and its result is named by that pair.
    let lhs = [
        operand("eq", "eq", left, LEFT_START),
        operand("ne", "ne", left, LEFT_START),
    ];
    let rhs = [
        operand("eq_right", "eq", equal, RIGHT_START),
        operand("ne_right", "ne", other, RIGHT_START),
    ];
    // Every histogram a comparison handed out, by the series and point it sits at, with the reset
    // hint as the number the fixtures wrote.
    let kept = |series: &[Series]| -> Vec<(String, usize, f64, f64, Option<i64>, i32)> {
        let mut kept: Vec<(String, usize, f64, f64, Option<i64>, i32)> = Vec::new();
        for series in series {
            let name = series.metric.name().unwrap_or_default().to_string();
            let mut cursor = series.fields.as_slice()[0].cursor();
            for (point, row) in series.time.positions() {
                match cursor.at(row).expect("the axis and the fields are aligned") {
                    SampleRef::Histogram(view) => kept.push((
                        name.clone(),
                        point,
                        view.count,
                        view.sum,
                        view.start_timestamp,
                        i32::from(view.reset_hint),
                    )),
                    _ => panic!("a comparison handed out a sample that is not a histogram"),
                }
            }
        }
        kept.sort_by(|a, b| (a.0.as_str(), a.1).cmp(&(b.0.as_str(), b.1)));
        kept
    };
    let matching = |op: BinaryOp| BinaryMatching {
        op,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    };

    let eq = binary_vectors(&matching(BinaryOp::Eq), &schema, &lhs, &schema, &rhs).unwrap();
    assert_eq!(
        kept(&eq),
        [
            ("eq".to_string(), 0, 4.0, 6.0, Some(LEFT_START), 2),
            ("eq".to_string(), 2, 4.0, 6.0, Some(LEFT_START), 2),
        ],
        "`==` did not hand back the left histogram it kept: {eq:?}"
    );

    let ne = binary_vectors(&matching(BinaryOp::Ne), &schema, &lhs, &schema, &rhs).unwrap();
    assert_eq!(
        kept(&ne),
        [
            ("ne".to_string(), 0, 4.0, 6.0, Some(LEFT_START), 2),
            ("ne".to_string(), 2, 4.0, 6.0, Some(LEFT_START), 2),
        ],
        "`!=` did not hand back the left histogram it kept: {ne:?}"
    );

    // The right operand's start timestamp is never what comes out, at any point of either
    // comparison.
    for series in eq.iter().chain(&ne) {
        let mut cursor = series.fields.as_slice()[0].cursor();
        for (point, row) in series.time.positions() {
            let SampleRef::Histogram(view) =
                cursor.at(row).expect("the axis and the fields are aligned")
            else {
                panic!("a comparison handed out a sample that is not a histogram");
            };
            assert_ne!(
                view.start_timestamp,
                Some(RIGHT_START),
                "the right operand's start timestamp came out at {point}: {series:?}"
            );
        }
    }

    // A `bool` comparison answers in floats, so nothing it hands out is a histogram and no start
    // timestamp leaves it: `job=eq` holds equal histograms, so it answers 1, and `job=ne` differs
    // in count and sum, so it answers 0.
    let bool_matching = BinaryMatching {
        return_bool: true,
        ..matching(BinaryOp::Eq)
    };
    let compared = binary_vectors(&bool_matching, &schema, &lhs, &schema, &rhs).unwrap();
    let answers = |series: &[Series]| -> Vec<(String, usize, f64)> {
        let mut answers: Vec<(String, usize, f64)> = Vec::new();
        for series in series {
            let job = series.metric.get("job").unwrap_or_default().to_string();
            let mut cursor = series.fields.as_slice()[0].cursor();
            for (point, row) in series.time.positions() {
                match cursor.at(row).expect("the axis and the fields are aligned") {
                    SampleRef::Float(value) => answers.push((job.clone(), point, value)),
                    _ => panic!("a `bool` comparison handed out a sample that is not a float"),
                }
            }
        }
        answers.sort_by(|a, b| (a.0.as_str(), a.1).cmp(&(b.0.as_str(), b.1)));
        answers
    };
    assert_eq!(
        answers(&compared),
        [
            ("eq".to_string(), 0, 1.0),
            ("eq".to_string(), 2, 1.0),
            ("ne".to_string(), 0, 0.0),
            ("ne".to_string(), 2, 0.0),
        ],
        "a `bool` comparison of histograms did not answer in floats: {compared:?}"
    );
}

/// A set operation hands back the sample it kept, start timestamp included.
///
/// `and`, `unless` and `or` choose samples rather than compute them, so each histogram that leaves
/// them is the histogram that arrived, payload included. The two operands were read with different
/// start timestamps and hold different counts and sums, so the count, the sum and the timestamp
/// together name the side and the point a kept sample came from. The one exception is `or` at a
/// point where the left side holds no sample: the sample kept is the right side's, and it carries
/// the right side's own start timestamp. What this pins is that the operators pass the stored
/// payload through, not a Prometheus result.
#[test]
fn a_set_operation_keeps_the_payload_of_the_sample_it_keeps() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 4).unwrap()).unwrap());
    let schema = Arc::new(schema(1, &grid));
    // The start timestamp each operand was read with: written into its samples by `operand` below
    // and repeated in every expectation, never read back through the operators under test.
    const LHS_START: i64 = 1_500;
    const RHS_START: i64 = 2_500;
    // count 4, sum 6, one span of two buckets holding 1 and 3.
    let lhs_text =
        "0/0/0/4010000000000000/4018000000000000/2/0_2/3ff0000000000000,4008000000000000///";
    // count 5, sum 8, so the count and the sum name the side a kept sample came from as well.
    let rhs_text =
        "0/0/0/4014000000000000/4020000000000000/2/0_2/4000000000000000,4008000000000000///";
    let operand = |name: &str, text: &str, start: i64, points: &[usize]| -> Series {
        let labels = Labels::from([
            ("__name__".to_string(), name.to_string()),
            ("job".to_string(), "x".to_string()),
        ]);
        let histogram = OwnedHistogram::parse(text);
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        for &point in points {
            let mut read = FloatHistogram::try_from_view(&histogram.view()).unwrap();
            read.start_timestamp = Some(start);
            builder
                .push_samples(point, &[Some(GridSample::Histogram(Box::new(read)))])
                .unwrap();
        }
        builder.finish(metric(&labels)).unwrap().unwrap()
    };
    // The left side holds 0 and 1, the right side 0 and 2, so each operator keeps a different set:
    // the shared point for `and`, the left-only point for `unless`, and the union for `or`.
    let lhs = operand("l", lhs_text, LHS_START, &[0, 1]);
    let rhs = operand("r", rhs_text, RHS_START, &[0, 2]);
    // Every histogram an operator handed out, by the series and point it sits at.
    let kept = |series: &[Series]| -> Vec<(String, usize, f64, f64, Option<i64>)> {
        let mut kept: Vec<(String, usize, f64, f64, Option<i64>)> = Vec::new();
        for series in series {
            let name = series.metric.name().unwrap_or_default().to_string();
            let mut cursor = series.fields.as_slice()[0].cursor();
            for (point, row) in series.time.positions() {
                match cursor.at(row).expect("the axis and the fields are aligned") {
                    SampleRef::Histogram(view) => kept.push((
                        name.clone(),
                        point,
                        view.count,
                        view.sum,
                        view.start_timestamp,
                    )),
                    _ => panic!("a set operator handed out a sample that is not a histogram"),
                }
            }
        }
        kept.sort_by(|a, b| (a.0.as_str(), a.1).cmp(&(b.0.as_str(), b.1)));
        kept
    };
    let labels = MatchingLabels::Ignoring(Vec::new());
    let eval = |op: SetOp| {
        set_vectors(
            op,
            &labels,
            &schema,
            std::slice::from_ref(&lhs),
            &schema,
            std::slice::from_ref(&rhs),
        )
        .unwrap()
    };

    let and = eval(SetOp::And);
    assert_eq!(
        kept(&and),
        [("l".to_string(), 0, 4.0, 6.0, Some(LHS_START))],
        "`and` did not hand back the left sample it kept: {and:?}"
    );

    let unless = eval(SetOp::Unless);
    assert_eq!(
        kept(&unless),
        [("l".to_string(), 1, 4.0, 6.0, Some(LHS_START))],
        "`unless` did not hand back the left sample it kept: {unless:?}"
    );

    // The right side comes back at point 2, where the left side holds no sample, and carries the
    // right side's own start timestamp -- the one case where the payload kept is not the left one.
    let or = eval(SetOp::Or);
    assert_eq!(
        kept(&or),
        [
            ("l".to_string(), 0, 4.0, 6.0, Some(LHS_START)),
            ("l".to_string(), 1, 4.0, 6.0, Some(LHS_START)),
            ("r".to_string(), 2, 5.0, 8.0, Some(RHS_START)),
        ],
        "`or` did not hand back each side's own sample: {or:?}"
    );
}
