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

use datafusion::error::DataFusionError;
use datatypes::series::samples::{FieldSpec, RowCursor, SampleRef, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{
    BinaryMatching, BinaryOp, Cardinality, MatchingLabels, SetOp, binary_vectors,
    binary_vectors_with_reservation, set_vectors,
};
use crate::grid_series::GridSeriesBuilder;
use crate::grid_series::reference::{
    Dense, OwnedHistogram, Points, assert_histogram_close, assert_result_eq, golden_cases,
    mixed_series, piece_ranges, random_dense, run_ranges, sparse_series,
};
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
        // The budgeted entry runs the same kernel: it admits the workspace it asks for, asks for a
        // growing total and returns what the public form returns, error text included.
        let (reserved, seen) = budgeted(
            &matching,
            &lhs.schema,
            &lhs.series,
            &rhs.schema,
            &rhs.series,
            usize::MAX,
        );
        assert!(
            seen.windows(2).all(|pair| pair[0] < pair[1]),
            "seed {seed}: {seen:?}"
        );
        assert_same_result(
            reserved,
            binary_vectors(
                &matching,
                &lhs.schema,
                &lhs.series,
                &rhs.schema,
                &rhs.series,
            ),
            &format!("seed {seed}"),
        );
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
    let text = include_str!("prometheus_cases.txt");
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
    assert!(
        matched > 100 && failed > 20,
        "{matched} matched, {failed} failed"
    );
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
        };
        let result = binary_vectors(&matching, &schema, &lhs, &schema, &rhs).unwrap();
        assert_eq!(result.len(), 1, "{op:?}");
    }
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

fn budget_matching(op: BinaryOp, labels: MatchingLabels) -> BinaryMatching {
    BinaryMatching {
        op,
        return_bool: false,
        labels,
        cardinality: Cardinality::OneToOne,
    }
}

/// Runs a binary operation with a budget admitting at most `limit` bytes, recording every total it
/// is asked for.
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

/// The workspace a binary operation asks for last: the total it needs admitted in all.
fn budget_total(
    matching: &BinaryMatching,
    schema: &SeriesSchema,
    lhs: &[Series],
    rhs: &[Series],
) -> usize {
    let (output, seen) = budgeted(matching, schema, lhs, schema, rhs, usize::MAX);
    assert!(output.is_ok(), "{output:?}");
    *seen.last().expect("the operation asks for its workspace")
}

/// One field's values of one label set, by grid point, as the bits they were computed with.
type FieldValues = BTreeMap<usize, u64>;

/// A float result: every label set with the values of each of its fields.
type FloatOutput = BTreeMap<Labels, Vec<FieldValues>>;

/// Every label set of a float result with its values per field and grid point, so two runs of the
/// same operation compare regardless of the order their label sets came back in.
fn float_output(series: Vec<Series>) -> FloatOutput {
    let mut output: FloatOutput = BTreeMap::new();
    for series in crate::golden::fold_pieces(series) {
        let width = series.fields.as_slice().len();
        let entry = output
            .entry(labels_of(&series.metric))
            .or_insert_with(|| vec![BTreeMap::new(); width]);
        for (field, samples) in series.fields.as_slice().iter().enumerate() {
            let mut cursor = samples.cursor();
            for (point, row) in series.time.positions() {
                match cursor.at(row).expect("the axis and the fields are aligned") {
                    SampleRef::Missing => {}
                    SampleRef::Float(value) => {
                        entry[field].insert(point, value.to_bits());
                    }
                    SampleRef::Histogram(_) => panic!("a float result holds no histogram"),
                }
            }
        }
    }
    output
}

/// Asserts two runs of one operation agree: the same label sets with the same values, or the same
/// error.
fn assert_same_result(
    actual: Result<Vec<Series>, DataFusionError>,
    expected: Result<Vec<Series>, DataFusionError>,
    context: &str,
) {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            assert_eq!(float_output(actual), float_output(expected), "{context}")
        }
        (Err(actual), Err(expected)) => {
            assert_eq!(actual.to_string(), expected.to_string(), "{context}")
        }
        (actual, expected) => panic!("{context}: got {actual:?}, expected {expected:?}"),
    }
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
            *seen.last().expect("the operation asks for its workspace"),
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
/// the group and keeps those rows until it reports the duplicate.
#[test]
fn a_duplicated_group_is_charged_the_rows_it_files() {
    let grid = budget_grid(32);
    let schema = Arc::new(schema(1, &grid));
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::On(vec![Arc::from("job")]));
    let at: Vec<usize> = (0..32).collect();
    let one = vec![budget_operand(&schema, "b", &at)];
    let total = |many: &[Series]| -> usize {
        let (output, seen) = budgeted(&matching, &schema, many, &schema, &one, usize::MAX);
        assert!(output.is_err(), "a duplicated group is rejected");
        *seen.last().expect("the operation asks for its workspace")
    };
    let single = total(&[budget_operand(&schema, "a", &at)]);
    let duplicated = total(&[
        budget_operand(&schema, "a", &at),
        budget_operand(&schema, "c", &at),
    ]);
    // The second member pairs at every position, so at least the slots of those 32 rows are charged
    // on top of the single pairing.
    assert!(
        duplicated >= single + 32 * FILED_ROW,
        "duplicated {duplicated} vs single {single}"
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
    };
    let lhs = vec![series("a", text)];
    let rhs = vec![series("b", text)];
    let run = |op: BinaryOp, return_bool: bool| {
        budgeted(
            &matching(op, return_bool),
            &schema,
            &lhs,
            &schema,
            &rhs,
            usize::MAX,
        )
    };
    let (added, seen) = run(BinaryOp::Add, false);
    let added = added.expect("`+` keeps the pair");
    assert_eq!(added.len(), 1, "{added:?}");
    let added_total = *seen.last().unwrap();
    // `%` is one of the pairings Prometheus skips: nothing of it is retained, so nothing of it is
    // charged as output.
    let (skipped, seen) = run(BinaryOp::Mod, false);
    assert!(
        skipped
            .expect("a skipped pairing is not an error")
            .is_empty()
    );
    let skipped_total = *seen.last().unwrap();
    assert!(
        added_total > skipped_total,
        "added {added_total}, skipped {skipped_total}"
    );
    // A `bool` comparison keeps a float, so its rows hold no histogram heap.
    let (booled, seen) = run(BinaryOp::Ne, true);
    let booled = booled.expect("a `bool` comparison holds");
    assert_eq!(booled.len(), 1, "{booled:?}");
    let booled_total = *seen.last().unwrap();
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
    let empty_operands = |text: &str| -> Vec<Series> { vec![series("a", text), series("b", text)] };
    let run = |op: BinaryOp, operands: &[Series]| {
        budgeted(
            &matching(op, false),
            &schema,
            operands,
            &schema,
            operands,
            usize::MAX,
        )
    };
    let (kept, kept_seen) = run(BinaryOp::Add, &empty_operands(empty));
    assert_eq!(
        kept.expect("an empty histogram is a histogram").len(),
        1,
        "an empty histogram keeps its row"
    );
    // The same operands under an operator the pair does not support keep nothing.
    let (unkept, unkept_seen) = run(BinaryOp::Mod, &empty_operands(empty));
    assert!(
        unkept
            .expect("a skipped pairing is not an error")
            .is_empty()
    );
    assert!(
        *kept_seen.last().unwrap() > *unkept_seen.last().unwrap(),
        "an empty histogram keeps its row"
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
        *seen.last().expect("the operation asks for its workspace")
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

/// A budget below what the operation needs stops it at whichever step it runs out, and the totals it
/// asks for never go back.
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
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]), "{seen:?}");
    // The last total it asks for is enough, and every earlier total is not: the operation outgrows
    // it in the tables, the rows or the merge.
    let needed = *seen.last().unwrap();
    assert!(
        budgeted(&matching, &schema, &lhs, &schema, &rhs, needed)
            .0
            .is_ok()
    );
    for &limit in &seen[..seen.len() - 1] {
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

/// An operand that disagrees with its schema is rejected exactly as the public form rejects it,
/// before any field's tables are admitted for it.
#[test]
fn a_budgeted_run_reports_the_public_rejection() {
    let grid = budget_grid(4);
    // A schema with no fields: the kernel's field loop would not run a single time.
    let empty = Arc::new(schema(0, &grid));
    let one = Arc::new(schema(1, &grid));
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let points: Points = BTreeMap::from([(0, f64::to_bits(1.0))]);
    let series = vec![sparse_series(&one, &labels, &points).expect("the operand holds a value")];
    let matching = budget_matching(BinaryOp::Add, MatchingLabels::Ignoring(Vec::new()));
    let public = binary_vectors(&matching, &empty, &series, &empty, &[]).unwrap_err();
    let (budgeted, seen) = budgeted(&matching, &empty, &series, &empty, &[], usize::MAX);
    assert_eq!(budgeted.unwrap_err().to_string(), public.to_string());
    // Only the output descriptors were admitted: no field was reserved for an operand the kernel
    // cannot read.
    assert_eq!(seen.len(), 1, "{seen:?}");
}
