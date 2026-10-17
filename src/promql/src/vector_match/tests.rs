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
use datatypes::series::samples::{FieldSpec, SampleRef, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{
    BinaryMatching, BinaryOp, Cardinality, MatchingLabels, SetOp, binary_vectors, set_vectors,
};
use crate::grid_series::GridSeriesBuilder;
use crate::grid_series::reference::{Dense, assert_series_eq, random_dense};

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
        let values = random_dense(rng, grid.timestamps().len(), fields);
        for (index, row) in values.iter().enumerate() {
            builder.push(index, row).unwrap();
        }
        if let Some(built) = builder.finish(metric(&labels)).unwrap() {
            series.push(built);
            dense.push((labels, values));
        }
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

fn present_at(side: &Side, point: usize) -> Vec<&(Labels, Dense)> {
    side.dense
        .iter()
        .filter(|(_, dense)| present(&dense[point]))
        .collect()
}

/// Prometheus `VectorBinop` evaluated on its own at every point, collecting every violation
/// instead of stopping at the first.
fn reference_binary(
    matching: &BinaryMatching,
    lhs: &Side,
    rhs: &Side,
    points: usize,
    fields: usize,
) -> Expected {
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

/// Prometheus `VectorAnd`, `VectorOr` and `VectorUnless` at every point.
fn reference_set(
    op: SetOp,
    labels: &MatchingLabels,
    lhs: &Side,
    rhs: &Side,
    points: usize,
) -> Expected {
    let fields = lhs.schema.fields.len();
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
            let actual: BTreeMap<Labels, Series> = actual
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
                assert_series_eq(actual.get(labels), dense, &metric(labels));
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
        let expected = reference_binary(&matching, &lhs, &rhs, grid.timestamps().len(), fields);
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
        let expected = reference_set(op, &labels, &lhs, &rhs, grid.timestamps().len());
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
        for &point in points {
            builder.push(point, &[Some(point as f64)]).unwrap();
        }
        let labels = labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        builder.finish(metric(&labels)).unwrap().unwrap()
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
    let TimeAxis::Grid(range) = &series.time else {
        panic!("expected a grid series");
    };
    series.fields.as_slice()[0]
        .iter()
        .enumerate()
        .filter_map(|(row, sample)| match sample {
            SampleRef::Float(value) => Some((range.start() + row, canonical_bits(value))),
            _ => None,
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
                    for point in 0..points as usize {
                        let value = values.get(&point).map(|bits| f64::from_bits(*bits));
                        builder.push(point, &[value]).unwrap();
                    }
                    let Some(series) = builder.finish(metric(&labels)).unwrap() else {
                        continue;
                    };
                    if labels.get("__name__").map(String::as_str) == Some("a") {
                        lhs.push(series);
                    } else {
                        rhs.push(series);
                    }
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
                let actual: BTreeMap<Labels, BTreeMap<usize, u64>> = actual
                    .iter()
                    .map(|series| (labels_of(&series.metric), points_of(series)))
                    .collect();
                assert_close(&actual, &expected, case);
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
        builder.push(0, &[Some(1.0)]).unwrap();
        let labels = labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        builder.finish(metric(&labels)).unwrap().unwrap()
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
