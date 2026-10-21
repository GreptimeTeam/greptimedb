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
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels, SetOp, binary_vectors,
    set_vectors,
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
