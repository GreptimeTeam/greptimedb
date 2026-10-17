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
use datatypes::series::samples::{FieldSpec, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
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
        labels.remove("__name__");
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
            assert!(!present(&dense[point]), "result label sets collide");
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
