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

use std::collections::BTreeMap;

use datafusion::error::DataFusionError;
use datatypes::arrow::array::{
    Array, BooleanArray, Float64Array, ListArray, StructArray, UInt32Array,
};
use datatypes::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datatypes::arrow::datatypes::{DataType, Field};
use datatypes::series::Label;
use datatypes::series::aggregate_state::{
    AggregateStateKind, AggregateStateSchema, StateComponent, StateFieldsBatch, StatePayload,
    StateSeries,
};
use datatypes::series::histogram::{
    BucketSideView, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields};
use datatypes::series::time::{EvalGrid, GridRange, GridSpan, GridSpec, TimeAxis};

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use super::*;
use crate::aggregate::state::StateRow;
use crate::golden::{assert_result, cases};
use crate::grid_series::GridSample;
use crate::grid_series::reference::{Labels, piece_ranges};

const CASES: &str = include_str!("prometheus_cases.txt");

/// A label set as name/value pairs, the way the fixture names one.
type LabelKey = Vec<(Arc<str>, Arc<str>)>;

/// The valued positions of one label set, by grid index.
type Positions = Vec<(usize, u64)>;

const OPS: [AggregateOp; 6] = [
    AggregateOp::Sum,
    AggregateOp::Count,
    AggregateOp::Avg,
    AggregateOp::Min,
    AggregateOp::Max,
    AggregateOp::Group,
];

fn schema(fields: &[&str]) -> Arc<SeriesSchema> {
    schema_with_grid_len(fields, 128)
}

fn schema_with_grid_len(fields: &[&str], len: u64) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields
            .iter()
            .map(|name| FieldSpec {
                name: (*name).into(),
            })
            .collect(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(-1000, 250, len).unwrap()).unwrap(),
        )),
    })
}

fn metric(name: &str, labels: &[(&str, &str)]) -> SeriesLabels {
    SeriesLabels::try_new(
        Some(name.into()),
        labels
            .iter()
            .map(|(name, value)| Label {
                name: (*name).into(),
                value: (*value).into(),
            })
            .collect(),
    )
    .unwrap()
}

/// Each field is split at a different offset into chunks with nonzero Arrow offsets.
fn input(
    schema: &SeriesSchema,
    metric: SeriesLabels,
    start: usize,
    fields: &[Vec<Option<f64>>],
) -> Series {
    let len = fields[0].len();
    let fields = fields
        .iter()
        .enumerate()
        .map(|(index, samples)| {
            let mut padded = vec![Some(999.0)];
            padded.extend(samples);
            padded.push(Some(999.0));
            let array = Float64Array::from(padded);
            let split = (index + 1).min(len);
            Samples::try_from_chunks(vec![
                Samples::from_float(array.slice(1, split)),
                Samples::from_float(array.slice(1 + split, len - split)),
            ])
            .unwrap()
        })
        .collect();
    Series::try_new(
        schema,
        metric,
        TimeAxis::Grid(GridRange::try_new(schema.eval_grid.clone().unwrap(), start, len).unwrap()),
        SeriesFields::try_from_vec(fields).unwrap(),
    )
    .unwrap()
}

/// A series whose axis carries only `positions`, in one field per row of `fields`.
///
/// The rows are the positions the runs hold, so a gap between them is a hole in the axis rather
/// than a null sample: the fold must reach them by absolute position.
fn sparse_input(
    schema: &SeriesSchema,
    metric: SeriesLabels,
    positions: &[usize],
    fields: &[Vec<Option<f64>>],
) -> Series {
    let mut spans: Vec<GridSpan> = Vec::new();
    for &position in positions {
        match spans.last_mut() {
            Some(span) if span.start() + span.len() == position => {
                *span = GridSpan::new(span.start(), span.len() + 1);
            }
            _ => spans.push(GridSpan::new(position, 1)),
        }
    }
    let time = TimeAxis::from_grid_spans(schema.eval_grid.clone().unwrap(), spans).unwrap();
    let fields = fields
        .iter()
        .map(|values| Samples::from_float(Float64Array::from(values.clone())))
        .collect();
    Series::try_new(
        schema,
        metric,
        time,
        SeriesFields::try_from_vec(fields).unwrap(),
    )
    .unwrap()
}

/// Every valued position of `pieces`, stitched into one dense vector over the grid.
///
/// `aggregate_float` returns one piece per run of consecutive positions that hold a result, so a
/// group whose values are interrupted arrives as several pieces; the first position and the
/// value vector are what the assertions here compare.
fn values_of(pieces: &[Series], field: usize, grid_len: usize) -> (usize, Vec<Option<f64>>) {
    let mut dense = vec![None; grid_len];
    for piece in pieces {
        // The axis says which position each row holds, so a piece of several runs is read at the
        // positions it carries instead of from its first one onwards.
        let mut cursor = piece
            .fields
            .get(field)
            .unwrap_or_else(|| panic!("lost field {field}: {:?}", piece.metric))
            .cursor();
        for (position, row) in piece.time.positions() {
            let value = match cursor.at(row).expect("the axis and the fields are aligned") {
                SampleRef::Float(value) => Some(value),
                SampleRef::Missing => None,
                _ => panic!("unexpected histogram"),
            };
            if value.is_some() {
                assert!(dense[position].is_none(), "two pieces overlap");
                dense[position] = value;
            }
        }
    }
    let first = dense.iter().position(Option::is_some).unwrap_or(0);
    let last = dense.iter().rposition(Option::is_some).map_or(0, |i| i + 1);
    (first, dense[first..last.max(first)].to_vec())
}

/// The first position `series` carries and its values, read row by row from the axis.
///
/// The axis says which positions the rows hold, so a result of several runs reports its first
/// carried position and its compact rows, instead of assuming the rows start there and are
/// adjacent.
fn values(series: &Series, field: usize) -> (usize, Vec<Option<f64>>) {
    let start = series
        .time
        .first_position()
        .unwrap_or_else(|| panic!("lost grid: {:?}", series.time));
    let values = series
        .fields
        .get(field)
        .unwrap()
        .iter()
        .map(|sample| match sample {
            SampleRef::Float(value) => Some(value),
            SampleRef::Missing => None,
            _ => panic!("unexpected histogram"),
        })
        .collect();
    (start, values)
}

fn grouping<'a>(header: &[String], labels: &'a [&'a str]) -> Grouping<'a> {
    match header[2].as_str() {
        "none" | "by" => Grouping::By(labels),
        "without" => Grouping::Without(labels),
        other => panic!("unmapped grouping {other}"),
    }
}

#[test]
fn aggregates_match_prometheus() {
    let mut matched = BTreeMap::<String, usize>::new();
    for case in cases(CASES) {
        let h = &case.header;
        let op = match h[1].as_str() {
            "sum" => AggregateOp::Sum,
            "count" => AggregateOp::Count,
            "avg" => AggregateOp::Avg,
            "min" => AggregateOp::Min,
            "max" => AggregateOp::Max,
            "group" => AggregateOp::Group,
            other => panic!("unmapped aggregate {other}"),
        };
        let labels = h[3]
            .split(',')
            .filter(|label| *label != "-")
            .collect::<Vec<_>>();
        let output = aggregate_float(&case.schema, &case.series, op, grouping(h, &labels)).unwrap();
        assert_result(&case, output);
        *matched.entry(format!("{} {}", h[1], h[2])).or_default() += 1;
    }
    for op in ["sum", "count", "avg", "min", "max", "group"] {
        for grouping in ["none", "by", "without"] {
            let count = matched
                .get(&format!("{op} {grouping}"))
                .copied()
                .unwrap_or_default();
            assert!(count >= 8, "{op} {grouping}: {count}");
        }
    }
}

/// `stddev`, `stdvar`, `quantile` and `count_values` evaluated by the Prometheus v3.15.0 engine;
/// see the header of `stat_cases.txt`.
#[test]
fn stat_aggregates_match_prometheus() {
    let mut matched = BTreeMap::<String, usize>::new();
    for case in cases(include_str!("stat_cases.txt")) {
        let h = &case.header;
        let labels = h[3]
            .split(',')
            .filter(|label| *label != "-")
            .collect::<Vec<_>>();
        let grouping = grouping(h, &labels);
        let output = match h[1].as_str() {
            "stddev" => aggregate_float(&case.schema, &case.series, AggregateOp::Stddev, grouping),
            "stdvar" => aggregate_float(&case.schema, &case.series, AggregateOp::Stdvar, grouping),
            "quantile" => {
                let q = case.scalar.as_ref().expect("quantile cases carry q");
                aggregate_quantile(&case.schema, &case.series, q, grouping)
            }
            "count_values" => count_values(&case.schema, &case.series, &h[4], grouping),
            other => panic!("unmapped aggregate {other}"),
        };
        assert_result(&case, output.unwrap());
        *matched.entry(format!("{} {}", h[1], h[2])).or_default() += 1;
    }
    for op in ["stddev", "stdvar", "quantile", "count_values"] {
        for grouping in ["none", "by", "without"] {
            let count = matched
                .get(&format!("{op} {grouping}"))
                .copied()
                .unwrap_or_default();
            assert!(count >= 12, "{op} {grouping}: {count}");
        }
    }
}

#[test]
fn compensated_sum_and_overflowing_avg_follow_prometheus() {
    let schema = schema(&["value"]);
    let series = [1e16, 1.0, -1e16]
        .into_iter()
        .enumerate()
        .map(|(i, value)| {
            input(
                &schema,
                metric("m", &[("i", &i.to_string())]),
                3,
                &[vec![Some(value)]],
            )
        })
        .collect::<Vec<_>>();
    // A plain left fold loses the 1.
    let sum = aggregate_float(&schema, &series, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    assert_eq!(values(&sum[0], 0), (3, vec![Some(1.0)]));
    let avg = aggregate_float(&schema, &series, AggregateOp::Avg, Grouping::By(&[])).unwrap();
    assert_eq!(values(&avg[0], 0), (3, vec![Some(1.0 / 3.0)]));

    let series = [f64::MAX, f64::MAX, f64::MAX / 2.0]
        .into_iter()
        .enumerate()
        .map(|(i, value)| {
            input(
                &schema,
                metric("m", &[("i", &i.to_string())]),
                3,
                &[vec![Some(value)]],
            )
        })
        .collect::<Vec<_>>();
    // The direct sum overflows, so the mean switches to the incremental form.
    let avg = aggregate_float(&schema, &series, AggregateOp::Avg, Grouping::By(&[])).unwrap();
    let (_, avg) = values(&avg[0], 0);
    // Bits from the same calculation in Go on amd64; a plain mean is +Inf.
    assert_eq!(avg, vec![Some(f64::from_bits(0x7feaaaaaaaaaaaa9))]);
    let sum = aggregate_float(&schema, &series, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    assert_eq!(values(&sum[0], 0), (3, vec![Some(f64::INFINITY)]));
}

/// Kahan compensation is part of Prometheus' algorithm, but its effect on these means stays
/// within the fixture tolerance, so the Prometheus bits (from `aggregate/prometheus_cases.txt`)
/// are compared exactly.
#[test]
fn avg_compensation_matches_prometheus_bits() {
    let schema = schema(&["value"]);
    let cases: [(&[u64], u64); 3] = [
        // Compensation added to the incremental mean.
        (
            &[
                0x7fefffffffffffff,
                0x7fe2b55f24962471,
                0x7f7b95c7ed3a8f06,
                0x7f75d53a379fb517,
                0x7f71a21ceafb8ae4,
                0xffe1f8b2ce599b68,
            ],
            0x7fc62c8462e82319,
        ),
        // The direct mean divides sum and compensation separately.
        (
            &[
                0x406c76fff31cc498,
                0xc300281b07086372,
                0xc2b33ace5f269b56,
                0xc2807b0afee3570f,
                0x40527b1b3a5c44e6,
            ],
            0xc2daea473b33e9f1,
        ),
        // The compensation is rescaled when the mean switches to the incremental form.
        (
            &[
                0xffe985777be7cf98,
                0x7fb976769842f3a1,
                0xffe6ef0261aba506,
                0x7fbc6cc7f64a3057,
            ],
            0xffd4dc0905e0e810,
        ),
    ];
    for (inputs, expected) in cases {
        let series = inputs
            .iter()
            .enumerate()
            .map(|(i, bits)| {
                input(
                    &schema,
                    metric("a", &[("i", &i.to_string())]),
                    1,
                    &[vec![Some(f64::from_bits(*bits))]],
                )
            })
            .collect::<Vec<_>>();
        let avg = aggregate_float(&schema, &series, AggregateOp::Avg, Grouping::By(&[])).unwrap();
        let (_, avg) = values(&avg[0], 0);
        assert_eq!(avg[0].map(f64::to_bits), Some(expected));
    }
}

#[test]
fn one_field_missing_and_another_nan_keep_row_nullability() {
    let schema = schema(&["missing", "value"]);
    let series = input(
        &schema,
        metric("m", &[]),
        0,
        &[
            vec![None, None, None],
            vec![Some(1.0), Some(f64::NAN), Some(2.0)],
        ],
    );
    let output = aggregate_float(&schema, &[series], AggregateOp::Sum, Grouping::By(&[])).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(values(&output[0], 0).1, vec![None, None, None]);
    let values = values(&output[0], 1).1;
    assert_eq!(values[0], Some(1.0));
    assert!(values[1].unwrap().is_nan());
    assert_eq!(values[2], Some(2.0));
}

#[test]
fn fields_are_aggregated_independently() {
    // A field without input values at a row is null there, even for count and group; the row
    // exists because another field has a value. Single-field projections are the oracle.
    let multi = schema(&["a", "b"]);
    let single = schema(&["value"]);
    let samples = [
        None,
        Some(-0.0),
        Some(0.0),
        Some(-5.0),
        Some(7.5),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(1e16),
    ];
    let mut seed = 17_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (seed >> 33) as usize
    };
    let mut series = Vec::new();
    for i in 0..24 {
        let (start, len) = (next() % 80, 1 + next() % 20);
        let fields = [0, 1].map(|_| (0..len).map(|_| samples[next() % samples.len()]).collect());
        let host = ["h1", "h2", ""][i % 3];
        series.push(input(
            &multi,
            metric(["a", "b"][i % 2], &[("host", host), ("i", &i.to_string())]),
            start,
            &fields,
        ));
    }
    let project = |field: usize| -> Vec<Series> {
        series
            .iter()
            .filter_map(|s| {
                let (start, values) = values(s, field);
                let first = values.iter().position(Option::is_some)?;
                let last = values.iter().rposition(Option::is_some)?;
                Some(input(
                    &single,
                    s.metric.clone(),
                    start + first,
                    &[values[first..=last].to_vec()],
                ))
            })
            .collect()
    };
    let grid_len = multi.eval_grid.as_ref().unwrap().spec().len as usize;
    let q_values: Vec<Option<f64>> = (0..grid_len)
        .map(|i| Some([0.0, 0.25, 0.5, 0.9, 1.0][i % 5]))
        .collect();
    let q = input(
        &schema(&["scalar"]),
        SeriesLabels::try_new(None, Vec::new()).unwrap(),
        0,
        &[q_values],
    );
    type Kernel<'a> = Box<dyn Fn(&Arc<SeriesSchema>, &[Series], Grouping<'_>) -> Vec<Series> + 'a>;
    let mut kernels: Vec<(String, Kernel<'_>)> = OPS
        .iter()
        .chain(&[AggregateOp::Stddev, AggregateOp::Stdvar])
        .map(|&op| -> (String, Kernel<'_>) {
            (
                format!("{op:?}"),
                Box::new(move |schema, input, grouping| {
                    aggregate_float(schema, input, op, grouping).unwrap()
                }),
            )
        })
        .collect();
    kernels.push((
        "quantile".into(),
        Box::new(|schema, input, grouping| {
            aggregate_quantile(schema, input, &q, grouping).unwrap()
        }),
    ));
    kernels.push((
        "count_values".into(),
        Box::new(|schema, input, grouping| count_values(schema, input, "value", grouping).unwrap()),
    ));
    for grouping in [
        Grouping::By(&[]),
        Grouping::By(&["host"]),
        Grouping::Without(&["i"]),
    ] {
        for (op, kernel) in &kernels {
            let output = kernel(&multi, &series, grouping);
            for field in [0, 1] {
                let expected = kernel(&single, &project(field), grouping);
                // The multi-field result is cut into pieces where its values are interrupted, so
                // the comparison is over the dense values of each label set, not over pieces.
                let grouped = |pieces: &[Series], field: usize| {
                    let mut by_metric: BTreeMap<LabelKey, Positions> = BTreeMap::new();
                    for piece in pieces {
                        let same: Vec<Series> = pieces
                            .iter()
                            .filter(|other| other.metric == piece.metric)
                            .cloned()
                            .collect();
                        let (start, values) = values_of(&same, field, grid_len);
                        let mut key: LabelKey = piece
                            .metric
                            .tags()
                            .iter()
                            .map(|tag| (tag.name.clone(), tag.value.clone()))
                            .collect();
                        if let Some(name) = piece.metric.name() {
                            key.insert(0, (Arc::from("__name__"), Arc::from(name)));
                        }
                        by_metric.insert(
                            key,
                            values
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| v.map(|v| (start + i, v.to_bits())))
                                .collect(),
                        );
                    }
                    by_metric
                };
                let actual = grouped(&output, field);
                let expected = grouped(&expected, 0);
                assert_eq!(actual, expected, "{op} {grouping:?} field {field}");
            }
        }
    }
}

#[test]
fn incremental_retained_bytes_match_full_recount_after_updates() {
    let schema = schema_with_grid_len(&["value", "other"], 256);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["zone", "rack"]))
            .unwrap();
    assert_eq!(
        aggregator.state_bytes(),
        aggregator.recounted_state_bytes(),
        "initial state"
    );
    let updates = [
        input(
            &schema,
            metric("m", &[("zone", "a"), ("rack", "1")]),
            0,
            &[vec![Some(1.0)], vec![Some(2.0)]],
        ),
        input(
            &schema,
            metric("m", &[("zone", "a"), ("rack", "1")]),
            64,
            &[vec![Some(3.0)], vec![Some(4.0)]],
        ),
        input(
            &schema,
            metric("m", &[("zone", "a"), ("rack", "1")]),
            0,
            &[vec![Some(5.0)], vec![Some(6.0)]],
        ),
        input(
            &schema,
            metric("m", &[("zone", "b"), ("rack", "2")]),
            128,
            &[vec![Some(7.0)], vec![Some(8.0)]],
        ),
    ];
    for (index, series) in updates.iter().enumerate() {
        let previous = aggregator.state_bytes();
        aggregator.update(series).unwrap();
        assert_eq!(
            aggregator.state_bytes(),
            aggregator.recounted_state_bytes(),
            "update {index}"
        );
        if index == 2 {
            assert_eq!(aggregator.state_bytes(), previous);
        }
    }
}

#[test]
fn incremental_retained_bytes_match_full_recount_for_many_groups() {
    let schema = schema(&["value"]);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["id"])).unwrap();
    for id in 0..512 {
        let id = id.to_string();
        let series = input(
            &schema,
            metric("m", &[("id", &id)]),
            id.parse::<usize>().unwrap() % 64,
            &[vec![Some(1.0)]],
        );
        aggregator.update(&series).unwrap();
        assert_eq!(
            aggregator.state_bytes(),
            aggregator.recounted_state_bytes(),
            "group {id}"
        );
    }
}

/// A state keeps the components its operation folds plus a presence word, which the count already
/// provides where the state has one. That is the whole width of a state, so a `max` state costs two
/// words where an `avg` state costs five instead of every state costing one fixed struct.
#[test]
fn a_state_is_as_wide_as_the_operation_it_belongs_to() {
    for (op, words) in [
        (AggregateOp::Count, 1),
        (AggregateOp::Group, 1),
        (AggregateOp::Min, 2),
        (AggregateOp::Max, 2),
        (AggregateOp::Sum, 3),
        (AggregateOp::Stddev, 3),
        (AggregateOp::Stdvar, 3),
        (AggregateOp::Avg, 5),
    ] {
        assert_eq!(op.state_words(), words, "{op:?}");
        let counted = op
            .state_fields()
            .iter()
            .any(|field| matches!(field, StateField::Count));
        assert_eq!(
            op.state_words(),
            op.state_fields().len() + usize::from(!counted),
            "{op:?} keeps its own components and nothing else"
        );
    }
}

/// What a block keeps is what its operation folds: for every operation the fold's retained figure
/// and the recount of its live blocks agree, and a block holds one state per point per field, each
/// as many words as the operation keeps.
#[test]
fn every_operation_recounts_the_states_it_keeps() {
    let schema = schema_with_grid_len(&["value", "other"], 256);
    for op in [
        AggregateOp::Sum,
        AggregateOp::Count,
        AggregateOp::Avg,
        AggregateOp::Min,
        AggregateOp::Max,
        AggregateOp::Group,
        AggregateOp::Stddev,
        AggregateOp::Stdvar,
    ] {
        let mut aggregator =
            FloatAggregator::try_new(&schema, op, Grouping::By(&["zone"])).unwrap();
        assert_eq!(
            aggregator.state_bytes(),
            aggregator.recounted_state_bytes(),
            "{op:?} initial state"
        );
        let updates = [
            input(
                &schema,
                metric("m", &[("zone", "a")]),
                0,
                &[vec![Some(1.0), Some(2.0)], vec![Some(3.0)]],
            ),
            input(
                &schema,
                metric("m", &[("zone", "a")]),
                64,
                &[vec![Some(4.0)], vec![Some(5.0)]],
            ),
            input(
                &schema,
                metric("m", &[("zone", "b")]),
                128,
                &[vec![Some(6.0)], vec![Some(7.0)]],
            ),
        ];
        for (index, series) in updates.iter().enumerate() {
            aggregator.update(series).unwrap();
            assert_eq!(
                aggregator.state_bytes(),
                aggregator.recounted_state_bytes(),
                "{op:?} update {index}"
            );
        }
        // Three blocks of two fields, each state as wide as the operation folds it.
        let blocks = &aggregator.groups[0].blocks;
        assert_eq!(blocks.len(), 2, "{op:?} blocks the first group wrote");
        assert_eq!(
            blocks.values().map(|block| block.len()).sum::<usize>(),
            blocks.len() * BLOCK_POINTS * schema.fields.len() * op.state_words(),
            "{op:?} words per block"
        );
    }
}

/// The unbudgeted entry point is the budgeted one with a no-op budget, so this compares one path
/// with itself: it is a smoke check that the wrapper still runs, not evidence that the kernels are
/// right. The numbers are checked against Prometheus in `aggregates_match_prometheus` and
/// `stat_aggregates_match_prometheus`.
#[test]
fn every_supported_kind_runs_through_the_unbudgeted_entry_point() {
    let schema = schema(&["value"]);
    let series = input(&schema, metric("m", &[]), 0, &[vec![Some(1.0), Some(3.0)]]);
    for op in [
        AggregateOp::Sum,
        AggregateOp::Count,
        AggregateOp::Avg,
        AggregateOp::Min,
        AggregateOp::Max,
        AggregateOp::Group,
        AggregateOp::Stddev,
        AggregateOp::Stdvar,
    ] {
        let mut plain = FloatAggregator::try_new(&schema, op, Grouping::By(&[])).unwrap();
        plain.update(&series).unwrap();
        let plain = plain.finish().unwrap();
        let mut budgeted =
            FloatAggregator::try_new_budgeted(&schema, op, Grouping::By(&[]), |_| Ok(())).unwrap();
        budgeted.update_budgeted(&series, |_| Ok(())).unwrap();
        let budgeted = budgeted.finish_budgeted(|_| Ok(())).unwrap();
        assert_eq!(
            values_of(&plain, 0, 128),
            values_of(&budgeted, 0, 128),
            "{op:?}"
        );
    }
}

#[test]
fn repeated_updates_in_one_group_and_block_keep_one_block_charge() {
    let schema = schema(&["value"]);
    let first = input(&schema, metric("m", &[]), 0, &[vec![Some(1.0)]]);
    let second = input(&schema, metric("m", &[]), 1, &[vec![Some(3.0)]]);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    aggregator.update(&first).unwrap();
    let retained = aggregator.state_bytes();
    aggregator.update(&second).unwrap();
    assert_eq!(aggregator.state_bytes(), retained);
}

#[test]
fn adjacent_blocks_are_admitted_and_accounted_separately() {
    let schema = schema(&["value"]);
    let series = input(&schema, metric("m", &[]), 63, &[vec![Some(1.0), Some(2.0)]]);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    let mut admissions = Vec::new();
    aggregator
        .update_budgeted(&series, |bytes| {
            admissions.push(bytes);
            Ok(())
        })
        .unwrap();
    assert_eq!(aggregator.groups(), 1);
    assert_eq!(aggregator.groups[0].blocks.len(), 2);
    // A new block is admitted on the retained state alone, so the last admission settles at
    // exactly what the fold keeps and no staging buffer is charged for the samples in it.
    let block = block_bytes(schema.fields.len(), AggregateOp::Sum);
    let (first, second) = (
        admissions[admissions.len() - 2],
        admissions[admissions.len() - 1],
    );
    assert_eq!(
        first,
        admissions[admissions.len() - 3]
            .saturating_add(btree_backing_bytes(1))
            .saturating_add(block),
        "the first block adds its states and its tree node to the retained state"
    );
    assert_eq!(
        second,
        first
            .saturating_add(btree_backing_bytes(2).saturating_sub(btree_backing_bytes(1)))
            .saturating_add(block),
        "the second block is admitted after the first"
    );
    assert_eq!(admissions.last().copied(), Some(aggregator.state_bytes()));
}

#[test]
fn a_rejected_block_allocation_does_not_insert_a_block() {
    let schema = schema(&["value"]);
    let series = input(&schema, metric("m", &[]), 0, &[vec![Some(1.0)]]);
    // The figure a new block's admission adds: the block's states and its tree node, on top of
    // whatever the fold already holds. Located by the value, not by which call reaches it, so
    // reordering the earlier admissions cannot make this test pass without testing anything.
    let block_states = block_bytes(schema.fields.len(), AggregateOp::Sum);
    let block = block_states.saturating_add(btree_backing_bytes(1));
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    let mut refused = None;
    let result = aggregator.update_budgeted(&series, |bytes| {
        // Everything up to the block's own admission is admitted; only the block is refused.
        if bytes >= block {
            refused = Some(bytes);
            return Err(DataFusionError::Execution("block allocation budget".into()));
        }
        Ok(())
    });
    assert!(
        result.is_err(),
        "the block was admitted with no budget left"
    );
    let refused = refused.expect("the refusal did not come from a block admission");
    assert!(
        refused >= block,
        "the refusal came from an admission smaller than a block"
    );
    assert_eq!(aggregator.groups(), 1);
    assert!(
        aggregator.groups[0].blocks.is_empty(),
        "a refused block was inserted anyway"
    );
}

/// A block the group already holds keeps its states when the next block is refused: the refusal is
/// an admission taken before the insertion, not a half-inserted block.
#[test]
fn a_rejected_later_block_keeps_the_blocks_the_group_already_holds() {
    let schema = schema(&["value"]);
    let first = input(&schema, metric("m", &[]), 0, &[vec![Some(1.0)]]);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    aggregator.update(&first).unwrap();
    let retained = aggregator.state_bytes();
    assert_eq!(aggregator.groups[0].blocks.len(), 1);

    // The group and the tree node are already there, so the block's own states are the whole
    // charge of the second block.
    let later = input(&schema, metric("m", &[]), 64, &[vec![Some(2.0)]]);
    let block_states = block_bytes(schema.fields.len(), AggregateOp::Sum);
    let threshold = retained
        .saturating_add(btree_backing_bytes(2).saturating_sub(btree_backing_bytes(1)))
        .saturating_add(block_states);
    let mut refused = false;
    let result = aggregator.update_budgeted(&later, |bytes| {
        if bytes >= threshold {
            refused = true;
            return Err(DataFusionError::Execution("later block budget".into()));
        }
        Ok(())
    });
    assert!(result.is_err(), "the second block was admitted");
    assert!(refused, "the refusal did not come from the second block");
    assert_eq!(aggregator.groups[0].blocks.len(), 1);
    assert!(
        !aggregator.groups[0].blocks.contains_key(&1),
        "a refused block was inserted anyway"
    );
    assert_eq!(
        aggregator.state_bytes(),
        retained,
        "a refused block was charged"
    );
}

/// The fold admits exactly the figures it reports: a bound equal to the peak it asked for admits
/// the whole series, and one byte less refuses it. The staging pass that used to buffer a block's
/// samples is gone, so the block no longer costs a buffer beside its states.
#[test]
fn a_bound_equal_to_the_fold_peak_admits_the_whole_series() {
    let schema = schema(&["value", "other"]);
    let series = input(
        &schema,
        metric("m", &[("zone", "a")]),
        60,
        &[
            vec![Some(1.0); 8],
            vec![
                None,
                Some(2.0),
                None,
                Some(3.0),
                None,
                Some(4.0),
                None,
                None,
            ],
        ],
    );
    let mut recorder =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["zone"])).unwrap();
    let mut peak = 0;
    recorder
        .update_budgeted(&series, |bytes| {
            peak = peak.max(bytes);
            Ok(())
        })
        .unwrap();

    let mut just_fitting =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["zone"])).unwrap();
    just_fitting
        .update_budgeted(&series, |bytes| match bytes <= peak {
            true => Ok(()),
            false => Err(DataFusionError::Execution(format!(
                "the fold asked for {bytes}, more than its own peak {peak}"
            ))),
        })
        .expect("a bound equal to the fold's own peak admits it");
    assert_eq!(
        values(&just_fitting.finish().unwrap()[0], 0),
        (60, vec![Some(1.0); 8])
    );

    let mut one_short =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["zone"])).unwrap();
    assert!(
        one_short
            .update_budgeted(&series, |bytes| match bytes < peak {
                true => Ok(()),
                false => Err(DataFusionError::Execution("one byte short".into())),
            })
            .is_err(),
        "the peak admission is needed, not spare"
    );
}

/// The fold releases the series' grouping workspace at its entry, so a block is charged on the
/// retained state alone and the retained figure is the recount afterwards.
#[test]
fn the_fold_releases_the_series_workspace_before_it_admits_a_block() {
    let schema = schema(&["value"]);
    let series = input(&schema, metric("m", &[("i", "0")]), 0, &[vec![Some(1.0)]]);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["i"])).unwrap();
    let mut admissions = Vec::new();
    aggregator
        .update_budgeted(&series, |bytes| {
            admissions.push(bytes);
            Ok(())
        })
        .unwrap();
    let state = aggregator.state_bytes();
    assert_eq!(aggregator.recounted_state_bytes(), state);

    let block = block_bytes(schema.fields.len(), AggregateOp::Sum);
    let entry = state
        .saturating_sub(block)
        .saturating_sub(btree_backing_bytes(1));
    assert_eq!(admissions[admissions.len() - 2], entry);
    assert_eq!(admissions[admissions.len() - 1], state);
    // Opening the group admitted the grouping workspace on top of the same retained state. The
    // entry asked for less than that, so the workspace is not charged while the block is.
    let grouping = admissions[..admissions.len() - 2]
        .iter()
        .copied()
        .max()
        .expect("the group was opened with an admission");
    assert!(
        grouping > entry,
        "the series workspace was still charged at the fold entry: {grouping} <= {entry}"
    );
}

/// A chunk admits its workspace and the group it may open in one pool call, releases the
/// workspace at the fold entry, and pays for each block it opens: three calls for a chunk that
/// falls in one block, four for one that opens two.
///
/// The fold used to admit the workspace, then a wider bound of it, then the grouping and its key,
/// then the new group on top of them: six admissions for a chunk that opens a group and one block,
/// seven for one that opens two, on top of the runtime's own resize once the chunk is folded.
#[test]
fn a_chunk_admits_its_workspace_release_and_blocks_in_three_or_four_calls() {
    let schema = schema(&["value", "other"]);
    let one_block = input(
        &schema,
        metric("m", &[("zone", "a")]),
        0,
        &[vec![Some(1.0), Some(2.0)], vec![Some(3.0)]],
    );
    // Two samples of the first field straddle the edge at 64, so the fold opens a second block.
    let two_blocks = input(
        &schema,
        metric("m", &[("zone", "a")]),
        63,
        &[vec![Some(1.0), Some(2.0)], vec![Some(3.0)]],
    );
    let admissions = |series: &Series| {
        let mut aggregator =
            FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["zone"])).unwrap();
        let mut calls = Vec::new();
        aggregator
            .update_budgeted(series, |bytes| {
                calls.push(bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(aggregator.state_bytes(), aggregator.recounted_state_bytes());
        calls
    };

    let one = admissions(&one_block);
    assert_eq!(
        one.len(),
        3,
        "the workspace, the release and one block: {one:?}"
    );
    assert!(
        one[0] > one[1],
        "the workspace was admitted beside the retained state: {one:?}"
    );
    let block = block_bytes(schema.fields.len(), AggregateOp::Sum);
    assert_eq!(
        one[2],
        one[1]
            .saturating_add(block)
            .saturating_add(btree_backing_bytes(1)),
        "the block is admitted on the retained state alone"
    );

    let two = admissions(&two_blocks);
    assert_eq!(
        two.len(),
        4,
        "the workspace, the release and two blocks: {two:?}"
    );
    assert!(two[3] > two[2], "each block is admitted separately");
}

/// A chunk that only writes into states its group already holds costs the workspace admission and
/// the release, and no block.
#[test]
fn a_chunk_of_existing_blocks_admits_only_its_workspace_and_release() {
    let schema = schema(&["value"]);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["zone"])).unwrap();
    aggregator
        .update(&input(
            &schema,
            metric("m", &[("zone", "a")]),
            0,
            &[vec![Some(1.0)]],
        ))
        .unwrap();
    let mut calls = Vec::new();
    aggregator
        .update_budgeted(
            &input(
                &schema,
                metric("m", &[("zone", "a")]),
                1,
                &[vec![Some(2.0)]],
            ),
            |bytes| {
                calls.push(bytes);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(calls.len(), 2, "the workspace and the release: {calls:?}");
    assert_eq!(aggregator.state_bytes(), aggregator.recounted_state_bytes());
}

/// A block a field holds no value in is not allocated: two fields whose samples fall in different
/// blocks leave the group with the blocks they write, not the blocks between them.
#[test]
fn blocks_are_allocated_only_where_a_field_holds_a_value() {
    let schema = schema_with_grid_len(&["a", "b"], 256);
    let mut fields = [vec![None; 130], vec![None; 130]];
    for (value, sample) in fields[0].iter_mut().take(64).enumerate() {
        *sample = Some(value as f64);
    }
    fields[1][128] = Some(1.0);
    fields[1][129] = Some(2.0);
    let series = input(&schema, metric("m", &[]), 0, &fields);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    aggregator.update(&series).unwrap();
    assert_eq!(aggregator.groups(), 1);
    assert_eq!(
        aggregator.groups[0]
            .blocks
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![0, 2],
        "the block between the two fields holds no sample and is not allocated"
    );
    assert_eq!(aggregator.state_bytes(), aggregator.recounted_state_bytes());
}

#[test]
fn a_map_growth_rejection_does_not_insert_the_new_group() {
    let schema = schema(&["value"]);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["i"])).unwrap();
    let first = input(&schema, metric("m", &[("i", "0")]), 0, &[vec![Some(1.0)]]);
    aggregator.update(&first).unwrap();
    let mut index = 1;
    while aggregator.group_index.len() < aggregator.group_index.capacity() {
        let label = index.to_string();
        let series = input(
            &schema,
            metric("m", &[("i", &label)]),
            0,
            &[vec![Some(1.0)]],
        );
        aggregator.update(&series).unwrap();
        index += 1;
    }
    let group_count = aggregator.groups();
    let old_state = aggregator.state_bytes();
    assert_eq!(aggregator.group_index.capacity(), group_count);

    // A resize keeps the old table while it fills the new one, so the figure the growth has to
    // admit is the whole new table on top of the old state -- not the difference between them.
    // That is the property asserted here: the refusal threshold is the state plus a whole new
    // table, so the test fails if the admission ever drops to the delta.
    let new_table = hash_map_backing_bytes(next_hash_capacity(group_count));
    let threshold = old_state.saturating_add(new_table);
    let label = index.to_string();
    let series = input(
        &schema,
        metric("m", &[("i", &label)]),
        0,
        &[vec![Some(1.0)]],
    );
    let mut peak = 0;
    let result = aggregator.update_budgeted(&series, |bytes| {
        peak = peak.max(bytes);
        if bytes >= threshold {
            Err(DataFusionError::Execution("old+new map budget".into()))
        } else {
            Ok(())
        }
    });
    assert!(
        result.is_err(),
        "the map grew with no budget for both tables"
    );
    assert!(
        peak >= threshold,
        "the growth admitted {peak}, less than the old state plus a new table ({threshold})"
    );
    assert_eq!(aggregator.group_index.capacity(), group_count);
    assert_eq!(aggregator.groups(), group_count);
    assert_eq!(
        aggregator
            .groups
            .iter()
            .map(|group| group.blocks.len())
            .sum::<usize>(),
        group_count
    );
}

#[test]
fn finish_rejection_after_first_group_is_budgeted_before_the_next() {
    let schema = schema(&["value"]);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["i"])).unwrap();
    for (label, value) in [("a", 1.0), ("b", 2.0)] {
        let series = input(
            &schema,
            metric("m", &[("i", label)]),
            0,
            &[vec![Some(value)]],
        );
        aggregator.update(&series).unwrap();
    }
    let mut calls = Vec::new();
    let result = aggregator.finish_budgeted(|bytes| {
        calls.push(bytes);
        if calls.len() == 4 {
            Err(DataFusionError::Execution("budget".into()))
        } else {
            Ok(())
        }
    });
    assert!(result.is_err());
    assert_eq!(calls.len(), 4);
    assert!(calls[1] > calls[0], "first builder peak");
    assert!(calls[2] < calls[1], "first group settle");
    assert!(calls[3] > calls[2], "second builder peak");

    // The base already includes the output Vec backing; later callbacks add only the
    // builder workspace or completed Series allocation.
    let builder_workspace = finish_working_bytes(1, 1, 1);
    let expected_output = aggregate_float(
        &schema,
        &[
            input(&schema, metric("m", &[("i", "a")]), 0, &[vec![Some(1.0)]]),
            input(&schema, metric("m", &[("i", "b")]), 0, &[vec![Some(2.0)]]),
        ],
        AggregateOp::Sum,
        Grouping::By(&["i"]),
    )
    .unwrap();
    let completed_series = crate::grid_series::series_bytes(&expected_output[0]);
    assert_eq!(calls[1] - calls[0], builder_workspace);
    assert_eq!(calls[2] - calls[0], completed_series);
    assert_eq!(calls[3] - calls[2], builder_workspace);
}

#[test]
fn state_blocks_keep_absolute_indices_across_boundaries_and_gaps() {
    // A 512-point grid spans eight state blocks; inputs straddle a block edge, leave whole
    // blocks empty and land in a far block, in both fields.
    let schema = Arc::new(SeriesSchema {
        fields: vec![
            FieldSpec { name: "a".into() },
            FieldSpec { name: "b".into() },
        ]
        .into(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(0, 1, 512).unwrap()).unwrap(),
        )),
    });
    let mut gap = vec![None; 200];
    gap[0] = Some(1.0);
    gap[199] = Some(2.0);
    let inputs = [
        input(
            &schema,
            metric("m", &[("i", "0")]),
            60,
            &[vec![Some(1.0); 8], vec![Some(10.0); 8]],
        ),
        input(
            &schema,
            metric("m", &[("i", "1")]),
            100,
            &[gap.clone(), vec![None; 200]],
        ),
        input(
            &schema,
            metric("m", &[("i", "2")]),
            62,
            &[vec![Some(3.0); 4], vec![None, None, Some(5.0), None]],
        ),
        input(
            &schema,
            metric("m", &[("i", "3")]),
            509,
            &[vec![Some(4.0); 3], vec![Some(6.0); 3]],
        ),
    ];
    let output = aggregate_float(&schema, &inputs, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    // One group, and its values are interrupted by empty blocks, so it arrives as several pieces
    // of the same label set. What must hold is the dense values, not the piece count.
    assert!(output.iter().all(|piece| piece.metric == output[0].metric));
    let mut a = vec![None; 452];
    let mut b = vec![None; 452];
    for i in 60..68 {
        a[i - 60] = Some(if (62..66).contains(&i) { 4.0 } else { 1.0 });
        b[i - 60] = Some(if i == 64 { 15.0 } else { 10.0 });
    }
    a[100 - 60] = Some(1.0);
    a[299 - 60] = Some(2.0);
    for i in 509..512 {
        a[i - 60] = Some(4.0);
        b[i - 60] = Some(6.0);
    }
    assert_eq!(values_of(&output, 0, 512), (60, a));
    assert_eq!(values_of(&output, 1, 512), (60, b));
}

/// Every float aggregate, over inputs that cross block boundaries with holes, nulls and
/// non-finite values in two fields, folded as one series each and as the runs those series are
/// made of.
///
/// The oracle is the same kernel over the same samples in another shape: a run carries only its
/// own rows, so a fold that read a block's states by row number instead of by absolute position,
/// or that handed a field's samples over in bulk instead of in their order, would part from the
/// whole-series fold here.
#[test]
fn every_aggregate_folds_a_series_whole_and_as_its_runs() {
    type Sample = (usize, Option<f64>, Option<f64>);

    let schema = schema_with_grid_len(&["a", "b"], 256);
    // Every row holds a value in at least one field. The positions straddle the 64-point block
    // edges, leave whole blocks empty, and repeat non-finite and signed-zero values.
    let inputs: [(&str, &[Sample]); 2] = [
        (
            "h1",
            &[
                (0, Some(1.0), None),
                (1, Some(f64::NAN), Some(2.0)),
                (62, None, Some(3.0)),
                (63, Some(4.0), Some(4.0)),
                (64, Some(5.0), None),
                (65, Some(f64::INFINITY), Some(5.0)),
                (127, Some(6.0), Some(f64::NEG_INFINITY)),
                (128, None, Some(7.0)),
                (255, Some(8.0), None),
            ],
        ),
        (
            "h2",
            &[
                (1, Some(9.0), Some(9.0)),
                (63, Some(10.0), None),
                (64, None, Some(11.0)),
                (128, Some(12.0), Some(-0.0)),
                (129, Some(13.0), Some(0.0)),
            ],
        ),
    ];
    let mut whole = Vec::new();
    let mut runs = Vec::new();
    for (host, samples) in inputs {
        assert!(
            samples.iter().all(|(_, a, b)| a.is_some() || b.is_some()),
            "a row with no value would not be a carried position"
        );
        let positions: Vec<usize> = samples.iter().map(|(position, _, _)| *position).collect();
        let fields = [
            samples.iter().map(|(_, a, _)| *a).collect::<Vec<_>>(),
            samples.iter().map(|(_, _, b)| *b).collect::<Vec<_>>(),
        ];
        let labels = || metric("m", &[("host", host)]);
        whole.push(sparse_input(&schema, labels(), &positions, &fields));
        // Each run of consecutive positions, handed over as its own series of the same label set.
        let mut start = 0;
        while start < positions.len() {
            let mut end = start + 1;
            while end < positions.len() && positions[end - 1] + 1 == positions[end] {
                end += 1;
            }
            runs.push(input(
                &schema,
                labels(),
                positions[start],
                &[
                    fields[0][start..end].to_vec(),
                    fields[1][start..end].to_vec(),
                ],
            ));
            start = end;
        }
    }
    for op in OPS
        .iter()
        .chain(&[AggregateOp::Stddev, AggregateOp::Stdvar])
        .copied()
    {
        for grouping in [
            Grouping::By(&[]),
            Grouping::By(&["host"]),
            Grouping::Without(&["host"]),
        ] {
            let folded = aggregate_float(&schema, &whole, op, grouping).unwrap();
            let mut chunked = FloatAggregator::try_new(&schema, op, grouping).unwrap();
            for run in &runs {
                chunked.update(run).unwrap();
            }
            let chunked = chunked.finish().unwrap();
            let (folded, chunked) = (flatten(&folded), flatten(&chunked));
            assert_eq!(
                folded.keys().collect::<Vec<_>>(),
                chunked.keys().collect::<Vec<_>>(),
                "{op:?} {grouping:?}"
            );
            for (labels, chunks) in &chunked {
                let singles = &folded[labels];
                assert_eq!(
                    singles.keys().collect::<Vec<_>>(),
                    chunks.keys().collect::<Vec<_>>(),
                    "{op:?} {grouping:?} {labels}"
                );
                for (key, &skipped) in chunks {
                    // The same samples in the same per-field order, so the bits must agree; a NaN
                    // is the same NaN, which `==` would not say.
                    assert_eq!(
                        singles[key].to_bits(),
                        skipped.to_bits(),
                        "{op:?} {grouping:?} {labels} {key:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn grouping_labels_follow_prometheus_and_treat_empty_values_as_absent() {
    let schema = schema(&["value"]);
    let series = [
        input(
            &schema,
            metric("a", &[("host", ""), ("job", "x")]),
            0,
            &[vec![Some(1.0)]],
        ),
        input(&schema, metric("b", &[("job", "x")]), 0, &[vec![Some(2.0)]]),
    ];
    let labels = |grouping| {
        aggregate_float(&schema, &series, AggregateOp::Sum, grouping)
            .unwrap()
            .into_iter()
            .map(|s| {
                let values = values(&s, 0).1;
                (s.metric, values)
            })
            .collect::<Vec<_>>()
    };
    let job = SeriesLabels::try_new(None, metric("m", &[("job", "x")]).tags().to_vec()).unwrap();
    assert_eq!(
        labels(Grouping::By(&["host", "job"])),
        [(job.clone(), vec![Some(3.0)])]
    );
    // `without` always drops the name; only `by (__name__)` keeps it.
    assert_eq!(
        labels(Grouping::Without(&["host"])),
        [(job, vec![Some(3.0)])]
    );
    let by_name = labels(Grouping::By(&["__name__"]));
    assert_eq!(by_name.len(), 2);
    assert_eq!(
        by_name[0].0,
        SeriesLabels::try_new(Some("a".into()), vec![]).unwrap()
    );
    let unnamed = [
        input(&schema, metric("", &[]), 0, &[vec![Some(1.0)]]),
        input(
            &schema,
            SeriesLabels::try_new(None, vec![]).unwrap(),
            0,
            &[vec![Some(2.0)]],
        ),
    ];
    let by_name = aggregate_float(
        &schema,
        &unnamed,
        AggregateOp::Sum,
        Grouping::By(&["__name__"]),
    )
    .unwrap();
    assert_eq!(by_name.len(), 1);
    assert_eq!(by_name[0].metric.name(), None);
    let none = labels(Grouping::By(&[]));
    assert_eq!(
        none,
        [(
            SeriesLabels::try_new(None, vec![]).unwrap(),
            vec![Some(3.0)]
        )]
    );
}

#[test]
fn min_and_max_keep_the_first_of_equal_zeros() {
    // Prometheus replaces the accumulator only on a strict comparison; the tolerance treats
    // the zeros as equal, but their sign shows through a later `1 / x`.
    let schema = schema(&["value"]);
    for (first, second) in [(0.0, -0.0), (-0.0, 0.0)] {
        let series = [("0", first), ("1", second)]
            .map(|(i, v)| input(&schema, metric("m", &[("i", i)]), 0, &[vec![Some(v)]]));
        for op in [AggregateOp::Min, AggregateOp::Max] {
            let output = aggregate_float(&schema, &series, op, Grouping::By(&[])).unwrap();
            let value = values(&output[0], 0).1[0].unwrap();
            assert_eq!(
                value.to_bits(),
                f64::to_bits(first),
                "{op:?} {first} {second}"
            );
        }
    }
}

#[test]
fn groups_keep_first_seen_order_for_nested_aggregation() {
    // `sum(sum by (g) (m))` from the Prometheus engine with series created as g=a, c, b: the
    // outer sum folds 1e308, -1e308, 1e308 and gets 1e308; label order would overflow.
    let schema = schema(&["value"]);
    let series = [("0", "a", 1e308), ("1", "c", -1e308), ("2", "b", 1e308)].map(|(a, g, v)| {
        input(
            &schema,
            metric("m", &[("a", a), ("g", g)]),
            0,
            &[vec![Some(v)]],
        )
    });
    let inner = aggregate_float(&schema, &series, AggregateOp::Sum, Grouping::By(&["g"])).unwrap();
    let order = inner
        .iter()
        .map(|s| s.metric.get("g").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(order, ["a", "c", "b"]);
    let outer = aggregate_float(&schema, &inner, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    assert_eq!(values(&outer[0], 0), (0, vec![Some(1e308)]));
}

#[test]
fn grouping_keeps_type_and_unit_labels() {
    // Unlike math and arithmetic, Prometheus `generateGroupingLabels` drops only the name.
    let schema = schema(&["value"]);
    let series = [input(
        &schema,
        metric(
            "a",
            &[("__type__", "gauge"), ("__unit__", "seconds"), ("job", "x")],
        ),
        0,
        &[vec![Some(1.0)]],
    )];
    let labels = |grouping| {
        aggregate_float(&schema, &series, AggregateOp::Sum, grouping).unwrap()[0]
            .metric
            .clone()
    };
    assert_eq!(
        labels(Grouping::Without(&["job"])),
        SeriesLabels::try_new(
            None,
            metric("a", &[("__type__", "gauge"), ("__unit__", "seconds")])
                .tags()
                .to_vec()
        )
        .unwrap()
    );
    assert_eq!(
        labels(Grouping::By(&["__unit__"])),
        SeriesLabels::try_new(
            None,
            metric("a", &[("__unit__", "seconds")]).tags().to_vec()
        )
        .unwrap()
    );
}

#[test]
fn empty_inputs_produce_no_groups() {
    let schema = schema(&["a", "b"]);
    let empty = input(&schema, metric("m", &[]), 127, &[vec![None], vec![None]]);
    for op in OPS {
        assert!(
            aggregate_float(&schema, &[], op, Grouping::By(&[]))
                .unwrap()
                .is_empty()
        );
        let output =
            aggregate_float(&schema, std::slice::from_ref(&empty), op, Grouping::By(&[])).unwrap();
        assert!(output.is_empty());
    }
}

#[test]
fn unsupported_shapes_and_mutated_schema_fail_without_partial_output() {
    let schema = schema(&["a", "b"]);
    let source = input(
        &schema,
        metric("m", &[]),
        11,
        &[vec![Some(1.0)], vec![Some(2.0)]],
    );
    // An empty `count_values` label name is a label whose name is empty, which Prometheus 3 reads as
    // valid UTF-8 like any other name, so it is answered rather than refused: the grouping keeps
    // that label alone, and each distinct value becomes one series carrying it, counting the one
    // sample it was made of.
    let mut counted = count_values(
        &schema,
        std::slice::from_ref(&source),
        "",
        Grouping::By(&[]),
    )
    .unwrap()
    .iter()
    .map(|series| {
        // A count lands in the field its sample came from, so the series' total is what says how
        // many samples the value was written for.
        let count = (0..series.fields.len())
            .flat_map(|field| values(series, field).1.into_iter().flatten())
            .sum::<f64>();
        (series.metric.get("").map(str::to_string), count)
    })
    .collect::<Vec<_>>();
    counted.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        vec![(Some("1".to_string()), 1.0), (Some("2".to_string()), 1.0)],
        counted,
        "the empty destination is one label, written once per distinct value"
    );
    assert!(matches!(
        aggregate_float(
            &schema,
            std::slice::from_ref(&source),
            AggregateOp::Sum,
            Grouping::By(&["a"])
        ),
        Err(DataFusionError::NotImplemented(_))
    ));
    let mut histogram = source.clone();
    let batch = HistogramBatchBuilder::new().finish().unwrap();
    histogram.fields = SeriesFields::try_from_vec(vec![
        source.fields.get(0).unwrap().clone(),
        Samples::try_from_histogram(UInt32Array::from(vec![None]), batch).unwrap(),
    ])
    .unwrap();
    // `count` and `group` ask only whether a sample is present, so a histogram is one
    // contribution for them, `min`/`max` skip it, and `sum`/`avg` fold whatever payload they are
    // handed -- this column holds no sample at all, so there is nothing for them to fold. No
    // operation this route folds refuses a histogram series any more; what it does with each sample
    // is the fold's decision.
    for op in OPS {
        let answer = aggregate_float(
            &schema,
            &[source.clone(), histogram.clone()],
            op,
            Grouping::By(&[]),
        );
        assert!(answer.is_ok(), "{op:?} takes a histogram: {answer:?}");
    }
    let mut invalid = source.clone();
    invalid.fields = SeriesFields::one(source.fields.get(0).unwrap().clone());
    // Wrong in two ways at once, the series fails as it does in Map.
    let mut raw_invalid = invalid.clone();
    raw_invalid.time = TimeAxis::try_new(vec![1750].into()).unwrap();
    assert_eq!(
        aggregate_float(
            &schema,
            &[raw_invalid.clone()],
            AggregateOp::Sum,
            Grouping::By(&[])
        )
        .unwrap_err()
        .to_string(),
        crate::map::map_math(&schema, &raw_invalid, crate::map::MathOp::Abs)
            .unwrap_err()
            .to_string()
    );
    assert!(aggregate_float(&schema, &[invalid], AggregateOp::Sum, Grouping::By(&[])).is_err());
    let mut raw = source.clone();
    raw.time = TimeAxis::try_new(vec![1750].into()).unwrap();
    assert!(aggregate_float(&schema, &[raw], AggregateOp::Sum, Grouping::By(&[])).is_err());
    let mut other_grid = schema.as_ref().clone();
    other_grid.eval_grid = Some(Arc::new(
        EvalGrid::try_new(GridSpec::try_new(0, 250, 128).unwrap()).unwrap(),
    ));
    let other_grid = Arc::new(other_grid);
    assert!(aggregate_float(&other_grid, &[source], AggregateOp::Sum, Grouping::By(&[])).is_err());
    let mut raw_schema = schema.as_ref().clone();
    raw_schema.eval_grid = None;
    let raw_schema = Arc::new(raw_schema);
    assert!(aggregate_float(&raw_schema, &[], AggregateOp::Sum, Grouping::By(&[])).is_err());
}

/// Aggregating a series of several runs folds the positions its axis carries, once each.
///
/// The rows of a sparse input are not adjacent, so a fold that walked `0..len` and added the
/// first position would aggregate the holes as if they were the carried rows.
#[test]
fn aggregates_fold_a_sparse_axis_by_its_positions() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1000, 8).unwrap()).unwrap());
    let schema = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: Some(grid),
    });
    let mut points = crate::grid_series::reference::Points::new();
    for (position, value) in [(1, 1.0), (2, 2.0), (6, 6.0)] {
        points.insert(position, f64::to_bits(value));
    }
    let a = crate::grid_series::reference::sparse_series(
        &schema,
        &Labels::from([("host".to_string(), "a".to_string())]),
        &points,
    )
    .unwrap();
    let mut other = crate::grid_series::reference::Points::new();
    for (position, value) in [(2, 20.0), (6, 60.0)] {
        other.insert(position, f64::to_bits(value));
    }
    let b = crate::grid_series::reference::sparse_series(
        &schema,
        &Labels::from([("host".to_string(), "b".to_string())]),
        &other,
    )
    .unwrap();

    // `By(&[])`: one group over both series, since their `host` labels differ.
    let pieces = aggregate_float(&schema, &[a, b], AggregateOp::Sum, Grouping::By(&[])).unwrap();
    // One group, one piece: the group holds a result at positions 1, 2 and 6.
    let dense = crate::grid_series::reference::stitch(&pieces);
    let values = dense
        .iter()
        .enumerate()
        .filter_map(|(position, row)| row[0].map(|value| (position, value)))
        .collect::<Vec<_>>();
    assert_eq!(values, vec![(1, 1.0), (2, 22.0), (6, 66.0)]);
}

/// F: an aggregation over inputs of several runs folds the carried positions and reports them as
/// the runs they make up, compared in full against the dense reference.
///
/// Series `a` holds 1, 2 and 6; series `b` holds 2 and 6 with a different value. Their sum is 1 at
/// position 1, 22 at 2 and 66 at 6, and the hole at 3..5 is not a row: the result's runs are
/// `[(1, 2), (6, 1)]`, not `[(1, 6)]`.
#[test]
fn an_aggregation_of_sparse_inputs_is_compared_in_full_against_the_reference() {
    use crate::grid_series::reference::{
        Dense, assert_pieces_eq, metric_of, run_ranges, sparse_series, stitch,
    };

    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 8).unwrap()).unwrap());
    let schema = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: Some(grid),
    });
    let points = |values: &[(usize, f64)]| -> crate::grid_series::reference::Points {
        values
            .iter()
            .map(|&(position, value)| (position, f64::to_bits(value)))
            .collect()
    };
    let a = sparse_series(
        &schema,
        &Labels::from([("host".to_string(), "a".to_string())]),
        &points(&[(1, 1.0), (2, 2.0), (6, 6.0)]),
    )
    .expect("three positions hold a value");
    let b = sparse_series(
        &schema,
        &Labels::from([("host".to_string(), "b".to_string())]),
        &points(&[(2, 20.0), (6, 60.0)]),
    )
    .expect("two positions hold a value");
    assert_eq!(run_ranges(&a), vec![(1, 2), (6, 1)]);
    assert_eq!(run_ranges(&b), vec![(2, 1), (6, 1)]);

    let pieces = aggregate_float(
        &schema,
        &[a, b.clone()],
        AggregateOp::Sum,
        Grouping::By(&[]),
    )
    .expect("both inputs are float series on the grid");
    let mut expected: Dense = vec![vec![None]; 8];
    expected[1] = vec![Some(1.0)];
    expected[2] = vec![Some(22.0)];
    expected[6] = vec![Some(66.0)];
    assert_pieces_eq(&pieces, &expected, &metric_of(&Labels::new()));
    assert_eq!(
        piece_ranges(&pieces),
        vec![(1, 2), (6, 1)],
        "the sum holds exactly the runs that hold a value"
    );
    for piece in &pieces {
        assert_eq!(piece.time.len(), 3, "three rows for three valued positions");
        for (index, field) in piece.fields.iter().enumerate() {
            assert_eq!(field.len(), 3, "field {index} is row-aligned");
        }
    }
    assert_eq!(stitch(&pieces), expected);

    // `count` keeps the same axis: two inputs hold something at positions 1, 2 and 6.
    let counts = aggregate_float(&schema, &[b], AggregateOp::Count, Grouping::By(&[]))
        .expect("one float input");
    let mut counted: Dense = vec![vec![None]; 8];
    counted[2] = vec![Some(1.0)];
    counted[6] = vec![Some(1.0)];
    assert_pieces_eq(&counts, &counted, &metric_of(&Labels::new()));
    assert_eq!(piece_ranges(&counts), vec![(2, 1), (6, 1)]);
}

/// A point where the `q` scalar has no value has no `quantile` result, as a missing scalar point
/// yields no output in the other scalar operators.
#[test]
fn quantile_without_q_at_a_point_has_no_result() {
    let schema = schema(&["value"]);
    let series = [
        input(
            &schema,
            metric("a", &[("i", "0")]),
            10,
            &[vec![Some(1.0); 3]],
        ),
        input(
            &schema,
            metric("a", &[("i", "1")]),
            10,
            &[vec![Some(3.0); 3]],
        ),
    ];
    let q = input(
        &schema,
        SeriesLabels::try_new(None, Vec::new()).unwrap(),
        10,
        &[vec![Some(0.5), None, Some(0.5)]],
    );
    let output = aggregate_quantile(&schema, &series, &q, Grouping::By(&[])).unwrap();
    let (start, values) = values_of(&output, 0, 128);
    assert_eq!((start, values), (10, vec![Some(2.0), None, Some(2.0)]));
}

/// Listing the value label in `by` repeats what `count_values` adds to the grouping, also when an
/// input field has that name; listing another field name is still unsupported.
#[test]
fn count_values_accepts_its_own_label_in_by() {
    let schema = schema(&["value"]);
    let series = [
        input(&schema, metric("m", &[("i", "0")]), 10, &[vec![Some(1.0)]]),
        input(&schema, metric("m", &[("i", "1")]), 10, &[vec![Some(2.0)]]),
    ];
    let rendered = |grouping| {
        let mut output: Vec<_> = count_values(&schema, &series, "value", grouping)
            .unwrap()
            .iter()
            .map(|series| (format!("{:?}", series.metric), values(series, 0)))
            .collect();
        output.sort_by(|a, b| a.0.cmp(&b.0));
        output
    };
    let implicit = rendered(Grouping::By(&[]));
    assert_eq!(implicit.len(), 2);
    assert_eq!(implicit, rendered(Grouping::By(&["value"])));
    assert!(matches!(
        count_values(&schema, &series, "c", Grouping::By(&["value"])),
        Err(DataFusionError::NotImplemented(_))
    ));
}

/// A series the aggregator rejects is not folded in part: the series after it aggregate as if it
/// had never been offered.
#[test]
fn a_rejected_series_leaves_the_aggregator_unchanged() {
    let schema = schema(&["value"]);
    let other = schema_with_grid_len(&["value"], 64);
    let first = input(&schema, metric("m", &[("i", "0")]), 10, &[vec![Some(1.0)]]);
    let second = input(&schema, metric("m", &[("i", "1")]), 10, &[vec![Some(2.0)]]);
    let foreign = input(&other, metric("m", &[("i", "2")]), 10, &[vec![Some(4.0)]]);

    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    aggregator.update(&first).unwrap();
    assert!(aggregator.update(&foreign).is_err());
    aggregator.update(&second).unwrap();
    let output = aggregator.finish().unwrap();
    assert_eq!(values_of(&output, 0, 128), (10, vec![Some(3.0)]));
}

/// A group of short-lived series, one sample each at its own point: every point aggregates the one
/// member present there, the shape whose cost must follow the samples and not members × points.
#[test]
fn quantile_over_one_point_members_reads_each_member_at_its_point() {
    let schema = schema(&["value"]);
    let series: Vec<Series> = (0..128)
        .map(|point| {
            input(
                &schema,
                metric("m", &[("i", &point.to_string())]),
                point,
                &[vec![Some(point as f64)]],
            )
        })
        .collect();
    let q = input(
        &schema,
        SeriesLabels::try_new(None, Vec::new()).unwrap(),
        0,
        &[vec![Some(0.5); 128]],
    );
    let output = aggregate_quantile(&schema, &series, &q, Grouping::By(&[])).unwrap();
    let expected: Vec<Option<f64>> = (0..128).map(|point| Some(point as f64)).collect();
    assert_eq!(values_of(&output, 0, 128), (0, expected));
}

/// A float sample for the partial tests: ordinary values, repeats, signed zeros, NaN and
/// infinities, but no finite values large enough to overflow when regrouped.
fn partial_sample(rng: &mut StdRng) -> f64 {
    match rng.random_range(0..20) {
        0 => f64::NAN,
        1 => f64::INFINITY,
        2 => f64::NEG_INFINITY,
        3 => 0.0,
        4 => -0.0,
        5..=8 => rng.random_range(0..4) as f64,
        _ => (rng.random::<f64>() - 0.5) * 2e6,
    }
}

/// Every valued (point, field) of `output` by rendered label set.
fn flatten(output: &[Series]) -> BTreeMap<String, BTreeMap<(usize, usize), f64>> {
    let mut flat: BTreeMap<String, BTreeMap<(usize, usize), f64>> = BTreeMap::new();
    for series in output {
        let points = flat.entry(format!("{:?}", series.metric)).or_default();
        for (field, samples) in series.fields.iter().enumerate() {
            for ((point, _), sample) in series.time.positions().zip(samples.iter()) {
                if let SampleRef::Float(value) = sample {
                    assert!(points.insert((point, field), value).is_none());
                }
            }
        }
    }
    flat
}

fn assert_flat_close(
    actual: &[Series],
    expected: &[Series],
    epsilon: f64,
    context: &dyn std::fmt::Display,
) {
    let (actual, expected) = (flatten(actual), flatten(expected));
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "{context}"
    );
    for (labels, expected) in &expected {
        let actual = &actual[labels];
        assert_eq!(
            actual.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>(),
            "{context} {labels}"
        );
        for (key, &value) in expected {
            // An epsilon of 0 asks for the same bits, NaN payloads aside.
            let same = if epsilon == 0.0 {
                actual[key].to_bits() == value.to_bits() || (actual[key].is_nan() && value.is_nan())
            } else {
                crate::grid_series::reference::close(actual[key], value, epsilon)
            };
            assert!(
                same,
                "{context} {labels} {key:?}: merged {} serial {value}",
                actual[key]
            );
        }
    }
}

/// The state a fold of `op` declares: the kind, and the components it keeps in kernel order.
fn state_schema_of(op: AggregateOp) -> AggregateStateSchema {
    let components = op
        .state_fields()
        .iter()
        .map(|field| match field {
            StateField::Value => StateComponent::Value,
            StateField::Mean => StateComponent::Mean,
            StateField::Compensation => StateComponent::Compensation,
            StateField::Count => StateComponent::Count,
            StateField::IncrementalMean => StateComponent::IncrementalMean,
        })
        .collect::<Vec<_>>();
    // The kind an executor would derive from the op, spelled out here so this encoder does not
    // depend on the runtime helper the fixture is standing in for.
    let kind = match op {
        AggregateOp::Sum => AggregateStateKind::Sum,
        AggregateOp::Count => AggregateStateKind::Count,
        AggregateOp::Avg => AggregateStateKind::Avg,
        AggregateOp::Min => AggregateStateKind::Min,
        AggregateOp::Max => AggregateStateKind::Max,
        AggregateOp::Group => AggregateStateKind::Group,
        AggregateOp::Stddev => AggregateStateKind::Stddev,
        AggregateOp::Stdvar => AggregateStateKind::Stdvar,
    };
    AggregateStateSchema::try_new(kind, components).unwrap()
}

/// One group encoded as the Arrow-backed row the executor hands the merge: field-major lists of
/// component structs, a null item where a field has no contribution at that position.
///
/// Only the components `op` declares are written, as an encoder that stores the declared layout and
/// nothing else would hand them over, so a merge reads exactly the columns the fold kept and a hole
/// stays a hole.
fn state_row_of(op: AggregateOp, group: StateSeries) -> StateRow {
    let state = state_schema_of(op);
    let item_fields: datatypes::arrow::datatypes::Fields = state
        .components
        .iter()
        .map(|component| {
            let ty = match component {
                StateComponent::IncrementalMean => DataType::Boolean,
                _ => DataType::Float64,
            };
            Field::new(component.as_str(), ty, true)
        })
        .collect();
    let item_type = DataType::Struct(item_fields.clone());
    let mut lists: Vec<Arc<dyn Array>> = Vec::with_capacity(group.states.len());
    for field in &group.states {
        // Only numeric payloads reach this encoder: `state_fields` describes numeric accumulators,
        // and every state these folds produce is one. A histogram payload is merged through
        // `HistogramAccumulator`'s own state instead, in `merged_histogram_groups`.
        let numeric = |payload: &Option<StatePayload>| match payload.as_ref() {
            Some(StatePayload::Float(state)) => Some(*state),
            Some(StatePayload::Histogram(_)) => {
                panic!("a numeric fold produced a histogram state")
            }
            None => None,
        };
        let columns = state
            .components
            .iter()
            .map(|component| match component {
                StateComponent::IncrementalMean => Arc::new(BooleanArray::from(
                    field
                        .iter()
                        .map(|state| numeric(state).map(|state| state.incremental_mean))
                        .collect::<Vec<_>>(),
                )) as Arc<dyn Array>,
                _ => Arc::new(Float64Array::from(
                    field
                        .iter()
                        .map(|state| {
                            numeric(state).map(|state| match component {
                                StateComponent::Value => state.value,
                                StateComponent::Mean => state.mean,
                                StateComponent::Compensation => state.compensation,
                                StateComponent::Count => state.count,
                                StateComponent::IncrementalMean => unreachable!("handled above"),
                                _ => {
                                    unreachable!("a numeric state declares no histogram component")
                                }
                            })
                        })
                        .collect::<Vec<_>>(),
                )),
            })
            .collect::<Vec<_>>();
        let presence = field.iter().map(Option::is_some).collect::<Vec<_>>();
        // `group` has no components to store, and an item struct with no fields has no length to
        // read off its columns, so its length is the one the presence buffer says.
        let items = if columns.is_empty() {
            StructArray::new_empty_fields(field.len(), Some(NullBuffer::from(presence)))
        } else {
            StructArray::new(
                item_fields.clone(),
                columns,
                Some(NullBuffer::from(presence)),
            )
        };
        lists.push(Arc::new(ListArray::new(
            Arc::new(Field::new("item", item_type.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, field.len() as i32])),
            Arc::new(items),
            None,
        )));
    }
    let fields: datatypes::arrow::datatypes::Fields = lists
        .iter()
        .enumerate()
        .map(|(index, list)| Field::new(format!("f{index}"), list.data_type().clone(), false))
        .collect();
    let batch =
        StateFieldsBatch::try_from_arrow(StructArray::new(fields, lists, None), &state).unwrap();
    StateRow {
        metric: group.metric,
        time: group.time,
        fields: batch,
        kind: state.kind,
    }
}

/// Merges one partition's partial the way the executor does: group by group, in the order the fold
/// produced them, each as the Arrow-backed row the merge reads.
fn merge_partial(merger: &mut FloatMerger, partial: PartialAggregate) {
    merge_partial_budgeted(merger, partial, |_| Ok(())).unwrap();
}

/// [`merge_partial`] with the caller's admission budget, for the tests that count what a merge
/// reserves before it reserves it.
fn merge_partial_budgeted(
    merger: &mut FloatMerger,
    partial: PartialAggregate,
    mut budget: impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    let op = partial.op;
    for group in partial.groups {
        let row = state_row_of(op, group);
        merger.merge_row(&row, &mut budget)?;
    }
    Ok(())
}

/// One group's value at point 0, folding each list of samples in its own partition and merging
/// the partitions in order, next to the serial fold of all samples.
fn merged_and_serial(op: AggregateOp, partitions: &[&[f64]]) -> (f64, f64) {
    let schema = schema(&["value"]);
    let series = |samples: &[f64]| -> Vec<Series> {
        samples
            .iter()
            .map(|&value| input(&schema, metric("m", &[]), 0, &[vec![Some(value)]]))
            .collect()
    };
    let mut merger = FloatMerger::try_new(&schema, op).unwrap();
    for samples in partitions {
        let mut aggregator = FloatAggregator::try_new(&schema, op, Grouping::By(&[])).unwrap();
        for series in series(samples) {
            aggregator.update(&series).unwrap();
        }
        merge_partial(&mut merger, aggregator.finish_partial().unwrap());
    }
    let all: Vec<Series> = partitions
        .iter()
        .flat_map(|samples| series(samples))
        .collect();
    let value = |output: Vec<Series>| values_of(&output, 0, 128).1[0].unwrap();
    (
        value(merger.finish().unwrap()),
        value(aggregate_float(&schema, &all, op, Grouping::By(&[])).unwrap()),
    )
}

/// Runs `run` with a budget that admits everything, returning the largest figure it was asked
/// for: the pool a call needs to succeed.
fn peak_admission(
    run: impl FnOnce(
        &mut dyn FnMut(usize) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError>,
) -> (usize, Result<(), DataFusionError>) {
    let mut peak = 0usize;
    let result = run(&mut |bytes| {
        peak = peak.max(bytes);
        Ok(())
    });
    (peak, result)
}

#[test]
fn finish_partial_reserves_one_slot_per_seen_point() {
    const SEEN: usize = 65_537;
    let schema = schema_with_grid_len(&["value"], SEEN as u64);
    let series = input(&schema, metric("m", &[]), 0, &[vec![Some(1.0); SEEN]]);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    aggregator.update(&series).unwrap();
    let expected = aggregator
        .state_bytes()
        .saturating_add(std::mem::size_of::<
            datatypes::series::aggregate_state::StateSeries,
        >())
        .saturating_add(std::mem::size_of::<datatypes::series::time::TimeAxis>())
        .saturating_add(
            SEEN.saturating_mul(
                std::mem::size_of::<datatypes::series::time::GridSpan>()
                    .saturating_add(std::mem::size_of::<usize>()),
            ),
        )
        .saturating_add(std::mem::size_of::<
            Vec<Option<datatypes::series::aggregate_state::StatePayload>>,
        >())
        .saturating_add(SEEN.saturating_mul(std::mem::size_of::<
            Option<datatypes::series::aggregate_state::StatePayload>,
        >()));
    let mut peak = 0usize;
    let partial = aggregator
        .finish_partial_budgeted(|bytes| {
            peak = peak.max(bytes);
            Ok(())
        })
        .unwrap();
    // Each field vector is reserved once, at the points that carry a state, so pushing does not
    // double it and the export holds one vector per field rather than one per point.
    assert_eq!(partial.groups[0].states.len(), 1);
    assert_eq!(partial.groups[0].states[0].len(), SEEN);
    assert_eq!(partial.groups[0].states[0].capacity(), SEEN);
    assert_eq!(peak, expected);
}

#[test]
fn merge_charges_one_block_per_new_block_not_per_point() {
    let names: Vec<String> = (0..64).map(|field| format!("f{field}")).collect();
    let labels: Vec<&str> = names.iter().map(String::as_str).collect();
    let schema = schema_with_grid_len(&labels, 64);
    let fields: Vec<Vec<Option<f64>>> = (0..64).map(|_| vec![Some(1.0); 64]).collect();
    let series = input(&schema, metric("m", &[]), 0, &fields);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    aggregator.update(&series).unwrap();
    let partial = aggregator.finish_partial().unwrap();
    assert_eq!(partial.groups[0].states[0].len(), 64);
    let block = block_bytes(64, AggregateOp::Sum);
    let mut merger = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
    let (peak, result) =
        peak_admission(|budget| merge_partial_budgeted(&mut merger, partial, budget));
    result.unwrap();
    assert!(
        peak < 8 * block,
        "admitted {peak} for the one block of {block} the points share"
    );
    assert_eq!(
        values_of(&merger.finish().unwrap(), 0, 64).1,
        [Some(1.0); 64]
    );
}

#[test]
fn merge_admits_groups_and_blocks_before_it_inserts_them() {
    let schema = schema(&["value"]);
    // A group opens on the first position its row carries, so every row here carries one: the
    // group, its index slot and its block are each admitted before they are allocated.
    let one_point =
        || TimeAxis::Grid(GridRange::try_new(schema.eval_grid.clone().unwrap(), 0, 1).unwrap());
    let point = || {
        Some(StatePayload::Float(AccumulatorState {
            value: 1.0,
            ..Default::default()
        }))
    };
    let partial = |ids: std::ops::Range<usize>| PartialAggregate {
        op: AggregateOp::Sum,
        groups: ids
            .map(|id| StateSeries {
                metric: metric("m", &[("i", &id.to_string())]),
                time: one_point(),
                states: vec![vec![point()]],
            })
            .collect(),
    };
    let mut merger = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
    let (peak, result) =
        peak_admission(|budget| merge_partial_budgeted(&mut merger, partial(0..64), budget));
    result.unwrap();
    assert!(peak > 0 && merger.state_bytes() > 0);
    // A refused group or block leaves nothing of itself behind; the caller drops the merge.
    let retained = merger.state_bytes();
    let refused = |_| Err(DataFusionError::Execution("pool exhausted".into()));
    assert!(merge_partial_budgeted(&mut merger, partial(100..108), refused).is_err());
    assert_eq!(merger.state_bytes(), retained);
    // A position one field carries and no field contributes to is not this merge's state.
    let invalid = PartialAggregate {
        op: AggregateOp::Sum,
        groups: vec![StateSeries {
            metric: metric("m", &[("i", "invalid")]),
            time: one_point(),
            states: vec![vec![None]],
        }],
    };
    assert!(merge_partial_budgeted(&mut merger, invalid, |_| Ok(())).is_err());
    assert_eq!(merger.state_bytes(), retained);
    assert!(
        merger
            .finish_budgeted(|_| Err(DataFusionError::Execution("pool exhausted".into())))
            .is_err()
    );
}

/// Building a label key copies the metric name, so the copy is admitted before it is made: a
/// refusal arrives before the merge touches anything, even for a group and block it already has.
#[test]
fn merge_admits_the_key_before_it_builds_it() {
    let schema = schema(&["value"]);
    let series = input(&schema, metric("m", &[("job", "a")]), 0, &[vec![Some(1.0)]]);
    let partial = || {
        let mut aggregator =
            FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
        aggregator.update(&series).unwrap();
        aggregator.finish_partial().unwrap()
    };
    let mut merger = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
    merge_partial(&mut merger, partial());
    let retained = merger.state_bytes();
    let mut calls = 0;
    let result = merge_partial_budgeted(&mut merger, partial(), |_| {
        calls += 1;
        Err(DataFusionError::Execution("pool exhausted".into()))
    });
    assert!(result.is_err());
    assert!(calls > 0, "the key was built without an admission");
    assert_eq!(merger.state_bytes(), retained, "a refused key was kept");

    // An existing group and block allocate no block, but the key still is admitted.
    let mut merger = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
    merge_partial(&mut merger, partial());
    let mut calls = 0;
    merge_partial_budgeted(&mut merger, partial(), |_| {
        calls += 1;
        Ok(())
    })
    .unwrap();
    assert!(
        calls > 0,
        "an existing group was merged without an admission"
    );
}

/// The merge holds two allocations of the metric name: its group's labels, and the copy its label
/// key made. Only one of them is why a longer name costs twice as much.
#[test]
fn a_long_metric_name_is_held_by_the_group_and_by_its_key() {
    let schema = schema(&["value"]);
    let retained = |name: &str| {
        // One point, so the group and its key are both opened; the block it needs is the same for
        // both names and cancels in the difference.
        let partial = PartialAggregate {
            op: AggregateOp::Sum,
            groups: vec![StateSeries {
                metric: metric(name, &[]),
                time: TimeAxis::Grid(
                    GridRange::try_new(schema.eval_grid.clone().unwrap(), 0, 1).unwrap(),
                ),
                states: vec![vec![Some(StatePayload::Float(AccumulatorState::default()))]],
            }],
        };
        let mut merger = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
        merge_partial(&mut merger, partial);
        merger.state_bytes()
    };
    const EXTRA: usize = 1000;
    let long = format!("m{}", "x".repeat(EXTRA));
    let delta = retained(&long) - retained("m");
    assert_eq!(
        delta,
        2 * EXTRA,
        "the name is held once by the group and once by the key"
    );
}

/// The budgeted partial path keeps the numbers of the public one: one partition, so the merged
/// result is the serial fold bit for bit, for every supported aggregation.
#[test]
fn budgeted_partial_state_merges_to_the_public_numbers() {
    let schema = schema(&["value"]);
    let series = input(
        &schema,
        metric("m", &[]),
        0,
        &[vec![Some(1.0), None, Some(3.0), Some(-2.0)]],
    );
    for op in [
        AggregateOp::Sum,
        AggregateOp::Count,
        AggregateOp::Avg,
        AggregateOp::Min,
        AggregateOp::Max,
        AggregateOp::Group,
        AggregateOp::Stddev,
        AggregateOp::Stdvar,
    ] {
        let serial = aggregate_float(
            &schema,
            std::slice::from_ref(&series),
            op,
            Grouping::By(&[]),
        )
        .unwrap();
        let expected = values_of(&serial, 0, 128);
        let mut aggregator = FloatAggregator::try_new(&schema, op, Grouping::By(&[])).unwrap();
        aggregator.update(&series).unwrap();
        let partial = aggregator.finish_partial_budgeted(|_| Ok(())).unwrap();
        let mut merger = FloatMerger::try_new(&schema, op).unwrap();
        merge_partial_budgeted(&mut merger, partial.clone(), |_| Ok(())).unwrap();
        let merged = merger.finish_budgeted(|_| Ok(())).unwrap();
        assert_eq!(values_of(&merged, 0, 128), expected, "{op:?} budgeted");
        let mut plain = FloatMerger::try_new(&schema, op).unwrap();
        merge_partial(&mut plain, partial);
        assert_eq!(
            values_of(&plain.finish().unwrap(), 0, 128),
            expected,
            "{op:?}"
        );
    }
}

/// Partial states merged in any partition and merge order give the serial result, within the
/// rounding a regrouped fold may change: the same groups, points and fields, the same NaN and
/// infinities. Each partial keeps only the components its aggregation declares, so the layout
/// in `state_fields` is all a merge needs.
#[test]
fn partials_merge_to_the_serial_result() {
    const OPS: [AggregateOp; 8] = [
        AggregateOp::Sum,
        AggregateOp::Count,
        AggregateOp::Avg,
        AggregateOp::Min,
        AggregateOp::Max,
        AggregateOp::Group,
        AggregateOp::Stddev,
        AggregateOp::Stdvar,
    ];
    for seed in 0..300 {
        let mut rng = StdRng::seed_from_u64(seed);
        let fields: Vec<String> = (0..rng.random_range(1..=3))
            .map(|field| format!("f{field}"))
            .collect();
        let names: Vec<&str> = fields.iter().map(String::as_str).collect();
        let schema = schema_with_grid_len(&names, rng.random_range(1..80));
        let points = schema.eval_grid.as_ref().unwrap().spec().len as usize;
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        let mut series = Vec::new();
        for index in 0..rng.random_range(1..12) {
            builder.begin_series();
            for point in 0..points {
                let row: Vec<Option<f64>> = (0..names.len())
                    .map(|_| rng.random_bool(0.6).then(|| partial_sample(&mut rng)))
                    .collect();
                builder.push(point, &row).unwrap();
            }
            let job = ["x", "y"][rng.random_range(0..2)];
            let index = index.to_string();
            series.extend(
                builder
                    .finish(metric("m", &[("job", job), ("i", &index)]))
                    .unwrap(),
            );
        }
        let parts: Vec<usize> = series.iter().map(|_| rng.random_range(0..4)).collect();
        let mut order: Vec<usize> = (0..4).collect();
        order.shuffle(&mut rng);
        let grouping = if rng.random_bool(0.5) {
            Grouping::By(&["job"])
        } else {
            Grouping::Without(&["i"])
        };

        for op in OPS {
            let serial = aggregate_float(&schema, &series, op, grouping).unwrap();
            let mut merger = FloatMerger::try_new(&schema, op).unwrap();
            for &part in &order {
                let mut aggregator = FloatAggregator::try_new(&schema, op, grouping).unwrap();
                for (series, _) in series.iter().zip(&parts).filter(|(_, p)| **p == part) {
                    aggregator.update(series).unwrap();
                }
                merge_partial(&mut merger, aggregator.finish_partial().unwrap());
            }
            let merged = merger.finish().unwrap();
            assert_flat_close(&merged, &serial, 1e-10, &format!("seed {seed} {op:?}"));

            // One partition holding every series is the serial fold, bit for bit.
            let mut aggregator = FloatAggregator::try_new(&schema, op, grouping).unwrap();
            for series in &series {
                aggregator.update(series).unwrap();
            }
            let mut merger = FloatMerger::try_new(&schema, op).unwrap();
            merge_partial(&mut merger, aggregator.finish_partial().unwrap());
            assert_flat_close(
                &merger.finish().unwrap(),
                &serial,
                0.0,
                &format!("seed {seed} {op:?} one partition"),
            );
        }

        let serial = count_values(&schema, &series, "value", grouping).unwrap();
        let partials = order.iter().flat_map(|&part| {
            let members: Vec<Series> = series
                .iter()
                .zip(&parts)
                .filter(|(_, p)| **p == part)
                .map(|(series, _)| series.clone())
                .collect();
            count_values(&schema, &members, "value", grouping).unwrap()
        });
        let merged = merge_count_values(&schema, partials).unwrap();
        assert_flat_close(&merged, &serial, 0.0, &format!("seed {seed} count_values"));
    }
}

/// The compensation travels with a partial and absorbs what adding two partial sums loses: `1`
/// survives next to `1e100` and `-1e100` whether it is a partition's own compensation or the
/// error of a merge, and `avg` divides it like the sum.
#[test]
fn compensation_survives_partials_and_merges() {
    assert_eq!(
        merged_and_serial(AggregateOp::Sum, &[&[-1e100], &[1e100, 1.0]]),
        (1.0, 1.0)
    );
    assert_eq!(
        merged_and_serial(AggregateOp::Sum, &[&[1e100], &[1.0], &[-1e100]]),
        (1.0, 1.0)
    );
    let (merged, serial) = merged_and_serial(AggregateOp::Avg, &[&[-1e100], &[1e100, 1.0]]);
    assert_eq!(merged, serial);
    assert!(crate::grid_series::reference::close(
        merged,
        1.0 / 3.0,
        1e-10
    ));
    // A partial's own compensation must survive a later cancellation of the sums: `1` rides in
    // the first partial's compensation while `1e84` and `-1e84` cancel across partials.
    let cancelling: &[&[f64]] = &[&[1e100, 1.0], &[-1e100, 1e84], &[-1e84]];
    assert_eq!(merged_and_serial(AggregateOp::Sum, cancelling), (1.0, 1.0));
    let (merged, serial) = merged_and_serial(AggregateOp::Avg, cancelling);
    assert_eq!(serial, 0.2);
    assert!(
        crate::grid_series::reference::close(merged, serial, 1e-10),
        "{merged}"
    );
    let (merged, serial) =
        merged_and_serial(AggregateOp::Avg, &[&[1e32, 1.0], &[-1e32, 1e16], &[-1e16]]);
    assert_eq!(serial, 0.2);
    assert!(
        crate::grid_series::reference::close(merged, serial, 1e-10),
        "{merged}"
    );
}

/// Merging finite partials does not overflow where the result is finite: `avg` switches to a
/// count-weighted mean when the summed partials overflow, and `stdvar` does not square the mean
/// difference on its own.
#[test]
fn merges_of_large_finite_partials_stay_finite() {
    for partitions in [
        &[&[1e308][..], &[1e308][..]][..],
        // Unequal counts, one partition already a mean: the weights decide the result.
        &[&[1e308, 1e308][..], &[0.0][..]][..],
        &[&[0.0][..], &[1e308, 1e308][..]][..],
    ] {
        let (merged, serial) = merged_and_serial(AggregateOp::Avg, partitions);
        assert!(
            merged.is_finite() && crate::grid_series::reference::close(merged, serial, 1e-10),
            "{partitions:?}: merged {merged} serial {serial}"
        );
    }
    let (stdvar, serial) = merged_and_serial(AggregateOp::Stdvar, &[&[0.0], &[1.5e154]]);
    assert!(stdvar.is_finite(), "{stdvar}");
    assert!(crate::grid_series::reference::close(stdvar, serial, 1e-10));
}

/// Samples that differ by about the rounding of their mean are outside the merge's agreement with
/// Prometheus: its serial Welford update rounds the mean back and doubles the variance, while the
/// pairwise merge keeps the exact one.
#[test]
fn ill_conditioned_variance_merges_to_the_exact_value() {
    let partitions: &[&[f64]] = &[&[1e16], &[1e16 + 2.0]];
    assert_eq!(
        merged_and_serial(AggregateOp::Stdvar, partitions),
        (1.0, 2.0)
    );
    assert_eq!(
        merged_and_serial(AggregateOp::Stddev, partitions),
        (1.0, std::f64::consts::SQRT_2)
    );
}

/// `min` and `max` keep the first of two equal values, so with `0` and `-0` the sign is that of
/// the partial merged first.
#[test]
fn equal_zero_extremes_follow_the_merge_order() {
    let schema = schema(&["value"]);
    for op in [AggregateOp::Min, AggregateOp::Max] {
        for (first, second) in [(0.0, -0.0), (-0.0, 0.0)] {
            let mut merger = FloatMerger::try_new(&schema, op).unwrap();
            for value in [first, second] {
                let mut aggregator =
                    FloatAggregator::try_new(&schema, op, Grouping::By(&[])).unwrap();
                aggregator
                    .update(&input(&schema, metric("m", &[]), 0, &[vec![Some(value)]]))
                    .unwrap();
                merge_partial(&mut merger, aggregator.finish_partial().unwrap());
            }
            let value = values_of(&merger.finish().unwrap(), 0, 128).1[0].unwrap();
            assert_eq!(
                value.to_bits(),
                f64::to_bits(first),
                "{op:?} {first} {second}"
            );
        }
    }
}

/// A row of another aggregation, field count or grid fails the merge.
#[test]
fn a_merge_rejects_state_that_does_not_fit() {
    let wide_schema = schema(&["a", "b"]);
    let schema = schema(&["value"]);
    let partial = |op| {
        let mut aggregator = FloatAggregator::try_new(&schema, op, Grouping::By(&[])).unwrap();
        aggregator
            .update(&input(&schema, metric("m", &[]), 5, &[vec![Some(1.0)]]))
            .unwrap();
        aggregator.finish_partial().unwrap()
    };
    let mut merger = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
    // Another aggregation: its state is not this merge's state.
    let other = state_row_of(AggregateOp::Avg, partial(AggregateOp::Avg).groups.remove(0));
    assert!(merger.merge_row(&other, |_| Ok(())).is_err());
    // More fields than the merge's schema holds: the shape no longer fits.
    let mut wide =
        FloatAggregator::try_new(&wide_schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    wide.update(&input(
        &wide_schema,
        metric("m", &[]),
        0,
        &[vec![Some(1.0)], vec![Some(2.0)]],
    ))
    .unwrap();
    let wide = state_row_of(
        AggregateOp::Sum,
        wide.finish_partial().unwrap().groups.remove(0),
    );
    assert!(merger.merge_row(&wide, |_| Ok(())).is_err());
    // An axis whose positions belong to another grid: the merge is bound to its own.
    // The fixture's grid holds 128 positions, so a position from a longer grid is out of bounds.
    let other_grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1, 256).unwrap()).unwrap());
    let mut late = state_row_of(AggregateOp::Sum, partial(AggregateOp::Sum).groups.remove(0));
    late.time = TimeAxis::from_grid_spans(other_grid, vec![GridSpan::new(200, 1)]).unwrap();
    assert!(merger.merge_row(&late, |_| Ok(())).is_err());
    // A carried position no field contributes to is not a state this merge can read.
    let empty = state_row_of(
        AggregateOp::Sum,
        StateSeries {
            metric: metric("m", &[]),
            time: TimeAxis::Grid(
                GridRange::try_new(schema.eval_grid.clone().unwrap(), 5, 1).unwrap(),
            ),
            states: vec![vec![None]],
        },
    );
    assert!(merger.merge_row(&empty, |_| Ok(())).is_err());
    assert!(merger.finish().unwrap().is_empty());
}

/// `2^-24` and the float below it format to different labels, so they count as two values: the
/// even 16-digit candidate for `2^-24` names the lower neighbour.
#[test]
fn count_values_keeps_neighbours_of_a_power_of_two_apart() {
    let schema = schema(&["value"]);
    let series = [0x3e70_0000_0000_0000, 0x3e6f_ffff_ffff_ffff]
        .into_iter()
        .enumerate()
        .map(|(index, bits)| {
            input(
                &schema,
                metric("m", &[("i", &index.to_string())]),
                0,
                &[vec![Some(f64::from_bits(bits))]],
            )
        })
        .collect::<Vec<_>>();
    let mut groups: Vec<_> = count_values(&schema, &series, "v", Grouping::By(&[]))
        .unwrap()
        .iter()
        .map(|series| {
            (
                series.metric.get("v").unwrap().to_string(),
                values(series, 0),
            )
        })
        .collect();
    groups.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        groups,
        [
            (
                "0.00000005960464477539062".to_string(),
                (0, vec![Some(1.0)])
            ),
            (
                "0.00000005960464477539063".to_string(),
                (0, vec![Some(1.0)])
            ),
        ]
    );
}

/// `count` and `group` ask whether a sample is present, so a histogram is one contribution.
///
/// Prometheus counts every present sample whatever its type is, and answers `1` for a non-empty
/// group. A native histogram therefore counts once - including one that observed nothing - and
/// contributes nothing to a group that has no other sample. Every other aggregate reads the value,
/// so it must still refuse a histogram rather than count it.
#[test]
fn count_and_group_count_histogram_samples() {
    use crate::grid_series::reference::OwnedHistogram;
    use crate::grid_series::{GridSample, GridSeriesBuilder};
    use crate::histogram_eval::float_histogram::FloatHistogram;

    // Two positions, so a series can be absent at the one under test: a series with no sample at
    // all is not an input, so sparse is how a missing contribution really arrives.
    let schema = schema_with_grid_len(&["value"], 2);
    let histogram =
        OwnedHistogram::parse("0/0/0/4000000000000000/3ff0000000000000/0/1_1/4000000000000000///");
    let empty = OwnedHistogram::parse("0/0/0/0/0/0/////");
    // `push_samples` takes one entry per field, so a position is one call.
    let series = |job: &str, first: Option<GridSample>, second: Option<GridSample>| {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        builder.push_samples(0, &[first]).unwrap();
        builder.push_samples(1, &[second]).unwrap();
        builder
            .finish(metric("m", &[("job", job)]))
            .unwrap()
            .unwrap()
    };
    let native = || {
        GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&histogram.view()).unwrap(),
        ))
    };
    let zero_observations = || {
        GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&empty.view()).unwrap(),
        ))
    };
    let value = |series: &[Series]| {
        let SampleRef::Float(value) = series[0].fields.as_slice()[0].sample(0) else {
            panic!("count and group answer floats");
        };
        value
    };

    // Two histograms at the first position, one of which observed nothing, are two present
    // samples there. The third series holds a float at the second position only, so the first
    // position has no contribution from it.
    let input = [
        series("a", Some(native()), None),
        series("b", Some(zero_observations()), None),
        series("c", None, Some(GridSample::Float(5.0))),
    ];
    assert_eq!(
        value(&aggregate_float(&schema, &input, AggregateOp::Count, Grouping::By(&[])).unwrap()),
        2.0,
        "every present histogram counts once"
    );
    assert_eq!(
        value(&aggregate_float(&schema, &input, AggregateOp::Group, Grouping::By(&[])).unwrap()),
        1.0,
    );
    // The sparse series must still contribute at the position it does hold: a fold that dropped it
    // would answer the same count at the first position and nothing at the second.
    let counted = aggregate_float(&schema, &input, AggregateOp::Count, Grouping::By(&[])).unwrap();
    let SampleRef::Float(second) = counted[0].fields.as_slice()[0].sample(1) else {
        panic!("a count is a float");
    };
    assert_eq!(second, 1.0, "the sparse series counts at its own position");

    // A float beside a histogram is one more present sample, not a different kind of answer.
    let mixed = [
        series("a", Some(native()), None),
        series("b", Some(GridSample::Float(5.0)), None),
    ];
    assert_eq!(
        value(&aggregate_float(&schema, &mixed, AggregateOp::Count, Grouping::By(&[])).unwrap()),
        2.0,
    );

    // `sum` and `avg` fold the payload instead of counting or skipping it, so a single histogram is
    // one contributing sample for both; their own tests cover what they answer.
    for op in [AggregateOp::Sum, AggregateOp::Avg] {
        let answer = aggregate_float(
            &schema,
            &[series("a", Some(native()), None)],
            op,
            Grouping::By(&[]),
        );
        assert!(answer.is_ok(), "{op:?} folds a histogram: {answer:?}");
    }
}

/// `min`, `max`, `stddev`, `stdvar` and `quantile` read a float value, so a histogram sample is
/// skipped rather than refused - and a group holding only histograms has no answer at all.
#[test]
fn value_reading_aggregates_skip_histogram_samples() {
    use crate::grid_series::reference::OwnedHistogram;
    use crate::grid_series::{GridSample, GridSeriesBuilder};
    use crate::histogram_eval::float_histogram::FloatHistogram;

    let schema = schema_with_grid_len(&["value"], 2);
    let histogram =
        OwnedHistogram::parse("0/0/0/4000000000000000/3ff0000000000000/0/1_1/4000000000000000///");
    let series = |job: &str, first: Option<GridSample>, second: Option<GridSample>| {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        builder.push_samples(0, &[first]).unwrap();
        builder.push_samples(1, &[second]).unwrap();
        builder
            .finish(metric("m", &[("job", job)]))
            .unwrap()
            .unwrap()
    };
    let native = || {
        GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&histogram.view()).unwrap(),
        ))
    };
    // Two float series and a histogram series: the histogram is skipped, so the answer is the
    // floats' - and the second position, where only the histogram is present, has no value.
    let input = [
        series("a", Some(GridSample::Float(3.0)), None),
        series("b", Some(GridSample::Float(7.0)), None),
        series("c", Some(native()), Some(native())),
    ];
    for (op, expected) in [(AggregateOp::Min, 3.0), (AggregateOp::Max, 7.0)] {
        let answer = aggregate_float(&schema, &input, op, Grouping::By(&[])).unwrap();
        let SampleRef::Float(first) = answer[0].fields.as_slice()[0].sample(0) else {
            panic!("{op:?} answers a float");
        };
        assert_eq!(first, expected, "{op:?} answered over the histogram sample");
        assert!(
            matches!(answer[0].fields.as_slice()[0].sample(1), SampleRef::Missing),
            "{op:?} answered a group holding only histograms"
        );
    }
    // `sum` and `avg` fold the payload, but one field's column cannot be both float and histogram,
    // so a group that holds both kinds in one field is refused rather than answered as one of them.
    for op in [AggregateOp::Sum, AggregateOp::Avg] {
        let error = aggregate_float(&schema, &input, op, Grouping::By(&[]))
            .expect_err("a field cannot carry both float and histogram samples");
        assert!(
            error.to_string().contains("cannot mix float and histogram"),
            "{op:?} answered {error:?}"
        );
    }
}

/// Prometheus counts a histogram sample under the text `FloatHistogram.String` writes it as, so
/// equal histograms share a group and a float beside them has its own.
#[test]
fn count_values_counts_histograms_by_their_text() {
    use crate::grid_series::reference::OwnedHistogram;
    use crate::grid_series::{GridSample, GridSeriesBuilder};
    use crate::histogram_eval::float_histogram::FloatHistogram;
    use crate::histogram_eval::histogram_string;

    let schema = schema_with_grid_len(&["value"], 1);
    let histogram =
        OwnedHistogram::parse("0/0/0/4000000000000000/3ff0000000000000/0/1_1/4000000000000000///");
    let text = histogram_string(&histogram.view()).unwrap();
    let series = |job: &str, sample: GridSample| {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        builder.push_samples(0, &[Some(sample)]).unwrap();
        builder
            .finish(metric("m", &[("job", job)]))
            .unwrap()
            .unwrap()
    };
    let sample = || {
        GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&histogram.view()).unwrap(),
        ))
    };
    let input = [
        series("a", sample()),
        series("b", sample()),
        series("c", GridSample::Float(2.0)),
    ];
    let mut counts: Vec<(String, f64)> = count_values(&schema, &input, "v", Grouping::By(&[]))
        .unwrap()
        .iter()
        .map(|series| {
            let SampleRef::Float(count) = series.fields.as_slice()[0].sample(0) else {
                panic!("a count is a float");
            };
            (series.metric.get("v").unwrap().to_string(), count)
        })
        .collect();
    counts.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(counts, [("2".to_string(), 1.0), (text, 2.0)]);
}

/// Prometheus' schema for a histogram whose buckets are given as explicit upper bounds.
const CUSTOM_BUCKETS: i32 = -53;

/// One native histogram sample over the given positive buckets.
///
/// The counts are given by bucket index, so a test can put an observation at one resolution's index
/// or another's; the spans are the consecutive runs those indices form.
fn histogram_sample(
    schema: i32,
    buckets: &[(i32, f64)],
    custom_values: &[f64],
    count: f64,
    sum: f64,
    reset_hint: CounterResetHint,
) -> GridSample {
    let indices: Vec<i32> = buckets.iter().map(|&(index, _)| index).collect();
    let counts: Vec<f64> = buckets.iter().map(|&(_, count)| count).collect();
    let (offsets, lengths) = spans_of(&indices);
    let empty = BucketSideView {
        span_offsets: &[],
        span_lengths: &[],
        buckets: &[],
    };
    let mut builder = HistogramBatchBuilder::new();
    builder
        .push(HistogramView {
            schema,
            zero_threshold: 0.0,
            sum,
            count,
            zero_count: 0.0,
            reset_hint,
            start_timestamp: Some(1_700_000_000_000),
            custom_values,
            positive: BucketSideView {
                span_offsets: &offsets,
                span_lengths: &lengths,
                buckets: &counts,
            },
            negative: empty,
        })
        .unwrap();
    let batch = builder.finish().unwrap();
    GridSample::Histogram(Box::new(
        FloatHistogram::try_from_view(&batch.view(0).unwrap()).unwrap(),
    ))
}

/// The spans consecutive runs of `indices` form, in the encoding a [`HistogramView`] takes.
fn spans_of(indices: &[i32]) -> (Vec<i32>, Vec<i32>) {
    let mut offsets: Vec<i32> = Vec::new();
    let mut lengths: Vec<i32> = Vec::new();
    let mut cursor = 0i32;
    for &index in indices {
        if !offsets.is_empty() && cursor == index {
            *lengths.last_mut().expect("a span") += 1;
            cursor += 1;
            continue;
        }
        offsets.push(if offsets.is_empty() {
            index
        } else {
            index - cursor
        });
        lengths.push(1);
        cursor = index + 1;
    }
    (offsets, lengths)
}

/// A series of one field holding `samples` at their absolute grid positions.
fn histogram_series(
    schema: &Arc<SeriesSchema>,
    job: &str,
    samples: Vec<(usize, GridSample)>,
) -> Series {
    let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
    for (position, sample) in samples {
        builder.push_samples(position, &[Some(sample)]).unwrap();
    }
    builder
        .finish(metric("m", &[("job", job)]))
        .unwrap()
        .unwrap()
}

/// The bucket indices and counts one side of an answered histogram carries.
fn side_buckets(side: &BucketSideView<'_>) -> Vec<(i32, f64)> {
    let mut out = Vec::new();
    let mut cursor = 0i32;
    let mut position = 0usize;
    for (span, (&offset, &length)) in side.span_offsets.iter().zip(side.span_lengths).enumerate() {
        cursor = if span == 0 { offset } else { cursor + offset };
        for _ in 0..length {
            out.push((cursor, side.buckets[position]));
            cursor += 1;
            position += 1;
        }
    }
    out
}

/// The native histogram an aggregate answered at one position of one field.
fn answered_histogram(series: &Series, field: usize, position: usize) -> HistogramView<'_> {
    match series.fields.as_slice()[field].sample(position) {
        SampleRef::Histogram(view) => view,
        SampleRef::Missing => panic!("the aggregate answered nothing at {position}"),
        SampleRef::Float(value) => panic!("the aggregate answered the float {value} at {position}"),
    }
}

fn answers_nothing(series: &Series, field: usize, position: usize) -> bool {
    matches!(
        series.fields.as_slice()[field].sample(position),
        SampleRef::Missing
    )
}

/// Two histograms of different exponential resolutions add into the coarser one, and a clean fold
/// records no evidence at all.
#[test]
fn sum_adds_histograms_of_two_exponential_resolutions() {
    let schema = schema_with_grid_len(&["value"], 1);
    // Schema 1 divides each of schema 0's buckets in two: schema 0's index 2 covers schema 1's
    // indices 3 and 4, so the finer sample's two buckets there have to merge.
    let coarse = histogram_sample(
        0,
        &[(0, 2.0), (1, 3.0)],
        &[],
        5.0,
        11.0,
        CounterResetHint::Unknown,
    );
    let fine = histogram_sample(
        1,
        &[(0, 1.0), (3, 4.0), (4, 1.0)],
        &[],
        6.0,
        13.0,
        CounterResetHint::Unknown,
    );
    let input = [
        histogram_series(&schema, "a", vec![(0, coarse)]),
        histogram_series(&schema, "b", vec![(0, fine)]),
    ];
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    for series in &input {
        aggregator.update(series).unwrap();
    }
    let evidence = aggregator.histogram_evidence(0, 0, 0).expect("a state");
    assert_eq!(
        evidence,
        HistogramEvidence::default(),
        "a clean fold reports nothing"
    );
    let answer = aggregator.finish().unwrap();
    let value = answered_histogram(&answer[0], 0, 0);
    assert_eq!(
        value.schema, 0,
        "the result is reduced to the coarser schema"
    );
    assert_eq!(value.count, 11.0);
    assert_eq!(value.sum, 24.0);
    assert_eq!(
        side_buckets(&value.positive),
        vec![(0, 3.0), (1, 3.0), (2, 5.0)]
    );
}

/// Custom buckets with different bounds add into their intersection, and the fold records that it
/// had to reconcile them.
#[test]
fn sum_reconciles_different_custom_bounds() {
    let schema = schema_with_grid_len(&["value"], 1);
    let first = histogram_sample(
        CUSTOM_BUCKETS,
        &[(0, 1.0), (1, 2.0), (2, 3.0)],
        &[1.0, 2.0, 4.0],
        6.0,
        10.0,
        CounterResetHint::Unknown,
    );
    let second = histogram_sample(
        CUSTOM_BUCKETS,
        &[(0, 1.0), (1, 1.0)],
        &[1.0, 3.0],
        2.0,
        4.0,
        CounterResetHint::Unknown,
    );
    let input = [
        histogram_series(&schema, "a", vec![(0, first)]),
        histogram_series(&schema, "b", vec![(0, second)]),
    ];
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    for series in &input {
        aggregator.update(series).unwrap();
    }
    let evidence = aggregator.histogram_evidence(0, 0, 0).expect("a state");
    assert!(evidence.custom_bounds_reconciled, "{evidence:?}");
    assert!(!evidence.poisoned);
    let answer = aggregator.finish().unwrap();
    let value = answered_histogram(&answer[0], 0, 0);
    assert_eq!(
        value.custom_values,
        [1.0],
        "the intersection of both bounds"
    );
    assert_eq!(value.count, 8.0);
    assert_eq!(value.sum, 14.0);
    // Bound 1.0 is the only one both samples share, so it is the only bucket boundary left: the
    // sample with 2, 4 above it folds both of its buckets into the same bucket, and the counts meet.
    assert_eq!(side_buckets(&value.positive), vec![(0, 2.0), (1, 6.0)]);
}

/// A point whose samples mix exponential and custom buckets cannot be added, so it is removed --
/// and the fold keeps the evidence that says why.
#[test]
fn sum_poisons_a_point_whose_layouts_cannot_be_added() {
    let schema = schema_with_grid_len(&["value"], 2);
    let exponential = histogram_sample(0, &[(0, 1.0)], &[], 1.0, 2.0, CounterResetHint::Unknown);
    let custom = histogram_sample(
        CUSTOM_BUCKETS,
        &[(0, 1.0)],
        &[1.0],
        1.0,
        3.0,
        CounterResetHint::Unknown,
    );
    // The two families meet at the second position, and the first still answers: a poisoned point
    // removes itself, not its group.
    let input = [
        histogram_series(
            &schema,
            "a",
            vec![(
                0,
                histogram_sample(0, &[(0, 1.0)], &[], 1.0, 2.0, CounterResetHint::Unknown),
            )],
        ),
        histogram_series(&schema, "b", vec![(1, custom)]),
        histogram_series(&schema, "c", vec![(1, exponential)]),
    ];
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    for series in &input {
        aggregator.update(series).unwrap();
    }
    let evidence = aggregator.histogram_evidence(0, 1, 0).expect("a state");
    assert!(evidence.poisoned, "{evidence:?}");
    assert!(evidence.incompatible_layout, "{evidence:?}");
    assert!(!evidence.custom_bounds_reconciled);
    let answer = aggregator.finish().unwrap();
    let first = answered_histogram(&answer[0], 0, 0);
    assert_eq!(first.sum, 2.0, "the exponential point still answers");
    assert!(
        answers_nothing(&answer[0], 0, 1),
        "the poisoned point is removed"
    );

    // A group whose only point is poisoned has nothing to answer at all.
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    for series in &input[1..] {
        aggregator.update(series).unwrap();
    }
    assert!(
        aggregator.finish().unwrap().is_empty(),
        "a group with only a poisoned point answers nothing"
    );
}

/// The fold records which reset hints it saw, one bit per hint, so a merge of two partitions can
/// report a contradiction between them.
#[test]
fn sum_records_both_counter_reset_hints() {
    let schema = schema_with_grid_len(&["value"], 1);
    let reset = histogram_sample(
        0,
        &[(0, 1.0)],
        &[],
        1.0,
        1.0,
        CounterResetHint::CounterReset,
    );
    let not_reset = histogram_sample(
        0,
        &[(0, 1.0)],
        &[],
        1.0,
        1.0,
        CounterResetHint::NotCounterReset,
    );
    let input = [
        histogram_series(&schema, "a", vec![(0, reset)]),
        histogram_series(&schema, "b", vec![(0, not_reset)]),
    ];
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    for series in &input {
        aggregator.update(series).unwrap();
    }
    let evidence = aggregator.histogram_evidence(0, 0, 0).expect("a state");
    assert!(evidence.counter_reset_seen, "{evidence:?}");
    assert!(evidence.not_counter_reset_seen, "{evidence:?}");
    assert!(!evidence.poisoned);
    // The two samples met at one point of one group, so that point contradicts itself.
    assert!(aggregator.evidence_summary().counter_reset_collision);
    // The hints are evidence, not payload: the answer is the two histograms' sum.
    let answer = aggregator.finish().unwrap();
    let value = answered_histogram(&answer[0], 0, 0);
    assert_eq!(value.count, 2.0);
    assert_eq!(value.sum, 2.0);
}

/// The reset hints are per point: a job that only ever resets and a job that never does are two
/// ordinary points, whatever the OR of their two bits says. Prometheus decides the contradiction
/// per group per step, and so does the summary's collision bit.
#[test]
fn sum_by_job_does_not_collide_a_reset_in_one_job_with_none_in_another() {
    let schema = schema_with_grid_len(&["value"], 1);
    let reset = histogram_sample(
        0,
        &[(0, 1.0)],
        &[],
        1.0,
        1.0,
        CounterResetHint::CounterReset,
    );
    let not_reset = histogram_sample(
        0,
        &[(0, 1.0)],
        &[],
        1.0,
        1.0,
        CounterResetHint::NotCounterReset,
    );
    let input = [
        histogram_series(&schema, "a", vec![(0, reset)]),
        histogram_series(&schema, "b", vec![(0, not_reset)]),
    ];
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["job"])).unwrap();
    for series in &input {
        aggregator.update(series).unwrap();
    }
    assert_eq!(aggregator.groups(), 2, "one group per job");
    let evidence = aggregator.evidence_summary();
    // Both bits are there -- they are what the two points reported -- and no point contradicts
    // itself, which is what decides the message.
    assert!(evidence.counter_reset_seen, "{evidence:?}");
    assert!(evidence.not_counter_reset_seen, "{evidence:?}");
    assert!(!evidence.counter_reset_collision, "{evidence:?}");
    // Each group answers on its own, so the answer is not removed either.
    let answer = aggregator.finish().unwrap();
    assert_eq!(answer.len(), 2);
    for series in &answer {
        assert_eq!(answered_histogram(series, 0, 0).count, 1.0);
    }
}

/// A poisoned point answers nothing, so its hints neither contradict nor agree: like its
/// reconciliation, the collision stays out of the summary -- and the point is removed anyway.
#[test]
fn a_poisoned_point_reports_no_reset_collision() {
    let schema = schema_with_grid_len(&["value"], 1);
    let exponential = histogram_sample(
        0,
        &[(0, 1.0)],
        &[],
        1.0,
        2.0,
        CounterResetHint::CounterReset,
    );
    let custom = histogram_sample(
        CUSTOM_BUCKETS,
        &[(0, 1.0)],
        &[1.0],
        1.0,
        3.0,
        CounterResetHint::NotCounterReset,
    );
    let input = [
        histogram_series(&schema, "a", vec![(0, exponential)]),
        histogram_series(&schema, "b", vec![(0, custom)]),
    ];
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    for series in &input {
        aggregator.update(series).unwrap();
    }
    let evidence = aggregator.evidence_summary();
    assert!(evidence.poisoned, "{evidence:?}");
    assert!(evidence.counter_reset_seen && evidence.not_counter_reset_seen);
    assert!(
        !evidence.counter_reset_collision,
        "a removed point does not contradict anything: {evidence:?}"
    );
    assert!(
        aggregator.finish().unwrap().is_empty(),
        "the poisoned point answers nothing"
    );
}

/// A histogram that observed nothing is still a sample: it opens the point, and it adds nothing.
#[test]
fn sum_counts_a_zero_observation_histogram_as_a_sample() {
    let schema = schema_with_grid_len(&["value"], 2);
    let empty = histogram_sample(0, &[], &[], 0.0, 0.0, CounterResetHint::Unknown);
    let observed = || histogram_sample(0, &[(0, 2.0)], &[], 2.0, 6.0, CounterResetHint::Unknown);
    let input = [
        histogram_series(&schema, "a", vec![(0, empty), (1, observed())]),
        histogram_series(&schema, "b", vec![(1, observed())]),
    ];
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    for series in &input {
        aggregator.update(series).unwrap();
    }
    let answer = aggregator.finish().unwrap();
    let first = answered_histogram(&answer[0], 0, 0);
    assert_eq!(first.count, 0.0);
    assert_eq!(first.sum, 0.0);
    assert!(
        first.positive.buckets.is_empty(),
        "a histogram that observed nothing has no buckets to answer"
    );
    let second = answered_histogram(&answer[0], 0, 1);
    assert_eq!(second.count, 4.0);
    assert_eq!(second.sum, 12.0);
    assert_eq!(side_buckets(&second.positive), vec![(0, 4.0)]);
}

/// The scalar fields of three histograms that cancel are summed with compensation, so the answer is
/// the correction and not the plain left-to-right `0`.
#[test]
fn sum_keeps_the_kahan_correction_across_cancellation() {
    let schema = schema_with_grid_len(&["value"], 1);
    let input: Vec<Series> = [1e16, 1.0, -1e16]
        .into_iter()
        .enumerate()
        .map(|(index, sum)| {
            histogram_series(
                &schema,
                &index.to_string(),
                vec![(
                    0,
                    histogram_sample(0, &[(0, 1.0)], &[], 1.0, sum, CounterResetHint::Unknown),
                )],
            )
        })
        .collect();
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    for series in &input {
        aggregator.update(series).unwrap();
    }
    let answer = aggregator.finish().unwrap();
    let value = answered_histogram(&answer[0], 0, 0);
    assert_eq!(value.sum, 1.0, "the compensated sum is 1, not the legacy 0");
    assert_eq!(value.count, 3.0);
    assert_eq!(side_buckets(&value.positive), vec![(0, 3.0)]);
}

/// One histogram is its own sum: the fold neither extrapolates nor changes the payload's meaning,
/// and the start timestamp of the sample does not survive into a derived result.
#[test]
fn sum_answers_a_single_histogram_unchanged() {
    let schema = schema_with_grid_len(&["value"], 1);
    let sample = histogram_sample(
        0,
        &[(0, 1.5), (2, 2.5)],
        &[],
        4.0,
        9.0,
        CounterResetHint::NotCounterReset,
    );
    let input = [histogram_series(&schema, "a", vec![(0, sample)])];
    let answer = aggregate_float(&schema, &input, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    let value = answered_histogram(&answer[0], 0, 0);
    assert_eq!(value.schema, 0);
    assert_eq!(value.count, 4.0);
    assert_eq!(value.sum, 9.0);
    assert_eq!(value.zero_count, 0.0);
    assert_eq!(side_buckets(&value.positive), vec![(0, 1.5), (2, 2.5)]);
    assert!(
        value.start_timestamp.is_none(),
        "a derived result carries no start timestamp of its own"
    );
}
/// The histogram the aggregate answered at the first position of its single group.
fn aggregate_answer(schema: &Arc<SeriesSchema>, input: &[Series], op: AggregateOp) -> Series {
    let mut answer = aggregate_float(schema, input, op, Grouping::By(&[])).unwrap();
    assert_eq!(answer.len(), 1, "one group");
    answer.remove(0)
}

/// An average divides the folded sum -- and its bucket counts -- by the number of samples.
#[test]
fn avg_divides_the_sum_by_the_sample_count() {
    let schema = schema_with_grid_len(&["value"], 1);
    let sample = |count: f64, sum: f64| {
        histogram_sample(0, &[(0, count)], &[], count, sum, CounterResetHint::Unknown)
    };
    let two = [
        histogram_series(&schema, "a", vec![(0, sample(2.0, 3.0))]),
        histogram_series(&schema, "b", vec![(0, sample(4.0, 5.0))]),
    ];
    let answer = aggregate_answer(&schema, &two, AggregateOp::Avg);
    let value = answered_histogram(&answer, 0, 0);
    assert_eq!(value.sum, 4.0, "(3 + 5) / 2");
    assert_eq!(value.count, 3.0, "(2 + 4) / 2");
    assert_eq!(side_buckets(&value.positive), vec![(0, 3.0)]);

    let three = [
        histogram_series(&schema, "a", vec![(0, sample(2.0, 3.0))]),
        histogram_series(&schema, "b", vec![(0, sample(4.0, 5.0))]),
        histogram_series(&schema, "c", vec![(0, sample(6.0, 10.0))]),
    ];
    let answer = aggregate_answer(&schema, &three, AggregateOp::Avg);
    let value = answered_histogram(&answer, 0, 0);
    assert_eq!(value.sum, 6.0, "(3 + 5 + 10) / 3");
    assert_eq!(value.count, 4.0, "(2 + 4 + 6) / 3");
    assert_eq!(side_buckets(&value.positive), vec![(0, 4.0)]);
}

/// The average keeps the compensated sum's correction, so three histograms that cancel average to
/// `1/3` and not to the plain left-to-right `0`.
#[test]
fn avg_keeps_the_kahan_correction_across_cancellation() {
    let schema = schema_with_grid_len(&["value"], 1);
    let input: Vec<Series> = [1e16, 1.0, -1e16]
        .into_iter()
        .enumerate()
        .map(|(index, sum)| {
            histogram_series(
                &schema,
                &index.to_string(),
                vec![(
                    0,
                    histogram_sample(0, &[(0, 1.0)], &[], 1.0, sum, CounterResetHint::Unknown),
                )],
            )
        })
        .collect();
    let answer = aggregate_answer(&schema, &input, AggregateOp::Avg);
    let value = answered_histogram(&answer, 0, 0);
    assert_eq!(
        value.sum,
        1.0 / 3.0,
        "the compensated average is 1/3, not the legacy 0"
    );
    assert_eq!(value.count, 1.0, "three samples of one observation each");
    assert_eq!(side_buckets(&value.positive), vec![(0, 1.0)]);
}

/// Two histograms of `1e308` at one observation each average to a finite `1e308`: the sum overflows,
/// so the fold discards the trial, starts a mean from the sum it had, and divides before adding.
#[test]
fn avg_switches_to_a_mean_where_the_sum_would_overflow() {
    let schema = schema_with_grid_len(&["value"], 1);
    let input = [
        histogram_series(
            &schema,
            "a",
            vec![(
                0,
                histogram_sample(0, &[(0, 1.0)], &[], 1.0, 1e308, CounterResetHint::Unknown),
            )],
        ),
        histogram_series(
            &schema,
            "b",
            vec![(
                0,
                histogram_sample(0, &[(0, 1.0)], &[], 1.0, 1e308, CounterResetHint::Unknown),
            )],
        ),
    ];
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Avg, Grouping::By(&[])).unwrap();
    for series in &input {
        aggregator.update(series).unwrap();
    }
    // Overflow switches the mode; it neither poisons the point nor answers `Inf`.
    let evidence = aggregator.histogram_evidence(0, 0, 0).expect("a state");
    assert!(!evidence.poisoned, "{evidence:?}");
    let answer = aggregator.finish().unwrap();
    let value = answered_histogram(&answer[0], 0, 0);
    assert!(value.sum.is_finite(), "an average of finite sums is finite");
    assert_eq!(value.sum, 1e308, "(1e308 + 1e308) / 2");
    assert_eq!(value.count, 1.0, "(1 + 1) / 2");
    assert_eq!(side_buckets(&value.positive), vec![(0, 1.0)]);
}

/// One histogram averaged is itself, divided by the one sample that produced it.
#[test]
fn avg_answers_a_single_histogram_divided_by_one() {
    let schema = schema_with_grid_len(&["value"], 1);
    let input = [histogram_series(
        &schema,
        "a",
        vec![(
            0,
            histogram_sample(
                0,
                &[(0, 1.5), (2, 2.5)],
                &[],
                4.0,
                9.0,
                CounterResetHint::Unknown,
            ),
        )],
    )];
    let answer = aggregate_answer(&schema, &input, AggregateOp::Avg);
    let value = answered_histogram(&answer, 0, 0);
    assert_eq!(value.sum, 9.0);
    assert_eq!(value.count, 4.0);
    assert_eq!(side_buckets(&value.positive), vec![(0, 1.5), (2, 2.5)]);
    assert!(
        value.start_timestamp.is_none(),
        "a derived result carries no start timestamp of its own"
    );
}

/// A histogram that observed nothing still counts in the average's divisor: the divisor is the
/// number of samples, not the number of observations.
#[test]
fn avg_counts_a_zero_observation_histogram_in_its_divisor() {
    let schema = schema_with_grid_len(&["value"], 1);
    let empty = histogram_sample(0, &[], &[], 0.0, 0.0, CounterResetHint::Unknown);
    let observed = histogram_sample(0, &[(0, 2.0)], &[], 2.0, 6.0, CounterResetHint::Unknown);
    let input = [
        histogram_series(&schema, "a", vec![(0, empty)]),
        histogram_series(&schema, "b", vec![(0, observed)]),
    ];
    let answer = aggregate_answer(&schema, &input, AggregateOp::Avg);
    let value = answered_histogram(&answer, 0, 0);
    // Two samples, two observations: the empty one is half the divisor even though it observed
    // nothing.
    assert_eq!(value.count, 1.0);
    assert_eq!(value.sum, 3.0);
    assert_eq!(side_buckets(&value.positive), vec![(0, 1.0)]);
}

/// Once an average has switched, the mean keeps tracking the samples that follow: two histograms of
/// `2^1023` force the switch, and two samples that observed nothing bring the mean down to half.
#[test]
fn avg_keeps_tracking_the_mean_after_the_switch() {
    let schema = schema_with_grid_len(&["value"], 1);
    let large = 2f64.powi(1023);
    // `large + large` is `Inf`, so the second sample is what forces the switch.
    assert!((large + large).is_infinite());
    let observed = || histogram_sample(0, &[(0, 1.0)], &[], 1.0, large, CounterResetHint::Unknown);
    let nothing = || histogram_sample(0, &[], &[], 0.0, 0.0, CounterResetHint::Unknown);
    let input = [
        histogram_series(&schema, "a", vec![(0, observed())]),
        histogram_series(&schema, "b", vec![(0, observed())]),
        histogram_series(&schema, "c", vec![(0, nothing())]),
        histogram_series(&schema, "d", vec![(0, nothing())]),
    ];
    let answer = aggregate_answer(&schema, &input, AggregateOp::Avg);
    let value = answered_histogram(&answer, 0, 0);
    // Four samples, two observations of `large` each: the mean is `large / 2` to within the
    // roundings of the weights `2/3` and `3/4` the mean path applies.
    let expected = large / 2.0;
    assert!(value.sum.is_finite());
    assert!(
        (value.sum - expected).abs() <= expected * 1e-14,
        "the mean tracked the switch: {} vs {expected}",
        value.sum
    );
    assert!(
        (value.sum - large).abs() > expected / 2.0,
        "the answer is not the sum the switch started from: {}",
        value.sum
    );
    // The count is an average like any other component: two observations over four samples.
    assert_eq!(value.count, 0.5, "(1 + 1 + 0 + 0) / 4");
    assert_eq!(side_buckets(&value.positive), vec![(0, 0.5)]);
}

/// The state a fold over `input` exports for a merge.
fn partial_state(
    op: AggregateOp,
    schema: &Arc<SeriesSchema>,
    input: &[Series],
) -> PartialAggregate {
    let mut aggregator = FloatAggregator::try_new(schema, op, Grouping::By(&[])).unwrap();
    for series in input {
        aggregator.update(series).unwrap();
    }
    aggregator.finish_partial().unwrap()
}

/// The per-group states a merge of `partials`' histogram payloads produces, in merge order.
///
/// A histogram payload's merge rules -- how the mean path weights each side by its own count, and
/// which evidence a merge keeps -- are `HistogramAccumulator`'s own `from_payload` and `merge_state`,
/// so a test that asks for those rules asks that API. Re-encoding the payload into the Arrow row an
/// executor hands over would restate a layout the state encoder already owns, in the fixture.
fn merged_histogram_groups(op: AggregateOp, partials: &[PartialAggregate]) -> Vec<Group> {
    let mut groups: Vec<Group> = Vec::new();
    let mut index: HashMap<LabelKey, usize> = HashMap::new();
    for partial in partials {
        assert_eq!(partial.op, op, "a merge folds the partitions of one op");
        for series in &partial.groups {
            // Groups pair by label set, so two partitions of one group meet at the same position.
            let at = *index.entry(label_key(&series.metric)).or_insert_with(|| {
                groups.push(Group {
                    metric: series.metric.clone(),
                    blocks: BTreeMap::new(),
                    histograms: BTreeMap::new(),
                });
                groups.len() - 1
            });
            let histograms = &mut groups[at].histograms;
            for (point, position) in series.time.positions() {
                for (field, states) in series.states.iter().enumerate() {
                    let Some(payload) = states[position].as_ref().and_then(StatePayload::histogram)
                    else {
                        continue;
                    };
                    match histograms.get_mut(&(point, field)) {
                        Some(accumulator) => accumulator.merge_state(op, payload).unwrap(),
                        None => {
                            histograms.insert(
                                (point, field),
                                HistogramAccumulator::from_payload(op, payload).unwrap(),
                            );
                        }
                    }
                }
            }
        }
    }
    groups
}

/// The answer a merge of `partials` reports at the first group's first point and field, or `None`
/// where the merge answers nothing there.
fn merged_histogram_answer(
    op: AggregateOp,
    partials: &[PartialAggregate],
) -> Option<FloatHistogram> {
    let groups = merged_histogram_groups(op, partials);
    let accumulator = groups[0]
        .histograms
        .get(&(0, 0))
        .expect("the merged histogram point");
    finished_histogram(op, accumulator).unwrap()
}

/// The evidence a merge of `partials` recorded, once a point's bits are read as the summary reports
/// them.
fn merged_histogram_evidence(op: AggregateOp, partials: &[PartialAggregate]) -> HistogramEvidence {
    evidence_summary(&merged_histogram_groups(op, partials))
}

/// One empty histogram row: the placeholder of the component's type a field with no state needs
/// beside its null item, and the one-row struct a null histogram column is padded with.
fn placeholder_histogram() -> Arc<HistogramBatch> {
    let empty = BucketSideView {
        span_offsets: &[],
        span_lengths: &[],
        buckets: &[],
    };
    let mut builder = HistogramBatchBuilder::new();
    builder
        .push(HistogramView {
            schema: 0,
            zero_threshold: 0.0,
            sum: 0.0,
            count: 0.0,
            zero_count: 0.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: &[],
            positive: empty,
            negative: empty,
        })
        .unwrap();
    builder.finish().unwrap()
}

/// One partition's histogram state at the grid's first position, encoded as the Arrow-backed row the
/// executor hands the merge: one list item per field, whose columns are the kind's own components in
/// [`AggregateStateKind::components`] order, and a null item where that field contributed nothing.
///
/// The two payload components are the payload's own one-row batches, which is what a frame writes
/// back out, so the row carries the state a partition exported rather than a second encoding of it.
/// This is the histogram counterpart of [`state_row_of`], which keeps refusing a histogram payload:
/// a numeric fold never produces one, so only a fixture that *decodes* such a row may build one.
fn histogram_state_row(
    kind: AggregateStateKind,
    schema: &Arc<SeriesSchema>,
    metric: SeriesLabels,
    states: &[Option<&HistogramState>],
) -> StateRow {
    let state_schema = AggregateStateSchema::try_new(kind, kind.components().to_vec()).unwrap();
    let item_fields: datatypes::arrow::datatypes::Fields = kind
        .components()
        .iter()
        .map(|component| {
            let ty = match component {
                StateComponent::HistogramValue | StateComponent::HistogramCompensation => {
                    placeholder_histogram().as_arrow().data_type().clone()
                }
                StateComponent::Count => DataType::Float64,
                _ => DataType::Boolean,
            };
            Field::new(component.as_str(), ty, true)
        })
        .collect();
    let mut lists: Vec<Arc<dyn Array>> = Vec::with_capacity(states.len());
    for state in states {
        let columns = kind
            .components()
            .iter()
            .map(|component| match component {
                StateComponent::HistogramValue => Arc::new(
                    state
                        .map_or_else(placeholder_histogram, |state| state.value.clone())
                        .as_arrow()
                        .clone(),
                ) as Arc<dyn Array>,
                StateComponent::HistogramCompensation => Arc::new(
                    state
                        .map_or_else(placeholder_histogram, |state| state.compensation.clone())
                        .as_arrow()
                        .clone(),
                ) as Arc<dyn Array>,
                StateComponent::Count => {
                    Arc::new(Float64Array::from(vec![state.map(|state| state.count)]))
                        as Arc<dyn Array>
                }
                _ => Arc::new(BooleanArray::from(vec![
                    state.and_then(|state| state.bool_component(*component)),
                ])) as Arc<dyn Array>,
            })
            .collect::<Vec<_>>();
        let items = StructArray::new(
            item_fields.clone(),
            columns,
            Some(NullBuffer::from(vec![state.is_some()])),
        );
        lists.push(Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(item_fields.clone()),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1])),
            Arc::new(items),
            None,
        )));
    }
    let fields: datatypes::arrow::datatypes::Fields = lists
        .iter()
        .enumerate()
        .map(|(index, list)| Field::new(format!("f{index}"), list.data_type().clone(), false))
        .collect();
    let batch =
        StateFieldsBatch::try_from_arrow(StructArray::new(fields, lists, None), &state_schema)
            .unwrap();
    StateRow {
        metric,
        time: TimeAxis::Grid(GridRange::try_new(schema.eval_grid.clone().unwrap(), 0, 1).unwrap()),
        fields: batch,
        kind,
    }
}

/// The same row with one component replaced by an all-null column: an item that is present while the
/// column the decoder reads from it is not, which is the state a writer that dropped a component
/// leaves behind.
fn row_without_component(row: &StateRow, component: StateComponent) -> StateRow {
    let dropped = row
        .fields
        .components()
        .iter()
        .position(|declared| *declared == component)
        .expect("the row declares the component");
    let mut lists: Vec<Arc<dyn Array>> = Vec::new();
    for field in 0..row.fields.len() {
        let items = row.fields.field(field).expect("the row has this field");
        let columns = items
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| {
                if index == dropped {
                    datatypes::arrow::array::new_null_array(column.data_type(), column.len())
                } else {
                    column.clone()
                }
            })
            .collect::<Vec<_>>();
        let item_fields = items.fields().clone();
        lists.push(Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(item_fields.clone()),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, items.len() as i32])),
            Arc::new(StructArray::new(
                item_fields,
                columns,
                items.nulls().cloned(),
            )),
            None,
        )));
    }
    let fields: datatypes::arrow::datatypes::Fields = lists
        .iter()
        .enumerate()
        .map(|(index, list)| Field::new(format!("f{index}"), list.data_type().clone(), false))
        .collect();
    let state_schema =
        AggregateStateSchema::try_new(row.kind, row.fields.components().to_vec()).unwrap();
    StateRow {
        metric: row.metric.clone(),
        time: row.time.clone(),
        fields: StateFieldsBatch::try_from_arrow(
            StructArray::new(fields, lists, None),
            &state_schema,
        )
        .unwrap(),
        kind: row.kind,
    }
}

/// The merge of `rows`, as the executor runs it: one partition's row after another.
fn merge_histogram_rows(
    schema: &Arc<SeriesSchema>,
    op: AggregateOp,
    rows: &[StateRow],
) -> FloatMerger {
    let mut merger = FloatMerger::try_new(schema, op).unwrap();
    for row in rows {
        merger.merge_row(row, |_| Ok(())).unwrap();
    }
    merger
}

/// The answer [`HistogramAccumulator`]'s own state gives for `payloads`, in the order the merge folds
/// them: the same state without the row decoder in between.
fn accumulated_answer(op: AggregateOp, payloads: &[&HistogramState]) -> Option<FloatHistogram> {
    let mut accumulator = HistogramAccumulator::from_payload(op, payloads[0]).unwrap();
    for payload in &payloads[1..] {
        accumulator.merge_state(op, payload).unwrap();
    }
    finished_histogram(op, &accumulator).unwrap()
}

/// The evidence [`HistogramAccumulator`]'s own state reports for `payloads`, read through the same
/// summary a merge's groups are read through.
fn accumulated_evidence(op: AggregateOp, payloads: &[&HistogramState]) -> HistogramEvidence {
    let mut accumulator = HistogramAccumulator::from_payload(op, payloads[0]).unwrap();
    for payload in &payloads[1..] {
        accumulator.merge_state(op, payload).unwrap();
    }
    evidence_summary(&[Group {
        metric: SeriesLabels::try_new(Some("m".into()), Vec::new()).unwrap(),
        blocks: BTreeMap::new(),
        histograms: BTreeMap::from([((0, 0), accumulator)]),
    }])
}

/// Compares two answers bit for bit: every scalar, every custom bound, and every bucket's index and
/// count.
///
/// A decode that drops a position, or that reads a component out of the wrong column, changes a count
/// or a bucket; a merge that reassociates an addition differently changes a `to_bits()`. Nothing
/// short of the same bits passes.
fn assert_histogram_bits(actual: &HistogramView<'_>, expected: &HistogramView<'_>) {
    let bits = |values: &[f64]| {
        values
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    };
    assert_eq!(actual.schema, expected.schema, "schema");
    assert_eq!(
        actual.zero_threshold.to_bits(),
        expected.zero_threshold.to_bits(),
        "zero threshold"
    );
    assert_eq!(
        actual.sum.to_bits(),
        expected.sum.to_bits(),
        "sum {} != {}",
        actual.sum,
        expected.sum
    );
    assert_eq!(
        actual.count.to_bits(),
        expected.count.to_bits(),
        "count {} != {}",
        actual.count,
        expected.count
    );
    assert_eq!(
        actual.zero_count.to_bits(),
        expected.zero_count.to_bits(),
        "zero count"
    );
    assert_eq!(actual.reset_hint, expected.reset_hint, "reset hint");
    assert_eq!(
        actual.start_timestamp, expected.start_timestamp,
        "start timestamp"
    );
    assert_eq!(
        bits(actual.custom_values),
        bits(expected.custom_values),
        "custom bounds"
    );
    for (side, actual, expected) in [
        ("positive", actual.positive, expected.positive),
        ("negative", actual.negative, expected.negative),
    ] {
        let actual = side_buckets(&actual);
        let expected = side_buckets(&expected);
        let indices =
            |buckets: &[(i32, f64)]| buckets.iter().map(|&(index, _)| index).collect::<Vec<_>>();
        assert_eq!(
            indices(&actual),
            indices(&expected),
            "{side} bucket indices"
        );
        for ((index, actual), (_, expected)) in actual.iter().zip(&expected) {
            assert_eq!(
                actual.to_bits(),
                expected.to_bits(),
                "{side} bucket {index}: {actual} != {expected}"
            );
        }
    }
}

/// The payload one partition of a one-group, one-field fold exported at its first point.
fn only_payload(partial: &PartialAggregate) -> &HistogramState {
    partial.groups[0].states[0][0]
        .as_ref()
        .and_then(StatePayload::histogram)
        .expect("the fold exported a histogram state")
}

/// One answer encoded as the row a series holds it in, so a merged answer and a serial fold's answer
/// compare through the same view.
fn answered_row(histogram: &FloatHistogram) -> Arc<HistogramBatch> {
    let mut builder = HistogramBatchBuilder::new();
    histogram.push_to(&mut builder).unwrap();
    builder.finish().unwrap()
}

/// Compares two answers' scalar fields and buckets within a relative tolerance.
///
/// A merge reassociates the additions, so it differs from the serial fold in the last bits; the
/// fixtures are chosen so nothing but those bits differs.
fn assert_histogram_close(actual: &HistogramView<'_>, expected: &HistogramView<'_>) {
    let close =
        |actual: f64, expected: f64| (actual - expected).abs() <= 1e-12 * expected.abs().max(1.0);
    assert_eq!(actual.schema, expected.schema);
    assert!(
        close(actual.sum, expected.sum),
        "sum {} != {}",
        actual.sum,
        expected.sum
    );
    assert!(
        close(actual.count, expected.count),
        "count {} != {}",
        actual.count,
        expected.count
    );
    assert!(
        close(actual.zero_count, expected.zero_count),
        "zero count {} != {}",
        actual.zero_count,
        expected.zero_count
    );
    let actual_buckets = side_buckets(&actual.positive);
    let expected_buckets = side_buckets(&expected.positive);
    assert_eq!(actual_buckets.len(), expected_buckets.len());
    for ((actual_index, actual_count), (expected_index, expected_count)) in
        actual_buckets.iter().zip(&expected_buckets)
    {
        assert_eq!(actual_index, expected_index);
        assert!(
            close(*actual_count, *expected_count),
            "bucket {actual_index}: {actual_count} != {expected_count}"
        );
    }
}

/// Two partitions' sums merge into the answer one fold over both partitions' samples gives.
#[test]
fn merging_two_partials_answers_the_sum_the_one_fold_answers() {
    let schema = schema_with_grid_len(&["value"], 1);
    let sample =
        |sum: f64| histogram_sample(0, &[(0, 1.0)], &[], 1.0, sum, CounterResetHint::Unknown);
    let first = [
        histogram_series(&schema, "a", vec![(0, sample(1.0))]),
        histogram_series(&schema, "b", vec![(0, sample(2.0))]),
    ];
    let second = [
        histogram_series(&schema, "c", vec![(0, sample(3.0))]),
        histogram_series(&schema, "d", vec![(0, sample(4.0))]),
    ];
    let both: Vec<Series> = first.iter().chain(&second).cloned().collect();
    let left = partial_state(AggregateOp::Sum, &schema, &first);
    let right = partial_state(AggregateOp::Sum, &schema, &second);
    let merged =
        merged_histogram_answer(AggregateOp::Sum, &[left, right]).expect("a merged answer");
    let complete = aggregate_answer(&schema, &both, AggregateOp::Sum);
    let row = answered_row(&merged);
    assert_histogram_close(
        &row.view(0).expect("one answer row"),
        &answered_histogram(&complete, 0, 0),
    );
    assert_eq!(merged.sum, 10.0);
    assert_eq!(merged.count, 4.0);
}

/// One partition that switched to a mean and one that is still summing merge into the answer one
/// fold over both partitions' samples gives.
#[test]
fn merging_a_mean_partial_with_a_sum_partial_answers_the_one_fold() {
    let schema = schema_with_grid_len(&["value"], 1);
    let large = || histogram_sample(0, &[(0, 1.0)], &[], 1.0, 1e308, CounterResetHint::Unknown);
    let nothing = || histogram_sample(0, &[], &[], 0.0, 0.0, CounterResetHint::Unknown);
    // The first partition's second sample overflows its sum, so it exports a mean; the second
    // partition never switches.
    let first = [
        histogram_series(&schema, "a", vec![(0, large())]),
        histogram_series(&schema, "b", vec![(0, large())]),
    ];
    let second = [
        histogram_series(&schema, "c", vec![(0, nothing())]),
        histogram_series(&schema, "d", vec![(0, nothing())]),
    ];
    let both: Vec<Series> = first.iter().chain(&second).cloned().collect();
    let left = partial_state(AggregateOp::Avg, &schema, &first);
    let right = partial_state(AggregateOp::Avg, &schema, &second);
    let left_payload = left.groups[0].states[0][0]
        .as_ref()
        .and_then(StatePayload::histogram)
        .expect("a histogram state");
    assert!(
        left_payload.incremental_mean,
        "the first partition switched"
    );
    let right_payload = right.groups[0].states[0][0]
        .as_ref()
        .and_then(StatePayload::histogram)
        .expect("a histogram state");
    assert!(
        !right_payload.incremental_mean,
        "the second partition did not"
    );

    let merged =
        merged_histogram_answer(AggregateOp::Avg, &[left, right]).expect("a merged answer");
    let complete = aggregate_answer(&schema, &both, AggregateOp::Avg);
    let row = answered_row(&merged);
    assert_histogram_close(
        &row.view(0).expect("one answer row"),
        &answered_histogram(&complete, 0, 0),
    );
    assert!(merged.sum.is_finite());
    assert_eq!(
        merged.sum,
        1e308 / 2.0,
        "two of four samples observed 1e308"
    );
}

/// An average whose two partitions each hold one `1e308` still averages to a finite `1e308`: the
/// merged sums would overflow, so the merge goes through its mean path.
#[test]
fn merging_two_overflowing_partials_answers_a_finite_mean() {
    let schema = schema_with_grid_len(&["value"], 1);
    let large = || histogram_sample(0, &[(0, 1.0)], &[], 1.0, 1e308, CounterResetHint::Unknown);
    let left = partial_state(
        AggregateOp::Avg,
        &schema,
        &[histogram_series(&schema, "a", vec![(0, large())])],
    );
    let right = partial_state(
        AggregateOp::Avg,
        &schema,
        &[histogram_series(&schema, "b", vec![(0, large())])],
    );
    let value = merged_histogram_answer(AggregateOp::Avg, &[left, right]).expect("a merged answer");
    assert!(value.sum.is_finite(), "a mean of finite sums is finite");
    assert_eq!(value.sum, 1e308, "(1e308 + 1e308) / 2");
    assert_eq!(value.count, 1.0);
}

/// A mean-path merge weights each side's mean by that side's own count, so partitions of unequal
/// size answer the weighted mean and not an average of two means.
///
/// Both partitions fold only the sample values `2^1023` and `2^1021`, which keeps every step exact:
/// the left one overflows on its second sample and switches to a mean of `2^1023` over 2 samples,
/// while the right one keeps summing six samples of `2^1021` into `3 * 2^1022`, a mean of `2^1021`
/// over 6. Eight samples in all, so the weights are exactly `2/8` and `6/8`, and the answer is
/// `2^1023 * 2/8 + 2^1021 * 6/8 = 7 * 2^1019`. Swapping the two weights answers
/// `2^1023 * 6/8 + 2^1021 * 2/8 = 13 * 2^1019`, which is why the assertion is on the literal.
#[test]
fn merging_unequal_mean_partials_weights_each_mean_by_its_count() {
    let schema = schema_with_grid_len(&["value"], 1);
    let large = || {
        histogram_sample(
            0,
            &[(0, 1.0)],
            &[],
            1.0,
            2f64.powi(1023),
            CounterResetHint::Unknown,
        )
    };
    let small = || {
        histogram_sample(
            0,
            &[(0, 1.0)],
            &[],
            1.0,
            2f64.powi(1021),
            CounterResetHint::Unknown,
        )
    };
    // `2^1023 + 2^1023` is `Inf`, so the left partition's second sample is what forces its switch.
    let left_input: Vec<Series> = (0..2)
        .map(|index| histogram_series(&schema, &format!("a{index}"), vec![(0, large())]))
        .collect();
    let right_input: Vec<Series> = (0..6)
        .map(|index| histogram_series(&schema, &format!("b{index}"), vec![(0, small())]))
        .collect();
    let left = partial_state(AggregateOp::Avg, &schema, &left_input);
    let right = partial_state(AggregateOp::Avg, &schema, &right_input);
    let left_payload = left.groups[0].states[0][0]
        .as_ref()
        .and_then(StatePayload::histogram)
        .expect("a histogram state");
    assert!(left_payload.incremental_mean, "the left partition switched");
    assert_eq!(left_payload.count, 2.0);
    let right_payload = right.groups[0].states[0][0]
        .as_ref()
        .and_then(StatePayload::histogram)
        .expect("a histogram state");
    assert!(
        !right_payload.incremental_mean,
        "the right partition kept its sum"
    );
    assert_eq!(right_payload.count, 6.0);
    // One side switching is enough to put the merge on its mean path.
    let value = merged_histogram_answer(AggregateOp::Avg, &[left, right]).expect("a merged answer");
    assert_eq!(
        value.sum,
        7.0 * 2f64.powi(1019),
        "2^1023 * 2/8 + 2^1021 * 6/8, not an average of the two means"
    );
    // The observation counts follow the same weights, and both sides average one observation per
    // sample, so the answer's count stays 1 whatever the weights are.
    assert_eq!(value.count, 1.0);
    assert_eq!(value.positive, vec![(0, 1.0)]);
}

/// A poisoned partition poisons the point whatever the other partition holds, and a poisoned point
/// answers nothing.
#[test]
fn a_poisoned_partial_poisons_the_merged_point() {
    let schema = schema_with_grid_len(&["value"], 1);
    let exponential = || histogram_sample(0, &[(0, 1.0)], &[], 1.0, 2.0, CounterResetHint::Unknown);
    let custom = || {
        histogram_sample(
            CUSTOM_BUCKETS,
            &[(0, 1.0)],
            &[1.0],
            1.0,
            3.0,
            CounterResetHint::Unknown,
        )
    };
    let poisoned = partial_state(
        AggregateOp::Sum,
        &schema,
        &[
            histogram_series(&schema, "a", vec![(0, exponential())]),
            histogram_series(&schema, "b", vec![(0, custom())]),
        ],
    );
    let legal = partial_state(
        AggregateOp::Sum,
        &schema,
        &[histogram_series(&schema, "c", vec![(0, exponential())])],
    );
    for order in [
        [poisoned.clone(), legal.clone()],
        [legal.clone(), poisoned.clone()],
    ] {
        let merged = merged_histogram_answer(AggregateOp::Sum, &order);
        assert!(
            merged.is_none(),
            "a poisoned point answers nothing, whichever side poisoned it"
        );
        let evidence = merged_histogram_evidence(AggregateOp::Sum, &order);
        assert!(evidence.poisoned);
        assert!(evidence.incompatible_layout);
    }
}

/// Which partition arrived first does not change what the merge reports: a reconciliation between
/// two partitions is reported whichever way the pair is merged.
#[test]
fn the_merged_evidence_does_not_depend_on_the_merge_order() {
    let schema = schema_with_grid_len(&["value"], 1);
    // Each partition folds one custom-bucket sample, so neither reconciles anything by itself: the
    // flag can only come from the merge.
    let left = partial_state(
        AggregateOp::Sum,
        &schema,
        &[histogram_series(
            &schema,
            "a",
            vec![(
                0,
                histogram_sample(
                    CUSTOM_BUCKETS,
                    &[(0, 1.0), (1, 2.0)],
                    &[1.0, 2.0],
                    3.0,
                    4.0,
                    CounterResetHint::Unknown,
                ),
            )],
        )],
    );
    let right = partial_state(
        AggregateOp::Sum,
        &schema,
        &[histogram_series(
            &schema,
            "b",
            vec![(
                0,
                histogram_sample(
                    CUSTOM_BUCKETS,
                    &[(0, 1.0)],
                    &[1.0, 3.0],
                    1.0,
                    2.0,
                    CounterResetHint::Unknown,
                ),
            )],
        )],
    );
    let forward = merged_histogram_evidence(AggregateOp::Sum, &[left.clone(), right.clone()]);
    let backward = merged_histogram_evidence(AggregateOp::Sum, &[right, left]);
    assert!(forward.custom_bounds_reconciled, "{forward:?}");
    assert_eq!(forward, backward, "the evidence is order independent");
}

/// The two partitions of one group can each hold one of the reset hints: merged into that group's
/// point, both bits are set, and the point -- one point's samples, whoever folded them -- does
/// contradict itself. The same two hints in two groups stay apart, exactly as they do in one fold.
#[test]
fn a_merge_collides_the_reset_hints_of_one_group_but_not_of_two() {
    let schema = schema_with_grid_len(&["value"], 1);
    let reset = || {
        histogram_sample(
            0,
            &[(0, 1.0)],
            &[],
            1.0,
            1.0,
            CounterResetHint::CounterReset,
        )
    };
    let not_reset = || {
        histogram_sample(
            0,
            &[(0, 1.0)],
            &[],
            1.0,
            1.0,
            CounterResetHint::NotCounterReset,
        )
    };
    // The partials are grouped by `job`, so the merge pairs the points of a job across partitions
    // and keeps two jobs apart -- the shape a `sum by (job)` hands to a `Final` node.
    let partial = |job: &str, sample: GridSample| {
        let series = histogram_series(&schema, job, vec![(0, sample)]);
        let mut aggregator =
            FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&["job"])).unwrap();
        aggregator.update(&series).unwrap();
        aggregator.finish_partial().unwrap()
    };
    // One group on each side of the merge: no point saw both hints.
    let left = partial("a", reset());
    let right = partial("b", not_reset());
    for one in [&left, &right] {
        assert!(
            !merged_histogram_evidence(AggregateOp::Sum, std::slice::from_ref(one))
                .counter_reset_collision
        );
    }
    let apart = merged_histogram_evidence(AggregateOp::Sum, &[left, right]);
    assert!(apart.counter_reset_seen && apart.not_counter_reset_seen);
    assert!(
        !apart.counter_reset_collision,
        "two groups do not contradict each other: {apart:?}"
    );

    // The same group on both sides: the point holds both hints once they are merged.
    let left = partial("a", reset());
    let right = partial("a", not_reset());
    let together = merged_histogram_evidence(AggregateOp::Sum, &[left, right]);
    assert!(
        together.counter_reset_collision,
        "one point saw both hints: {together:?}"
    );
}

/// Two partitions can each be valid and still disagree on the bucket family: neither partial is
/// poisoned, so the merge is where the incompatibility first shows. It removes the point and
/// reports it, exactly as the fold does when both layouts meet inside one partition -- it does not
/// let the add refuse the layouts and fail the statement.
#[test]
fn a_merge_of_two_valid_partials_with_different_layouts_removes_the_point() {
    let schema = schema_with_grid_len(&["value"], 1);
    for op in [AggregateOp::Sum, AggregateOp::Avg] {
        let exponential = partial_state(
            op,
            &schema,
            &[histogram_series(
                &schema,
                "a",
                vec![(
                    0,
                    histogram_sample(0, &[(0, 1.0)], &[], 1.0, 2.0, CounterResetHint::Unknown),
                )],
            )],
        );
        let custom = partial_state(
            op,
            &schema,
            &[histogram_series(
                &schema,
                "a",
                vec![(
                    0,
                    histogram_sample(
                        CUSTOM_BUCKETS,
                        &[(0, 1.0)],
                        &[1.0, 3.0],
                        1.0,
                        2.0,
                        CounterResetHint::Unknown,
                    ),
                )],
            )],
        );
        // Each partial on its own is a valid answer.
        assert!(!merged_histogram_evidence(op, std::slice::from_ref(&exponential)).poisoned);
        assert!(!merged_histogram_evidence(op, std::slice::from_ref(&custom)).poisoned);
        for order in [
            [exponential.clone(), custom.clone()],
            [custom.clone(), exponential.clone()],
        ] {
            let evidence = merged_histogram_evidence(op, &order);
            assert!(
                evidence.poisoned,
                "{op:?} kept a point whose two partitions disagree on the bucket family: {evidence:?}"
            );
            assert!(evidence.incompatible_layout, "{op:?}: {evidence:?}");
        }
    }
}

/// A sum state declares no mean mode and no count, so its export cannot claim otherwise: the kind's
/// own component list is what a state frame accepts, and it has neither component.
#[test]
fn a_sum_partial_export_cannot_claim_a_mean_mode() {
    let schema = schema_with_grid_len(&["value"], 1);
    let input = [histogram_series(
        &schema,
        "a",
        vec![(
            0,
            histogram_sample(0, &[(0, 1.0)], &[], 1.0, 2.0, CounterResetHint::Unknown),
        )],
    )];
    let partial = partial_state(AggregateOp::Sum, &schema, &input);
    let payload = partial.groups[0].states[0][0]
        .as_ref()
        .and_then(StatePayload::histogram)
        .expect("a histogram state");
    assert!(!payload.incremental_mean);
    assert_eq!(payload.count, 0.0);
    // And the layout cannot be declared with the component it would need.
    let components = crate::native::plan::aggregate_state_binding(
        crate::native::plan::AggregateKind::Sum,
        crate::extension_plan::FieldKind::Histogram,
    )
    .expect("a histogram sum binds state")
    .1;
    assert!(
        !components.contains(&datatypes::series::aggregate_state::StateComponent::IncrementalMean)
    );
    let mut claimed = components.clone();
    claimed.push(datatypes::series::aggregate_state::StateComponent::IncrementalMean);
    assert!(
        datatypes::series::aggregate_state::AggregateStateSchema::try_new(
            datatypes::series::aggregate_state::AggregateStateKind::HistogramSum,
            claimed,
        )
        .is_err(),
        "a sum state cannot declare a mean mode"
    );
}

/// One row per point of a run: a span covers a whole run of adjacent points, so the run's own
/// extent says nothing about which row a point is written to.
///
/// Three adjacent points coalesce into one span and keep three rows, one per point. A row index
/// taken from the run instead writes all three into the run's first row and leaves the other two
/// empty, which is what this fixture then exports: `3` at the run's first point and nothing after
/// it, so a merge of that state answers one point per run rather than one point per sample.
#[test]
fn three_adjacent_points_export_one_row_each_and_merge_to_three_answers() {
    let schema = schema_with_grid_len(&["value"], 3);
    // One sample per point, so what a row carries is that point's own value.
    let samples = (0..3)
        .map(|position| {
            let sum = position as f64 + 1.0;
            (
                position,
                histogram_sample(0, &[(0, 1.0)], &[], 1.0, sum, CounterResetHint::Unknown),
            )
        })
        .collect();
    let series = histogram_series(&schema, "a", samples);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    aggregator.update(&series).unwrap();
    let partial = aggregator.finish_partial().unwrap();
    let exported = &partial.groups[0];
    // The three adjacent points share one span and still open three rows.
    assert_eq!(
        exported.time.len(),
        3,
        "the run covers three timestamps, one per point"
    );
    let payload_sum = |payload: &StatePayload| {
        payload
            .histogram_component(StateComponent::HistogramValue)
            .expect("a histogram payload carries its value")
            .view(0)
            .expect("one histogram per component")
            .sum
    };
    let exported_sums: Vec<Option<f64>> = exported.states[0]
        .iter()
        .map(|state| state.as_ref().map(payload_sum))
        .collect();
    assert_eq!(
        exported_sums,
        vec![Some(1.0), Some(2.0), Some(3.0)],
        "every point of the run is a row of its own"
    );

    // Merging that export answers those three points, not the run's last one.
    let mut merged: BTreeMap<(usize, usize), HistogramAccumulator> = BTreeMap::new();
    for (point, position) in exported.time.positions() {
        for (field, states) in exported.states.iter().enumerate() {
            let Some(payload) = states[position].as_ref().and_then(StatePayload::histogram) else {
                continue;
            };
            match merged.get_mut(&(point, field)) {
                Some(accumulator) => accumulator.merge_state(AggregateOp::Sum, payload).unwrap(),
                None => {
                    merged.insert(
                        (point, field),
                        HistogramAccumulator::from_payload(AggregateOp::Sum, payload).unwrap(),
                    );
                }
            }
        }
    }
    let merged_sums: Vec<f64> = (0..3)
        .map(|point| {
            let accumulator = merged.get(&(point, 0)).expect("a state at every point");
            finished_histogram(AggregateOp::Sum, accumulator)
                .unwrap()
                .expect("an answer")
                .sum
        })
        .collect();
    assert_eq!(
        merged_sums,
        vec![1.0, 2.0, 3.0],
        "each point of the run answers its own sample"
    );
}

/// The export admits what each histogram payload is about to build before it builds it.
///
/// A group that folded histograms keeps its states in the group's own map rather than the blocks, so
/// the point count a float group reports for it is zero. Charging only that figure admits the rows
/// and the slots but nothing for the two one-row Arrow batches each payload is built as, and those
/// exist before the chunk that will be charged for them does.
#[test]
fn export_admits_each_histogram_payload_before_it_builds_it() {
    let points = 3usize;
    let schema = schema_with_grid_len(&["value"], points as u64);
    let samples = (0..points)
        .map(|position| {
            let buckets: Vec<(i32, f64)> = [(0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)].to_vec();
            let sum = position as f64 + 1.0;
            (
                position,
                histogram_sample(0, &buckets, &[], 4.0, sum, CounterResetHint::Unknown),
            )
        })
        .collect();
    let series = histogram_series(&schema, "a", samples);
    let mut aggregator =
        FloatAggregator::try_new(&schema, AggregateOp::Sum, Grouping::By(&[])).unwrap();
    aggregator.update(&series).unwrap();
    let state_bytes = aggregator.state_bytes();
    // The figure the payloads alone need, taken from the fold before it is exported: this is what
    // the export has to cover while the batches it builds are the only copy that exists.
    let payloads: usize = aggregator
        .groups
        .iter()
        .flat_map(|group| group.histograms.values())
        .map(crate::aggregate::state::exported_payload_bytes)
        .sum();
    let mut peak = 0usize;
    let partial = aggregator
        .finish_partial_budgeted(|bytes| {
            peak = peak.max(bytes);
            Ok(())
        })
        .unwrap();
    let rows = partial.groups[0].states[0].len();
    assert_eq!(rows, points, "one row per point");
    let slots = std::mem::size_of::<TimeAxis>()
        .saturating_add(rows.saturating_mul(
            std::mem::size_of::<GridSpan>().saturating_add(std::mem::size_of::<usize>()),
        ))
        .saturating_add(std::mem::size_of::<Vec<Option<StatePayload>>>())
        .saturating_add(rows.saturating_mul(std::mem::size_of::<Option<StatePayload>>()));
    // One group, its rows, and the two batches under each of their payloads.
    let expected = state_bytes
        .saturating_add(std::mem::size_of::<StateSeries>())
        .saturating_add(slots)
        .saturating_add(payloads);
    assert_eq!(
        peak, expected,
        "the export admits the fold, the rows it opens and the payloads it builds"
    );
    // Two one-row batches per point, each of them charged for its own layout: a payload counted once
    // would leave half of what the export builds unadmitted.
    assert!(
        payloads
            >= points
                .saturating_mul(2)
                .saturating_mul(crate::aggregate::state::HISTOGRAM_LAYOUT_ROW_BYTES),
        "the export admitted {payloads} for {points} points, less than both batches of each"
    );
}

/// A merge folds the right partial's compensation into the merged sum.
///
/// Every other histogram merge fixture carries a zero compensation on both sides, so the second add
/// of the merge -- the one that folds the other side's compensation into what the first add returned
/// -- is invisible to them. Left folds `-1e16` and right folds `1e16` then `1`: the right partition's
/// sum rounds to `1e16` and keeps the `1` it lost as its compensation, so a merge that adds only the
/// two values answers `0` and answering `1` needs that second add.
#[test]
fn merging_a_compensation_partial_answers_the_one_fold() {
    let schema = schema_with_grid_len(&["value"], 1);
    let sample =
        |sum: f64| histogram_sample(0, &[(0, 1.0)], &[], 1.0, sum, CounterResetHint::Unknown);
    let left = partial_state(
        AggregateOp::Sum,
        &schema,
        &[histogram_series(&schema, "a", vec![(0, sample(-1e16))])],
    );
    let right = partial_state(
        AggregateOp::Sum,
        &schema,
        &[
            histogram_series(&schema, "b", vec![(0, sample(1e16))]),
            histogram_series(&schema, "c", vec![(0, sample(1.0))]),
        ],
    );
    let payload_of = |partial: &PartialAggregate| {
        partial.groups[0].states[0][0]
            .as_ref()
            .expect("a state at the only point")
            .clone()
    };
    let compensation_sum = |payload: &StatePayload| {
        payload
            .histogram_component(StateComponent::HistogramCompensation)
            .expect("a histogram payload carries its compensation")
            .view(0)
            .expect("one histogram per component")
            .sum
    };
    let left = payload_of(&left);
    let right = payload_of(&right);
    assert_eq!(compensation_sum(&left), 0.0, "the left fold is exact");
    assert_eq!(
        compensation_sum(&right),
        1.0,
        "the right fold kept the `1` that `1e16 + 1` rounds away"
    );

    let left = left.histogram().expect("a histogram payload");
    let right = right.histogram().expect("a histogram payload");
    let mut merged = HistogramAccumulator::from_payload(AggregateOp::Sum, left).unwrap();
    merged.merge_state(AggregateOp::Sum, right).unwrap();
    let answer = finished_histogram(AggregateOp::Sum, &merged)
        .unwrap()
        .expect("an answer");
    assert_eq!(
        answer.sum, 1.0,
        "the merged sum folds the right partial's compensation in"
    );
}

/// Two partitions' histogram rows merge into the answer `HistogramAccumulator` gives for the same
/// two states, bit for bit.
///
/// This is the row path a distributed `sum` takes: `merge_row` decodes each row's components back
/// into the payload an accumulator folds, so a component it reads out of the wrong column -- or a
/// position it drops -- answers something other than the state's own merge does.
#[test]
fn merging_histogram_rows_answers_the_sum_the_accumulator_answers() {
    let schema = schema_with_grid_len(&["value"], 1);
    let sample =
        |sum: f64| histogram_sample(0, &[(0, 1.0)], &[], 1.0, sum, CounterResetHint::Unknown);
    let first = [
        histogram_series(&schema, "a", vec![(0, sample(1.0))]),
        histogram_series(&schema, "b", vec![(0, sample(2.0))]),
    ];
    let second = [
        histogram_series(&schema, "c", vec![(0, sample(3.0))]),
        histogram_series(&schema, "d", vec![(0, sample(4.0))]),
    ];
    let left = partial_state(AggregateOp::Sum, &schema, &first);
    let right = partial_state(AggregateOp::Sum, &schema, &second);
    let (left_payload, right_payload) = (only_payload(&left), only_payload(&right));

    let rows = [
        histogram_state_row(
            AggregateStateKind::HistogramSum,
            &schema,
            metric("m", &[]),
            &[Some(left_payload)],
        ),
        histogram_state_row(
            AggregateStateKind::HistogramSum,
            &schema,
            metric("m", &[]),
            &[Some(right_payload)],
        ),
    ];
    let merger = merge_histogram_rows(&schema, AggregateOp::Sum, &rows);
    let evidence = merger.evidence_summary();
    let merged = merger.finish().unwrap();
    assert_eq!(merged.len(), 1, "one group");
    let answer = answered_histogram(&merged[0], 0, 0);
    // The samples `1 + 2` in one partition and `3 + 4` in the other: four observations, each
    // partition having folded two of them into one bucket.
    assert_eq!(answer.sum, 10.0);
    assert_eq!(answer.count, 4.0);
    assert_eq!(side_buckets(&answer.positive), vec![(0, 4.0)]);

    // The same two states merged without the row decoder in between.
    let direct = accumulated_answer(AggregateOp::Sum, &[left_payload, right_payload])
        .expect("the states answer a sum");
    let expected = answered_row(&direct);
    assert_histogram_bits(&answer, &expected.view(0).expect("one answer row"));
    assert_eq!(
        evidence,
        accumulated_evidence(AggregateOp::Sum, &[left_payload, right_payload])
    );
}

/// A row path average weights each partition by its own count, exactly as the accumulator's own merge
/// does: a mean-mode partition and a summing one meet in the mean path, and every bit of the answer
/// matches the state merge's.
///
/// Both partitions fold only `2^1023` and `2^1021`, which keeps every step exact. The left one
/// overflows into a mean of `2^1023` over 2 samples, the right one keeps summing six samples of
/// `2^1021`, so the answer is `2^1023 * 2/8 + 2^1021 * 6/8 = 7 * 2^1019` -- an average of the two
/// means, `13 * 2^1019`, or the merged sums, `+Inf`, are all different values.
#[test]
fn merging_histogram_rows_weights_the_avg_by_each_partitions_count() {
    let schema = schema_with_grid_len(&["value"], 1);
    let large = || {
        histogram_sample(
            0,
            &[(0, 1.0)],
            &[],
            1.0,
            2f64.powi(1023),
            CounterResetHint::Unknown,
        )
    };
    let small = || {
        histogram_sample(
            0,
            &[(0, 1.0)],
            &[],
            1.0,
            2f64.powi(1021),
            CounterResetHint::Unknown,
        )
    };
    let left_input: Vec<Series> = (0..2)
        .map(|index| histogram_series(&schema, &format!("a{index}"), vec![(0, large())]))
        .collect();
    let right_input: Vec<Series> = (0..6)
        .map(|index| histogram_series(&schema, &format!("b{index}"), vec![(0, small())]))
        .collect();
    let left = partial_state(AggregateOp::Avg, &schema, &left_input);
    let right = partial_state(AggregateOp::Avg, &schema, &right_input);
    let (left_payload, right_payload) = (only_payload(&left), only_payload(&right));
    // The row carries both components the mean path reads: the count and the mode flag.
    assert_eq!(left_payload.count, 2.0);
    assert!(left_payload.incremental_mean, "the left partition switched");
    assert_eq!(right_payload.count, 6.0);
    assert!(!right_payload.incremental_mean);

    let rows = [
        histogram_state_row(
            AggregateStateKind::HistogramAvg,
            &schema,
            metric("m", &[]),
            &[Some(left_payload)],
        ),
        histogram_state_row(
            AggregateStateKind::HistogramAvg,
            &schema,
            metric("m", &[]),
            &[Some(right_payload)],
        ),
    ];
    let merger = merge_histogram_rows(&schema, AggregateOp::Avg, &rows);
    let evidence = merger.evidence_summary();
    let merged = merger.finish().unwrap();
    assert_eq!(merged.len(), 1, "one group");
    let answer = answered_histogram(&merged[0], 0, 0);
    assert_eq!(answer.sum, 7.0 * 2f64.powi(1019));
    assert_eq!(answer.count, 1.0);
    assert_eq!(side_buckets(&answer.positive), vec![(0, 1.0)]);

    let direct = accumulated_answer(AggregateOp::Avg, &[left_payload, right_payload])
        .expect("the states answer an average");
    let expected = answered_row(&direct);
    assert_histogram_bits(&answer, &expected.view(0).expect("one answer row"));
    assert_eq!(
        evidence,
        accumulated_evidence(AggregateOp::Avg, &[left_payload, right_payload])
    );
}

/// A poisoned partition poisons the merged point through the row path too: the row's poison bit is
/// read back and merged, so the point answers nothing whichever side carrying it arrived first, and
/// the merge reports the same evidence the state merge reports.
#[test]
fn a_poisoned_histogram_row_poisons_the_merged_point() {
    let schema = schema_with_grid_len(&["value"], 1);
    let exponential = || histogram_sample(0, &[(0, 1.0)], &[], 1.0, 2.0, CounterResetHint::Unknown);
    let custom = || {
        histogram_sample(
            CUSTOM_BUCKETS,
            &[(0, 1.0)],
            &[1.0],
            1.0,
            3.0,
            CounterResetHint::Unknown,
        )
    };
    for (kind, op) in [
        (AggregateStateKind::HistogramSum, AggregateOp::Sum),
        (AggregateStateKind::HistogramAvg, AggregateOp::Avg),
    ] {
        // One partition folds both layouts, which removes its point and marks it poisoned; the other
        // partition is an ordinary answer.
        let poisoned = partial_state(
            op,
            &schema,
            &[
                histogram_series(&schema, "a", vec![(0, exponential())]),
                histogram_series(&schema, "b", vec![(0, custom())]),
            ],
        );
        let legal = partial_state(
            op,
            &schema,
            &[histogram_series(&schema, "c", vec![(0, exponential())])],
        );
        assert!(only_payload(&poisoned).poisoned);
        assert!(!only_payload(&legal).poisoned);
        for order in [[&poisoned, &legal], [&legal, &poisoned]] {
            let rows = order.map(|partial| {
                histogram_state_row(
                    kind,
                    &schema,
                    metric("m", &[]),
                    &[Some(only_payload(partial))],
                )
            });
            let merger = merge_histogram_rows(&schema, op, &rows);
            let evidence = merger.evidence_summary();
            let merged = merger.finish().unwrap();
            assert!(
                merged.is_empty(),
                "{op:?} answered {} series for a poisoned point",
                merged.len()
            );
            assert!(evidence.poisoned, "{op:?}: {evidence:?}");
            assert!(evidence.incompatible_layout, "{op:?}: {evidence:?}");
            let payloads = [only_payload(&poisoned), only_payload(&legal)];
            assert_eq!(evidence, accumulated_evidence(op, &payloads), "{op:?}");
            assert!(
                accumulated_answer(op, &payloads).is_none(),
                "{op:?}: the states answer nothing either"
            );
        }
    }
}

/// Each field of a row decodes into its own slot: the state one field carries at a position never
/// reaches its sibling, a sibling that is a hole at that position decodes to no payload rather than
/// to a zero-observation histogram, and the merge of a second partition adds to the field it names.
///
/// Field 0 is carried by both partitions and field 1 by the second one only, so the hole in the first
/// partition's field 1 must leave field 1 to the second partition's own state -- a zero there would
/// both invent an observation and a bucket.
#[test]
fn merging_histogram_rows_keeps_each_field_in_its_own_state() {
    let value_schema = schema_with_grid_len(&["value"], 1);
    let schema = schema_with_grid_len(&["a", "b"], 1);
    let fold = |samples: Vec<GridSample>| {
        let input: Vec<Series> = samples
            .into_iter()
            .enumerate()
            .map(|(index, sample)| {
                histogram_series(&value_schema, &format!("s{index}"), vec![(0, sample)])
            })
            .collect();
        partial_state(AggregateOp::Sum, &value_schema, &input)
    };
    // Two samples of `1` and `2` fold into a sum of `3`: the first partition's field 0.
    let left = fold(vec![
        histogram_sample(0, &[(0, 1.0)], &[], 1.0, 1.0, CounterResetHint::Unknown),
        histogram_sample(0, &[(0, 1.0)], &[], 1.0, 2.0, CounterResetHint::Unknown),
    ]);
    // The second partition's field 0 adds `3` more, and its field 1 is a state of its own.
    let right = fold(vec![histogram_sample(
        0,
        &[(0, 1.0)],
        &[],
        1.0,
        3.0,
        CounterResetHint::Unknown,
    )]);
    let other = fold(vec![histogram_sample(
        0,
        &[(0, 1.0), (1, 2.0)],
        &[],
        3.0,
        9.0,
        CounterResetHint::Unknown,
    )]);
    let (left, right, other) = (
        only_payload(&left),
        only_payload(&right),
        only_payload(&other),
    );
    let rows = [
        histogram_state_row(
            AggregateStateKind::HistogramSum,
            &schema,
            metric("m", &[]),
            &[Some(left), None],
        ),
        histogram_state_row(
            AggregateStateKind::HistogramSum,
            &schema,
            metric("m", &[]),
            &[Some(right), Some(other)],
        ),
    ];
    let merger = merge_histogram_rows(&schema, AggregateOp::Sum, &rows);
    let merged = merger.finish().unwrap();
    assert_eq!(merged.len(), 1, "one group, one series");

    let first = answered_histogram(&merged[0], 0, 0);
    assert_eq!(first.sum, 6.0, "1 + 2 + 3");
    assert_eq!(first.count, 3.0);
    assert_eq!(side_buckets(&first.positive), vec![(0, 3.0)]);
    let expected = answered_row(&accumulated_answer(AggregateOp::Sum, &[left, right]).unwrap());
    assert_histogram_bits(&first, &expected.view(0).expect("one answer row"));

    // The hole in the first partition did not become a zero: field 1 is the second partition's own
    // state, in its sum, its count and its buckets.
    let second = answered_histogram(&merged[0], 1, 0);
    assert_eq!(second.sum, 9.0);
    assert_eq!(second.count, 3.0);
    assert_eq!(side_buckets(&second.positive), vec![(0, 1.0), (1, 2.0)]);
    let expected = answered_row(&accumulated_answer(AggregateOp::Sum, &[other]).unwrap());
    assert_histogram_bits(&second, &expected.view(0).expect("one answer row"));
}

/// A row whose item claims a state its columns cannot fill is refused rather than read as a zero:
/// a present position with a null histogram component, and an average's null count, both fail the
/// decode, and a position whose every field is a hole names a contribution that is not there.
#[test]
fn a_histogram_row_with_a_missing_component_is_refused() {
    let schema = schema_with_grid_len(&["value"], 1);
    let sample = || histogram_sample(0, &[(0, 1.0)], &[], 1.0, 2.0, CounterResetHint::Unknown);
    let input = || [histogram_series(&schema, "a", vec![(0, sample())])];

    let sum_partial = partial_state(AggregateOp::Sum, &schema, &input());
    let sum_row = histogram_state_row(
        AggregateStateKind::HistogramSum,
        &schema,
        metric("m", &[]),
        &[Some(only_payload(&sum_partial))],
    );
    let mut merger = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
    let error = merger
        .merge_row(
            &row_without_component(&sum_row, StateComponent::HistogramCompensation),
            |_| Ok(()),
        )
        .expect_err("a present state with a null compensation");
    assert!(
        error
            .to_string()
            .contains("present aggregate state has a missing component"),
        "{error}"
    );

    let avg_partial = partial_state(AggregateOp::Avg, &schema, &input());
    let avg_row = histogram_state_row(
        AggregateStateKind::HistogramAvg,
        &schema,
        metric("m", &[]),
        &[Some(only_payload(&avg_partial))],
    );
    let mut merger = FloatMerger::try_new(&schema, AggregateOp::Avg).unwrap();
    let error = merger
        .merge_row(
            &row_without_component(&avg_row, StateComponent::Count),
            |_| Ok(()),
        )
        .expect_err("a present average with a null count");
    assert!(
        error
            .to_string()
            .contains("present aggregate state has a missing component"),
        "{error}"
    );

    let empty = histogram_state_row(
        AggregateStateKind::HistogramSum,
        &schema,
        metric("m", &[]),
        &[None],
    );
    let mut merger = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
    let error = merger
        .merge_row(&empty, |_| Ok(()))
        .expect_err("a position no field carries");
    assert!(
        error
            .to_string()
            .contains("aggregate state names a position with no contribution"),
        "{error}"
    );
}

/// The row decoder reads the compensation as the payload's own second component; it does not re-check
/// the layout `HistogramState::validate_layout` pins to the live value at the producer, so a row that
/// disagrees never shifts the value's counts: the bucket family clash is refused where the two
/// histograms are added, and an extra empty bucket is added, and then dropped, in its own place.
///
/// The fixture's value carries one bucket `(0, 1.0)` and a sum of `2`, and the compensation's extra
/// bucket sits at an index the value does not use: a decode that indexed the compensation's counts by
/// position rather than by bucket index would shift the value's count into that bucket, and one that
/// let the family clash through would answer a histogram where the kernel refuses the add.
#[test]
fn a_histogram_row_whose_compensation_carries_another_layout_never_shifts_the_value() {
    let schema = schema_with_grid_len(&["value"], 1);
    let partial = partial_state(
        AggregateOp::Sum,
        &schema,
        &[histogram_series(
            &schema,
            "a",
            vec![(
                0,
                histogram_sample(0, &[(0, 1.0)], &[], 1.0, 2.0, CounterResetHint::Unknown),
            )],
        )],
    );
    let payload = only_payload(&partial);
    let empty = BucketSideView {
        span_offsets: &[],
        span_lengths: &[],
        buckets: &[],
    };
    let compensation_batch = |schema: i32, custom: &[f64], indices: &[i32], counts: &[f64]| {
        let (offsets, lengths) = spans_of(indices);
        let mut builder = HistogramBatchBuilder::new();
        builder
            .push(HistogramView {
                schema,
                zero_threshold: 0.0,
                sum: 0.0,
                count: 0.0,
                zero_count: 0.0,
                reset_hint: CounterResetHint::Unknown,
                start_timestamp: None,
                custom_values: custom,
                positive: BucketSideView {
                    span_offsets: &offsets,
                    span_lengths: &lengths,
                    buckets: counts,
                },
                negative: empty,
            })
            .unwrap();
        builder.finish().unwrap()
    };
    let row_of = |compensation: Arc<HistogramBatch>| {
        let state = HistogramState {
            compensation,
            ..payload.clone()
        };
        histogram_state_row(
            AggregateStateKind::HistogramSum,
            &schema,
            metric("m", &[]),
            &[Some(&state)],
        )
    };

    // A compensation of the other bucket family decodes, so the row is accepted; the refusal is the
    // kernel's, where the two layouts would be indexed together.
    let mut merger = FloatMerger::try_new(&schema, AggregateOp::Sum).unwrap();
    merger
        .merge_row(
            &row_of(compensation_batch(CUSTOM_BUCKETS, &[1.0], &[0], &[0.0])),
            |_| Ok(()),
        )
        .expect("the decoder does not validate the layout");
    let error = merger
        .finish()
        .expect_err("exponential and custom buckets cannot be added");
    assert!(
        error.to_string().contains(
            "cannot apply this operation to histograms with exponential and custom buckets"
        ),
        "{error}"
    );

    // A compensation that only carries one more, empty, bucket: the counts add by bucket index, so
    // the value's bucket keeps its count and the empty one is dropped from the answer.
    let row = row_of(compensation_batch(0, &[], &[0, 1], &[0.0, 0.0]));
    let merger = merge_histogram_rows(&schema, AggregateOp::Sum, &[row]);
    let merged = merger.finish().unwrap();
    let answer = answered_histogram(&merged[0], 0, 0);
    assert_eq!(answer.sum, 2.0);
    assert_eq!(answer.count, 1.0);
    assert_eq!(
        side_buckets(&answer.positive),
        vec![(0, 1.0)],
        "the empty bucket is dropped where it is, not merged into its neighbour"
    );
    // The same state with its own compensation: the extra empty bucket changed no bit.
    let direct = accumulated_answer(AggregateOp::Sum, &[payload]).expect("the state answers a sum");
    let expected = answered_row(&direct);
    assert_histogram_bits(&answer, &expected.view(0).expect("one answer row"));
}
