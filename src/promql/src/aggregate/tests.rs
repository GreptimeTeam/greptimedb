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

use datatypes::arrow::array::{Float64Array, UInt32Array};
use datatypes::series::Label;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};

use super::*;
use crate::golden::{assert_result, cases};
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

fn metric(name: &str, labels: &[(&str, &str)]) -> MetricName {
    MetricName::try_new(
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
    metric: MetricName,
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
    let grid_len = multi.eval_grid.as_ref().unwrap().timestamps().len();
    let q_values: Vec<Option<f64>> = (0..grid_len)
        .map(|i| Some([0.0, 0.25, 0.5, 0.9, 1.0][i % 5]))
        .collect();
    let q = input(
        &schema(&["scalar"]),
        MetricName::try_new(None, Vec::new()).unwrap(),
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
    let job = MetricName::try_new(None, metric("m", &[("job", "x")]).tags().to_vec()).unwrap();
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
        MetricName::try_new(Some("a".into()), vec![]).unwrap()
    );
    let unnamed = [
        input(&schema, metric("", &[]), 0, &[vec![Some(1.0)]]),
        input(
            &schema,
            MetricName::try_new(None, vec![]).unwrap(),
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
        [(MetricName::try_new(None, vec![]).unwrap(), vec![Some(3.0)])]
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
        MetricName::try_new(
            None,
            metric("a", &[("__type__", "gauge"), ("__unit__", "seconds")])
                .tags()
                .to_vec()
        )
        .unwrap()
    );
    assert_eq!(
        labels(Grouping::By(&["__unit__"])),
        MetricName::try_new(
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
    // Prometheus rejects an empty `count_values` label name at evaluation.
    assert!(matches!(
        count_values(
            &schema,
            std::slice::from_ref(&source),
            "",
            Grouping::By(&[])
        ),
        Err(DataFusionError::Execution(_))
    ));
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
    for op in OPS {
        assert!(matches!(
            aggregate_float(
                &schema,
                &[source.clone(), histogram.clone()],
                op,
                Grouping::By(&[])
            ),
            Err(DataFusionError::NotImplemented(_))
        ));
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

/// `count_values` writes value labels the way Go's `strconv.FormatFloat(v, 'f', -1, 64)` does,
/// including the round-half-even choice of the last shortest digit; see the header of
/// `format_float_cases.txt` (promql-compat `cmd/narthex-golden-formatfloat`).
#[test]
fn value_labels_format_like_go() {
    let mut checked = 0;
    for line in include_str!("format_float_cases.txt").lines() {
        let (bits, expected) = line.split_once(' ').unwrap();
        let value = f64::from_bits(u64::from_str_radix(bits, 16).unwrap());
        assert_eq!(format_value(value), expected, "{bits}");
        checked += 1;
    }
    assert!(checked > 1_800, "{checked}");
    for (value, expected) in [
        (f64::NAN, "NaN"),
        (f64::INFINITY, "+Inf"),
        (f64::NEG_INFINITY, "-Inf"),
    ] {
        assert_eq!(format_value(value), expected);
    }
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
        MetricName::try_new(None, Vec::new()).unwrap(),
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
        MetricName::try_new(None, Vec::new()).unwrap(),
        0,
        &[vec![Some(0.5); 128]],
    );
    let output = aggregate_quantile(&schema, &series, &q, Grouping::By(&[])).unwrap();
    let expected: Vec<Option<f64>> = (0..128).map(|point| Some(point as f64)).collect();
    assert_eq!(values_of(&output, 0, 128), (0, expected));
}
