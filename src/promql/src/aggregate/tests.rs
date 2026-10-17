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

use datatypes::arrow::array::{Float64Array, UInt32Array};
use datatypes::series::Label;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec};

use super::*;
use crate::golden::{assert_result, cases};

const CASES: &str = include_str!("prometheus_cases.txt");

const OPS: [AggregateOp; 6] = [
    AggregateOp::Sum,
    AggregateOp::Count,
    AggregateOp::Avg,
    AggregateOp::Min,
    AggregateOp::Max,
    AggregateOp::Group,
];

fn schema(fields: &[&str]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields
            .iter()
            .map(|name| FieldSpec {
                name: (*name).into(),
            })
            .collect(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(-1000, 250, 128).unwrap()).unwrap(),
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

fn values(series: &Series, field: usize) -> (usize, Vec<Option<f64>>) {
    let TimeAxis::Grid(range) = &series.time else {
        panic!("lost grid")
    };
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
    (range.start(), values)
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
    for grouping in [
        Grouping::By(&[]),
        Grouping::By(&["host"]),
        Grouping::Without(&["i"]),
    ] {
        for op in OPS {
            let output = aggregate_float(&multi, &series, op, grouping).unwrap();
            for field in [0, 1] {
                let expected = aggregate_float(&single, &project(field), op, grouping).unwrap();
                let expected = expected
                    .iter()
                    .map(|s| (s.metric.clone(), values(s, 0)))
                    .collect::<Vec<_>>();
                for actual in &output {
                    let (start, actual_values) = values(actual, field);
                    let present = actual_values
                        .iter()
                        .enumerate()
                        .filter_map(|(i, v)| v.map(|v| (start + i, v.to_bits())))
                        .collect::<Vec<_>>();
                    let reference = expected
                        .iter()
                        .find(|(metric, _)| metric == &actual.metric)
                        .map_or_else(Vec::new, |(_, (start, v))| {
                            v.iter()
                                .enumerate()
                                .filter_map(|(i, v)| v.map(|v| (start + i, v.to_bits())))
                                .collect()
                        });
                    assert_eq!(present, reference, "{op:?} {grouping:?} field {field}");
                }
                assert!(
                    expected
                        .iter()
                        .all(|(m, _)| output.iter().any(|s| &s.metric == m))
                );
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
