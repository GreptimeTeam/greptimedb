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

use common_function::aggrs::extremum::Extremum;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::functions_aggregate::{average::avg, count::count, min_max::max, sum::sum};
use datafusion_expr::{Expr, ExprSchemable, col, lit};
use datatypes::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, StringArray, UInt32Array, UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Float64Type, Schema, UInt64Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::series::Label;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::FieldSpec;
use datatypes::series::time::GridSpec;

use super::*;

const OPS: [AggregateOp; 6] = [
    AggregateOp::Sum,
    AggregateOp::Count,
    AggregateOp::Avg,
    AggregateOp::Min,
    AggregateOp::Max,
    AggregateOp::Group,
];
type Rows = BTreeMap<(Vec<(String, String)>, usize), Vec<Option<f64>>>;

fn schema() -> SeriesSchema {
    SeriesSchema {
        fields: vec![
            FieldSpec { name: "a".into() },
            FieldSpec { name: "b".into() },
        ]
        .into(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(-1000, 250, 128).unwrap()).unwrap(),
        )),
    }
}

fn input(
    schema: &SeriesSchema,
    host: Option<&str>,
    instance: &str,
    start: usize,
    fields: [Vec<Option<f64>>; 2],
) -> Series {
    let mut tags = vec![Label {
        name: "instance".into(),
        value: instance.into(),
    }];
    if let Some(host) = host {
        tags.push(Label {
            name: "host".into(),
            value: host.into(),
        });
    }
    let len = fields[0].len();
    let fields = fields
        .into_iter()
        .enumerate()
        .map(|(index, samples)| {
            // Different boundaries and nonzero Arrow offsets per field.
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
        MetricName::try_new(Some("requests".into()), tags).unwrap(),
        TimeAxis::Grid(GridRange::try_new(schema.eval_grid.clone().unwrap(), start, len).unwrap()),
        SeriesFields::try_from_vec(fields).unwrap(),
    )
    .unwrap()
}

fn nullable_float(samples: &Samples, index: usize) -> Option<f64> {
    match samples.sample(index) {
        SampleRef::Float(v) => Some(v),
        SampleRef::Missing => None,
        _ => panic!("unexpected histogram"),
    }
}

fn labels(metric: &MetricName) -> Vec<(String, String)> {
    let mut tags = metric
        .tags()
        .iter()
        .map(|tag| (tag.name.to_string(), tag.value.to_string()))
        .collect::<Vec<_>>();
    if let Some(name) = metric.name() {
        tags.push(("__name__".into(), name.into()));
    }
    tags.sort();
    tags
}

fn actual_rows(output: &[Series]) -> Rows {
    let mut rows = BTreeMap::new();
    for series in output {
        let TimeAxis::Grid(range) = &series.time else {
            panic!("lost grid")
        };
        assert!(!range.is_empty());
        for endpoint in [0, range.len() - 1] {
            assert!(
                series
                    .fields
                    .iter()
                    .any(|field| nullable_float(field, endpoint).is_some())
            );
        }
        for offset in 0..range.len() {
            let fields = series
                .fields
                .iter()
                .map(|field| nullable_float(field, offset))
                .collect::<Vec<_>>();
            if fields.iter().any(Option::is_some) {
                assert!(
                    rows.insert((labels(&series.metric), range.start() + offset), fields)
                        .is_none()
                );
            }
        }
    }
    rows
}

fn reference_batch(schema: &SeriesSchema, input: &[Series]) -> RecordBatch {
    let mut points = Vec::new();
    let mut hosts = Vec::new();
    let mut instances = Vec::new();
    let mut values = [Vec::new(), Vec::new()];
    // Brute-force absolute grid expansion is deliberately confined to the oracle.
    for point in 0..schema.eval_grid.as_ref().unwrap().spec().len as usize {
        for series in input {
            let TimeAxis::Grid(range) = &series.time else {
                panic!("raw input")
            };
            let Some(offset) = point.checked_sub(range.start()) else {
                continue;
            };
            let row = series
                .fields
                .iter()
                .map(|field| nullable_float(field, offset))
                .collect::<Vec<_>>();
            if row.iter().all(Option::is_none) {
                continue;
            }
            points.push(point as u64);
            hosts.push(series.metric.get("host"));
            instances.push(series.metric.get("instance"));
            for (values, sample) in values.iter_mut().zip(row) {
                values.push(sample);
            }
        }
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(points)),
        Arc::new(StringArray::from(hosts)),
        Arc::new(StringArray::from(instances)),
        Arc::new(Float64Array::from(values[0].clone())),
        Arc::new(Float64Array::from(values[1].clone())),
    ];
    let fields = vec![
        Field::new("point", DataType::UInt64, false),
        Field::new("host", DataType::Utf8, true),
        Field::new("instance", DataType::Utf8, true),
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
    ];
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

async fn reference_rows(batch: RecordBatch, op: AggregateOp, grouping: Grouping<'_>) -> Rows {
    let keys: Vec<&str> = match grouping {
        Grouping::All => vec![],
        Grouping::By(names) => ["host", "instance"]
            .into_iter()
            .filter(|n| names.contains(n))
            .collect(),
        Grouping::Without(names) => ["host", "instance"]
            .into_iter()
            .filter(|n| !names.contains(n))
            .collect(),
    };
    let name = match grouping {
        Grouping::By(names) => names.contains(&"__name__"),
        Grouping::Without(names) => !names.contains(&"__name__"),
        Grouping::All => true,
    };
    let aggregates = ["a", "b"]
        .into_iter()
        .map(|field| {
            let expr: Expr = match op {
                AggregateOp::Sum => sum(col(field)),
                AggregateOp::Avg => avg(col(field)),
                AggregateOp::Count => count(col(field))
                    .cast_to(
                        &DataType::Float64,
                        &datafusion_common::DFSchema::try_from(batch.schema()).unwrap(),
                    )
                    .unwrap(),
                AggregateOp::Min => Extremum::min_udaf().call(vec![col(field)]),
                AggregateOp::Max => Extremum::max_udaf().call(vec![col(field)]),
                AggregateOp::Group => max(lit(1.0_f64)),
            };
            expr.alias(field)
        })
        .collect::<Vec<_>>();
    let mut groups = vec![col("point")];
    groups.extend(keys.iter().map(|key| col(*key)));
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let batches = context
        .read_batch(batch)
        .unwrap()
        .aggregate(groups, aggregates)
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = BTreeMap::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut labels = vec![];
            if name {
                labels.push(("__name__".to_string(), "requests".to_string()));
            }
            for key in &keys {
                let column = batch.column_by_name(key).unwrap().as_string::<i32>();
                if column.is_valid(row) {
                    labels.push((key.to_string(), column.value(row).to_string()));
                }
            }
            labels.sort();
            let point = batch
                .column_by_name("point")
                .unwrap()
                .as_primitive::<UInt64Type>()
                .value(row) as usize;
            let values = ["a", "b"]
                .iter()
                .map(|field| {
                    let column = batch
                        .column_by_name(field)
                        .unwrap()
                        .as_primitive::<Float64Type>();
                    column.is_valid(row).then(|| column.value(row))
                })
                .collect();
            assert!(rows.insert((labels, point), values).is_none());
        }
    }
    rows
}

fn assert_rows(actual: Rows, expected: Rows) {
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>()
    );
    for (key, expected) in expected {
        for (actual, expected) in actual[&key].iter().zip(expected) {
            match (actual, expected) {
                (None, None) => {}
                (Some(a), Some(b)) if b.is_nan() => assert!(a.is_nan()),
                (Some(a), Some(b)) => assert_eq!(a.to_bits(), b.to_bits(), "{key:?}"),
                _ => panic!("validity differs at {key:?}: {actual:?} vs {expected:?}"),
            }
        }
    }
}

#[tokio::test]
async fn grouped_aggregates_match_main_on_shifted_sparse_multifield_inputs() {
    // Boundaries: staggered starts/ends, holes, disjoint ranges, empty/missing labels,
    // unequal field chunks/slices, null-only fields, NaNs, infinities and signed zero.
    let schema = schema();
    let samples = [
        None,
        Some(-0.0),
        Some(0.0),
        Some(-5.0),
        Some(7.0),
        Some(f64::NAN),
        Some(f64::from_bits(
            common_query::prometheus::PROMETHEUS_STALE_NAN_BITS,
        )),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
    ];
    let mut seed = 17_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 32
    };
    let mut input_series = Vec::new();
    for series in 0..20 {
        let start = next() as usize % 80;
        let len = next() as usize % 20;
        let fields = std::array::from_fn(|_| {
            (0..len)
                .map(|_| samples[next() as usize % samples.len()])
                .collect()
        });
        input_series.push(input(
            &schema,
            [None, Some(""), Some("a"), Some("b")][series % 4],
            &format!("i{}", series % 3),
            start,
            fields,
        ));
    }
    let batch = reference_batch(&schema, &input_series);
    for grouping in [
        Grouping::All,
        Grouping::By(&[]),
        Grouping::By(&["host"]),
        Grouping::By(&["__name__", "host", "unknown"]),
        Grouping::Without(&["instance"]),
        Grouping::Without(&["host", "__name__"]),
    ] {
        for op in OPS {
            let output = aggregate_float(&schema, &input_series, op, grouping).unwrap();
            assert_rows(
                actual_rows(&output),
                reference_rows(batch.clone(), op, grouping).await,
            );
        }
    }
}

#[test]
fn count_zero_and_group_one_differ_from_an_absent_point() {
    let schema = schema();
    let source = input(
        &schema,
        Some("a"),
        "i",
        11,
        [
            vec![None, None, None, None],
            vec![None, Some(2.0), None, Some(4.0)],
        ],
    );
    for (op, expected) in [
        (AggregateOp::Count, vec![Some(0.0), None, Some(0.0)]),
        (AggregateOp::Group, vec![Some(1.0), None, Some(1.0)]),
        (AggregateOp::Sum, vec![None; 3]),
    ] {
        let output =
            aggregate_float(&schema, std::slice::from_ref(&source), op, Grouping::All).unwrap();
        assert_eq!(output.len(), 1);
        assert!(
            matches!(&output[0].time, TimeAxis::Grid(range) if range.start() == 12 && range.len() == 3)
        );
        let actual = output[0]
            .fields
            .get(0)
            .unwrap()
            .iter()
            .map(|v| match v {
                SampleRef::Float(v) => Some(v),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
    let empty = input(&schema, Some("a"), "i", 127, [vec![None], vec![None]]);
    for op in OPS {
        assert!(
            aggregate_float(&schema, &[], op, Grouping::All)
                .unwrap()
                .is_empty()
        );
        assert!(
            aggregate_float(&schema, std::slice::from_ref(&empty), op, Grouping::All)
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn unsupported_shapes_and_mutated_schema_fail_without_partial_output() {
    let schema = schema();
    let source = input(
        &schema,
        Some("a"),
        "i",
        11,
        [vec![Some(1.0)], vec![Some(2.0)]],
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
    let mut other = source.clone();
    other.metric.set("__name__", "other".into()).unwrap();
    assert!(matches!(
        aggregate_float(
            &schema,
            &[source.clone(), other],
            AggregateOp::Sum,
            Grouping::All
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
                Grouping::All
            ),
            Err(DataFusionError::NotImplemented(_))
        ));
    }
    let mut invalid = source.clone();
    invalid.fields = SeriesFields::one(source.fields.get(0).unwrap().clone());
    assert!(aggregate_float(&schema, &[invalid], AggregateOp::Sum, Grouping::All).is_err());
    let mut raw = source;
    raw.time = TimeAxis::try_new(vec![1750].into()).unwrap();
    assert!(aggregate_float(&schema, &[raw], AggregateOp::Sum, Grouping::All).is_err());
    let mut other_grid = schema.clone();
    other_grid.eval_grid = Some(Arc::new(
        EvalGrid::try_new(GridSpec::try_new(0, 250, 128).unwrap()).unwrap(),
    ));
    let source = input(
        &schema,
        Some("a"),
        "i",
        11,
        [vec![Some(1.0)], vec![Some(2.0)]],
    );
    assert!(aggregate_float(&other_grid, &[source], AggregateOp::Sum, Grouping::All).is_err());
    let mut raw_schema = schema;
    raw_schema.eval_grid = None;
    assert!(aggregate_float(&raw_schema, &[], AggregateOp::Sum, Grouping::All).is_err());
}
