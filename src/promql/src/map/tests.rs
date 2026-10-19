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

mod arithmetic;
mod broadcast;
mod comparison;
mod math_functions;
mod prometheus;

use std::sync::Arc;

use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use datafusion::functions::math;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datatypes::arrow::array::{AsArray, UInt32Array};
use datatypes::arrow::buffer::NullBuffer;
use datatypes::arrow::datatypes::{DataType, Field, Float64Type};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::samples::{FieldSpec, HistogramSamples, SampleRef};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec};
use datatypes::series::{Label, MetricName};

use super::*;
use crate::functions::IeeeSqrt;

fn schema(names: &[&str]) -> SeriesSchema {
    SeriesSchema {
        fields: names
            .iter()
            .map(|name| FieldSpec {
                name: (*name).into(),
            })
            .collect(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(1000, 500, 64).unwrap()).unwrap(),
        )),
    }
}

fn series(schema: &SeriesSchema, start: usize, fields: Vec<Samples>) -> Series {
    let time = TimeAxis::Grid(
        GridRange::try_new(schema.eval_grid.clone().unwrap(), start, fields[0].len()).unwrap(),
    );
    let metric = MetricName::try_new(
        Some("requests".into()),
        vec![
            Label {
                name: "host".into(),
                value: "a".into(),
            },
            Label {
                name: "empty".into(),
                value: "".into(),
            },
        ],
    )
    .unwrap();
    Series::try_new(
        schema,
        metric,
        time,
        SeriesFields::try_from_vec(fields).unwrap(),
    )
    .unwrap()
}

fn floats(samples: &[Option<f64>]) -> Samples {
    Samples::from_float(Float64Array::from(samples.to_vec()))
}

fn assert_floats(actual: &Samples, expected: &[Option<f64>]) {
    assert_eq!(actual.len(), expected.len());
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        match (actual, expected) {
            (SampleRef::Missing, None) => {}
            (SampleRef::Float(actual), Some(expected)) if expected.is_nan() => {
                assert!(actual.is_nan(), "sample {index}: {actual} is not NaN");
            }
            (SampleRef::Float(actual), Some(expected)) => {
                assert_eq!(actual.to_bits(), expected.to_bits(), "sample {index}");
            }
            _ => panic!("unexpected sample kind at {index}"),
        }
    }
}

#[test]
fn math_preserves_missing_and_uses_ieee_float_semantics() {
    // Boundaries: negative values, signed zero, infinities, ordinary NaN and Missing.
    let schema = schema(&["value"]);
    let input = series(
        &schema,
        3,
        vec![floats(&[
            Some(-2.5),
            Some(-0.5),
            Some(-0.0),
            Some(0.0),
            Some(4.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            None,
        ])],
    );
    let cases = [
        (
            MathOp::Abs,
            [
                2.5,
                0.5,
                0.0,
                0.0,
                4.0,
                f64::INFINITY,
                f64::INFINITY,
                f64::NAN,
            ],
        ),
        (
            MathOp::Ceil,
            [
                -2.0,
                -0.0,
                -0.0,
                0.0,
                4.0,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NAN,
            ],
        ),
        (
            MathOp::Floor,
            [
                -3.0,
                -1.0,
                -0.0,
                0.0,
                4.0,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NAN,
            ],
        ),
        (
            MathOp::Sqrt,
            [
                f64::NAN,
                f64::NAN,
                -0.0,
                0.0,
                2.0,
                f64::INFINITY,
                f64::NAN,
                f64::NAN,
            ],
        ),
        (
            MathOp::Round { nearest: 1.0 },
            [
                -2.0,
                0.0,
                0.0,
                0.0,
                4.0,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NAN,
            ],
        ),
    ];
    for (op, expected) in cases {
        let output = map_math(&schema, &input, op).unwrap().unwrap();
        let expected = expected
            .into_iter()
            .map(Some)
            .chain([None])
            .collect::<Vec<_>>();
        assert_floats(output.fields.get(0).unwrap(), &expected);
    }
}

#[test]
fn short_multifield_slices_keep_absolute_grid_and_labels() {
    let schema = schema(&["left", "right"]);
    let first = floats(&[Some(99.0), Some(-1.2), None, Some(-3.2), Some(99.0)]);
    let second = floats(&[Some(99.0), Some(4.8), Some(-0.0), None, Some(99.0)]);
    let fields = vec![
        Samples::try_from_chunks(vec![first.slice(1, 2).unwrap(), first.slice(3, 1).unwrap()])
            .unwrap(),
        Samples::try_from_chunks(vec![
            second.slice(1, 1).unwrap(),
            second.slice(2, 2).unwrap(),
        ])
        .unwrap(),
    ];
    let input = series(&schema, 7, fields);
    let output = map_math(&schema, &input, MathOp::Floor).unwrap().unwrap();
    let TimeAxis::Grid(range) = &output.time else {
        panic!("lost grid axis")
    };
    assert_eq!((range.start(), range.len()), (7, 3));
    assert!(Arc::ptr_eq(
        range.grid(),
        schema.eval_grid.as_ref().unwrap()
    ));
    assert_eq!(output.time.timestamp(0), Some(4500));
    assert_eq!(output.time.timestamp(2), Some(5500));
    // Prometheus drops the name; main keeps it because its HTTP adapter follows Call arguments.
    assert_eq!(output.metric.name(), None);
    assert_eq!(output.metric.tags(), &input.metric.tags()[1..]);
    assert_eq!(input.metric.name(), Some("requests"));
    assert_floats(
        input.fields.get(0).unwrap(),
        &[Some(-1.2), None, Some(-3.2)],
    );
    for (field, lengths) in output.fields.iter().zip([[2, 1], [1, 2]]) {
        assert_eq!(
            field
                .segments()
                .iter()
                .map(Samples::len)
                .collect::<Vec<_>>(),
            lengths
        );
    }
    drop(input);
    drop(schema);
    drop(first);
    drop(second);
    assert_floats(
        output.fields.get(0).unwrap(),
        &[Some(-2.0), None, Some(-4.0)],
    );
    assert_floats(
        output.fields.get(1).unwrap(),
        &[Some(4.0), Some(-0.0), None],
    );
}

#[test]
fn empty_and_all_missing_ranges_are_not_expanded() {
    let schema = schema(&["value"]);
    // Empty at grid end, one missing point, and a longer all-missing range.
    for (start, len) in [(64, 0), (9, 1), (2, 4)] {
        let input = series(&schema, start, vec![floats(&vec![None; len])]);
        for op in [
            MathOp::Abs,
            MathOp::Ceil,
            MathOp::Floor,
            MathOp::Sqrt,
            MathOp::Round { nearest: 0.0 },
        ] {
            let output = map_math(&schema, &input, op).unwrap().unwrap();
            let TimeAxis::Grid(range) = &output.time else {
                panic!("lost grid axis")
            };
            assert_eq!((range.start(), range.len()), (start, len));
            assert_floats(output.fields.get(0).unwrap(), &vec![None; len]);
        }
    }
}

#[test]
fn evaluated_missingness_depends_on_validity_not_nan_payloads() {
    let schema = schema(&["value"]);
    let stale = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
    let ordinary_nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let input = series(
        &schema,
        1,
        vec![Samples::from_float(Float64Array::new(
            vec![stale, ordinary_nan, -1.0].into(),
            Some(NullBuffer::from(vec![false, true, true])),
        ))],
    );
    let output = map_math(&schema, &input, MathOp::Abs).unwrap().unwrap();
    assert_floats(
        output.fields.get(0).unwrap(),
        &[None, Some(ordinary_nan), Some(1.0)],
    );
    let SampleRef::Float(actual_nan) = output.fields.get(0).unwrap().sample(1) else {
        panic!("lost NaN")
    };
    assert_eq!(actual_nan.to_bits(), ordinary_nan.to_bits());

    // Removing the sign of this ordinary NaN produces the reserved raw stale bits. A later Map
    // must not reinterpret that computed value as a missing sample or reject the evaluated input.
    let negative_nan = f64::from_bits(PROMETHEUS_STALE_NAN_BITS | (1_u64 << 63));
    let input = series(&schema, 1, vec![floats(&[Some(negative_nan)])]);
    let absolute = map_math(&schema, &input, MathOp::Abs).unwrap().unwrap();
    let SampleRef::Float(computed_nan) = absolute.fields.get(0).unwrap().sample(0) else {
        panic!("lost computed NaN")
    };
    assert_eq!(computed_nan.to_bits(), PROMETHEUS_STALE_NAN_BITS);
    let rounded = map_math(&schema, &absolute, MathOp::Ceil).unwrap().unwrap();
    assert_floats(rounded.fields.get(0).unwrap(), &[Some(f64::NAN)]);
}

fn histogram_batch() -> Arc<HistogramBatch> {
    let empty_side = BucketSideView {
        span_offsets: &[],
        span_lengths: &[],
        buckets: &[],
    };
    let mut builder = HistogramBatchBuilder::new();
    builder
        .push(HistogramView {
            schema: 1,
            zero_threshold: 0.01,
            sum: 0.0,
            count: 1.0,
            zero_count: 1.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: &[],
            positive: empty_side,
            negative: empty_side,
        })
        .unwrap();
    builder.finish().unwrap()
}

#[test]
fn histogram_and_mixed_fields_are_rejected_without_partial_success() {
    let batch = histogram_batch();
    let histogram = Samples::try_from_histogram(UInt32Array::from(vec![0]), batch.clone()).unwrap();
    let mixed = Samples::try_from_mixed(
        Float64Array::from(vec![Some(1.0), None]),
        HistogramSamples::try_new(UInt32Array::from(vec![None, Some(0)]), batch.clone()).unwrap(),
    )
    .unwrap();
    let null_histogram = Samples::try_from_histogram(UInt32Array::from(vec![None]), batch).unwrap();
    let chunks = Samples::try_from_chunks(vec![floats(&[Some(-1.0)]), histogram.clone()]).unwrap();
    let schema = schema(&["float", "unsupported"]);
    for unsupported in [histogram, mixed, null_histogram, chunks] {
        let prefix = floats(&vec![Some(-2.0); unsupported.len()]);
        let input = series(&schema, 2, vec![prefix, unsupported]);
        for op in [MathOp::Abs, MathOp::Clamp { min: 2.0, max: 1.0 }] {
            assert!(matches!(
                map_math(&schema, &input, op),
                Err(DataFusionError::NotImplemented(_))
            ));
        }
        assert_floats(
            input.fields.get(0).unwrap(),
            &vec![Some(-2.0); input.time.len()],
        );
    }
}

#[test]
fn raw_time_axes_are_not_treated_as_evaluated_grids() {
    let mut schema = schema(&["value"]);
    let input = series(&schema, 2, vec![floats(&[Some(-1.0), Some(2.0)])]);
    schema.eval_grid = None;
    for time in [
        TimeAxis::try_new(vec![2000, 2500].into()).unwrap(),
        TimeAxis::try_from_chunks(vec![vec![2000].into(), vec![2500].into()]).unwrap(),
    ] {
        let raw =
            Series::try_new(&schema, input.metric.clone(), time, input.fields.clone()).unwrap();
        assert!(
            matches!(map_math(&schema, &raw, MathOp::Abs), Err(DataFusionError::Execution(reason)) if reason.contains("grid"))
        );
    }
}

#[test]
fn mismatched_schema_and_mutated_field_lengths_return_typed_errors() {
    let schema = schema(&["value"]);
    let mut input = series(&schema, 2, vec![floats(&[Some(-1.0), Some(2.0)])]);
    let mut other_grid = schema.clone();
    other_grid.eval_grid = Some(Arc::new(
        EvalGrid::try_new(GridSpec::try_new(0, 500, 64).unwrap()).unwrap(),
    ));
    assert!(
        matches!(map_math(&other_grid, &input, MathOp::Abs), Err(DataFusionError::External(error)) if error.is::<datatypes::error::Error>())
    );
    input.fields = SeriesFields::one(floats(&[Some(-1.0)]));
    assert!(
        matches!(map_math(&schema, &input, MathOp::Abs), Err(DataFusionError::External(error)) if error.is::<datatypes::error::Error>())
    );
}

fn ordinary_math(op: MathOp, input: Float64Array) -> Float64Array {
    // Calibration against main's UDFs, whose values agree with Prometheus for these functions;
    // the Prometheus engine's own results are checked in `prometheus`.
    let number_rows = input.len();
    let args = vec![ColumnarValue::Array(Arc::new(input))];
    let udf = match op {
        MathOp::Abs => math::abs(),
        MathOp::Ceil => math::ceil(),
        MathOp::Floor => math::floor(),
        MathOp::Sqrt => Arc::new(IeeeSqrt::scalar_udf()),
        _ => unreachable!("main agrees with Prometheus only for abs, ceil, floor and sqrt"),
    };
    let arg_fields = args
        .iter()
        .map(|arg| Arc::new(Field::new("value", arg.data_type(), true)))
        .collect();
    let output = udf
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("output", DataType::Float64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap();
    let ColumnarValue::Array(output) = output else {
        panic!("expected array")
    };
    output.as_primitive::<Float64Type>().clone()
}

fn float_boundary_samples() -> Float64Array {
    let mut samples = vec![
        Some(-2.5),
        Some((-0.5_f64).next_down()),
        Some(-0.5),
        Some((-0.5_f64).next_up()),
        Some(0.5_f64.next_down()),
        Some(0.5),
        Some(0.5_f64.next_up()),
        Some(2.5),
        Some(123.456),
    ];
    // Both signs, every exponent, and mantissa boundaries cover zeros, subnormals, normal
    // magnitudes, infinities, and signaling/quiet NaNs without a random seed or new dependency.
    for exponent in 0..=0x7ff_u64 {
        for mantissa in [0, 1, 1 << 51, (1 << 52) - 1] {
            for sign in [0, 1_u64 << 63] {
                samples.push(Some(f64::from_bits(sign | (exponent << 52) | mantissa)));
            }
        }
        samples.push(None);
    }
    Float64Array::from(samples)
}

#[test]
fn math_matches_main_udfs_across_float_boundaries() {
    let input = float_boundary_samples();
    let mut schema = schema(&["dense", "chunked"]);
    let grid_len = u64::try_from(input.len() + 6).unwrap();
    schema.eval_grid = Some(Arc::new(
        EvalGrid::try_new(GridSpec::try_new(1000, 500, grid_len).unwrap()).unwrap(),
    ));
    let chunks = (0..input.len())
        .step_by(61)
        .map(|offset| Samples::from_float(input.slice(offset, 61.min(input.len() - offset))))
        .collect();
    let series = series(
        &schema,
        5,
        vec![
            Samples::from_float(input.clone()),
            Samples::try_from_chunks(chunks).unwrap(),
        ],
    );
    // Round shares its calculation with the UDF; the Prometheus fixtures check it instead.
    let cases = [MathOp::Abs, MathOp::Ceil, MathOp::Floor, MathOp::Sqrt];
    for op in cases {
        let output = map_math(&schema, &series, op).unwrap().unwrap();
        let expected = ordinary_math(op, input.clone()).iter().collect::<Vec<_>>();
        for field in output.fields.iter() {
            assert_floats(field, &expected);
        }
        assert_eq!(output.metric.name(), None);
        assert_eq!(output.metric.tags(), &series.metric.tags()[1..]);
        let TimeAxis::Grid(range) = output.time else {
            panic!("lost grid axis")
        };
        assert_eq!((range.start(), range.len()), (5, input.len()));
    }
}

#[test]
fn math_drops_reserved_metadata_and_empty_labels_with_the_name() {
    let schema = schema(&["value"]);
    let mut input = series(&schema, 0, vec![floats(&[Some(-1.0)])]);
    for label in ["__type__", "__unit__", "__other__"] {
        input.metric.set(label, "x".into()).unwrap();
    }
    let output = map_math(&schema, &input, MathOp::Abs).unwrap().unwrap();
    let names = output
        .metric
        .tags()
        .iter()
        .map(|label| label.name.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(output.metric.name(), None);
    assert_eq!(names, ["__other__", "host"]);
}
