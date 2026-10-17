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

use datafusion_expr::Operator;

use super::*;
use crate::functions::IeeeComparison;

const OPERATIONS: [(ComparisonOp, Operator); 6] = [
    (ComparisonOp::Equal, Operator::Eq),
    (ComparisonOp::NotEqual, Operator::NotEq),
    (ComparisonOp::Less, Operator::Lt),
    (ComparisonOp::LessEqual, Operator::LtEq),
    (ComparisonOp::Greater, Operator::Gt),
    (ComparisonOp::GreaterEqual, Operator::GtEq),
];

fn ordinary_compare(
    input: &Float64Array,
    scalar: ScalarOperand,
    op: Operator,
) -> Vec<Option<bool>> {
    let samples = ColumnarValue::Array(Arc::new(input.clone()));
    let literal = |v| ColumnarValue::Scalar(ScalarValue::Float64(Some(v)));
    let args = match scalar {
        ScalarOperand::Left(v) => vec![literal(v), samples],
        ScalarOperand::Right(v) => vec![samples, literal(v)],
    };
    let field = Arc::new(Field::new("value", DataType::Float64, true));
    let output = IeeeComparison::scalar_udf(op)
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![field; 2],
            number_rows: input.len(),
            return_field: Arc::new(Field::new("output", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
        .into_array(input.len())
        .unwrap();
    output.as_boolean().iter().collect()
}

#[test]
fn comparisons_match_main_ieee_udf_for_both_sides_and_modes() {
    // Boundaries: both NaN signs/payloads, signed zero, all exponents, nulls, holes and chunks.
    let input = float_boundary_samples();
    let mut schema = schema(&["value"]);
    schema.eval_grid = Some(Arc::new(
        EvalGrid::try_new(GridSpec::try_new(0, 1000, (input.len() + 9) as u64).unwrap()).unwrap(),
    ));
    let chunks = (0..input.len())
        .step_by(67)
        .map(|offset| Samples::from_float(input.slice(offset, 67.min(input.len() - offset))))
        .collect();
    let source = series(&schema, 9, vec![Samples::try_from_chunks(chunks).unwrap()]);
    for (op, ordinary_op) in OPERATIONS {
        for scalar in [
            -2.0,
            -0.0,
            0.0,
            1.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
        ] {
            for scalar in [ScalarOperand::Left(scalar), ScalarOperand::Right(scalar)] {
                let expected = ordinary_compare(&input, scalar, ordinary_op);
                for mode in [Comparison::Filter(op), Comparison::Bool(op)] {
                    let output = map_compare(&schema, &source, mode, scalar).unwrap();
                    let expected = input
                        .iter()
                        .zip(&expected)
                        .map(|(sample, matched)| match mode {
                            Comparison::Filter(_) => {
                                matched.and_then(|yes| if yes { sample } else { None })
                            }
                            Comparison::Bool(_) => matched.map(f64::from),
                        })
                        .collect::<Vec<_>>();
                    assert_floats(output.fields.get(0).unwrap(), &expected);
                    assert_eq!(output.metric.tags(), source.metric.tags());
                    assert_eq!(
                        output.metric.name(),
                        if matches!(mode, Comparison::Filter(_)) {
                            Some("requests")
                        } else {
                            None
                        }
                    );
                    assert!(
                        matches!((&output.time, &source.time), (TimeAxis::Grid(a), TimeAxis::Grid(b)) if a == b)
                    );
                }
            }
        }
    }
}

#[test]
fn bool_supports_independent_fields_but_filter_rejects_them() {
    let schema = schema(&["a", "b"]);
    let input = series(
        &schema,
        4,
        vec![
            floats(&[Some(-0.0), None, Some(2.0)]),
            Samples::try_from_chunks(vec![
                floats(&[Some(0.0)]),
                floats(&[Some(f64::NAN), Some(-2.0)]),
            ])
            .unwrap(),
        ],
    );
    let output = map_compare(
        &schema,
        &input,
        Comparison::Bool(ComparisonOp::Equal),
        ScalarOperand::Right(0.0),
    )
    .unwrap();
    assert_floats(output.fields.get(0).unwrap(), &[Some(1.0), None, Some(0.0)]);
    assert_floats(
        output.fields.get(1).unwrap(),
        &[Some(1.0), Some(0.0), Some(0.0)],
    );
    assert!(matches!(
        map_compare(
            &schema,
            &input,
            Comparison::Filter(ComparisonOp::Equal),
            ScalarOperand::Right(0.0)
        ),
        Err(DataFusionError::NotImplemented(_))
    ));
}

#[test]
fn filter_keeps_vector_rhs_value_and_missing_positions() {
    let schema = schema(&["value"]);
    for samples in [
        vec![],
        vec![None, None],
        vec![Some(1.0), Some(3.0), None, Some(4.0)],
    ] {
        let input = series(&schema, 5, vec![floats(&samples)]);
        let output = map_compare(
            &schema,
            &input,
            Comparison::Filter(ComparisonOp::Less),
            ScalarOperand::Left(2.0),
        )
        .unwrap();
        let expected = samples
            .iter()
            .map(|v| v.filter(|v| *v > 2.0))
            .collect::<Vec<_>>();
        assert_floats(output.fields.get(0).unwrap(), &expected);
        assert!(
            matches!(&output.time, TimeAxis::Grid(range) if range.start() == 5 && range.len() == samples.len())
        );
    }
}

#[test]
fn scalar_operations_produce_unlabelled_short_ranges_and_checked_power_errors() {
    let schema = schema(&["value"]);
    let range = GridRange::try_new(schema.eval_grid.clone().unwrap(), 8, 3).unwrap();
    for (op, expected) in [
        (ArithmeticOp::Add, 10.0),
        (ArithmeticOp::Subtract, 6.0),
        (ArithmeticOp::Multiply, 16.0),
        (ArithmeticOp::Divide, 4.0),
        (ArithmeticOp::Modulo, 0.0),
        (ArithmeticOp::Power, 64.0),
        (ArithmeticOp::Atan2, 8.0_f64.atan2(2.0)),
    ] {
        let output = scalar_arithmetic(&schema, range.clone(), op, [8.0, 2.0]).unwrap();
        assert_floats(output.fields.get(0).unwrap(), &[Some(expected); 3]);
        assert!(output.metric.name().is_none() && output.metric.tags().is_empty());
        assert!(matches!(&output.time, TimeAxis::Grid(actual) if actual == &range));
    }
    assert!(scalar_arithmetic(&schema, range.clone(), ArithmeticOp::Power, [0.0, -1.0]).is_err());
    for (op, expected) in [
        (ComparisonOp::Equal, 0.0),
        (ComparisonOp::NotEqual, 1.0),
        (ComparisonOp::Less, 0.0),
        (ComparisonOp::LessEqual, 0.0),
        (ComparisonOp::Greater, 0.0),
        (ComparisonOp::GreaterEqual, 0.0),
    ] {
        let output = scalar_compare(&schema, range.clone(), op, [f64::NAN, f64::NAN]).unwrap();
        assert_floats(output.fields.get(0).unwrap(), &[Some(expected); 3]);
        assert!(output.metric.name().is_none() && output.metric.tags().is_empty());
    }
}

#[test]
fn comparisons_reject_raw_axes_and_late_histogram_chunks() {
    let schema = schema(&["value"]);
    let unsupported = Samples::try_from_chunks(vec![
        floats(&[Some(1.0)]),
        Samples::try_from_histogram(UInt32Array::from(vec![0]), histogram_batch()).unwrap(),
    ])
    .unwrap();
    let input = series(&schema, 3, vec![unsupported]);
    for comparison in [
        Comparison::Bool(ComparisonOp::Equal),
        Comparison::Filter(ComparisonOp::Equal),
    ] {
        assert!(matches!(
            map_compare(&schema, &input, comparison, ScalarOperand::Right(1.0)),
            Err(DataFusionError::NotImplemented(_))
        ));
        let mut raw_schema = schema.clone();
        raw_schema.eval_grid = None;
        let raw = Series::try_new(
            &raw_schema,
            input.metric.clone(),
            TimeAxis::try_new(vec![2500, 3000].into()).unwrap(),
            SeriesFields::one(floats(&[Some(1.0), None])),
        )
        .unwrap();
        assert!(matches!(
            map_compare(&raw_schema, &raw, comparison, ScalarOperand::Right(1.0)),
            Err(DataFusionError::Execution(_))
        ));
    }
}
