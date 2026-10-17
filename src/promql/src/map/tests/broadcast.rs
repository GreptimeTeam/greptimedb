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

use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion_common::DFSchema;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Expr, Operator, col};
use datatypes::arrow::datatypes::Schema;
use datatypes::arrow::record_batch::RecordBatch;

use super::*;
use crate::functions::IeeeComparison;

fn scalar(schema: &SeriesSchema, start: usize, samples: Samples) -> Series {
    let scalar_schema = SeriesSchema {
        fields: vec![FieldSpec {
            name: "scalar".into(),
        }]
        .into(),
        eval_grid: schema.eval_grid.clone(),
    };
    Series::try_new(
        &scalar_schema,
        MetricName::try_new(None, vec![]).unwrap(),
        TimeAxis::Grid(
            GridRange::try_new(schema.eval_grid.clone().unwrap(), start, samples.len()).unwrap(),
        ),
        SeriesFields::one(samples),
    )
    .unwrap()
}

fn ordinary_arithmetic(
    op: ArithmeticOp,
    left: Float64Array,
    right: Float64Array,
) -> Result<Float64Array> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Float64, true),
        Field::new("right", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(left), Arc::new(right)])?;
    let (left, right) = (col("left"), col("right"));
    let expr: Expr = match op {
        ArithmeticOp::Add => left + right,
        ArithmeticOp::Subtract => left - right,
        ArithmeticOp::Multiply => left * right,
        ArithmeticOp::Divide => left / right,
        ArithmeticOp::Modulo => left % right,
        ArithmeticOp::Power => math::power().call(vec![left, right]),
        ArithmeticOp::Atan2 => math::atan2().call(vec![left, right]),
    };
    let expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema)?,
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    )?;
    Ok(expr
        .evaluate(&batch)?
        .into_array(batch.num_rows())?
        .as_primitive::<Float64Type>()
        .clone())
}

#[test]
fn dynamic_scalar_arithmetic_aligns_absolute_indices_and_independent_chunks() {
    // Boundaries: two fields, unequal chunk boundaries, sliced offsets, different starts/ends,
    // scalar on either side, null operands, signed zero, infinity, NaN and checked power errors.
    let schema = schema(&["a", "b"]);
    let first = Float64Array::from(vec![
        Some(4.0),
        Some(-0.0),
        None,
        Some(-2.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(8.0),
        Some(16.0),
    ]);
    let second = Float64Array::from(vec![
        None,
        Some(2.0),
        Some(3.0),
        Some(4.0),
        Some(5.0),
        Some(-1.0),
        Some(7.0),
        Some(8.0),
    ]);
    let input = series(
        &schema,
        7,
        vec![
            Samples::try_from_chunks(vec![
                Samples::from_float(first.slice(0, 2)),
                Samples::from_float(first.slice(2, 6)),
            ])
            .unwrap(),
            Samples::try_from_chunks(vec![
                Samples::from_float(second.slice(0, 5)),
                Samples::from_float(second.slice(5, 3)),
            ])
            .unwrap(),
        ],
    );
    let scalar_samples = Float64Array::from(vec![
        Some(99.0),
        Some(98.0),
        Some(2.0),
        Some(-1.0),
        Some(0.0),
        None,
        Some(1.0),
        Some(0.0),
        Some(f64::INFINITY),
        Some(-2.0),
        Some(97.0),
    ]);
    let scalar = scalar(
        &schema,
        5,
        Samples::try_from_chunks(vec![
            Samples::from_float(scalar_samples.slice(0, 3)),
            Samples::from_float(scalar_samples.slice(3, 3)),
            Samples::from_float(scalar_samples.slice(6, 5)),
        ])
        .unwrap(),
    );
    for op in [
        ArithmeticOp::Add,
        ArithmeticOp::Subtract,
        ArithmeticOp::Multiply,
        ArithmeticOp::Divide,
        ArithmeticOp::Modulo,
        ArithmeticOp::Power,
        ArithmeticOp::Atan2,
    ] {
        for side in [
            ScalarSeriesOperand::Left(&scalar),
            ScalarSeriesOperand::Right(&scalar),
        ] {
            let expected = [&first, &second]
                .into_iter()
                .map(|field| {
                    let scalar = scalar_samples.slice(2, 8);
                    match side {
                        ScalarSeriesOperand::Left(_) => {
                            ordinary_arithmetic(op, scalar, field.clone())
                        }
                        ScalarSeriesOperand::Right(_) => {
                            ordinary_arithmetic(op, field.clone(), scalar)
                        }
                    }
                })
                .collect::<Result<Vec<_>>>();
            match (map_arithmetic_series(&schema, &input, op, side), expected) {
                (Ok(output), Ok(expected)) => {
                    for (field, expected) in output.fields.iter().zip(expected) {
                        assert_floats(field, &expected.iter().collect::<Vec<_>>());
                    }
                    assert!(
                        matches!(&output.time, TimeAxis::Grid(range) if range.start() == 7 && range.len() == 8)
                    );
                    assert!(output.metric.name().is_none());
                    assert_eq!(output.metric.tags(), input.metric.tags());
                }
                (Err(actual), Err(expected)) => {
                    assert_eq!(
                        std::mem::discriminant(&actual),
                        std::mem::discriminant(&expected)
                    );
                    assert_eq!(actual.to_string(), expected.to_string());
                }
                (actual, expected) => panic!("{op:?}: {actual:?} differs from {expected:?}"),
            }
        }
    }
}

#[test]
fn dynamic_comparison_preserves_vector_values_and_main_ieee_rules() {
    let schema = schema(&["value"]);
    let vector_values =
        Float64Array::from(vec![Some(-0.0), Some(2.0), None, Some(f64::NAN), Some(4.0)]);
    let input = series(&schema, 6, vec![Samples::from_float(vector_values.clone())]);
    let scalar_values =
        Float64Array::from(vec![Some(0.0), None, Some(5.0), Some(f64::NAN), Some(2.0)]);
    let scalar = scalar(
        &schema,
        6,
        Samples::try_from_chunks(vec![
            Samples::from_float(scalar_values.slice(0, 1)),
            Samples::from_float(scalar_values.slice(1, 4)),
        ])
        .unwrap(),
    );
    for (op, ordinary_op) in [
        (ComparisonOp::Equal, Operator::Eq),
        (ComparisonOp::NotEqual, Operator::NotEq),
        (ComparisonOp::Less, Operator::Lt),
        (ComparisonOp::LessEqual, Operator::LtEq),
        (ComparisonOp::Greater, Operator::Gt),
        (ComparisonOp::GreaterEqual, Operator::GtEq),
    ] {
        for side in [
            ScalarSeriesOperand::Left(&scalar),
            ScalarSeriesOperand::Right(&scalar),
        ] {
            let v = ColumnarValue::Array(Arc::new(vector_values.clone()));
            let s = ColumnarValue::Array(Arc::new(scalar_values.clone()));
            let args = match side {
                ScalarSeriesOperand::Left(_) => vec![s, v],
                ScalarSeriesOperand::Right(_) => vec![v, s],
            };
            let field = Arc::new(Field::new("operand", DataType::Float64, true));
            let expected = IeeeComparison::scalar_udf(ordinary_op)
                .invoke_with_args(ScalarFunctionArgs {
                    args,
                    arg_fields: vec![field; 2],
                    number_rows: 5,
                    return_field: Arc::new(Field::new("output", DataType::Boolean, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                })
                .unwrap()
                .into_array(5)
                .unwrap();
            for mode in [Comparison::Filter(op), Comparison::Bool(op)] {
                let output = map_compare_series(&schema, &input, mode, side).unwrap();
                let expected = vector_values
                    .iter()
                    .zip(expected.as_boolean().iter())
                    .map(|(sample, yes)| match mode {
                        Comparison::Bool(_) => yes.map(f64::from),
                        Comparison::Filter(_) => {
                            yes.and_then(|yes| if yes { sample } else { None })
                        }
                    })
                    .collect::<Vec<_>>();
                assert_floats(output.fields.get(0).unwrap(), &expected);
                assert_eq!(
                    output.metric.name(),
                    if matches!(mode, Comparison::Bool(_)) {
                        None
                    } else {
                        Some("requests")
                    }
                );
                assert_eq!(output.metric.tags(), input.metric.tags());
            }
        }
    }
}

#[test]
fn intersection_handles_disjoint_empty_and_partial_scalar_ranges() {
    let schema = schema(&["value"]);
    let input = series(
        &schema,
        5,
        vec![floats(&[Some(10.0), Some(20.0), Some(30.0)])],
    );
    for (start, samples, output_start, expected) in [
        (3, vec![Some(1.0), Some(2.0), Some(3.0)], 5, vec![Some(7.0)]),
        (7, vec![Some(3.0), Some(4.0)], 7, vec![Some(27.0)]),
        (8, vec![Some(1.0)], 8, vec![]),
        (2, vec![], 5, vec![]),
        (64, vec![], 64, vec![]),
    ] {
        let scalar = scalar(&schema, start, floats(&samples));
        let output = map_arithmetic_series(
            &schema,
            &input,
            ArithmeticOp::Subtract,
            ScalarSeriesOperand::Right(&scalar),
        )
        .unwrap();
        assert_floats(output.fields.get(0).unwrap(), &expected);
        assert!(
            matches!(&output.time, TimeAxis::Grid(range) if range.start() == output_start && range.len() == expected.len())
        );
    }
}

#[test]
fn dynamic_scalar_preflight_rejects_unsupported_data_even_outside_overlap() {
    let schema = schema(&["value"]);
    let input = series(&schema, 5, vec![floats(&[Some(2.0)])]);
    let mut named = scalar(&schema, 5, floats(&[Some(1.0)]));
    named.metric.set("host", "a".into()).unwrap();
    let mut multi = scalar(&schema, 5, floats(&[Some(1.0)]));
    multi.fields =
        SeriesFields::try_from_vec(vec![floats(&[Some(1.0)]), floats(&[Some(1.0)])]).unwrap();
    let mut other_grid = scalar(&schema, 5, floats(&[Some(1.0)]));
    other_grid.time = TimeAxis::Grid(
        GridRange::try_new(
            Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 500, 64).unwrap()).unwrap()),
            5,
            1,
        )
        .unwrap(),
    );
    let histogram = scalar(
        &schema,
        20,
        Samples::try_from_histogram(UInt32Array::from(vec![0]), histogram_batch()).unwrap(),
    );
    for scalar in [named, multi, other_grid, histogram] {
        assert!(
            map_arithmetic_series(
                &schema,
                &input,
                ArithmeticOp::Add,
                ScalarSeriesOperand::Right(&scalar)
            )
            .is_err()
        );
        assert!(
            map_compare_series(
                &schema,
                &input,
                Comparison::Bool(ComparisonOp::Equal),
                ScalarSeriesOperand::Right(&scalar)
            )
            .is_err()
        );
    }
    let multi_schema = super::schema(&["a", "b"]);
    let multi_vector = series(
        &multi_schema,
        5,
        vec![floats(&[Some(1.0)]), floats(&[Some(2.0)])],
    );
    let scalar = scalar(&multi_schema, 5, floats(&[Some(1.0)]));
    let mut raw_scalar = scalar.clone();
    raw_scalar.time = TimeAxis::try_new(vec![3500].into()).unwrap();
    assert!(
        map_arithmetic_series(
            &multi_schema,
            &multi_vector,
            ArithmeticOp::Add,
            ScalarSeriesOperand::Right(&raw_scalar)
        )
        .is_err()
    );
    assert!(matches!(
        map_compare_series(
            &multi_schema,
            &multi_vector,
            Comparison::Filter(ComparisonOp::Equal),
            ScalarSeriesOperand::Right(&scalar)
        ),
        Err(DataFusionError::NotImplemented(_))
    ));
    let output = map_compare_series(
        &multi_schema,
        &multi_vector,
        Comparison::Bool(ComparisonOp::Equal),
        ScalarSeriesOperand::Right(&scalar),
    )
    .unwrap();
    assert_floats(output.fields.get(0).unwrap(), &[Some(1.0)]);
    assert_floats(output.fields.get(1).unwrap(), &[Some(0.0)]);
}

#[test]
fn dynamic_power_propagates_late_error_and_keeps_masked_zero_missing() {
    let schema = schema(&["value"]);
    let input = series(&schema, 4, vec![floats(&[Some(2.0), None])]);
    let scalar = scalar(&schema, 4, floats(&[Some(2.0), Some(-1.0)]));
    let output = map_arithmetic_series(
        &schema,
        &input,
        ArithmeticOp::Power,
        ScalarSeriesOperand::Right(&scalar),
    )
    .unwrap();
    assert_floats(output.fields.get(0).unwrap(), &[Some(4.0), None]);
    let invalid = series(
        &schema,
        4,
        vec![Samples::try_from_chunks(vec![floats(&[Some(2.0)]), floats(&[Some(-0.0)])]).unwrap()],
    );
    assert!(matches!(
        map_arithmetic_series(
            &schema,
            &invalid,
            ArithmeticOp::Power,
            ScalarSeriesOperand::Right(&scalar)
        ),
        Err(DataFusionError::ArrowError(_, _))
    ));
}
