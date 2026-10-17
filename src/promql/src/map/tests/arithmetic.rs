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
use datafusion_expr::{Expr, col, lit};
use datatypes::arrow::datatypes::Schema;
use datatypes::arrow::record_batch::RecordBatch;

use super::*;

const OPERATIONS: [ArithmeticOp; 7] = [
    ArithmeticOp::Add,
    ArithmeticOp::Subtract,
    ArithmeticOp::Multiply,
    ArithmeticOp::Divide,
    ArithmeticOp::Modulo,
    ArithmeticOp::Power,
    ArithmeticOp::Atan2,
];

fn ordinary_expression(expr: Expr, input: Float64Array) -> Result<Float64Array> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(input)])?;
    let physical = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema)?,
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    )?;
    Ok(physical
        .evaluate(&batch)?
        .into_array(batch.num_rows())?
        .as_primitive::<Float64Type>()
        .clone())
}

fn ordinary_arithmetic(
    op: ArithmeticOp,
    scalar: ScalarOperand,
    input: Float64Array,
) -> Result<Float64Array> {
    // Use the ordinary planner's expression builders, not the native operation dispatcher.
    let (left, right) = match scalar {
        ScalarOperand::Left(scalar) => (lit(scalar), col("value")),
        ScalarOperand::Right(scalar) => (col("value"), lit(scalar)),
    };
    let expr = match op {
        ArithmeticOp::Add => left + right,
        ArithmeticOp::Subtract => left - right,
        ArithmeticOp::Multiply => left * right,
        ArithmeticOp::Divide => left / right,
        ArithmeticOp::Modulo => left % right,
        ArithmeticOp::Power => math::power().call(vec![left, right]),
        ArithmeticOp::Atan2 => math::atan2().call(vec![left, right]),
    };
    ordinary_expression(expr, input)
}

#[test]
fn scalar_arithmetic_matches_ordinary_expressions_in_both_directions() {
    let input = float_boundary_samples();
    let mut schema = schema(&["value"]);
    schema.eval_grid = Some(Arc::new(
        EvalGrid::try_new(
            GridSpec::try_new(1000, 500, (input.len() + 8).try_into().unwrap()).unwrap(),
        )
        .unwrap(),
    ));
    let chunks = (0..input.len())
        .step_by(61)
        .map(|offset| Samples::from_float(input.slice(offset, 61.min(input.len() - offset))))
        .collect();
    let input_series = series(&schema, 7, vec![Samples::try_from_chunks(chunks).unwrap()]);
    for op in OPERATIONS {
        for scalar in [
            -2.5,
            -1.0,
            -0.0,
            0.0,
            0.5,
            1.0,
            f64::from_bits(1),
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
        ] {
            for operand in [ScalarOperand::Left(scalar), ScalarOperand::Right(scalar)] {
                let expected = ordinary_arithmetic(op, operand, input.clone());
                let actual = map_arithmetic(&schema, &input_series, op, operand);
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_floats(
                            actual.fields.get(0).unwrap(),
                            &expected.iter().collect::<Vec<_>>(),
                        );
                        assert_eq!(actual.metric.name(), None, "{op:?}, {operand:?}");
                        assert_eq!(actual.metric.tags(), input_series.metric.tags());
                        let TimeAxis::Grid(range) = actual.time else {
                            panic!("lost grid axis")
                        };
                        assert_eq!((range.start(), range.len()), (7, input.len()));
                    }
                    (Err(actual), Err(expected)) => assert_eq!(
                        actual.to_string(),
                        expected.to_string(),
                        "{op:?}, {operand:?}"
                    ),
                    (actual, expected) => {
                        panic!("{op:?}, {operand:?}: native {actual:?}, ordinary {expected:?}")
                    }
                }
            }
        }
    }
}

#[test]
fn unary_negation_matches_ordinary_expression_and_keeps_missing_positions() {
    let schema = schema(&["value"]);
    let nan = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
    let input = Float64Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(-2.5),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(nan),
        None,
    ]);
    let input_series = series(&schema, 6, vec![Samples::from_float(input.clone())]);
    let output = map_negate(&schema, &input_series).unwrap();
    let expected = ordinary_expression(Expr::Negative(Box::new(col("value"))), input).unwrap();
    assert_floats(
        output.fields.get(0).unwrap(),
        &expected.iter().collect::<Vec<_>>(),
    );
    assert_eq!(output.metric.name(), None);
    assert_eq!(output.metric.tags(), input_series.metric.tags());
    assert_eq!(input_series.metric.name(), Some("requests"));
    assert_eq!(output.time.timestamp(0), Some(4000));
    assert_eq!(output.time.timestamp(6), Some(7000));
}

#[test]
fn noncommutative_arithmetic_keeps_operand_order_across_multifield_slices() {
    let schema = schema(&["first", "second"]);
    let first = floats(&[Some(99.0), Some(2.0), None, Some(4.0), Some(99.0)]);
    let second = floats(&[Some(99.0), None, Some(8.0), Some(-2.0), Some(99.0)]);
    let input = series(
        &schema,
        9,
        vec![
            Samples::try_from_chunks(vec![first.slice(1, 1).unwrap(), first.slice(2, 2).unwrap()])
                .unwrap(),
            Samples::try_from_chunks(vec![
                second.slice(1, 2).unwrap(),
                second.slice(3, 1).unwrap(),
            ])
            .unwrap(),
        ],
    );
    let left = map_arithmetic(
        &schema,
        &input,
        ArithmeticOp::Divide,
        ScalarOperand::Left(8.0),
    )
    .unwrap();
    let right = map_arithmetic(
        &schema,
        &input,
        ArithmeticOp::Divide,
        ScalarOperand::Right(8.0),
    )
    .unwrap();
    assert_floats(left.fields.get(0).unwrap(), &[Some(4.0), None, Some(2.0)]);
    assert_floats(left.fields.get(1).unwrap(), &[None, Some(1.0), Some(-4.0)]);
    assert_floats(right.fields.get(0).unwrap(), &[Some(0.25), None, Some(0.5)]);
    assert_floats(
        right.fields.get(1).unwrap(),
        &[None, Some(1.0), Some(-0.25)],
    );
    for (actual, source) in left.fields.iter().zip(input.fields.iter()) {
        assert_eq!(
            actual
                .segments()
                .iter()
                .map(Samples::len)
                .collect::<Vec<_>>(),
            source
                .segments()
                .iter()
                .map(Samples::len)
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(left.time.timestamp(0), Some(5500));
    assert_eq!(left.time.timestamp(2), Some(6500));
    assert_floats(input.fields.get(0).unwrap(), &[Some(2.0), None, Some(4.0)]);
}

#[test]
fn power_preserves_missing_values_and_rejects_a_late_invalid_sample() {
    let schema = schema(&["first", "second"]);
    // Both scalar directions exercise masked zeros/negative exponents: null payloads must not
    // cause a domain error. Empty and all-null arrays must not manufacture samples either.
    for (len, operand) in [
        (0, ScalarOperand::Right(-1.0)),
        (2, ScalarOperand::Right(-1.0)),
        (2, ScalarOperand::Left(0.0)),
    ] {
        let input = series(
            &schema,
            4,
            vec![Samples::from_float(Float64Array::new_null(len)); 2],
        );
        let output = map_arithmetic(&schema, &input, ArithmeticOp::Power, operand).unwrap();
        for field in output.fields.iter() {
            assert_floats(field, &vec![None; len]);
        }
    }
    for (masked, operand, expected) in [
        (0.0, ScalarOperand::Right(-1.0), 0.5),
        (-1.0, ScalarOperand::Left(0.0), 0.0),
    ] {
        let samples = Float64Array::new(
            vec![masked, 2.0].into(),
            Some(NullBuffer::from(vec![false, true])),
        );
        let input = series(&schema, 4, vec![Samples::from_float(samples); 2]);
        let output = map_arithmetic(&schema, &input, ArithmeticOp::Power, operand).unwrap();
        assert_floats(output.fields.get(0).unwrap(), &[None, Some(expected)]);
    }
    let invalid =
        Samples::try_from_chunks(vec![floats(&[Some(2.0)]), floats(&[Some(-0.0)])]).unwrap();
    let input = series(&schema, 4, vec![floats(&[Some(4.0), Some(8.0)]), invalid]);
    assert!(
        matches!(map_arithmetic(&schema, &input, ArithmeticOp::Power, ScalarOperand::Right(-1.0)), Err(DataFusionError::ArrowError(error, _)) if error.to_string().contains("zero raised to a negative power"))
    );
    assert_floats(input.fields.get(0).unwrap(), &[Some(4.0), Some(8.0)]);
}

#[test]
fn arithmetic_rejects_unevaluated_and_histogram_inputs() {
    let schema = schema(&["value"]);
    let histogram =
        Samples::try_from_histogram(UInt32Array::from(vec![0]), histogram_batch()).unwrap();
    let input = series(&schema, 2, vec![histogram]);
    assert!(matches!(
        map_negate(&schema, &input),
        Err(DataFusionError::NotImplemented(_))
    ));
    for op in OPERATIONS {
        assert!(matches!(
            map_arithmetic(&schema, &input, op, ScalarOperand::Right(2.0)),
            Err(DataFusionError::NotImplemented(_))
        ));
    }
    let raw_schema = SeriesSchema {
        eval_grid: None,
        ..schema
    };
    let raw = Series::try_new(
        &raw_schema,
        input.metric,
        TimeAxis::try_new(vec![2000].into()).unwrap(),
        SeriesFields::one(floats(&[Some(1.0)])),
    )
    .unwrap();
    assert!(matches!(
        map_negate(&raw_schema, &raw),
        Err(DataFusionError::Execution(_))
    ));
    assert!(matches!(
        map_arithmetic(
            &raw_schema,
            &raw,
            ArithmeticOp::Add,
            ScalarOperand::Left(1.0)
        ),
        Err(DataFusionError::Execution(_))
    ));
}
