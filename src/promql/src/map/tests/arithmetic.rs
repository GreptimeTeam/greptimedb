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

#[test]
fn unary_negation_drops_name_and_keeps_missing_positions_and_payloads() {
    let schema = schema(&["value"]);
    let stale = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
    let input = series(
        &schema,
        6,
        vec![floats(&[Some(-0.0), Some(2.5), Some(-stale), None])],
    );
    let mut typed = input.clone();
    typed.metric.set("__type__", "gauge".into()).unwrap();
    let output = map_negate(&schema, &typed).unwrap();
    let field = output.fields.get(0).unwrap();
    assert_floats(field, &[Some(0.0), Some(-2.5), Some(stale), None]);
    let SampleRef::Float(negated) = field.iter().nth(2).unwrap() else {
        panic!("missing negated NaN");
    };
    assert_eq!(negated.to_bits(), PROMETHEUS_STALE_NAN_BITS);
    assert_eq!(output.metric.name(), None);
    assert_eq!(output.metric.tags(), input.metric.tags());
    assert_eq!(output.time.timestamp(0), Some(4000));
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
        let lengths = |samples: &Samples| {
            samples
                .segments()
                .iter()
                .map(Samples::len)
                .collect::<Vec<_>>()
        };
        assert_eq!(lengths(actual), lengths(source));
    }
    assert_eq!(left.time.timestamp(0), Some(5500));
    assert_eq!(left.time.timestamp(2), Some(6500));
}

#[test]
fn power_and_modulo_follow_go_special_cases() {
    let schema = schema(&["value"]);
    let input = series(
        &schema,
        0,
        vec![floats(&[
            Some(0.0),
            Some(-0.0),
            Some(f64::NAN),
            Some(-7.0),
            None,
        ])],
    );
    let field = |op, scalar| {
        map_arithmetic(&schema, &input, op, scalar)
            .unwrap()
            .fields
            .get(0)
            .unwrap()
            .clone()
    };
    // Main's DataFusion power fails on a zero base with a negative exponent.
    assert_floats(
        &field(ArithmeticOp::Power, ScalarOperand::Right(-1.0)),
        &[
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            Some(-1.0 / 7.0),
            None,
        ],
    );
    assert_floats(
        &field(ArithmeticOp::Power, ScalarOperand::Right(0.0)),
        &[Some(1.0), Some(1.0), Some(1.0), Some(1.0), None],
    );
    assert_floats(
        &field(ArithmeticOp::Power, ScalarOperand::Left(1.0)),
        &[Some(1.0), Some(1.0), Some(1.0), Some(1.0), None],
    );
    assert_floats(
        &field(ArithmeticOp::Modulo, ScalarOperand::Right(3.0)),
        &[Some(0.0), Some(-0.0), Some(f64::NAN), Some(-1.0), None],
    );
    assert_floats(
        &field(ArithmeticOp::Modulo, ScalarOperand::Left(5.0)),
        &[
            Some(f64::NAN),
            Some(f64::NAN),
            Some(f64::NAN),
            Some(5.0),
            None,
        ],
    );
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
