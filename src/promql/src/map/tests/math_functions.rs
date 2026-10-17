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

use common_function::scalars::math::{ClampMaxFunction, ClampMinFunction};

use super::*;

#[test]
fn clamp_matches_ordinary_functions_for_nan_bounds_and_chunked_slices() {
    // Boundaries: NaN in either bound/sample, infinities, equal/reversed bounds, signed zero,
    // missing samples, independently chunked fields and nonzero slices.
    let schema = schema(&["a", "b"]);
    let samples = [
        Some(-4.0),
        Some(-0.0),
        Some(0.0),
        Some(4.0),
        Some(f64::NEG_INFINITY),
        Some(f64::INFINITY),
        Some(f64::NAN),
        None,
    ];
    let dense = Float64Array::from(samples.to_vec());
    let chunks = Samples::try_from_chunks(vec![
        Samples::from_float(dense.slice(0, 3)),
        Samples::from_float(dense.slice(3, 5)),
    ])
    .unwrap();
    let input = series(&schema, 7, vec![Samples::from_float(dense.clone()), chunks]);
    for min in [
        -2.0,
        -0.0,
        0.0,
        2.0,
        f64::NEG_INFINITY,
        f64::INFINITY,
        f64::NAN,
    ] {
        for max in [
            -2.0,
            -0.0,
            0.0,
            2.0,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NAN,
        ] {
            let cases: [(MathOp, Box<dyn Function>, Vec<f64>); 3] = [
                (
                    MathOp::Clamp { min, max },
                    Box::new(ClampFunction::default()),
                    vec![min, max],
                ),
                (
                    MathOp::ClampMin { min },
                    Box::new(ClampMinFunction::default()),
                    vec![min],
                ),
                (
                    MathOp::ClampMax { max },
                    Box::new(ClampMaxFunction::default()),
                    vec![max],
                ),
            ];
            for (op, function, bounds) in cases {
                let mut args = vec![ColumnarValue::Array(Arc::new(dense.clone()))];
                args.extend(bounds.into_iter().map(|v| ColumnarValue::Scalar(v.into())));
                let field = Arc::new(Field::new("value", DataType::Float64, true));
                let expected = function.invoke_with_args(ScalarFunctionArgs {
                    arg_fields: vec![field.clone(); args.len()],
                    args,
                    number_rows: dense.len(),
                    return_field: field,
                    config_options: Arc::new(ConfigOptions::default()),
                });
                let output = map_math(&schema, &input, op);
                match (output, expected) {
                    (Ok(output), Ok(expected)) => {
                        let expected = expected.into_array(dense.len()).unwrap();
                        let expected = expected
                            .as_primitive::<Float64Type>()
                            .iter()
                            .collect::<Vec<_>>();
                        for field in output.fields.iter() {
                            assert_floats(field, &expected);
                        }
                        assert!(
                            matches!((&output.time, &input.time), (TimeAxis::Grid(actual), TimeAxis::Grid(expected)) if actual == expected)
                        );
                        assert_eq!(output.metric, input.metric);
                    }
                    (Err(DataFusionError::Execution(_)), Err(DataFusionError::Execution(_))) => {}
                    (actual, expected) => panic!("{op:?}: {actual:?} differs from {expected:?}"),
                }
            }
        }
    }
    let clipped = map_math(
        &schema,
        &input,
        MathOp::Clamp {
            min: -0.0,
            max: 0.0,
        },
    )
    .unwrap();
    assert_floats(
        clipped.fields.get(0).unwrap(),
        &[
            Some(-0.0),
            Some(-0.0),
            Some(0.0),
            Some(0.0),
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            None,
        ],
    );
}

#[test]
fn invalid_clamp_bounds_fail_even_for_empty_or_all_missing_series() {
    let schema = schema(&["value"]);
    for samples in [vec![], vec![None, None]] {
        let input = series(&schema, 12, vec![floats(&samples)]);
        assert!(matches!(
            map_math(&schema, &input, MathOp::Clamp { min: 2.0, max: 1.0 }),
            Err(DataFusionError::Execution(_))
        ));
        let output = map_math(
            &schema,
            &input,
            MathOp::Clamp {
                min: f64::NAN,
                max: 1.0,
            },
        )
        .unwrap();
        assert_floats(output.fields.get(0).unwrap(), &samples);
        assert!(
            matches!((&output.time, &input.time), (TimeAxis::Grid(actual), TimeAxis::Grid(expected)) if actual == expected)
        );
    }
}

#[test]
fn pi_produces_an_unlabelled_scalar_only_on_the_requested_range() {
    let schema = schema(&["value"]);
    for len in [0, 1, 7] {
        let range = GridRange::try_new(schema.eval_grid.clone().unwrap(), 13, len).unwrap();
        let output = map_pi(&schema, range.clone()).unwrap();
        assert!(matches!(&output.time, TimeAxis::Grid(actual) if actual == &range));
        assert!(output.metric.name().is_none());
        assert!(output.metric.tags().is_empty());
        assert_floats(
            output.fields.get(0).unwrap(),
            &vec![Some(std::f64::consts::PI); len],
        );
    }
    let invalid = super::schema(&["a", "b"]);
    let range = GridRange::try_new(invalid.eval_grid.clone().unwrap(), 0, 1).unwrap();
    assert!(map_pi(&invalid, range).is_err());
}
