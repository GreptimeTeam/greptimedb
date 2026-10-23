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
        SeriesLabels::try_new(None, vec![]).unwrap(),
        TimeAxis::Grid(
            GridRange::try_new(schema.eval_grid.clone().unwrap(), start, samples.len()).unwrap(),
        ),
        SeriesFields::one(samples),
    )
    .unwrap()
}

#[test]
fn time_varying_scalar_aligns_absolute_indices_across_independent_chunks() {
    // The vector covers 7..15 and the scalar 5..16, each chunked at different offsets.
    let schema = Arc::new(schema(&["a", "b"]));
    let first = Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
        None,
        Some(4.0),
        Some(5.0),
        Some(6.0),
        Some(7.0),
        Some(8.0),
    ]);
    let second = Float64Array::from(vec![
        None,
        Some(20.0),
        Some(30.0),
        Some(40.0),
        Some(50.0),
        Some(60.0),
        Some(70.0),
        None,
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
        Some(-1.0),
        Some(-2.0),
        Some(100.0),
        Some(200.0),
        Some(300.0),
        None,
        Some(500.0),
        Some(600.0),
        Some(700.0),
        Some(800.0),
        Some(-3.0),
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
    let output = mapped(
        map_arithmetic_series(
            &schema,
            &input,
            ArithmeticOp::Subtract,
            ScalarSeriesOperand::Left(&scalar),
        )
        .unwrap(),
    );
    // The scalar holds no value at grid position 10, so neither field has a result there: that
    // position is not a row, and the two sides of it form separate runs of one series.
    assert_eq!(run_ranges(&output), vec![(7, 3), (11, 4)]);
    assert_eq!(
        run_floats(&output, 0),
        vec![
            ((7, 3), vec![Some(99.0), Some(198.0), None]),
            (
                (11, 4),
                vec![Some(495.0), Some(594.0), Some(693.0), Some(792.0)]
            ),
        ]
    );
    assert_eq!(
        run_floats(&output, 1),
        vec![
            ((7, 3), vec![None, Some(180.0), Some(270.0)]),
            ((11, 4), vec![Some(450.0), Some(540.0), Some(630.0), None]),
        ]
    );
    assert!(output.metric.name().is_none());
    assert_eq!(output.metric.tags(), &input.metric.tags()[1..]);
    let output = mapped(
        map_arithmetic_series(
            &schema,
            &input,
            ArithmeticOp::Subtract,
            ScalarSeriesOperand::Right(&scalar),
        )
        .unwrap(),
    );
    assert_eq!(run_ranges(&output), vec![(7, 3), (11, 4)]);
    assert_eq!(
        run_floats(&output, 0),
        vec![
            ((7, 3), vec![Some(-99.0), Some(-198.0), None]),
            (
                (11, 4),
                vec![Some(-495.0), Some(-594.0), Some(-693.0), Some(-792.0)]
            ),
        ]
    );
}

#[test]
fn time_varying_comparison_keeps_vector_values_and_trims() {
    let schema = Arc::new(schema(&["value"]));
    let input = series(
        &schema,
        6,
        vec![floats(&[
            Some(-0.0),
            Some(2.0),
            None,
            Some(f64::NAN),
            Some(4.0),
            Some(1.0),
        ])],
    );
    let scalar = scalar(
        &schema,
        6,
        floats(&[
            Some(0.0),
            None,
            Some(5.0),
            Some(f64::NAN),
            Some(2.0),
            Some(9.0),
        ]),
    );
    let output = mapped(
        map_compare_series(
            &schema,
            &input,
            Comparison::Filter(ComparisonOp::Less),
            ScalarSeriesOperand::Left(&scalar),
        )
        .unwrap(),
    );
    // Only position 10 passes, and it is the whole result: one run, one row.
    assert_eq!(run_ranges(&output), vec![(10, 1)]);
    assert_eq!(run_floats(&output, 0), vec![((10, 1), vec![Some(4.0)])]);
    assert_eq!(output.metric, input.metric);
    let output = mapped(
        map_compare_series(
            &schema,
            &input,
            Comparison::Bool(ComparisonOp::Equal),
            ScalarSeriesOperand::Right(&scalar),
        )
        .unwrap(),
    );
    // Bool emits at every position where both operands exist. The scalar holds no value at grid
    // position 7, so that position has no row and ends a run; 8 to 10 follow it.
    assert_eq!(run_ranges(&output), vec![(6, 1), (9, 3)]);
    assert_eq!(
        run_floats(&output, 0),
        vec![
            ((6, 1), vec![Some(1.0)]),
            ((9, 3), vec![Some(0.0), Some(0.0), Some(0.0)]),
        ]
    );
    assert!(output.metric.name().is_none());
}

#[test]
fn disjoint_or_missing_overlap_produces_no_series() {
    let schema = Arc::new(schema(&["value"]));
    let input = series(
        &schema,
        5,
        vec![floats(&[Some(10.0), Some(20.0), Some(30.0)])],
    );
    for (start, samples, expected) in [
        (
            3,
            vec![Some(1.0), Some(2.0), Some(3.0)],
            Some(((5, 1), vec![Some(7.0)])),
        ),
        (
            7,
            vec![Some(3.0), Some(4.0)],
            Some(((7, 1), vec![Some(27.0)])),
        ),
        (
            4,
            vec![None, Some(1.0), None, None],
            Some(((5, 1), vec![Some(9.0)])),
        ),
        (5, vec![None, None, None], None),
        (8, vec![Some(1.0)], None),
        (2, vec![], None),
        (64, vec![], None),
    ] {
        let scalar = scalar(&schema, start, floats(&samples));
        let output = map_arithmetic_series(
            &schema,
            &input,
            ArithmeticOp::Subtract,
            ScalarSeriesOperand::Right(&scalar),
        )
        .unwrap();
        // An overlap with no valid scalar point yields no series at all.
        match (output, expected) {
            (None, None) => {}
            (Some(output), Some((range, values))) => {
                assert_eq!(run_ranges(&output), vec![range]);
                assert_eq!(run_floats(&output, 0), vec![(range, values)]);
            }
            (output, expected) => panic!("{start}: {output:?} != {expected:?}"),
        }
    }
}

#[test]
fn time_varying_scalar_preflight_rejects_unsupported_data_even_outside_overlap() {
    let schema = Arc::new(schema(&["value"]));
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
    let mut raw = scalar(&schema, 5, floats(&[Some(1.0)]));
    raw.time = TimeAxis::try_new(vec![3500].into()).unwrap();
    for scalar in [named, multi, other_grid, histogram, raw] {
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
}

/// F: a vector–scalar operation between two sparse operands aligns by shared runs, not by
/// subtracting first positions, and reports the surviving positions as their own runs.
///
/// Vector positions 1, 5 and 9; scalar positions 5 and 9. Only 5 and 9 are evaluated, and the
/// result's axis names them as two runs with a hole between -- a row offset taken from the first
/// position would pair 1 with 5 and 5 with 9.
#[test]
fn a_vector_scalar_operation_of_sparse_operands_keeps_the_shared_positions() {
    use crate::grid_series::reference::{
        Dense, Labels, assert_result_eq, run_ranges, sparse_series,
    };

    // The vector schema of this module carries the grid; the scalar must sit on the same one.
    let vector_schema = Arc::new(schema(&["value"]));
    let scalar_schema = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: "scalar".into(),
        }]
        .into(),
        eval_grid: vector_schema.eval_grid.clone(),
    });
    // The metric of the vector, which these operations keep after dropping `__name__` (the fixture
    // has none, so it is the vector's own metric).
    let labels = Labels::from([("host".to_string(), "a".to_string())]);
    let without_name = crate::grid_series::reference::metric_of(&labels);
    let points = |values: &[(usize, f64)]| -> crate::grid_series::reference::Points {
        values
            .iter()
            .map(|&(position, value)| (position, f64::to_bits(value)))
            .collect()
    };
    let vector = sparse_series(
        &vector_schema,
        &labels,
        &points(&[(1, 10.0), (5, 50.0), (9, 90.0)]),
    )
    .expect("three positions hold a value");
    let scalar = sparse_series(
        &scalar_schema,
        &Labels::new(),
        &points(&[(5, 2.0), (9, 3.0)]),
    )
    .expect("two positions hold a value");
    assert_eq!(run_ranges(&vector), vec![(1, 1), (5, 1), (9, 1)]);
    assert_eq!(run_ranges(&scalar), vec![(5, 1), (9, 1)]);
    assert!(
        scalar.metric.name().is_none() && scalar.metric.tags().is_empty(),
        "a grid scalar is unlabelled"
    );

    let output = map_arithmetic_series(
        &vector_schema,
        &vector,
        ArithmeticOp::Add,
        ScalarSeriesOperand::Right(&scalar),
    )
    .unwrap()
    .expect("positions 5 and 9 hold both operands");
    assert_eq!(
        run_ranges(&output),
        vec![(5, 1), (9, 1)],
        "only the shared positions survive, as two runs"
    );
    assert_eq!(output.time.len(), 2);
    // The reference expands over the schema's whole grid, not the span the fixture covers.
    let points_on_grid = vector_schema
        .eval_grid
        .as_ref()
        .expect("the fixture schema has a grid")
        .spec()
        .len as usize;
    let mut expected: Dense = vec![vec![None]; points_on_grid];
    expected[5] = vec![Some(52.0)];
    expected[9] = vec![Some(93.0)];
    assert_result_eq(Some(&output), &expected, &without_name);

    // The scalar on the left: `2 - 50` and `3 - 90`, not the same values as above.
    let output = map_arithmetic_series(
        &vector_schema,
        &vector,
        ArithmeticOp::Subtract,
        ScalarSeriesOperand::Left(&scalar),
    )
    .unwrap()
    .expect("positions 5 and 9 hold both operands");
    let mut expected: Dense = vec![vec![None]; points_on_grid];
    expected[5] = vec![Some(-48.0)];
    expected[9] = vec![Some(-87.0)];
    assert_result_eq(Some(&output), &expected, &without_name);

    // A filter against the scalar keeps the vector value where it passes and drops the rest.
    let output = map_compare_series(
        &vector_schema,
        &vector,
        Comparison::Filter(ComparisonOp::Greater),
        ScalarSeriesOperand::Right(&scalar),
    )
    .unwrap()
    .expect("both shared positions pass");
    assert_eq!(run_ranges(&output), vec![(5, 1), (9, 1)]);
    let mut expected: Dense = vec![vec![None]; points_on_grid];
    expected[5] = vec![Some(50.0)];
    expected[9] = vec![Some(90.0)];
    assert_result_eq(Some(&output), &expected, &without_name);

    // Too tight a bound: nothing passes, so the operation yields no series at all.
    let high = sparse_series(
        &scalar_schema,
        &Labels::new(),
        &points(&[(5, 1_000.0), (9, 1_000.0)]),
    )
    .expect("two positions hold a value");
    assert!(
        map_compare_series(
            &vector_schema,
            &vector,
            Comparison::Filter(ComparisonOp::Greater),
            ScalarSeriesOperand::Right(&high),
        )
        .unwrap()
        .is_none(),
        "nothing passes, so there is no result series"
    );
}

/// `round` and the clamp functions with time-varying scalar parameters, evaluated by the
/// Prometheus v3.15.0 engine; see the header of `scalar_parameter_cases.txt`.
#[test]
fn parameterized_math_matches_prometheus() {
    use std::collections::BTreeMap;

    use crate::grid_series::reference::{Labels, Points, labels_of, points_of};

    let parse = |line: &str| -> (Labels, Points) {
        let (labels, points) = line.split_once(" ;").unwrap();
        let labels = labels
            .split(',')
            .filter(|pair| *pair != "-")
            .map(|pair| {
                let (name, value) = pair.split_once('=').unwrap();
                (name.to_string(), value.to_string())
            })
            .collect();
        let points = points
            .split_whitespace()
            .map(|point| {
                let (index, bits) = point.split_once(':').unwrap();
                (
                    index.parse().unwrap(),
                    u64::from_str_radix(bits, 16).unwrap(),
                )
            })
            .collect();
        (labels, points)
    };
    let mut checked = 0;
    for case in include_str!("../scalar_parameter_cases.txt")
        .split("end\n")
        .filter(|case| case.contains("case "))
    {
        let lines: Vec<&str> = case.lines().filter(|line| !line.starts_with('#')).collect();
        let name = lines[0].strip_prefix("case ").unwrap();
        let points: u64 = lines[2].split_whitespace().nth(3).unwrap().parse().unwrap();
        let grid = Arc::new(
            EvalGrid::try_new(GridSpec::try_new(100_000, 10_000, points).unwrap()).unwrap(),
        );
        let input_schema = Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: "value".into(),
            }]
            .into(),
            eval_grid: Some(grid.clone()),
        });
        let sparse_series = crate::grid_series::reference::sparse_series;
        let (mut inputs, mut parameters, mut expected) = (Vec::new(), Vec::new(), BTreeMap::new());
        for line in &lines[3..] {
            let (kind, rest) = line.split_once(' ').unwrap();
            let (labels, values) = parse(rest);
            match kind {
                "series" => inputs.extend(sparse_series(&input_schema, &labels, &values)),
                // A parameter is `scalar(p)`: unlabelled, with a value at every point.
                "param" => parameters.push(
                    sparse_series(&input_schema, &Labels::new(), &values)
                        .expect("a value at every point"),
                ),
                "result" => {
                    expected.insert(labels, values);
                }
                other => panic!("unexpected line {other}"),
            }
        }
        let function = match (name, parameters.as_slice()) {
            ("round", [to_nearest]) => ParameterizedMath::Round { to_nearest },
            ("clamp", [min, max]) => ParameterizedMath::Clamp { min, max },
            ("clamp_min", [min]) => ParameterizedMath::ClampMin { min },
            ("clamp_max", [max]) => ParameterizedMath::ClampMax { max },
            other => panic!("unknown case {other:?}"),
        };
        let mut actual = BTreeMap::new();
        for input in &inputs {
            if let Some(output) = map_math_with_parameters(&input_schema, input, function).unwrap()
            {
                actual.insert(labels_of(&output.metric), points_of(&output));
            }
        }
        // Go's NaN and Rust's differ in their payload; a NaN is a NaN.
        let canonical = |series: BTreeMap<Labels, Points>| -> BTreeMap<Labels, Points> {
            series
                .into_iter()
                .map(|(labels, points)| {
                    let points = points
                        .into_iter()
                        .map(|(index, bits)| {
                            (
                                index,
                                crate::grid_series::reference::canonical_bits(f64::from_bits(bits)),
                            )
                        })
                        .collect();
                    (labels, points)
                })
                .collect()
        };
        assert_eq!(canonical(actual), canonical(expected), "{case}");
        checked += 1;
    }
    assert!(checked >= 40, "{checked}");
}

/// Parameters are read at each point of the input, whether they cover the grid or not: a point
/// where a parameter has no value, absent from its axis or null, has no result, and every field
/// takes the parameters of its point.
#[test]
fn parameters_are_read_per_point_and_missing_ones_drop_the_point() {
    let schema = Arc::new(schema(&["a", "b"]));
    let input = series(
        &schema,
        2,
        vec![
            floats(&[Some(1.4), Some(2.6), Some(3.5), Some(4.4)]),
            floats(&[Some(-1.4), None, Some(-3.5), Some(9.0)]),
        ],
    );
    // `min` covers 2..=3 and 5 with a null at 3; `max` covers 2..=5.
    let min = Series::try_new(
        &SeriesSchema {
            fields: vec![FieldSpec {
                name: "scalar".into(),
            }]
            .into(),
            eval_grid: schema.eval_grid.clone(),
        },
        SeriesLabels::try_new(None, vec![]).unwrap(),
        TimeAxis::from_grid_spans(
            schema.eval_grid.clone().unwrap(),
            vec![GridSpan::new(2, 2), GridSpan::new(5, 1)],
        )
        .unwrap(),
        SeriesFields::one(floats(&[Some(0.0), None, Some(5.0)])),
    )
    .unwrap();
    let max = scalar(
        &schema,
        2,
        floats(&[Some(2.0), Some(2.0), Some(2.0), Some(4.0)]),
    );
    let output = map_math_with_parameters(
        &schema,
        &input,
        ParameterizedMath::Clamp {
            min: &min,
            max: &max,
        },
    )
    .unwrap()
    .unwrap();
    // Point 2 clamps both fields to [0, 2]; point 3 has a null `min`; point 4 has no `min`; point 5
    // has `max < min`, so no result.
    assert_eq!(run_floats(&output, 0), vec![((2, 1), vec![Some(1.4)])]);
    assert_eq!(run_floats(&output, 1), vec![((2, 1), vec![Some(0.0)])]);
}

/// Like `map_math`, the parameterized functions skip native histogram samples.
#[test]
fn parameterized_math_skips_histogram_samples() {
    let schema = Arc::new(schema(&["mixed"]));
    let mixed = Samples::try_from_mixed(
        Float64Array::from(vec![Some(1.26), None]),
        HistogramSamples::try_new(UInt32Array::from(vec![None, Some(0)]), histogram_batch())
            .unwrap(),
    )
    .unwrap();
    let input = series(&schema, 2, vec![mixed]);
    let to_nearest = scalar(&schema, 2, floats(&[Some(0.5), Some(0.5)]));
    let output = map_math_with_parameters(
        &schema,
        &input,
        ParameterizedMath::Round {
            to_nearest: &to_nearest,
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(run_floats(&output, 0), vec![((2, 1), vec![Some(1.5)])]);
}
