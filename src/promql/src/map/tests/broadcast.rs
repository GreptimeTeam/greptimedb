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
        MetricName::try_new(None, vec![]).unwrap(),
        TimeAxis::Grid(
            GridRange::try_new(schema.eval_grid.clone().unwrap(), start, samples.len()).unwrap(),
        ),
        SeriesFields::one(samples),
    )
    .unwrap()
}

fn grid_range(series: &Series) -> (usize, usize) {
    let TimeAxis::Grid(range) = &series.time else {
        panic!("lost grid axis")
    };
    (range.start(), range.len())
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
    let output = map_arithmetic_series(
        &schema,
        &input,
        ArithmeticOp::Subtract,
        ScalarSeriesOperand::Left(&scalar),
    )
    .unwrap()
    .unwrap();
    assert_eq!(grid_range(&output), (7, 8));
    assert_floats(
        output.fields.get(0).unwrap(),
        &[
            Some(99.0),
            Some(198.0),
            None,
            None,
            Some(495.0),
            Some(594.0),
            Some(693.0),
            Some(792.0),
        ],
    );
    assert_floats(
        output.fields.get(1).unwrap(),
        &[
            None,
            Some(180.0),
            Some(270.0),
            None,
            Some(450.0),
            Some(540.0),
            Some(630.0),
            None,
        ],
    );
    assert_eq!(output.metric.name(), None);
    assert_eq!(output.metric.tags(), &input.metric.tags()[1..]);
    let output = map_arithmetic_series(
        &schema,
        &input,
        ArithmeticOp::Subtract,
        ScalarSeriesOperand::Right(&scalar),
    )
    .unwrap()
    .unwrap();
    assert_floats(
        output.fields.get(0).unwrap(),
        &[
            Some(-99.0),
            Some(-198.0),
            None,
            None,
            Some(-495.0),
            Some(-594.0),
            Some(-693.0),
            Some(-792.0),
        ],
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
    let output = map_compare_series(
        &schema,
        &input,
        Comparison::Filter(ComparisonOp::Less),
        ScalarSeriesOperand::Left(&scalar),
    )
    .unwrap()
    .unwrap();
    assert_eq!(grid_range(&output), (10, 1));
    assert_floats(output.fields.get(0).unwrap(), &[Some(4.0)]);
    assert_eq!(output.metric, input.metric);
    let output = map_compare_series(
        &schema,
        &input,
        Comparison::Bool(ComparisonOp::Equal),
        ScalarSeriesOperand::Right(&scalar),
    )
    .unwrap()
    .unwrap();
    assert_eq!(grid_range(&output), (6, 6));
    assert_floats(
        output.fields.get(0).unwrap(),
        &[Some(1.0), None, None, Some(0.0), Some(0.0), Some(0.0)],
    );
    assert_eq!(output.metric.name(), None);
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
        match (output, expected) {
            (None, None) => {}
            (Some(output), Some((range, values))) => {
                assert_eq!(grid_range(&output), range);
                assert_floats(output.fields.get(0).unwrap(), &values);
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
