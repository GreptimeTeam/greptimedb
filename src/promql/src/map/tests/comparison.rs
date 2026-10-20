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

#[test]
fn filter_compares_fields_independently_and_trims_the_output() {
    let schema = Arc::new(schema(&["a", "b"]));
    let input = series(
        &schema,
        4,
        vec![
            floats(&[Some(1.0), Some(1.0), Some(5.0), None, Some(7.0), Some(1.0)]),
            Samples::try_from_chunks(vec![
                floats(&[None, Some(9.0)]),
                floats(&[None, Some(0.0), None, Some(f64::NAN)]),
            ])
            .unwrap(),
        ],
    );
    let output = mapped(
        map_compare(
            &schema,
            &input,
            Comparison::Filter(ComparisonOp::Greater),
            ScalarOperand::Right(4.0),
        )
        .unwrap(),
    );
    // Field 1 keeps position 5 and field 0 keeps position 6, so 5 and 6 stay one run; position 7
    // fails in both fields, which costs no row and ends the run; position 8 keeps field 0.
    assert_eq!(run_ranges(&output), vec![(5, 2), (8, 1)]);
    assert_eq!(
        run_floats(&output, 0),
        vec![((5, 2), vec![None, Some(5.0)]), ((8, 1), vec![Some(7.0)])]
    );
    assert_eq!(
        run_floats(&output, 1),
        vec![((5, 2), vec![Some(9.0), None]), ((8, 1), vec![None])]
    );
    assert_eq!(output.metric, input.metric);
    for scalar in [ScalarOperand::Right(100.0), ScalarOperand::Right(f64::NAN)] {
        let filter = Comparison::Filter(ComparisonOp::Greater);
        assert!(
            map_compare(&schema, &input, filter, scalar)
                .unwrap()
                .is_none(),
            "nothing passes, so the filter yields no series"
        );
    }
}

#[test]
fn filter_with_left_scalar_keeps_the_vector_value() {
    let schema = Arc::new(schema(&["value"]));
    let input = series(
        &schema,
        5,
        vec![floats(&[Some(1.0), Some(3.0), None, Some(4.0)])],
    );
    let output = mapped(
        map_compare(
            &schema,
            &input,
            Comparison::Filter(ComparisonOp::Less),
            ScalarOperand::Left(2.0),
        )
        .unwrap(),
    );
    // Position 6 holds 3.0 and position 8 holds 4.0; position 7 failed the filter, so it costs no
    // row and the result is two runs rather than one padded stretch.
    assert_eq!(run_ranges(&output), vec![(6, 1), (8, 1)]);
    assert_eq!(
        run_floats(&output, 0),
        vec![((6, 1), vec![Some(3.0)]), ((8, 1), vec![Some(4.0)])]
    );
}

#[test]
fn bool_emits_every_present_field_and_drops_the_name() {
    let schema = Arc::new(schema(&["a", "b"]));
    let input = series(
        &schema,
        4,
        vec![
            floats(&[Some(-0.0), None, Some(2.0)]),
            Samples::try_from_chunks(vec![floats(&[Some(0.0)]), floats(&[Some(f64::NAN), None])])
                .unwrap(),
        ],
    );
    let output = mapped(
        map_compare(
            &schema,
            &input,
            Comparison::Bool(ComparisonOp::Equal),
            ScalarOperand::Right(0.0),
        )
        .unwrap(),
    );
    // Every position keeps a value in at least one field, so the result is one run; a field
    // missing where the other has a value keeps a null row.
    assert_eq!(run_ranges(&output), vec![(4, 3)]);
    assert_eq!(
        run_floats(&output, 0),
        vec![((4, 3), vec![Some(1.0), None, Some(0.0)])]
    );
    assert_eq!(
        run_floats(&output, 1),
        vec![((4, 3), vec![Some(1.0), Some(0.0), None])]
    );
    assert!(output.metric.name().is_none());
    assert_eq!(output.metric.tags(), &input.metric.tags()[1..]);
}

#[test]
fn scalar_operations_produce_unlabelled_short_ranges() {
    let schema = schema(&["value"]);
    let range = GridRange::try_new(schema.eval_grid.clone().unwrap(), 8, 3).unwrap();
    let output =
        scalar_arithmetic(&schema, range.clone(), ArithmeticOp::Power, [0.0, -1.0]).unwrap();
    assert_floats(output.fields.get(0).unwrap(), &[Some(f64::INFINITY); 3]);
    assert!(output.metric.name().is_none() && output.metric.tags().is_empty());
    assert!(matches!(&output.time, TimeAxis::Grid(actual) if actual == &range));
    let output = scalar_compare(
        &schema,
        range.clone(),
        ComparisonOp::NotEqual,
        [f64::NAN, f64::NAN],
    )
    .unwrap();
    assert_floats(output.fields.get(0).unwrap(), &[Some(1.0); 3]);
    assert!(output.metric.name().is_none() && output.metric.tags().is_empty());
    assert!(matches!(&output.time, TimeAxis::Grid(actual) if actual == &range));
}

#[test]
fn comparisons_reject_raw_axes_and_late_histogram_chunks() {
    let schema = Arc::new(schema(&["value"]));
    let unsupported = Samples::try_from_chunks(vec![
        floats(&[Some(1.0)]),
        Samples::try_from_histogram(UInt32Array::from(vec![0]), histogram_batch()).unwrap(),
    ])
    .unwrap();
    let input = series(&schema, 3, vec![unsupported]);
    // A segment of another type is rejected even when it holds no present sample.
    let all_missing = Samples::try_from_chunks(vec![
        floats(&[Some(1.0)]),
        Samples::try_from_histogram(UInt32Array::from(vec![None]), histogram_batch()).unwrap(),
    ])
    .unwrap();
    let all_missing = series(&schema, 3, vec![all_missing]);
    let mut raw_schema = schema.as_ref().clone();
    raw_schema.eval_grid = None;
    let raw_schema = Arc::new(raw_schema);
    let raw = Series::try_new(
        &raw_schema,
        input.metric.clone(),
        TimeAxis::try_new(vec![2500, 3000].into()).unwrap(),
        SeriesFields::one(floats(&[Some(1.0), None])),
    )
    .unwrap();
    for comparison in [
        Comparison::Bool(ComparisonOp::Equal),
        Comparison::Filter(ComparisonOp::Equal),
    ] {
        for input in [&input, &all_missing] {
            assert!(matches!(
                map_compare(&schema, input, comparison, ScalarOperand::Right(1.0)),
                Err(DataFusionError::NotImplemented(_))
            ));
        }
        assert!(matches!(
            map_compare(&raw_schema, &raw, comparison, ScalarOperand::Right(1.0)),
            Err(DataFusionError::Execution(_))
        ));
    }
}

/// F: a filter over a series of several runs keeps a scattered subset, and the result is a
/// fragmented axis of exactly the kept runs -- not a padded envelope from the first kept position
/// to the last.
///
/// The input carries positions 0, 1, 4, 7 and 9 of a twelve-position grid. Field 0 passes at 0, 4
/// and 9; field 1 passes at 1 and 7. Positions 0 and 1 are one run, so the result's runs are
/// `[(0, 2), (4, 1), (7, 1), (9, 1)]`.
#[test]
fn a_filter_over_runs_keeps_exactly_the_runs_that_pass() {
    use datatypes::series::time::GridSpan;

    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 12).unwrap()).unwrap());
    let schema = Arc::new(SeriesSchema {
        fields: vec![
            FieldSpec {
                name: Arc::from("a"),
            },
            FieldSpec {
                name: Arc::from("b"),
            },
        ]
        .into(),
        eval_grid: Some(grid),
    });
    let labels =
        crate::grid_series::reference::Labels::from([("host".to_string(), "a".to_string())]);
    // Both fields carry the same row positions, so one `Points` map builds the fixture and each
    // field gets its own values.
    let mut a = crate::grid_series::reference::Points::new();
    let mut b = crate::grid_series::reference::Points::new();
    // Only 0 and 9 pass in field 0 and only 4 in field 1; 1 and 7 fail in both, so they are the
    // positions a filter must drop rather than pad over.
    for (position, (first, second)) in [
        (0, (5.0, 1.0)),
        (1, (1.0, 1.0)),
        (4, (1.0, 8.0)),
        (7, (1.0, 1.0)),
        (9, (6.0, 1.0)),
    ] {
        a.insert(position, f64::to_bits(first));
        b.insert(position, f64::to_bits(second));
    }
    // `sparse_series` builds one field, so the fixture is assembled by hand: one axis over the
    // five run-forming positions, and one field per input column.
    let time = TimeAxis::from_grid_spans(
        schema.eval_grid.clone().unwrap(),
        vec![
            GridSpan::new(0, 2),
            GridSpan::new(4, 1),
            GridSpan::new(7, 1),
            GridSpan::new(9, 1),
        ],
    )
    .expect("the spans are ordered and inside the grid");
    let field = |points: &crate::grid_series::reference::Points| {
        Samples::from_float(Float64Array::from(
            points
                .values()
                .map(|bits| f64::from_bits(*bits))
                .collect::<Vec<_>>(),
        ))
    };
    let metric = crate::grid_series::reference::metric_of(&labels);
    let input = Series::try_new(
        &schema,
        metric.clone(),
        time,
        SeriesFields::try_from_vec(vec![field(&a), field(&b)])
            .expect("the two fields have the same length"),
    )
    .expect("the fields are aligned with the axis");
    assert_eq!(
        crate::grid_series::reference::run_ranges(&input),
        vec![(0, 2), (4, 1), (7, 1), (9, 1)]
    );

    // Greater than 4: field 0 keeps 5 at 0 and 6 at 9; field 1 keeps 8 at 4. Positions 1 and 7
    // fail in both fields.
    let output = map_compare(
        &schema,
        &input,
        Comparison::Filter(ComparisonOp::Greater),
        ScalarOperand::Right(4.0),
    )
    .unwrap()
    .expect("positions 0, 4 and 9 keep a value in some field");
    assert_eq!(
        run_ranges(&output),
        vec![(0, 1), (4, 1), (9, 1)],
        "a filter's output is a fragmented axis of exactly the kept runs"
    );
    assert_eq!(output.time.len(), 3, "three kept positions, three rows");
    assert_eq!(output.fields.len(), 2, "both fields survived the filter");
    for (index, field) in output.fields.iter().enumerate() {
        assert_eq!(
            field.len(),
            output.time.len(),
            "field {index} is aligned with the axis"
        );
    }
    let mut expected = vec![vec![None; 2]; 12];
    expected[0] = vec![Some(5.0), None];
    expected[4] = vec![None, Some(8.0)];
    expected[9] = vec![Some(6.0), None];
    crate::grid_series::reference::assert_result_eq(Some(&output), &expected, &input.metric);
    // A padded envelope would have covered 0..=9 with rows at 1, 2, 3, 5, 6 and 8.
    assert!(
        output.time.len() < 10,
        "the result carries {} rows, an envelope would carry 10",
        output.time.len()
    );
}

/// F: a bool comparison keeps every position the input carries, so the output is the input's axis
/// again -- and a comparison against NaN is false everywhere but `!=`.
#[test]
fn a_bool_comparison_keeps_the_axis_of_a_sparse_input() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, 10).unwrap()).unwrap());
    let schema = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: Some(grid),
    });
    let labels =
        crate::grid_series::reference::Labels::from([("host".to_string(), "a".to_string())]);
    let mut points = crate::grid_series::reference::Points::new();
    for (position, value) in [(1, 1.0), (2, 20.0), (6, 3.0)] {
        points.insert(position, f64::to_bits(value));
    }
    let input = crate::grid_series::reference::sparse_series(&schema, &labels, &points)
        .expect("three positions hold a value");
    assert_eq!(
        crate::grid_series::reference::run_ranges(&input),
        vec![(1, 2), (6, 1)]
    );

    let output = map_compare(
        &schema,
        &input,
        Comparison::Bool(ComparisonOp::Greater),
        ScalarOperand::Right(10.0),
    )
    .unwrap()
    .expect("a bool comparison holds a value at every carried position");
    assert_eq!(
        run_ranges(&output),
        vec![(1, 2), (6, 1)],
        "a bool comparison emits 0/1, so it keeps every carried position"
    );
    assert_eq!(output.time.len(), 3);
    let mut expected = vec![vec![None]; 10];
    expected[1] = vec![Some(0.0)];
    expected[2] = vec![Some(1.0)];
    expected[6] = vec![Some(0.0)];
    // A bool comparison drops only `__name__`, so the input's other labels stay.
    crate::grid_series::reference::assert_result_eq(
        Some(&output),
        &expected,
        &MetricName::try_new(
            None,
            vec![Label {
                name: "host".into(),
                value: "a".into(),
            }],
        )
        .unwrap(),
    );
}
