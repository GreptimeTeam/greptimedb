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

fn grid_range(series: &Series) -> (usize, usize) {
    let TimeAxis::Grid(range) = &series.time else {
        panic!("lost grid axis")
    };
    (range.start(), range.len())
}

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
    let output = map_compare(
        &schema,
        &input,
        Comparison::Filter(ComparisonOp::Greater),
        ScalarOperand::Right(4.0),
    )
    .unwrap()
    .unwrap();
    assert_eq!(grid_range(&output), (5, 4));
    assert_floats(
        output.fields.get(0).unwrap(),
        &[None, Some(5.0), None, Some(7.0)],
    );
    assert_floats(
        output.fields.get(1).unwrap(),
        &[Some(9.0), None, None, None],
    );
    assert_eq!(output.metric, input.metric);
    for scalar in [ScalarOperand::Right(100.0), ScalarOperand::Right(f64::NAN)] {
        let filter = Comparison::Filter(ComparisonOp::Greater);
        assert!(
            map_compare(&schema, &input, filter, scalar)
                .unwrap()
                .is_none()
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
    let output = map_compare(
        &schema,
        &input,
        Comparison::Filter(ComparisonOp::Less),
        ScalarOperand::Left(2.0),
    )
    .unwrap()
    .unwrap();
    assert_eq!(grid_range(&output), (6, 3));
    assert_floats(output.fields.get(0).unwrap(), &[Some(3.0), None, Some(4.0)]);
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
    let output = map_compare(
        &schema,
        &input,
        Comparison::Bool(ComparisonOp::Equal),
        ScalarOperand::Right(0.0),
    )
    .unwrap()
    .unwrap();
    assert_eq!(grid_range(&output), (4, 3));
    assert_floats(output.fields.get(0).unwrap(), &[Some(1.0), None, Some(0.0)]);
    assert_floats(output.fields.get(1).unwrap(), &[Some(1.0), Some(0.0), None]);
    assert_eq!(output.metric.name(), None);
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
