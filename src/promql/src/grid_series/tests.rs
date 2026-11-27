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

use std::mem::size_of;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, UInt32Array};
use datatypes::series::histogram::{
    CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, HistogramSamples, SampleRef, Samples, Series, SeriesFields,
    SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::reference::{
    Labels, OwnedHistogram, Points, assert_pieces_eq, assert_result_eq, metric_of, points_of,
    random_dense, run_ranges, series_of, stitch,
};
use super::{GridSample, GridSeriesBuilder, merge_same_labelset};
use crate::histogram_eval::float_histogram::FloatHistogram;

fn grid_schema(fields: usize, grid_len: u64) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: (0..fields)
            .map(|index| FieldSpec {
                name: Arc::from(format!("f{index}")),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(-5_000, 1_000, grid_len).unwrap()).unwrap(),
        )),
    })
}

fn metric(host: usize) -> SeriesLabels {
    SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(format!("h{host}")),
        }],
    )
    .unwrap()
}

#[test]
fn the_series_of_one_push_sequence_matches_the_pushed_rows() {
    for seed in 0..200 {
        let mut rng = StdRng::seed_from_u64(seed);
        let fields = rng.random_range(1..=3);
        let grid_len = rng.random_range(1..64);
        let mut builder = GridSeriesBuilder::try_new(grid_schema(fields, grid_len)).unwrap();
        for host in 0..3 {
            builder.begin_series();
            let expected = random_dense(&mut rng, grid_len as usize, fields);
            for (index, row) in expected.iter().enumerate() {
                // Missing rows may be pushed as all-None or skipped entirely.
                if row.iter().any(Option::is_some) || rng.random_bool(0.5) {
                    builder.push(index, row).unwrap();
                }
            }
            let series = builder.finish(metric(host)).unwrap();
            assert_pieces_eq(series.as_slice(), &expected, &metric(host));
        }
    }
}

#[test]
fn builder_rejects_invalid_rows() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(2, 4)).unwrap();
    builder.push(1, &[Some(1.0), None]).unwrap();
    assert!(builder.push(1, &[Some(2.0), None]).is_err());
    assert!(builder.push(0, &[Some(2.0), None]).is_err());
    assert!(builder.push(4, &[Some(2.0), None]).is_err());
    assert!(builder.push(2, &[Some(2.0)]).is_err());

    let mut schema = grid_schema(1, 4).as_ref().clone();
    schema.eval_grid = None;
    assert!(GridSeriesBuilder::try_new(Arc::new(schema)).is_err());
}

#[test]
fn merge_treats_empty_label_values_as_absent() {
    let schema = grid_schema(1, 2);
    let labels = |host: Option<&str>| {
        let mut labels = Labels::from([("job".to_string(), "x".to_string())]);
        if let Some(host) = host {
            labels.insert("host".to_string(), host.to_string());
        }
        labels
    };
    let series = vec![
        series_of(
            &schema,
            &labels(Some("")),
            &Points::from([(0, 1f64.to_bits())]),
        )
        .unwrap(),
        series_of(&schema, &labels(None), &Points::from([(1, 2f64.to_bits())])).unwrap(),
    ];
    let merged = merge_same_labelset(&schema, series).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        points_of(&merged[0]),
        Points::from([(0, 1f64.to_bits()), (1, 2f64.to_bits())])
    );
}

#[test]
fn merge_keeps_every_row_of_many_disjoint_members() {
    const MEMBERS: usize = 64;
    let schema = grid_schema(1, (2 * MEMBERS) as u64);
    let series = (0..MEMBERS)
        .map(|index| {
            let labels = Labels::from([
                ("job".to_string(), "x".to_string()),
                ("instance".to_string(), index.to_string()),
            ]);
            series_of(
                &schema,
                &labels,
                &Points::from([(2 * index, (index as f64 + 1.0).to_bits())]),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    // Dropping `instance` makes every member share one label set.
    let stripped = series
        .into_iter()
        .map(|mut series| {
            series.metric.remove_all(&["instance"]);
            series
        })
        .collect::<Vec<_>>();

    let merged = merge_same_labelset(&schema, stripped).unwrap();
    assert_eq!(merged.len(), 1, "one label set, so one merged series");
    let expected = (0..MEMBERS)
        .map(|index| (2 * index, (index as f64 + 1.0).to_bits()))
        .collect::<Points>();
    assert_eq!(
        points_of(&merged[0]),
        expected,
        "every member's row survives the merge with its own value"
    );
    assert_eq!(
        merged[0].time.len(),
        MEMBERS,
        "the merged axis carries one row per member, not the span"
    );
}

#[test]
fn merge_combines_disjoint_fields_at_one_point() {
    let schema = grid_schema(2, 1);
    let series = |row: [Option<f64>; 2]| {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        builder.push(0, &row).unwrap();
        builder
            .finish(metric(0))
            .unwrap()
            .expect("the push retained one row")
    };
    let merged = merge_same_labelset(
        &schema,
        vec![series([Some(1.0), None]), series([None, Some(2.0)])],
    )
    .unwrap();
    assert_eq!(merged.len(), 1);
    assert_result_eq(
        Some(&merged[0]),
        &vec![vec![Some(1.0), Some(2.0)]],
        &metric(0),
    );
    assert!(
        merge_same_labelset(
            &schema,
            vec![series([Some(1.0), None]), series([Some(2.0), None])]
        )
        .is_err()
    );
}

#[test]
fn merge_keeps_histograms_and_rejects_any_doubled_field() {
    #[derive(Clone, Copy)]
    enum Value {
        Float(f64),
        Histogram(usize),
    }

    let schema = grid_schema(2, 3);
    let histograms = [
        OwnedHistogram::parse("0/0/0/3ff0000000000000/3ff0000000000000/0/1_1/3ff0000000000000///"),
        OwnedHistogram::parse("0/0/0/4000000000000000/4008000000000000/1/0_1/4000000000000000///"),
    ];
    let histogram =
        |index: usize| FloatHistogram::try_from_view(&histograms[index].view()).unwrap();
    let sample = |value: Option<Value>| {
        value.map(|value| match value {
            Value::Float(float) => GridSample::Float(float),
            Value::Histogram(index) => GridSample::Histogram(Box::new(histogram(index))),
        })
    };
    let series = |rows: &[(usize, [Option<Value>; 2])]| {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        for &(point, row) in rows {
            builder
                .push_samples(point, &[sample(row[0]), sample(row[1])])
                .unwrap();
        }
        builder.finish(metric(0)).unwrap().unwrap()
    };
    let describe = |series: &Series, field: usize| -> Vec<(usize, String)> {
        let samples = &series.fields.as_slice()[field];
        series
            .time
            .positions()
            .filter_map(|(point, row)| match samples.sample(row) {
                SampleRef::Missing => None,
                SampleRef::Float(value) => Some((point, format!("{value}"))),
                SampleRef::Histogram(view) => Some((
                    point,
                    format!("{:?}", FloatHistogram::try_from_view(&view).unwrap()),
                )),
            })
            .collect()
    };

    let merged = merge_same_labelset(
        &schema,
        vec![
            series(&[
                (0, [Some(Value::Histogram(0)), None]),
                (2, [Some(Value::Float(1.0)), None]),
            ]),
            series(&[
                (0, [None, Some(Value::Float(2.0))]),
                (1, [Some(Value::Histogram(1)), None]),
            ]),
        ],
    )
    .unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        describe(&merged[0], 0),
        [
            (0, format!("{:?}", histogram(0))),
            (1, format!("{:?}", histogram(1))),
            (2, "1".to_string()),
        ]
    );
    assert_eq!(describe(&merged[0], 1), [(0, "2".to_string())]);

    for second in [Value::Float(3.0), Value::Histogram(1)] {
        let error = merge_same_labelset(
            &schema,
            vec![
                series(&[(1, [Some(Value::Histogram(0)), None])]),
                series(&[(1, [Some(second), None])]),
            ],
        )
        .unwrap_err();
        assert!(error.to_string().contains("same labelset"), "{error}");
    }
}

/// Each shard holds its own histogram table; merged rows must still name the payload their shard
/// pushed.
#[test]
fn merge_keeps_each_shards_histogram_payload() {
    let payloads = [
        OwnedHistogram::parse(
            "0/0/0/3ff0000000000000/3ff0000000000000/1/0_2/4000000000000000,4008000000000000///",
        ),
        OwnedHistogram::parse("0/0/0/4008000000000000/4014000000000000/1/1_1/4010000000000000///"),
        OwnedHistogram::parse("0/0/0/4014000000000000/4018000000000000/1/0_1/4014000000000000///"),
    ];
    // The `(sum, count)` each payload was written with, in payload order.
    let scalars = [(1.0, 1.0), (5.0, 3.0), (6.0, 5.0)];
    let schema = grid_schema(1, 8);
    let shard = |rows: &[(usize, usize)]| {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        for &(point, payload) in rows {
            let histogram = FloatHistogram::try_from_view(&payloads[payload].view()).unwrap();
            builder
                .push_samples(point, &[Some(GridSample::Histogram(Box::new(histogram)))])
                .unwrap();
        }
        builder
            .finish(metric(0))
            .unwrap()
            .expect("the shard holds the rows it pushed")
    };
    assert!(
        GridSeriesBuilder::try_new(schema.clone())
            .unwrap()
            .finish(metric(0))
            .unwrap()
            .is_none(),
        "an empty shard handed out a series"
    );
    let merged = merge_same_labelset(
        &schema,
        vec![shard(&[(0, 0), (3, 1)]), shard(&[(1, 1)]), shard(&[(4, 2)])],
    )
    .unwrap();
    assert_eq!(merged.len(), 1, "one label set, so one merged series");
    let field = merged[0].fields.get(0).expect("one field");
    let merged_rows = merged[0]
        .time
        .positions()
        .map(|(point, row)| {
            let SampleRef::Histogram(view) = field.sample(row) else {
                panic!("position {point} of the merged series is not a histogram");
            };
            let histogram = FloatHistogram::try_from_view(&view).unwrap();
            (point, histogram.sum, histogram.count)
        })
        .collect::<Vec<_>>();
    // Position 2 is a hole in every shard.
    assert_eq!(
        vec![
            (0, scalars[0].0, scalars[0].1),
            (1, scalars[1].0, scalars[1].1),
            (3, scalars[1].0, scalars[1].1),
            (4, scalars[2].0, scalars[2].1),
        ],
        merged_rows
    );
    let SampleRef::Histogram(first) = field.sample(0) else {
        panic!("the merged field holds no histogram");
    };
    assert_eq!(&[2.0, 3.0], first.positive.buckets);
    let SampleRef::Histogram(second) = field.sample(1) else {
        panic!("the merged field holds no histogram");
    };
    assert_eq!(&[4.0], second.positive.buckets);
    assert_eq!(1, second.positive.span_lengths.len());
    assert_eq!(1, second.positive.span_offsets[0]);
}

#[test]
fn runs_never_bridge_positions_without_a_value() {
    const SPAN: usize = 1_000_000;
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, SPAN as u64)).unwrap();
    builder.push(10, &[Some(1.0)]).unwrap();
    builder.push(900_000, &[Some(2.0)]).unwrap();
    assert!(builder.has_pending());

    let series = builder.finish(metric(0)).unwrap().expect("two rows");
    assert_eq!(
        series.time.grid().expect("a grid series").spec().len as usize,
        SPAN,
        "the grid really holds a million positions"
    );
    assert_eq!(run_ranges(&series), vec![(10, 1), (900_000, 1)]);
    assert!(
        !series.time.is_contiguous(),
        "two far-apart rows are two runs"
    );
    assert_eq!(series.time.len(), 2, "two values, two rows");
    assert_eq!(series.fields.len(), 1);
    assert_eq!(
        series.fields.as_slice()[0].len(),
        2,
        "the one field holds a row per value"
    );
    let values = match series.fields.as_slice()[0].dense() {
        Some(DenseSamplesRef::Float(values)) => values,
        _ => panic!("expected float samples"),
    };
    assert_eq!(values.len(), 2, "the value buffer holds one row per value");
    assert_eq!(values.values().as_ref(), &[1.0, 2.0]);
    assert!(
        std::mem::size_of_val(values.values().as_ref()) < SPAN / 1000,
        "the value buffer is sized by the span, not by the values"
    );
    assert_eq!(
        (series.time.timestamp(0), series.time.timestamp(1)),
        (Some(-5_000 + 10 * 1_000), Some(-5_000 + 900_000 * 1_000))
    );
    assert!(!builder.has_pending());
}

#[test]
fn a_prefix_flush_hands_out_the_whole_buffer_and_ends_nothing() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    builder.push(0, &[Some(1.0)]).unwrap();
    builder.push(5, &[Some(2.0)]).unwrap();
    builder.push(9, &[Some(3.0)]).unwrap();

    let prefix = builder
        .take_buffered(metric(0))
        .unwrap()
        .expect("three rows");
    assert_eq!(run_ranges(&prefix), vec![(0, 1), (5, 1), (9, 1)]);
    assert!(!builder.has_pending());
    // The index cursor survives a flush; only `begin_series` resets it.
    assert!(builder.take_buffered(metric(0)).unwrap().is_none());
    assert!(builder.push(9, &[Some(4.0)]).is_err());
    assert!(builder.finish(metric(0)).unwrap().is_none());

    builder.begin_series();
    builder.push(0, &[Some(7.0)]).unwrap();
    let next = builder.finish(metric(1)).unwrap().expect("one row");
    assert_eq!(run_ranges(&next), vec![(0, 1)]);
}

#[test]
fn an_all_missing_push_holds_no_row() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    builder.push(2, &[Some(1.0)]).unwrap();
    builder.push(3, &[None]).unwrap();
    builder.push(4, &[Some(2.0)]).unwrap();
    builder.push(7, &[None]).unwrap();

    let series = builder.finish(metric(0)).unwrap().expect("two rows");
    assert_eq!(run_ranges(&series), vec![(2, 1), (4, 1)]);
    assert!(!builder.has_pending());
}

#[test]
fn rows_stay_aligned_across_fields() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(2, 4)).unwrap();
    builder.push(1, &[Some(1.0), None]).unwrap();
    builder.push(2, &[None, Some(2.0)]).unwrap();
    let series = builder.finish(metric(0)).unwrap().expect("two rows");
    assert_eq!(
        run_ranges(&series),
        vec![(1, 2)],
        "complementary fields at adjacent positions are ONE run"
    );
    assert_eq!(
        stitch(std::slice::from_ref(&series)),
        vec![
            vec![None, None],
            vec![Some(1.0), None],
            vec![None, Some(2.0)],
            vec![None, None],
        ]
    );
}

#[test]
fn a_multi_run_series_keeps_one_row_per_value() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 100)).unwrap();
    for (offset, index) in [0usize, 1, 2, 50, 51, 99].into_iter().enumerate() {
        builder.push(index, &[Some(offset as f64)]).unwrap();
    }
    let series = builder.finish(metric(0)).unwrap().expect("six rows");
    assert_eq!(run_ranges(&series), vec![(0, 3), (50, 2), (99, 1)]);

    let values = match series.fields.as_slice()[0].dense() {
        Some(DenseSamplesRef::Float(values)) => values,
        _ => panic!("expected float samples"),
    };
    assert_eq!(values.len(), 6);
    assert_eq!(values.values().as_ref(), &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0]);
    assert_eq!(
        std::mem::size_of_val(values.values().as_ref()),
        6 * size_of::<f64>()
    );
    let mut cursor = series.fields.as_slice()[0].cursor();
    let read: Vec<(usize, f64)> = series
        .time
        .positions()
        .map(|(position, row)| match cursor.at(row).expect("aligned") {
            SampleRef::Float(value) => (position, value),
            _ => panic!("expected a float"),
        })
        .collect();
    assert_eq!(
        read,
        vec![
            (0, 0.0),
            (1, 1.0),
            (2, 2.0),
            (50, 3.0),
            (51, 4.0),
            (99, 5.0)
        ]
    );
}

#[test]
fn skipped_and_all_missing_positions_are_the_same_series() {
    let mut skipped = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    let mut pushed = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    for index in 0..10 {
        let value = [0, 1, 5].contains(&index).then_some(index as f64);
        if value.is_some() {
            skipped.push(index, &[value]).unwrap();
        }
        pushed.push(index, &[value]).unwrap();
    }
    let skipped = skipped.finish(metric(0)).unwrap().expect("three rows");
    let pushed = pushed.finish(metric(0)).unwrap().expect("three rows");
    assert_eq!(run_ranges(&skipped), run_ranges(&pushed));
    assert_eq!(
        stitch(std::slice::from_ref(&skipped)),
        stitch(std::slice::from_ref(&pushed))
    );
}

#[test]
fn one_builder_serves_several_series() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    builder.push(0, &[Some(1.0)]).unwrap();
    builder.push(2, &[Some(2.0)]).unwrap();
    let first = builder.finish(metric(0)).unwrap().expect("two rows");
    assert_eq!(run_ranges(&first), vec![(0, 1), (2, 1)]);
    assert_eq!(first.metric, metric(0));

    assert!(
        builder.push(1, &[Some(3.0)]).is_err(),
        "a series ends where the positions restart"
    );
    builder.begin_series();
    builder.push(1, &[Some(3.0)]).unwrap();
    builder.push(3, &[Some(4.0)]).unwrap();
    let second = builder.finish(metric(1)).unwrap().expect("two rows");
    assert_eq!(run_ranges(&second), vec![(1, 1), (3, 1)]);
    assert_eq!(second.metric, metric(1));
    assert!(!builder.has_pending());
}

// Merging fragments of one label set: payloads are shared, not rebuilt.

/// The start of a dense float segment's buffer, to check a merge shared it.
fn float_ptr(segment: &Samples) -> *const f64 {
    match segment.dense() {
        Some(DenseSamplesRef::Float(values)) => values.values().as_ptr(),
        _ => panic!("expected a dense float segment"),
    }
}

type FragmentRow<'a> = (usize, Option<f64>, Option<&'a str>, Option<f64>);

/// A two-field fragment from `(position, field 0 float, field 0 histogram, field 1 float)` rows in
/// ascending position order.
fn fragment_with_histograms(
    schema: &Arc<SeriesSchema>,
    labels: &Labels,
    rows: &[FragmentRow<'_>],
) -> (Series, Arc<HistogramBatch>) {
    let mut batch = HistogramBatchBuilder::new();
    let mut spans: Vec<GridSpan> = Vec::new();
    let mut floats = Vec::with_capacity(rows.len());
    let mut indices = Vec::with_capacity(rows.len());
    let mut seconds = Vec::with_capacity(rows.len());
    for &(position, float, histogram, second) in rows {
        match spans.last_mut() {
            Some(span) if span.start() + span.len() == position => {
                *span = GridSpan::new(span.start(), span.len() + 1);
            }
            _ => spans.push(GridSpan::new(position, 1)),
        }
        floats.push(float);
        indices.push(histogram.map(|text| {
            batch
                .push(OwnedHistogram::parse(text).view())
                .expect("the fixture histogram is valid")
        }));
        seconds.push(second);
    }
    let batch = batch.finish().expect("an empty histogram table is valid");
    let histograms = HistogramSamples::try_new(UInt32Array::from(indices), batch.clone()).unwrap();
    let mixed = Samples::try_from_mixed(Float64Array::from(floats), histograms).unwrap();
    let fields = SeriesFields::try_from_vec(vec![
        mixed,
        Samples::from_float(Float64Array::from(seconds)),
    ])
    .unwrap();
    let time = TimeAxis::from_grid_spans(schema.eval_grid.clone().unwrap(), spans).unwrap();
    (
        Series::try_new(schema, metric_of(labels), time, fields).unwrap(),
        batch,
    )
}

#[test]
fn merge_of_adjacent_fragments_is_one_run() {
    let schema = grid_schema(1, 4);
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let first = series_of(&schema, &labels, &Points::from([(0, 1f64.to_bits())])).unwrap();
    let second = series_of(
        &schema,
        &labels,
        &Points::from([(1, 2f64.to_bits()), (2, 3f64.to_bits())]),
    )
    .unwrap();
    let first_ptr = float_ptr(&first.fields.as_slice()[0].segments()[0]);
    let second_ptr = float_ptr(&second.fields.as_slice()[0].segments()[0]);

    let merged = merge_same_labelset(&schema, vec![first, second]).unwrap();
    assert_eq!(merged.len(), 1, "one label set, one merged series");
    assert_eq!(merged[0].time.len(), 3, "one row per value");
    assert_eq!(
        run_ranges(&merged[0]),
        vec![(0, 3)],
        "fragments holding adjacent positions share one run"
    );
    assert_eq!(
        points_of(&merged[0]),
        Points::from([
            (0, 1f64.to_bits()),
            (1, 2f64.to_bits()),
            (2, 3f64.to_bits()),
        ])
    );
    // One slice per fragment, neither copied.
    let chunks = merged[0].fields.as_slice()[0].segments();
    assert_eq!(chunks.len(), 2, "one slice per fragment");
    assert_eq!(float_ptr(&chunks[0]), first_ptr);
    assert_eq!(float_ptr(&chunks[1]), second_ptr);
    assert_eq!(chunks[1].len(), 2, "the fragment's two rows are one slice");
}

#[test]
fn merge_keeps_native_histograms_and_complementary_fields_without_copying_them() {
    let schema = grid_schema(2, 4);
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let histogram = "0/0/0/3ff0000000000000/3ff0000000000000/0/1_1/3ff0000000000000///";
    let (first, first_batch) =
        fragment_with_histograms(&schema, &labels, &[(0, None, Some(histogram), Some(1.0))]);
    let (second, _) = fragment_with_histograms(
        &schema,
        &labels,
        &[(1, Some(2.0), None, None), (2, None, None, Some(f64::NAN))],
    );
    let second_field1 = float_ptr(&second.fields.as_slice()[1].segments()[0]);

    let merged = merge_same_labelset(&schema, vec![first, second]).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        run_ranges(&merged[0]),
        vec![(0, 3)],
        "positions 0, 1 and 2 are one run"
    );
    assert_eq!(merged[0].time.len(), 3);
    assert_eq!(merged[0].fields.len(), 2);

    let field0 = merged[0].fields.as_slice()[0].segments();
    assert_eq!(field0.len(), 3, "a histogram, a float, and one null run");
    let DenseSamplesRef::Mixed(mixed) = field0[0].dense().unwrap() else {
        panic!("the histogram lost its mixed column")
    };
    assert!(
        Arc::ptr_eq(mixed.histograms().batch(), &first_batch),
        "the histogram table was rebuilt"
    );
    assert!(
        matches!(field0[0].sample(0), SampleRef::Histogram(view) if view.count == 1.0),
        "the histogram did not survive as a histogram"
    );
    assert!(
        matches!(field0[1].sample(0), SampleRef::Float(value) if value.to_bits() == 2f64.to_bits()),
        "field 0 lost the other fragment's float"
    );
    assert!(
        matches!(field0[2].sample(0), SampleRef::Missing),
        "field 0 filled a position only field 1 holds"
    );

    let field1 = merged[0].fields.as_slice()[1].segments();
    assert_eq!(field1.len(), 3, "a float, one null run, and a NaN");
    assert!(
        matches!(field1[0].sample(0), SampleRef::Float(value) if value.to_bits() == 1f64.to_bits())
    );
    assert!(matches!(field1[1].sample(0), SampleRef::Missing));
    assert!(
        matches!(field1[2].sample(0), SampleRef::Float(value) if value.is_nan()),
        "the NaN was dropped"
    );
    assert_eq!(
        float_ptr(&field1[2]),
        second_field1.wrapping_add(1),
        "the NaN was copied instead of sliced"
    );

    let at_zero =
        || fragment_with_histograms(&schema, &labels, &[(0, None, Some(histogram), None)]).0;
    assert!(
        merge_same_labelset(&schema, vec![at_zero(), at_zero()]).is_err(),
        "two histograms of one field at one position merged"
    );
}

#[test]
fn merge_of_many_segments_keeps_one_row_per_value() {
    const GRID: usize = 1_000_000;
    const SEGMENTS: usize = 1_000;
    let schema = grid_schema(1, GRID as u64);
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let fragment = |offset: usize| {
        let mut chunks = Vec::with_capacity(SEGMENTS);
        let mut spans = Vec::with_capacity(SEGMENTS);
        for segment in 0..SEGMENTS {
            let position = offset + segment * (GRID / SEGMENTS);
            chunks.push(Samples::from_float(Float64Array::from(vec![
                position as f64,
            ])));
            spans.push(GridSpan::new(position, 1));
        }
        let time = TimeAxis::from_grid_spans(schema.eval_grid.clone().unwrap(), spans).unwrap();
        let fields = SeriesFields::one(Samples::try_from_chunks(chunks).unwrap());
        Series::try_new(&schema, metric_of(&labels), time, fields).unwrap()
    };

    let merged = merge_same_labelset(&schema, vec![fragment(0), fragment(1)]).unwrap();
    assert_eq!(merged.len(), 1);
    let series = &merged[0];
    assert_eq!(
        series.time.grid().unwrap().spec().len as usize,
        GRID,
        "the grid really holds a million positions"
    );
    assert_eq!(series.time.len(), 2 * SEGMENTS, "one row per value");
    assert_eq!(
        series.time.runs().count(),
        SEGMENTS,
        "two adjacent values per segment are one run, and the holes between are spans"
    );
    assert_eq!(run_ranges(series)[0], (0, 2));
    let field = &series.fields.as_slice()[0];
    assert_eq!(
        field.len(),
        2 * SEGMENTS,
        "the field holds one sample per merged row"
    );
    assert_eq!(
        field.segments().len(),
        2 * SEGMENTS,
        "one chunk per value: no hole was densified into the payload"
    );
    assert!(field.segments().iter().all(|chunk| chunk.len() == 1));
    assert_eq!(
        points_of(series).len(),
        2 * SEGMENTS,
        "every value survived and no hole became one"
    );
}

#[test]
fn merge_of_empty_fragments_yields_one_empty_series() {
    let schema = grid_schema(2, 4);
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let empty = || {
        let time =
            TimeAxis::from_grid_spans(schema.eval_grid.clone().unwrap(), Vec::new()).unwrap();
        let fields = SeriesFields::try_from_vec(vec![
            Samples::from_float(Float64Array::new_null(0)),
            Samples::from_float(Float64Array::new_null(0)),
        ])
        .unwrap();
        Series::try_new(&schema, metric_of(&labels), time, fields).unwrap()
    };

    let merged = merge_same_labelset(&schema, vec![empty(), empty()]).unwrap();
    assert_eq!(merged.len(), 1, "the label set vanished");
    assert_eq!(merged[0].metric, metric_of(&labels));
    assert_eq!(merged[0].time.len(), 0);
    assert!(
        merged[0].time.grid().is_some(),
        "an empty series keeps its grid"
    );
    assert!(
        run_ranges(&merged[0]).is_empty(),
        "an empty series has no run"
    );
    assert_eq!(merged[0].fields.len(), 2);
    for field in merged[0].fields.iter() {
        assert!(field.is_empty(), "an empty fragment contributed a row");
    }
}

#[test]
fn merge_rejects_fragments_whose_fields_do_not_fill_their_axis() {
    let schema = grid_schema(1, 8);
    let grid = schema.eval_grid.clone().unwrap();
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let partner = || series_of(&schema, &labels, &Points::from([(1, 7f64.to_bits())])).unwrap();
    // `(axis rows, field rows)`: shorter field, longer field, and a value on an empty axis.
    for (axis_rows, field_rows) in [(2, 1), (1, 2), (0, 1)] {
        let spans = if axis_rows == 0 {
            Vec::new()
        } else {
            vec![GridSpan::new(1, axis_rows)]
        };
        let time = TimeAxis::from_grid_spans(grid.clone(), spans).unwrap();
        let values = Float64Array::from(
            (0..field_rows)
                .map(|row| row as f64 + 1.0)
                .collect::<Vec<_>>(),
        );
        let malformed = Series {
            metric: metric_of(&labels),
            time,
            fields: SeriesFields::one(Samples::from_float(values)),
        };
        let error = merge_same_labelset(&schema, vec![partner(), malformed])
            .expect_err("a fragment whose fields do not fill its axis merged");
        assert!(
            error.to_string().contains("rows of its axis"),
            "{axis_rows} axis rows with a {field_rows}-row field: {error}"
        );
    }
}

// The builder's output shape: fast path, run count, row count and field layout.

mod acceptance {
    use std::sync::Arc;

    use datafusion::arrow::array::Float64Array;
    use datatypes::series::samples::{
        DenseSamplesRef, FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
    };
    use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};

    use super::super::reference::{
        Labels, OwnedHistogram, Points, expand, metric_of, run_ranges, sparse_series, stitch,
    };
    use super::super::{GridSample, GridSeriesBuilder};
    use super::metric;

    fn schema(fields: usize, len: u64) -> Arc<SeriesSchema> {
        Arc::new(SeriesSchema {
            fields: (0..fields)
                .map(|index| FieldSpec {
                    name: Arc::from(format!("f{index}")),
                })
                .collect::<Vec<_>>()
                .into(),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(0, 10, len).unwrap()).unwrap(),
            )),
        })
    }

    fn build(
        schema: &Arc<SeriesSchema>,
        len: usize,
        value: impl Fn(usize) -> Vec<Option<f64>>,
    ) -> Option<Series> {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        for index in 0..len {
            builder.push(index, &value(index)).unwrap();
        }
        builder.finish(metric(0)).unwrap()
    }

    #[test]
    fn a_contiguous_result_is_a_grid_range_and_not_a_span_list() {
        const POINTS: u64 = 9;
        let schema = schema(1, POINTS);
        let series = build(&schema, POINTS as usize, |index| {
            vec![Some(index as f64 + 1.0)]
        })
        .expect("every position holds a value");
        let range = series.time.as_grid_range().unwrap_or_else(|| {
            panic!(
                "a contiguous result took the span path, got {:?}",
                series.time
            )
        });
        assert_eq!((range.start(), range.len()), (0, POINTS as usize));
        assert!(series.time.is_contiguous());
        assert_eq!(
            run_ranges(&series),
            vec![(0, POINTS as usize)],
            "one run over the whole span"
        );
        assert_eq!(series.time.len(), POINTS as usize);
        assert!(
            matches!(
                series.time.timestamps().unwrap(),
                std::borrow::Cow::Borrowed(_)
            ),
            "a single range lends its timestamps"
        );
    }

    #[test]
    fn alternating_values_and_holes_make_one_series_of_one_run_per_value() {
        const POINTS: usize = 16;
        let schema = schema(2, POINTS as u64);
        let series = build(&schema, POINTS, |index| {
            let value = (index % 2 == 0).then_some(index as f64);
            vec![value, value]
        })
        .expect("eight positions hold a value");

        let expected: Vec<(usize, usize)> =
            (0..POINTS).step_by(2).map(|index| (index, 1)).collect();
        assert_eq!(run_ranges(&series), expected, "one run per value");
        assert_eq!(
            series.time.runs().count(),
            POINTS / 2,
            "eight runs over sixteen positions"
        );
        assert_eq!(series.time.len(), POINTS / 2, "one row per value");
        assert_eq!(series.fields.len(), 2, "both fields survived");
        for (index, field) in series.fields.iter().enumerate() {
            assert_eq!(
                field.len(),
                POINTS / 2,
                "field {index} holds one sample per carried row"
            );
        }
        let dense = expand(&series);
        assert_eq!(dense.len(), POINTS, "the reference expands over the grid");
        assert_eq!(
            dense
                .iter()
                .enumerate()
                .filter(|(_, row)| row.iter().any(Option::is_some))
                .map(|(index, _)| index)
                .collect::<Vec<_>>(),
            (0..POINTS).step_by(2).collect::<Vec<_>>(),
            "exactly the even positions hold a value"
        );
    }

    #[test]
    fn one_series_with_holes_and_several_prefix_flushes_expand_alike() {
        const POINTS: usize = 12;
        let schema = schema(1, POINTS as u64);
        let mut holed = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        let valued = |index: usize| matches!(index, 0 | 1 | 4 | 9);
        for index in 0..POINTS {
            let value = valued(index).then_some(index as f64);
            holed.push(index, &[value]).unwrap();
        }
        let one = holed.finish(metric(0)).unwrap().expect("four values");
        assert_eq!(run_ranges(&one), vec![(0, 2), (4, 1), (9, 1)]);

        let mut flushed = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        let mut pieces = Vec::new();
        for (index, take) in [(0..5), (5..10), (10..POINTS)].into_iter().enumerate() {
            for position in take {
                let value = valued(position).then_some(position as f64);
                flushed.push(position, &[value]).unwrap();
            }
            if index == 0 {
                pieces.push(
                    flushed
                        .take_buffered(metric(0))
                        .unwrap()
                        .expect("the first prefix holds two values"),
                );
            }
        }
        pieces.push(flushed.finish(metric(0)).unwrap().expect("two more values"));
        // `stitch` panics if a flush repeated a row.
        assert_eq!(pieces.len(), 2, "two hand-outs of the one series");
        let expanded = expand(&one);
        let stitched = stitch(&pieces);
        assert_eq!(stitched, expanded, "the flushed rows moved");
        let prefix = run_ranges(&pieces[0]);
        assert_eq!(prefix, vec![(0, 2), (4, 1)]);
        assert_eq!(
            &run_ranges(&one)[..prefix.len()],
            prefix.as_slice(),
            "the prefix is the beginning of the whole series"
        );
    }

    #[test]
    fn complementary_field_holes_keep_every_field_row_aligned() {
        const POINTS: usize = 8;
        let schema = schema(2, POINTS as u64);
        let series = build(&schema, POINTS, |index| match index {
            0 => vec![Some(10.0), None],
            2 => vec![None, Some(2.5)],
            5 => vec![Some(50.0), Some(-5.0)],
            _ => vec![None, None],
        })
        .expect("three positions hold a value");
        assert_eq!(
            run_ranges(&series),
            vec![(0, 1), (2, 1), (5, 1)],
            "three runs, one per valued position"
        );
        assert_eq!(series.time.len(), 3);
        assert_eq!(series.fields.len(), 2, "both fields survived");
        for (index, field) in series.fields.iter().enumerate() {
            assert_eq!(field.len(), 3, "field {index} is row-aligned with the axis");
        }
        let mut a = series.fields.as_slice()[0].cursor();
        let mut b = series.fields.as_slice()[1].cursor();
        let observed: Vec<(usize, Vec<Option<f64>>)> = series
            .time
            .positions()
            .map(|(position, row)| {
                let read = |sample: SampleRef<'_>| match sample {
                    SampleRef::Float(value) => Some(value),
                    SampleRef::Missing => None,
                    SampleRef::Histogram(_) => panic!("unexpected histogram"),
                };
                (
                    position,
                    vec![
                        read(a.at(row).expect("field a is aligned")),
                        read(b.at(row).expect("field b is aligned")),
                    ],
                )
            })
            .collect();
        assert_eq!(
            observed,
            vec![
                (0, vec![Some(10.0), None]),
                (2, vec![None, Some(2.5)]),
                (5, vec![Some(50.0), Some(-5.0)]),
            ],
            "a present value became missing, or a field lost its row"
        );
    }

    #[test]
    fn an_ordinary_nan_is_a_present_value_and_not_a_hole() {
        const POINTS: usize = 6;
        let schema = schema(1, POINTS as u64);
        let nan = f64::NAN;
        let series = build(&schema, POINTS, |index| match index {
            0 => vec![None],
            1 => vec![Some(nan)],
            2 => vec![None],
            3 => vec![Some(3.0)],
            4 => vec![None],
            _ => vec![Some(nan)],
        })
        .expect("three positions hold a value");
        assert_eq!(
            run_ranges(&series),
            vec![(1, 1), (3, 1), (5, 1)],
            "a NaN position holds a row like any other value"
        );
        assert_eq!(series.time.len(), 3, "the NaN did not become a hole");
        let mut cursor = series.fields.as_slice()[0].cursor();
        let read: Vec<(usize, Option<f64>)> = series
            .time
            .positions()
            .map(|(position, row)| {
                let value = match cursor.at(row).expect("aligned") {
                    SampleRef::Float(value) => Some(value),
                    SampleRef::Missing => None,
                    SampleRef::Histogram(_) => panic!("unexpected histogram"),
                };
                (position, value)
            })
            .collect();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].0, 1);
        assert!(read[0].1.is_some_and(f64::is_nan), "the NaN was dropped");
        assert_eq!(read[1], (3, Some(3.0)));
        assert_eq!(read[2].0, 5);
        assert!(
            read[2].1.is_some_and(f64::is_nan),
            "the second NaN was dropped"
        );
        let dense = expand(&series);
        assert_eq!(dense.len(), POINTS);
        assert!(dense[1][0].is_some_and(f64::is_nan));
        assert_eq!(dense[2], vec![None]);
        assert_eq!(dense[3], vec![Some(3.0)]);
        assert!(dense[5][0].is_some_and(f64::is_nan));
    }

    #[test]
    fn mixed_and_histogram_fields_keep_their_sample_kinds() {
        const POINTS: usize = 8;
        let schema = schema(2, POINTS as u64);
        let text = "0/0/0/3ff0000000000000/3ff0000000000000/0/1_1/3ff0000000000000///";
        let owned = OwnedHistogram::parse(text);
        // Field 0 is mixed, field 1 all-histogram; positions 2, 4, 5 and 7 are holes.
        let rows: [(usize, Option<f64>, bool); 4] = [
            (0, Some(0.0), false),
            (1, None, true),
            (3, Some(3.0), true),
            (6, Some(6.0), false),
        ];
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        let mut expected_rows = Vec::new();
        for index in 0..POINTS {
            let row = rows.iter().find(|(position, _, _)| *position == index);
            let (Some((_, float, histogram)), _) = (row, ()) else {
                continue;
            };
            let mut values = Vec::with_capacity(2);
            values.push(float.map(GridSample::Float));
            expected_rows.push((index, float.is_some(), *histogram));
            let histogram_sample = if *histogram {
                Some(GridSample::Histogram(Box::new(
                    crate::histogram_eval::float_histogram::FloatHistogram::try_from_view(
                        &owned.view(),
                    )
                    .unwrap(),
                )))
            } else {
                None
            };
            values.push(histogram_sample);
            builder.push_samples(index, &values).unwrap();
        }
        let series = builder.finish(metric(0)).unwrap().expect("four rows");
        assert_eq!(series.fields.len(), 2, "a field was lost");
        assert_eq!(series.time.len(), expected_rows.len());
        for (index, field) in series.fields.iter().enumerate() {
            assert_eq!(
                field.len(),
                expected_rows.len(),
                "field {index} is row-aligned with the axis"
            );
        }
        assert_eq!(
            run_ranges(&series),
            vec![(0, 2), (3, 1), (6, 1)],
            "rows at 0-1, 3 and 6"
        );
        assert!(
            matches!(
                series.fields.as_slice()[0].dense(),
                Some(DenseSamplesRef::Float(_))
            ),
            "the float-only field is one dense float column"
        );
        assert!(
            matches!(
                series.fields.as_slice()[1].dense(),
                Some(DenseSamplesRef::Mixed(_))
            ),
            "the histogram field carries its indices beside a null float column"
        );
        let mut floats = series.fields.as_slice()[0].cursor();
        let mut histograms = series.fields.as_slice()[1].cursor();
        for &(position, has_float, has_histogram) in &expected_rows {
            let row = series.time.row_of(position).expect("a carried position");
            match (floats.at(row).expect("aligned"), has_float) {
                (SampleRef::Float(value), true) => {
                    assert_eq!(value, position as f64);
                }
                (SampleRef::Missing, false) => {}
                (SampleRef::Histogram(_), _) => panic!("field 0 read a histogram at {position}"),
                (sample, expected) => panic!(
                    "field 0 at {position}: present={expected}, read {}",
                    match sample {
                        SampleRef::Missing => "missing",
                        SampleRef::Float(_) => "a float",
                        SampleRef::Histogram(_) => unreachable!(),
                    }
                ),
            }
            match (histograms.at(row).expect("aligned"), has_histogram) {
                (SampleRef::Histogram(view), true) => {
                    assert_eq!(view.count, 1.0, "the histogram kept its count");
                }
                (SampleRef::Missing, false) => {}
                (SampleRef::Float(_), _) => {
                    panic!("field 1 read a float at {position}, not a histogram")
                }
                (SampleRef::Missing, true) => {
                    panic!("field 1 lost its histogram at {position}")
                }
                (SampleRef::Histogram(_), false) => {
                    panic!("field 1 invented a histogram at {position}")
                }
            }
        }
    }

    /// Axis and chunk representations of one set of points must read the same.
    #[test]
    fn one_set_of_points_in_five_representations_expands_the_same() {
        const POINTS: usize = 10;
        let schema = schema(1, POINTS as u64);
        let grid = schema.eval_grid.clone().unwrap();
        let labels = Labels::from([("job".to_string(), "x".to_string())]);
        let mut points = Points::new();
        for position in [0, 1, 4, 7, 8] {
            points.insert(position, f64::to_bits(position as f64 + 1.0));
        }
        let mut expected = vec![vec![None]; POINTS];
        for (position, bits) in &points {
            expected[*position] = vec![Some(f64::from_bits(*bits))];
        }

        // 1. A contiguous axis with explicit nulls over the whole span.
        let contiguous = super::super::reference::series_of(&schema, &labels, &points)
            .expect("five positions hold a value");
        assert!(contiguous.time.as_grid_range().is_some());
        assert_eq!(
            contiguous.time.len(),
            9,
            "the dense fixture is trimmed to the span from 0 to the last valued position"
        );

        // 2. A fragmented axis: one run per maximal stretch of consecutive positions.
        let fragmented = sparse_series(&schema, &labels, &points).expect("five positions");
        assert_eq!(fragmented.time.len(), points.len());
        assert_eq!(run_ranges(&fragmented), vec![(0, 2), (4, 1), (7, 2)]);

        // 3. The same positions with adjacent spans kept apart.
        let regrouped = {
            let values: Vec<f64> = points.values().map(|bits| f64::from_bits(*bits)).collect();
            let time = TimeAxis::from_grid_spans(
                grid.clone(),
                vec![
                    GridSpan::new(0, 1),
                    GridSpan::new(1, 1),
                    GridSpan::new(4, 1),
                    GridSpan::new(7, 2),
                ],
            )
            .unwrap();
            Series::try_new(
                &schema,
                metric_of(&labels),
                time,
                SeriesFields::one(Samples::from_float(Float64Array::from(values))),
            )
            .unwrap()
        };
        assert_eq!(regrouped.time.len(), points.len());
        assert_eq!(
            regrouped.time.runs().count(),
            4,
            "the regrouped axis stores four spans"
        );
        assert_eq!(
            fragmented.time.runs().count(),
            3,
            "the coalesced axis stores three"
        );

        // 4. The same rows with the field samples split into two and three chunks.
        let chunked = |cuts: &[usize]| -> Series {
            let values: Vec<f64> = points.values().map(|bits| f64::from_bits(*bits)).collect();
            let mut chunks = Vec::new();
            let mut start = 0;
            for end in cuts.iter().copied().chain(std::iter::once(values.len())) {
                chunks.push(Samples::from_float(Float64Array::from(
                    values[start..end].to_vec(),
                )));
                start = end;
            }
            let samples = Samples::try_from_chunks(chunks).unwrap();
            assert_eq!(samples.len(), values.len(), "chunking kept every sample");
            Series::try_new(
                &schema,
                metric_of(&labels),
                fragmented.time.clone(),
                SeriesFields::one(samples),
            )
            .unwrap()
        };
        let two_chunks = chunked(&[2]);
        let three_chunks = chunked(&[1, 3]);

        let variants = [
            contiguous.clone(),
            fragmented.clone(),
            regrouped,
            two_chunks.clone(),
            three_chunks.clone(),
        ];
        let expanded: Vec<_> = variants.iter().map(expand).collect();
        assert_eq!(expanded[0], expected, "the contiguous axis");
        for (index, dense) in expanded.iter().enumerate().skip(1) {
            assert_eq!(*dense, expected, "variant {index} disagrees");
        }

        // Regrouped spans change the run list, so only the expanded positions are compared there.
        let positions = |series: &Series| -> Vec<usize> {
            series
                .time
                .positions()
                .map(|(position, _)| position)
                .collect()
        };
        assert_eq!(
            positions(&contiguous),
            (0..9).collect::<Vec<_>>(),
            "the dense fixture covers 0..=8, the span the points span"
        );
        assert_eq!(positions(&fragmented), vec![0, 1, 4, 7, 8]);
        assert_eq!(positions(&variants[2]), positions(&fragmented));
        assert_eq!(run_ranges(&variants[3]), run_ranges(&fragmented));
        assert_eq!(run_ranges(&variants[4]), run_ranges(&fragmented));
        for (index, series) in variants.iter().enumerate() {
            assert_eq!(
                series.time.grid().map(|grid| grid.spec()),
                Some(grid.spec()),
                "variant {index} lost the grid"
            );
        }
        let read = |series: &Series| -> Vec<Option<f64>> {
            let mut cursor = series.fields.as_slice()[0].cursor();
            series
                .time
                .positions()
                .map(|(_, row)| match cursor.at(row).expect("aligned") {
                    SampleRef::Float(value) => Some(value),
                    SampleRef::Missing => None,
                    SampleRef::Histogram(_) => panic!("unexpected histogram"),
                })
                .collect()
        };
        assert_eq!(
            read(&two_chunks),
            vec![Some(1.0), Some(2.0), Some(5.0), Some(8.0), Some(9.0)]
        );
        assert_eq!(read(&three_chunks), read(&two_chunks));
    }
}

/// The payload of one histogram with floats as bits.
fn payload(view: &HistogramView<'_>) -> Vec<u64> {
    let hint = match view.reset_hint {
        CounterResetHint::Unknown => 0,
        CounterResetHint::CounterReset => 1,
        CounterResetHint::NotCounterReset => 2,
        CounterResetHint::Gauge => 3,
        CounterResetHint::Unrecognized(value) => (value as u64) | (1 << 32),
    };
    let mut bits = vec![
        view.schema as u64,
        hint,
        view.start_timestamp.map_or(u64::MAX, |start| start as u64),
        view.zero_threshold.to_bits(),
        view.zero_count.to_bits(),
        view.count.to_bits(),
        view.sum.to_bits(),
        view.custom_values.len() as u64,
    ];
    bits.extend(view.custom_values.iter().map(|value| value.to_bits()));
    for side in [&view.positive, &view.negative] {
        bits.push(side.span_offsets.len() as u64);
        bits.push(side.span_lengths.len() as u64);
        bits.push(side.buckets.len() as u64);
        bits.extend(side.span_offsets.iter().map(|&offset| offset as u64));
        bits.extend(side.span_lengths.iter().map(|&length| length as u64));
        bits.extend(side.buckets.iter().map(|bucket| bucket.to_bits()));
    }
    bits
}

/// `push_refs` and `push_samples` write the same payload, start timestamp included.
#[test]
fn a_pushed_view_writes_the_payload_of_the_owned_route() {
    let schema = grid_schema(2, 3);
    let histograms = [
        OwnedHistogram::parse("0/0/0/3ff0000000000000/3ff0000000000000/0/1_1/3ff0000000000000///"),
        OwnedHistogram::parse("0/0/0/4000000000000000/4008000000000000/1/0_1/4000000000000000///"),
    ];
    let mut stamped = histograms[1].view();
    stamped.start_timestamp = Some(1_500);
    let views = [histograms[0].view(), stamped];

    let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
    builder
        .push_samples(
            0,
            &[
                Some(GridSample::Histogram(Box::new(
                    FloatHistogram::try_from_view(&views[0]).unwrap(),
                ))),
                Some(GridSample::Float(2.0)),
            ],
        )
        .unwrap();
    builder
        .push_samples(
            2,
            &[
                None,
                Some(GridSample::Histogram(Box::new(
                    FloatHistogram::try_from_view(&views[1]).unwrap(),
                ))),
            ],
        )
        .unwrap();
    let owned = builder.finish(metric(0)).unwrap().unwrap();

    let mut builder = GridSeriesBuilder::try_new(schema).unwrap();
    builder
        .push_refs(0, &[SampleRef::Histogram(views[0]), SampleRef::Float(2.0)])
        .unwrap();
    builder
        .push_refs(2, &[SampleRef::Missing, SampleRef::Histogram(views[1])])
        .unwrap();
    let by_view = builder.finish(metric(0)).unwrap().unwrap();

    let described = |series: &Series, field: usize| -> Vec<(usize, String, Option<Vec<u64>>)> {
        let samples = &series.fields.as_slice()[field];
        series
            .time
            .positions()
            .map(|(point, row)| match samples.sample(row) {
                SampleRef::Missing => (point, "missing".to_string(), None),
                SampleRef::Float(value) => {
                    (point, "float".to_string(), Some(vec![value.to_bits()]))
                }
                SampleRef::Histogram(view) => {
                    (point, "histogram".to_string(), Some(payload(&view)))
                }
            })
            .collect()
    };
    for field in 0..2 {
        assert_eq!(
            described(&owned, field),
            described(&by_view, field),
            "field {field} came back different through the view route"
        );
    }
    assert_eq!(
        described(&by_view, 1),
        vec![
            (0, "float".to_string(), Some(vec![2.0f64.to_bits()])),
            (2, "histogram".to_string(), Some(payload(&views[1]))),
        ],
        "the pushed view lost the payload it was read with"
    );
    assert_ne!(
        payload(&views[1]),
        payload(&histograms[1].view()),
        "the start timestamp is part of what is compared"
    );
}
