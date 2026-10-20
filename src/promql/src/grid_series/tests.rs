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

use datatypes::series::samples::{DenseSamplesRef, FieldSpec, SampleRef, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::reference::{
    Labels, Points, assert_pieces_eq, assert_result_eq, points_of, random_dense, run_ranges,
    series_of, stitch,
};
use super::{GridSeriesBuilder, merge_same_labelset};

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

fn metric(host: usize) -> MetricName {
    MetricName::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(format!("h{host}")),
        }],
    )
    .unwrap()
}

/// The series one builder hands out, expanded over the whole grid, has to reproduce the dense
/// matrix it was pushed from.
///
/// This is the contract of the whole change: one series per logical output, one dense answer, and
/// the rows never cover a position holding no value.
#[test]
fn the_series_of_one_push_sequence_matches_the_pushed_rows() {
    for seed in 0..200 {
        let mut rng = StdRng::seed_from_u64(seed);
        let fields = rng.random_range(1..=3);
        let grid_len = rng.random_range(1..64);
        let mut builder = GridSeriesBuilder::try_new(grid_schema(fields, grid_len)).unwrap();
        // One builder serves several series, as it does inside an operator.
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

/// Label sets compare without empty values, which Prometheus does not have: a series with
/// `host=""` and one without `host` merge.
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

/// Members whose rows never share a position merge without colliding, so the whole merge runs to
/// completion for every row. Each member is located once per row while collecting; resolving it
/// again per row inside the merge would be a search per row, which is what a wide fan-in of
/// disjoint single-point members would pay.
#[test]
fn merge_keeps_every_row_of_many_disjoint_members() {
    const MEMBERS: usize = 64;
    let schema = grid_schema(1, (2 * MEMBERS) as u64);
    // One value per member at an even position, so no two members share a position and the
    // collision check never fires.
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

/// Series of one label set merge field by field: different fields at one point combine, and
/// only one field present in both collides.
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

/// Two values a million positions apart are one series with two rows, addressed by two runs: the
/// positions between them cost neither a row nor a value buffer sized by the span.
#[test]
fn runs_never_bridge_positions_without_a_value() {
    const SPAN: usize = 1_000_000;
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, SPAN as u64)).unwrap();
    builder.push(10, &[Some(1.0)]).unwrap();
    builder.push(900_000, &[Some(2.0)]).unwrap();
    assert!(builder.has_pending());

    let series = builder.finish(metric(0)).unwrap().expect("two rows");
    // One series over a million-position grid, with two runs, and the axis says where they sit.
    assert_eq!(
        series
            .time
            .grid()
            .expect("a grid series")
            .timestamps()
            .len(),
        SPAN,
        "the grid really holds a million positions"
    );
    assert_eq!(run_ranges(&series), vec![(10, 1), (900_000, 1)]);
    assert!(
        !series.time.is_contiguous(),
        "two far-apart rows are two runs"
    );
    assert_eq!(series.time.len(), 2, "two values, two rows");
    // Two field rows, and the value buffer is exactly as long as the values pushed: nothing is
    // sized by the grid or by the span between the runs.
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
    // The axis resolves both rows on the shared grid, so a consumer reads the right instants
    // without the series carrying one per position of the span.
    assert_eq!(
        (series.time.timestamp(0), series.time.timestamp(1)),
        (Some(-5_000 + 10 * 1_000), Some(-5_000 + 900_000 * 1_000))
    );
    assert!(!builder.has_pending());
}

/// A prefix flush hands out the whole buffer as one series, runs and all, and ends nothing: the
/// same logical series keeps going, so the position cursor is not reset either.
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
    // A second flush has nothing to hand out, and the series is not over: only `begin_series`
    // resets it, so a position before the prefix is still refused.
    assert!(builder.take_buffered(metric(0)).unwrap().is_none());
    assert!(builder.push(9, &[Some(4.0)]).is_err());
    assert!(builder.finish(metric(0)).unwrap().is_none());

    builder.begin_series();
    builder.push(0, &[Some(7.0)]).unwrap();
    let next = builder.finish(metric(1)).unwrap().expect("one row");
    assert_eq!(run_ranges(&next), vec![(0, 1)]);
}

/// A push whose values are all missing holds no row, so it neither opens nor widens a run.
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

/// Values and nulls alternating over a long grid: every valued position is a run of its own, so
/// the run count grows with the values while the row count stays at one per value.
#[test]
fn alternating_values_and_nulls_yield_one_row_per_value() {
    const GRID: usize = 200_000;
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, GRID as u64)).unwrap();
    for index in 0..GRID {
        let value = (index % 2 == 0).then_some(index as f64);
        builder.push(index, &[value]).unwrap();
    }
    let series = builder.finish(metric(0)).unwrap().expect("half the grid");
    assert_eq!(
        series.time.len(),
        GRID / 2,
        "one row per value, and none for the empties"
    );
    let expected: Vec<(usize, usize)> = (0..GRID / 2).map(|i| (i * 2, 1)).collect();
    assert_eq!(run_ranges(&series), expected);
}

/// Rows of one series stay aligned across fields: a field with no value where another has one
/// still gets a row there, holding a null. That is the one null a series keeps.
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

/// A series of several runs is one row buffer plus one span per run: the rows are compact, so the
/// holes between the runs are not in the value buffer at all.
///
/// The assertion is not vacuous: the runs are separated by holes, so a builder that kept the
/// envelope would have to write a row per position of it, and the buffer would be 100 long.
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
    // One buffer holding exactly the six pushed rows, in push order.
    assert_eq!(values.len(), 6);
    assert_eq!(values.values().as_ref(), &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0]);
    assert_eq!(
        std::mem::size_of_val(values.values().as_ref()),
        6 * size_of::<f64>()
    );
    // The rows read back through the axis are the pushed values at the pushed positions.
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

/// Dense pushes of the same positions give the same series whether the gaps were skipped or
/// pushed as all-missing rows.
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

/// Series are handed out in the order their rows were pushed, so one builder serves many series,
/// as long as each one starts with `begin_series`.
#[test]
fn one_builder_serves_several_series() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    builder.push(0, &[Some(1.0)]).unwrap();
    builder.push(2, &[Some(2.0)]).unwrap();
    let first = builder.finish(metric(0)).unwrap().expect("two rows");
    assert_eq!(run_ranges(&first), vec![(0, 1), (2, 1)]);
    assert_eq!(first.metric, metric(0));

    // Positions start over with each series, which only `begin_series` allows.
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

/// A prefix flush hands every buffered row to the caller, so `finish` only has the rows pushed
/// after it: nothing is repeated, and nothing the caller took is dropped.
#[test]
fn finish_hands_out_only_the_rows_pushed_after_a_prefix_flush() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 20)).unwrap();
    builder.push(0, &[Some(1.0)]).unwrap();
    builder.push(5, &[Some(2.0)]).unwrap();
    builder.push(9, &[Some(3.0)]).unwrap();

    let prefix = builder
        .take_buffered(metric(0))
        .unwrap()
        .expect("three rows");
    assert_eq!(run_ranges(&prefix), vec![(0, 1), (5, 1), (9, 1)]);

    // A row after the prefix: it belongs to the same series, so the metric is the same and only
    // the positions after the ones already handed out can appear.
    builder.push(15, &[Some(4.0)]).unwrap();
    let rest = builder
        .finish(metric(0))
        .unwrap()
        .expect("the row pushed since");
    assert_eq!(
        run_ranges(&rest),
        vec![(15, 1)],
        "finish repeated a row the caller already took"
    );
    assert_eq!(
        match rest.fields.as_slice()[0].dense() {
            Some(DenseSamplesRef::Float(values)) => values.value(0),
            _ => panic!("expected float samples"),
        },
        4.0,
        "the row carries another position's value"
    );
    assert!(!builder.has_pending());
}

// ---------------------------------------------------------------------------------------------
// Acceptance: the builder's observable shape.
//
// A builder hands out one `Series` per logical output, whose axis names the runs of positions that
// hold a value. What these tests pin is that shape -- the fast path, the run count, the row count,
// the field layout -- and not just the values, which the differential tests already cover.
// ---------------------------------------------------------------------------------------------

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

    /// The schema of a `fields`-field series on a `len`-position grid with a ten-millisecond step.
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

    /// Pushes one row per position, `value(index)` giving each field its value or a hole.
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

    /// B: a contiguous result keeps the `TimeAxis::Grid` fast path.
    ///
    /// Every position holding a value is one range, and a range is what the axis stores: a span
    /// list of one element would be the same rows described the slower way.
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
        // The axis lends the grid's buffer instead of copying it: the fast path is not only a
        // label, it is the same rows without per-series timestamp storage.
        assert!(
            matches!(series.time.timestamps(), std::borrow::Cow::Borrowed(_)),
            "a single range lends its timestamps"
        );
    }

    /// B: alternating valued and missing positions stay one series of one-run-per-value.
    ///
    /// Sixteen positions, eight values: eight runs and eight rows, in **one** series with eight
    /// field samples per field. The constant that would inflate is the grid: the axis holds eight
    /// rows, not sixteen.
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

    /// B: the positions delivered in one series with holes and in several pushes give the same
    /// dense result, so the delivery boundaries do not decide the answer.
    ///
    /// The builder is the production entry point for this shape: `take_buffered` is the prefix
    /// flush a streaming evaluator uses, and `finish` closes the series.
    #[test]
    fn one_series_with_holes_and_several_prefix_flushes_expand_alike() {
        const POINTS: usize = 12;
        let schema = schema(1, POINTS as u64);
        // One builder, one series, every position pushed: holes are pushed as nulls, so this is
        // one dense series over the whole grid.
        let mut holed = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        let valued = |index: usize| matches!(index, 0 | 1 | 4 | 9);
        for index in 0..POINTS {
            let value = valued(index).then_some(index as f64);
            holed.push(index, &[value]).unwrap();
        }
        let one = holed.finish(metric(0)).unwrap().expect("four values");
        assert_eq!(run_ranges(&one), vec![(0, 2), (4, 1), (9, 1)]);

        // The same rows flushed as prefixes: rows 0..5, then 5..10, then the rest.
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
        // A prefix flush does not end the series, so the pieces are the pieces of one logical
        // output: together they cover each valued position once.
        assert_eq!(pieces.len(), 2, "two hand-outs of the one series");
        let mut expanded = expand(&one);
        let stitched = stitch(&pieces);
        assert_eq!(stitched, expanded, "the flushed rows moved");
        // The first hand-out is the prefix of the whole, in run terms.
        let prefix = run_ranges(&pieces[0]);
        assert_eq!(prefix, vec![(0, 2), (4, 1)]);
        assert_eq!(
            &run_ranges(&one)[..prefix.len()],
            prefix.as_slice(),
            "the prefix is the beginning of the whole series"
        );
        expanded[4] = vec![Some(4.0)];
        assert_eq!(stitched[4], expanded[4]);
    }

    /// D: fields with complementary holes keep one row per position and a null where a field has
    /// no value, so no field loses a row another field fills.
    ///
    /// Field A holds values at positions 0 and 5, field B at 2 and 5. Positions 0, 2 and 5 hold
    /// something, so the axis carries three rows and each field has a null at the two positions it
    /// does not fill.
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

    /// D: an ordinary NaN is a *value*: it keeps its row and its payload is not a hole.
    ///
    /// The stale marker Prometheus uses is a particular NaN payload; an ordinary NaN must survive
    /// as a sample, including when it is the only value at a position and when it sits next to a
    /// hole.
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
        // The dense reference agrees, and it holds no value at the hole.
        let dense = expand(&series);
        assert_eq!(dense.len(), POINTS);
        assert!(dense[1][0].is_some_and(f64::is_nan));
        assert_eq!(dense[2], vec![None]);
        assert_eq!(dense[3], vec![Some(3.0)]);
        assert!(dense[5][0].is_some_and(f64::is_nan));
    }

    /// D: a float field and a histogram field on one axis, and an all-histogram one, keep their
    /// sample kinds and their row alignment.
    ///
    /// `Samples::try_from_mixed` rejects a position holding both a float and a histogram, so the
    /// two kinds take turns; the positions holding one are the rows of the axis, and a histogram
    /// sample reads back as `SampleRef::Histogram`, not as a float.
    #[test]
    fn mixed_and_histogram_fields_keep_their_sample_kinds() {
        const POINTS: usize = 8;
        let schema = schema(2, POINTS as u64);
        let text = "0/0/0/3ff0000000000000/3ff0000000000000/0/1_1/3ff0000000000000///";
        let owned = OwnedHistogram::parse(text);
        // Position -> the value of each field: field 0 is mixed (float or histogram), field 1 is
        // all-histogram. A position holds a row when either field has a value there.
        // A position holds a row only when some field has a value there, so 2, 4, 5 and 7 -- which
        // are absent from this list -- are holes rather than rows of nulls.
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
            // Field 1 holds a histogram whenever the row says so.
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
        // A field that never held a histogram stays a plain float column; the all-histogram one is
        // a mixed column whose float side is null everywhere.
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

    /// E: one set of points, five representations of it, one answer.
    ///
    /// The variants are a contiguous axis with explicit nulls, a fragmented axis built by
    /// `sparse_series`, the same fragmented positions split into different span groupings, and the
    /// field samples split into different chunk boundaries. A representation is a way of saying
    /// where the rows sit and where the sample descriptors end, so none of them may change what a
    /// reader sees.
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

        // 3. The same positions, split into different span groupings: `[(0,2),(4,1),(7,2)]` and
        //    `[(0,1),(1,1),(4,1),(7,2)]` are the same rows, described with three and four spans.
        //    The axis keeps adjacent spans apart, so the raw span count differs while the set of
        //    carried positions -- and therefore the expanded matrix -- does not.
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

        // 4. The same rows with the field samples in different chunks: two chunks and three
        //    chunks, split at different points. A chunk boundary is a descriptor, not a row.
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

        // Where the axis is genuinely the same, the runs are compared directly; across regroupings
        // only the expanded positions are, since a span boundary at 0|1 and at 0..2 are the same
        // rows.
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
        // The chunked variants read the same samples row by row.
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
