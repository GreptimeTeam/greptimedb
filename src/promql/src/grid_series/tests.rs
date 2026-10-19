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

use datatypes::series::samples::{DenseSamplesRef, FieldSpec, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::reference::{
    Labels, Points, assert_pieces_eq, assert_series_eq, piece_ranges, points_of, random_dense,
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

/// The pieces of one series, each expanded over the whole grid and stitched back together, have
/// to reproduce the dense matrix they were pushed from.
///
/// This is the contract of the whole change: many pieces, one dense answer, and the pieces never
/// overlap and never cover a position holding no value.
/// The one piece of a series that can only have one; panics when it has several.
fn single<T>(mut pieces: Vec<T>) -> T {
    assert_eq!(pieces.len(), 1, "expected exactly one piece");
    pieces.pop().unwrap()
}

#[test]
fn pieces_of_one_series_stitch_back_to_the_pushed_rows() {
    for seed in 0..200 {
        let mut rng = StdRng::seed_from_u64(seed);
        let fields = rng.random_range(1..=3);
        let grid_len = rng.random_range(1..64);
        let mut builder = GridSeriesBuilder::try_new(grid_schema(fields, grid_len)).unwrap();
        // One builder serves several series, as it does inside an operator.
        for host in 0..3 {
            let expected = random_dense(&mut rng, grid_len as usize, fields);
            for (index, row) in expected.iter().enumerate() {
                // Missing rows may be pushed as all-None or skipped entirely.
                if row.iter().any(Option::is_some) || rng.random_bool(0.5) {
                    builder.push(index, row).unwrap();
                }
            }
            let pieces = builder.finish(metric(host)).unwrap();
            assert_pieces_eq(&pieces, &expected, &metric(host));
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

/// Series of one label set merge field by field: different fields at one point combine, and
/// only one field present in both collides.
#[test]
fn merge_combines_disjoint_fields_at_one_point() {
    let schema = grid_schema(2, 1);
    let series = |row: [Option<f64>; 2]| {
        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
        builder.push(0, &row).unwrap();
        single(builder.finish(metric(0)).unwrap())
    };
    let merged = merge_same_labelset(
        &schema,
        vec![series([Some(1.0), None]), series([None, Some(2.0)])],
    )
    .unwrap();
    assert_eq!(merged.len(), 1);
    assert_series_eq(
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

/// A piece ends where a position holds no value: two values far apart give two one-row pieces
/// instead of one long piece of nulls.
#[test]
fn pieces_never_bridge_positions_without_a_value() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 1_000_000)).unwrap();
    builder.push(10, &[Some(1.0)]).unwrap();
    builder.push(900_000, &[Some(2.0)]).unwrap();
    assert!(builder.has_pending());

    let pieces = builder.finish(metric(0)).unwrap();
    assert_eq!(piece_ranges(&pieces), vec![(10, 1), (900_000, 1)]);
    assert_eq!(
        pieces
            .iter()
            .map(|piece| match &piece.time {
                TimeAxis::Grid(range) => range.len(),
                _ => unreachable!(),
            })
            .sum::<usize>(),
        2,
        "two values, two rows"
    );
    assert!(!builder.has_pending());
}

/// Taking the pieces one at a time gives the same shape as taking them all at once.
#[test]
fn pieces_can_be_taken_one_at_a_time() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    builder.push(0, &[Some(1.0)]).unwrap();
    builder.push(5, &[Some(2.0)]).unwrap();
    builder.push(9, &[Some(3.0)]).unwrap();

    let mut taken = Vec::new();
    while let Some(piece) = builder.take_run(metric(0)).unwrap() {
        taken.push(piece);
    }
    assert_eq!(piece_ranges(&taken), vec![(0, 1), (5, 1), (9, 1)]);
    assert!(!builder.has_pending());
    assert!(builder.take_run(metric(0)).unwrap().is_none());
    assert!(builder.finish(metric(0)).unwrap().is_empty());
}

/// A push whose values are all missing holds no row, so it neither opens nor widens a piece.
#[test]
fn an_all_missing_push_holds_no_row() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    builder.push(2, &[Some(1.0)]).unwrap();
    builder.push(3, &[None]).unwrap();
    builder.push(4, &[Some(2.0)]).unwrap();
    builder.push(7, &[None]).unwrap();

    let pieces = builder.finish(metric(0)).unwrap();
    assert_eq!(piece_ranges(&pieces), vec![(2, 1), (4, 1)]);
    assert!(!builder.has_pending());
}

/// Values and nulls alternating over a long grid: every valued position is its own piece, so the
/// piece count grows with the values while the row count stays at one per value.
#[test]
fn alternating_values_and_nulls_yield_one_row_per_value() {
    const GRID: usize = 200_000;
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, GRID as u64)).unwrap();
    for index in 0..GRID {
        let value = (index % 2 == 0).then_some(index as f64);
        builder.push(index, &[value]).unwrap();
    }
    let pieces = builder.finish(metric(0)).unwrap();
    assert_eq!(
        pieces.len(),
        GRID / 2,
        "one piece per value, which is the accepted cost of this design"
    );
    let expected: Vec<(usize, usize)> = (0..GRID / 2).map(|i| (i * 2, 1)).collect();
    assert_eq!(piece_ranges(&pieces), expected);
}

/// Rows inside one piece stay aligned across fields: a field with no value where another has one
/// still gets a row there, holding a null. That is the one null a piece keeps.
#[test]
fn a_piece_keeps_its_fields_row_aligned() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(2, 4)).unwrap();
    builder.push(1, &[Some(1.0), None]).unwrap();
    builder.push(2, &[None, Some(2.0)]).unwrap();
    let pieces = builder.finish(metric(0)).unwrap();
    assert_eq!(
        piece_ranges(&pieces),
        vec![(1, 2)],
        "complementary fields at adjacent positions are ONE piece"
    );
    assert_eq!(
        stitch(&pieces),
        vec![
            vec![None, None],
            vec![Some(1.0), None],
            vec![None, Some(2.0)],
            vec![None, None],
        ]
    );
}

/// Every piece of one series is a slice of the same value buffer, so the pieces of a series with
/// many runs cost descriptors rather than copies of the data.
///
/// The assertion is not vacuous: the pieces are separated by holes, so a builder copying each
/// piece would give every piece its own base address, while slices of one buffer are offset by
/// exactly the distance between their compact positions.
#[test]
fn pieces_share_one_values_buffer() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 100)).unwrap();
    for (offset, index) in [0usize, 1, 2, 50, 51, 99].into_iter().enumerate() {
        builder.push(index, &[Some(offset as f64)]).unwrap();
    }
    let pieces = builder.finish(metric(0)).unwrap();
    assert_eq!(piece_ranges(&pieces), vec![(0, 3), (50, 2), (99, 1)]);

    let float = |piece: &Series| match piece.fields.as_slice()[0].dense() {
        Some(DenseSamplesRef::Float(values)) => values.clone(),
        _ => panic!("expected float samples"),
    };
    // Compact offsets of the three runs: the holes between them are not in the buffer at all.
    let offsets = [0usize, 3, 5];
    let base = float(&pieces[0]).values().as_ptr() as usize;
    for (piece, offset) in pieces.iter().zip(offsets) {
        let values = float(piece);
        assert_eq!(
            (values.values().as_ptr() as usize) - base,
            offset * size_of::<f64>(),
            "a piece is not a slice of the same buffer: it was copied"
        );
        // The values are those pushed, so the offsets above are the ones that were sliced.
        let expected: Vec<f64> = (offset..offset + values.len()).map(|v| v as f64).collect();
        assert_eq!(values.values().as_ref(), expected.as_slice());
    }
    // One buffer, so the three pieces together hold exactly the six pushed rows.
    let total: usize = pieces.iter().map(|piece| float(piece).len()).sum();
    assert_eq!(total, 6, "the pieces do not share one row-per-value buffer");
}

/// Dense pushes of the same positions give the same pieces whether the gaps were skipped or
/// pushed as all-missing rows.
#[test]
fn skipped_and_all_missing_positions_are_the_same_series() {
    let mut skipped = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    let mut pushed = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    for index in 0..10 {
        let value = [0, 1, 5].contains(&index).then_some(index as f64);
        if value.is_none() {
            skipped.push(index, &[None]).unwrap();
        } else {
            skipped.push(index, &[value]).unwrap();
        }
        pushed.push(index, &[value]).unwrap();
    }
    let skipped = skipped.finish(metric(0)).unwrap();
    let pushed = pushed.finish(metric(0)).unwrap();
    assert_eq!(piece_ranges(&skipped), piece_ranges(&pushed));
    assert_eq!(stitch(&skipped), stitch(&pushed));
}

/// Series are handed out in the order their rows were pushed, so one builder serves many series.
#[test]
fn one_builder_serves_several_series() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(1, 10)).unwrap();
    builder.push(0, &[Some(1.0)]).unwrap();
    builder.push(2, &[Some(2.0)]).unwrap();
    let first = builder.finish(metric(0)).unwrap();
    assert_eq!(piece_ranges(&first), vec![(0, 1), (2, 1)]);
    assert!(first.iter().all(|piece| piece.metric == metric(0)));

    builder.push(1, &[Some(3.0)]).unwrap();
    builder.push(3, &[Some(4.0)]).unwrap();
    let second = builder.finish(metric(1)).unwrap();
    assert_eq!(piece_ranges(&second), vec![(1, 1), (3, 1)]);
    assert!(second.iter().all(|piece| piece.metric == metric(1)));
    assert!(!builder.has_pending());
}
