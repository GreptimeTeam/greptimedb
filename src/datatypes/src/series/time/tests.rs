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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::buffer::{Buffer, ScalarBuffer};

use super::{EvalGrid, GridRange, GridRanges, GridSpan, GridSpec, TimeAxis};

#[test]
fn grid_specs_validate_and_ranges_are_inclusive_without_rounding_up() {
    assert_eq!(GridSpec::try_from_range(-5, 6, 6).unwrap().len, 2);
    assert_eq!(GridSpec::try_from_range(4, 4, 2).unwrap().len, 1);
    assert_eq!(GridSpec::try_from_range(4, 5, 2).unwrap().len, 1);
    assert!(GridSpec::try_from_range(5, 4, 1).is_err());
    assert!(GridSpec::try_from_range(i64::MIN, i64::MAX, 1).is_err());
    assert!(GridSpec::try_from_range(0, 1, 0).is_err());
    assert!(GridSpec::try_new(0, -1, 1).is_err());
    assert!(GridSpec::try_new(0, 1, 0).is_err());
    assert!(GridSpec::try_new(i64::MAX, 1, 2).is_err());
    assert_eq!(
        &EvalGrid::try_new(GridSpec::try_new(i64::MIN, i64::MAX, 3).unwrap())
            .unwrap()
            .timestamps()[..],
        &[i64::MIN, -1, i64::MAX - 1]
    );
    assert!(GridSpec::try_new(i64::MIN, i64::MAX, 4).is_err());
    assert!(GridSpec::try_new(0, i64::MAX, u64::MAX).is_err());
    assert!(GridSpec::try_new(i64::MIN, 1, 2).is_ok());
    assert!(GridSpec::try_new(i64::MAX - 1, 1, 2).is_ok());
}

#[test]
fn grid_materialization_revalidates_public_specs_and_shares_ranges_by_value() {
    assert!(
        EvalGrid::try_new(GridSpec {
            start_ms: 0,
            step_ms: 0,
            len: 1
        })
        .is_err()
    );
    assert!(
        EvalGrid::try_new(GridSpec {
            start_ms: 0,
            step_ms: 1,
            len: 0
        })
        .is_err()
    );
    assert!(
        EvalGrid::try_new(GridSpec {
            start_ms: i64::MAX,
            step_ms: 1,
            len: 2
        })
        .is_err()
    );

    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(-5, 3, 5).unwrap()).unwrap());
    assert_eq!(&grid.timestamps()[..], &[-5, -2, 1, 4, 7]);
    let a = GridRange::try_new(grid.clone(), 1, 3).unwrap();
    let b = GridRange::try_new(Arc::new(EvalGrid::try_new(grid.spec()).unwrap()), 1, 3).unwrap();
    assert_eq!(a, b);
    let values = a.timestamps();
    assert_eq!(values.as_ptr(), grid.timestamps().as_ptr().wrapping_add(1));
    assert_eq!(&values[..], &[-2, 1, 4]);
    assert!(GridRange::try_new(grid.clone(), 5, 0).unwrap().is_empty());
    assert!(GridRange::try_new(grid.clone(), 4, 2).is_err());
    assert!(GridRange::try_new(grid, usize::MAX, 1).is_err());
}

#[test]
fn grid_axis_slices_keep_absolute_indices_and_shared_grid_alive() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(10, 10, 5).unwrap()).unwrap());
    let axis = TimeAxis::Grid(GridRange::try_new(grid.clone(), 1, 4).unwrap());
    let nested = axis.slice(1, 2).unwrap();
    let TimeAxis::Grid(range) = &nested else {
        panic!("grid slice changed representation")
    };
    assert_eq!(range.start(), 2);
    assert_eq!(range.len(), 2);
    drop(axis);
    drop(grid);
    assert_eq!(nested.timestamp(0), Some(30));
    assert_eq!(nested.timestamp(1), Some(40));
    assert_eq!(nested.timestamp(2), None);
    assert!(nested.slice(1, usize::MAX).is_err());
    assert!(nested.slice(2, 1).is_err());
}

#[test]
fn explicit_timestamps_require_strict_order_and_slice_without_copying() {
    let source = ScalarBuffer::from(vec![1_i64, 3, 5, 7]);
    let axis = TimeAxis::try_new(source.clone()).unwrap();
    let clone = axis.clone();
    let TimeAxis::Explicit(values) = &axis else {
        panic!("expected explicit axis")
    };
    let TimeAxis::Explicit(cloned_values) = &clone else {
        panic!("expected explicit axis")
    };
    assert!(values.ptr_eq(cloned_values));
    let slice = axis.slice(1, 2).unwrap();
    let TimeAxis::Explicit(sliced) = &slice else {
        panic!("expected explicit slice")
    };
    assert_eq!(sliced.as_ptr(), source.as_ptr().wrapping_add(1));
    assert_eq!(&sliced[..], &[3, 5]);
    let nested = slice.slice(1, 1).unwrap();
    let TimeAxis::Explicit(nested_values) = &nested else {
        panic!("expected nested explicit slice")
    };
    assert_eq!(nested_values.as_ptr(), source.as_ptr().wrapping_add(2));
    assert!(slice.slice(1, usize::MAX).is_err());
    assert!(slice.slice(2, 1).is_err());
    drop(source);
    drop(axis);
    drop(clone);
    drop(slice);
    assert_eq!(nested.timestamp(0), Some(5));
    assert_eq!(nested.timestamp(1), None);
    assert!(TimeAxis::try_new(ScalarBuffer::from(vec![1_i64, 1])).is_err());
    assert!(TimeAxis::try_new(ScalarBuffer::from(vec![2_i64, 1])).is_err());
}

#[test]
fn chunked_timestamps_validate_boundaries_and_keep_chunked_slices() {
    let axis = TimeAxis::try_from_chunks(vec![
        ScalarBuffer::from(Vec::<i64>::new()),
        ScalarBuffer::from(vec![1_i64, 3, 5]),
        ScalarBuffer::from(Vec::<i64>::new()),
        ScalarBuffer::from(vec![7_i64, 9]),
    ])
    .unwrap();
    let TimeAxis::ExplicitChunks(chunks) = &axis else {
        panic!("expected chunked axis")
    };
    assert_eq!(chunks.len(), 2);
    let first_ptr = chunks[0].as_ptr();
    let second_ptr = chunks[1].as_ptr();
    let slice = axis.slice(1, 3).unwrap();
    let TimeAxis::ExplicitChunks(sliced) = &slice else {
        panic!("slice flattened chunks")
    };
    assert_eq!(sliced.len(), 2);
    assert_eq!(sliced[0].as_ptr(), first_ptr.wrapping_add(1));
    assert_eq!(sliced[1].as_ptr(), second_ptr);
    assert_eq!(slice.timestamp(0), Some(3));
    assert_eq!(slice.timestamp(1), Some(5));
    assert_eq!(slice.timestamp(2), Some(7));
    assert_eq!(slice.timestamp(3), None);
    let tail = axis.slice(5, 0).unwrap();
    assert!(tail.is_empty());
    let nested = slice.slice(1, 2).unwrap();
    let TimeAxis::ExplicitChunks(nested_chunks) = &nested else {
        panic!("nested chunk slice flattened representation")
    };
    assert_eq!(nested_chunks.len(), 2);
    assert_eq!(nested_chunks[0].as_ptr(), first_ptr.wrapping_add(2));
    assert_eq!(nested_chunks[1].as_ptr(), second_ptr);
    assert_eq!(&nested_chunks[0][..], &[5]);
    assert_eq!(&nested_chunks[1][..], &[7]);
    assert!(axis.slice(usize::MAX, 1).is_err());
    drop(slice);
    drop(axis);
    assert_eq!(nested.timestamp(0), Some(5));
    assert_eq!(nested.timestamp(1), Some(7));

    assert!(TimeAxis::try_from_chunks(vec![ScalarBuffer::from(vec![1_i64, 1])]).is_err());
    assert!(
        TimeAxis::try_from_chunks(vec![
            ScalarBuffer::from(vec![1_i64, 3]),
            ScalarBuffer::from(vec![3_i64, 5]),
        ])
        .is_err()
    );
    assert!(
        TimeAxis::try_from_chunks(vec![
            ScalarBuffer::from(vec![1_i64, 3]),
            ScalarBuffer::from(vec![2_i64, 5]),
        ])
        .is_err()
    );
}

struct BufferOwner {
    values: Vec<i64>,
    drops: Arc<AtomicUsize>,
}

impl Drop for BufferOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn owned_timestamps(values: Vec<i64>, drops: Arc<AtomicUsize>) -> ScalarBuffer<i64> {
    let mut owner = BufferOwner { values, drops };
    let pointer = std::ptr::NonNull::new(owner.values.as_mut_ptr().cast()).unwrap();
    let len = owner.values.len() * std::mem::size_of::<i64>();
    // SAFETY: the vector allocation remains stable in the Arc owner for the buffer lifetime.
    let element_count = owner.values.len();
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, len, Arc::new(owner)) };
    ScalarBuffer::new(buffer, 0, element_count)
}

#[test]
fn sliced_explicit_and_chunk_buffers_retain_and_release_real_allocations() {
    let explicit_drops = Arc::new(AtomicUsize::new(0));
    let axis = TimeAxis::try_new(owned_timestamps(vec![2, 4, 6], explicit_drops.clone())).unwrap();
    let slice = axis.slice(1, 2).unwrap();
    drop(axis);
    assert_eq!(explicit_drops.load(Ordering::SeqCst), 0);
    assert_eq!(slice.timestamp(0), Some(4));
    drop(slice);
    assert_eq!(explicit_drops.load(Ordering::SeqCst), 1);

    let first_drops = Arc::new(AtomicUsize::new(0));
    let second_drops = Arc::new(AtomicUsize::new(0));
    let axis = TimeAxis::try_from_chunks(vec![
        owned_timestamps(vec![1, 3], first_drops.clone()),
        owned_timestamps(vec![5, 7], second_drops.clone()),
    ])
    .unwrap();
    let slice = axis.slice(1, 2).unwrap();
    drop(axis);
    assert_eq!(first_drops.load(Ordering::SeqCst), 0);
    assert_eq!(second_drops.load(Ordering::SeqCst), 0);
    assert_eq!(slice.timestamp(0), Some(3));
    assert_eq!(slice.timestamp(1), Some(5));
    drop(slice);
    assert_eq!(first_drops.load(Ordering::SeqCst), 1);
    assert_eq!(second_drops.load(Ordering::SeqCst), 1);
}

/// The axis of the sparse tests: positions 1..3 and 5..6 of a six-point grid.
fn sparse_grid() -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(10, 10, 6).unwrap()).unwrap())
}

fn sparse_axis() -> TimeAxis {
    TimeAxis::GridRanges(
        GridRanges::try_new(
            sparse_grid(),
            vec![GridSpan::new(1, 2), GridSpan::new(5, 1)],
        )
        .unwrap(),
    )
}

#[test]
fn sparse_axis_counts_carried_rows_and_maps_them_to_timestamps() {
    let axis = sparse_axis();
    // Three rows over the six positions from the first to the last, so `len` is not the envelope.
    assert_eq!(axis.len(), 3);
    assert_eq!(axis.first_position(), Some(1));
    assert_eq!(axis.last_position(), Some(5));
    assert!(!axis.is_contiguous());
    assert_eq!(
        axis.grid().map(|grid| grid.spec()),
        Some(sparse_grid().spec())
    );
    assert_eq!(axis.timestamp(0), Some(20));
    assert_eq!(axis.timestamp(1), Some(30));
    assert_eq!(axis.timestamp(2), Some(60));
    assert_eq!(axis.timestamp(3), None);

    let dense = TimeAxis::Grid(GridRange::try_new(sparse_grid(), 1, 2).unwrap());
    assert_eq!(dense.len(), 2);
    assert!(dense.is_contiguous());
    assert_eq!(
        (dense.first_position(), dense.last_position()),
        (Some(1), Some(2))
    );
}

#[test]
fn sparse_axis_slices_stay_compact_and_keep_the_grid() {
    let axis = sparse_axis();
    // Inside one span: the result is the single-range fast path.
    let inside = axis.slice(2, 1).unwrap();
    let TimeAxis::Grid(range) = &inside else {
        panic!("a slice inside one span is not a grid range")
    };
    assert_eq!((range.start(), range.len()), (5, 1));
    assert_eq!(inside.timestamp(0), Some(60));

    // Across the gap: rows 1 and 2 are positions 2 and 5, not positions 2, 3, 4 and 5.
    let across = axis.slice(1, 2).unwrap();
    let TimeAxis::GridRanges(_) = &across else {
        panic!("a slice across a gap stayed a grid range")
    };
    assert_eq!(
        across
            .runs()
            .map(|run| (run.position, run.row, run.len))
            .collect::<Vec<_>>(),
        vec![(2, 0, 1), (5, 1, 1)]
    );
    assert_eq!(across.len(), 2);
    assert_eq!(
        (across.timestamp(0), across.timestamp(1)),
        (Some(30), Some(60))
    );

    // A slice that starts and ends in the same span still shares the grid.
    let whole = axis.slice(0, 2).unwrap();
    let TimeAxis::Grid(range) = &whole else {
        panic!("a slice inside the first span is not a grid range")
    };
    assert_eq!((range.start(), range.len()), (1, 2));

    // Empty slices keep the grid: at a row, and right after the last row.
    for (offset, position) in [(0, 1), (2, 5), (3, 6)] {
        let empty = axis.slice(offset, 0).unwrap();
        let TimeAxis::Grid(range) = &empty else {
            panic!("an empty slice lost the grid range it was cut at")
        };
        assert!(range.is_empty());
        assert_eq!(range.start(), position);
        assert_eq!(range.grid().spec(), sparse_grid().spec());
    }
    let (offset, len) = (3, 0);
    assert!(axis.slice(offset, len).is_ok());
    assert!(axis.slice(3, 1).is_err());
    assert!(axis.slice(4, 0).is_err());
    assert!(axis.slice(2, usize::MAX).is_err());

    // A nested slice of a sparse slice keeps working.
    let nested = across.slice(1, 1).unwrap();
    assert_eq!(nested.timestamp(0), Some(60));
}

#[test]
fn sparse_axis_positions_walk_every_row_once_in_order() {
    let axis = sparse_axis();
    assert_eq!(
        axis.positions().collect::<Vec<_>>(),
        vec![(1, 0), (2, 1), (5, 2)]
    );
    let dense = TimeAxis::Grid(GridRange::try_new(sparse_grid(), 2, 2).unwrap());
    assert_eq!(dense.positions().collect::<Vec<_>>(), vec![(2, 0), (3, 1)]);
    let explicit = TimeAxis::try_new(ScalarBuffer::from(vec![1_i64, 3])).unwrap();
    // Without a grid a row sits at its own position.
    assert_eq!(
        explicit.positions().collect::<Vec<_>>(),
        vec![(0, 0), (1, 1)]
    );
    let mut positions = axis.positions();
    assert_eq!(positions.len(), axis.len());
    assert_eq!(positions.next(), Some((1, 0)));
    assert_eq!(positions.len(), 2, "a consumed row is no longer counted");
    // The last row of the first span.
    assert_eq!(positions.next(), Some((2, 1)));
    assert_eq!(positions.len(), 1);
    // The next row is in the second span, across the gap.
    assert_eq!(positions.next(), Some((5, 2)));
    assert_eq!(positions.len(), 0);
    assert_eq!(positions.next(), None);
    assert_eq!(positions.len(), 0, "an exhausted walk reports nothing left");
}

#[test]
fn sparse_axis_row_of_answers_carried_positions_only() {
    let axis = sparse_axis();
    for (position, row) in [(1, 0), (2, 1), (5, 2)] {
        assert_eq!(axis.row_of(position), Some(row));
    }
    for position in [0, 3, 4, 6, 9] {
        assert_eq!(axis.row_of(position), None, "position {position}");
    }
    let explicit = TimeAxis::try_new(ScalarBuffer::from(vec![1_i64, 3])).unwrap();
    assert_eq!(explicit.row_of(1), Some(1));
    assert_eq!(explicit.row_of(2), None);
}

#[test]
fn sparse_spans_are_validated() {
    let grid = sparse_grid();
    let build = |spans: Vec<GridSpan>| GridRanges::try_new(grid.clone(), spans);
    assert!(build(vec![GridSpan::new(0, 1), GridSpan::new(2, 3)]).is_ok());
    // Adjacent runs are allowed.
    assert!(build(vec![GridSpan::new(0, 2), GridSpan::new(2, 2)]).is_ok());
    // A span may end at the grid end, which is index 6 on this grid.
    assert!(build(vec![GridSpan::new(4, 2)]).is_ok());
    assert!(build(vec![GridSpan::new(5, 1)]).is_ok());

    // Overlapping and out-of-order spans.
    assert!(build(vec![GridSpan::new(1, 2), GridSpan::new(2, 1)]).is_err());
    assert!(build(vec![GridSpan::new(2, 1), GridSpan::new(1, 1)]).is_err());
    // Past the grid end.
    assert!(build(vec![GridSpan::new(5, 2)]).is_err());
    assert!(build(vec![GridSpan::new(6, 1)]).is_err());
    // Zero-length spans, alone and in a list.
    assert!(build(vec![GridSpan::new(1, 0)]).is_err());
    assert!(build(vec![GridSpan::new(1, 1), GridSpan::new(3, 0)]).is_err());
    // Index and length arithmetic is checked.
    assert!(build(vec![GridSpan::new(usize::MAX, 1)]).is_err());
    assert!(build(vec![GridSpan::new(usize::MAX - 1, 2)]).is_err());
    assert!(build(vec![GridSpan::new(usize::MAX, usize::MAX)]).is_err());
}

#[test]
fn empty_sparse_axis_keeps_its_grid() {
    let axis = TimeAxis::GridRanges(GridRanges::empty(sparse_grid()));
    assert!(axis.is_empty());
    assert_eq!(axis.len(), 0);
    assert!(!axis.is_contiguous());
    assert_eq!(axis.first_position(), None);
    assert_eq!(axis.last_position(), None);
    assert_eq!(
        axis.grid().map(|grid| grid.spec()),
        Some(sparse_grid().spec())
    );
    assert_eq!(axis.timestamp(0), None);
    assert_eq!(axis.row_of(0), None);
    assert_eq!(axis.positions().collect::<Vec<_>>(), Vec::new());

    // Slicing an empty axis keeps it empty and on its grid.
    let empty = axis.slice(0, 0).unwrap();
    let TimeAxis::GridRanges(ranges) = &empty else {
        panic!("an empty sparse slice invented a position")
    };
    assert_eq!(ranges.len(), 0);
    assert_eq!(ranges.grid().spec(), sparse_grid().spec());
}

#[test]
fn sparse_runs_are_the_carried_rows_and_intersect_by_a_merge_walk() {
    let axis = sparse_axis();
    let runs: Vec<_> = axis.runs().collect();
    assert_eq!(runs.len(), 2);
    assert_eq!((runs[0].position, runs[0].row, runs[0].len), (1, 0, 2));
    assert_eq!((runs[1].position, runs[1].row, runs[1].len), (5, 2, 1));

    // An axis sharing the middle row and the last position with `axis`.
    let other = TimeAxis::GridRanges(
        GridRanges::try_new(
            sparse_grid(),
            vec![GridSpan::new(2, 1), GridSpan::new(5, 1)],
        )
        .unwrap(),
    );
    let shared: Vec<_> = axis.intersect_runs(&other).collect();
    assert_eq!(shared.len(), 2);
    assert_eq!(
        (
            shared[0].position,
            shared[0].row,
            shared[0].other_row,
            shared[0].len
        ),
        (2, 1, 0, 1)
    );
    assert_eq!(
        (
            shared[1].position,
            shared[1].row,
            shared[1].other_row,
            shared[1].len
        ),
        (5, 2, 1, 1)
    );

    // An overlapping stretch of several rows is one shared run with two row offsets.
    let left = TimeAxis::GridRanges(
        GridRanges::try_new(sparse_grid(), vec![GridSpan::new(0, 4)]).unwrap(),
    );
    let right = TimeAxis::GridRanges(
        GridRanges::try_new(sparse_grid(), vec![GridSpan::new(1, 3)]).unwrap(),
    );
    let shared: Vec<_> = left.intersect_runs(&right).collect();
    assert_eq!(shared.len(), 1);
    assert_eq!(
        (
            shared[0].position,
            shared[0].row,
            shared[0].other_row,
            shared[0].len
        ),
        (1, 1, 0, 3)
    );

    // Disjoint axes share nothing, and an empty axis shares nothing either.
    let far = TimeAxis::GridRanges(
        GridRanges::try_new(sparse_grid(), vec![GridSpan::new(3, 2)]).unwrap(),
    );
    assert_eq!(axis.intersect_runs(&far).count(), 0);
    let empty = TimeAxis::GridRanges(GridRanges::empty(sparse_grid()));
    assert_eq!(axis.intersect_runs(&empty).count(), 0);
    assert_eq!(empty.intersect_runs(&axis).count(), 0);

    // A dense axis is one run, so its intersection with a sparse axis is the sparse rows.
    let dense = TimeAxis::Grid(GridRange::try_new(sparse_grid(), 0, 6).unwrap());
    let shared: Vec<_> = dense.intersect_runs(&axis).collect();
    assert_eq!(
        shared
            .iter()
            .map(|pair| (pair.position, pair.row, pair.other_row, pair.len))
            .collect::<Vec<_>>(),
        vec![(1, 1, 0, 2), (5, 5, 2, 1)]
    );
}

#[test]
fn from_grid_spans_keeps_the_single_span_fast_path() {
    let grid = sparse_grid();
    let single = TimeAxis::from_grid_spans(grid.clone(), vec![GridSpan::new(1, 2)]).unwrap();
    let TimeAxis::Grid(range) = &single else {
        panic!("one span is a grid range")
    };
    assert_eq!((range.start(), range.len()), (1, 2));

    let several =
        TimeAxis::from_grid_spans(grid.clone(), vec![GridSpan::new(1, 1), GridSpan::new(5, 1)])
            .unwrap();
    assert!(matches!(several, TimeAxis::GridRanges(_)));
    assert_eq!(several.len(), 2);

    let none = TimeAxis::from_grid_spans(grid, Vec::new()).unwrap();
    assert!(none.is_empty());
    assert_eq!(
        none.grid().map(|grid| grid.spec()),
        Some(sparse_grid().spec())
    );
    // The same validation as the checked constructor.
    assert!(TimeAxis::from_grid_spans(sparse_grid(), vec![GridSpan::new(5, 3)]).is_err());
}

// Tests of the fragmented grid axis as an end-to-end contract: what a slice, a walk and a point
// lookup say about a series that carries only the positions holding a value.
mod acceptance {
    use std::sync::Arc;

    use arrow::array::Float64Array;

    use super::super::{EvalGrid, GridRanges, GridSpan, GridSpec, TimeAxis};
    use crate::series::MetricName;
    use crate::series::samples::{
        FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
    };

    /// The grid of the fragmented-series fixture: twelve positions, ten milliseconds apart.
    fn fixture_grid() -> Arc<EvalGrid> {
        Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 10, 12).unwrap()).unwrap())
    }

    /// A two-field series over positions 1, 2, 3, 6, 7 and 9 of [`fixture_grid`]: three runs, and two
    /// fields that disagree about which rows hold a value.
    ///
    /// The first field holds a value everywhere; the second has a hole, a NaN -- a *value*, not a
    /// hole -- and another hole, so a slice that shifts by one row changes what it reads.
    fn fragmented_series() -> (Arc<SeriesSchema>, Series) {
        let grid = fixture_grid();
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
            eval_grid: Some(grid.clone()),
        });
        let time = TimeAxis::from_grid_spans(
            grid,
            vec![
                GridSpan::new(1, 3),
                GridSpan::new(6, 2),
                GridSpan::new(9, 1),
            ],
        )
        .unwrap();
        let a = Samples::from_float(Float64Array::from(vec![11.0, 12.0, 13.0, 16.0, 17.0, 19.0]));
        let b = Samples::from_float(Float64Array::from(vec![
            None,
            Some(f64::NAN),
            Some(13.0),
            Some(16.0),
            None,
            Some(19.0),
        ]));
        let series = Series::try_new(
            &schema,
            MetricName::try_new(Some(Arc::from("m")), vec![]).unwrap(),
            time,
            SeriesFields::try_from_vec(vec![a, b]).unwrap(),
        )
        .unwrap();
        (schema, series)
    }

    /// One sample read of two fields is the same reading: same kind, same bits, and any NaN as NaN,
    /// the way results are compared elsewhere in the tree.
    fn same_reading(a: SampleRef<'_>, b: SampleRef<'_>) -> bool {
        match (a, b) {
            (SampleRef::Missing, SampleRef::Missing) => true,
            (SampleRef::Float(a), SampleRef::Float(b)) if a.is_nan() || b.is_nan() => {
                a.is_nan() && b.is_nan()
            }
            (SampleRef::Float(a), SampleRef::Float(b)) => a.to_bits() == b.to_bits(),
            (SampleRef::Histogram(_), SampleRef::Histogram(_)) => false,
            _ => false,
        }
    }

    /// Slicing a fragmented series cuts the axis and every field at the same compact offset, keeps the
    /// absolute positions of the rows, and keeps the grid the rows were addressed on.
    ///
    /// The cases cover a slice inside one run (which is the single-range fast path), one that starts
    /// on the last row of a run and ends on the first row of the next, one spanning two runs, one at
    /// the very end of the axis, and the two empty slices -- inside the axis and one past its last
    /// row. A slice taken by the span start rather than by the row offset would read a neighbour's
    /// rows and fail the timestamp and field comparisons.
    #[test]
    fn slicing_a_fragmented_series_cuts_every_field_with_its_axis() {
        let (schema, series) = fragmented_series();
        assert_eq!(series.time.len(), 6);
        assert!(!series.time.is_contiguous());
        assert_eq!(
            series
                .time
                .runs()
                .map(|run| (run.position, run.len))
                .collect::<Vec<_>>(),
            vec![(1, 3), (6, 2), (9, 1)]
        );
        assert_eq!(
            series.time.positions().collect::<Vec<_>>(),
            vec![(1, 0), (2, 1), (3, 2), (6, 3), (7, 4), (9, 5)]
        );

        let cases: [(usize, usize, &str); 6] = [
            (1, 2, "inside one run"),
            (2, 2, "exactly across a run boundary"),
            (1, 4, "spanning two runs"),
            (4, 2, "at the very end"),
            (2, 0, "empty, between two rows"),
            (6, 0, "empty, one past the last row"),
        ];
        for (offset, len, context) in cases {
            let time = series
                .time
                .slice(offset, len)
                .unwrap_or_else(|error| panic!("{context}: slicing the axis failed: {error}"));
            let fields = series
                .fields
                .iter()
                .map(|field| {
                    field.slice(offset, len).unwrap_or_else(|error| {
                        panic!("{context}: slicing a field failed: {error}")
                    })
                })
                .collect::<Vec<_>>();
            let sliced = Series::try_new(
                &schema,
                series.metric.clone(),
                time,
                SeriesFields::try_from_vec(fields).expect("the fields stay equally long"),
            )
            .unwrap_or_else(|error| {
                panic!("{context}: the sliced series is inconsistent: {error}")
            });

            assert_eq!(sliced.time.len(), len, "{context}: the axis length");
            assert_eq!(sliced.fields.len(), 2, "{context}: a field was lost");
            for (index, field) in sliced.fields.iter().enumerate() {
                assert_eq!(field.len(), len, "{context}: field {index} length");
            }
            assert_eq!(
                sliced.time.grid().map(|grid| grid.spec()),
                series.time.grid().map(|grid| grid.spec()),
                "{context}: the slice lost its grid"
            );
            for index in 0..len {
                assert_eq!(
                    sliced.time.timestamp(index),
                    series.time.timestamp(offset + index),
                    "{context}: timestamp {index} moved"
                );
            }
            // The rows of the slice sit at the absolute positions the original rows sat at, so a
            // consumer that reads through the axis reaches the same instants.
            let original_positions: Vec<usize> = series
                .time
                .positions()
                .skip(offset)
                .take(len)
                .map(|(position, _)| position)
                .collect();
            assert_eq!(
                sliced
                    .time
                    .positions()
                    .map(|(position, _)| position)
                    .collect::<Vec<_>>(),
                original_positions,
                "{context}: the slice moved the rows"
            );
            for (position, row) in sliced.time.positions() {
                assert_eq!(
                    sliced.time.row_of(position),
                    Some(row),
                    "{context}: row_of disagrees with the walk at {position}"
                );
            }

            // Every field reads the samples the original field reads at `offset + row`.
            for (field_index, sliced_field) in sliced.fields.iter().enumerate() {
                let original_field = &series.fields.as_slice()[field_index];
                let mut original_cursor = original_field.cursor();
                let mut sliced_cursor = sliced_field.cursor();
                for row in 0..len {
                    let want = original_cursor
                        .at(offset + row)
                        .expect("the original field is long enough");
                    let got = sliced_cursor.at(row).expect("the slice is long enough");
                    assert!(
                        same_reading(want, got),
                        "{context}: field {field_index} row {row} (original row {}) reads differently",
                        offset + row
                    );
                }
            }
            if len == 0 {
                // An empty slice still names the grid it was cut from, and the series built on it is
                // accepted: `Series::try_new` checks the field lengths against the axis.
                assert!(sliced.time.is_empty(), "{context}: an empty slice is empty");
                assert!(
                    sliced
                        .time
                        .grid()
                        .is_some_and(|grid| grid.spec() == fixture_grid().spec()),
                    "{context}: an empty slice lost the grid"
                );
            }
        }

        // The two shapes the slice must keep: inside one run it stays the single-range fast path, and
        // across a boundary it becomes a span list -- never a filled-in range.
        assert!(
            series.time.slice(1, 2).unwrap().as_grid_range().is_some(),
            "a slice inside one run is a grid range"
        );
        let across = series.time.slice(2, 2).unwrap();
        assert!(
            across.as_grid_range().is_none(),
            "a slice across a run boundary is not one range: {across:?}"
        );
        assert_eq!(across.len(), 2);
        assert_eq!(
            across
                .runs()
                .map(|run| (run.position, run.len))
                .collect::<Vec<_>>(),
            vec![(3, 1), (6, 1)]
        );
        assert!(
            series.time.slice(6, 1).is_err(),
            "a slice past the last row is rejected"
        );
        assert!(
            series.time.slice(usize::MAX, 1).is_err(),
            "an overflowing slice is rejected"
        );
    }

    /// A highly fragmented axis: `k` runs of one to three rows, `V` rows in total, on a grid far
    /// longer than both.
    ///
    /// The descriptors scale with the runs and the rows with the values; nothing scales with the
    /// grid. What this test can observe is that shape: the run count, the row count, the number of
    /// items a walk yields, and which positions a point lookup answers for. It cannot observe the
    /// cost of the loops behind them, and it deliberately makes no timing assertion: an inner-loop
    /// cost is not a property of the API, so asserting on it would only measure the machine.
    #[test]
    fn a_fragmented_axis_costs_its_runs_and_rows_not_its_grid() {
        const GRID: usize = 100_000;
        let grid =
            Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1, GRID as u64).unwrap()).unwrap());
        // Runs of one to three rows, spread so that consecutive runs are always separated by at
        // least one position holding no row.
        let mut spans = Vec::new();
        let mut position = 0;
        let mut run_index = 0;
        while position < GRID && spans.len() < 20_000 {
            let len = 1 + run_index % 3;
            spans.push(GridSpan::new(position, len));
            position += len + 1 + run_index % 5;
            run_index += 1;
        }
        let runs = spans.len();
        let rows: usize = spans.iter().map(GridSpan::len).sum();
        assert_eq!(runs, 20_000, "the fixture is as fragmented as intended");
        assert!(rows > runs, "{rows} rows over {runs} runs");
        assert!(
            rows < GRID / 2,
            "{rows} rows over a grid of {GRID}: the axis carries a small part of it"
        );
        let axis = TimeAxis::GridRanges(GridRanges::try_new(grid.clone(), spans.clone()).unwrap());

        // Descriptors scale with the runs, not with the grid.
        assert_eq!(axis.runs().count(), runs, "one descriptor per run");
        assert_eq!(axis.len(), rows, "the axis carries one row per value");
        assert!(axis.len() < GRID, "the axis is not the grid");
        assert!(!axis.is_contiguous());

        // One walk visits exactly the carried rows, once each and in order: a walk that
        // materialised the holes would yield GRID items.
        let walked: Vec<(usize, usize)> = axis.positions().collect();
        assert_eq!(walked.len(), rows, "the walk yields the carried rows only");
        assert_ne!(walked.len(), GRID, "the walk did not visit the grid");
        assert_eq!(axis.positions().len(), rows, "the walk knows its length");
        assert!(walked.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert!(walked.iter().enumerate().all(|(row, &(_, r))| row == r));
        let expected_positions: Vec<usize> = spans
            .iter()
            .flat_map(|span| span.start()..span.start() + span.len())
            .collect();
        assert_eq!(
            walked
                .iter()
                .map(|&(position, _)| position)
                .collect::<Vec<_>>(),
            expected_positions
        );

        // Point lookups answer at the carried positions and nowhere else.
        for (row, &position) in expected_positions.iter().enumerate() {
            assert_eq!(
                axis.row_of(position),
                Some(row),
                "carried position {position}"
            );
        }
        for (index, span) in spans.iter().enumerate().take(200) {
            let hole = span.start() + span.len();
            assert_eq!(
                axis.row_of(hole),
                None,
                "run {index}: position {hole} holds no row"
            );
        }
        assert_eq!(axis.row_of(GRID), None, "past the grid");
        assert_eq!(axis.row_of(0), Some(0), "the first run starts at 0");
        // Every carried row resolves to a timestamp on the shared grid, which is what a consumer
        // reads instead of a timestamp per position.
        let last = *expected_positions.last().expect("the fixture carries rows");
        assert_eq!(axis.timestamp(rows - 1), Some(last as i64));
        assert_eq!(axis.timestamp(rows), None, "past the last row");
    }
}
