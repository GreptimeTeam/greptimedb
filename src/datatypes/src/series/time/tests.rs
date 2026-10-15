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

use super::{EvalGrid, GridRange, GridSpec, TimeAxis};

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
