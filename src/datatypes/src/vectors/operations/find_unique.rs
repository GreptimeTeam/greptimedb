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

use common_base::BitVec;

use crate::scalars::ScalarVector;
use crate::vectors::constant::ConstantVector;
use crate::vectors::{NullVector, Vector};

// To implement `find_unique()` correctly, we need to keep in mind that always marks an element as
// selected when it is different from the previous one, and leaves the `selected` unchanged
// in any other case.
pub(crate) fn find_unique_scalar<'a, T: ScalarVector>(
    vector: &'a T,
    selected: &'a mut BitVec,
    prev_vector: Option<&'a T>,
) where
    T::RefItem<'a>: PartialEq,
{
    assert!(selected.len() >= vector.len());

    if vector.is_empty() {
        return;
    }

    for ((i, current), next) in vector
        .iter_data()
        .enumerate()
        .zip(vector.iter_data().skip(1))
    {
        if current != next {
            // If next element is a different element, we mark it as selected.
            selected.set(i + 1, true);
        }
    }

    // Marks first element as selected if it is different from previous element, otherwise
    // keep selected bitmap unchanged.
    let is_first_not_duplicate = prev_vector
        .map(|pv| {
            if pv.is_empty() {
                true
            } else {
                let last = pv.get_data(pv.len() - 1);
                last != vector.get_data(0)
            }
        })
        .unwrap_or(true);
    if is_first_not_duplicate {
        selected.set(0, true);
    }
}

pub(crate) fn find_unique_null(
    vector: &NullVector,
    selected: &mut BitVec,
    prev_vector: Option<&NullVector>,
) {
    if vector.is_empty() {
        return;
    }

    let is_first_not_duplicate = prev_vector.map(NullVector::is_empty).unwrap_or(true);
    if is_first_not_duplicate {
        selected.set(0, true);
    }
}

pub(crate) fn find_unique_constant(
    vector: &ConstantVector,
    selected: &mut BitVec,
    prev_vector: Option<&ConstantVector>,
) {
    if vector.is_empty() {
        return;
    }

    let is_first_not_duplicate = prev_vector
        .map(|pv| {
            if pv.is_empty() {
                true
            } else {
                vector.get_constant_ref() != pv.get_constant_ref()
            }
        })
        .unwrap_or(true);

    if is_first_not_duplicate {
        selected.set(0, true);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::{Date, DateTime};

    use super::*;
    use crate::timestamp::*;
    use crate::vectors::{Int32Vector, StringVector, Vector, VectorOp};

    fn check_bitmap(expect: &[bool], selected: &BitVec) {
        let actual = selected.iter().collect::<Vec<_>>();
        assert_eq!(expect, actual);
    }

    fn check_find_unique_scalar(expect: &[bool], input: &[i32], prev: Option<&[i32]>) {
        check_find_unique_scalar_opt(expect, input.iter().map(|v| Some(*v)), prev);
    }

    fn check_find_unique_scalar_opt(
        expect: &[bool],
        input: impl Iterator<Item = Option<i32>>,
        prev: Option<&[i32]>,
    ) {
        let input = Int32Vector::from(input.collect::<Vec<_>>());
        let prev = prev.map(Int32Vector::from_slice);

        let mut selected = BitVec::repeat(false, input.len());
        input.find_unique(&mut selected, prev.as_ref().map(|v| v as _));

        check_bitmap(expect, &selected);
    }

    #[test]
    fn test_find_unique_scalar() {
        check_find_unique_scalar(&[], &[], None);
        check_find_unique_scalar(&[true], &[1], None);
        check_find_unique_scalar(&[true, false], &[1, 1], None);
        check_find_unique_scalar(&[true, true], &[1, 2], None);
        check_find_unique_scalar(&[true, true, true, true], &[1, 2, 3, 4], None);
        check_find_unique_scalar(&[true, false, true, false], &[1, 1, 3, 3], None);
        check_find_unique_scalar(&[true, false, false, false, true], &[2, 2, 2, 2, 3], None);

        check_find_unique_scalar(&[true], &[5], Some(&[]));
        check_find_unique_scalar(&[true], &[5], Some(&[3]));
        check_find_unique_scalar(&[false], &[5], Some(&[5]));
        check_find_unique_scalar(&[false], &[5], Some(&[4, 5]));
        check_find_unique_scalar(&[false, true], &[5, 6], Some(&[4, 5]));
        check_find_unique_scalar(&[false, true, false], &[5, 6, 6], Some(&[4, 5]));
        check_find_unique_scalar(
            &[false, true, false, true, true],
            &[5, 6, 6, 7, 8],
            Some(&[4, 5]),
        );

        check_find_unique_scalar_opt(
            &[true, true, false, true, false],
            [Some(1), Some(2), Some(2), None, None].into_iter(),
            None,
        );
    }

    #[test]
    fn test_find_unique_scalar_multi_times_with_prev() {
        let prev = Int32Vector::from_slice([1]);

        let v1 = Int32Vector::from_slice([2, 3, 4]);
        let mut selected = BitVec::repeat(false, v1.len());
        v1.find_unique(&mut selected, Some(&prev));

        // Though element in v2 are the same as prev, but we should still keep them.
        let v2 = Int32Vector::from_slice([1, 1, 1]);
        v2.find_unique(&mut selected, Some(&prev));

        check_bitmap(&[true, true, true], &selected);
    }

    fn new_bitmap(bits: &[bool]) -> BitVec {
        BitVec::from_iter(bits)
    }

    #[test]
    fn test_find_unique_scalar_with_prev() {
        let prev = Int32Vector::from_slice([1]);

        let mut selected = new_bitmap(&[true, false, true, false]);
        let v = Int32Vector::from_slice([2, 3, 4, 5]);
        v.find_unique(&mut selected, Some(&prev));
        // All elements are different.
        check_bitmap(&[true, true, true, true], &selected);

        let mut selected = new_bitmap(&[true, false, true, false]);
        let v = Int32Vector::from_slice([1, 2, 3, 4]);
        v.find_unique(&mut selected, Some(&prev));
        // Though first element is duplicate, but we keep the flag unchanged.
        check_bitmap(&[true, true, true, true], &selected);

        // Same case as above, but now `prev` is None.
        let mut selected = new_bitmap(&[true, false, true, false]);
        let v = Int32Vector::from_slice([1, 2, 3, 4]);
        v.find_unique(&mut selected, None);
        check_bitmap(&[true, true, true, true], &selected);

        // Same case as above, but now `prev` is empty.
        let mut selected = new_bitmap(&[true, false, true, false]);
        let v = Int32Vector::from_slice([1, 2, 3, 4]);
        v.find_unique(&mut selected, Some(&Int32Vector::from_slice([])));
        check_bitmap(&[true, true, true, true], &selected);

        let mut selected = new_bitmap(&[false, false, false, false]);
        let v = Int32Vector::from_slice([2, 2, 4, 5]);
        v.find_unique(&mut selected, Some(&prev));
        // only v[1] is duplicate.
        check_bitmap(&[true, false, true, true], &selected);
    }

    fn check_find_unique_null(len: usize) {
        let input = NullVector::new(len);
        let mut selected = BitVec::repeat(false, input.len());
        input.find_unique(&mut selected, None);

        let mut expect = vec![false; len];
        if !expect.is_empty() {
            expect[0] = true;
        }
        check_bitmap(&expect, &selected);

        let mut selected = BitVec::repeat(false, input.len());
        let prev = Some(NullVector::new(1));
        input.find_unique(&mut selected, prev.as_ref().map(|v| v as _));
        let expect = vec![false; len];
        check_bitmap(&expect, &selected);
    }

    #[test]
    fn test_find_unique_null() {
        for len in 0..5 {
            check_find_unique_null(len);
        }
    }

    #[test]
    fn test_find_unique_null_with_prev() {
        let prev = NullVector::new(1);

        // Keep flags unchanged.
        let mut selected = new_bitmap(&[true, false, true, false]);
        let v = NullVector::new(4);
        v.find_unique(&mut selected, Some(&prev));
        check_bitmap(&[true, false, true, false], &selected);

        // Keep flags unchanged.
        let mut selected = new_bitmap(&[false, false, true, false]);
        v.find_unique(&mut selected, Some(&prev));
        check_bitmap(&[false, false, true, false], &selected);

        // Prev is None, select first element.
        let mut selected = new_bitmap(&[false, false, true, false]);
        v.find_unique(&mut selected, None);
        check_bitmap(&[true, false, true, false], &selected);

        // Prev is empty, select first element.
        let mut selected = new_bitmap(&[false, false, true, false]);
        v.find_unique(&mut selected, Some(&NullVector::new(0)));
        check_bitmap(&[true, false, true, false], &selected);
    }

    fn check_find_unique_constant(len: usize) {
        let input = ConstantVector::new(Arc::new(Int32Vector::from_slice([8])), len);
        let mut selected = BitVec::repeat(false, len);
        input.find_unique(&mut selected, None);

        let mut expect = vec![false; len];
        if !expect.is_empty() {
            expect[0] = true;
        }
        check_bitmap(&expect, &selected);

        let mut selected = BitVec::repeat(false, len);
        let prev = Some(ConstantVector::new(
            Arc::new(Int32Vector::from_slice([8])),
            1,
        ));
        input.find_unique(&mut selected, prev.as_ref().map(|v| v as _));
        let expect = vec![false; len];
        check_bitmap(&expect, &selected);
    }

    #[test]
    fn test_find_unique_constant() {
        for len in 0..5 {
            check_find_unique_constant(len);
        }
    }

    #[test]
    fn test_find_unique_constant_with_prev() {
        let prev = ConstantVector::new(Arc::new(Int32Vector::from_slice([1])), 1);

        // Keep flags unchanged.
        let mut selected = new_bitmap(&[true, false, true, false]);
        let v = ConstantVector::new(Arc::new(Int32Vector::from_slice([1])), 4);
        v.find_unique(&mut selected, Some(&prev));
        check_bitmap(&[true, false, true, false], &selected);

        // Keep flags unchanged.
        let mut selected = new_bitmap(&[false, false, true, false]);
        v.find_unique(&mut selected, Some(&prev));
        check_bitmap(&[false, false, true, false], &selected);

        // Prev is None, select first element.
        let mut selected = new_bitmap(&[false, false, true, false]);
        v.find_unique(&mut selected, None);
        check_bitmap(&[true, false, true, false], &selected);

        // Prev is empty, select first element.
        let mut selected = new_bitmap(&[false, false, true, false]);
        v.find_unique(
            &mut selected,
            Some(&ConstantVector::new(
                Arc::new(Int32Vector::from_slice([1])),
                0,
            )),
        );
        check_bitmap(&[true, false, true, false], &selected);

        // Different constant vector.
        let mut selected = new_bitmap(&[false, false, true, false]);
        let v = ConstantVector::new(Arc::new(Int32Vector::from_slice([2])), 4);
        v.find_unique(&mut selected, Some(&prev));
        check_bitmap(&[true, false, true, false], &selected);
    }

    #[test]
    fn test_find_unique_string() {
        let input = StringVector::from_slice(&["a", "a", "b", "c"]);
        let mut selected = BitVec::repeat(false, 4);
        input.find_unique(&mut selected, None);
        let expect = vec![true, false, true, true];
        check_bitmap(&expect, &selected);
    }

    macro_rules! impl_find_unique_date_like_test {
        ($VectorType: ident, $ValueType: ident, $method: ident) => {{
            use $crate::vectors::$VectorType;

            let v = $VectorType::from_iterator([8, 8, 9, 10].into_iter().map($ValueType::$method));
            let mut selected = BitVec::repeat(false, 4);
            v.find_unique(&mut selected, None);
            let expect = vec![true, false, true, true];
            check_bitmap(&expect, &selected);
        }};
    }

    #[test]
    fn test_find_unique_date_like() {
        impl_find_unique_date_like_test!(DateVector, Date, new);
        impl_find_unique_date_like_test!(DateTimeVector, DateTime, new);
        impl_find_unique_date_like_test!(TimestampSecondVector, TimestampSecond, from);
        impl_find_unique_date_like_test!(TimestampMillisecondVector, TimestampMillisecond, from);
        impl_find_unique_date_like_test!(TimestampMicrosecondVector, TimestampMicrosecond, from);
        impl_find_unique_date_like_test!(TimestampNanosecondVector, TimestampNanosecond, from);
    }
}
