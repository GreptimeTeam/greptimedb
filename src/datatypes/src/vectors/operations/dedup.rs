use arrow::bitmap::MutableBitmap;

use crate::scalars::ScalarVector;
use crate::vectors::{ConstantVector, NullVector, Vector};

pub(crate) fn dedup_scalar<'a, T: ScalarVector>(
    vector: &'a T,
    selected: &'a mut MutableBitmap,
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

    // Always retain the first element.
    selected.set(0, true);

    // Then check whether still keep the first element based last element in previous vector.
    if let Some(pv) = &prev_vector {
        if !pv.is_empty() {
            let last = pv.get_data(pv.len() - 1);
            if last == vector.get_data(0) {
                selected.set(0, false);
            }
        }
    }
}

pub(crate) fn dedup_null(
    vector: &NullVector,
    selected: &mut MutableBitmap,
    prev_vector: Option<&NullVector>,
) {
    if vector.is_empty() {
        return;
    }

    let no_prev_element = prev_vector.map(|v| v.is_empty()).unwrap_or(true);
    if no_prev_element {
        // Retain first element if no previous element (we known that it must
        // be null).
        selected.set(0, true);
    }
}

pub(crate) fn dedup_constant(
    vector: &ConstantVector,
    selected: &mut MutableBitmap,
    prev_vector: Option<&ConstantVector>,
) {
    if vector.is_empty() {
        return;
    }

    let equal_to_prev = if let Some(prev) = prev_vector {
        !prev.is_empty() && vector.get_constant_ref() == prev.get_constant_ref()
    } else {
        false
    };

    if !equal_to_prev {
        selected.set(0, true);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::vectors::{Int32Vector, StringVector, VectorOp};

    fn check_bitmap(expect: &[bool], selected: &MutableBitmap) {
        assert_eq!(expect.len(), selected.len());
        for (exp, v) in expect.iter().zip(selected.iter()) {
            assert_eq!(*exp, v);
        }
    }

    fn check_dedup_scalar(expect: &[bool], input: &[i32], prev: Option<&[i32]>) {
        check_dedup_scalar_opt(expect, input.iter().map(|v| Some(*v)), prev);
    }

    fn check_dedup_scalar_opt(
        expect: &[bool],
        input: impl Iterator<Item = Option<i32>>,
        prev: Option<&[i32]>,
    ) {
        let input = Int32Vector::from_iter(input);
        let prev = prev.map(Int32Vector::from_slice);

        let mut selected = MutableBitmap::from_len_zeroed(input.len());
        input.dedup(&mut selected, prev.as_ref().map(|v| v as _));

        check_bitmap(expect, &selected);
    }

    #[test]
    fn test_dedup_scalar() {
        check_dedup_scalar(&[], &[], None);
        check_dedup_scalar(&[true], &[1], None);
        check_dedup_scalar(&[true, false], &[1, 1], None);
        check_dedup_scalar(&[true, true], &[1, 2], None);
        check_dedup_scalar(&[true, true, true, true], &[1, 2, 3, 4], None);
        check_dedup_scalar(&[true, false, true, false], &[1, 1, 3, 3], None);
        check_dedup_scalar(&[true, false, false, false, true], &[2, 2, 2, 2, 3], None);

        check_dedup_scalar(&[true], &[5], Some(&[]));
        check_dedup_scalar(&[true], &[5], Some(&[3]));
        check_dedup_scalar(&[false], &[5], Some(&[5]));
        check_dedup_scalar(&[false], &[5], Some(&[4, 5]));
        check_dedup_scalar(&[false, true], &[5, 6], Some(&[4, 5]));
        check_dedup_scalar(&[false, true, false], &[5, 6, 6], Some(&[4, 5]));
        check_dedup_scalar(
            &[false, true, false, true, true],
            &[5, 6, 6, 7, 8],
            Some(&[4, 5]),
        );

        check_dedup_scalar_opt(
            &[true, true, false, true, false],
            [Some(1), Some(2), Some(2), None, None].into_iter(),
            None,
        );
    }

    fn check_dedup_null(len: usize) {
        let input = NullVector::new(len);
        let mut selected = MutableBitmap::from_len_zeroed(input.len());
        input.dedup(&mut selected, None);

        let mut expect = vec![false; len];
        if !expect.is_empty() {
            expect[0] = true;
        }
        check_bitmap(&expect, &selected);

        let mut selected = MutableBitmap::from_len_zeroed(input.len());
        let prev = Some(NullVector::new(1));
        input.dedup(&mut selected, prev.as_ref().map(|v| v as _));
        let expect = vec![false; len];
        check_bitmap(&expect, &selected);
    }

    #[test]
    fn test_dedup_null() {
        for len in 0..5 {
            check_dedup_null(len);
        }
    }

    fn check_dedup_constant(len: usize) {
        let input = ConstantVector::new(Arc::new(Int32Vector::from_slice(&[8])), len);
        let mut selected = MutableBitmap::from_len_zeroed(len);
        input.dedup(&mut selected, None);

        let mut expect = vec![false; len];
        if !expect.is_empty() {
            expect[0] = true;
        }
        check_bitmap(&expect, &selected);

        let mut selected = MutableBitmap::from_len_zeroed(len);
        let prev = Some(ConstantVector::new(
            Arc::new(Int32Vector::from_slice(&[8])),
            1,
        ));
        input.dedup(&mut selected, prev.as_ref().map(|v| v as _));
        let expect = vec![false; len];
        check_bitmap(&expect, &selected);
    }

    #[test]
    fn test_dedup_constant() {
        for len in 0..5 {
            check_dedup_constant(len);
        }
    }

    #[test]
    fn test_dedup_string() {
        let input = StringVector::from_slice(&["a", "a", "b", "c"]);
        let mut selected = MutableBitmap::from_len_zeroed(4);
        input.dedup(&mut selected, None);
        let expect = vec![true, false, true, true];
        check_bitmap(&expect, &selected);
    }

    macro_rules! impl_dedup_date_like_test {
        ($VectorType: ident, $ValueType: ident, $method: ident) => {{
            use common_time::$ValueType;
            use $crate::vectors::$VectorType;

            let v = $VectorType::from_iterator([8, 8, 9, 10].into_iter().map($ValueType::$method));
            let mut selected = MutableBitmap::from_len_zeroed(4);
            v.dedup(&mut selected, None);
            let expect = vec![true, false, true, true];
            check_bitmap(&expect, &selected);
        }};
    }

    #[test]
    fn test_dedup_date_like() {
        impl_dedup_date_like_test!(DateVector, Date, new);
        impl_dedup_date_like_test!(DateTimeVector, DateTime, new);
        impl_dedup_date_like_test!(TimestampVector, Timestamp, from_millis);
    }
}
