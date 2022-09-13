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

    let has_prev_element = prev_vector.map(|v| !v.is_empty()).unwrap_or(false);
    if has_prev_element {
        // Retain first element if previous element exists (we known that it must
        // be null).
        selected.set(0, true);
    }
}

pub(crate) fn dedup_constant(
    vector: &ConstantVector,
    selected: &mut MutableBitmap,
    prev_vector: Option<&ConstantVector>,
) {
    let inner = vector.inner();
    let prev_inner = prev_vector.map(|pv| &**pv.inner());
    inner.dedup(selected, prev_inner);
}
