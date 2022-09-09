use arrow::bitmap::MutableBitmap;

use crate::scalars::ScalarVector;
use crate::vectors::{NullVector, Vector};

/// Match `vector` and apply `body` with scalar vector type `T` if `vector` is a
/// scalar vector, or apply `nbody` if the vector is a `NullVector`.
macro_rules! match_scalar_vector {
    ($vector: ident, | $_: tt $T: ident | $body: tt, $nbody: tt) => {{
        use $crate::data_type::ConcreteDataType;
        use $crate::vectors::all::*;

        macro_rules! __with_ty_scalar__ {
            ( $_ $T: ident ) => {
                $body
            };
        }

        match $vector.data_type() {
            ConcreteDataType::Null(_) => $nbody
            ConcreteDataType::Boolean(_) => __with_ty_scalar__! { BooleanVector },
            ConcreteDataType::Int8(_) => __with_ty_scalar__! { Int8Vector },
            ConcreteDataType::Int16(_) => __with_ty_scalar__! { Int16Vector },
            ConcreteDataType::Int32(_) => __with_ty_scalar__! { Int32Vector },
            ConcreteDataType::Int64(_) => __with_ty_scalar__! { Int64Vector },
            ConcreteDataType::UInt8(_) => __with_ty_scalar__! { UInt8Vector },
            ConcreteDataType::UInt16(_) => __with_ty_scalar__! { UInt16Vector },
            ConcreteDataType::UInt32(_) => __with_ty_scalar__! { UInt32Vector },
            ConcreteDataType::UInt64(_) => __with_ty_scalar__! { UInt64Vector },
            ConcreteDataType::Float32(_) => __with_ty_scalar__! { Float32Vector },
            ConcreteDataType::Float64(_) => __with_ty_scalar__! { Float64Vector },
            ConcreteDataType::Binary(_) => __with_ty_scalar__! { BinaryVector },
            ConcreteDataType::String(_) => __with_ty_scalar__! { StringVector },
            ConcreteDataType::Date(_) => __with_ty_scalar__! { DateVector },
            ConcreteDataType::DateTime(_) => __with_ty_scalar__! { DateTimeVector },
            ConcreteDataType::Timestamp(_) => __with_ty_scalar__! { TimestampVector },
            ConcreteDataType::List(_) => __with_ty_scalar__! { ListVector },
        }
    }};
}

/// Dedup elements in `vector` and mark `i-th` bit of `selected` to `true` if the `i-th` element
/// of `vector` is retained.
///
/// If there are multiple duplicate elements, this function retains the **first** element.
/// If the first element of `vector` is equal to the last element of `prev_vector`, then that
/// first element is also considered as duplicated and won't be retained.
/// The caller should ensure the `selected` bitmap is intialized by setting `[0, vector.len())`
/// bits to false.
///
/// # Panics
/// Panics if `selected.len() < vector.len()`.
fn dedup(vector: &dyn Vector, prev_vector: Option<&dyn Vector>, selected: &mut MutableBitmap) {
    assert!(selected.len() >= vector.len());

    let mut compute = Dedup {
        prev_vector,
        selected,
    };
    match_scalar_vector!(vector, |$S| {
        let v = vector.as_any().downcast_ref::<$S>().unwrap();
        compute.compute_scalar(v)
    },
    {
        let v = vector.as_any().downcast_ref::<NullVector>().unwrap();
        compute.compute_null(v)
    })
}

struct Dedup<'a> {
    /// Previous vector.
    prev_vector: Option<&'a dyn Vector>,
    /// Denotes which element in vector should be retained.
    ///
    /// `selected` should be filled by `false` at initialization.
    selected: &'a mut MutableBitmap,
}

impl<'a> Dedup<'a> {
    fn compute_scalar<'b, T: ScalarVector>(&'a mut self, vector: &'b T)
    where
        T::RefItem<'b>: PartialEq,
        'a: 'b,
    {
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
                self.selected.set(i + 1, true);
            }
        }

        // Always retain the first element.
        self.selected.set(0, true);

        // Then check whether still keep the first element based last element in previous vector.
        if let Some(prev_vector) = &self.prev_vector {
            let prev_vector = prev_vector.as_any().downcast_ref::<T>().unwrap();
            if !prev_vector.is_empty() {
                let last = prev_vector.get_data(prev_vector.len() - 1);
                if last == vector.get_data(0) {
                    self.selected.set(0, false);
                }
            }
        }
    }

    fn compute_null(&mut self, vector: &NullVector) {
        if vector.is_empty() {
            return;
        }

        let has_prev_element = self.prev_vector.map(|v| !v.is_empty()).unwrap_or(false);
        if has_prev_element {
            // Retain first element if previous element exists (we known that it must
            // be null).
            self.selected.set(0, true);
        }
    }
}
