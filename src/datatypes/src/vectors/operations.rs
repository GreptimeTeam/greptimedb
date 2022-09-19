mod dedup;
mod filter;
mod replicate;

use arrow::bitmap::MutableBitmap;

use crate::error::Result;
use crate::types::PrimitiveElement;
use crate::vectors::all::*;
use crate::vectors::{Vector, VectorRef};

/// Vector compute operations.
pub trait VectorOp {
    /// Copies each element according `offsets` parameter.
    /// (`i-th` element should be copied `offsets[i] - offsets[i - 1]` times.)
    ///
    /// # Panics
    /// Panics if `offsets.len() != self.len()`.
    fn replicate(&self, offsets: &[usize]) -> VectorRef;

    /// Dedup elements in `self` and mark `i-th` bit of `selected` to `true` if the `i-th` element
    /// of `self` is retained.
    ///
    /// The caller should ensure
    /// 1. the `selected` bitmap is intialized by setting `[0, vector.len())`
    /// bits to false.
    /// 2. `vector` and `prev_vector` are sorted.
    ///
    /// If there are multiple duplicate elements, this function retains the **first** element.
    /// If the first element of `self` is equal to the last element of `prev_vector`, then that
    /// first element is also considered as duplicated and won't be retained.
    ///
    /// # Panics
    /// Panics if
    /// - `selected.len() < self.len()`.
    /// - `prev_vector` and `self` have different data types.
    fn dedup(&self, selected: &mut MutableBitmap, prev_vector: Option<&dyn Vector>);

    /// Filters the vector, returns elements matching the `filter` (i.e. where the values are true).
    ///
    /// Note that the nulls of `filter` are interpreted as `false` will lead to these elements being masked out.
    fn filter(&self, filter: &BooleanVector) -> Result<VectorRef>;
}

macro_rules! impl_scalar_vector_op {
    ($( { $VectorType: ident, $replicate: ident } ),+) => {$(
        impl VectorOp for $VectorType {
            fn replicate(&self, offsets: &[usize]) -> VectorRef {
                replicate::$replicate(self, offsets)
            }

            fn dedup(&self, selected: &mut MutableBitmap, prev_vector: Option<&dyn Vector>) {
                let prev_vector = prev_vector.map(|pv| pv.as_any().downcast_ref::<$VectorType>().unwrap());
                dedup::dedup_scalar(self, selected, prev_vector);
            }

            fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
                filter::filter_non_constant!(self, $VectorType, filter)
            }
        }
    )+};
}

impl_scalar_vector_op!(
    { BinaryVector, replicate_scalar },
    { BooleanVector, replicate_scalar },
    { ListVector, replicate_scalar },
    { StringVector, replicate_scalar },
    { DateVector, replicate_date },
    { DateTimeVector, replicate_datetime },
    { TimestampVector, replicate_timestamp }
);

impl VectorOp for ConstantVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_constant(self, offsets)
    }

    fn dedup(&self, selected: &mut MutableBitmap, prev_vector: Option<&dyn Vector>) {
        let prev_vector = prev_vector.and_then(|pv| pv.as_any().downcast_ref::<ConstantVector>());
        dedup::dedup_constant(self, selected, prev_vector);
    }

    fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
        filter::filter_constant(self, filter)
    }
}

impl VectorOp for NullVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_null(self, offsets)
    }

    fn dedup(&self, selected: &mut MutableBitmap, prev_vector: Option<&dyn Vector>) {
        let prev_vector = prev_vector.and_then(|pv| pv.as_any().downcast_ref::<NullVector>());
        dedup::dedup_null(self, selected, prev_vector);
    }

    fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
        filter::filter_non_constant!(self, NullVector, filter)
    }
}

impl<T> VectorOp for PrimitiveVector<T>
where
    T: PrimitiveElement,
{
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_primitive(self, offsets)
    }

    fn dedup(&self, selected: &mut MutableBitmap, prev_vector: Option<&dyn Vector>) {
        let prev_vector =
            prev_vector.and_then(|pv| pv.as_any().downcast_ref::<PrimitiveVector<T>>());
        dedup::dedup_scalar(self, selected, prev_vector);
    }

    fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
        filter::filter_non_constant!(self, PrimitiveVector<T>, filter)
    }
}
