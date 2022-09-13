mod replicate;

use crate::types::PrimitiveElement;
use crate::vectors::all::*;
use crate::vectors::VectorRef;

/// Vector compute operations.
pub trait VectorOp {
    /// Copies each element according `offsets` parameter.
    /// (`i-th` element should be copied `offsets[i] - offsets[i - 1]` times.)
    ///
    /// # Panics
    /// Panics if `offsets.len() != self.len()`.
    fn replicate(&self, offsets: &[usize]) -> VectorRef;
}

macro_rules! impl_vector_op {
    ($($VectorType: ident),+) => {$(
        impl VectorOp for $VectorType {
            fn replicate(&self, offsets: &[usize]) -> VectorRef {
                replicate::replicate_scalar(self, offsets)
            }
        }
    )+};
}

impl_vector_op!(BinaryVector, BooleanVector, ListVector, StringVector);

impl VectorOp for ConstantVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_constant(self, offsets)
    }
}

impl VectorOp for DateVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_date(self, offsets)
    }
}

impl VectorOp for DateTimeVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_datetime(self, offsets)
    }
}

impl VectorOp for NullVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_null(self, offsets)
    }
}

impl<T: PrimitiveElement> VectorOp for PrimitiveVector<T> {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_primitive(self, offsets)
    }
}

impl VectorOp for TimestampVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_timestamp(self, offsets)
    }
}
