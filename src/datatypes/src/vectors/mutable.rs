use std::any::Any;

use crate::error::Result;
use crate::prelude::*;

/// Mutable vector that could be used to build an immutable vector.
pub trait MutableVector: Send + Sync {
    /// Returns the data type of the vector.
    fn data_type(&self) -> ConcreteDataType;

    /// Returns the length of the vector.
    fn len(&self) -> usize;

    /// Returns whether the vector is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert to Any, to enable dynamic casting.
    fn as_any(&self) -> &dyn Any;

    /// Convert to mutable Any, to enable dynamic casting.
    fn as_mut_any(&mut self) -> &mut dyn Any;

    /// Convert `self` to an (immutable) [VectorRef] and reset `self`.
    fn to_vector(&mut self) -> VectorRef;

    /// Push value ref to this mutable vector.
    ///
    /// # Panics
    /// Panics if
    /// - The data type of `vector` is different from this mutable vector's.
    fn push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        // FIXME(yingwen): Remove this.
        unimplemented!()
    }

    /// Extend this mutable vector by slice of `vector`.
    ///
    /// # Panics
    /// Panics if
    /// - `offset + length > vector.len()`.
    /// - The data type of `vector` is different from this mutable vector's.
    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        // FIXME(yingwen): Remove this.
        unimplemented!()
    }
}
