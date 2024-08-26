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

mod cast;
mod filter;
mod find_unique;
mod replicate;
mod take;

use common_base::BitVec;

use crate::error::{self, Result};
use crate::types::LogicalPrimitiveType;
use crate::vectors::constant::ConstantVector;
use crate::vectors::{
    BinaryVector, BooleanVector, ConcreteDataType, Decimal128Vector, ListVector, NullVector,
    PrimitiveVector, StringVector, UInt32Vector, Vector, VectorRef,
};

/// Vector compute operations.
pub trait VectorOp {
    /// Copies each element according `offsets` parameter.
    /// - `i-th` element should be copied `offsets[i] - offsets[i - 1]` times
    /// - `0-th` element would be copied `offsets[0]` times
    ///
    /// # Panics
    /// Panics if `offsets.len() != self.len()`.
    fn replicate(&self, offsets: &[usize]) -> VectorRef;

    /// Mark `i-th` bit of `selected` to `true` if the `i-th` element of `self` is unique, which
    /// means there is no elements behind it have same value as it.
    ///
    /// The caller should ensure
    /// 1. the length of `selected` bitmap is equal to `vector.len()`.
    /// 2. `vector` and `prev_vector` are sorted.
    ///
    /// If there are multiple duplicate elements, this function retains the **first** element.
    /// The first element is considered as unique if the first element of `self` is different
    /// from its previous element, that is the last element of `prev_vector`.
    ///
    /// # Panics
    /// Panics if
    /// - `selected.len() < self.len()`.
    /// - `prev_vector` and `self` have different data types.
    fn find_unique(&self, selected: &mut BitVec, prev_vector: Option<&dyn Vector>);

    /// Filters the vector, returns elements matching the `filter` (i.e. where the values are true).
    ///
    /// Note that the nulls of `filter` are interpreted as `false` will lead to these elements being masked out.
    fn filter(&self, filter: &BooleanVector) -> Result<VectorRef>;

    /// Cast vector to the provided data type and return a new vector with type to_type, if possible.
    ///
    /// TODO(dennis) describe behaviors in details.
    fn cast(&self, to_type: &ConcreteDataType) -> Result<VectorRef>;

    /// Take elements from the vector by the given indices.
    ///
    /// # Panics
    /// Panics if an index is out of bounds.
    fn take(&self, indices: &UInt32Vector) -> Result<VectorRef>;
}

macro_rules! impl_scalar_vector_op {
    ($($VectorType: ident),+) => {$(
        impl VectorOp for $VectorType {
            fn replicate(&self, offsets: &[usize]) -> VectorRef {
                replicate::replicate_scalar(self, offsets)
            }

            fn find_unique(&self, selected: &mut BitVec, prev_vector: Option<&dyn Vector>) {
                let prev_vector = prev_vector.map(|pv| pv.as_any().downcast_ref::<$VectorType>().unwrap());
                find_unique::find_unique_scalar(self, selected, prev_vector);
            }

            fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
                filter::filter_non_constant!(self, $VectorType, filter)
            }

            fn cast(&self, to_type: &ConcreteDataType) -> Result<VectorRef> {
                cast::cast_non_constant!(self, to_type)
            }

            fn take(&self, indices: &UInt32Vector) -> Result<VectorRef> {
                take::take_indices!(self, $VectorType, indices)
            }
        }
    )+};
}

impl_scalar_vector_op!(
    BinaryVector,
    BooleanVector,
    ListVector,
    StringVector,
    Decimal128Vector
);

impl<T: LogicalPrimitiveType> VectorOp for PrimitiveVector<T> {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        std::sync::Arc::new(replicate::replicate_primitive(self, offsets))
    }

    fn find_unique(&self, selected: &mut BitVec, prev_vector: Option<&dyn Vector>) {
        let prev_vector =
            prev_vector.and_then(|pv| pv.as_any().downcast_ref::<PrimitiveVector<T>>());
        find_unique::find_unique_scalar(self, selected, prev_vector);
    }

    fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
        filter::filter_non_constant!(self, PrimitiveVector<T>, filter)
    }

    fn cast(&self, to_type: &ConcreteDataType) -> Result<VectorRef> {
        cast::cast_non_constant!(self, to_type)
    }

    fn take(&self, indices: &UInt32Vector) -> Result<VectorRef> {
        take::take_indices!(self, PrimitiveVector<T>, indices)
    }
}

impl VectorOp for NullVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate::replicate_null(self, offsets)
    }

    fn find_unique(&self, selected: &mut BitVec, prev_vector: Option<&dyn Vector>) {
        let prev_vector = prev_vector.and_then(|pv| pv.as_any().downcast_ref::<NullVector>());
        find_unique::find_unique_null(self, selected, prev_vector);
    }

    fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
        filter::filter_non_constant!(self, NullVector, filter)
    }
    fn cast(&self, _to_type: &ConcreteDataType) -> Result<VectorRef> {
        // TODO(dennis): impl it when NullVector has other datatype.
        error::UnsupportedOperationSnafu {
            op: "cast",
            vector_type: self.vector_type_name(),
        }
        .fail()
    }

    fn take(&self, indices: &UInt32Vector) -> Result<VectorRef> {
        take::take_indices!(self, NullVector, indices)
    }
}

impl VectorOp for ConstantVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        self.replicate_vector(offsets)
    }

    fn find_unique(&self, selected: &mut BitVec, prev_vector: Option<&dyn Vector>) {
        let prev_vector = prev_vector.and_then(|pv| pv.as_any().downcast_ref::<ConstantVector>());
        find_unique::find_unique_constant(self, selected, prev_vector);
    }

    fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
        self.filter_vector(filter)
    }

    fn cast(&self, to_type: &ConcreteDataType) -> Result<VectorRef> {
        self.cast_vector(to_type)
    }

    fn take(&self, indices: &UInt32Vector) -> Result<VectorRef> {
        self.take_vector(indices)
    }
}
