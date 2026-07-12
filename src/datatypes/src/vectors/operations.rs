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
mod replicate;
mod take;

use std::sync::Arc;

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

            fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
                filter::filter_non_constant!(self, $VectorType, filter)
            }

            fn cast(&self, to_type: &ConcreteDataType) -> Result<VectorRef> {
                if let Some(vector) = self.as_any().downcast_ref::<BinaryVector>() {
                    match to_type {
                        ConcreteDataType::Json(_) => {
                            let json_vector = vector.convert_binary_to_json()?;
                            return Ok(Arc::new(json_vector) as VectorRef);
                        }
                        ConcreteDataType::Vector(d) => {
                            let vector = vector.convert_binary_to_vector(d.dim)?;
                            return Ok(Arc::new(vector) as VectorRef);
                        }
                        _ => {}
                    }
                }
                cast::cast_non_constant!(self, to_type)
            }

            fn take(&self, indices: &UInt32Vector) -> Result<VectorRef> {
                take::take_indices!(self, $VectorType, indices)
            }
        }
    )+};
}

impl_scalar_vector_op!(BinaryVector, BooleanVector, ListVector, StringVector);

impl VectorOp for Decimal128Vector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        std::sync::Arc::new(replicate::replicate_decimal128(self, offsets))
    }

    fn filter(&self, filter: &BooleanVector) -> Result<VectorRef> {
        filter::filter_non_constant!(self, Decimal128Vector, filter)
    }

    fn cast(&self, to_type: &ConcreteDataType) -> Result<VectorRef> {
        cast::cast_non_constant!(self, to_type)
    }

    fn take(&self, indices: &UInt32Vector) -> Result<VectorRef> {
        take::take_indices!(self, Decimal128Vector, indices)
    }
}

impl<T: LogicalPrimitiveType> VectorOp for PrimitiveVector<T> {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        std::sync::Arc::new(replicate::replicate_primitive(self, offsets))
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
