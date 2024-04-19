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

use std::sync::Arc;

use arrow::array::Datum;
use arrow::compute::kernels::numeric;
use datatypes::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, UInt64Array,
};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::arrow::error::Result as ArrowResult;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{self};
use datatypes::vectors::{Helper, NullVector, VectorRef};
use pyo3::pyclass as pyo3class;

#[cfg(test)]
mod tests;

/// The Main FFI type `PyVector` that is used in PyO3
#[pyo3class(name = "vector")]
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PyVector {
    pub(crate) vector: VectorRef,
}

impl From<VectorRef> for PyVector {
    fn from(vector: VectorRef) -> Self {
        Self { vector }
    }
}

/// Performs `val - arr`.
/// TODO: figure out whether pyo3 backend can support this
#[allow(dead_code)]
pub(crate) fn arrow_rsub(arr: &dyn Datum, val: &dyn Datum) -> Result<ArrayRef, String> {
    numeric::sub(val, arr).map_err(|e| format!("rsub error: {e}"))
}

/// Performs `val / arr`
pub(crate) fn arrow_rtruediv(arr: &dyn Datum, val: &dyn Datum) -> Result<ArrayRef, String> {
    numeric::div(val, arr).map_err(|e| format!("rtruediv error: {e}"))
}

/// Performs `val / arr`, but cast to i64.
/// TODO: figure out whether pyo3 backend can support this
#[allow(dead_code)]
pub(crate) fn arrow_rfloordiv(arr: &dyn Datum, val: &dyn Datum) -> Result<ArrayRef, String> {
    let array = numeric::div(val, arr).map_err(|e| format!("rfloordiv divide error: {e}"))?;
    compute::cast(&array, &ArrowDataType::Int64).map_err(|e| format!("rfloordiv cast error: {e}"))
}

pub(crate) fn wrap_result<F>(f: F) -> impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, String>
where
    F: Fn(&dyn Datum, &dyn Datum) -> ArrowResult<ArrayRef>,
{
    move |left, right| f(left, right).map_err(|e| format!("arithmetic error {e}"))
}

pub(crate) fn wrap_bool_result<F>(
    op_bool_arr: F,
) -> impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, String>
where
    F: Fn(&dyn Datum, &dyn Datum) -> ArrowResult<BooleanArray>,
{
    move |a: &dyn Datum, b: &dyn Datum| -> Result<ArrayRef, String> {
        let array = op_bool_arr(a, b).map_err(|e| format!("logical op error: {e}"))?;
        Ok(Arc::new(array))
    }
}

#[inline]
fn is_float(datatype: &ArrowDataType) -> bool {
    matches!(
        datatype,
        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64
    )
}

#[inline]
fn is_signed(datatype: &ArrowDataType) -> bool {
    matches!(
        datatype,
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64
    )
}

#[inline]
fn is_unsigned(datatype: &ArrowDataType) -> bool {
    matches!(
        datatype,
        ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64
    )
}

fn cast(array: ArrayRef, target_type: &ArrowDataType) -> Result<ArrayRef, String> {
    compute::cast(&array, target_type).map_err(|e| e.to_string())
}

impl AsRef<PyVector> for PyVector {
    fn as_ref(&self) -> &PyVector {
        self
    }
}

impl PyVector {
    #[inline]
    pub(crate) fn data_type(&self) -> ConcreteDataType {
        self.vector.data_type()
    }

    #[inline]
    pub(crate) fn arrow_data_type(&self) -> ArrowDataType {
        self.vector.data_type().as_arrow_type()
    }

    pub(crate) fn vector_and(left: &Self, right: &Self) -> Result<Self, String> {
        let left = left.to_arrow_array();
        let right = right.to_arrow_array();
        let left = left
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| format!("Can't cast {left:#?} as a Boolean Array"))?;
        let right = right
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| format!("Can't cast {right:#?} as a Boolean Array"))?;
        let res =
            Arc::new(compute::kernels::boolean::and(left, right).map_err(|err| err.to_string())?)
                as ArrayRef;
        let ret = Helper::try_into_vector(res.clone()).map_err(|err| err.to_string())?;
        Ok(ret.into())
    }
    pub(crate) fn vector_or(left: &Self, right: &Self) -> Result<Self, String> {
        let left = left.to_arrow_array();
        let right = right.to_arrow_array();
        let left = left
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| format!("Can't cast {left:#?} as a Boolean Array"))?;
        let right = right
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| format!("Can't cast {right:#?} as a Boolean Array"))?;
        let res =
            Arc::new(compute::kernels::boolean::or(left, right).map_err(|err| err.to_string())?)
                as ArrayRef;
        let ret = Helper::try_into_vector(res.clone()).map_err(|err| err.to_string())?;
        Ok(ret.into())
    }
    pub(crate) fn vector_invert(left: &Self) -> Result<Self, String> {
        let zelf = left.to_arrow_array();
        let zelf = zelf
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| format!("Can't cast {left:#?} as a Boolean Array"))?;
        let res = Arc::new(compute::kernels::boolean::not(zelf).map_err(|err| err.to_string())?)
            as ArrayRef;
        let ret = Helper::try_into_vector(res.clone()).map_err(|err| err.to_string())?;
        Ok(ret.into())
    }
    /// create a ref to inner vector
    #[inline]
    pub fn as_vector_ref(&self) -> VectorRef {
        self.vector.clone()
    }

    #[inline]
    pub fn to_arrow_array(&self) -> ArrayRef {
        self.vector.to_arrow_array()
    }

    pub(crate) fn scalar_arith_op<F>(
        &self,
        right: value::Value,
        target_type: Option<ArrowDataType>,
        op: F,
    ) -> Result<Self, String>
    where
        F: Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, String>,
    {
        let right_type = right.data_type().as_arrow_type();
        // assuming they are all 64 bit type if possible
        let left = self.to_arrow_array();

        let left_type = left.data_type();
        let right_type = &right_type;
        let target_type = Self::coerce_types(left_type, right_type, &target_type);
        let left = cast(left, &target_type)?;
        let left_len = left.len();

        // Convert `right` to an array of `target_type`.
        let right: Box<dyn Array> = if is_float(&target_type) {
            match right {
                value::Value::Int64(v) => Box::new(Float64Array::from_value(v as f64, left_len)),
                value::Value::UInt64(v) => Box::new(Float64Array::from_value(v as f64, left_len)),
                value::Value::Float64(v) => {
                    Box::new(Float64Array::from_value(f64::from(v), left_len))
                }
                _ => unreachable!(),
            }
        } else if is_signed(&target_type) {
            match right {
                value::Value::Int64(v) => Box::new(Int64Array::from_value(v, left_len)),
                value::Value::UInt64(v) => Box::new(Int64Array::from_value(v as i64, left_len)),
                value::Value::Float64(v) => Box::new(Int64Array::from_value(v.0 as i64, left_len)),
                _ => unreachable!(),
            }
        } else if is_unsigned(&target_type) {
            match right {
                value::Value::Int64(v) => Box::new(UInt64Array::from_value(v as u64, left_len)),
                value::Value::UInt64(v) => Box::new(UInt64Array::from_value(v, left_len)),
                value::Value::Float64(v) => Box::new(UInt64Array::from_value(v.0 as u64, left_len)),
                _ => unreachable!(),
            }
        } else {
            return Err(format!(
                "Can't cast source operand of type {:?} into target type of {:?}",
                right_type, &target_type
            ));
        };

        let result = op(&left, &right.as_ref())?;

        Ok(Helper::try_into_vector(result.clone())
            .map_err(|e| format!("Can't cast result into vector, result: {result:?}, err: {e:?}",))?
            .into())
    }

    /// Returns the type that should be used for the result of an arithmetic operation
    fn coerce_types(
        left_type: &ArrowDataType,
        right_type: &ArrowDataType,
        target_type: &Option<ArrowDataType>,
    ) -> ArrowDataType {
        // TODO(discord9): found better way to cast between signed and unsigned types
        target_type.clone().unwrap_or_else(|| {
            if is_signed(left_type) && is_signed(right_type) {
                ArrowDataType::Int64
            } else if is_unsigned(left_type) && is_unsigned(right_type) {
                ArrowDataType::UInt64
            } else {
                ArrowDataType::Float64
            }
        })
    }

    pub(crate) fn vector_arith_op<F>(
        &self,
        right: &Self,
        target_type: Option<ArrowDataType>,
        op: F,
    ) -> Result<PyVector, String>
    where
        F: Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, String>,
    {
        let left = self.to_arrow_array();
        let right = right.to_arrow_array();

        let left_type = &left.data_type();
        let right_type = &right.data_type();

        let target_type = Self::coerce_types(left_type, right_type, &target_type);

        let left = cast(left, &target_type)?;
        let right = cast(right, &target_type)?;

        let result = op(&left, &right)?;

        Ok(Helper::try_into_vector(result.clone())
            .map_err(|e| format!("Can't cast result into vector, result: {result:?}, err: {e:?}",))?
            .into())
    }

    pub(crate) fn len(&self) -> usize {
        self.as_vector_ref().len()
    }
}

impl Default for PyVector {
    fn default() -> PyVector {
        PyVector {
            vector: Arc::new(NullVector::new(0)),
        }
    }
}
