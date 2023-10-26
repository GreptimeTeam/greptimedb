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

#[cfg(test)]
mod tests;
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::Datum;
use arrow::compute::kernels::{cmp, numeric};
use datatypes::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, UInt64Array,
};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::arrow::error::Result as ArrowResult;
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::value::{self, OrderedFloat};
use datatypes::vectors::{Helper, NullVector, VectorRef};
#[cfg(feature = "pyo3_backend")]
use pyo3::pyclass as pyo3class;
use rustpython_vm::builtins::{PyBaseExceptionRef, PyBool, PyFloat, PyInt, PyNone, PyStr};
use rustpython_vm::sliceable::{SaturatedSlice, SequenceIndex, SequenceIndexOp};
use rustpython_vm::types::PyComparisonOp;
use rustpython_vm::{
    pyclass as rspyclass, AsObject, PyObject, PyObjectRef, PyPayload, PyRef, PyResult,
    VirtualMachine,
};

use crate::python::rspython::utils::is_instance;

/// The Main FFI type `PyVector` that is used both in RustPython and PyO3
#[cfg_attr(feature = "pyo3_backend", pyo3class(name = "vector"))]
#[rspyclass(module = false, name = "vector")]
#[repr(transparent)]
#[derive(PyPayload, Debug, Clone)]
pub struct PyVector {
    pub(crate) vector: VectorRef,
}

pub(crate) type PyVectorRef = PyRef<PyVector>;

impl From<VectorRef> for PyVector {
    fn from(vector: VectorRef) -> Self {
        Self { vector }
    }
}

fn to_type_error(vm: &'_ VirtualMachine) -> impl FnOnce(String) -> PyBaseExceptionRef + '_ {
    |msg: String| vm.new_type_error(msg)
}

/// Performs `val - arr`.
pub(crate) fn arrow_rsub(arr: &dyn Datum, val: &dyn Datum) -> Result<ArrayRef, String> {
    numeric::sub(val, arr).map_err(|e| format!("rsub error: {e}"))
}

/// Performs `val / arr`
pub(crate) fn arrow_rtruediv(arr: &dyn Datum, val: &dyn Datum) -> Result<ArrayRef, String> {
    numeric::div(val, arr).map_err(|e| format!("rtruediv error: {e}"))
}

/// Performs `val / arr`, but cast to i64.
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

#[cfg(feature = "pyo3_backend")]
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

    pub(crate) fn rspy_scalar_arith_op<F>(
        &self,
        other: PyObjectRef,
        target_type: Option<ArrowDataType>,
        op: F,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector>
    where
        F: Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, String>,
    {
        // the right operand only support PyInt or PyFloat,
        let right = {
            if is_instance::<PyInt>(&other, vm) {
                other.try_into_value::<i64>(vm).map(value::Value::Int64)?
            } else if is_instance::<PyFloat>(&other, vm) {
                other
                    .try_into_value::<f64>(vm)
                    .map(|v| (value::Value::Float64(OrderedFloat(v))))?
            } else {
                return Err(vm.new_type_error(format!(
                    "Can't cast right operand into Scalar of Int or Float, actual: {}",
                    other.class().name()
                )));
            }
        };
        self.scalar_arith_op(right, target_type, op)
            .map_err(to_type_error(vm))
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

    pub(crate) fn rspy_vector_arith_op<F>(
        &self,
        other: PyObjectRef,
        target_type: Option<ArrowDataType>,
        op: F,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector>
    where
        F: Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, String>,
    {
        let right = other.downcast_ref::<PyVector>().ok_or_else(|| {
            vm.new_type_error(format!(
                "Can't cast right operand into PyVector, actual type: {}",
                other.class().name()
            ))
        })?;
        self.vector_arith_op(right, target_type, op)
            .map_err(to_type_error(vm))
    }

    pub(crate) fn _getitem(&self, needle: &PyObject, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        if let Some(seq) = needle.payload::<PyVector>() {
            let mask = seq.to_arrow_array();
            let mask = mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    vm.new_type_error(format!("Can't cast {seq:#?} as a Boolean Array"))
                })?;
            let res = compute::filter(self.to_arrow_array().as_ref(), mask)
                .map_err(|err| vm.new_runtime_error(format!("Arrow Error: {err:#?}")))?;
            let ret = Helper::try_into_vector(res.clone()).map_err(|e| {
                vm.new_type_error(format!("Can't cast result into vector, err: {e:?}"))
            })?;
            Ok(Self::from(ret).into_pyobject(vm))
        } else {
            match SequenceIndex::try_from_borrowed_object(vm, needle, "vector")? {
                SequenceIndex::Int(i) => self.getitem_by_index(i, vm),
                SequenceIndex::Slice(slice) => self.getitem_by_slice(&slice, vm),
            }
        }
    }

    pub(crate) fn getitem_by_index(&self, i: isize, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        // in the newest version of rustpython_vm, wrapped_at for isize is replace by wrap_index(i, len)
        let i = i.wrapped_at(self.len()).ok_or_else(|| {
            vm.new_index_error(format!("PyVector index {i} out of range {}", self.len()))
        })?;
        val_to_pyobj(self.as_vector_ref().get(i), vm)
    }

    /// Return a `PyVector` in `PyObjectRef`
    fn getitem_by_slice(
        &self,
        slice: &SaturatedSlice,
        vm: &VirtualMachine,
    ) -> PyResult<PyObjectRef> {
        // adjust_indices so negative number is transform to usize
        let (mut range, step, slice_len) = slice.adjust_indices(self.len());
        let vector = self.as_vector_ref();
        let mut buf = vector.data_type().create_mutable_vector(slice_len);
        if slice_len == 0 {
            let v: PyVector = buf.to_vector().into();
            Ok(v.into_pyobject(vm))
        } else if step == 1 {
            let v: PyVector = vector.slice(range.next().unwrap_or(0), slice_len).into();
            Ok(v.into_pyobject(vm))
        } else if step.is_negative() {
            // Negative step require special treatment
            // range.start > range.stop if slice can found no-empty
            for i in range.rev().step_by(step.unsigned_abs()) {
                // Safety: This mutable vector is created from the vector's data type.
                buf.push_value_ref(vector.get_ref(i));
            }
            let v: PyVector = buf.to_vector().into();
            Ok(v.into_pyobject(vm))
        } else {
            for i in range.step_by(step.unsigned_abs()) {
                // Safety: This mutable vector is created from the vector's data type.
                buf.push_value_ref(vector.get_ref(i));
            }
            let v: PyVector = buf.to_vector().into();
            Ok(v.into_pyobject(vm))
        }
    }

    /// Unsupported
    /// TODO(discord9): make it work
    #[allow(unused)]
    fn setitem_by_index(
        zelf: PyRef<Self>,
        i: isize,
        value: PyObjectRef,
        vm: &VirtualMachine,
    ) -> PyResult<()> {
        Err(vm.new_not_implemented_error("setitem_by_index unimplemented".to_string()))
    }

    /// rich compare, return a boolean array, accept type are vec and vec and vec and number
    pub(crate) fn richcompare(
        &self,
        other: PyObjectRef,
        op: PyComparisonOp,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector> {
        if rspy_is_pyobj_scalar(&other, vm) {
            let scalar_op = get_arrow_scalar_op(op);
            self.rspy_scalar_arith_op(other, None, scalar_op, vm)
        } else {
            let arr_op = get_arrow_op(op);
            self.rspy_vector_arith_op(other, None, wrap_result(arr_op), vm)
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.as_vector_ref().len()
    }
}

/// get corresponding arrow op function according to given PyComaprsionOp
fn get_arrow_op(op: PyComparisonOp) -> impl Fn(&dyn Datum, &dyn Datum) -> ArrowResult<ArrayRef> {
    let op_bool_arr = match op {
        PyComparisonOp::Eq => cmp::eq,
        PyComparisonOp::Ne => cmp::neq,
        PyComparisonOp::Gt => cmp::gt,
        PyComparisonOp::Lt => cmp::lt,
        PyComparisonOp::Ge => cmp::gt_eq,
        PyComparisonOp::Le => cmp::lt_eq,
    };

    move |a: &dyn Datum, b: &dyn Datum| -> ArrowResult<ArrayRef> {
        let array = op_bool_arr(a, b)?;
        Ok(Arc::new(array))
    }
}

/// get corresponding arrow scalar op function according to given PyComaprsionOp
fn get_arrow_scalar_op(
    op: PyComparisonOp,
) -> impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, String> {
    let op_bool_arr = match op {
        PyComparisonOp::Eq => cmp::eq,
        PyComparisonOp::Ne => cmp::neq,
        PyComparisonOp::Gt => cmp::gt,
        PyComparisonOp::Lt => cmp::lt,
        PyComparisonOp::Ge => cmp::gt_eq,
        PyComparisonOp::Le => cmp::lt_eq,
    };

    move |a: &dyn Datum, b: &dyn Datum| -> Result<ArrayRef, String> {
        let array = op_bool_arr(a, b).map_err(|e| format!("scalar op error: {e}"))?;
        Ok(Arc::new(array))
    }
}

/// if this pyobj can be cast to a scalar value(i.e Null/Int/Float/Bool)
#[inline]
pub(crate) fn rspy_is_pyobj_scalar(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
    is_instance::<PyNone>(obj, vm)
        || is_instance::<PyInt>(obj, vm)
        || is_instance::<PyFloat>(obj, vm)
        || is_instance::<PyBool>(obj, vm)
        || is_instance::<PyStr>(obj, vm)
}

/// convert a DataType `Value` into a `PyObjectRef`
pub fn val_to_pyobj(val: value::Value, vm: &VirtualMachine) -> PyResult {
    Ok(match val {
        // This comes from:https://github.com/RustPython/RustPython/blob/8ab4e770351d451cfdff5dc2bf8cce8df76a60ab/vm/src/builtins/singletons.rs#L37
        // None in Python is universally singleton so
        // use `vm.ctx.new_int` and `new_***` is more idiomatic for there are certain optimize can be used in this way(small int pool etc.)
        value::Value::Null => vm.ctx.none(),
        value::Value::Boolean(v) => vm.ctx.new_bool(v).into(),
        value::Value::UInt8(v) => vm.ctx.new_int(v).into(),
        value::Value::UInt16(v) => vm.ctx.new_int(v).into(),
        value::Value::UInt32(v) => vm.ctx.new_int(v).into(),
        value::Value::UInt64(v) => vm.ctx.new_int(v).into(),
        value::Value::Int8(v) => vm.ctx.new_int(v).into(),
        value::Value::Int16(v) => vm.ctx.new_int(v).into(),
        value::Value::Int32(v) => vm.ctx.new_int(v).into(),
        value::Value::Int64(v) => vm.ctx.new_int(v).into(),
        value::Value::Float32(v) => vm.ctx.new_float(v.0 as f64).into(),
        value::Value::Float64(v) => vm.ctx.new_float(v.0).into(),
        value::Value::String(s) => vm.ctx.new_str(s.as_utf8()).into(),
        // is this copy necessary?
        value::Value::Binary(b) => vm.ctx.new_bytes(b.deref().to_vec()).into(),
        // TODO(dennis):is `Date` and `DateTime` supported yet? For now just ad hoc into PyInt, but it's better to be cast into python Date, DateTime objects etc..
        value::Value::Date(v) => vm.ctx.new_int(v.val()).into(),
        value::Value::DateTime(v) => vm.ctx.new_int(v.val()).into(),
        // FIXME(dennis): lose the timestamp unit here
        Value::Timestamp(v) => vm.ctx.new_int(v.value()).into(),
        value::Value::List(list) => {
            let list = list.items().as_ref();
            match list {
                Some(list) => {
                    let list: Vec<_> = list
                        .iter()
                        .map(|v| val_to_pyobj(v.clone(), vm))
                        .collect::<Result<_, _>>()?;
                    vm.ctx.new_list(list).into()
                }
                None => vm.ctx.new_list(Vec::new()).into(),
            }
        }
        #[allow(unreachable_patterns)]
        _ => return Err(vm.new_type_error(format!("Convert from {val:?} is not supported yet"))),
    })
}

impl Default for PyVector {
    fn default() -> PyVector {
        PyVector {
            vector: Arc::new(NullVector::new(0)),
        }
    }
}
