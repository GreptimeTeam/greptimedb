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

use std::ops::Deref;
use std::sync::Arc;

use common_time::date::Date;
use common_time::datetime::DateTime;
use common_time::timestamp::Timestamp;
use datatypes::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, UInt64Array,
};
use datatypes::arrow::compute;
use datatypes::arrow::compute::kernels::{arithmetic, comparison};
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::arrow::error::Result as ArrowResult;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::prelude::Value;
use datatypes::value::{self, OrderedFloat};
use datatypes::vectors::{Helper, NullVector, VectorRef};
use rustpython_vm::builtins::{PyBaseExceptionRef, PyBool, PyBytes, PyFloat, PyInt, PyNone, PyStr};
use rustpython_vm::sliceable::{SaturatedSlice, SequenceIndex, SequenceIndexOp};
use rustpython_vm::types::PyComparisonOp;
use rustpython_vm::{
    pyclass as rspyclass, AsObject, PyObject, PyObjectRef, PyPayload, PyRef, PyResult,
    VirtualMachine,
};

use crate::python::utils::is_instance;

#[rspyclass(module = false, name = "vector")]
#[derive(PyPayload, Debug)]
pub struct PyVector {
    pub(crate) vector: VectorRef,
}

impl From<VectorRef> for PyVector {
    fn from(vector: VectorRef) -> Self {
        Self { vector }
    }
}

fn emit_cast_error(
    vm: &VirtualMachine,
    src_ty: &ArrowDataType,
    dst_ty: &ArrowDataType,
) -> PyBaseExceptionRef {
    vm.new_type_error(format!(
        "Can't cast source operand of type {src_ty:?} into target type of {dst_ty:?}",
    ))
}

/// Performs `val - arr`.
pub(crate) fn arrow_rsub(
    arr: &dyn Array,
    val: &dyn Array,
    vm: &VirtualMachine,
) -> PyResult<ArrayRef> {
    arithmetic::subtract_dyn(val, arr).map_err(|e| vm.new_type_error(format!("rsub error: {e}")))
}

/// Performs `val / arr`
pub(crate) fn arrow_rtruediv(
    arr: &dyn Array,
    val: &dyn Array,
    vm: &VirtualMachine,
) -> PyResult<ArrayRef> {
    arithmetic::divide_dyn(val, arr).map_err(|e| vm.new_type_error(format!("rtruediv error: {e}")))
}

/// Performs `val / arr`, but cast to i64.
pub(crate) fn arrow_rfloordiv(
    arr: &dyn Array,
    val: &dyn Array,
    vm: &VirtualMachine,
) -> PyResult<ArrayRef> {
    let array = arithmetic::divide_dyn(val, arr)
        .map_err(|e| vm.new_type_error(format!("rtruediv divide error: {e}")))?;
    compute::cast(&array, &ArrowDataType::Int64)
        .map_err(|e| vm.new_type_error(format!("rtruediv cast error: {e}")))
}

pub(crate) fn wrap_result<F>(
    f: F,
) -> impl Fn(&dyn Array, &dyn Array, &VirtualMachine) -> PyResult<ArrayRef>
where
    F: Fn(&dyn Array, &dyn Array) -> ArrowResult<ArrayRef>,
{
    move |left, right, vm| {
        f(left, right).map_err(|e| vm.new_type_error(format!("arithmetic error {e}")))
    }
}

fn is_float(datatype: &ArrowDataType) -> bool {
    matches!(
        datatype,
        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64
    )
}

fn is_signed(datatype: &ArrowDataType) -> bool {
    matches!(
        datatype,
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64
    )
}

fn is_unsigned(datatype: &ArrowDataType) -> bool {
    matches!(
        datatype,
        ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64
    )
}

fn cast(array: ArrayRef, target_type: &ArrowDataType, vm: &VirtualMachine) -> PyResult<ArrayRef> {
    compute::cast(&array, target_type).map_err(|e| vm.new_type_error(e.to_string()))
}

impl AsRef<PyVector> for PyVector {
    fn as_ref(&self) -> &PyVector {
        self
    }
}

impl PyVector {
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
        other: PyObjectRef,
        target_type: Option<ArrowDataType>,
        op: F,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector>
    where
        F: Fn(&dyn Array, &dyn Array, &VirtualMachine) -> PyResult<ArrayRef>,
    {
        // the right operand only support PyInt or PyFloat,
        let (right, right_type) = {
            if is_instance::<PyInt>(&other, vm) {
                other
                    .try_into_value::<i64>(vm)
                    .map(|v| (value::Value::Int64(v), ArrowDataType::Int64))?
            } else if is_instance::<PyFloat>(&other, vm) {
                other.try_into_value::<f64>(vm).map(|v| {
                    (
                        value::Value::Float64(OrderedFloat(v)),
                        ArrowDataType::Float64,
                    )
                })?
            } else {
                return Err(vm.new_type_error(format!(
                    "Can't cast right operand into Scalar of Int or Float, actual: {}",
                    other.class().name()
                )));
            }
        };
        // assuming they are all 64 bit type if possible
        let left = self.to_arrow_array();

        let left_type = left.data_type();
        let right_type = &right_type;
        // TODO(discord9): found better way to cast between signed and unsigned type
        let target_type = target_type.unwrap_or_else(|| {
            if is_signed(left_type) && is_signed(right_type) {
                ArrowDataType::Int64
            } else if is_unsigned(left_type) && is_unsigned(right_type) {
                ArrowDataType::UInt64
            } else {
                ArrowDataType::Float64
            }
        });
        let left = cast(left, &target_type, vm)?;
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
            return Err(emit_cast_error(vm, right_type, &target_type));
        };

        let result = op(left.as_ref(), right.as_ref(), vm)?;

        Ok(Helper::try_into_vector(result.clone())
            .map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {result:?}, err: {e:?}",
                ))
            })?
            .into())
    }

    pub(crate) fn arith_op<F>(
        &self,
        other: PyObjectRef,
        target_type: Option<ArrowDataType>,
        op: F,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector>
    where
        F: Fn(&dyn Array, &dyn Array) -> ArrowResult<ArrayRef>,
    {
        let right = other.downcast_ref::<PyVector>().ok_or_else(|| {
            vm.new_type_error(format!(
                "Can't cast right operand into PyVector, actual: {}",
                other.class().name()
            ))
        })?;
        let left = self.to_arrow_array();
        let right = right.to_arrow_array();

        let left_type = &left.data_type();
        let right_type = &right.data_type();

        let target_type = target_type.unwrap_or_else(|| {
            if is_signed(left_type) && is_signed(right_type) {
                ArrowDataType::Int64
            } else if is_unsigned(left_type) && is_unsigned(right_type) {
                ArrowDataType::UInt64
            } else {
                ArrowDataType::Float64
            }
        });

        let left = cast(left, &target_type, vm)?;
        let right = cast(right, &target_type, vm)?;

        let result = op(left.as_ref(), right.as_ref())
            .map_err(|e| vm.new_type_error(format!("Can't compute op, error: {e}")))?;

        Ok(Helper::try_into_vector(result.clone())
            .map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {result:?}, err: {e:?}",
                ))
            })?
            .into())
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
        let i = i
            .wrapped_at(self.len())
            .ok_or_else(|| vm.new_index_error("PyVector index out of range".to_owned()))?;
        Ok(val_to_pyobj(self.as_vector_ref().get(i), vm))
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
            for i in range.rev().step_by(step.unsigned_abs()) {
                // Safety: This mutable vector is created from the vector's data type.
                buf.push_value_ref(vector.get_ref(i)).unwrap();
            }
            let v: PyVector = buf.to_vector().into();
            Ok(v.into_pyobject(vm))
        } else {
            for i in range.step_by(step.unsigned_abs()) {
                // Safety: This mutable vector is created from the vector's data type.
                buf.push_value_ref(vector.get_ref(i)).unwrap();
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
        if is_pyobj_scalar(&other, vm) {
            let scalar_op = get_arrow_scalar_op(op);
            self.scalar_arith_op(other, None, scalar_op, vm)
        } else {
            let arr_op = get_arrow_op(op);
            self.arith_op(other, None, arr_op, vm)
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.as_vector_ref().len()
    }
}

/// get corresponding arrow op function according to given PyComaprsionOp
///
/// TODO(discord9): impl scalar version function
fn get_arrow_op(op: PyComparisonOp) -> impl Fn(&dyn Array, &dyn Array) -> ArrowResult<ArrayRef> {
    let op_bool_arr = match op {
        PyComparisonOp::Eq => comparison::eq_dyn,
        PyComparisonOp::Ne => comparison::neq_dyn,
        PyComparisonOp::Gt => comparison::gt_dyn,
        PyComparisonOp::Lt => comparison::lt_dyn,
        PyComparisonOp::Ge => comparison::gt_eq_dyn,
        PyComparisonOp::Le => comparison::lt_eq_dyn,
    };

    move |a: &dyn Array, b: &dyn Array| -> ArrowResult<ArrayRef> {
        let array = op_bool_arr(a, b)?;
        Ok(Arc::new(array))
    }
}

/// get corresponding arrow scalar op function according to given PyComaprsionOp
///
/// TODO(discord9): impl scalar version function
fn get_arrow_scalar_op(
    op: PyComparisonOp,
) -> impl Fn(&dyn Array, &dyn Array, &VirtualMachine) -> PyResult<ArrayRef> {
    let op_bool_arr = match op {
        PyComparisonOp::Eq => comparison::eq_dyn,
        PyComparisonOp::Ne => comparison::neq_dyn,
        PyComparisonOp::Gt => comparison::gt_dyn,
        PyComparisonOp::Lt => comparison::lt_dyn,
        PyComparisonOp::Ge => comparison::gt_eq_dyn,
        PyComparisonOp::Le => comparison::lt_eq_dyn,
    };

    move |a: &dyn Array, b: &dyn Array, vm| -> PyResult<ArrayRef> {
        let array =
            op_bool_arr(a, b).map_err(|e| vm.new_type_error(format!("scalar op error: {e}")))?;
        Ok(Arc::new(array))
    }
}

/// if this pyobj can be cast to a scalar value(i.e Null/Int/Float/Bool)
#[inline]
pub(crate) fn is_pyobj_scalar(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
    //let is_instance = |ty: &PyObject| obj.is_instance(ty, vm).unwrap_or(false);
    is_instance::<PyNone>(obj, vm)
        || is_instance::<PyInt>(obj, vm)
        || is_instance::<PyFloat>(obj, vm)
        || is_instance::<PyBool>(obj, vm)
}

/// convert a `PyObjectRef` into a `datatypes::Value`(is that ok?)
/// if `obj` can be convert to given ConcreteDataType then return inner `Value` else return None
/// if dtype is None, return types with highest precision
/// Not used for now but may be use in future
pub(crate) fn pyobj_try_to_typed_val(
    obj: PyObjectRef,
    vm: &VirtualMachine,
    dtype: Option<ConcreteDataType>,
) -> Option<value::Value> {
    if let Some(dtype) = dtype {
        match dtype {
            ConcreteDataType::Null(_) => {
                if is_instance::<PyNone>(&obj, vm) {
                    Some(value::Value::Null)
                } else {
                    None
                }
            }
            ConcreteDataType::Boolean(_) => {
                if is_instance::<PyBool>(&obj, vm) || is_instance::<PyInt>(&obj, vm) {
                    Some(value::Value::Boolean(
                        obj.try_into_value::<bool>(vm).unwrap_or(false),
                    ))
                } else {
                    None
                }
            }
            ConcreteDataType::Int8(_)
            | ConcreteDataType::Int16(_)
            | ConcreteDataType::Int32(_)
            | ConcreteDataType::Int64(_) => {
                if is_instance::<PyInt>(&obj, vm) {
                    match dtype {
                        ConcreteDataType::Int8(_) => {
                            obj.try_into_value::<i8>(vm).ok().map(value::Value::Int8)
                        }
                        ConcreteDataType::Int16(_) => {
                            obj.try_into_value::<i16>(vm).ok().map(value::Value::Int16)
                        }
                        ConcreteDataType::Int32(_) => {
                            obj.try_into_value::<i32>(vm).ok().map(value::Value::Int32)
                        }
                        ConcreteDataType::Int64(_) => {
                            obj.try_into_value::<i64>(vm).ok().map(value::Value::Int64)
                        }
                        _ => unreachable!(),
                    }
                } else {
                    None
                }
            }
            ConcreteDataType::UInt8(_)
            | ConcreteDataType::UInt16(_)
            | ConcreteDataType::UInt32(_)
            | ConcreteDataType::UInt64(_) => {
                if is_instance::<PyInt>(&obj, vm)
                    && obj.clone().try_into_value::<i64>(vm).unwrap_or(-1) >= 0
                {
                    match dtype {
                        ConcreteDataType::UInt8(_) => {
                            obj.try_into_value::<u8>(vm).ok().map(value::Value::UInt8)
                        }
                        ConcreteDataType::UInt16(_) => {
                            obj.try_into_value::<u16>(vm).ok().map(value::Value::UInt16)
                        }
                        ConcreteDataType::UInt32(_) => {
                            obj.try_into_value::<u32>(vm).ok().map(value::Value::UInt32)
                        }
                        ConcreteDataType::UInt64(_) => {
                            obj.try_into_value::<u64>(vm).ok().map(value::Value::UInt64)
                        }
                        _ => unreachable!(),
                    }
                } else {
                    None
                }
            }
            ConcreteDataType::Float32(_) | ConcreteDataType::Float64(_) => {
                if is_instance::<PyFloat>(&obj, vm) {
                    match dtype {
                        ConcreteDataType::Float32(_) => obj
                            .try_into_value::<f32>(vm)
                            .ok()
                            .map(|v| value::Value::Float32(OrderedFloat(v))),
                        ConcreteDataType::Float64(_) => obj
                            .try_into_value::<f64>(vm)
                            .ok()
                            .map(|v| value::Value::Float64(OrderedFloat(v))),
                        _ => unreachable!(),
                    }
                } else {
                    None
                }
            }

            ConcreteDataType::String(_) => {
                if is_instance::<PyStr>(&obj, vm) {
                    obj.try_into_value::<String>(vm)
                        .ok()
                        .map(|v| value::Value::String(v.into()))
                } else {
                    None
                }
            }
            ConcreteDataType::Binary(_) => {
                if is_instance::<PyBytes>(&obj, vm) {
                    obj.try_into_value::<Vec<u8>>(vm).ok().and_then(|v| {
                        String::from_utf8(v)
                            .ok()
                            .map(|v| value::Value::String(v.into()))
                    })
                } else {
                    None
                }
            }
            ConcreteDataType::List(_) => unreachable!(),
            ConcreteDataType::Date(_)
            | ConcreteDataType::DateTime(_)
            | ConcreteDataType::Timestamp(_) => {
                if is_instance::<PyInt>(&obj, vm) {
                    match dtype {
                        ConcreteDataType::Date(_) => obj
                            .try_into_value::<i32>(vm)
                            .ok()
                            .map(Date::new)
                            .map(value::Value::Date),
                        ConcreteDataType::DateTime(_) => obj
                            .try_into_value::<i64>(vm)
                            .ok()
                            .map(DateTime::new)
                            .map(value::Value::DateTime),
                        ConcreteDataType::Timestamp(_) => {
                            // FIXME(dennis): we always consider the timestamp unit is millis, it's not correct if user define timestamp column with other units.
                            obj.try_into_value::<i64>(vm)
                                .ok()
                                .map(Timestamp::new_millisecond)
                                .map(value::Value::Timestamp)
                        }
                        _ => unreachable!(),
                    }
                } else {
                    None
                }
            }
        }
    } else if is_instance::<PyNone>(&obj, vm) {
        // if Untyped then by default return types with highest precision
        Some(value::Value::Null)
    } else if is_instance::<PyBool>(&obj, vm) {
        Some(value::Value::Boolean(
            obj.try_into_value::<bool>(vm).unwrap_or(false),
        ))
    } else if is_instance::<PyInt>(&obj, vm) {
        obj.try_into_value::<i64>(vm).ok().map(value::Value::Int64)
    } else if is_instance::<PyFloat>(&obj, vm) {
        obj.try_into_value::<f64>(vm)
            .ok()
            .map(|v| value::Value::Float64(OrderedFloat(v)))
    } else if is_instance::<PyStr>(&obj, vm) {
        obj.try_into_value::<Vec<u8>>(vm).ok().and_then(|v| {
            String::from_utf8(v)
                .ok()
                .map(|v| value::Value::String(v.into()))
        })
    } else if is_instance::<PyBytes>(&obj, vm) {
        obj.try_into_value::<Vec<u8>>(vm).ok().and_then(|v| {
            String::from_utf8(v)
                .ok()
                .map(|v| value::Value::String(v.into()))
        })
    } else {
        None
    }
}

/// convert a DataType `Value` into a `PyObjectRef`
pub fn val_to_pyobj(val: value::Value, vm: &VirtualMachine) -> PyObjectRef {
    match val {
        // This comes from:https://github.com/RustPython/RustPython/blob/8ab4e770351d451cfdff5dc2bf8cce8df76a60ab/vm/src/builtins/singletons.rs#L37
        // None in Python is universally singleton so
        // use `vm.ctx.new_int` and `new_***` is more idomtic for there are cerntain optimize can be use in this way(small int pool etc.)
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
                    let list: Vec<_> = list.iter().map(|v| val_to_pyobj(v.clone(), vm)).collect();
                    vm.ctx.new_list(list).into()
                }
                None => vm.ctx.new_list(Vec::new()).into(),
            }
        }
    }
}

impl Default for PyVector {
    fn default() -> PyVector {
        PyVector {
            vector: Arc::new(NullVector::new(0)),
        }
    }
}
