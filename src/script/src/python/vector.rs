// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::Arc;

use common_time::date::Date;
use common_time::datetime::DateTime;
use common_time::timestamp::Timestamp;
use datatypes::arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray};
use datatypes::arrow::datatypes::DataType;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::value::OrderedFloat;
use datatypes::vectors::{Helper, NullVector, VectorBuilder, VectorRef};
use datatypes::{arrow, value};
use rustpython_vm::builtins::{PyBaseExceptionRef, PyBool, PyBytes, PyFloat, PyInt, PyNone, PyStr};
use rustpython_vm::function::{Either, OptionalArg, PyComparisonValue};
use rustpython_vm::protocol::{PyMappingMethods, PySequenceMethods};
use rustpython_vm::sliceable::{SaturatedSlice, SequenceIndex, SequenceIndexOp};
use rustpython_vm::types::{AsMapping, AsSequence, Comparable, PyComparisonOp};
use rustpython_vm::{
    pyclass, AsObject, PyObject, PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine,
};

use crate::python::utils::{is_instance, PyVectorRef};

#[pyclass(module = false, name = "vector")]
#[derive(PyPayload, Debug)]
pub struct PyVector {
    vector: VectorRef,
}

impl From<VectorRef> for PyVector {
    fn from(vector: VectorRef) -> Self {
        Self { vector }
    }
}

fn emit_cast_error(
    vm: &VirtualMachine,
    src_ty: &DataType,
    dst_ty: &DataType,
) -> PyBaseExceptionRef {
    vm.new_type_error(format!(
        "Can't cast source operand of type {:?} into target type of {:?}",
        src_ty, dst_ty
    ))
}
fn arrow2_rsub_scalar(
    arr: &dyn Array,
    val: &dyn Scalar,
    _vm: &VirtualMachine,
) -> PyResult<Box<dyn Array>> {
    // b - a => a * (-1) + b
    let neg = arithmetics::mul_scalar(arr, &PrimitiveScalar::new(DataType::Int64, Some(-1i64)));
    Ok(arithmetics::add_scalar(neg.as_ref(), val))
}

fn arrow2_rtruediv_scalar(
    arr: &dyn Array,
    val: &dyn Scalar,
    vm: &VirtualMachine,
) -> PyResult<Box<dyn Array>> {
    // val / arr => one_arr / arr * val (this is simpler to write)
    let one_arr: Box<dyn Array> = if is_float(arr.data_type()) {
        Box::new(PrimitiveArray::from_values(vec![1f64; arr.len()]))
    } else if is_integer(arr.data_type()) {
        Box::new(PrimitiveArray::from_values(vec![1i64; arr.len()]))
    } else {
        return Err(vm.new_not_implemented_error(format!(
            "truediv of {:?} Scalar with {:?} Array is not supported",
            val.data_type(),
            arr.data_type()
        )));
    };
    let tmp = arithmetics::mul_scalar(one_arr.as_ref(), val);
    Ok(arithmetics::div(tmp.as_ref(), arr))
}

fn arrow2_rfloordiv_scalar(
    arr: &dyn Array,
    val: &dyn Scalar,
    vm: &VirtualMachine,
) -> PyResult<Box<dyn Array>> {
    // val // arr => one_arr // arr * val (this is simpler to write)
    let one_arr: Box<dyn Array> = if is_float(arr.data_type()) {
        Box::new(PrimitiveArray::from_values(vec![1f64; arr.len()]))
    } else if is_integer(arr.data_type()) {
        Box::new(PrimitiveArray::from_values(vec![1i64; arr.len()]))
    } else {
        return Err(vm.new_not_implemented_error(format!(
            "truediv of {:?} Scalar with {:?} Array is not supported",
            val.data_type(),
            arr.data_type()
        )));
    };
    let tmp = arithmetics::mul_scalar(one_arr.as_ref(), val);

    Ok(arrow::compute::cast::cast(
        arithmetics::div(tmp.as_ref(), arr).as_ref(),
        &DataType::Int64,
        cast::CastOptions {
            wrapped: false,
            partial: true,
        },
    )
    .unwrap())
}

fn wrap_result<F>(
    f: F,
) -> impl Fn(&dyn Array, &dyn Scalar, &VirtualMachine) -> PyResult<Box<dyn Array>>
where
    F: Fn(&dyn Array, &dyn Scalar) -> Box<dyn Array>,
{
    move |left, right, _vm| Ok(f(left, right))
}

fn is_float(datatype: &DataType) -> bool {
    matches!(
        datatype,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

fn is_integer(datatype: &DataType) -> bool {
    is_signed(datatype) || is_unsigned(datatype)
}

fn is_signed(datatype: &DataType) -> bool {
    matches!(
        datatype,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn is_unsigned(datatype: &DataType) -> bool {
    matches!(
        datatype,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    )
}

fn cast(array: ArrayRef, target_type: &DataType, vm: &VirtualMachine) -> PyResult<Box<dyn Array>> {
    cast::cast(
        array.as_ref(),
        target_type,
        CastOptions {
            wrapped: true,
            partial: true,
        },
    )
    .map_err(|e| vm.new_type_error(e.to_string()))
}
fn from_debug_error(err: impl std::fmt::Debug, vm: &VirtualMachine) -> PyBaseExceptionRef {
    vm.new_runtime_error(format!("Runtime Error: {err:#?}"))
}

impl AsRef<PyVector> for PyVector {
    fn as_ref(&self) -> &PyVector {
        self
    }
}

/// PyVector type wraps a greptime vector, impl multiply/div/add/sub opeerators etc.
#[pyclass(with(AsMapping, AsSequence, Comparable))]
impl PyVector {
    pub(crate) fn new(
        iterable: OptionalArg<PyObjectRef>,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector> {
        if let OptionalArg::Present(iterable) = iterable {
            let mut elements: Vec<PyObjectRef> = iterable.try_to_value(vm)?;

            if elements.is_empty() {
                return Ok(PyVector::default());
            }

            let datatype = get_concrete_type(&elements[0], vm)?;
            let mut buf = VectorBuilder::with_capacity(datatype.clone(), elements.len());

            for obj in elements.drain(..) {
                let val = if let Some(v) =
                    pyobj_try_to_typed_val(obj.clone(), vm, Some(datatype.clone()))
                {
                    v
                } else {
                    return Err(vm.new_type_error(format!(
                        "Can't cast pyobject {:?} into concrete type {:?}",
                        obj, datatype
                    )));
                };
                buf.push(&val);
            }

            Ok(PyVector {
                vector: buf.finish(),
            })
        } else {
            Ok(PyVector::default())
        }
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

    fn scalar_arith_op<F>(
        &self,
        other: PyObjectRef,
        target_type: Option<DataType>,
        op: F,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector>
    where
        F: Fn(&dyn Array, &dyn Scalar, &VirtualMachine) -> PyResult<Box<dyn Array>>,
    {
        // the right operand only support PyInt or PyFloat,
        let (right, right_type) = {
            if is_instance::<PyInt>(&other, vm) {
                other
                    .try_into_value::<i64>(vm)
                    .map(|v| (value::Value::Int64(v), DataType::Int64))?
            } else if is_instance::<PyFloat>(&other, vm) {
                other
                    .try_into_value::<f64>(vm)
                    .map(|v| (value::Value::Float64(OrderedFloat(v)), DataType::Float64))?
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
                DataType::Int64
            } else if is_unsigned(left_type) && is_unsigned(right_type) {
                DataType::UInt64
            } else {
                DataType::Float64
            }
        });
        let left = cast(left, &target_type, vm)?;
        let right: Box<dyn Scalar> = if is_float(&target_type) {
            match right {
                value::Value::Int64(v) => {
                    Box::new(PrimitiveScalar::new(target_type, Some(v as f64)))
                }
                value::Value::UInt64(v) => {
                    Box::new(PrimitiveScalar::new(target_type, Some(v as f64)))
                }
                value::Value::Float64(v) => {
                    Box::new(PrimitiveScalar::new(target_type, Some(f64::from(v))))
                }
                _ => unreachable!(),
            }
        } else if is_signed(&target_type) {
            match right {
                value::Value::Int64(v) => Box::new(PrimitiveScalar::new(target_type, Some(v))),
                value::Value::UInt64(v) => {
                    Box::new(PrimitiveScalar::new(target_type, Some(v as i64)))
                }
                value::Value::Float64(v) => {
                    Box::new(PrimitiveScalar::new(DataType::Float64, Some(v.0 as i64)))
                }
                _ => unreachable!(),
            }
        } else if is_unsigned(&target_type) {
            match right {
                value::Value::Int64(v) => Box::new(PrimitiveScalar::new(target_type, Some(v))),
                value::Value::UInt64(v) => Box::new(PrimitiveScalar::new(target_type, Some(v))),
                value::Value::Float64(v) => {
                    Box::new(PrimitiveScalar::new(target_type, Some(f64::from(v))))
                }
                _ => unreachable!(),
            }
        } else {
            return Err(emit_cast_error(vm, right_type, &target_type));
        };

        let result = op(left.as_ref(), right.as_ref(), vm)?;

        Ok(Helper::try_into_vector(&*result)
            .map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {:?}, err: {:?}",
                    result, e
                ))
            })?
            .into())
    }

    fn arith_op<F>(
        &self,
        other: PyObjectRef,
        target_type: Option<DataType>,
        op: F,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector>
    where
        F: Fn(&dyn Array, &dyn Array) -> Box<dyn Array>,
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
                DataType::Int64
            } else if is_unsigned(left_type) && is_unsigned(right_type) {
                DataType::UInt64
            } else {
                DataType::Float64
            }
        });

        let left = cast(left, &target_type, vm)?;
        let right = cast(right, &target_type, vm)?;

        let result = op(left.as_ref(), right.as_ref());

        Ok(Helper::try_into_vector(&*result)
            .map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {:?}, err: {:?}",
                    result, e
                ))
            })?
            .into())
    }

    #[pymethod(name = "__radd__")]
    #[pymethod(magic)]
    fn add(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, wrap_result(arithmetics::add_scalar), vm)
        } else {
            self.arith_op(other, None, arithmetics::add, vm)
        }
    }

    #[pymethod(magic)]
    fn sub(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, wrap_result(arithmetics::sub_scalar), vm)
        } else {
            self.arith_op(other, None, arithmetics::sub, vm)
        }
    }

    #[pymethod(magic)]
    fn rsub(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, arrow2_rsub_scalar, vm)
        } else {
            self.arith_op(other, None, |a, b| arithmetics::sub(b, a), vm)
        }
    }

    #[pymethod(name = "__rmul__")]
    #[pymethod(magic)]
    fn mul(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, wrap_result(arithmetics::mul_scalar), vm)
        } else {
            self.arith_op(other, None, arithmetics::mul, vm)
        }
    }

    #[pymethod(magic)]
    fn truediv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(
                other,
                Some(DataType::Float64),
                wrap_result(arithmetics::div_scalar),
                vm,
            )
        } else {
            self.arith_op(other, Some(DataType::Float64), arithmetics::div, vm)
        }
    }

    #[pymethod(magic)]
    fn rtruediv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, Some(DataType::Float64), arrow2_rtruediv_scalar, vm)
        } else {
            self.arith_op(
                other,
                Some(DataType::Float64),
                |a, b| arithmetics::div(b, a),
                vm,
            )
        }
    }

    #[pymethod(magic)]
    fn floordiv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(
                other,
                Some(DataType::Int64),
                wrap_result(arithmetics::div_scalar),
                vm,
            )
        } else {
            self.arith_op(other, Some(DataType::Int64), arithmetics::div, vm)
        }
    }

    #[pymethod(magic)]
    fn rfloordiv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            // FIXME: DataType convert problem, target_type should be inferred?
            self.scalar_arith_op(other, Some(DataType::Int64), arrow2_rfloordiv_scalar, vm)
        } else {
            self.arith_op(
                other,
                Some(DataType::Int64),
                |a, b| arithmetics::div(b, a),
                vm,
            )
        }
    }

    /// rich compare, return a boolean array, accept type are vec and vec and vec and number
    fn richcompare(
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

    // it seems rustpython's richcompare support is not good
    // The Comparable Trait only support normal cmp
    // (yes there is a slot_richcompare function, but it is not used in anywhere)
    // so use our own function
    // TODO(discord9): test those function

    #[pymethod(name = "eq")]
    #[pymethod(magic)]
    fn eq(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.richcompare(other, PyComparisonOp::Eq, vm)
    }

    #[pymethod(name = "ne")]
    #[pymethod(magic)]
    fn ne(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.richcompare(other, PyComparisonOp::Ne, vm)
    }

    #[pymethod(name = "gt")]
    #[pymethod(magic)]
    fn gt(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.richcompare(other, PyComparisonOp::Gt, vm)
    }

    #[pymethod(name = "lt")]
    #[pymethod(magic)]
    fn lt(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.richcompare(other, PyComparisonOp::Lt, vm)
    }

    #[pymethod(name = "ge")]
    #[pymethod(magic)]
    fn ge(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.richcompare(other, PyComparisonOp::Ge, vm)
    }

    #[pymethod(name = "le")]
    #[pymethod(magic)]
    fn le(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.richcompare(other, PyComparisonOp::Le, vm)
    }

    #[pymethod(magic)]
    fn and(&self, other: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let left = self.to_arrow_array();
        let left = left
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| vm.new_type_error(format!("Can't cast {left:#?} as a Boolean Array")))?;
        let right = other.to_arrow_array();
        let right = right
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| vm.new_type_error(format!("Can't cast {left:#?} as a Boolean Array")))?;
        let res = compute::boolean::and(left, right).map_err(|err| from_debug_error(err, vm))?;
        let res = Arc::new(res) as ArrayRef;
        let ret = Helper::try_into_vector(&*res).map_err(|err| from_debug_error(err, vm))?;
        Ok(ret.into())
    }

    #[pymethod(magic)]
    fn or(&self, other: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let left = self.to_arrow_array();
        let left = left
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| vm.new_type_error(format!("Can't cast {left:#?} as a Boolean Array")))?;
        let right = other.to_arrow_array();
        let right = right
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| vm.new_type_error(format!("Can't cast {left:#?} as a Boolean Array")))?;
        let res = compute::boolean::or(left, right).map_err(|err| from_debug_error(err, vm))?;
        let res = Arc::new(res) as ArrayRef;
        let ret = Helper::try_into_vector(&*res).map_err(|err| from_debug_error(err, vm))?;
        Ok(ret.into())
    }

    #[pymethod(magic)]
    fn invert(&self, vm: &VirtualMachine) -> PyResult<PyVector> {
        dbg!();
        let left = self.to_arrow_array();
        let left = left
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| vm.new_type_error(format!("Can't cast {left:#?} as a Boolean Array")))?;
        let res = compute::boolean::not(left);
        let res = Arc::new(res) as ArrayRef;
        let ret = Helper::try_into_vector(&*res).map_err(|err| from_debug_error(err, vm))?;
        Ok(ret.into())
    }

    #[pymethod(magic)]
    fn len(&self) -> usize {
        self.as_vector_ref().len()
    }

    /// take a boolean array and filters the Array, returning elements matching the filter (i.e. where the values are true).
    #[pymethod(name = "filter")]
    fn filter(&self, other: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let left = self.to_arrow_array();
        let right: ArrayRef = other.to_arrow_array();
        let filter = right.as_any().downcast_ref::<BooleanArray>();
        match filter {
            Some(filter) => {
                let res = compute::filter::filter(left.as_ref(), filter);

                let res =
                    res.map_err(|err| vm.new_runtime_error(format!("Arrow Error: {err:#?}")))?;
                let ret = Helper::try_into_vector(&*res).map_err(|e| {
                    vm.new_type_error(format!(
                        "Can't cast result into vector, result: {:?}, err: {:?}",
                        res, e
                    ))
                })?;
                Ok(ret.into())
            }
            None => Err(vm.new_runtime_error(format!(
                "Can't cast operand into a Boolean Array, which is {right:#?}"
            ))),
        }
    }

    #[pymethod(magic)]
    fn doc(&self) -> PyResult<PyStr> {
        Ok(PyStr::from(
            "PyVector is like a Python array, a compact array of elem of same datatype, but Readonly for now",
        ))
    }

    fn _getitem(&self, needle: &PyObject, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        if let Some(seq) = needle.payload::<PyVector>() {
            let mask = seq.to_arrow_array();
            let mask = mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    vm.new_type_error(format!("Can't cast {seq:#?} as a Boolean Array"))
                })?;
            // let left = self.to_arrow_array();
            let res = compute::filter::filter(self.to_arrow_array().as_ref(), mask)
                .map_err(|err| vm.new_runtime_error(format!("Arrow Error: {err:#?}")))?;
            let ret = Helper::try_into_vector(&*res).map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {:?}, err: {:?}",
                    res, e
                ))
            })?;
            Ok(Self::from(ret).into_pyobject(vm))
        } else {
            match SequenceIndex::try_from_borrowed_object(vm, needle, "vector")? {
                SequenceIndex::Int(i) => self.getitem_by_index(i, vm),
                SequenceIndex::Slice(slice) => self.getitem_by_slice(&slice, vm),
            }
        }
    }

    fn getitem_by_index(&self, i: isize, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
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

        let mut buf = VectorBuilder::with_capacity(vector.data_type(), slice_len);
        if slice_len == 0 {
            let v: PyVector = buf.finish().into();
            Ok(v.into_pyobject(vm))
        } else if step == 1 {
            let v: PyVector = vector.slice(range.next().unwrap_or(0), slice_len).into();
            Ok(v.into_pyobject(vm))
        } else if step.is_negative() {
            // Negative step require special treatment
            for i in range.rev().step_by(step.unsigned_abs()) {
                buf.push(&vector.get(i))
            }
            let v: PyVector = buf.finish().into();
            Ok(v.into_pyobject(vm))
        } else {
            for i in range.step_by(step.unsigned_abs()) {
                buf.push(&vector.get(i))
            }
            let v: PyVector = buf.finish().into();
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
}

/// get corresponding arrow op function according to given PyComaprsionOp
///
/// TODO(discord9): impl scalar version function
fn get_arrow_op(op: PyComparisonOp) -> impl Fn(&dyn Array, &dyn Array) -> Box<dyn Array> {
    let op_bool_arr = match op {
        PyComparisonOp::Eq => comparison::eq,
        PyComparisonOp::Ne => comparison::neq,
        PyComparisonOp::Gt => comparison::gt,
        PyComparisonOp::Lt => comparison::lt,
        PyComparisonOp::Ge => comparison::gt_eq,
        PyComparisonOp::Le => comparison::lt_eq,
    };

    move |a: &dyn Array, b: &dyn Array| -> Box<dyn Array> {
        let ret = op_bool_arr(a, b);
        Box::new(ret) as _
    }
}

/// get corresponding arrow scalar op function according to given PyComaprsionOp
///
/// TODO(discord9): impl scalar version function
fn get_arrow_scalar_op(
    op: PyComparisonOp,
) -> impl Fn(&dyn Array, &dyn Scalar, &VirtualMachine) -> PyResult<Box<dyn Array>> {
    let op_bool_arr = match op {
        PyComparisonOp::Eq => comparison::eq_scalar,
        PyComparisonOp::Ne => comparison::neq_scalar,
        PyComparisonOp::Gt => comparison::gt_scalar,
        PyComparisonOp::Lt => comparison::lt_scalar,
        PyComparisonOp::Ge => comparison::gt_eq_scalar,
        PyComparisonOp::Le => comparison::lt_eq_scalar,
    };

    move |a: &dyn Array, b: &dyn Scalar, _vm| -> PyResult<Box<dyn Array>> {
        let ret = op_bool_arr(a, b);
        Ok(Box::new(ret) as _)
    }
}

/// if this pyobj can be cast to a scalar value(i.e Null/Int/Float/Bool)
#[inline]
fn is_pyobj_scalar(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
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
pub fn pyobj_try_to_typed_val(
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

fn get_concrete_type(obj: &PyObjectRef, vm: &VirtualMachine) -> PyResult<ConcreteDataType> {
    if is_instance::<PyNone>(obj, vm) {
        Ok(ConcreteDataType::null_datatype())
    } else if is_instance::<PyBool>(obj, vm) {
        Ok(ConcreteDataType::boolean_datatype())
    } else if is_instance::<PyInt>(obj, vm) {
        Ok(ConcreteDataType::int64_datatype())
    } else if is_instance::<PyFloat>(obj, vm) {
        Ok(ConcreteDataType::float64_datatype())
    } else if is_instance::<PyStr>(obj, vm) {
        Ok(ConcreteDataType::string_datatype())
    } else {
        Err(vm.new_type_error(format!("Unsupported pyobject type: {:?}", obj)))
    }
}

impl AsMapping for PyVector {
    const AS_MAPPING: PyMappingMethods = PyMappingMethods {
        length: Some(|mapping, _vm| Ok(Self::mapping_downcast(mapping).len())),
        subscript: Some(|mapping, needle, vm| Self::mapping_downcast(mapping)._getitem(needle, vm)),
        ass_subscript: None,
    };
}

impl AsSequence for PyVector {
    const AS_SEQUENCE: PySequenceMethods = PySequenceMethods {
        length: Some(|seq, _vm| Ok(Self::sequence_downcast(seq).len())),
        item: Some(|seq, i, vm| {
            let zelf = Self::sequence_downcast(seq);
            zelf.getitem_by_index(i, vm)
        }),
        ass_item: Some(|_seq, _i, _value, vm| {
            Err(vm.new_type_error("PyVector object doesn't support item assigns".to_owned()))
        }),
        ..PySequenceMethods::NOT_IMPLEMENTED
    };
}

impl Comparable for PyVector {
    fn slot_richcompare(
        zelf: &PyObject,
        other: &PyObject,
        op: PyComparisonOp,
        vm: &VirtualMachine,
    ) -> PyResult<Either<PyObjectRef, PyComparisonValue>> {
        // TODO(discord9): return a boolean array of compare result
        if let Some(zelf) = zelf.downcast_ref::<Self>() {
            let ret: PyVector = zelf.richcompare(other.to_owned(), op, vm)?;
            let ret = ret.into_pyobject(vm);
            Ok(Either::A(ret))
        } else {
            // Safety: we are manually drop this ref, so no problem here
            let r = unsafe {
                let ptr = std::ptr::NonNull::from(zelf);
                ManuallyDrop::new(PyObjectRef::from_raw(ptr.as_ptr()))
            };
            Err(vm.new_type_error(format!(
                "unexpected payload {:?} for {}",
                r,
                op.method_name(&vm.ctx).as_str()
            )))
        }
    }
    fn cmp(
        _zelf: &rustpython_vm::Py<Self>,
        _other: &PyObject,
        _op: PyComparisonOp,
        _vm: &VirtualMachine,
    ) -> PyResult<PyComparisonValue> {
        Ok(PyComparisonValue::NotImplemented)
    }
}
#[cfg(test)]
pub mod tests {

    use std::sync::Arc;

    use datatypes::vectors::{Float32Vector, Int32Vector, NullVector};
    use rustpython_vm::builtins::PyList;
    use rustpython_vm::class::PyClassImpl;
    use rustpython_vm::protocol::PySequence;
    use value::Value;

    use super::*;

    type PredicateFn = Option<fn(PyResult<PyObjectRef>, &VirtualMachine) -> bool>;
    /// test the paired `val_to_obj` and `pyobj_to_val` func
    #[test]
    fn test_val2pyobj2val() {
        rustpython_vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
            let i = value::Value::Float32(OrderedFloat(2.0));
            let j = value::Value::Int32(1);
            let dtype = i.data_type();
            let obj = val_to_pyobj(i, vm);
            assert!(is_pyobj_scalar(&obj, vm));
            let obj_1 = obj.clone();
            let obj_2 = obj.clone();
            let ri = pyobj_try_to_typed_val(obj, vm, Some(dtype));
            let rj = pyobj_try_to_typed_val(obj_1, vm, Some(j.data_type()));
            let rn = pyobj_try_to_typed_val(obj_2, vm, None);
            assert_eq!(rj, None);
            assert_eq!(rn, Some(value::Value::Float64(OrderedFloat(2.0))));
            assert_eq!(ri, Some(value::Value::Float32(OrderedFloat(2.0))));
            let typed_lst = {
                [
                    Value::Null,
                    Value::Boolean(true),
                    Value::Boolean(false),
                    // PyInt is Big Int
                    Value::Int16(2),
                    Value::Int32(2),
                    Value::Int64(2),
                    Value::UInt16(2),
                    Value::UInt32(2),
                    Value::UInt64(2),
                    Value::Float32(OrderedFloat(2.0)),
                    Value::Float64(OrderedFloat(2.0)),
                    Value::String("123".into()),
                    // TODO(discord9): test Bytes and Date/DateTime
                ]
            };
            for val in typed_lst {
                let obj = val_to_pyobj(val.clone(), vm);
                let ret = pyobj_try_to_typed_val(obj, vm, Some(val.data_type()));
                assert_eq!(ret, Some(val));
            }
        })
    }

    #[test]
    fn test_getitem_by_index_in_vm() {
        rustpython_vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
            PyVector::make_class(&vm.ctx);
            let a: VectorRef = Arc::new(Int32Vector::from_vec(vec![1, 2, 3, 4]));
            let a = PyVector::from(a);
            assert_eq!(
                1,
                a.getitem_by_index(0, vm)
                    .map(|v| v.try_into_value::<i32>(vm).unwrap_or(0))
                    .unwrap_or(0)
            );
            assert!(a.getitem_by_index(4, vm).ok().is_none());
            assert_eq!(
                4,
                a.getitem_by_index(-1, vm)
                    .map(|v| v.try_into_value::<i32>(vm).unwrap_or(0))
                    .unwrap_or(0)
            );
            assert!(a.getitem_by_index(-5, vm).ok().is_none());

            let a: VectorRef = Arc::new(NullVector::new(42));
            let a = PyVector::from(a);
            let a = a.into_pyobject(vm);
            assert!(PySequence::find_methods(&a, vm).is_some());
            assert!(PySequence::new(&a, vm).is_some());
        })
    }

    pub fn execute_script(
        interpreter: &rustpython_vm::Interpreter,
        script: &str,
        test_vec: Option<PyVector>,
        predicate: PredicateFn,
    ) -> Result<(PyObjectRef, Option<bool>), PyRef<rustpython_vm::builtins::PyBaseException>> {
        let mut pred_res = None;
        interpreter
            .enter(|vm| {
                PyVector::make_class(&vm.ctx);
                let scope = vm.new_scope_with_builtins();
                let a: VectorRef = Arc::new(Int32Vector::from_vec(vec![1, 2, 3, 4]));
                let a = PyVector::from(a);
                let b: VectorRef = Arc::new(Float32Vector::from_vec(vec![1.2, 2.0, 3.4, 4.5]));
                let b = PyVector::from(b);
                scope
                    .locals
                    .as_object()
                    .set_item("a", vm.new_pyobj(a), vm)
                    .expect("failed");

                scope
                    .locals
                    .as_object()
                    .set_item("b", vm.new_pyobj(b), vm)
                    .expect("failed");

                if let Some(v) = test_vec {
                    scope
                        .locals
                        .as_object()
                        .set_item("test_vec", vm.new_pyobj(v), vm)
                        .expect("failed");
                }

                let code_obj = vm
                    .compile(
                        script,
                        rustpython_compiler_core::Mode::BlockExpr,
                        "<embedded>".to_owned(),
                    )
                    .map_err(|err| vm.new_syntax_error(&err))?;
                let ret = vm.run_code_obj(code_obj, scope);
                pred_res = predicate.map(|f| f(ret.clone(), vm));
                ret
            })
            .map(|r| (r, pred_res))
    }

    #[test]
    #[allow(clippy::print_stdout)]
    // for debug purpose, also this is already a test function so allow print_stdout shouldn't be a problem?
    fn test_execute_script() {
        fn is_eq<T: std::cmp::PartialEq + rustpython_vm::TryFromObject>(
            v: PyResult,
            i: T,
            vm: &VirtualMachine,
        ) -> bool {
            v.and_then(|v| v.try_into_value::<T>(vm))
                .map(|v| v == i)
                .unwrap_or(false)
        }

        let snippet: Vec<(&str, PredicateFn)> = vec![
            ("1", Some(|v, vm| is_eq(v, 1i32, vm))),
            ("len(a)", Some(|v, vm| is_eq(v, 4i32, vm))),
            ("a[-1]", Some(|v, vm| is_eq(v, 4i32, vm))),
            ("a[0]*5", Some(|v, vm| is_eq(v, 5i32, vm))),
            (
                "list(a)",
                Some(|v, vm| {
                    v.map_or(false, |obj| {
                        obj.is_instance(PyList::class(vm).into(), vm)
                            .unwrap_or(false)
                    })
                }),
            ),
            (
                "len(a[1:-1])#elem in [1,3)",
                Some(|v, vm| is_eq(v, 2i64, vm)),
            ),
            ("(a+1)[0]", Some(|v, vm| is_eq(v, 2i32, vm))),
            ("(a-1)[0]", Some(|v, vm| is_eq(v, 0i32, vm))),
            ("(a*2)[0]", Some(|v, vm| is_eq(v, 2i64, vm))),
            ("(a/2.0)[2]", Some(|v, vm| is_eq(v, 1.5f64, vm))),
            ("(a/2)[2]", Some(|v, vm| is_eq(v, 1.5f64, vm))),
            ("(a//2)[2]", Some(|v, vm| is_eq(v, 1i32, vm))),
            ("(2-a)[0]", Some(|v, vm| is_eq(v, 1i32, vm))),
            ("(3/a)[1]", Some(|v, vm| is_eq(v, 1.5, vm))),
            ("(3//a)[1]", Some(|v, vm| is_eq(v, 1, vm))),
            ("(3/a)[2]", Some(|v, vm| is_eq(v, 1.0, vm))),
            (
                "(a+1)[0] + (a-1)[0] * (a/2.0)[2]",
                Some(|v, vm| is_eq(v, 2.0, vm)),
            ),
        ];

        let interpreter = rustpython_vm::Interpreter::without_stdlib(Default::default());
        for (code, pred) in snippet {
            let result = execute_script(&interpreter, code, None, pred);

            println!(
                "\u{001B}[35m{code}\u{001B}[0m: {:?}{}",
                result.clone().map(|v| v.0),
                result
                    .clone()
                    .map(|v| if let Some(v) = v.1 {
                        if v {
                            "\u{001B}[32m...[ok]\u{001B}[0m".to_string()
                        } else {
                            "\u{001B}[31m...[failed]\u{001B}[0m".to_string()
                        }
                    } else {
                        "\u{001B}[36m...[unapplicable]\u{001B}[0m".to_string()
                    })
                    .unwrap()
            );

            if let Ok(p) = result {
                if let Some(v) = p.1 {
                    if !v {
                        panic!("{code}: {:?}\u{001B}[12m...[failed]\u{001B}[0m", p.0)
                    }
                }
            } else {
                panic!("{code}: {:?}", result)
            }
        }
    }
}
