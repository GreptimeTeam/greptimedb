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

use arrow::compute::kernels::numeric;
use common_time::date::Date;
use common_time::datetime::DateTime;
use common_time::timestamp::Timestamp;
use crossbeam_utils::atomic::AtomicCell;
use datatypes::arrow::array::{Array, BooleanArray};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::value::{self, OrderedFloat};
use datatypes::vectors::Helper;
use once_cell::sync::Lazy;
use rustpython_vm::builtins::{PyBaseExceptionRef, PyBool, PyBytes, PyFloat, PyInt, PyNone, PyStr};
use rustpython_vm::convert::ToPyResult;
use rustpython_vm::function::{Either, OptionalArg, PyComparisonValue};
use rustpython_vm::protocol::{PyMappingMethods, PyNumberMethods, PySequenceMethods};
use rustpython_vm::types::{
    AsMapping, AsNumber, AsSequence, Comparable, PyComparisonOp, Representable,
};
use rustpython_vm::{
    atomic_func, pyclass as rspyclass, Py, PyObject, PyObjectRef, PyPayload, PyRef, PyResult,
    VirtualMachine,
};

use crate::python::ffi_types::vector::{
    arrow_rfloordiv, arrow_rsub, arrow_rtruediv, rspy_is_pyobj_scalar, wrap_result, PyVector,
};
use crate::python::rspython::utils::{is_instance, obj_cast_to};
/// PyVectors' rustpython specify methods

fn to_type_error(vm: &'_ VirtualMachine) -> impl FnOnce(String) -> PyBaseExceptionRef + '_ {
    |msg: String| vm.new_type_error(msg)
}

pub(crate) type PyVectorRef = PyRef<PyVector>;
/// PyVector type wraps a greptime vector, impl multiply/div/add/sub opeerators etc.
#[rspyclass(with(AsMapping, AsSequence, Comparable, AsNumber, Representable))]
impl PyVector {
    #[pymethod]
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
            let mut buf = datatype.create_mutable_vector(elements.len());

            for obj in elements.drain(..) {
                let val = if let Some(v) =
                    pyobj_try_to_typed_val(obj.clone(), vm, Some(datatype.clone()))
                {
                    v
                } else {
                    return Err(vm.new_type_error(format!(
                        "Can't cast pyobject {obj:?} into concrete type {datatype:?}",
                    )));
                };
                // Safety: `pyobj_try_to_typed_val()` has checked the data type.
                buf.push_value_ref(val.as_value_ref());
            }

            Ok(PyVector {
                vector: buf.to_vector(),
            })
        } else {
            Ok(PyVector::default())
        }
    }

    #[pymethod(name = "__radd__")]
    #[pymethod(magic)]
    fn add(zelf: PyObjectRef, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let zelf = obj_cast_to::<PyVector>(zelf, vm)?;
        if rspy_is_pyobj_scalar(&other, vm) {
            zelf.rspy_scalar_arith_op(other, None, wrap_result(numeric::add), vm)
        } else {
            zelf.rspy_vector_arith_op(other, None, wrap_result(numeric::add), vm)
        }
    }

    #[pymethod(magic)]
    fn sub(zelf: PyObjectRef, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let zelf = obj_cast_to::<PyVector>(zelf, vm)?;
        if rspy_is_pyobj_scalar(&other, vm) {
            zelf.rspy_scalar_arith_op(other, None, wrap_result(numeric::sub), vm)
        } else {
            zelf.rspy_vector_arith_op(other, None, wrap_result(numeric::sub), vm)
        }
    }

    #[pymethod(magic)]
    fn rsub(zelf: PyObjectRef, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let zelf = obj_cast_to::<PyVector>(zelf, vm)?;
        if rspy_is_pyobj_scalar(&other, vm) {
            zelf.rspy_scalar_arith_op(other, None, arrow_rsub, vm)
        } else {
            zelf.rspy_vector_arith_op(other, None, wrap_result(|a, b| numeric::sub(b, a)), vm)
        }
    }

    #[pymethod(name = "__rmul__")]
    #[pymethod(magic)]
    fn mul(zelf: PyObjectRef, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let zelf = obj_cast_to::<PyVector>(zelf, vm)?;
        if rspy_is_pyobj_scalar(&other, vm) {
            zelf.rspy_scalar_arith_op(other, None, wrap_result(numeric::mul), vm)
        } else {
            zelf.rspy_vector_arith_op(other, None, wrap_result(numeric::mul), vm)
        }
    }

    #[pymethod(magic)]
    fn truediv(zelf: PyObjectRef, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let zelf = obj_cast_to::<PyVector>(zelf, vm)?;
        if rspy_is_pyobj_scalar(&other, vm) {
            zelf.rspy_scalar_arith_op(
                other,
                Some(ArrowDataType::Float64),
                wrap_result(numeric::div),
                vm,
            )
        } else {
            zelf.rspy_vector_arith_op(
                other,
                Some(ArrowDataType::Float64),
                wrap_result(numeric::div),
                vm,
            )
        }
    }

    #[pymethod(magic)]
    fn rtruediv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if rspy_is_pyobj_scalar(&other, vm) {
            self.rspy_scalar_arith_op(other, Some(ArrowDataType::Float64), arrow_rtruediv, vm)
        } else {
            self.rspy_vector_arith_op(
                other,
                Some(ArrowDataType::Float64),
                wrap_result(|a, b| numeric::div(b, a)),
                vm,
            )
        }
    }

    #[pymethod(magic)]
    fn floordiv(zelf: PyObjectRef, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let zelf = obj_cast_to::<PyVector>(zelf, vm)?;
        if rspy_is_pyobj_scalar(&other, vm) {
            zelf.rspy_scalar_arith_op(
                other,
                Some(ArrowDataType::Int64),
                wrap_result(numeric::div),
                vm,
            )
        } else {
            zelf.rspy_vector_arith_op(
                other,
                Some(ArrowDataType::Int64),
                wrap_result(numeric::div),
                vm,
            )
        }
    }

    #[pymethod(magic)]
    fn rfloordiv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if rspy_is_pyobj_scalar(&other, vm) {
            // FIXME: DataType convert problem, target_type should be inferred?
            self.rspy_scalar_arith_op(other, Some(ArrowDataType::Int64), arrow_rfloordiv, vm)
        } else {
            self.rspy_vector_arith_op(
                other,
                Some(ArrowDataType::Int64),
                wrap_result(|a, b| numeric::div(b, a)),
                vm,
            )
        }
    }

    fn obj_to_vector(obj: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyRef<PyVector>> {
        obj.downcast::<PyVector>().map_err(|e| {
            vm.new_type_error(format!(
                "Can't cast right operand into PyVector, actual type: {}",
                e.class().name()
            ))
        })
    }

    #[pymethod(magic)]
    fn and(zelf: PyObjectRef, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let zelf = Self::obj_to_vector(zelf, vm)?;
        let other = Self::obj_to_vector(other, vm)?;
        Self::vector_and(&zelf, &other).map_err(to_type_error(vm))
    }

    #[pymethod(magic)]
    fn or(zelf: PyObjectRef, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let zelf = Self::obj_to_vector(zelf, vm)?;
        let other = Self::obj_to_vector(other, vm)?;
        Self::vector_or(&zelf, &other).map_err(to_type_error(vm))
    }

    #[pymethod(magic)]
    fn invert(zelf: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let zelf = Self::obj_to_vector(zelf, vm)?;
        Self::vector_invert(&zelf).map_err(to_type_error(vm))
    }

    #[pymethod(name = "__len__")]
    fn len_rspy(&self) -> usize {
        self.len()
    }

    #[pymethod(name = "concat")]
    fn concat(&self, other: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let left = self.to_arrow_array();
        let right = other.to_arrow_array();

        let res = compute::concat(&[left.as_ref(), right.as_ref()]);
        let res = res.map_err(|err| vm.new_runtime_error(format!("Arrow Error: {err:#?}")))?;
        let ret = Helper::try_into_vector(res.clone()).map_err(|e| {
            vm.new_type_error(format!(
                "Can't cast result into vector, result: {res:?}, err: {e:?}",
            ))
        })?;
        Ok(ret.into())
    }

    /// take a boolean array and filters the Array, returning elements matching the filter (i.e. where the values are true).
    #[pymethod(name = "filter")]
    fn filter(&self, other: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let left = self.to_arrow_array();
        let right = other.to_arrow_array();
        let filter = right.as_any().downcast_ref::<BooleanArray>();
        match filter {
            Some(filter) => {
                let res = compute::filter(left.as_ref(), filter);

                let res =
                    res.map_err(|err| vm.new_runtime_error(format!("Arrow Error: {err:#?}")))?;
                let ret = Helper::try_into_vector(res.clone()).map_err(|e| {
                    vm.new_type_error(format!(
                        "Can't cast result into vector, result: {res:?}, err: {e:?}",
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
}

impl Representable for PyVector {
    #[inline]
    fn repr_str(zelf: &Py<Self>, _vm: &VirtualMachine) -> PyResult<String> {
        Ok(format!("{:#?}", *zelf))
    }
}

impl AsNumber for PyVector {
    fn as_number() -> &'static PyNumberMethods {
        // FIXME(discord9): have to use `&PyObject.to_owned()` here
        // because it seems to be the only way to convert a `&PyObject` to `PyObjectRef`.
        static AS_NUMBER: PyNumberMethods = PyNumberMethods {
            and: Some(|a, b, vm| PyVector::and(a.to_owned(), b.to_owned(), vm).to_pyresult(vm)),
            or: Some(|a, b, vm| PyVector::or(a.to_owned(), b.to_owned(), vm).to_pyresult(vm)),
            invert: Some(|a, vm| PyVector::invert((*a).to_owned(), vm).to_pyresult(vm)),
            add: Some(|a, b, vm| PyVector::add(a.to_owned(), b.to_owned(), vm).to_pyresult(vm)),
            subtract: Some(|a, b, vm| {
                PyVector::sub(a.to_owned(), b.to_owned(), vm).to_pyresult(vm)
            }),
            multiply: Some(|a, b, vm| {
                PyVector::mul(a.to_owned(), b.to_owned(), vm).to_pyresult(vm)
            }),
            true_divide: Some(|a, b, vm| {
                PyVector::truediv(a.to_owned(), b.to_owned(), vm).to_pyresult(vm)
            }),
            floor_divide: Some(|a, b, vm| {
                PyVector::floordiv(a.to_owned(), b.to_owned(), vm).to_pyresult(vm)
            }),
            ..PyNumberMethods::NOT_IMPLEMENTED
        };
        &AS_NUMBER
    }
}

impl AsMapping for PyVector {
    fn as_mapping() -> &'static PyMappingMethods {
        static AS_MAPPING: PyMappingMethods = PyMappingMethods {
            length: atomic_func!(|mapping, _vm| Ok(PyVector::mapping_downcast(mapping).len())),
            subscript: atomic_func!(
                |mapping, needle, vm| PyVector::mapping_downcast(mapping)._getitem(needle, vm)
            ),
            ass_subscript: AtomicCell::new(None),
        };
        &AS_MAPPING
    }
}

impl AsSequence for PyVector {
    fn as_sequence() -> &'static PySequenceMethods {
        static AS_SEQUENCE: Lazy<PySequenceMethods> = Lazy::new(|| PySequenceMethods {
            length: atomic_func!(|seq, _vm| Ok(PyVector::sequence_downcast(seq).len())),
            item: atomic_func!(|seq, i, vm| {
                let zelf = PyVector::sequence_downcast(seq);
                zelf.getitem_by_index(i, vm)
            }),
            ass_item: atomic_func!(|_seq, _i, _value, vm| {
                Err(vm.new_type_error("PyVector object doesn't support item assigns".to_string()))
            }),
            ..PySequenceMethods::NOT_IMPLEMENTED
        });
        &AS_SEQUENCE
    }
}

impl Comparable for PyVector {
    fn slot_richcompare(
        zelf: &PyObject,
        other: &PyObject,
        op: PyComparisonOp,
        vm: &VirtualMachine,
    ) -> PyResult<Either<PyObjectRef, PyComparisonValue>> {
        if let Some(zelf) = zelf.downcast_ref::<Self>() {
            let ret: PyVector = zelf.richcompare(other.to_owned(), op, vm)?;
            let ret = ret.into_pyobject(vm);
            Ok(Either::A(ret))
        } else {
            Err(vm.new_type_error(format!(
                "unexpected payload {:?} for {}",
                zelf,
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
        Err(vm.new_type_error(format!("Unsupported pyobject type: {obj:?}")))
    }
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
    // TODO(discord9): use `PyResult` instead of `Option` for better error handling
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
            _ => None,
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
