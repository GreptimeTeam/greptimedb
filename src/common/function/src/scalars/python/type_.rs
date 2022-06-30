use std::ops::Deref;

use arrow::array::{Array, ArrayRef};
use arrow::compute::arithmetics;
use arrow::compute::cast;
use arrow::compute::cast::CastOptions;
use arrow::datatypes::DataType;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::{OrderedF32, OrderedFloat};
use datatypes::{
    value,
    vectors::{Helper, VectorRef},
};
use rustpython_vm::types::{Constructor, PyComparisonOp};
use rustpython_vm::{
    builtins::{PyBool, PyBytes, PyFloat, PyInt, PyNone, PyStr},
    protocol::PySequenceMethods,
    pyclass, pyimpl,
    types::AsSequence,
    AsObject, PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine,
};

#[pyclass(module = false, name = "vector")]
#[derive(PyPayload, Clone)]
pub struct PyVector {
    vector: VectorRef,
}

impl From<VectorRef> for PyVector {
    fn from(vector: VectorRef) -> Self {
        Self { vector }
    }
}

impl std::fmt::Debug for PyVector {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            fmt,
            "PyVector [{:?} ; {}]]",
            self.vector.data_type(),
            self.vector.len()
        )
    }
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

impl AsRef<PyVector> for PyVector {
    fn as_ref(&self) -> &PyVector {
        self
    }
}

#[pyimpl]
impl PyVector {
    #[inline]
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
        let left = self.vector.to_arrow_array();
        let right = right.vector.to_arrow_array();

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

        Ok(PyVector {
            vector: Helper::try_into_vector(&*result).map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {:?}, err: {:?}",
                    result, e
                ))
            })?,
        })
    }

    #[pymethod(name = "__radd__")]
    #[pymethod(magic)]
    fn add(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.arith_op(other, None, arithmetics::add, vm)
    }

    #[pymethod(magic)]
    fn sub(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.arith_op(other, None, arithmetics::sub, vm)
    }

    #[pymethod(magic)]
    fn rsub(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.arith_op(other, None, |a, b| arithmetics::sub(b, a), vm)
    }

    #[pymethod(name = "__rmul__")]
    #[pymethod(magic)]
    fn mul(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.arith_op(other, None, arithmetics::mul, vm)
    }

    #[pymethod(magic)]
    fn truediv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.arith_op(other, Some(DataType::Float64), arithmetics::div, vm)
    }

    #[pymethod(magic)]
    fn rtruediv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.arith_op(
            other,
            Some(DataType::Float64),
            |a, b| arithmetics::div(b, a),
            vm,
        )
    }

    #[pymethod(magic)]
    fn floordiv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.arith_op(other, Some(DataType::Int64), arithmetics::div, vm)
    }

    #[pymethod(magic)]
    fn rfloordiv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        self.arith_op(
            other,
            Some(DataType::Int64),
            |a, b| arithmetics::div(b, a),
            vm,
        )
    }

    #[pymethod(magic)]
    fn len(&self) -> usize {
        self.vector.len()
    }

    fn getitem_by_index(&self, i: isize, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        let i = pythonic_index(i, self.len())
            .ok_or_else(|| vm.new_index_error("PyVector index out of range".to_owned()))?;
        PyInt::from(1i32).into_ref(vm);
        Ok(into_py_obj(self.vector.get(i), vm).into())
    }

    fn setitem_by_index(
        zelf: PyRef<Self>,
        i: isize,
        value: PyObjectRef,
        vm: &VirtualMachine,
    ) -> PyResult<()> {
        unimplemented!()
    }
}
/// convert a `PyObjectRef` into a `datatypess::Value`(is that ok?)
/// if `obj` can be convert to given ConcreteDataType then return inner `Value` else return None
fn into_datatypes_value(
    obj: PyObjectRef,
    vm: &VirtualMachine,
    dtype: ConcreteDataType,
) -> Option<value::Value> {
    use value::Value;

    match dtype {
        ConcreteDataType::Null(_) => {
            if obj
                .is_instance(PyNone::class(vm).into(), vm)
                .unwrap_or(false)
            {
                Some(Value::Null)
            } else {
                None
            }
        }
        ConcreteDataType::Boolean(_) => {
            if obj
                .is_instance(PyBool::class(vm).into(), vm)
                .unwrap_or(false)
            {
                Some(Value::Boolean(
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
            if obj
                .is_instance(PyInt::class(vm).into(), vm)
                .unwrap_or(false)
            {
                match dtype {
                    ConcreteDataType::Int8(_) => obj
                        .try_into_value::<i8>(vm)
                        .ok()
                        .and_then(|v| Some(Value::Int8(v))),
                    ConcreteDataType::Int16(_) => obj
                        .try_into_value::<i16>(vm)
                        .ok()
                        .and_then(|v| Some(Value::Int16(v))),
                    ConcreteDataType::Int32(_) => obj
                        .try_into_value::<i32>(vm)
                        .ok()
                        .and_then(|v| Some(Value::Int32(v))),
                    ConcreteDataType::Int64(_) => obj
                        .try_into_value::<i64>(vm)
                        .ok()
                        .and_then(|v| Some(Value::Int64(v))),
                    _ => unreachable!(),
                }
            } else {
                unreachable!()
            }
        }
        ConcreteDataType::UInt8(_)
        | ConcreteDataType::UInt16(_)
        | ConcreteDataType::UInt32(_)
        | ConcreteDataType::UInt64(_) => {
            if obj
                .is_instance(PyInt::class(vm).into(), vm)
                .unwrap_or(false)
                && obj.clone().try_into_value::<i64>(vm).unwrap_or(-1) >= 0
            {
                match dtype {
                    ConcreteDataType::UInt8(_) => obj
                        .try_into_value::<u8>(vm)
                        .ok()
                        .and_then(|v| Some(Value::UInt8(v))),
                    ConcreteDataType::UInt16(_) => obj
                        .try_into_value::<u16>(vm)
                        .ok()
                        .and_then(|v| Some(Value::UInt16(v))),
                    ConcreteDataType::UInt32(_) => obj
                        .try_into_value::<u32>(vm)
                        .ok()
                        .and_then(|v| Some(Value::UInt32(v))),
                    ConcreteDataType::UInt64(_) => obj
                        .try_into_value::<u64>(vm)
                        .ok()
                        .and_then(|v| Some(Value::UInt64(v))),
                    _ => unreachable!(),
                }
            } else {
                None
            }
        }
        ConcreteDataType::Float32(_) | ConcreteDataType::Float64(_) => {
            if obj
                .is_instance(PyFloat::class(vm).into(), vm)
                .unwrap_or(false)
            {
                match dtype {
                    ConcreteDataType::Float32(_) => obj
                        .try_into_value::<f32>(vm)
                        .ok()
                        .and_then(|v| Some(Value::Float32(OrderedFloat(v)))),
                    ConcreteDataType::Float64(_) => obj
                        .try_into_value::<f64>(vm)
                        .ok()
                        .and_then(|v| Some(Value::Float64(OrderedFloat(v)))),
                    _ => unreachable!(),
                }
            } else {
                None
            }
        }
        _ => todo!(),
    }
}
/// convert a DataType `Value` into a `PyObjectRef`(is that ok?)
fn into_py_obj(val: value::Value, vm: &VirtualMachine) -> PyObjectRef {
    use value::Value::*;
    match val {
        // FIXME: properly init a `None`
        // This comes from:https://github.com/RustPython/RustPython/blob/8ab4e770351d451cfdff5dc2bf8cce8df76a60ab/vm/src/builtins/singletons.rs#L37
        // None in Python is universally singleton so
        Null => vm.ctx.none(),
        // FIXME: properly init a `bool`
        Boolean(v) => PyInt::from(if v { 1u8 } else { 0u8 }).into_pyobject(vm),
        UInt8(v) => PyInt::from(v).into_pyobject(vm),
        UInt32(v) => PyInt::from(v).into_pyobject(vm),
        UInt64(v) => PyInt::from(v).into_pyobject(vm),
        Int16(v) => PyInt::from(v).into_pyobject(vm),
        Int32(v) => PyInt::from(v).into_pyobject(vm),
        Int64(v) => PyInt::from(v).into_pyobject(vm),
        Float32(v) => PyFloat::from(v.0 as f64).into_pyobject(vm),
        Float64(v) => PyFloat::from(v.0).into_pyobject(vm),
        String(s) => PyStr::from(s.as_utf8()).into_pyobject(vm),
        // is this copy necessary?
        Binary(b) => PyBytes::from(b.deref().to_vec()).into_pyobject(vm),
        Date(v) => PyInt::from(v).into_pyobject(vm),
        DateTime(v) => PyInt::from(v).into_pyobject(vm),
        _ => todo!(),
    }
}

/// check `i` is in range of `len` in a Pythonic manner
///
/// note that using Python semantics `-1` means last elem in list
/// So `i` is isize
fn pythonic_index(i: isize, len: usize) -> Option<usize> {
    if i >= 0 {
        // This try_into should never return Err for isize->usize when isize>0 which should always fit
        // but for the sake of do not panic, we set default value of len(So rustpython will panic for us if unthinkable happened and need to panic)
        let i = i.try_into().unwrap_or(len);
        if i < len {
            Some(i)
        } else {
            None
        }
    } else {
        // i < 0, count rev from last elem to first
        let i = (-i).try_into().unwrap_or(len);
        if i <= len {
            // Starting from -1 so:
            Some(len - i)
        } else {
            None
        }
    }
}

impl AsSequence for PyVector {
    const AS_SEQUENCE: PySequenceMethods = PySequenceMethods {
        length: Some(|seq, _vm| Ok(Self::sequence_downcast(seq).len())),
        item: Some(|seq, i, vm| {
            let zelf = Self::sequence_downcast(seq);
            zelf.getitem_by_index(i, vm)
        }),
        ass_item: Some(|seq, i, value, vm| {
            let zelf = Self::sequence_downcast(seq);
            if let Some(value) = value {
                Self::setitem_by_index(zelf.to_owned(), i, value, vm)
            } else {
                Err(vm.new_type_error("PyVector object doesn't support item deletion".to_owned()))
            }
        }),
        ..PySequenceMethods::NOT_IMPLEMENTED
    };
}
#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_wrapped_at() {
        let i: isize = 1;
        let len: usize = 3;
        assert_eq!(pythonic_index(i, len), Some(1));
        let i: isize = -1;
        assert_eq!(pythonic_index(i, len), Some(2));
        let i: isize = -4;
        assert_eq!(pythonic_index(i, len), None);
        let i: isize = 4;
        assert_eq!(pythonic_index(i, len), None);
    }
}
