use std::ops::Deref;

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use arrow::compute::arithmetics;
use arrow::compute::cast;
use arrow::compute::cast::CastOptions;
use arrow::datatypes::DataType;
use arrow::scalar::{PrimitiveScalar, Scalar};
use datatypes::data_type::ConcreteDataType;
use datatypes::value::OrderedFloat;
use datatypes::{
    value,
    vectors::{Helper, VectorBuilder, VectorRef},
};
use rustpython_vm::{
    builtins::{PyBool, PyBytes, PyFloat, PyInt, PyNone, PyStr, PyTypeRef, PyBaseExceptionRef},
    function::{FuncArgs, OptionalArg},
    protocol::{PyMappingMethods, PySequenceMethods},
    pyclass, pyimpl,
    sliceable::{wrap_index, SaturatedSlice, SequenceIndex},
    types::{AsMapping, AsSequence, Constructor, Initializer},
    AsObject, PyObject, PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine,
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
fn arrow2_rsub(arr: &dyn Array, val: &dyn Scalar) -> Box<dyn Array> {
    // b - a => a * (-1) + b
    let neg = arithmetics::mul_scalar(arr, &PrimitiveScalar::new(DataType::Int64, Some(-1i64)));
    arithmetics::add_scalar(neg.as_ref(), val)
}

fn arrow2_rtruediv(arr: &dyn Array, val: &dyn Scalar) -> Box<dyn Array> {
    // val / arr => one_arr / arr * val (this is simpler to write)
    let one_arr: Box<dyn Array> = if is_float(arr.data_type()) {
        Box::new(PrimitiveArray::from_values(vec![1f64; arr.len()]))
    } else if is_integer(arr.data_type()) {
        Box::new(PrimitiveArray::from_values(vec![1i64; arr.len()]))
    } else {
        unimplemented!(
            "truediv of {:?} Scalar with {:?} Array is not supported",
            val.data_type(),
            arr.data_type()
        )
    };
    let tmp = arithmetics::mul_scalar(one_arr.as_ref(), val);
    arithmetics::div(tmp.as_ref(), arr)
}

fn arrow2_rfloordiv(arr: &dyn Array, val: &dyn Scalar) -> Box<dyn Array> {
    // val // arr => one_arr // arr * val (this is simpler to write)
    let one_arr: Box<dyn Array> = if is_float(arr.data_type()) {
        Box::new(PrimitiveArray::from_values(vec![1f64; arr.len()]))
    } else if is_integer(arr.data_type()) {
        Box::new(PrimitiveArray::from_values(vec![1i64; arr.len()]))
    } else {
        unimplemented!(
            "truediv of {:?} Scalar with {:?} Array is not supported",
            val.data_type(),
            arr.data_type()
        )
    };
    let tmp = arithmetics::mul_scalar(one_arr.as_ref(), val);

    arrow::compute::cast::cast(
        arithmetics::div(tmp.as_ref(), arr).as_ref(),
        &DataType::Int64,
        cast::CastOptions {
            wrapped: false,
            partial: true,
        },
    )
    .expect("Can't cast to Int64: ")
}
/// use `rustpython`'s `is_instance` method to check if a PyObject is a instance of class.
/// if `PyResult` is Err, then this function return `false`
fn is_instance(obj: &PyObjectRef, ty: &PyObject, vm: &VirtualMachine) -> bool {
    obj.is_instance(ty, vm).unwrap_or(false)
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

impl AsRef<PyVector> for PyVector {
    fn as_ref(&self) -> &PyVector {
        self
    }
}

#[pyimpl(with(AsMapping, AsSequence, Constructor, Initializer))]
impl PyVector {
    #[inline]
    fn scalar_arith_op<F>(
        &self,
        other: PyObjectRef,
        target_type: Option<DataType>,
        op: F,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector>
    where
        F: Fn(&dyn Array, &dyn Scalar) -> Box<dyn Array>,
    {
        let (right, right_type) = {
            if is_instance(&other, PyInt::class(vm).into(), vm) {
                other
                    .clone()
                    .try_into_value::<i64>(vm)
                    .ok()
                    .map(|v| (value::Value::Int64(v), DataType::Int64))
            } else if is_instance(&other, PyFloat::class(vm).into(), vm) {
                other
                    .clone()
                    .try_into_value::<f64>(vm)
                    .ok()
                    .map(|v| (value::Value::Float64(OrderedFloat(v)), DataType::Float64))
            } else {
                None
            }
        }
        .ok_or_else(|| {
            // if above return a `None` then in here is wrapped as `PyResult::Err(...)
            vm.new_type_error(format!(
                "Can't cast right operand into Scalar of Int or Float, actual: {}",
                other.class().name()
            ))
            // and return by `?` operator
        })?;
        // assuming they are all 64 bit type if possible

        let left = self.vector.to_arrow_array();

        let left_type = left.data_type();
        let right_type = &right_type;
        // TODO: found better way to cast between signed and unsigned type
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
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, arithmetics::add_scalar, vm)
        } else {
            self.arith_op(other, None, arithmetics::add, vm)
        }
    }

    #[pymethod(magic)]
    fn sub(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, arithmetics::sub_scalar, vm)
        } else {
            self.arith_op(other, None, arithmetics::sub, vm)
        }
    }

    #[pymethod(magic)]
    fn rsub(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, arrow2_rsub, vm)
        } else {
            self.arith_op(other, None, |a, b| arithmetics::sub(b, a), vm)
        }
    }

    #[pymethod(name = "__rmul__")]
    #[pymethod(magic)]
    fn mul(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, arithmetics::mul_scalar, vm)
        } else {
            self.arith_op(other, None, arithmetics::mul, vm)
        }
    }

    #[pymethod(magic)]
    fn truediv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, Some(DataType::Float64), arithmetics::div_scalar, vm)
        } else {
            self.arith_op(other, Some(DataType::Float64), arithmetics::div, vm)
        }
    }

    #[pymethod(magic)]
    fn rtruediv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, Some(DataType::Float64), arrow2_rtruediv, vm)
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
            self.scalar_arith_op(other, Some(DataType::Int64), arithmetics::div_scalar, vm)
        } else {
            self.arith_op(other, Some(DataType::Int64), arithmetics::div, vm)
        }
    }

    #[pymethod(magic)]
    fn rfloordiv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            // FIXME: DataType convert problem, target_type should be infered?
            self.scalar_arith_op(other, Some(DataType::Int64), arrow2_rfloordiv, vm)
        } else {
            self.arith_op(
                other,
                Some(DataType::Int64),
                |a, b| arithmetics::div(b, a),
                vm,
            )
        }
    }

    #[pymethod(magic)]
    fn len(&self) -> usize {
        self.vector.len()
    }

    #[pymethod(magic)]
    fn doc(&self) -> PyResult<PyStr> {
        Ok(PyStr::from(
            "PyVector is like a Python array, a compact array of elem of same datatype, but Readonly for now",
        ))
    }

    fn _getitem(&self, needle: &PyObject, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        match SequenceIndex::try_from_borrowed_object(vm, needle, "mmap")? {
            SequenceIndex::Int(i) => self.getitem_by_index(i, vm),
            SequenceIndex::Slice(slice) => self.getitem_by_slice(&slice, vm),
        }
    }

    fn getitem_by_index(&self, i: isize, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        // in the newest version of rustpython_vm, wrapped_at for isize is replace by wrap_index(i, len)
        let i = wrap_index(i, self.len())
            .ok_or_else(|| vm.new_index_error("PyVector index out of range".to_owned()))?;
        Ok(val_to_pyobj(self.vector.get(i), vm))
    }

    /// Return a `PyVector` in `PyObjectRef`
    fn getitem_by_slice(
        &self,
        slice: &SaturatedSlice,
        vm: &VirtualMachine,
    ) -> PyResult<PyObjectRef> {
        // adjust_indices so negative number is transform to usize
        let (mut range, step, slice_len) = slice.adjust_indices(self.len());
        let mut buf = VectorBuilder::with_capacity(self.vector.data_type(), slice_len);
        if slice_len == 0 {
            let v: PyVector = buf.finish().into();
            Ok(v.into_pyobject(vm))
        } else if step == 1 {
            let v: PyVector = self
                .vector
                .slice(range.next().unwrap_or(0), slice_len)
                .into();
            Ok(v.into_pyobject(vm))
        } else if step.is_negative() {
            // Negative step require special treatment
            for i in range.rev().step_by(step.unsigned_abs()) {
                buf.push(&self.vector.get(i))
            }
            let v: PyVector = buf.finish().into();
            Ok(v.into_pyobject(vm))
        } else {
            for i in range.step_by(step.unsigned_abs()) {
                buf.push(&self.vector.get(i))
            }
            let v: PyVector = buf.finish().into();
            Ok(v.into_pyobject(vm))
        }
    }

    /// Unsupport
    /// TODO: make it work
    #[allow(unused)]
    fn setitem_by_index(
        zelf: PyRef<Self>,
        i: isize,
        value: PyObjectRef,
        vm: &VirtualMachine,
    ) -> PyResult<()> {
        unimplemented!()
    }
}

/// if this pyobj can be cast to a scalar value(i.e Null/Int/Float/Bool)
#[inline]
fn is_pyobj_scalar(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
    //let is_instance = |ty: &PyObject| obj.is_instance(ty, vm).unwrap_or(false);
    is_instance(obj, PyNone::class(vm).into(), vm)
        || is_instance(obj, PyInt::class(vm).into(), vm)
        || is_instance(obj, PyFloat::class(vm).into(), vm)
        || is_instance(obj, PyBool::class(vm).into(), vm)
}

/// convert a `PyObjectRef` into a `datatypess::Value`(is that ok?)
/// if `obj` can be convert to given ConcreteDataType then return inner `Value` else return None
/// if dtype is None, return types with highest precision
/// Not used for now but may be use in future
#[allow(unused)]
fn pyobj_try_to_typed_val(
    obj: PyObjectRef,
    vm: &VirtualMachine,
    dtype: Option<ConcreteDataType>,
) -> Option<value::Value> {
    if let Some(dtype) = dtype {
        match dtype {
            ConcreteDataType::Null(_) => {
                if is_instance(&obj, PyNone::class(vm).into(), vm) {
                    Some(value::Value::Null)
                } else {
                    None
                }
            }
            ConcreteDataType::Boolean(_) => {
                if is_instance(&obj, PyBool::class(vm).into(), vm)
                    || is_instance(&obj, PyInt::class(vm).into(), vm)
                {
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
                if is_instance(&obj, PyInt::class(vm).into(), vm) {
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
                if is_instance(&obj, PyInt::class(vm).into(), vm)
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
                if is_instance(&obj, PyFloat::class(vm).into(), vm) {
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
                if is_instance(&obj, PyStr::class(vm).into(), vm) {
                    obj.try_into_value::<String>(vm)
                        .ok()
                        .map(|v| value::Value::String(v.into()))
                } else {
                    None
                }
            }
            ConcreteDataType::Binary(_) => {
                if is_instance(&obj, PyBytes::class(vm).into(), vm) {
                    obj.try_into_value::<Vec<u8>>(vm).ok().and_then(|v| {
                        String::from_utf8(v)
                            .ok()
                            .map(|v| value::Value::String(v.into()))
                    })
                } else {
                    None
                }
            }
        }
    } else if is_instance(&obj, PyNone::class(vm).into(), vm) {
        // if Untyped then by default return types with highest precision
        Some(value::Value::Null)
    } else if is_instance(&obj, PyBool::class(vm).into(), vm) {
        Some(value::Value::Boolean(
            obj.try_into_value::<bool>(vm).unwrap_or(false),
        ))
    } else if is_instance(&obj, PyInt::class(vm).into(), vm) {
        obj.try_into_value::<i64>(vm).ok().map(value::Value::Int64)
    } else if is_instance(&obj, PyFloat::class(vm).into(), vm) {
        obj.try_into_value::<f64>(vm)
            .ok()
            .map(|v| value::Value::Float64(OrderedFloat(v)))
    } else if is_instance(&obj, PyStr::class(vm).into(), vm) {
        obj.try_into_value::<Vec<u8>>(vm).ok().and_then(|v| {
            String::from_utf8(v)
                .ok()
                .map(|v| value::Value::String(v.into()))
        })
    } else if is_instance(&obj, PyBytes::class(vm).into(), vm) {
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
fn val_to_pyobj(val: value::Value, vm: &VirtualMachine) -> PyObjectRef {
    match val {
        // This comes from:https://github.com/RustPython/RustPython/blob/8ab4e770351d451cfdff5dc2bf8cce8df76a60ab/vm/src/builtins/singletons.rs#L37
        // None in Python is universally singleton so
        value::Value::Null => vm.ctx.none(),
        value::Value::Boolean(v) => vm.ctx.new_bool(v).into(),
        value::Value::UInt8(v) => PyInt::from(v).into_pyobject(vm),
        value::Value::UInt16(v) => PyInt::from(v).into_pyobject(vm),
        value::Value::UInt32(v) => PyInt::from(v).into_pyobject(vm),
        value::Value::UInt64(v) => PyInt::from(v).into_pyobject(vm),
        value::Value::Int8(v) => PyInt::from(v).into_pyobject(vm),
        value::Value::Int16(v) => PyInt::from(v).into_pyobject(vm),
        value::Value::Int32(v) => PyInt::from(v).into_pyobject(vm),
        value::Value::Int64(v) => PyInt::from(v).into_pyobject(vm),
        value::Value::Float32(v) => PyFloat::from(v.0 as f64).into_pyobject(vm),
        value::Value::Float64(v) => PyFloat::from(v.0).into_pyobject(vm),
        value::Value::String(s) => PyStr::from(s.as_utf8()).into_pyobject(vm),
        // is this copy necessary?
        value::Value::Binary(b) => PyBytes::from(b.deref().to_vec()).into_pyobject(vm),
        // is `Date` and `DateTime` supported yet? For now just ad hoc into PyInt
        value::Value::Date(v) => PyInt::from(v).into_pyobject(vm),
        value::Value::DateTime(v) => PyInt::from(v).into_pyobject(vm),
    }
}

impl Constructor for PyVector {
    type Args = FuncArgs;

    /// TODO: found out how to make it work in python
    #[allow(unused)]
    fn py_new(cls: PyTypeRef, args: FuncArgs, vm: &VirtualMachine) -> PyResult {
        todo!()
    }
}

impl Initializer for PyVector {
    type Args = OptionalArg<PyObjectRef>;

    /// TODO: found out how to test it in python
    #[allow(unused)]
    fn init(zelf: PyRef<Self>, iterable: Self::Args, vm: &VirtualMachine) -> PyResult<()> {
        todo!()
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
#[cfg(test)]
pub mod tests {

    use std::sync::Arc;

    use datatypes::vectors::{Float32Vector, Int32Vector, NullVector};
    use rustpython_vm::{builtins::PyList, class::PyClassImpl, protocol::PySequence};
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
                    // TODO: test Bytes and Date/DateTime
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
        script: &str,
        test_vec: Option<PyVector>,
        predicate: PredicateFn,
    ) -> Result<(PyObjectRef, Option<bool>), PyRef<rustpython_vm::builtins::PyBaseException>> {
        let mut pred_res = None;
        rustpython_vm::Interpreter::without_stdlib(Default::default())
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
                        rustpython_vm::compile::Mode::BlockExpr,
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
        for (code, pred) in snippet {
            let result = execute_script(code, None, pred);

            println!(
                "\u{001B}[35m{code}\u{001B}[0m: {:?}{}",
                result.clone().map(|v| v.0),
                result
                    .clone()
                    .map(|v| if let Some(v) = v.1 {
                        if v {
                            "\u{001B}[32m...[ok]\u{001B}[0m".to_string()
                        } else {
                            "\u{001B}[12m...[failed]\u{001B}[0m".to_string()
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
