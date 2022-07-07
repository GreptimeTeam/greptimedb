use std::ops::Deref;

use arrow::array::{Array, ArrayRef};
use arrow::compute::arithmetics;
use arrow::compute::cast;
use arrow::compute::cast::CastOptions;
use arrow::datatypes::DataType;
use arrow::scalar::{PrimitiveScalar, Scalar};
//use common_base::bytes::StringBytes;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::OrderedFloat;
use datatypes::{
    value,
    vectors::{Helper, VectorBuilder, VectorRef},
};
use rustpython_vm::{
    builtins::{PyBool, PyBytes, PyFloat, PyInt, PyNone, PyStr, PyTypeRef},
    function::{FuncArgs, OptionalArg},
    protocol::{PyMappingMethods, PySequenceMethods},
    pyclass, pyimpl,
    sliceable::{SaturatedSlice, SequenceIndex},
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
        let right = pyobj_try_to_typed_val(other.clone(), vm, None).ok_or_else(|| {
            vm.new_type_error(format!(
                "Can't cast right operand into Scalar, actual: {}",
                other.class().name()
            ))
        })?;
        use value::Value;
        // assuming they are all 64 bit type if possible

        let left = self.vector.to_arrow_array();

        let left_type = &left.data_type();
        //let right_type = &right.data_type();

        let target_type = target_type.unwrap_or_else(|| {
            if is_signed(left_type) {
                DataType::Int64
            } else if is_unsigned(left_type) {
                DataType::UInt64
            } else {
                DataType::Float64
            }
        });
        let left = cast(left, &target_type, vm)?;
        let right: Box<dyn Scalar> = if target_type == DataType::Float64 {
            match right {
                Value::Int64(v) => {
                    Box::new(PrimitiveScalar::new(DataType::Float64, Some(v as f64)))
                }
                Value::Float64(v) => {
                    Box::new(PrimitiveScalar::new(DataType::Float64, Some(f64::from(v))))
                }
                _ => unreachable!(),
            }
        } else {
            match right {
                Value::Int64(v) => Box::new(PrimitiveScalar::new(DataType::Int64, Some(v))),
                Value::Float64(v) => Box::new(PrimitiveScalar::new(
                    DataType::Float64,
                    Some(f64::from(v) as i64),
                )),
                _ => unreachable!(),
            }
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
        self.arith_op(other, None, |a, b| arithmetics::sub(b, a), vm)
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
        self.arith_op(
            other,
            Some(DataType::Float64),
            |a, b| arithmetics::div(b, a),
            vm,
        )
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
        let i = pythonic_index(i, self.len())
            .ok_or_else(|| vm.new_index_error("PyVector index out of range".to_owned()))?;
        PyInt::from(1i32).into_ref(vm);
        Ok(val_to_pyobj(self.vector.get(i), vm))
    }

    /// Return a `PyVector` in `PyObjectRef`
    fn getitem_by_slice(
        &self,
        slice: &SaturatedSlice,
        vm: &VirtualMachine,
    ) -> PyResult<PyObjectRef> {
        // println!("{:?}", slice);
        // adjust_indices so negative number is transform to usize
        let (mut range, step, slice_len) = slice.adjust_indices(self.len());
        // println!("{:?},{step},{slice_len}", range);
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
fn is_pyobj_scalar(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
    let is_instance = |ty: &PyObject| obj.is_instance(ty, vm).unwrap_or(false);
    is_instance(PyNone::class(vm).into())
        || is_instance(PyInt::class(vm).into())
        || is_instance(PyFloat::class(vm).into())
        || is_instance(PyBool::class(vm).into())
}

/// convert a `PyObjectRef` into a `datatypess::Value`(is that ok?)
/// if `obj` can be convert to given ConcreteDataType then return inner `Value` else return None
/// if dtype is None, return types with highest precision
fn pyobj_try_to_typed_val(
    obj: PyObjectRef,
    vm: &VirtualMachine,
    dtype: Option<ConcreteDataType>,
) -> Option<value::Value> {
    use value::Value;
    let is_instance = |ty: &PyObject| obj.is_instance(ty, vm).unwrap_or(false);
    if let Some(dtype) = dtype {
        match dtype {
            ConcreteDataType::Null(_) => {
                if is_instance(PyNone::class(vm).into()) {
                    Some(Value::Null)
                } else {
                    None
                }
            }
            ConcreteDataType::Boolean(_) => {
                if is_instance(PyBool::class(vm).into()) || is_instance(PyInt::class(vm).into()) {
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
                if is_instance(PyInt::class(vm).into()) {
                    match dtype {
                        ConcreteDataType::Int8(_) => {
                            obj.try_into_value::<i8>(vm).ok().map(Value::Int8)
                        }
                        ConcreteDataType::Int16(_) => {
                            obj.try_into_value::<i16>(vm).ok().map(Value::Int16)
                        }
                        ConcreteDataType::Int32(_) => {
                            obj.try_into_value::<i32>(vm).ok().map(Value::Int32)
                        }
                        ConcreteDataType::Int64(_) => {
                            obj.try_into_value::<i64>(vm).ok().map(Value::Int64)
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
                if is_instance(PyInt::class(vm).into())
                    && obj.clone().try_into_value::<i64>(vm).unwrap_or(-1) >= 0
                {
                    match dtype {
                        ConcreteDataType::UInt8(_) => {
                            obj.try_into_value::<u8>(vm).ok().map(Value::UInt8)
                        }
                        ConcreteDataType::UInt16(_) => {
                            obj.try_into_value::<u16>(vm).ok().map(Value::UInt16)
                        }
                        ConcreteDataType::UInt32(_) => {
                            obj.try_into_value::<u32>(vm).ok().map(Value::UInt32)
                        }
                        ConcreteDataType::UInt64(_) => {
                            obj.try_into_value::<u64>(vm).ok().map(Value::UInt64)
                        }
                        _ => unreachable!(),
                    }
                } else {
                    None
                }
            }
            ConcreteDataType::Float32(_) | ConcreteDataType::Float64(_) => {
                if is_instance(PyFloat::class(vm).into()) {
                    match dtype {
                        ConcreteDataType::Float32(_) => obj
                            .try_into_value::<f32>(vm)
                            .ok()
                            .map(|v| Value::Float32(OrderedFloat(v))),
                        ConcreteDataType::Float64(_) => obj
                            .try_into_value::<f64>(vm)
                            .ok()
                            .map(|v| Value::Float64(OrderedFloat(v))),
                        _ => unreachable!(),
                    }
                } else {
                    None
                }
            }

            ConcreteDataType::String(_) => {
                if is_instance(PyStr::class(vm).into()) {
                    obj.try_into_value::<String>(vm)
                        .ok()
                        .map(|v| Value::String(v.into()))
                } else {
                    None
                }
            }
            ConcreteDataType::Binary(_) => {
                if is_instance(PyBytes::class(vm).into()) {
                    obj.try_into_value::<Vec<u8>>(vm)
                        .ok()
                        .and_then(|v| String::from_utf8(v).ok().map(|v| Value::String(v.into())))
                } else {
                    None
                }
            } //_ => unimplemented!("Unsupported data type of value {:?}", dtype),
        }
    } else if is_instance(PyNone::class(vm).into()) {
        // Untyped so by default return types with highest precision
        Some(Value::Null)
    } else if is_instance(PyBool::class(vm).into()) {
        Some(Value::Boolean(
            obj.try_into_value::<bool>(vm).unwrap_or(false),
        ))
    } else if is_instance(PyInt::class(vm).into()) {
        obj.try_into_value::<i64>(vm).ok().map(Value::Int64)
    } else if is_instance(PyFloat::class(vm).into()) {
        obj.try_into_value::<f64>(vm)
            .ok()
            .map(|v| Value::Float64(OrderedFloat(v)))
    } else if is_instance(PyStr::class(vm).into()) {
        obj.try_into_value::<Vec<u8>>(vm)
            .ok()
            .and_then(|v| String::from_utf8(v).ok().map(|v| Value::String(v.into())))
    } else if is_instance(PyBytes::class(vm).into()) {
        obj.try_into_value::<Vec<u8>>(vm)
            .ok()
            .and_then(|v| String::from_utf8(v).ok().map(|v| Value::String(v.into())))
    } else {
        None
    }
}

/// convert a DataType `Value` into a `PyObjectRef`
fn val_to_pyobj(val: value::Value, vm: &VirtualMachine) -> PyObjectRef {
    use value::Value::*;
    match val {
        // FIXME: properly init a `None`
        // This comes from:https://github.com/RustPython/RustPython/blob/8ab4e770351d451cfdff5dc2bf8cce8df76a60ab/vm/src/builtins/singletons.rs#L37
        // None in Python is universally singleton so
        Null => vm.ctx.none(),
        Boolean(v) => vm.ctx.new_bool(v).into(),
        UInt8(v) => PyInt::from(v).into_pyobject(vm),
        UInt16(v) => PyInt::from(v).into_pyobject(vm),
        UInt32(v) => PyInt::from(v).into_pyobject(vm),
        UInt64(v) => PyInt::from(v).into_pyobject(vm),
        Int8(v) => PyInt::from(v).into_pyobject(vm),
        Int16(v) => PyInt::from(v).into_pyobject(vm),
        Int32(v) => PyInt::from(v).into_pyobject(vm),
        Int64(v) => PyInt::from(v).into_pyobject(vm),
        Float32(v) => PyFloat::from(v.0 as f64).into_pyobject(vm),
        Float64(v) => PyFloat::from(v.0).into_pyobject(vm),
        String(s) => PyStr::from(s.as_utf8()).into_pyobject(vm),
        // is this copy necessary?
        Binary(b) => PyBytes::from(b.deref().to_vec()).into_pyobject(vm),
        // is `Date` and `DateTime` supported yet? For now just ad hoc into PyInt
        Date(v) => PyInt::from(v).into_pyobject(vm),
        DateTime(v) => PyInt::from(v).into_pyobject(vm)
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

impl Constructor for PyVector {
    type Args = FuncArgs;

    /// TODO: found out how to make it work in python
    #[allow(unused)]
    fn py_new(cls: PyTypeRef, args: FuncArgs, vm: &VirtualMachine) -> PyResult {
        //println!("Call constr: {:?}", args);
        todo!()
        /*PyVector::default()
        .into_ref_with_type(vm, cls)
        .map(Into::into)*/
    }
}

impl Initializer for PyVector {
    type Args = OptionalArg<PyObjectRef>;

    /// TODO: found out how to test it in python
    #[allow(unused)]
    fn init(zelf: PyRef<Self>, iterable: Self::Args, vm: &VirtualMachine) -> PyResult<()> {
        //println!("Call init: {:?}", iterable);
        Ok(())
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

    use rustpython_vm::protocol::PySequence;

    use super::*;
    /// test the paired `val_to_obj` and `pyobj_to_val` func
    #[test]
    fn test_val2pyobj2val() {
        use rustpython_vm as vm;
        vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
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
            //println!("{:?}, {:?}, {:?}", ri, rj, rn);
            assert_eq!(rj, None);
            assert_eq!(rn, Some(value::Value::Float64(OrderedFloat(2.0))));
            assert_eq!(ri, Some(value::Value::Float32(OrderedFloat(2.0))));
            let typed_lst = {
                use value::Value::*;
                [
                    Null,
                    Boolean(true),
                    Boolean(false),
                    // PyInt is Big Int
                    Int16(2),
                    Int32(2),
                    Int64(2),
                    UInt16(2),
                    UInt32(2), 
                    UInt64(2),
                    Float32(OrderedFloat(2.0)),
                    Float64(OrderedFloat(2.0)),
                    String("123".into()),
                    // TODO: testBytes and Date/DateTime 
                ]
            };
            for val in typed_lst {
                let obj = val_to_pyobj(val.clone(), vm);
                //println!("{:?}", obj);
                let ret = pyobj_try_to_typed_val(obj, vm, Some(val.data_type()));
                assert_eq!(ret, Some(val.clone()));
                //println!("{:?}, {:?}", ret, Some(val));
            }
            //assert_eq!(rj)
        })
    }

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

    #[test]
    fn test_getitem_by_index_in_vm() {
        use std::sync::Arc;

        use datatypes::vectors::*;
        use rustpython_vm as vm;
        use rustpython_vm::class::PyClassImpl;
        vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
            PyVector::make_class(&vm.ctx);
            let a: VectorRef = Arc::new(Int32Vector::from_vec(vec![1, 2, 3, 4]));
            let a = PyVector::from(a);
            //println!("{:?}", a.getitem_by_index(0, vm));
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

    pub fn execute_script(script: &str, test_vec: Option<PyVector>) -> PyResult {
        use std::sync::Arc;

        use datatypes::vectors::*;
        use rustpython_vm as vm;
        use rustpython_vm::class::PyClassImpl;
        vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
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
                    vm::compile::Mode::BlockExpr,
                    "<embedded>".to_owned(),
                )
                .map_err(|err| vm.new_syntax_error(&err))?;
            vm.run_code_obj(code_obj, scope)
        })
    }

    #[test]
    fn test_execute_script() {
        type RetType = Option<fn(PyResult<PyObjectRef>) -> bool>;
        let snippet: Vec<(&str, RetType)> = vec![
            ("len(a)", Some(|v| v.is_ok())),
            ("a[0]=1#Unsupport?", Some(|v| v.is_err())),
            ("a[-1]", None),
            ("a[0]*5", None),
            ("list(a)", None),
            ("a[1:-1]#elem in [1,3)", None),
            ("(a+1)[0]", Some(|v| v.is_ok())),
            ("(a-1)[0]", Some(|v| v.is_ok())),
            ("(a*2)[0]", Some(|v| v.is_ok())),
            ("(a/2.0)[2]", Some(|v| v.is_ok())),
            ("(a/2)[2]", Some(|v| v.is_ok())),
            ("(a//2)[2]", Some(|v| v.is_ok())),
            (
                "vector",
                Some(|_v| {
                    // possibly need to load the module of PyVector, but how
                    //println!("{:?}", v);
                    true
                }),
            ),
        ];
        for (code, pred) in snippet {
            let result = execute_script(code, None);
            println!(
                "\u{001B}[35m{code}\u{001B}[0m: \u{001B}[32m{:?}\u{001B}[0m",
                result
            );
            if let Some(p) = pred {
                assert!(p(result))
            }
        }

        //assert!(false);
    }
}
