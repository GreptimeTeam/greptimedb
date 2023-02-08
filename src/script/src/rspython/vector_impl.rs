use std::sync::Arc;

use crossbeam_utils::atomic::AtomicCell;
use datatypes::arrow::array::{Array, ArrayRef, BooleanArray};
use datatypes::arrow::compute;
use datatypes::arrow::compute::kernels::{arithmetic, boolean};
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::vectors::Helper;
use once_cell::sync::Lazy;
use rustpython_vm::builtins::{PyBaseExceptionRef, PyBool, PyFloat, PyInt, PyNone, PyStr};
use rustpython_vm::function::{Either, OptionalArg, PyComparisonValue};
use rustpython_vm::protocol::{PyMappingMethods, PySequenceMethods};
use rustpython_vm::types::{AsMapping, AsSequence, Comparable, PyComparisonOp};
use rustpython_vm::{
    atomic_func, pyclass as rspyclass, PyObject, PyObjectRef, PyPayload, PyResult, VirtualMachine,
};

use crate::ffi_types::vector::{
    arrow_rfloordiv, arrow_rsub, arrow_rtruediv, is_pyobj_scalar, pyobj_try_to_typed_val,
    wrap_result, PyVector,
};
use crate::python::utils::{is_instance, PyVectorRef};
/// PyVectors' rustpython specify methods

/// PyVector type wraps a greptime vector, impl multiply/div/add/sub opeerators etc.
#[rspyclass(with(AsMapping, AsSequence, Comparable))]
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
                buf.push_value_ref(val.as_value_ref()).unwrap();
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
    fn add(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, wrap_result(arithmetic::add_dyn), vm)
        } else {
            self.arith_op(other, None, arithmetic::add_dyn, vm)
        }
    }

    #[pymethod(magic)]
    fn sub(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, wrap_result(arithmetic::subtract_dyn), vm)
        } else {
            self.arith_op(other, None, arithmetic::subtract_dyn, vm)
        }
    }

    #[pymethod(magic)]
    fn rsub(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, arrow_rsub, vm)
        } else {
            self.arith_op(other, None, |a, b| arithmetic::subtract_dyn(b, a), vm)
        }
    }

    #[pymethod(name = "__rmul__")]
    #[pymethod(magic)]
    fn mul(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, None, wrap_result(arithmetic::multiply_dyn), vm)
        } else {
            self.arith_op(other, None, arithmetic::multiply_dyn, vm)
        }
    }

    #[pymethod(magic)]
    fn truediv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(
                other,
                Some(ArrowDataType::Float64),
                wrap_result(arithmetic::divide_dyn),
                vm,
            )
        } else {
            self.arith_op(
                other,
                Some(ArrowDataType::Float64),
                arithmetic::divide_dyn,
                vm,
            )
        }
    }

    #[pymethod(magic)]
    fn rtruediv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(other, Some(ArrowDataType::Float64), arrow_rtruediv, vm)
        } else {
            self.arith_op(
                other,
                Some(ArrowDataType::Float64),
                |a, b| arithmetic::divide_dyn(b, a),
                vm,
            )
        }
    }

    #[pymethod(magic)]
    fn floordiv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            self.scalar_arith_op(
                other,
                Some(ArrowDataType::Int64),
                wrap_result(arithmetic::divide_dyn),
                vm,
            )
        } else {
            self.arith_op(
                other,
                Some(ArrowDataType::Int64),
                arithmetic::divide_dyn,
                vm,
            )
        }
    }

    #[pymethod(magic)]
    fn rfloordiv(&self, other: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        if is_pyobj_scalar(&other, vm) {
            // FIXME: DataType convert problem, target_type should be inferred?
            self.scalar_arith_op(other, Some(ArrowDataType::Int64), arrow_rfloordiv, vm)
        } else {
            self.arith_op(
                other,
                Some(ArrowDataType::Int64),
                |a, b| arithmetic::divide_dyn(b, a),
                vm,
            )
        }
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
        let res = boolean::and(left, right).map_err(|err| from_debug_error(err, vm))?;
        let res = Arc::new(res) as ArrayRef;
        let ret = Helper::try_into_vector(res.clone()).map_err(|err| from_debug_error(err, vm))?;
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
        let res = boolean::or(left, right).map_err(|err| from_debug_error(err, vm))?;
        let res = Arc::new(res) as ArrayRef;
        let ret = Helper::try_into_vector(res.clone()).map_err(|err| from_debug_error(err, vm))?;
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
        let res = boolean::not(left).map_err(|err| from_debug_error(err, vm))?;
        let res = Arc::new(res) as ArrayRef;
        let ret = Helper::try_into_vector(res.clone()).map_err(|err| from_debug_error(err, vm))?;
        Ok(ret.into())
    }

    #[pymethod(name = "__len__")]
    fn len_rspy(&self) -> usize {
        self.len()
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
                Err(vm.new_type_error("PyVector object doesn't support item assigns".to_owned()))
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
        // TODO(discord9): return a boolean array of compare result
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

fn from_debug_error(err: impl std::fmt::Debug, vm: &VirtualMachine) -> PyBaseExceptionRef {
    vm.new_runtime_error(format!("Runtime Error: {err:#?}"))
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
