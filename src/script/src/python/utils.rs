use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, PrimitiveArray};
use rustpython_vm::builtins::{PyBool, PyFloat, PyInt};
use rustpython_vm::{builtins::PyBaseExceptionRef, PyObjectRef, PyPayload, PyRef, VirtualMachine};
use snafu::OptionExt;
use snafu::{Backtrace, GenerateImplicitData};

use crate::python::error;
use crate::python::error::ret_other_error_with;
use crate::python::PyVector;

pub(crate) type PyVectorRef = PyRef<PyVector>;

/// use `rustpython`'s `is_instance` method to check if a PyObject is a instance of class.
/// if `PyResult` is Err, then this function return `false`
pub fn is_instance<T: PyPayload>(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
    obj.is_instance(T::class(vm).into(), vm).unwrap_or(false)
}

pub fn format_py_error(excep: PyBaseExceptionRef, vm: &VirtualMachine) -> error::Error {
    let mut msg = String::new();
    if let Err(e) = vm.write_exception(&mut msg, &excep) {
        return error::Error::PyRuntime {
            msg: format!("Failed to write exception msg, err: {}", e),
            backtrace: Backtrace::generate(),
        };
    }

    error::Error::PyRuntime {
        msg,
        backtrace: Backtrace::generate(),
    }
}

/// convert a single PyVector or a number(a constant)(wrapping in PyObjectRef) into a Array(or a constant array)
pub fn py_vec_obj_to_array(
    obj: &PyObjectRef,
    vm: &VirtualMachine,
    col_len: usize,
) -> Result<ArrayRef, error::Error> {
    if is_instance::<PyVector>(obj, vm) {
        let pyv = obj.payload::<PyVector>().with_context(|| {
            ret_other_error_with(format!("can't cast obj {:?} to PyVector", obj))
        })?;
        Ok(pyv.to_arrow_array())
    } else if is_instance::<PyInt>(obj, vm) {
        let val = obj
            .to_owned()
            .try_into_value::<i64>(vm)
            .map_err(|e| format_py_error(e, vm))?;
        let ret = PrimitiveArray::from_vec(vec![val; col_len]);
        Ok(Arc::new(ret) as _)
    } else if is_instance::<PyFloat>(obj, vm) {
        let val = obj
            .to_owned()
            .try_into_value::<f64>(vm)
            .map_err(|e| format_py_error(e, vm))?;
        let ret = PrimitiveArray::from_vec(vec![val; col_len]);
        Ok(Arc::new(ret) as _)
    } else if is_instance::<PyBool>(obj, vm) {
        let val = obj
            .to_owned()
            .try_into_value::<bool>(vm)
            .map_err(|e| format_py_error(e, vm))?;

        let ret = BooleanArray::from_iter(std::iter::repeat(Some(val)).take(col_len));
        Ok(Arc::new(ret) as _)
    } else {
        ret_other_error_with(format!("Expect a vector or a constant, found {:?}", obj)).fail()
    }
}
