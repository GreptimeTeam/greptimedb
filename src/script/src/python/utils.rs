use rustpython_vm::{builtins::PyBaseExceptionRef, PyObjectRef, PyPayload, VirtualMachine, PyRef};
use snafu::{Backtrace, GenerateImplicitData};

use crate::python::error;
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
