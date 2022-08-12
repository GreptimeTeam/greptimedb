use rustpython_vm::{PyObjectRef, PyPayload, VirtualMachine};

/// use `rustpython`'s `is_instance` method to check if a PyObject is a instance of class.
/// if `PyResult` is Err, then this function return `false`
pub fn is_instance<T: PyPayload>(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
    obj.is_instance(T::class(vm).into(), vm).unwrap_or(false)
}
