use rustpython_vm::{PyObjectRef, PyPayload, VirtualMachine};

/// use `rustpython`'s `is_instance` method to check if a PyObject is a instance of class.
/// if `PyResult` is Err, then this function return `false`
pub(crate) fn is_instance<T: PyPayload>(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
    obj.is_instance(T::class(vm).into(), vm).unwrap_or(false)
}

/// for now it's only used in test code so cfg_attr to allow(unused) for not test code
#[cfg_attr(not(test), allow(unused))]
pub(crate) fn to_serde_excep(
    excep: rustpython_vm::builtins::PyBaseExceptionRef,
    vm: &VirtualMachine,
) -> Result<String, String> {
    let mut chain = String::new();
    let r = vm.write_exception(&mut chain, &excep);
    // FIXME(discord9): better error handling, perhaps with chain calls?
    if let Err(r) = r {
        return Err(format!("Fail to write to string, error: {:#?}", r));
    }
    Ok(chain)
}
