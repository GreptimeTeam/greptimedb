//! python udf supports
use rustpython_vm as vm;
mod type_;
use std::sync::Arc;

use common_recordbatch::RecordBatch;
use datatypes::vectors::*;
use rustpython_vm::{class::PyClassImpl, AsObject};
use type_::PyVector;

#[allow(unused)]
pub fn coprocessor(script: &str, rb: &RecordBatch) -> Result<RecordBatch, String> {
    // 1. parse the script and check if it's only a function with `@coprocessor` decorator, and get `args` and `returns`,
    // 2. check for exist of `args` in `rb`, if not found, return error
    // 3. get args from `rb`, and set_item in a python vm, then call function with given args,
    // 4. get returns as a PyList, and assign them according to `returns`
    // 5. return a assembled RecordBatch
    todo!()
}
pub fn execute_script(script: &str) -> vm::PyResult {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_execute_script() {
        let result = execute_script("a//b");
        assert!(result.is_ok());
    }
}
