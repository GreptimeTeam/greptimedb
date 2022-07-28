//! python udf supports
//! use the function `coprocessor` to parse and run a python function with arguments from recordBatch, and return a newly assembled RecordBatch
mod copr_parse;
mod coprocessor;
mod error;
#[cfg(test)]
mod test;
mod type_;

use std::sync::Arc;

pub use coprocessor::exec_coprocessor;
use coprocessor::AnnotationInfo;
use datatypes::vectors::{Float32Vector, Int32Vector, VectorRef};
use rustpython_parser::ast::Location;
use rustpython_vm as vm;
use rustpython_vm::{class::PyClassImpl, AsObject};
use type_::PyVector;

pub use crate::error::Error;

pub fn pretty_print_loc_in_src(script: &str, loc: &Location, desc: &str) -> String {
    let lines: Vec<&str> = script.split('\n').collect();
    let indicate = format!(
        "
\u{001B}[1;34m-->\u{001B}[0m python coprocessor function:{loc}
{line}
\u{001B}[1;31m{arrow:>pad$} {desc}\u{001B}[0m",
        line = lines[loc.row() - 1],
        pad = loc.column(),
        arrow = "^"
    );
    indicate
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
