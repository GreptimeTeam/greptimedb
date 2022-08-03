use std::sync::Arc;

use datatypes::vectors::VectorRef;
use rustpython_vm::class::PyClassImpl;
use rustpython_vm::{scope::Scope, AsObject, VirtualMachine};

use super::*;
fn set_items_in_scope(
    scope: &Scope,
    vm: &VirtualMachine,
    arg_names: &[&str],
    args: Vec<PyVector>,
) -> Result<(), String> {
    let res = arg_names.iter().zip(args).try_for_each(|(name, vector)| {
            scope
                .locals
                .as_object()
                .set_item(name.to_owned(), vm.new_pyobj(vector), vm)
                .map_err(|err| {
                    format!(
                        "Error in setting var {name} in scope: \n{}",
                        to_serde_excep(err, vm).unwrap_or_else(|double_err| format!(
                            "Another exception occur during serialize exception to string:\n{double_err}"
                        ))
                    )
                })
        });
    res
}

#[test]
fn test_vm() {
    rustpython_vm::Interpreter::with_init(Default::default(), |vm| {
        vm.add_native_module("udf_builtins", Box::new(udf_builtins::make_module));
        // this can be in `.enter()` closure, but for clearity, put it in the `with_init()`
        PyVector::make_class(&vm.ctx);
    })
    .enter(|vm| {
        let values = vec![1.0, 2.0, 3.0];
        let pows = vec![0i8, -1i8, 3i8];

        let args: Vec<VectorRef> = vec![
            Arc::new(datatypes::vectors::Float32Vector::from_vec(values)),
            Arc::new(datatypes::vectors::Int8Vector::from_vec(pows)),
        ];
        let args: Vec<PyVector> = args.into_iter().map(PyVector::from).collect();

        let scope = vm.new_scope_with_builtins();
        set_items_in_scope(&scope, vm, &["values", "pows"], args).unwrap();
        let code_obj = vm
            .compile(
                r#"
from udf_builtins import *
approx_distinct(pows)"#,
                rustpython_vm::compile::Mode::BlockExpr,
                "<embedded>".to_owned(),
            )
            .map_err(|err| vm.new_syntax_error(&err))
            .unwrap();
        let res = vm.run_code_obj(code_obj, scope);
        println!("{:#?}", res);
        if let Err(e) = res {
            let err_res = to_serde_excep(e, vm).unwrap();
            println!("Error:\n{err_res}");
        }
    });
}
fn to_serde_excep(
    excep: rustpython_vm::builtins::PyBaseExceptionRef,
    vm: &VirtualMachine,
) -> Result<String, String> {
    let mut chain = String::new();
    let r = vm.write_exception(&mut chain, &excep);
    // FIXME: better error handling, perhaps with chain calls?
    if let Err(r) = r {
        return Err(format!("Fail to write to string, error: {:#?}", r));
    }
    Ok(chain)
}
