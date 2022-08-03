use std::{collections::HashMap, fs::File, io::Read, path::Path, sync::Arc};

use datatypes::vectors::VectorRef;
use ron::from_str as from_ron_string;
use ron::to_string as to_ron_string;
use rustpython_vm::{class::PyClassImpl, scope::Scope, AsObject, VirtualMachine};
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Serialize, Deserialize)]
struct TestCase {
    input: HashMap<String, Var>,
    script: String,
    expect: Var
}

#[derive(Debug, Serialize, Deserialize)]
struct Var {
    value: PyVar,
    ty: DataType,
}

#[derive(Debug, Serialize, Deserialize)]
enum PyVar {
    FloatVec(Vec<f64>),
    IntVec(Vec<i64>),
    Int(i64),
    Float(f64),
}

impl PyVar{
    fn from_py_obj(obj: &PyObjectRef, vm:&VirtualMachine)->Self{
        if is_instance::<PyVector>(&obj, vm){
            let res = obj.payload::<PyVector>().unwrap();
        }
        todo!()
    }
}

#[test]
fn run_testcases() {
    let loc = Path::new("src/scalars/py_udf_module/builtins/testcases.ron");
    let loc = loc.to_str().expect("Fail to parse path");
    let mut file = File::open(loc).expect("Fail to open file");
    let mut buf = String::new();
    file.read_to_string(&mut buf)
        .expect("Fail to read to string");
    let testcases: Vec<TestCase> = from_ron_string(&buf).expect("Fail to convert to testcases");
    dbg!(testcases);
}

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
