use std::{collections::HashMap, fs::File, io::Read, path::Path, sync::Arc, thread::panicking};

use arrow::array::{Float64Array, Int64Array};
use datatypes::vectors::VectorRef;
use ron::from_str as from_ron_string;
use rustpython_vm::{
    class::PyClassImpl, convert::ToPyObject, scope::Scope, AsObject, VirtualMachine,
};
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Serialize, Deserialize)]
struct TestCase {
    input: HashMap<String, Var>,
    script: String,
    expect: Result<Var, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Var {
    value: PyVar,
    ty: DataType,
}

/// Null element just not supported for now for simplicity with writing test cases
#[derive(Debug, Serialize, Deserialize)]
enum PyVar {
    FloatVec(Vec<f64>),
    IntVec(Vec<i64>),
    Int(i64),
    Float(f64),
    /// just for test if the length of FloatVec is of the same as `VagueFloat.0`
    VagueFloat(usize),
    /// just for test if the length of IntVec is of the same as `VagueInt.0`
    VagueInt(usize),
}

impl PartialEq for PyVar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PyVar::FloatVec(a), PyVar::FloatVec(b)) => a == b,
            (PyVar::IntVec(a), PyVar::IntVec(b)) => a == b,
            (PyVar::Float(a), PyVar::Float(b)) => a == b,
            (PyVar::Int(a), PyVar::Int(b)) => a == b,
            (PyVar::VagueFloat(len), PyVar::FloatVec(v)) => *len == v.len(),
            (PyVar::VagueInt(len), PyVar::IntVec(v)) => *len == v.len(),
            (PyVar::FloatVec(v), PyVar::VagueFloat(len)) => *len == v.len(),
            (PyVar::IntVec(v), PyVar::VagueInt(len)) => *len == v.len(),
            (_, _) => false,
        }
    }
}

fn is_float(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// unsigned included
fn is_int(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
    )
}

impl PyVar {
    fn from_py_obj(obj: &PyObjectRef, vm: &VirtualMachine) -> Result<Self, String> {
        if is_instance::<PyVector>(obj, vm) {
            let res = obj.payload::<PyVector>().unwrap();
            let res = res.to_arrow_array();
            let ty = res.data_type();
            if is_float(ty) {
                let vec_f64 = arrow::compute::cast::cast(
                    res.as_ref(),
                    &DataType::Float64,
                    CastOptions {
                        wrapped: true,
                        partial: true,
                    },
                )
                .map_err(|err| format!("{err:#?}"))?;
                assert_eq!(vec_f64.data_type(), &DataType::Float64);
                let vec_f64 = vec_f64
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or(format!("Can't cast {vec_f64:#?} to Float64Array!"))?;
                let ret: Vec<f64> = vec_f64
                    .into_iter()
                    .enumerate()
                    .map(|(idx, v)| {
                        v.ok_or(format!(
                            "No null element expected, found one in {idx} position"
                        ))
                        .map(|v| v.to_owned())
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                Ok(Self::FloatVec(ret))
            } else if is_int(ty) {
                let vec_int = arrow::compute::cast::cast(
                    res.as_ref(),
                    &DataType::Int64,
                    CastOptions {
                        wrapped: true,
                        partial: true,
                    },
                )
                .map_err(|err| format!("{err:#?}"))?;
                assert_eq!(vec_int.data_type(), &DataType::Int64);
                let vec_i64 = vec_int
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or(format!("Can't cast {vec_int:#?} to Int64Array!"))?;
                let ret: Vec<i64> = vec_i64
                    .into_iter()
                    .enumerate()
                    .map(|(idx, v)| {
                        v.ok_or(format!(
                            "No null element expected, found one in {idx} position"
                        ))
                        .map(|v| v.to_owned())
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                Ok(Self::IntVec(ret))
            } else {
                Err(format!("unspupported DataType:{ty:#?}"))
            }
        } else if is_instance::<PyInt>(obj, vm) {
            let res = obj
                .to_owned()
                .try_into_value::<i64>(vm)
                .map_err(|err| to_serde_excep(err, vm).unwrap())?;
            Ok(Self::Int(res))
        } else if is_instance::<PyFloat>(obj, vm) {
            let res = obj
                .to_owned()
                .try_into_value::<f64>(vm)
                .map_err(|err| to_serde_excep(err, vm).unwrap())?;
            Ok(Self::Float(res))
        } else if is_instance::<PyList>(obj, vm) {
            let res = obj.payload::<PyList>().unwrap();
            let res: Vec<f64> = res
                .borrow_vec()
                .iter()
                .map(|obj| {
                    let res = Self::from_py_obj(obj, vm).unwrap();
                    assert!(matches!(res, Self::Float(_) | Self::Int(_)));
                    match res {
                        Self::Float(v) => Ok(v),
                        Self::Int(v) => Ok(v as f64),
                        _ => Err(format!("Expect only int/float in list")),
                    }
                })
                .collect::<Result<_, _>>()?;
            Ok(Self::FloatVec(res))
        } else {
            todo!()
        }
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
    for (idx, case) in testcases.into_iter().enumerate() {
        rustpython_vm::Interpreter::with_init(Default::default(), |vm| {
            vm.add_native_module("udf_builtins", Box::new(udf_builtins::make_module));
            // this can be in `.enter()` closure, but for clearity, put it in the `with_init()`
            PyVector::make_class(&vm.ctx);
        })
        .enter(|vm| {
            let scope = vm.new_scope_with_builtins();
            case.input
                .iter()
                .try_for_each(|(k, v)| -> Result<(), String> {
                    match &v.value {
                        PyVar::FloatVec(v) => {
                            let v: VectorRef =
                                Arc::new(datatypes::vectors::Float64Vector::from_vec(v.clone()));
                            let v = PyVector::from(v);
                            set_item_into_scope(&scope, vm, k, v)
                        }
                        PyVar::IntVec(v) => {
                            let v: VectorRef =
                                Arc::new(datatypes::vectors::Int64Vector::from_vec(v.clone()));
                            let v = PyVector::from(v);
                            set_item_into_scope(&scope, vm, k, v)
                        }
                        PyVar::Int(v) => set_item_into_scope(&scope, vm, k, *v),
                        PyVar::Float(v) => set_item_into_scope(&scope, vm, k, *v),
                        _ => panic!("Vague not allowed in input!")
                    }
                })
                .unwrap();
            let code_obj = vm
                .compile(
                    &case.script,
                    rustpython_vm::compile::Mode::BlockExpr,
                    "<embedded>".to_owned(),
                )
                .map_err(|err| vm.new_syntax_error(&err))
                .unwrap();
            let res = vm.run_code_obj(code_obj, scope);
            match res {
                Err(e) => {
                    let err_res = to_serde_excep(e, vm).unwrap();
                    println!("Error:\n{err_res}");
                    if case.expect.is_ok(){
                        panic!("Expect Ok, found Error")
                    }
                    if !err_res.contains(&case.expect.unwrap_err()){
                        panic!("Error message not containing")
                    }
                }
                Ok(obj) => {
                    let ser = PyVar::from_py_obj(&obj, vm);
                    match (ser, case.expect){
                        (Ok(real), Ok(expect)) => {
                            if !(real == expect.value){
                                panic!("Not as Expected for code:\n{}\n Real Value is {real:#?}, but expect {expect:#?}", case.script)
                            }
                        },
                        (Err(real), Err(expect)) => {
                            if !expect.contains(&real){
                                panic!("Expect Err(\"{expect}\"), found {real}")
                            }
                        },
                        (Ok(real), Err(expect)) => panic!("Expect Err({expect}), found Ok({real:?})"),
                        (Err(real), Ok(expect)) => panic!("Expect Ok({expect:?}), found Err({real})"),
                    };
                }
            };
            println!("Testcase {idx} ... passed!");
        });
    }
}

fn set_item_into_scope(
    scope: &Scope,
    vm: &VirtualMachine,
    name: &str,
    value: impl ToPyObject,
) -> Result<(), String> {
    scope
        .locals
        .as_object()
        .set_item(&name.to_owned(), vm.new_pyobj(value), vm)
        .map_err(|err| {
            format!(
                "Error in setting var {name} in scope: \n{}",
                to_serde_excep(err, vm).unwrap_or_else(|double_err| format!(
                    "Another exception occur during serialize exception to string:\n{double_err}"
                ))
            )
        })
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

#[allow(unused_must_use)]
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
sin(values)"#,
                rustpython_vm::compile::Mode::BlockExpr,
                "<embedded>".to_owned(),
            )
            .map_err(|err| vm.new_syntax_error(&err))
            .unwrap();
        let res = vm.run_code_obj(code_obj, scope);
        println!("{:#?}", res);
        match res {
            Err(e) => {
                let err_res = to_serde_excep(e, vm).unwrap();
                println!("Error:\n{err_res}");
            }
            Ok(obj) => {
                let _ser = PyVar::from_py_obj(&obj, vm);
                dbg!(_ser);
            }
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
