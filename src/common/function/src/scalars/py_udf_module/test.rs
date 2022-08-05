use std::{collections::HashMap, fs::File, io::Read, path::Path, sync::Arc};

use arrow::{
    array::{Float64Array, Int64Array},
    compute::cast::CastOptions,
    datatypes::DataType,
};
use datatypes::vectors::VectorRef;
use ron::from_str as from_ron_string;
use rustpython_vm::{
    builtins::{PyFloat, PyInt, PyList},
    class::PyClassImpl,
    convert::ToPyObject,
    scope::Scope,
    AsObject, PyObjectRef, VirtualMachine,
};
use serde::{Deserialize, Serialize};

use super::builtins::*;
use crate::scalars::python::PyVector;

#[derive(Debug, Serialize, Deserialize)]
struct TestCase {
    input: HashMap<String, Var>,
    script: String,
    expect: Result<Var, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Var {
    value: PyValue,
    ty: DataType,
}

/// for floating number comparsion
const EPS: f64 = 2.0 * f64::EPSILON;

/// Null element just not supported for now for simplicity with writing test cases
#[derive(Debug, Serialize, Deserialize)]
enum PyValue {
    FloatVec(Vec<f64>),
    IntVec(Vec<i64>),
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    /// for test if the length of FloatVec is of the same as `LenFloatVec.0`
    LenFloatVec(usize),
    /// for test if the length of IntVec is of the same as `LenIntVec.0`
    LenIntVec(usize),
    /// for test if result is within the bound of err using formula:
    /// `(res - value).abs() < (value.abs()* error_percent)`
    FloatWithError {
        value: f64,
        error_percent: f64,
    },
}

impl PyValue {
    /// compare if results is just as expect, not using PartialEq because it is not transtive .e.g. [1,2,3] == len(3) == [4,5,6]
    fn just_as_expect(&self, other: &Self) -> bool {
        match (self, other) {
            (PyValue::FloatVec(a), PyValue::FloatVec(b)) => a
                .iter()
                .zip(b)
                .fold(true, |acc, (x, y)| acc && (x - y).abs() <= EPS),
            (PyValue::IntVec(a), PyValue::IntVec(b)) => a == b,
            (PyValue::Float(a), PyValue::Float(b)) => (a - b).abs() <= EPS,
            (PyValue::Int(a), PyValue::Int(b)) => a == b,
            // for just compare the length of vector
            (PyValue::LenFloatVec(len), PyValue::FloatVec(v)) => *len == v.len(),
            (PyValue::LenIntVec(len), PyValue::IntVec(v)) => *len == v.len(),
            (PyValue::FloatVec(v), PyValue::LenFloatVec(len)) => *len == v.len(),
            (PyValue::IntVec(v), PyValue::LenIntVec(len)) => *len == v.len(),
            (
                Self::Float(v),
                Self::FloatWithError {
                    value,
                    error_percent,
                },
            )
            | (
                Self::FloatWithError {
                    value,
                    error_percent,
                },
                Self::Float(v),
            ) => (v - value).abs() < (value.abs() * error_percent),
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

impl PyValue {
    fn to_py_obj(&self, vm: &VirtualMachine) -> Result<PyObjectRef, String> {
        let v: VectorRef = match self {
            PyValue::FloatVec(v) => {
                Arc::new(datatypes::vectors::Float64Vector::from_vec(v.clone()))
            }
            PyValue::IntVec(v) => Arc::new(datatypes::vectors::Int64Vector::from_vec(v.clone())),
            PyValue::Int(v) => return Ok(vm.ctx.new_int(*v).into()),
            PyValue::Float(v) => return Ok(vm.ctx.new_float(*v).into()),
            Self::Bool(v) => return Ok(vm.ctx.new_bool(*v).into()),
            Self::Str(s) => return Ok(vm.ctx.new_str(s.as_str()).into()),
            _ => return Err(format!("Unsupported type:{self:#?}")),
        };
        let v = PyVector::from(v).to_pyobject(vm);
        Ok(v)
    }

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
                    .collect::<Result<_, String>>()?;
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
                        _ => Err(format!("Expect only int/float in list, found {res:#?}")),
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
    let loc = Path::new("src/scalars/py_udf_module/testcases.ron");
    let loc = loc.to_str().expect("Fail to parse path");
    let mut file = File::open(loc).expect("Fail to open file");
    let mut buf = String::new();
    file.read_to_string(&mut buf)
        .expect("Fail to read to string");
    let testcases: Vec<TestCase> = from_ron_string(&buf).expect("Fail to convert to testcases");
    let cached_vm = rustpython_vm::Interpreter::with_init(Default::default(), |vm| {
        vm.add_native_module("udf_builtins", Box::new(udf_builtins::make_module));
        // this can be in `.enter()` closure, but for clearity, put it in the `with_init()`
        PyVector::make_class(&vm.ctx);
    });
    for (idx, case) in testcases.into_iter().enumerate() {
        print!("Testcase {idx} ...");
        cached_vm
        .enter(|vm| {
            let scope = vm.new_scope_with_builtins();
            case.input
                .iter()
                .try_for_each(|(k, v)| -> Result<(), String> {
                    let v = PyValue::to_py_obj(&v.value, vm).unwrap();
                    set_item_into_scope(&scope, vm, k, v)
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
                    if case.expect.is_ok(){
                        println!("\nError:\n{err_res}");
                        panic!("Expect Ok, found Error")
                    }
                    if !err_res.contains(&case.expect.unwrap_err()){
                        panic!("Error message not containing")
                    }
                }
                Ok(obj) => {
                    let ser = PyValue::from_py_obj(&obj, vm);
                    match (ser, case.expect){
                        (Ok(real), Ok(expect)) => {
                            if !(real.just_as_expect(&expect.value)){
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
        });
        println!(" passed!");
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
                let _ser = PyValue::from_py_obj(&obj, vm);
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
