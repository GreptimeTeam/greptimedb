// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use common_telemetry::{error, info};
use datatypes::arrow::array::{Float64Array, Int64Array};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field};
use datatypes::vectors::{Float64Vector, Int64Vector, VectorRef};
use ron::from_str as from_ron_string;
use rustpython_vm::builtins::{PyFloat, PyInt, PyList};
use rustpython_vm::class::PyClassImpl;
use rustpython_vm::convert::ToPyObject;
use rustpython_vm::scope::Scope;
use rustpython_vm::{AsObject, PyObjectRef, VirtualMachine};
use serde::{Deserialize, Serialize};

use super::*;
use crate::python::ffi_types::PyVector;
use crate::python::rspython::utils::is_instance;
use crate::python::utils::format_py_error;
#[test]
fn convert_scalar_to_py_obj_and_back() {
    rustpython_vm::Interpreter::with_init(Default::default(), |vm| {
        // this can be in `.enter()` closure, but for clearity, put it in the `with_init()`
        let _ = PyVector::make_class(&vm.ctx);
    })
    .enter(|vm| {
        let col = DFColValue::Scalar(ScalarValue::Float64(Some(1.0)));
        let to = try_into_py_obj(col, vm).unwrap();
        let back = try_into_columnar_value(to, vm).unwrap();
        if let DFColValue::Scalar(ScalarValue::Float64(Some(v))) = back {
            if (v - 1.0).abs() > 2.0 * f64::EPSILON {
                panic!("Expect 1.0, found {v}")
            }
        } else {
            panic!("Convert errors, expect 1.0")
        }
        let col = DFColValue::Scalar(ScalarValue::Int64(Some(1)));
        let to = try_into_py_obj(col, vm).unwrap();
        let back = try_into_columnar_value(to, vm).unwrap();
        if let DFColValue::Scalar(ScalarValue::Int64(Some(v))) = back {
            assert_eq!(v, 1);
        } else {
            panic!("Convert errors, expect 1")
        }
        let col = DFColValue::Scalar(ScalarValue::UInt64(Some(1)));
        let to = try_into_py_obj(col, vm).unwrap();
        let back = try_into_columnar_value(to, vm).unwrap();
        if let DFColValue::Scalar(ScalarValue::Int64(Some(v))) = back {
            assert_eq!(v, 1);
        } else {
            panic!("Convert errors, expect 1")
        }
        let col = DFColValue::Scalar(ScalarValue::List(
            Some(vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
            ]),
            Arc::new(Field::new("item", ArrowDataType::Int64, false)),
        ));
        let to = try_into_py_obj(col, vm).unwrap();
        let back = try_into_columnar_value(to, vm).unwrap();
        if let DFColValue::Scalar(ScalarValue::List(Some(list), field)) = back {
            assert_eq!(list.len(), 2);
            assert_eq!(*field.data_type(), ArrowDataType::Int64);
        }
        let list: Vec<PyObjectRef> = vec![vm.ctx.new_int(1).into(), vm.ctx.new_int(2).into()];
        let nested_list: Vec<PyObjectRef> =
            vec![vm.ctx.new_list(list).into(), vm.ctx.new_int(3).into()];
        let list_obj = vm.ctx.new_list(nested_list).into();
        let col = try_into_columnar_value(list_obj, vm);
        if let Err(err) = col {
            let reason = format_py_error(err, vm);
            assert!(format!("{reason}").contains(
                "TypeError: All elements in a list should be same type to cast to Datafusion list!"
            ));
        }

        let list: PyVector =
            PyVector::from(
                Arc::new(Float64Vector::from_slice([0.1f64, 0.2, 0.3, 0.4])) as VectorRef
            );
        let nested_list: Vec<PyObjectRef> = vec![list.into_pyobject(vm), vm.ctx.new_int(3).into()];
        let list_obj = vm.ctx.new_list(nested_list).into();
        let expect_err = try_into_columnar_value(list_obj, vm);
        assert!(expect_err.is_err());
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct TestCase {
    input: HashMap<String, Var>,
    script: String,
    expect: Result<Var, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Var {
    value: PyValue,
    ty: ArrowDataType,
}

/// for floating number comparison
const EPS: f64 = 2.0 * f64::EPSILON;

/// Null element just not supported for now for simplicity with writing test cases
#[derive(Debug, Serialize, Deserialize)]
enum PyValue {
    FloatVec(Vec<f64>),
    FloatVecWithNull(Vec<Option<f64>>),
    IntVec(Vec<i64>),
    IntVecWithNull(Vec<Option<i64>>),
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

            (Self::FloatVecWithNull(a), Self::FloatVecWithNull(b)) => a == b,

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

fn is_float(ty: &ArrowDataType) -> bool {
    matches!(
        ty,
        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64
    )
}

/// unsigned included
fn is_int(ty: &ArrowDataType) -> bool {
    matches!(
        ty,
        ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64
            | ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
    )
}

impl PyValue {
    fn to_py_obj(&self, vm: &VirtualMachine) -> Result<PyObjectRef, String> {
        let v: VectorRef = match self {
            PyValue::FloatVec(v) => {
                Arc::new(datatypes::vectors::Float64Vector::from_vec(v.clone()))
            }
            PyValue::IntVec(v) => Arc::new(Int64Vector::from_vec(v.clone())),
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
                let vec_f64 = compute::cast(&res, &ArrowDataType::Float64)
                    .map_err(|err| format!("{err:#?}"))?;
                assert_eq!(vec_f64.data_type(), &ArrowDataType::Float64);
                let vec_f64 = vec_f64
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| format!("Can't cast {vec_f64:#?} to Float64Array!"))?;
                let ret = vec_f64.into_iter().collect::<Vec<_>>();
                if ret.iter().all(|x| x.is_some()) {
                    Ok(Self::FloatVec(
                        ret.into_iter().map(|i| i.unwrap()).collect(),
                    ))
                } else {
                    Ok(Self::FloatVecWithNull(ret))
                }
            } else if is_int(ty) {
                let vec_int = compute::cast(&res, &ArrowDataType::Int64)
                    .map_err(|err| format!("{err:#?}"))?;
                assert_eq!(vec_int.data_type(), &ArrowDataType::Int64);
                let vec_i64 = vec_int
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| format!("Can't cast {vec_int:#?} to Int64Array!"))?;
                let ret: Vec<i64> = vec_i64
                    .into_iter()
                    .enumerate()
                    .map(|(idx, v)| {
                        v.ok_or_else(|| {
                            format!("No null element expected, found one in {idx} position")
                        })
                    })
                    .collect::<Result<_, String>>()?;
                Ok(Self::IntVec(ret))
            } else {
                Err(format!("unspupported ArrowDataType:{ty:#?}"))
            }
        } else if is_instance::<PyInt>(obj, vm) {
            let res = obj
                .clone()
                .try_into_value::<i64>(vm)
                .map_err(|err| format_py_error(err, vm).to_string())?;
            Ok(Self::Int(res))
        } else if is_instance::<PyFloat>(obj, vm) {
            let res = obj
                .clone()
                .try_into_value::<f64>(vm)
                .map_err(|err| format_py_error(err, vm).to_string())?;
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
fn run_builtin_fn_testcases() {
    common_telemetry::init_default_ut_logging();

    let loc = Path::new("src/python/rspython/builtins/testcases.ron");
    let loc = loc.to_str().expect("Fail to parse path");
    let mut file = File::open(loc).expect("Fail to open file");
    let mut buf = String::new();
    let _ = file.read_to_string(&mut buf).unwrap();
    let testcases: Vec<TestCase> = from_ron_string(&buf).expect("Fail to convert to testcases");
    let cached_vm = rustpython_vm::Interpreter::with_init(Default::default(), |vm| {
        vm.add_native_module("greptime", Box::new(greptime_builtin::make_module));
        let _ = PyVector::make_class(&vm.ctx);
    });
    for (idx, case) in testcases.into_iter().enumerate() {
        info!("Testcase {idx} ...");
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
                    rustpython_compiler_core::Mode::BlockExpr,
                    "<embedded>".to_string(),
                )
                .map_err(|err| vm.new_syntax_error(&err))
                .unwrap();
            let res = vm.run_code_obj(code_obj, scope);
            match res {
                Err(e) => {
                    let err_res = format_py_error(e, vm).to_string();
                    match case.expect{
                        Ok(v) => {
                            error!("\nError:\n{err_res}");
                            panic!("Expect Ok: {v:?}, found Error");
                        },
                        Err(err) => {
                            if !err_res.contains(&err){
                                panic!("Error message not containing, expect {err_res}, found {err}")
                            }
                        }
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
        .set_item(&name.to_string(), vm.new_pyobj(value), vm)
        .map_err(|err| {
            format!(
                "Error in setting var {name} in scope: \n{}",
                format_py_error(err, vm)
            )
        })
}

fn set_lst_of_vecs_in_scope(
    scope: &Scope,
    vm: &VirtualMachine,
    arg_names: &[&str],
    args: Vec<PyVector>,
) -> Result<(), String> {
    let res = arg_names.iter().zip(args).try_for_each(|(name, vector)| {
        scope
            .locals
            .as_object()
            .set_item(&name.to_string(), vm.new_pyobj(vector), vm)
            .map_err(|err| {
                format!(
                    "Error in setting var {name} in scope: \n{}",
                    format_py_error(err, vm)
                )
            })
    });
    res
}

#[allow(unused_must_use)]
#[test]
fn test_vm() {
    common_telemetry::init_default_ut_logging();

    rustpython_vm::Interpreter::with_init(Default::default(), |vm| {
        vm.add_native_module("udf_builtins", Box::new(greptime_builtin::make_module));
        // this can be in `.enter()` closure, but for clearity, put it in the `with_init()`
        let _ = PyVector::make_class(&vm.ctx);
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
        set_lst_of_vecs_in_scope(&scope, vm, &["values", "pows"], args).unwrap();
        let code_obj = vm
            .compile(
                r#"
from udf_builtins import *
sin(values)"#,
                rustpython_compiler_core::Mode::BlockExpr,
                "<embedded>".to_string(),
            )
            .map_err(|err| vm.new_syntax_error(&err))
            .unwrap();
        let res = vm.run_code_obj(code_obj, scope);
        match res {
            Err(e) => {
                let err_res = format_py_error(e, vm).to_string();
                error!("Error:\n{err_res}");
            }
            Ok(obj) => {
                let _ser = PyValue::from_py_obj(&obj, vm);
                dbg!(_ser);
            }
        }
    });
}
