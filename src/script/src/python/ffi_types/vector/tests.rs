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

//! Here are pair-tests for vector types in both rustpython and cpython
//!

// TODO: sample record batch

use std::collections::HashMap;
use std::sync::Arc;

use datatypes::scalars::ScalarVector;
use datatypes::vectors::{BooleanVector, Float64Vector, Int64Vector, VectorRef};
#[cfg(feature = "pyo3_backend")]
use pyo3::{types::PyDict, Python};
use rustpython_compiler::Mode;
use rustpython_vm::class::PyClassImpl;
use rustpython_vm::{vm, AsObject};

use crate::python::ffi_types::PyVector;
#[cfg(feature = "pyo3_backend")]
use crate::python::pyo3::{init_cpython_interpreter, vector_impl::into_pyo3_cell};

#[derive(Debug, Clone)]
struct TestCase {
    eval: String,
    result: VectorRef,
}

#[test]
fn test_eval_py_vector_in_pairs() {
    let locals: HashMap<_, _> = sample_py_vector()
        .into_iter()
        .map(|(k, v)| (k, PyVector::from(v)))
        .collect();

    let testcases = get_test_cases();

    for testcase in testcases {
        #[cfg(feature = "pyo3_backend")]
        eval_pyo3(testcase.clone(), locals.clone());
        eval_rspy(testcase, locals.clone())
    }
}

fn sample_py_vector() -> HashMap<String, VectorRef> {
    let b1 = Arc::new(BooleanVector::from_slice(&[false, false, true, true])) as VectorRef;
    let b2 = Arc::new(BooleanVector::from_slice(&[false, true, false, true])) as VectorRef;
    let f1 = Arc::new(Float64Vector::from_slice([0.0f64, 2.0, 10.0, 42.0])) as VectorRef;
    let f2 = Arc::new(Float64Vector::from_slice([-0.1f64, -42.0, 2., 7.0])) as VectorRef;
    let f3 = Arc::new(Float64Vector::from_slice([1.0f64, -42.0, 2., 7.0])) as VectorRef;
    HashMap::from([
        ("b1".to_owned(), b1),
        ("b2".to_owned(), b2),
        ("f1".to_owned(), f1),
        ("f2".to_owned(), f2),
        ("f3".to_owned(), f3),
    ])
}

/// testcases for test basic operations
/// this is more powerful&flexible than standalone testcases configure file
fn get_test_cases() -> Vec<TestCase> {
    let testcases = [
        TestCase {
            eval: "b1 & b2".to_string(),
            result: Arc::new(BooleanVector::from_slice(&[false, false, false, true])) as VectorRef,
        },
        TestCase {
            eval: "b1 | b2".to_string(),
            result: Arc::new(BooleanVector::from_slice(&[false, true, true, true])) as VectorRef,
        },
        TestCase {
            eval: "~b1".to_string(),
            result: Arc::new(BooleanVector::from_slice(&[true, true, false, false])) as VectorRef,
        },
        TestCase {
            eval: "f1+f2".to_string(),
            result: Arc::new(Float64Vector::from_slice([-0.1f64, -40.0, 12., 49.0])) as VectorRef,
        },
        TestCase {
            eval: "f1-f2".to_string(),
            result: Arc::new(Float64Vector::from_slice([0.1f64, 44.0, 8., 35.0])) as VectorRef,
        },
        TestCase {
            eval: "f1*f2".to_string(),
            result: Arc::new(Float64Vector::from_slice([-0.0f64, -84.0, 20., 42.0 * 7.0]))
                as VectorRef,
        },
        TestCase {
            eval: "f1/f2".to_string(),
            result: Arc::new(Float64Vector::from_slice([
                0.0 / -0.1f64,
                2. / -42.,
                10. / 2.,
                42. / 7.,
            ])) as VectorRef,
        },
        TestCase {
            eval: "f2.__rtruediv__(f1)".to_string(),
            result: Arc::new(Float64Vector::from_slice([
                0.0 / -0.1f64,
                2. / -42.,
                10. / 2.,
                42. / 7.,
            ])) as VectorRef,
        },
        TestCase {
            eval: "f2.__floordiv__(f3)".to_string(),
            result: Arc::new(Int64Vector::from_slice([0, 1, 1, 1])) as VectorRef,
        },
        TestCase {
            eval: "f3.__rfloordiv__(f2)".to_string(),
            result: Arc::new(Int64Vector::from_slice([0, 1, 1, 1])) as VectorRef,
        },
        TestCase {
            eval: "f3.filter(b1)".to_string(),
            result: Arc::new(Float64Vector::from_slice([2.0, 7.0])) as VectorRef,
        },
    ];
    Vec::from(testcases)
}
#[cfg(feature = "pyo3_backend")]
fn eval_pyo3(testcase: TestCase, locals: HashMap<String, PyVector>) {
    init_cpython_interpreter().unwrap();
    Python::with_gil(|py| {
        let locals = {
            let locals_dict = PyDict::new(py);
            for (k, v) in locals {
                locals_dict
                    .set_item(k, into_pyo3_cell(py, v).unwrap())
                    .unwrap();
            }
            locals_dict
        };
        let res = py.eval(&testcase.eval, None, Some(locals)).unwrap();
        let res_vec = res.extract::<PyVector>().unwrap();
        let raw_arr = res_vec.as_vector_ref().to_arrow_array();
        let expect_arr = testcase.result.to_arrow_array();
        if *raw_arr != *expect_arr {
            panic!("{raw_arr:?}!={expect_arr:?}")
        }
    })
}

fn eval_rspy(testcase: TestCase, locals: HashMap<String, PyVector>) {
    vm::Interpreter::with_init(Default::default(), |vm| {
        let _ = PyVector::make_class(&vm.ctx);
    })
    .enter(|vm| {
        let scope = vm.new_scope_with_builtins();
        locals.into_iter().for_each(|(k, v)| {
            scope
                .locals
                .as_object()
                .set_item(&k, vm.new_pyobj(v), vm)
                .unwrap();
        });
        let code_obj = vm
            .compile(&testcase.eval, Mode::Eval, "<embedded>".to_string())
            .map_err(|err| vm.new_syntax_error(&err))
            .unwrap();
        let obj = vm
            .run_code_obj(code_obj, scope)
            .map_err(|e| {
                let mut output = String::new();
                vm.write_exception(&mut output, &e).unwrap();
                (e, output)
            })
            .unwrap();
        let v = obj.downcast::<PyVector>().unwrap();
        let result_arr = v.to_arrow_array();
        let expect_arr = testcase.result.to_arrow_array();
        if *result_arr != *expect_arr {
            panic!("{result_arr:?}!={expect_arr:?}")
        }
    });
}
