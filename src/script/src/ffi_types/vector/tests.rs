//! Here are pair-tests for vector types in both rustpython and cpython
//!

// TODO: sample record batch

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::scalars::ScalarVector;
use datatypes::vectors::{BooleanVector, Float64Vector, VectorRef};
use pyo3::types::PyDict;
use pyo3::{PyCell, Python, ToPyObject};
use rustpython_compiler::Mode;
use rustpython_vm::class::PyClassImpl;
use rustpython_vm::{vm, AsObject};

use crate::ffi_types::PyVector;
use crate::pyo3::into_pyo3_cell;

#[derive(Debug, Clone)]
struct TestCase {
    eval: String,
    result: VectorRef,
}

#[test]
fn test_eval_py_vector() {
    let locals: HashMap<_, _> = sample_py_vector()
        .into_iter()
        .map(|(k, v)| (k, PyVector::from(v)))
        .collect();

    // for test basic operations
    // this is better than standalone testcases configure file
    let testcases = get_test_cases();

    for testcase in testcases {
        eval_pyo3(testcase.clone(), locals.clone());
        eval_rspy(testcase, locals.clone())
    }
}

fn sample_py_vector() -> HashMap<String, VectorRef> {
    let b1 = Arc::new(BooleanVector::from_slice(&[false, false, true, true])) as VectorRef;
    let b2 = Arc::new(BooleanVector::from_slice(&[false, true, false, true])) as VectorRef;
    let f1 = Arc::new(Float64Vector::from_slice(&[0.0f64, 2.0, 10.0, 42.0])) as VectorRef;
    let f2 = Arc::new(Float64Vector::from_slice(&[-0.1f64, -42.0, 2., 7.0])) as VectorRef;
    HashMap::from([
        ("b1".to_owned(), b1),
        ("b2".to_owned(), b2),
        ("f1".to_owned(), f1),
        ("f2".to_owned(), f2),
    ])
}

fn get_test_cases()-> Vec<TestCase>{
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
            result: Arc::new(Float64Vector::from_slice(&[-0.1f64, -40.0, 12., 49.0])) as VectorRef,
        },
        TestCase {
            eval: "f1-f2".to_string(),
            result: Arc::new(Float64Vector::from_slice(&[0.1f64, 44.0, 8., 35.0])) as VectorRef,
        },
        TestCase {
            eval: "f1*f2".to_string(),
            result: Arc::new(Float64Vector::from_slice(&[-0.0f64, -84.0, 20., 42.0 * 7.0]))
                as VectorRef,
        },
        TestCase {
            eval: "f1/f2".to_string(),
            result: Arc::new(Float64Vector::from_slice(&[
                0.0 / -0.1f64,
                2. / -42.,
                10. / 2.,
                42. / 7.,
            ])) as VectorRef,
        },
    ];
    Vec::from(testcases)
}


fn eval_pyo3(testcase: TestCase, locals: HashMap<String, PyVector>) {
    pyo3::prepare_freethreaded_python();
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
        if *raw_arr != *expect_arr{
            panic!("{raw_arr:?}!={expect_arr:?}")
        }
    })
}

fn eval_rspy(testcase: TestCase, locals: HashMap<String, PyVector>) {
    vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
        PyVector::make_class(&vm.ctx);
        let scope = vm.new_scope_with_builtins();
        locals.into_iter().for_each(|(k,v)|{
            scope
            .locals
            .as_object()
            .set_item(&k, vm.new_pyobj(v), vm).unwrap();
        });
        let code_obj = vm.compile(&testcase.eval,
                Mode::Eval,
                "<embedded>".to_owned(),
        ).map_err(|err| vm.new_syntax_error(&err)).unwrap();
        vm.run_code_obj(code_obj, scope).unwrap();
    });
}
