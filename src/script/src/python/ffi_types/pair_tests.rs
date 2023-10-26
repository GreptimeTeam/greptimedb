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

mod sample_testcases;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::kernels::numeric;
use common_query::Output;
use common_recordbatch::RecordBatch;
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::compute;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::VectorRef;
#[cfg(feature = "pyo3_backend")]
use pyo3::{types::PyDict, Python};
use rustpython_compiler::Mode;

use crate::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use crate::python::engine::sample_script_engine;
use crate::python::ffi_types::pair_tests::sample_testcases::{
    generate_copr_intgrate_tests, sample_test_case,
};
use crate::python::ffi_types::PyVector;
#[cfg(feature = "pyo3_backend")]
use crate::python::pyo3::{init_cpython_interpreter, vector_impl::into_pyo3_cell};
use crate::python::rspython::init_interpreter;

// TODO(discord9): paired test for slicing Vector
// & slice tests & lit() function for dataframe & test with full coprocessor&query engine ability
/// generate testcases that should be tested in paired both in RustPython and CPython
#[derive(Debug, Clone)]
struct CodeBlockTestCase {
    input: HashMap<String, VectorRef>,
    script: String,
    expect: VectorRef,
}

/// TODO(discord9): input a simple recordbatch, set a query engine, and such,
/// so that for a full Coprocessor it will work
#[derive(Debug, Clone, Default)]
struct CoprTestCase {
    // will be build to a RecordBatch and feed to coprocessor
    script: String,
    expect: Option<HashMap<String, VectorRef>>,
}

#[allow(unused)]
fn into_recordbatch(input: HashMap<String, VectorRef>) -> RecordBatch {
    let mut schema = Vec::new();
    let mut columns = Vec::new();
    for (name, v) in input {
        schema.push(ColumnSchema::new(name, v.data_type(), false));
        columns.push(v);
    }
    let schema = Arc::new(Schema::new(schema));

    RecordBatch::new(schema, columns).unwrap()
}

#[tokio::test]
#[allow(clippy::print_stdout)]
async fn integrated_py_copr_test() {
    let testcases = generate_copr_intgrate_tests();
    let script_engine = sample_script_engine();
    for (idx, case) in testcases.into_iter().enumerate() {
        println!("Testcase {idx}:\n script: {}", case.script);
        let script = case.script;
        let script = script_engine
            .compile(&script, CompileContext::default())
            .await
            .unwrap();
        let output = script
            .execute(HashMap::default(), EvalContext::default())
            .await
            .unwrap();
        let res = match output {
            Output::Stream(s) => common_recordbatch::util::collect_batches(s).await.unwrap(),
            Output::RecordBatches(rbs) => rbs,
            _ => unreachable!(),
        };
        let rb = res.iter().next().expect("One and only one recordbatch");
        if let Some(expect_result) = case.expect {
            let mut actual_result = HashMap::new();
            for col_sch in rb.schema.column_schemas() {
                let col = rb.column_by_name(&col_sch.name).unwrap();
                let _ = actual_result.insert(col_sch.name.clone(), col.clone());
            }
            for (name, col) in expect_result {
                let actual_col = actual_result.get(&name).unwrap_or_else(|| {
                    panic!("Expect column with name: {name} in {actual_result:?}")
                });
                if !check_equal(col.clone(), actual_col.clone()) {
                    panic!("Column {name} doesn't match, expect {col:?}, found {actual_col:?}")
                }
            }
        }
        println!(".. Ok");
    }
}

#[allow(clippy::print_stdout)]
#[test]
fn pyo3_rspy_test_in_pairs() {
    let testcases = sample_test_case();
    for case in testcases {
        println!("Testcase: {}", case.script);
        eval_rspy(case.clone());
        #[cfg(feature = "pyo3_backend")]
        eval_pyo3(case);
    }
}

fn check_equal(v0: VectorRef, v1: VectorRef) -> bool {
    let v0 = v0.to_arrow_array();
    let v1 = v1.to_arrow_array();
    if v0.len() != v1.len() {
        return false;
    }
    fn is_float(ty: &ArrowDataType) -> bool {
        use ArrowDataType::*;
        matches!(ty, Float16 | Float32 | Float64)
    }
    if is_float(v0.data_type()) || is_float(v1.data_type()) {
        let v0 = compute::cast(&v0, &ArrowDataType::Float64).unwrap();
        let v0 = v0.as_any().downcast_ref::<Float64Array>().unwrap();

        let v1 = compute::cast(&v1, &ArrowDataType::Float64).unwrap();
        let v1 = v1.as_any().downcast_ref::<Float64Array>().unwrap();

        let res = numeric::sub(v0, v1).unwrap();
        let res = res.as_any().downcast_ref::<Float64Array>().unwrap();
        res.iter().all(|v| {
            if let Some(v) = v {
                v.abs() <= 2.0 * f32::EPSILON as f64
            } else {
                false
            }
        })
    } else {
        *v0 == *v1
    }
}

/// will panic if something is wrong, used in tests only
fn eval_rspy(case: CodeBlockTestCase) {
    let interpreter = init_interpreter();
    interpreter.enter(|vm| {
        let scope = vm.new_scope_with_builtins();
        for (k, v) in case.input {
            let v = PyVector::from(v);
            scope.locals.set_item(&k, vm.new_pyobj(v), vm).unwrap();
        }
        let code_obj = vm
            .compile(&case.script, Mode::BlockExpr, "<embedded>".to_owned())
            .map_err(|err| {
                dbg!(&err);
                vm.new_syntax_error(&err)
            })
            .unwrap();
        let result_vector = vm
            .run_code_obj(code_obj, scope)
            .map_err(|e| {
                dbg!(&e);
                dbg!(&case.script);
                e
            })
            .unwrap()
            .downcast::<PyVector>()
            .unwrap();

        if !check_equal(result_vector.as_vector_ref(), case.expect.clone()) {
            panic!(
                "(RsPy)code:{}\nReal: {:?}!=Expected: {:?}",
                case.script, result_vector, case.expect
            )
        }
    });
}

#[cfg(feature = "pyo3_backend")]
fn eval_pyo3(case: CodeBlockTestCase) {
    init_cpython_interpreter().unwrap();
    Python::with_gil(|py| {
        let locals = {
            let locals_dict = PyDict::new(py);
            for (k, v) in case.input {
                let v = PyVector::from(v);
                locals_dict
                    .set_item(k, into_pyo3_cell(py, v).unwrap())
                    .unwrap();
            }
            locals_dict
        };
        py.run(&case.script, None, Some(locals)).unwrap();
        let res_vec = locals
            .get_item("ret")
            .unwrap()
            .extract::<PyVector>()
            .map_err(|e| {
                dbg!(&case.script);
                e
            })
            .unwrap();
        if !check_equal(res_vec.as_vector_ref(), case.expect.clone()) {
            panic!(
                "(PyO3)code:{}\nReal: {:?}!=Expected: {:?}",
                case.script, res_vec, case.expect
            )
        }
    })
}
