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

use common_recordbatch::RecordBatch;
use common_telemetry::timer;
use datatypes::vectors::{Helper, VectorRef};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::{PyDict, PyList, PyModule, PyTuple};
use pyo3::{pymethods, PyAny, PyCell, PyObject, PyResult, Python, ToPyObject};
use snafu::{ensure, Backtrace, GenerateImplicitData, ResultExt};

use crate::python::error::{self, NewRecordBatchSnafu, OtherSnafu, Result};
use crate::python::ffi_types::copr::PyQueryEngine;
use crate::python::ffi_types::{check_args_anno_real_type, select_from_rb, Coprocessor, PyVector};
use crate::python::metric;
use crate::python::pyo3::dataframe_impl::PyDataFrame;
use crate::python::pyo3::utils::{init_cpython_interpreter, pyo3_obj_try_to_typed_val};

#[pymethods]
impl PyQueryEngine {
    #[pyo3(name = "sql")]
    pub(crate) fn sql_pyo3(&self, py: Python<'_>, s: String) -> PyResult<PyObject> {
        let res = self
            .query_with_new_thread(s)
            .map_err(PyValueError::new_err)?;
        match res {
            crate::python::ffi_types::copr::Either::Rb(rbs) => {
                let mut top_vec = Vec::with_capacity(rbs.iter().count());
                for rb in rbs.iter() {
                    let mut vec_of_vec = Vec::with_capacity(rb.columns().len());
                    for v in rb.columns() {
                        let v = PyVector::from(v.to_owned());
                        let v = PyCell::new(py, v)?;
                        vec_of_vec.push(v.to_object(py));
                    }
                    let vec_of_vec = PyList::new(py, vec_of_vec);
                    top_vec.push(vec_of_vec);
                }
                let top_vec = PyList::new(py, top_vec);
                Ok(top_vec.to_object(py))
            }
            crate::python::ffi_types::copr::Either::AffectedRows(count) => Ok(count.to_object(py)),
        }
    }
    // TODO: put this into greptime module
}

/// Execute a `Coprocessor` with given `RecordBatch`
pub(crate) fn pyo3_exec_parsed(
    copr: &Coprocessor,
    rb: &Option<RecordBatch>,
    params: &HashMap<String, String>,
) -> Result<RecordBatch> {
    let _t = timer!(metric::METRIC_PYO3_EXEC_TOTAL_ELAPSED);
    // i.e params or use `vector(..)` to construct a PyVector
    let arg_names = &copr.deco_args.arg_names.clone().unwrap_or(vec![]);
    let args: Vec<PyVector> = if let Some(rb) = rb {
        let args = select_from_rb(rb, arg_names)?;
        check_args_anno_real_type(arg_names, &args, copr, rb)?;
        args
    } else {
        Vec::new()
    };
    // Just in case cpython is not inited
    init_cpython_interpreter().unwrap();
    Python::with_gil(|py| -> Result<_> {
        let _t = timer!(metric::METRIC_PYO3_EXEC_ELAPSED);

        let mut cols = (|| -> PyResult<_> {
            let dummy_decorator = "
# Postponed evaluation of annotations(PEP 563) so annotation can be set freely
# This is needed for Python < 3.9
from __future__ import annotations
# A dummy decorator, actual implementation is in Rust code
def copr(*dummy, **kwdummy):
    def inner(func):
        return func
    return inner
coprocessor = copr
";
            let gen_call = format!("\n_return_from_coprocessor = {}(*_args_for_coprocessor, **_kwargs_for_coprocessor)", copr.name);
            let script = format!("{}{}{}", dummy_decorator, copr.script, gen_call);

            let args = args
                .clone()
                .into_iter()
                .map(|v| PyCell::new(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            let args = PyTuple::new(py, args);

            let kwargs = PyDict::new(py);
            if let Some(_copr_kwargs) = &copr.kwarg {
                for (k, v) in params {
                    kwargs.set_item(k, v)?;
                }
            }

            let py_main = PyModule::import(py, "__main__")?;
            let globals = py_main.dict();

            let locals = PyDict::new(py);

            if let Some(engine) = &copr.query_engine {
                let query_engine = PyQueryEngine::from_weakref(engine.clone());
                let query_engine = PyCell::new(py, query_engine)?;
                globals.set_item("__query__", query_engine)?;
            }

            // TODO(discord9): find out why `dataframe` is not in scope
            if let Some(rb) = rb {
                let dataframe = PyDataFrame::from_record_batch(rb.df_record_batch())
                    .map_err(|err|
                        PyValueError::new_err(
                            format!("Can't create dataframe from record batch: {}", err
                        )
                    )
                )?;
                let dataframe = PyCell::new(py, dataframe)?;
                globals.set_item("__dataframe__", dataframe)?;
            }

            locals.set_item("_args_for_coprocessor", args)?;
            locals.set_item("_kwargs_for_coprocessor", kwargs)?;
            // `greptime` is already import when init interpreter, so no need to set in here

             // TODO(discord9): find a better way to set `dataframe` and `query` in scope/ or set it into module(latter might be impossible and not idomatic even in python)
            // set `dataframe` and `query` in scope/ or set it into module
            // could generate a call in python code and use Python::run to run it, just like in RustPython
            // Expect either: a PyVector Or a List/Tuple of PyVector
            py.run(&script, Some(globals), Some(locals))?;
            let result = locals.get_item("_return_from_coprocessor").ok_or_else(|| PyValueError::new_err("Can't find return value of coprocessor function"))?;

            let col_len = rb.as_ref().map(|rb| rb.num_rows()).unwrap_or(1);
            py_any_to_vec(result, col_len)
        })()
        .map_err(|err| error::Error::PyRuntime {
            msg: err.into_value(py).to_string(),
            backtrace: Backtrace::generate(),
        })?;
        ensure!(
            cols.len() == copr.deco_args.ret_names.len(),
            OtherSnafu {
                reason: format!(
                    "The number of return Vector is wrong, expect {}, found {}",
                    copr.deco_args.ret_names.len(),
                    cols.len()
                )
            }
        );
        copr.check_and_cast_type(&mut cols)?;
        let schema = copr.gen_schema(&cols)?;
        RecordBatch::new(schema, cols).context(NewRecordBatchSnafu)
    })
}

/// Cast return of py script result to `Vec<VectorRef>`,
/// constants will be broadcast to length of `col_len`
fn py_any_to_vec(obj: &PyAny, col_len: usize) -> PyResult<Vec<VectorRef>> {
    // check if obj is of two types:
    // 1. tuples of PyVector
    // 2. a single PyVector
    let check = if obj.is_instance_of::<PyTuple>()? {
        let tuple = obj.downcast::<PyTuple>()?;

        (0..tuple.len())
            .map(|idx| tuple.get_item(idx).map(|i| i.is_instance_of::<PyVector>()))
            .all(|i| matches!(i, Ok(Ok(true))))
    } else {
        obj.is_instance_of::<PyVector>()?
    };
    if !check {
        return Err(PyRuntimeError::new_err(format!(
            "Expect a tuple of vectors or one single vector, found {obj}"
        )));
    }
    if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let len = tuple.len();
        let v = (0..len)
            .map(|idx| tuple.get_item(idx))
            .map(|elem| {
                elem.map(|any| py_obj_broadcast_to_vec(any, col_len))
                    .and_then(|v| v)
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(v)
    } else {
        let ret = py_obj_broadcast_to_vec(obj, col_len)?;
        Ok(vec![ret])
    }
}

fn py_obj_broadcast_to_vec(obj: &PyAny, col_len: usize) -> PyResult<VectorRef> {
    if let Ok(v) = obj.extract::<PyVector>() {
        Ok(v.as_vector_ref())
    } else {
        let val = pyo3_obj_try_to_typed_val(obj, None)?;
        let handler = |e: datatypes::Error| PyValueError::new_err(e.to_string());
        let v = Helper::try_from_scalar_value(
            val.try_to_scalar_value(&val.data_type()).map_err(handler)?,
            col_len,
        )
        .map_err(handler)?;
        Ok(v)
    }
}

#[cfg(test)]
mod copr_test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_recordbatch::RecordBatch;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Float32Vector, Float64Vector, VectorRef};

    use crate::python::ffi_types::copr::{exec_parsed, parse, BackendType};

    #[test]
    #[allow(unused_must_use)]
    fn simple_test_pyo3_copr() {
        let python_source = r#"
@copr(args=["cpu", "mem"], returns=["ref"], backend="pyo3")
def a(cpu, mem, **kwargs):
    import greptime as gt
    from greptime import vector, log2, sum, pow, col, lit, dataframe
    for k, v in kwargs.items():
        print("%s == %s" % (k, v))
    print(dataframe().select([col("cpu")<lit(0.3)]).collect())
    return (0.5 < cpu) & ~(cpu >= 0.75)
    "#;
        let cpu_array = Float32Vector::from_slice([0.9f32, 0.8, 0.7, 0.3]);
        let mem_array = Float64Vector::from_slice([0.1f64, 0.2, 0.3, 0.4]);
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("cpu", ConcreteDataType::float32_datatype(), false),
            ColumnSchema::new("mem", ConcreteDataType::float64_datatype(), false),
        ]));
        let rb = RecordBatch::new(
            schema,
            [
                Arc::new(cpu_array) as VectorRef,
                Arc::new(mem_array) as VectorRef,
            ],
        )
        .unwrap();
        let copr = parse::parse_and_compile_copr(python_source, None).unwrap();
        assert_eq!(copr.backend, BackendType::CPython);
        let ret = exec_parsed(
            &copr,
            &Some(rb),
            &HashMap::from([("a".to_string(), "1".to_string())]),
        );
        dbg!(&ret);
        assert!(ret.is_ok());
    }
}
