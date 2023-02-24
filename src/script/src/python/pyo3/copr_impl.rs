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
use datatypes::vectors::{Helper, VectorRef};
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyDict, PyList, PyModule, PyTuple};
use pyo3::{pymethods, PyAny, PyCell, PyObject, PyResult, Python, ToPyObject};
use snafu::{ensure, Backtrace, GenerateImplicitData, ResultExt};

use crate::python::error::{self, NewRecordBatchSnafu, OtherSnafu, Result};
use crate::python::ffi_types::copr::PyQueryEngine;
use crate::python::ffi_types::{check_args_anno_real_type, select_from_rb, Coprocessor, PyVector};
use crate::python::pyo3::utils::{init_cpython_interpreter, pyo3_obj_try_to_typed_val};

#[pymethods]
impl PyQueryEngine {
    #[pyo3(name = "sql")]
    pub(crate) fn sql_pyo3(&self, py: Python<'_>, s: String) -> PyResult<PyObject> {
        let query = self.get_ref();
        let res = self
            .query_with_new_thread(query, s)
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
    rb: &RecordBatch,
    params: &HashMap<String, String>,
) -> Result<RecordBatch> {
    let arg_names = if let Some(names) = &copr.deco_args.arg_names {
        names
    } else {
        return OtherSnafu {
            reason: "PyO3 Backend doesn't support params yet".to_string(),
        }
        .fail();
    };
    let args: Vec<PyVector> = select_from_rb(rb, arg_names)?;
    check_args_anno_real_type(&args, copr, rb)?;
    // Just in case cpython is not inited
    init_cpython_interpreter();
    Python::with_gil(|py| -> Result<_> {
        let mut cols = (|| -> PyResult<_> {
            let dummy_decorator = "
# A dummy decorator, actual implementation is in Rust code
def copr(*dummy, **kwdummy):
    def inner(func):
        return func
    return inner
coprocessor = copr
";
            let script = format!("{}{}", dummy_decorator, copr.script);
            let module = PyModule::from_code(py, &script, "<embedded>", "<mod_embed>")?;
            let fun = module.getattr(copr.name.as_str())?;

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

            // TODO(discord9): set `dataframe` and `query` in scope/ or set it into module
            // idea: dynamically change `greptime` module
            // by insert instantiated `dataframe` and `query` into it
            // Expect either: a PyVector Or a List/Tuple of PyVector
            let result = fun.call(args, Some(kwargs))?;
            let col_len = rb.num_rows();
            py_any_to_vec(result, col_len)
        })()
        .map_err(|err| error::Error::PyRuntime {
            msg: err.to_string(),
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

    use crate::python::ffi_types::copr::{exec_parsed, parse};

    #[test]
    #[allow(unused_must_use)]
    fn simple_test_pyo3_copr() {
        let python_source = r#"
@copr(args=["cpu", "mem"], returns=["ref"], backend="pyo3")
def a(cpu, mem, **kwargs):
    import greptime as gt
    from greptime import vector, log2, sum, pow
    for k, v in kwargs.items():
        print("%s == %s" % (k, v))
    return (0.5 < cpu) & ~( cpu >= 0.75)
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
        dbg!(&copr);
        let ret = exec_parsed(
            &copr,
            &Some(rb),
            &HashMap::from([("a".to_string(), "1".to_string())]),
        );
        dbg!(ret);
    }
}
