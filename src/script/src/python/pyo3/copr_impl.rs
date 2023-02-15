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

use common_recordbatch::RecordBatch;
use datatypes::vectors::{Helper, VectorRef};
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyDict, PyModule, PyTuple};
use pyo3::{PyAny, PyCell, PyResult, Python};
use snafu::{ensure, Backtrace, GenerateImplicitData, ResultExt};

use crate::python::error::{self, NewRecordBatchSnafu, OtherSnafu, Result};
use crate::python::ffi_types::{check_args_anno_real_type, select_from_rb, Coprocessor, PyVector};
use crate::python::pyo3::utils::pyo3_obj_try_to_typed_val;

/// Execute a `Coprocessor` with given `RecordBatch`
pub(crate) fn pyo3_exec_parsed(copr: &Coprocessor, rb: &RecordBatch) -> Result<RecordBatch> {
    let args: Vec<PyVector> = select_from_rb(rb, &copr.deco_args.arg_names)?;
    check_args_anno_real_type(&args, copr, rb)?;
    // Just in case python is not inited
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| -> Result<_> {
        let mut cols = (|| -> PyResult<_> {
            let module = PyModule::from_code(py, &copr.script, "<embedded>", "<mod_embed>")?;
            let fun = module.getattr(copr.name.as_str())?;
            let kwargs = PyDict::new(py);
            for (k, v) in copr.deco_args.arg_names.iter().zip(args.into_iter()) {
                let v = PyCell::new(py, v)?;
                kwargs.set_item(k, v)?;
            }
            // Expect either: a PyVector Or a List/Tuple of PyVector
            let result = fun.call((), Some(kwargs))?;
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
