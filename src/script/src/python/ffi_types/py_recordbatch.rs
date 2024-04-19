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

//! PyRecordBatch is a Python class that wraps a RecordBatch,
//! and provide a PyMapping Protocol to
//! access the columns of the RecordBatch.

use common_recordbatch::RecordBatch;
use pyo3::exceptions::{PyKeyError, PyRuntimeError};
use pyo3::{pyclass as pyo3class, pymethods, PyObject, PyResult, Python};

use crate::python::ffi_types::PyVector;

/// This is a Wrapper around a RecordBatch, impl PyMapping Protocol so you can do both `a[0]` and `a["number"]` to retrieve column.
#[pyo3class(name = "PyRecordBatch")]
#[derive(Debug)]
pub(crate) struct PyRecordBatch {
    record_batch: RecordBatch,
}

impl PyRecordBatch {
    pub fn new(record_batch: RecordBatch) -> Self {
        Self { record_batch }
    }

    fn len(&self) -> usize {
        self.record_batch.num_rows()
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(record_batch: RecordBatch) -> Self {
        Self::new(record_batch)
    }
}

#[pymethods]
impl PyRecordBatch {
    fn __repr__(&self) -> String {
        // TODO(discord9): a better pretty print
        format!("{:#?}", &self.record_batch.df_record_batch())
    }
    fn __getitem__(&self, py: Python, key: PyObject) -> PyResult<PyVector> {
        let column = if let Ok(key) = key.extract::<String>(py) {
            self.record_batch.column_by_name(&key)
        } else if let Ok(key) = key.extract::<usize>(py) {
            Some(self.record_batch.column(key))
        } else {
            return Err(PyRuntimeError::new_err(format!(
                "Expect either str or int, found {key:?}"
            )));
        }
        .ok_or_else(|| PyKeyError::new_err(format!("Column {} not found", key)))?;
        let v = PyVector::from(column.clone());
        Ok(v)
    }
    fn __iter__(&self) -> PyResult<Vec<PyVector>> {
        let iter: Vec<_> = self
            .record_batch
            .columns()
            .iter()
            .map(|i| PyVector::from(i.clone()))
            .collect();
        Ok(iter)
    }
    fn __len__(&self) -> PyResult<usize> {
        Ok(self.len())
    }
}
