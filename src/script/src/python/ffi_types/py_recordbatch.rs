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
use crossbeam_utils::atomic::AtomicCell;
#[cfg(feature = "pyo3_backend")]
use pyo3::{
    exceptions::{PyKeyError, PyRuntimeError},
    pyclass as pyo3class, pymethods, PyObject, PyResult, Python,
};
use rustpython_vm::builtins::PyStr;
use rustpython_vm::protocol::PyMappingMethods;
use rustpython_vm::types::AsMapping;
use rustpython_vm::{
    atomic_func, pyclass as rspyclass, PyObject as RsPyObject, PyPayload, PyResult as RsPyResult,
    VirtualMachine,
};

use crate::python::ffi_types::PyVector;

/// This is a Wrapper around a RecordBatch, impl PyMapping Protocol so you can do both `a[0]` and `a["number"]` to retrieve column.
#[cfg_attr(feature = "pyo3_backend", pyo3class(name = "PyRecordBatch"))]
#[rspyclass(module = false, name = "PyRecordBatch")]
#[derive(Debug, PyPayload)]
pub(crate) struct PyRecordBatch {
    record_batch: RecordBatch,
}

impl PyRecordBatch {
    pub fn new(record_batch: RecordBatch) -> Self {
        Self { record_batch }
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(record_batch: RecordBatch) -> Self {
        Self::new(record_batch)
    }
}

#[cfg(feature = "pyo3_backend")]
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

impl PyRecordBatch {
    fn len(&self) -> usize {
        self.record_batch.num_rows()
    }
    fn get_item(&self, needle: &RsPyObject, vm: &VirtualMachine) -> RsPyResult {
        if let Ok(index) = needle.try_to_value::<usize>(vm) {
            let column = self.record_batch.column(index);
            let v = PyVector::from(column.clone());
            Ok(v.into_pyobject(vm))
        } else if let Ok(index) = needle.try_to_value::<String>(vm) {
            let key = index.as_str();

            let v = self.record_batch.column_by_name(key).ok_or_else(|| {
                vm.new_key_error(PyStr::from(format!("Column {} not found", key)).into_pyobject(vm))
            })?;
            let v: PyVector = v.clone().into();
            Ok(v.into_pyobject(vm))
        } else {
            Err(vm.new_key_error(
                PyStr::from(format!("Expect either str or int, found {needle:?}"))
                    .into_pyobject(vm),
            ))
        }
    }
}

#[rspyclass(with(AsMapping))]
impl PyRecordBatch {
    #[pymethod(name = "__repr__")]
    fn rspy_repr(&self) -> String {
        format!("{:#?}", &self.record_batch.df_record_batch())
    }
}

impl AsMapping for PyRecordBatch {
    fn as_mapping() -> &'static PyMappingMethods {
        static AS_MAPPING: PyMappingMethods = PyMappingMethods {
            length: atomic_func!(|mapping, _vm| Ok(PyRecordBatch::mapping_downcast(mapping).len())),
            subscript: atomic_func!(
                |mapping, needle, vm| PyRecordBatch::mapping_downcast(mapping).get_item(needle, vm)
            ),
            ass_subscript: AtomicCell::new(None),
        };
        &AS_MAPPING
    }
}
