use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::ffi_types::vector::PyVector;

/// TODO(discord9): add similar methods
#[pymethods]
impl PyVector {
    #[new]
    fn py_new(iterable: &PyList) -> PyResult<Self> {
        todo!()
    }
    fn __add__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __radd__(&self, other: PyObject) -> PyResult<Self> {
        self.__add__(other)
    }

    fn __sub__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __rsub__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __mul__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __truediv__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __rtruediv__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __floordiv__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __rfloordiv__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __and__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __or__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __invert__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __doc__(&self, other: PyObject) -> PyResult<String> {
        Ok("PyVector is like a Python array, a compact array of elem of same datatype, but Readonly for now".to_string())
    }
}

fn py_obj_to_val(obj: PyObject) {}
