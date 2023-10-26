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

use arrow::array::{make_array, ArrayData, Datum};
use arrow::compute::kernels::{cmp, numeric};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute;
use datatypes::arrow::array::{Array, ArrayRef};
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::vectors::Helper;
use pyo3::exceptions::{PyIndexError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::{PyBool, PyFloat, PyInt, PySequence, PySlice, PyString, PyType};

use super::utils::val_to_py_any;
use crate::python::ffi_types::vector::{arrow_rtruediv, wrap_bool_result, wrap_result, PyVector};
use crate::python::pyo3::utils::{pyo3_obj_try_to_typed_val, to_py_err};

macro_rules! get_con_type {
    ($obj:ident, $($pyty:ident => $con_ty:ident),*$(,)?) => {
    $(
        if $obj.is_instance_of::<$pyty>() {
            Ok(ConcreteDataType::$con_ty())
        }
    ) else* else{
        Err(PyValueError::new_err("Unsupported pyobject type: {obj:?}"))
    }
    };
}

fn get_py_type(obj: &PyAny) -> PyResult<ConcreteDataType> {
    // Bool need to precede Int because `PyBool` is also a instance of `PyInt`
    get_con_type!(obj,
        PyBool => boolean_datatype,
        PyInt => int64_datatype,
        PyFloat => float64_datatype,
        PyString => string_datatype
    )
}

fn pyo3_is_obj_scalar(obj: &PyAny) -> bool {
    get_py_type(obj).is_ok()
}

impl PyVector {
    fn pyo3_scalar_arith_op<F>(
        &self,
        py: Python<'_>,
        right: PyObject,
        target_type: Option<ArrowDataType>,
        op: F,
    ) -> PyResult<Self>
    where
        F: Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, String> + Send,
    {
        let right = pyo3_obj_try_to_typed_val(right.as_ref(py), None)?;
        py.allow_threads(|| {
            self.scalar_arith_op(right, target_type, op)
                .map_err(PyValueError::new_err)
        })
    }
    fn pyo3_vector_arith_op<F>(
        &self,
        py: Python<'_>,
        right: PyObject,
        target_type: Option<ArrowDataType>,
        op: F,
    ) -> PyResult<Self>
    where
        F: Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, String> + Send,
    {
        let right = right.extract::<PyVector>(py)?;
        py.allow_threads(|| {
            self.vector_arith_op(&right, target_type, op)
                .map_err(PyValueError::new_err)
        })
    }
}

#[pymethods]
impl PyVector {
    /// convert from numpy array to [`PyVector`]
    #[classmethod]
    fn from_numpy(cls: &PyType, py: Python<'_>, obj: PyObject) -> PyResult<PyObject> {
        let pa = py.import("pyarrow")?;
        let obj = pa.call_method1("array", (obj,))?;
        let zelf = Self::from_pyarrow(cls, py, obj.into())?;
        Ok(zelf.into_py(py))
    }

    fn numpy(&self, py: Python<'_>) -> PyResult<PyObject> {
        let pa_arrow = self.to_arrow_array().to_data().to_pyarrow(py)?;
        let ndarray = pa_arrow.call_method0(py, "to_numpy")?;
        Ok(ndarray)
    }

    /// create a `PyVector` with a `PyList` that contains only elements of same type
    #[new]
    pub(crate) fn py_new(iterable: PyObject, py: Python<'_>) -> PyResult<Self> {
        let iterable = iterable.downcast::<PySequence>(py)?;
        let dtype = get_py_type(iterable.get_item(0)?)?;
        let mut buf = dtype.create_mutable_vector(iterable.len()?);
        for i in 0..iterable.len()? {
            let element = iterable.get_item(i)?;
            let val = pyo3_obj_try_to_typed_val(element, Some(dtype.clone()))?;
            buf.push_value_ref(val.as_value_ref());
        }
        Ok(buf.to_vector().into())
    }
    fn __richcmp__(&self, py: Python<'_>, other: PyObject, op: CompareOp) -> PyResult<Self> {
        let op_fn = match op {
            CompareOp::Lt => cmp::lt,
            CompareOp::Le => cmp::lt_eq,
            CompareOp::Eq => cmp::eq,
            CompareOp::Ne => cmp::neq,
            CompareOp::Gt => cmp::gt,
            CompareOp::Ge => cmp::gt_eq,
        };
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, None, wrap_bool_result(op_fn))
        } else {
            self.pyo3_vector_arith_op(py, other, None, wrap_bool_result(op_fn))
        }
    }

    fn __add__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, None, wrap_result(numeric::add))
        } else {
            self.pyo3_vector_arith_op(py, other, None, wrap_result(numeric::add))
        }
    }
    fn __radd__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        self.__add__(py, other)
    }

    fn __sub__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, None, wrap_result(numeric::sub))
        } else {
            self.pyo3_vector_arith_op(py, other, None, wrap_result(numeric::sub))
        }
    }
    fn __rsub__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, None, wrap_result(|a, b| numeric::sub(b, a)))
        } else {
            self.pyo3_vector_arith_op(py, other, None, wrap_result(|a, b| numeric::sub(b, a)))
        }
    }
    fn __mul__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, None, wrap_result(numeric::mul))
        } else {
            self.pyo3_vector_arith_op(py, other, None, wrap_result(numeric::mul))
        }
    }
    fn __rmul__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        self.__mul__(py, other)
    }
    fn __truediv__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(
                py,
                other,
                Some(ArrowDataType::Float64),
                wrap_result(numeric::div),
            )
        } else {
            self.pyo3_vector_arith_op(
                py,
                other,
                Some(ArrowDataType::Float64),
                wrap_result(numeric::div),
            )
        }
    }
    #[allow(unused)]
    fn __rtruediv__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, Some(ArrowDataType::Float64), arrow_rtruediv)
        } else {
            self.pyo3_vector_arith_op(
                py,
                other,
                Some(ArrowDataType::Float64),
                wrap_result(|a, b| numeric::div(b, a)),
            )
        }
    }
    #[allow(unused)]
    fn __floordiv__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(
                py,
                other,
                Some(ArrowDataType::Int64),
                wrap_result(numeric::div),
            )
        } else {
            self.pyo3_vector_arith_op(
                py,
                other,
                Some(ArrowDataType::Int64),
                wrap_result(numeric::div),
            )
        }
    }
    #[allow(unused)]
    fn __rfloordiv__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, Some(ArrowDataType::Int64), arrow_rtruediv)
        } else {
            self.pyo3_vector_arith_op(
                py,
                other,
                Some(ArrowDataType::Int64),
                wrap_result(|a, b| numeric::div(b, a)),
            )
        }
    }
    fn __and__(&self, other: &Self) -> PyResult<Self> {
        Self::vector_and(self, other).map_err(PyValueError::new_err)
    }
    fn __or__(&self, other: &Self) -> PyResult<Self> {
        Self::vector_or(self, other).map_err(PyValueError::new_err)
    }
    fn __invert__(&self) -> PyResult<Self> {
        Self::vector_invert(self).map_err(PyValueError::new_err)
    }

    #[pyo3(name = "concat")]
    fn pyo3_concat(&self, py: Python<'_>, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| {
            let left = self.to_arrow_array();
            let right = other.to_arrow_array();

            let res = compute::concat(&[left.as_ref(), right.as_ref()]);
            let res = res.map_err(|err| PyValueError::new_err(format!("Arrow Error: {err:#?}")))?;
            let ret = Helper::try_into_vector(res.clone()).map_err(|e| {
                PyValueError::new_err(format!(
                    "Can't cast result into vector, result: {res:?}, err: {e:?}",
                ))
            })?;
            Ok(ret.into())
        })
    }

    /// take a boolean array and filters the Array, returning elements matching the filter (i.e. where the values are true).
    #[pyo3(name = "filter")]
    fn pyo3_filter(&self, py: Python<'_>, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| {
            let left = self.to_arrow_array();
            let right = other.to_arrow_array();
            if let Some(filter) = right.as_any().downcast_ref::<BooleanArray>() {
                let res = compute::filter(left.as_ref(), filter);
                let res =
                    res.map_err(|err| PyValueError::new_err(format!("Arrow Error: {err:#?}")))?;
                let ret = Helper::try_into_vector(res.clone()).map_err(|e| {
                    PyValueError::new_err(format!(
                        "Can't cast result into vector, result: {res:?}, err: {e:?}",
                    ))
                })?;
                Ok(ret.into())
            } else {
                Err(PyValueError::new_err(format!(
                    "Can't cast operand into a Boolean Array, which is {right:#?}"
                )))
            }
        })
    }
    fn __len__(&self) -> usize {
        self.len()
    }
    fn __doc__(&self) -> PyResult<String> {
        Ok("PyVector is like a Python array, a compact array of elem of same datatype, but Readonly for now".to_string())
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{self:#?}"))
    }
    /// Convert to `pyarrow` 's array
    pub(crate) fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        self.to_arrow_array().to_data().to_pyarrow(py)
    }
    /// Convert from `pyarrow`'s array
    #[classmethod]
    pub(crate) fn from_pyarrow(_cls: &PyType, py: Python, obj: PyObject) -> PyResult<PyVector> {
        let array = make_array(ArrayData::from_pyarrow(obj.as_ref(py))?);
        let v = Helper::try_into_vector(array).map_err(to_py_err)?;
        Ok(v.into())
    }

    /// PyO3's Magic Method for slicing and indexing
    fn __getitem__(&self, py: Python, needle: PyObject) -> PyResult<PyObject> {
        if let Ok(needle) = needle.extract::<PyVector>(py) {
            let mask = needle.to_arrow_array();
            let mask = mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    PyValueError::new_err(
                        "A Boolean Array is requested for slicing, found {mask:?}",
                    )
                })?;
            let result = compute::filter(&self.to_arrow_array(), mask)
                .map_err(|err| PyRuntimeError::new_err(format!("Arrow Error: {err:#?}")))?;
            let ret = Helper::try_into_vector(result.clone()).map_err(|e| {
                PyRuntimeError::new_err(format!("Can't cast result into vector, err: {e:?}"))
            })?;
            let ret = Self::from(ret).into_py(py);
            Ok(ret)
        } else if let Ok(slice) = needle.downcast::<PySlice>(py) {
            let indices = slice.indices(self.len() as _)?;
            let (start, stop, step, _slicelength) = (
                indices.start,
                indices.stop,
                indices.step,
                indices.slicelength,
            );
            if start < 0 {
                return Err(PyValueError::new_err(format!(
                    "Negative start is not supported, found {start} in {indices:?}"
                )));
            } // Negative stop is supported, means from "indices.start" to the actual start of the vector
            let vector = self.as_vector_ref();

            let mut buf = vector
                .data_type()
                .create_mutable_vector(indices.slicelength as usize);
            let v = if indices.slicelength == 0 {
                buf.to_vector()
            } else {
                if indices.step > 0 {
                    let range = if stop == -1 {
                        start as usize..start as usize
                    } else {
                        start as usize..stop as usize
                    };
                    for i in range.step_by(step.unsigned_abs()) {
                        buf.push_value_ref(vector.get_ref(i));
                    }
                } else {
                    // if no-empty, then stop < start
                    // note: start..stop is empty is start >= stop
                    // stop>=-1
                    let range = { (stop + 1) as usize..=start as usize };
                    for i in range.rev().step_by(step.unsigned_abs()) {
                        buf.push_value_ref(vector.get_ref(i));
                    }
                }
                buf.to_vector()
            };
            let v: PyVector = v.into();
            Ok(v.into_py(py))
        } else if let Ok(index) = needle.extract::<isize>(py) {
            // deal with negative index
            let len = self.len() as isize;
            let index = if index < 0 { len + index } else { index };
            if index < 0 || index >= len {
                return Err(PyIndexError::new_err(format!(
                    "Index out of bound, index: {index}, len: {len}",
                    index = index,
                    len = len
                )));
            }
            let val = self.as_vector_ref().get(index as usize);
            val_to_py_any(py, val)
        } else {
            Err(PyValueError::new_err(
                "{needle:?} is neither a Vector nor a int, can't use for slicing or indexing",
            ))
        }
    }
}

#[cfg(test)]
pub(crate) fn into_pyo3_cell(py: Python, val: PyVector) -> PyResult<&PyCell<PyVector>> {
    PyCell::new(py, val)
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;
    use std::sync::Arc;

    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{BooleanVector, Float64Vector, VectorRef};
    use pyo3::types::{PyDict, PyModule};
    use pyo3::{PyCell, Python};

    use crate::python::ffi_types::vector::PyVector;
    use crate::python::pyo3::init_cpython_interpreter;
    fn sample_vector() -> HashMap<String, PyVector> {
        let mut locals = HashMap::new();
        let b = BooleanVector::from_slice(&[true, false, true, true]);
        let b: PyVector = (Arc::new(b) as VectorRef).into();
        locals.insert("bv1".to_string(), b);
        let b = BooleanVector::from_slice(&[false, false, false, true]);
        let b: PyVector = (Arc::new(b) as VectorRef).into();
        locals.insert("bv2".to_string(), b);

        let f = Float64Vector::from_slice([0.0f64, 1.0, 42.0, 3.0]);
        let f: PyVector = (Arc::new(f) as VectorRef).into();
        locals.insert("fv1".to_string(), f);
        let f = Float64Vector::from_slice([1919.810f64, 0.114, 51.4, 3.0]);
        let f: PyVector = (Arc::new(f) as VectorRef).into();
        locals.insert("fv2".to_string(), f);
        locals
    }
    #[test]
    fn test_py_vector_api() {
        init_cpython_interpreter().unwrap();
        Python::with_gil(|py| {
            let module = PyModule::new(py, "gt").unwrap();
            module.add_class::<PyVector>().unwrap();
            // Import and get sys.modules
            let sys = PyModule::import(py, "sys").unwrap();
            let py_modules: &PyDict = sys.getattr("modules").unwrap().downcast().unwrap();

            // Insert foo into sys.modules
            py_modules.set_item("gt", module).unwrap();

            let locals = PyDict::new(py);
            for (k, v) in sample_vector() {
                locals.set_item(k, PyCell::new(py, v).unwrap()).unwrap();
            }
            // ~bool_v1&bool_v2
            py.run(
                r#"
from gt import vector
print(vector([1,2]))
print(fv1+fv2)
"#,
                None,
                Some(locals),
            )
            .unwrap();
        });
    }
}
