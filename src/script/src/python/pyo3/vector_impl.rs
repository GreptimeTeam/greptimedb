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

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute;
use datafusion::arrow::compute::kernels::{arithmetic, comparison};
use datatypes::arrow::array::{Array, ArrayRef};
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::vectors::Helper;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyString};

use crate::python::ffi_types::vector::{arrow_rtruediv, wrap_bool_result, wrap_result, PyVector};
use crate::python::pyo3::utils::pyo3_obj_try_to_typed_val;

macro_rules! get_con_type {
    ($obj:ident, $($pyty:ident => $con_ty:ident),*$(,)?) => {
    $(
        if $obj.is_instance_of::<$pyty>()?{
            Ok(ConcreteDataType::$con_ty())
        }
    )else* else{
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
        F: Fn(&dyn Array, &dyn Array) -> Result<ArrayRef, String> + Send,
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
        F: Fn(&dyn Array, &dyn Array) -> Result<ArrayRef, String> + Send,
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
    /// create a `PyVector` with a `PyList` that contains only elements of same type
    #[new]
    pub(crate) fn py_new(iterable: &PyList) -> PyResult<Self> {
        let dtype = get_py_type(iterable.get_item(0)?)?;
        let mut buf = dtype.create_mutable_vector(iterable.len());
        for i in 0..iterable.len() {
            let element = iterable.get_item(i)?;
            let val = pyo3_obj_try_to_typed_val(element, Some(dtype.clone()))?;
            buf.push_value_ref(val.as_value_ref());
        }
        Ok(buf.to_vector().into())
    }
    fn __richcmp__(&self, py: Python<'_>, other: PyObject, op: CompareOp) -> PyResult<Self> {
        let op_fn = match op {
            CompareOp::Lt => comparison::lt_dyn,
            CompareOp::Le => comparison::lt_eq_dyn,
            CompareOp::Eq => comparison::eq_dyn,
            CompareOp::Ne => comparison::neq_dyn,
            CompareOp::Gt => comparison::gt_dyn,
            CompareOp::Ge => comparison::gt_eq_dyn,
        };
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, None, wrap_bool_result(op_fn))
        } else {
            self.pyo3_vector_arith_op(py, other, None, wrap_bool_result(op_fn))
        }
    }

    fn __add__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, None, wrap_result(arithmetic::add_dyn))
        } else {
            self.pyo3_vector_arith_op(py, other, None, wrap_result(arithmetic::add_dyn))
        }
    }
    fn __radd__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        self.__add__(py, other)
    }

    fn __sub__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, None, wrap_result(arithmetic::subtract_dyn))
        } else {
            self.pyo3_vector_arith_op(py, other, None, wrap_result(arithmetic::subtract_dyn))
        }
    }
    fn __rsub__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(
                py,
                other,
                None,
                wrap_result(|a, b| arithmetic::subtract_dyn(b, a)),
            )
        } else {
            self.pyo3_vector_arith_op(
                py,
                other,
                None,
                wrap_result(|a, b| arithmetic::subtract_dyn(b, a)),
            )
        }
    }
    fn __mul__(&self, py: Python<'_>, other: PyObject) -> PyResult<Self> {
        if pyo3_is_obj_scalar(other.as_ref(py)) {
            self.pyo3_scalar_arith_op(py, other, None, wrap_result(arithmetic::multiply_dyn))
        } else {
            self.pyo3_vector_arith_op(py, other, None, wrap_result(arithmetic::multiply_dyn))
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
                wrap_result(arithmetic::divide_dyn),
            )
        } else {
            self.pyo3_vector_arith_op(
                py,
                other,
                Some(ArrowDataType::Float64),
                wrap_result(arithmetic::divide_dyn),
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
                wrap_result(|a, b| arithmetic::divide_dyn(b, a)),
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
                wrap_result(arithmetic::divide_dyn),
            )
        } else {
            self.pyo3_vector_arith_op(
                py,
                other,
                Some(ArrowDataType::Int64),
                wrap_result(arithmetic::divide_dyn),
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
                wrap_result(|a, b| arithmetic::divide_dyn(b, a)),
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

        let f = Float64Vector::from_slice(&[0.0f64, 1.0, 42.0, 3.0]);
        let f: PyVector = (Arc::new(f) as VectorRef).into();
        locals.insert("fv1".to_string(), f);
        let f = Float64Vector::from_slice(&[1919.810f64, 0.114, 51.4, 3.0]);
        let f: PyVector = (Arc::new(f) as VectorRef).into();
        locals.insert("fv2".to_string(), f);
        locals
    }
    #[test]
    fn test_py_vector_api() {
        init_cpython_interpreter();
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
