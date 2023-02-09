use datafusion::from_slice::FromSlice;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::value::{self, OrderedFloat};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyString};

use crate::ffi_types::vector::PyVector;

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
    get_con_type!(obj,
        PyBool => boolean_datatype,
        PyInt => int64_datatype,
        PyFloat => float64_datatype,
        PyString => string_datatype
    )
}

fn pyo3_is_obj_scalar(obj: &PyAny)->bool{
    get_py_type(obj).is_ok()
}

/// TODO(discord9): add similar methods
#[pymethods]
impl PyVector {
    /// create a `PyVector` with a `PyList` that contains only elements of same type
    #[new]
    fn py_new(iterable: &PyList) -> PyResult<Self> {
        let dtype = get_py_type(iterable.get_item(0)?)?;
        let mut buf = dtype.create_mutable_vector(iterable.len());
        for i in 0..iterable.len() {
            let element = iterable.get_item(i)?;
            let val = pyo3_obj_try_to_typed_val(element, Some(dtype.clone()))?;
            buf.push_value_ref(val.as_value_ref())
                .map_err(|e| e.to_string())
                .map_err(PyValueError::new_err)?;
        }
        Ok(buf.to_vector().into())
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
    fn __and__(&self, other: &Self) -> PyResult<Self> {
        Self::vector_and(self, other).map_err(PyValueError::new_err)
    }
    fn __or__(&self, other: &Self) -> PyResult<Self> {
        Self::vector_or(self, other).map_err(PyValueError::new_err)
    }
    fn __invert__(&self) -> PyResult<Self> {
        Self::vector_invert(self).map_err(PyValueError::new_err)
    }
    fn __len__(&self) -> usize {
        self.len()
    }
    fn __doc__(&self) -> PyResult<String> {
        Ok("PyVector is like a Python array, a compact array of elem of same datatype, but Readonly for now".to_string())
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

macro_rules! to_con_type {
    ($dtype:ident,$obj:ident, $($cty:ident => $rty:ty),*$(,)?) => {
        match $dtype {
            $(
                ConcreteDataType::$cty(_) => $obj.extract::<$rty>().map(value::Value::$cty),
            )*
            _ => unreachable!(),
        }
    };
    ($dtype:ident,$obj:ident, $($cty:ident =ord=> $rty:ty),*$(,)?) => {
        match $dtype {
            $(
                ConcreteDataType::$cty(_) => $obj.extract::<$rty>().map(OrderedFloat).map(value::Value::$cty),
            )*
            _ => unreachable!(),
        }
    };
}

/// to int/float/boolean, if dtype is None, then convert to highest prec type
fn pyo3_obj_try_to_typed_val(
    obj: &PyAny,
    dtype: Option<ConcreteDataType>,
) -> PyResult<value::Value> {
    if let Ok(num) = obj.downcast::<PyInt>() {
        if let Some(dtype) = dtype {
            if dtype.is_signed() || dtype.is_unsigned() {
                let ret = to_con_type!(dtype, num,
                    Int8 => i8,
                    Int16 => i16,
                    Int32 => i32,
                    Int64 => i64,
                    UInt8 => u8,
                    UInt16 => u16,
                    UInt32 => u32,
                    UInt64 => u64,
                )?;
                Ok(ret)
            } else {
                todo!()
            }
        } else {
            num.extract::<i64>().map(value::Value::Int64)
        }
    } else if let Ok(num) = obj.downcast::<PyFloat>() {
        if let Some(dtype) = dtype {
            if dtype.is_float() {
                let ret = to_con_type!(dtype, num,
                    Float32 =ord=> f32,
                    Float64 =ord=> f64,
                )?;
                Ok(ret)
            } else {
                // return error
                todo!()
            }
        } else {
            num.extract::<f64>()
                .map(|v| value::Value::Float64(OrderedFloat(v)))
        }
    } else if let Ok(b) = obj.downcast::<PyBool>() {
        todo!()
    } else {
        todo!()
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;
    use std::sync::Arc;

    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{BooleanVector, VectorRef};
    use pyo3::types::{PyDict, PyModule};
    use pyo3::{PyCell, Python};

    use crate::ffi_types::vector::PyVector;
    fn sample_vector() -> HashMap<String, PyVector> {
        let mut locals = HashMap::new();
        let b = BooleanVector::from_slice(&[true, false, true, true]);
        let b: PyVector = (Arc::new(b) as VectorRef).into();
        locals.insert("bool_v1".to_string(), b);
        let b = BooleanVector::from_slice(&[false, false, false, true]);
        let b: PyVector = (Arc::new(b) as VectorRef).into();
        locals.insert("bool_v2".to_string(), b);
        locals
    }
    #[test]
    fn test_py_vector_api() {
        let b = BooleanVector::from_slice(&[true, false, true, true]);
        let b = Arc::new(b) as VectorRef;
        let b: PyVector = b.into();
        pyo3::prepare_freethreaded_python();
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
"#,
                None,
                Some(locals),
            )
            .unwrap();
        });
    }
}
